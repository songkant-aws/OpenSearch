/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CloseableIterator;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShuffleBufferAccess;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;
import org.opensearch.analytics.spi.ShuffleScanInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

/**
 * Handler for {@link ShuffleScanInstructionNode} on a hash-shuffle worker.
 *
 * <p>Bridges the node-local {@link org.opensearch.analytics.exec.shuffle.ShuffleBufferManager}
 * to a DataFusion {@code StreamingTable} via {@link
 * NativeBridge#registerPartitionStreamOnSessionContext} — the worker's Substrait plan
 * references the resulting {@code NamedScan} so the hash-join's input resolves to the
 * partitioned stream.
 *
 * <p>The handler runs synchronously on the data-node executor thread that processes the
 * fragment's instruction list. It waits only until its side has a first chunk (or reaches EOF),
 * then live-drains later chunks while producers are still running and pushes each IPC chunk into
 * the native sender. Rust performs IPC decode directly into the bounded DataFusion input channel.
 *
 * <p>Chain-ordering requirement: same as {@link BroadcastInjectionHandler} — must run AFTER
 * {@link ShardScanInstructionHandler} so the {@code SessionContextHandle} exists. The
 * dispatcher appends two of these per partition (left, right) after the shard-scan setup.
 *
 * <p>Schema discovery: the partition's IPC chunks each include their own schema header (per
 * the Arrow IPC stream spec). The handler reads the first chunk's header to derive the schema
 * for the streaming-table registration. If the buffer's accumulated bytes are empty (no
 * producer rows for this partition) the handler treats the partition as a zero-batch stream —
 * the registered table is still resolvable so the worker plan binds, and the join produces
 * zero rows from this partition.
 *
 * <p>Failure surface: any IO exception during decode, alignment, or FFM registration is
 * surfaced as a {@link RuntimeException} that propagates back through
 * {@code AnalyticsSearchService}'s instruction-handler loop and fails the fragment.
 *
 * @opensearch.internal
 */
public class ShuffleScanHandler implements FragmentInstructionHandler<ShuffleScanInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(ShuffleScanHandler.class);

    /** Cap on how long the consumer waits for a side's first chunk/EOF and for each subsequent
     *  empty-queue interval while producers remain active. The cap exists solely as a backstop
     *  against stuck producers (cancelled queries cascade through the
     *  walker faster than this). Operator-tuneable via {@code analytics.mpp.shuffle.recv_timeout}
     *  once that cluster setting is plumbed into {@link ShardScanExecutionContext}; today the
     *  handler reads the JVM system property of the same name as a stopgap so integration
     *  tests can dial it down without waiting on full SPI plumbing.
     *
     *  <p>5s default keeps test timelines tight while leaving headroom for slow CI hosts.
     *  Real shuffle producers are far faster — a single batch RTT over local transport is
     *  microseconds. */
    private static final long DEFAULT_AWAIT_READY_TIMEOUT_MS = Long.parseLong(
        System.getProperty("analytics.mpp.shuffle.recv_timeout_ms", "5000")
    );

    @Override
    public BackendExecutionContext apply(
        ShuffleScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        if (!(backendContext instanceof DataFusionSessionState sessionState)) {
            throw new IllegalStateException(
                "ShuffleScanHandler: expected DataFusionSessionState from a prior handler "
                    + "(typically ShardScanInstructionHandler), got "
                    + (backendContext == null ? "null" : backendContext.getClass().getSimpleName())
                    + ". The shuffle-scan instruction must be appended after the scan-setup instruction "
                    + "in the worker stage's plan alternative."
            );
        }
        if (!(commonContext instanceof ShardScanExecutionContext shardCtx)) {
            throw new IllegalStateException(
                "ShuffleScanHandler: expected ShardScanExecutionContext, got "
                    + (commonContext == null ? "null" : commonContext.getClass().getSimpleName())
            );
        }
        ShuffleBufferRegistry registry = shardCtx.getShuffleBufferRegistry();
        if (registry == null) {
            throw new IllegalStateException(
                "ShuffleScanHandler: ShuffleBufferRegistry not plumbed into ShardScanExecutionContext. "
                    + "AnalyticsSearchService.setShuffleBufferRegistry must be called at plugin startup."
            );
        }
        // The instruction carries side ("left"/"right") explicitly. namedInputId is the canonical
        // "input-<producerStageId>" the fragment convertor emits for the StageInputScan leaf below
        // the stripped OpenSearchShuffleExchange — that's what the worker's Substrait plan binds
        // its NamedScan against.
        String inputId = node.getNamedInputId();
        String side = node.getSide();
        boolean isLeftSide = "left".equals(side);
        if (!isLeftSide && !"right".equals(side)) {
            throw new IllegalStateException(
                "ShuffleScanHandler: side must be 'left' or 'right', got '" + side + "' (namedInputId=" + inputId + ")"
            );
        }

        ShuffleBufferAccess buffer = registry.getOrCreate(node.getQueryId(), node.getTargetStageId(), node.getShufflePartitionIndex());
        // expectedSenders for both sides are set eagerly by the ShuffleWorkerSetupHandler
        // (which runs before any ShuffleScanHandler) so the buffer knows BOTH sides' counts
        // before either side's awaitReady call blocks. Setting them per-side here would
        // deadlock — the second side's count is set only AFTER the first side's blocked.

        try {
            LOGGER.debug(
                "ShuffleScanHandler: awaiting partition stream queryId={}, stage={}, partition={}, side={}, expectedSenders={}",
                node.getQueryId(),
                node.getTargetStageId(),
                node.getShufflePartitionIndex(),
                side,
                node.getExpectedSenders()
            );
            // Wait only for this side's first chunk or EOF. The live iterator below continues to
            // wait for later chunks, so DataFusion can build/consume while transport is in flight.
            if (!buffer.awaitReadable(side, DEFAULT_AWAIT_READY_TIMEOUT_MS)) {
                throw new RuntimeException(
                    "ShuffleScanHandler: timed out waiting for shuffle input to become readable for "
                        + inputId
                        + " (timeout="
                        + DEFAULT_AWAIT_READY_TIMEOUT_MS
                        + "ms)"
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("ShuffleScanHandler: interrupted while awaiting shuffle producers for " + inputId, e);
        }

        // LAZY drain: pull chunks one at a time. With spill, only ONE chunk is heap-resident at a
        // time — the rest stream from the spill file — so an over-budget partition drains without
        // re-materializing (the whole point of disk spill). The iterator owns the spill-file handle.
        CloseableIterator<byte[]> chunks = isLeftSide
            ? buffer.streamLeft(DEFAULT_AWAIT_READY_TIMEOUT_MS)
            : buffer.streamRight(DEFAULT_AWAIT_READY_TIMEOUT_MS);

        // Peek the first chunk for the schema (one chunk in heap is fine). No first chunk → empty
        // partition: register an empty memtable and return. Close the iterator on every path here.
        byte[] firstChunk;
        try {
            firstChunk = chunks.hasNext() ? chunks.next() : null;
        } catch (RuntimeException e) {
            chunks.close();
            throw new RuntimeException("ShuffleScanHandler: failed to read first shuffle chunk for " + inputId, e);
        }
        // M3 agg-shuffle worker: the producer ships the PARTIAL aggregate's physical batches whose
        // state columns are named <alias>[<state>] (e.g. sum_qty[sum]), but the worker FINAL
        // fragment's Substrait base_schema declares the Calcite LOGICAL names (sum_qty). DataFusion's
        // Substrait consumer binds base_schema to the registered provider BY NAME, so registering the
        // table with the raw IPC (physical) names fails the FINAL with "No field named sum_qty". When
        // producerPlanBytes is present, register via the partial-plan derivation (mirrors the working
        // coordinator-reduce register_partition_stream) so the table carries logical names; the
        // physically-named batches still feed in positionally. Null producerPlanBytes (join-shuffle)
        // keeps the raw-IPC path unchanged.
        byte[] producerPlanBytes = node.getProducerPlanBytes();

        if (firstChunk == null) {
            chunks.close();
            if (producerPlanBytes != null) {
                // Empty agg-shuffle partition: register the (logical-named) streaming table from the
                // partial plan so the FINAL binds, then close the sender immediately — 0 rows for
                // this partition. A 0-column empty memtable would fail the FINAL's by-name bind.
                LOGGER.debug(
                    "ShuffleScanHandler: empty partition for {} (queryId={}, stage={}, part={}); registering logical-named empty stream from partial plan",
                    inputId,
                    node.getQueryId(),
                    node.getTargetStageId(),
                    node.getShufflePartitionIndex()
                );
                long emptySenderPtr = NativeBridge.registerPartitionStreamOnSessionContextFromPartialPlan(
                    sessionState.sessionContextHandle().getPointer(),
                    inputId,
                    producerPlanBytes
                );
                new DatafusionPartitionSender(emptySenderPtr).close();
                return backendContext;
            }
            LOGGER.debug(
                "ShuffleScanHandler: empty partition for {} (queryId={}, stage={}, part={}); registering empty memtable",
                inputId,
                node.getQueryId(),
                node.getTargetStageId(),
                node.getShufflePartitionIndex()
            );
            NativeBridge.registerMemtableOnSessionContext(
                sessionState.sessionContextHandle().getPointer(),
                inputId,
                new byte[0],
                new long[0],
                new long[0]
            );
            return backendContext;
        }

        // Register the partition stream. Any failure here must close the chunk iterator (which owns an
        // open spill-file handle when the partition was spilled) — ownership only transfers to the
        // drain thread AFTER a sender is successfully constructed below. (codex review: spill-file
        // handle leak on registration throw.)
        long senderPtr;
        try {
            if (producerPlanBytes != null) {
                // Agg-shuffle worker: register with the LOGICAL schema derived by re-lowering the
                // producer's PARTIAL plan (fixes the q1/q15 sum_qty[sum] by-name bind).
                senderPtr = NativeBridge.registerPartitionStreamOnSessionContextFromPartialPlan(
                    sessionState.sessionContextHandle().getPointer(),
                    inputId,
                    producerPlanBytes
                );
            } else {
                // Join-shuffle: schema from the first chunk's IPC header (producer ships raw rows whose
                // names already match the consumer's expected input — no re-lowering needed).
                senderPtr = NativeBridge.registerPartitionStreamOnSessionContext(
                    sessionState.sessionContextHandle().getPointer(),
                    inputId,
                    firstChunk
                );
            }
        } catch (Exception e) {
            chunks.close();
            throw new RuntimeException("ShuffleScanHandler: failed to register partition stream for " + inputId, e);
        }
        DatafusionPartitionSender sender = new DatafusionPartitionSender(senderPtr);

        // Drain chunks into the native sender on a background thread. The native partition
        // stream is a bounded mpsc (capacity 4): synchronous draining inside this handler
        // would block on the 5th send because the consumer (engine.execute → HashJoinExec)
        // doesn't run until ALL handlers finish. Running the drain off-thread lets
        // engine.execute start in parallel, drain the channel, and unblock the sender.
        //
        // The background thread owns the lifecycle of BOTH the chunk iterator (releases the
        // spill-file handle) and the sender (closing it signals EOF to the native StreamingTable).
        // Failures still close both so the partition terminates rather than hanging the join.
        final byte[] firstChunkFinal = firstChunk;
        final CloseableIterator<byte[]> chunkIter = chunks;
        final DatafusionPartitionSender finalSender = sender;
        Thread drainThread = new Thread(() -> {
            int chunkCount = 0;
            Throwable drainFailure = null;
            try {
                finalSender.sendIpc(firstChunkFinal);
                chunkCount++;
                while (finalSender.isReceiverDropped() == false && chunkIter.hasNext()) {
                    finalSender.sendIpc(chunkIter.next());
                    chunkCount++;
                }
                LOGGER.debug(
                    "ShuffleScanHandler.drain: drained {} native IPC chunks for {} (side={}, partition={})",
                    chunkCount,
                    inputId,
                    side,
                    node.getShufflePartitionIndex()
                );
            } catch (Throwable t) {
                // A drain/IPC/spill-read failure here means the partition stream is TRUNCATED. Closing
                // the sender cleanly would signal EOF to the native StreamingTable, so the join/agg
                // would silently produce WRONG results from partial input. Instead FAIL the sender: it
                // pushes an error into the channel so the consumer's stream yields an ERROR and the
                // query fails loudly rather than under-delivering. (#17: native df_sender_fail.)
                drainFailure = t;
                LOGGER.error(
                    "ShuffleScanHandler.drain FAILED for " + inputId + " — partition stream truncated, failing the consumer stream",
                    t
                );
            } finally {
                try {
                    chunkIter.close();
                } catch (Throwable closeErr) {
                    LOGGER.warn("ShuffleScanHandler.drain: chunk iterator close failed for " + inputId, closeErr);
                }
                try {
                    if (drainFailure != null) {
                        // Truncated partition: surface an error to the consumer (not a clean EOF).
                        finalSender.fail("shuffle drain failed for " + inputId + " (side=" + side + "): " + drainFailure);
                    } else {
                        finalSender.close();
                    }
                } catch (Throwable closeErr) {
                    LOGGER.warn("ShuffleScanHandler.drain: sender teardown failed for " + inputId, closeErr);
                }
            }
        }, "shuffle-drain-" + node.getQueryId() + "-" + node.getTargetStageId() + "-" + side + "-" + node.getShufflePartitionIndex());
        drainThread.setDaemon(true);
        drainThread.start();

        return backendContext;
    }

}
