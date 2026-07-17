/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Streaming input for coordinator-reduce execution.
//!
//! A [`channel`] produces a paired [`PartitionStreamSender`] /
//! [`PartitionStreamReceiver`]. The sender is exposed through the FFM bridge so
//! Java can push Arrow `RecordBatch`es synchronously via
//! [`PartitionStreamSender::send_blocking`]. The receiver implements DataFusion's
//! [`RecordBatchStream`] and is wrapped in a [`SingleReceiverPartition`] so it
//! can be registered on a `SessionContext` as a `StreamingTable`.
//!
//! # Backpressure
//!
//! The underlying mpsc is bounded (capacity 4) — chosen small so the sender
//! back-pressures when the DataFusion execute side falls behind. Under load the
//! Java feeder thread blocks on `send_blocking`, which naturally stalls the
//! shard response pipeline.
//!
//! # Spill-backed replay
//!
//! Replay is opt-in. A normal exchange partition is one-shot even when the
//! DataFusion disk manager is enabled, avoiding an unconditional disk tee for
//! every shuffle batch. Callers that can actually execute an input more than
//! once construct a replayable partition; its first pass forwards batches while
//! writing a spill file, and later sequential executions read that file.

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::SpillManager;
use futures::{stream, Stream, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Bounded channel capacity. Small by design — producers back-pressure when the
/// DataFusion execute side falls behind.
const CHANNEL_CAPACITY: usize = 4;

/// Producer side of a partition stream.
///
/// Owned by the FFM bridge via `Box::into_raw`; dropping the sender (e.g. via
/// `df_sender_close`) closes the channel, which signals EOF to the DataFusion
/// receiver side.
pub struct PartitionStreamSender {
    tx: mpsc::Sender<Result<RecordBatch, DataFusionError>>,
    schema: SchemaRef,
    /// Out-of-band terminal-failure flag shared with the receiver. Set by [`try_fail`] when the
    /// producer/drain died mid-stream. The receiver consults it on channel-close so a truncated
    /// partition surfaces as an `Err` even when the bounded data channel was full and the error
    /// couldn't be enqueued in-band (then the sender drops → channel closes → would otherwise look
    /// like a clean EOF). (codex round-5 BLOCKER #1.)
    failure: Arc<OnceLock<String>>,
}

/// Outcome of a blocking send. `ReceiverDropped` is the benign terminal case — the
/// DataFusion consumer finished (e.g. a `LimitExec` satisfied its fetch) and dropped the
/// receiver. Surfaced as a distinct variant so the FFM layer can signal it without the Java
/// side substring-matching an error message.
#[must_use]
pub enum SendOutcome {
    Sent,
    ReceiverDropped,
}

impl PartitionStreamSender {
    /// Returns the schema this sender was created with.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Push a batch into the channel from a synchronous (non-async) context.
    ///
    /// The provided `handle` is used to drive the async send — typically the
    /// `io_runtime` handle from the global `RuntimeManager`. This lets the FFM
    /// bridge push without being async itself and without requiring the calling
    /// thread to be a Tokio worker.
    ///
    /// Blocks while the channel is full (natural backpressure). Returns
    /// [`SendOutcome::ReceiverDropped`] only if the receiver has been dropped —
    /// the sole failure mode of `mpsc::Sender::send`.
    pub fn send_blocking(
        &self,
        batch: Result<RecordBatch, DataFusionError>,
        handle: &Handle,
    ) -> SendOutcome {
        match handle.block_on(self.tx.send(batch)) {
            Ok(()) => SendOutcome::Sent,
            Err(_) => SendOutcome::ReceiverDropped,
        }
    }

    /// Marks the stream as TERMINALLY FAILED. Used by the failure path (`sender_fail`) when the
    /// producer/drain died mid-stream: the consumer must see an `Err` (so the query fails) rather than
    /// a clean EOF. Records the reason in the out-of-band `failure` flag (shared with the receiver) and
    /// returns immediately — NO in-band channel send, so it can NEVER block/deadlock against a full
    /// channel + non-polling consumer (the round-4 concern), and never double-emits the error.
    ///
    /// Delivery is via the flag, not the channel: the caller (`sender_fail`) drops the sender right
    /// after this, which closes the channel; the receiver drains any already-queued `Ok` batches in
    /// FIFO order, then on close consults the flag and yields the recorded error as its terminal item
    /// instead of `None`. So a truncated partition can NEVER be consumed as complete, regardless of
    /// channel fullness. `OnceLock` — the first failure wins; later calls are no-ops. (codex round-5
    /// BLOCKER #1; supersedes the round-4 best-effort in-band approach.)
    pub fn fail(&self, err: DataFusionError) {
        let _ = self.failure.set(err.to_string());
    }
}

impl fmt::Debug for PartitionStreamSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionStreamSender")
            .field("schema", &self.schema)
            .finish()
    }
}

/// Consumer side of a partition stream.
///
/// Implements [`Stream`] + [`RecordBatchStream`] so DataFusion can poll it
/// directly. Typically handed to [`SingleReceiverPartition`] and registered on
/// a `SessionContext` as a `StreamingTable`.
pub struct PartitionStreamReceiver {
    rx: mpsc::Receiver<Result<RecordBatch, DataFusionError>>,
    schema: SchemaRef,
    /// Shared with the sender (see [`PartitionStreamSender::failure`]). On channel-close, if this is
    /// set the receiver yields it as a terminal `Err` rather than a clean `None`/EOF.
    failure: Arc<OnceLock<String>>,
    /// Guards the out-of-band error so it's emitted exactly once: after yielding the `Err` we set
    /// this and then return `None` on the next poll.
    failure_emitted: bool,
}

impl fmt::Debug for PartitionStreamReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionStreamReceiver")
            .field("schema", &self.schema)
            .finish()
    }
}

impl Stream for PartitionStreamReceiver {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.failure_emitted {
            // Terminal: we already surfaced the out-of-band error. Stream is done.
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            // Channel closed (sender dropped). If the producer/drain recorded an out-of-band failure
            // (try_fail) but couldn't enqueue it in-band (full channel + timeout), surface it here as a
            // terminal Err so a TRUNCATED partition fails the query instead of looking like clean EOF.
            // (codex round-5 BLOCKER #1.)
            Poll::Ready(None) => {
                // Clone the message out before the mutable borrow of failure_emitted below.
                let recorded = self.failure.get().cloned();
                match recorded {
                    Some(msg) => {
                        self.failure_emitted = true;
                        Poll::Ready(Some(Err(DataFusionError::Execution(msg))))
                    }
                    None => Poll::Ready(None),
                }
            }
            other => other,
        }
    }
}

impl RecordBatchStream for PartitionStreamReceiver {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Creates a paired sender/receiver over a bounded mpsc (capacity
/// [`CHANNEL_CAPACITY`]).
///
/// Both halves share the provided [`SchemaRef`]. Dropping the sender closes the
/// channel — the receiver's `poll_next` then yields `Ready(None)` once any
/// buffered batches are drained, which DataFusion interprets as end-of-input.
pub fn channel(schema: SchemaRef) -> (PartitionStreamSender, PartitionStreamReceiver) {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let failure: Arc<OnceLock<String>> = Arc::new(OnceLock::new());
    let sender = PartitionStreamSender {
        tx,
        schema: Arc::clone(&schema),
        failure: Arc::clone(&failure),
    };
    let receiver = PartitionStreamReceiver {
        rx,
        schema,
        failure,
        failure_emitted: false,
    };
    (sender, receiver)
}

/// Wraps a [`PartitionStreamReceiver`] so it can be registered as a DataFusion
/// `StreamingTable` partition.
///
/// DataFusion's [`PartitionStream::execute`] contract may invoke `execute` more
/// than once across the life of a plan. The receiver can only be consumed once,
/// so the first `execute` takes it and subsequent calls return an empty stream
/// (zero batches, end-of-stream immediately) rather than panicking.
pub(crate) struct SingleReceiverPartition {
    schema: SchemaRef,
    receiver: Mutex<Option<PartitionStreamReceiver>>,
    replay: Arc<Mutex<ReplayState>>,
    replay_enabled: bool,
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug)]
enum ReplayState {
    Unopened,
    Active,
    Replayable(Option<RefCountedTempFile>),
    OneShotConsumed,
    Poisoned(String),
}

impl SingleReceiverPartition {
    pub(crate) fn new(receiver: PartitionStreamReceiver) -> Self {
        Self::with_replay(receiver, false)
    }

    /** Explicit replay contract for adaptive callers that may execute this input twice. */
    #[allow(dead_code)]
    pub(crate) fn new_replayable(receiver: PartitionStreamReceiver) -> Self {
        Self::with_replay(receiver, true)
    }

    fn with_replay(receiver: PartitionStreamReceiver, replay_enabled: bool) -> Self {
        Self {
            schema: Arc::clone(&receiver.schema),
            receiver: Mutex::new(Some(receiver)),
            replay: Arc::new(Mutex::new(ReplayState::Unopened)),
            replay_enabled,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for SingleReceiverPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleReceiverPartition")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for SingleReceiverPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut replay = self.replay.lock().expect("partition replay mutex poisoned");
        match &*replay {
            ReplayState::Replayable(file) => {
                let manager = SpillManager::new(
                    ctx.runtime_env(),
                    SpillMetrics::new(&self.metrics, 0),
                    Arc::clone(&self.schema),
                );
                return match file {
                    Some(file) => match manager.read_spill_as_stream(file.clone(), None) {
                        Ok(stream) => stream,
                        Err(error) => error_stream(Arc::clone(&self.schema), error),
                    },
                    None => empty_stream(Arc::clone(&self.schema)),
                };
            }
            ReplayState::Active => {
                return error_stream(
                    Arc::clone(&self.schema),
                    DataFusionError::Execution(
                        "shuffle partition replay requested while the first pass is active"
                            .to_string(),
                    ),
                );
            }
            ReplayState::Poisoned(reason) => {
                return error_stream(
                    Arc::clone(&self.schema),
                    DataFusionError::Execution(format!(
                        "shuffle partition is not replayable: {reason}"
                    )),
                );
            }
            ReplayState::OneShotConsumed => return empty_stream(Arc::clone(&self.schema)),
            ReplayState::Unopened => {}
        }

        let taken = self
            .receiver
            .lock()
            .expect("partition mutex poisoned")
            .take();
        let Some(receiver) = taken else {
            *replay = ReplayState::Poisoned("missing first-pass receiver".to_string());
            return error_stream(
                Arc::clone(&self.schema),
                DataFusionError::Internal(
                    "shuffle partition missing first-pass receiver".to_string(),
                ),
            );
        };

        if self.replay_enabled == false
            || ctx.runtime_env().disk_manager.tmp_files_enabled() == false
        {
            *replay = ReplayState::OneShotConsumed;
            return Box::pin(receiver);
        }

        let manager = SpillManager::new(
            ctx.runtime_env(),
            SpillMetrics::new(&self.metrics, 0),
            Arc::clone(&self.schema),
        );
        let mut writer = match manager.create_in_progress_file("replayable shuffle partition") {
            Ok(writer) => writer,
            Err(error) => {
                *replay = ReplayState::Poisoned(error.to_string());
                return error_stream(Arc::clone(&self.schema), error);
            }
        };

        let (tx, output) = mpsc::channel(CHANNEL_CAPACITY);
        let shared_state = Arc::clone(&self.replay);
        *replay = ReplayState::Active;
        drop(replay);

        tokio::spawn(async move {
            let mut input = Box::pin(receiver);
            while let Some(item) = input.next().await {
                match item {
                    Ok(batch) => {
                        if batch.num_rows() > 0 {
                            if let Err(error) = writer.append_batch(&batch) {
                                let reason = error.to_string();
                                let _ = tx.send(Err(error)).await;
                                *shared_state
                                    .lock()
                                    .expect("partition replay mutex poisoned") =
                                    ReplayState::Poisoned(reason);
                                return;
                            }
                        }
                        if tx.send(Ok(batch)).await.is_err() {
                            *shared_state
                                .lock()
                                .expect("partition replay mutex poisoned") = ReplayState::Poisoned(
                                "first-pass consumer stopped before exchange EOF".to_string(),
                            );
                            return;
                        }
                    }
                    Err(error) => {
                        let reason = error.to_string();
                        let _ = tx.send(Err(error)).await;
                        *shared_state
                            .lock()
                            .expect("partition replay mutex poisoned") =
                            ReplayState::Poisoned(reason);
                        return;
                    }
                }
            }

            match writer.finish() {
                Ok(file) => {
                    *shared_state
                        .lock()
                        .expect("partition replay mutex poisoned") = ReplayState::Replayable(file);
                }
                Err(error) => {
                    let reason = error.to_string();
                    let _ = tx.send(Err(error)).await;
                    *shared_state
                        .lock()
                        .expect("partition replay mutex poisoned") = ReplayState::Poisoned(reason);
                }
            }
        });

        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            ReceiverStream::new(output),
        ))
    }
}

fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::empty()))
}

fn error_stream(schema: SchemaRef, error: DataFusionError) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream::once(async move { Err(error) }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn test_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn channel_preserves_schema() {
        let schema = test_schema();
        let (sender, receiver) = channel(Arc::clone(&schema));
        assert_eq!(sender.schema(), &schema);
        assert_eq!(RecordBatchStream::schema(&receiver), schema);
    }

    #[tokio::test]
    async fn receiver_yields_sent_batches_then_eof() {
        let schema = test_schema();
        let (sender, mut receiver) = channel(Arc::clone(&schema));

        let producer_schema = Arc::clone(&schema);
        let producer = tokio::spawn(async move {
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[1, 2])))
                .await
                .unwrap();
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[3])))
                .await
                .unwrap();
            drop(sender);
        });

        let first = receiver.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        let second = receiver.next().await.unwrap().unwrap();
        assert_eq!(second.num_rows(), 1);
        assert!(receiver.next().await.is_none());
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn send_blocking_pushes_through_handle() {
        let schema = test_schema();
        let (sender, mut receiver) = channel(Arc::clone(&schema));
        let handle = Handle::current();

        let sender_schema = Arc::clone(&schema);
        let producer = std::thread::spawn(move || {
            let outcome = sender.send_blocking(Ok(test_batch(&sender_schema, &[7, 8, 9])), &handle);
            assert!(matches!(outcome, SendOutcome::Sent));
            drop(sender);
        });

        let batch = receiver.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert!(receiver.next().await.is_none());
        producer.join().unwrap();
    }

    #[test]
    fn send_blocking_reports_receiver_dropped() {
        let rt = tokio::runtime::Runtime::new().expect("runtime builds");
        let handle = rt.handle().clone();

        let schema = test_schema();
        let (sender, receiver) = channel(Arc::clone(&schema));
        drop(receiver);

        let outcome = std::thread::spawn(move || {
            sender.send_blocking(Ok(test_batch(&schema, &[1])), &handle)
        })
        .join()
        .unwrap();
        assert!(matches!(outcome, SendOutcome::ReceiverDropped));
    }

    #[tokio::test]
    async fn fail_surfaces_error_even_when_channel_full_then_closed() {
        // codex round-5 BLOCKER #1: a truncated partition must NEVER be consumed as a clean EOF, even
        // when the bounded channel is FULL. fail() records out-of-band (no channel send), so it works
        // regardless of channel fullness; the receiver drains queued Oks in FIFO order, then yields the
        // terminal Err on channel-close.
        let schema = test_schema();
        let (sender, mut receiver) = channel(Arc::clone(&schema));

        // Saturate the channel (capacity 4) — fail() must not depend on a free slot.
        for i in 0..CHANNEL_CAPACITY {
            sender
                .tx
                .try_send(Ok(test_batch(&schema, &[i as i64])))
                .expect("prefill within capacity");
        }

        sender.fail(DataFusionError::Execution("spill read failed".to_string()));
        drop(sender); // close the channel

        // Queued Ok batches drain first (FIFO).
        for _ in 0..CHANNEL_CAPACITY {
            let item = receiver.next().await.expect("a prefilled batch");
            assert!(item.is_ok(), "prefilled items are Ok batches");
        }
        // Then the stream yields the terminal Err exactly once (not None/clean-EOF).
        let terminal = receiver.next().await.expect("a terminal item, not EOF");
        let err = terminal.expect_err("truncated partition must surface as Err, never clean EOF");
        assert!(
            err.to_string().contains("spill read failed"),
            "error carries the failure reason: {err}"
        );
        // Emitted exactly once — the stream is done afterward.
        assert!(
            receiver.next().await.is_none(),
            "stream ends after the terminal error"
        );
    }

    #[tokio::test]
    async fn clean_close_without_failure_is_eof_not_error() {
        // Control: a normal drop (no try_fail) must still be a clean EOF — the failure path must not
        // make every close look like an error.
        let schema = test_schema();
        let (sender, mut receiver) = channel(Arc::clone(&schema));
        let producer_schema = Arc::clone(&schema);
        let producer = tokio::spawn(async move {
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[1])))
                .await
                .unwrap();
            drop(sender);
        });
        let batch = receiver.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(
            receiver.next().await.is_none(),
            "clean close is EOF, not Err"
        );
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn single_receiver_partition_replays_after_first_pass() {
        let schema = test_schema();
        let (sender, receiver) = channel(Arc::clone(&schema));
        let partition = SingleReceiverPartition::new_replayable(receiver);
        assert_eq!(partition.schema(), &schema);

        let producer_schema = Arc::clone(&schema);
        let producer = tokio::spawn(async move {
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[42])))
                .await
                .unwrap();
            drop(sender);
        });

        let ctx = Arc::new(TaskContext::default());
        let mut first = partition.execute(Arc::clone(&ctx));
        let batch = first.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(first.next().await.is_none());
        producer.await.unwrap();

        // The first pass is simultaneously persisted. A later pass reads the
        // completed spill file and yields the same partition again.
        let mut second = partition.execute(ctx);
        let replayed = second.next().await.unwrap().unwrap();
        assert_eq!(replayed.num_rows(), 1);
        assert_eq!(
            replayed
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
        assert!(second.next().await.is_none());
    }

    #[tokio::test]
    async fn normal_partition_does_not_tee_when_disk_is_enabled() {
        let schema = test_schema();
        let (sender, receiver) = channel(Arc::clone(&schema));
        let partition = SingleReceiverPartition::new(receiver);
        let producer_schema = Arc::clone(&schema);
        tokio::spawn(async move {
            sender
                .tx
                .send(Ok(test_batch(&producer_schema, &[7])))
                .await
                .unwrap();
        });

        let ctx = Arc::new(TaskContext::default());
        let mut first = partition.execute(Arc::clone(&ctx));
        assert_eq!(first.next().await.unwrap().unwrap().num_rows(), 1);
        assert!(first.next().await.is_none());

        // The default exchange contract is one-shot. Disk availability alone must not create a
        // replay file; a second execution is empty unless the caller explicitly opted in.
        let mut second = partition.execute(ctx);
        assert!(second.next().await.is_none());
    }
}
