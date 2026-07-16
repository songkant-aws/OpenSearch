/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.worker;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.WorkerFragmentRequest;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.WorkerExecutionTarget;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerFragmentStageExecutionTests extends OpenSearchTestCase {

    public void testWorkerRequestPreservesProfileFlagOnWire() throws Exception {
        WorkerFragmentRequest request = new WorkerFragmentRequest("query", 2, 1, List.of(), true);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                WorkerFragmentRequest restored = new WorkerFragmentRequest(in);
                assertTrue(restored.isProfile());
                assertEquals("query", restored.getQueryId());
                assertEquals(2, restored.getStageId());
                assertEquals(1, restored.getPartitionIndex());
                assertEquals(0, restored.getAttempt());
            }
        }
    }

    public void testWorkerRequestPreservesRetryAttemptOnWire() throws Exception {
        WorkerFragmentRequest request = new WorkerFragmentRequest("query", 2, 1, List.of(), true, 3);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                assertEquals(3, new WorkerFragmentRequest(in).getAttempt());
            }
        }
    }

    public void testWorkerTaskCreatesNextAttemptWithSameTarget() {
        WorkerExecutionTarget target = new WorkerExecutionTarget(mock(DiscoveryNode.class), 0);
        WorkerStageTask first = new WorkerStageTask(new StageTaskId(2, 0), target);
        WorkerStageTask retry = first.nextAttempt();
        assertSame(target, retry.target());
        assertEquals(1, retry.attempt());
        assertEquals(first.id(), retry.id());
    }

    public void testTrailingMetricsAreAttachedToWorkerTask() {
        Stage stage = mock(Stage.class);
        when(stage.getStageId()).thenReturn(2);
        QueryContext config = mock(QueryContext.class);
        when(config.queryId()).thenReturn("query");
        when(config.operationListeners()).thenReturn(List.of());

        Function<WorkerExecutionTarget, WorkerFragmentRequest> requestBuilder = target -> null;
        WorkerFragmentStageExecution execution = new WorkerFragmentStageExecution(
            stage,
            config,
            mock(ExchangeSink.class),
            mock(ClusterService.class),
            requestBuilder,
            mock(AnalyticsSearchTransportService.class)
        );
        WorkerStageTask task = new WorkerStageTask(new StageTaskId(2, 0), new WorkerExecutionTarget(mock(DiscoveryNode.class), 0));
        StreamingResponseListener<FragmentExecutionArrowResponse> listener = execution.responseListenerFor(
            task,
            ActionListener.wrap(ignored -> {}, failure -> fail(failure.getMessage()))
        );
        byte[] metrics = "{\"spill_trigger_count\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        listener.onStreamComplete(metrics);

        assertArrayEquals(metrics, task.dataNodeMetrics());
    }
}
