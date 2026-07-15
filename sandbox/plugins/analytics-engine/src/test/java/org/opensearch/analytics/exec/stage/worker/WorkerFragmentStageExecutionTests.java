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
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerFragmentStageExecutionTests extends OpenSearchTestCase {

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
