/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage.shard;

import org.opensearch.analytics.exec.stage.StageTask;
import org.opensearch.analytics.exec.stage.StageTaskId;
import org.opensearch.analytics.planner.dag.ExecutionTarget;

/**
 * {@link StageTask} variant dispatched to a remote data node via Arrow Flight.
 * The {@link ExecutionTarget} carries the routing key and the per-partition
 * fragment payload the data node needs to set up its input source.
 *
 * @opensearch.internal
 */
public final class ShardStageTask extends StageTask {

    private final ExecutionTarget target;
    private final int attempt;

    public ShardStageTask(StageTaskId id, ExecutionTarget target) {
        this(id, target, 0);
    }

    public ShardStageTask(StageTaskId id, ExecutionTarget target, int attempt) {
        super(id);
        this.target = target;
        this.attempt = attempt;
    }

    public ExecutionTarget target() {
        return target;
    }

    public int attempt() {
        return attempt;
    }

    public ShardStageTask nextAttempt(ExecutionTarget nextTarget) {
        return new ShardStageTask(id(), nextTarget, attempt + 1);
    }
}
