/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Join rel carrying viable backends. Top-down traits expose coordinator, co-located
 * single-shard, hash-shuffle, and broadcast implementations; cost ranks only concrete
 * legal alternatives. {@code right} is always the build side in Substrait.
 *
 * <p>Implements {@link DistributionAware}: under the post-CBO distribution-enforcement pass (Option B,
 * {@code MPP-GENERAL-SCHEDULING-DESIGN.md}), an INNER/LEFT/RIGHT/FULL/SEMI/ANTI equi-join can co-partition
 * on its equi keys — it requires {@code WORKER+HASH(leftKeys,N)} on the left input and
 * {@code WORKER+HASH(rightKeys,N)} on the right, and outputs {@code WORKER+HASH(leftKeys,N)}. That lets a
 * parent join/aggregate keyed on the same column consume the output with no further exchange, so the
 * multi-tier cascade emerges for any chain depth. A pure-theta join (no equi key) imposes no requirement
 * (stays coordinator-gathered).
 *
 * @opensearch.internal
 */
public class OpenSearchJoin extends Join implements OpenSearchRelNode, DistributionAware {

    /** Physical implementation selected for a worker hash-shuffle join. */
    public enum JoinAlgorithm {
        /** Compatibility fallback for joins distributed only by the post-CBO enforcement pass. */
        AUTO,
        HASH,
        SORT_MERGE
    }

    private final List<String> viableBackends;
    private final JoinAlgorithm joinAlgorithm;
    private final double estimatedBuildRows;
    private final double estimatedBuildBytesPerWorker;
    private final long hashJoinMaxBuildRows;
    private final long hashJoinMaxBytesPerWorker;

    public OpenSearchJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<String> viableBackends
    ) {
        this(cluster, traitSet, left, right, condition, joinType, viableBackends, JoinAlgorithm.AUTO, Double.NaN, Double.NaN, -1L, -1L);
    }

    public OpenSearchJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<String> viableBackends,
        JoinAlgorithm joinAlgorithm,
        double estimatedBuildRows,
        double estimatedBuildBytesPerWorker,
        long hashJoinMaxBuildRows,
        long hashJoinMaxBytesPerWorker
    ) {
        super(cluster, traitSet, List.of(), left, right, condition, Set.of(), joinType);
        this.viableBackends = viableBackends;
        this.joinAlgorithm = joinAlgorithm;
        this.estimatedBuildRows = estimatedBuildRows;
        this.estimatedBuildBytesPerWorker = estimatedBuildBytesPerWorker;
        this.hashJoinMaxBuildRows = hashJoinMaxBuildRows;
        this.hashJoinMaxBytesPerWorker = hashJoinMaxBytesPerWorker;
    }

    public OpenSearchJoin withJoinAlgorithm(
        JoinAlgorithm algorithm,
        double buildRows,
        double buildBytesPerWorker,
        long maxBuildRows,
        long maxBytesPerWorker
    ) {
        return new OpenSearchJoin(
            getCluster(),
            getTraitSet(),
            getLeft(),
            getRight(),
            getCondition(),
            getJoinType(),
            viableBackends,
            algorithm,
            buildRows,
            buildBytesPerWorker,
            maxBuildRows,
            maxBytesPerWorker
        );
    }

    public JoinAlgorithm getJoinAlgorithm() {
        return joinAlgorithm;
    }

    public double getEstimatedBuildRows() {
        return estimatedBuildRows;
    }

    public double getEstimatedBuildBytesPerWorker() {
        return estimatedBuildBytesPerWorker;
    }

    public long getHashJoinMaxBuildRows() {
        return hashJoinMaxBuildRows;
    }

    public long getHashJoinMaxBytesPerWorker() {
        return hashJoinMaxBytesPerWorker;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Output field storage is the concatenation of left and right input storage —
     * matches Calcite's join row type ordering (left fields first, then right).
     *
     * <p>SEMI / ANTI joins project only the left side — Calcite's {@code Join#getRowType}
     * exposes left fields only in those cases, so our storage metadata must mirror that or
     * downstream walkers (e.g. {@code OpenSearchJoinRule.collectStorageFormats} on a wrapping
     * outer join) index past the row and pick up phantom formats from the right.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<FieldStorageInfo> result = new ArrayList<>();
        appendChildStorage(getLeft(), result);
        if (getJoinType().projectsRight()) {
            appendChildStorage(getRight(), result);
        }
        return result;
    }

    private static void appendChildStorage(RelNode child, List<FieldStorageInfo> out) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(child);
        if (unwrapped instanceof OpenSearchRelNode os) {
            out.addAll(os.getOutputFieldStorage());
        }
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new OpenSearchJoin(
            getCluster(),
            traitSet,
            left,
            right,
            conditionExpr,
            joinType,
            viableBackends,
            joinAlgorithm,
            estimatedBuildRows,
            estimatedBuildBytesPerWorker,
            hashJoinMaxBuildRows,
            hashJoinMaxBytesPerWorker
        );
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null || requiredDistribution.getType() != org.apache.calcite.rel.RelDistribution.Type.SINGLETON) {
            return null;
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
        OpenSearchDistribution singleton = traitDef.coordSingleton();
        return Pair.of(
            getTraitSet().replace(singleton),
            List.of(getLeft().getTraitSet().replace(singleton), getRight().getTraitSet().replace(singleton))
        );
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childId != 0 && childId != 1) {
            return null;
        }
        OpenSearchDistribution childDistribution = OpenSearchRelNode.distributionOf(childTraits);
        if (childDistribution == null || childDistribution.getType() == org.apache.calcite.rel.RelDistribution.Type.ANY) {
            return null;
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) childDistribution.getTraitDef();

        if (childDistribution.getType() == org.apache.calcite.rel.RelDistribution.Type.SINGLETON
            && childDistribution.getLocality() == OpenSearchDistribution.Locality.SHARD
            && Integer.valueOf(1).equals(childDistribution.getShardCount())
            && childDistribution.getTableId() != null
            && childDistribution.getTableId().equals(OpenSearchRelNode.singleShardTableId(childId == 0 ? getRight() : getLeft()))) {
            List<RelTraitSet> inputs = new ArrayList<>(
                List.of(getLeft().getTraitSet().replace(childDistribution), getRight().getTraitSet().replace(childDistribution))
            );
            inputs.set(childId, childTraits);
            return Pair.of(getTraitSet().replace(childDistribution), inputs);
        }

        if (childDistribution.getType() == org.apache.calcite.rel.RelDistribution.Type.SINGLETON
            && childDistribution.getLocality() == OpenSearchDistribution.Locality.COORDINATOR) {
            OpenSearchDistribution singleton = traitDef.coordSingleton();
            List<RelTraitSet> inputs = new ArrayList<>(
                List.of(getLeft().getTraitSet().replace(singleton), getRight().getTraitSet().replace(singleton))
            );
            inputs.set(childId, childTraits);
            return Pair.of(getTraitSet().replace(singleton), inputs);
        }

        JoinInfo info = analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            return null;
        }
        if (childDistribution.getType() == org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED
            && childDistribution.getLocality() == OpenSearchDistribution.Locality.WORKER
            && childDistribution.getPartitionCount() != null) {
            List<Integer> expectedKeys = childId == 0 ? info.leftKeys : info.rightKeys;
            if (!childDistribution.getKeys().equals(expectedKeys)) {
                return null;
            }
            int partitionCount = childDistribution.getPartitionCount();
            OpenSearchDistribution leftHash = traitDef.hash(info.leftKeys, partitionCount);
            OpenSearchDistribution rightHash = traitDef.hash(info.rightKeys, partitionCount);
            List<RelTraitSet> inputs = new ArrayList<>(
                List.of(getLeft().getTraitSet().replace(leftHash), getRight().getTraitSet().replace(rightHash))
            );
            inputs.set(childId, childTraits);
            return Pair.of(getTraitSet().replace(leftHash), inputs);
        }

        return deriveBroadcastTraits(childTraits, childId, childDistribution);
    }

    private Pair<RelTraitSet, List<RelTraitSet>> deriveBroadcastTraits(
        RelTraitSet childTraits,
        int childId,
        OpenSearchDistribution childDistribution
    ) {
        RelNode other = childId == 0 ? getRight() : getLeft();
        OpenSearchDistribution otherDistribution = distributionOf(other);
        if (otherDistribution == null) {
            return null;
        }
        boolean childIsBuild = childDistribution.getType() == org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED
            && childDistribution.getLocality() == OpenSearchDistribution.Locality.REPLICATED;
        boolean otherIsBuild = otherDistribution.getType() == org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED
            && otherDistribution.getLocality() == OpenSearchDistribution.Locality.REPLICATED;
        OpenSearchDistribution probeDistribution = childIsBuild ? otherDistribution : childDistribution;
        boolean probeIsShard = probeDistribution.getType() == org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED
            && probeDistribution.getLocality() == OpenSearchDistribution.Locality.SHARD;
        if (probeIsShard == false || childIsBuild == otherIsBuild) {
            return null;
        }
        int buildId = childIsBuild ? childId : 1 - childId;
        boolean buildAllowed = buildId == 0
            ? getJoinType() == JoinRelType.INNER || getJoinType() == JoinRelType.RIGHT
            : getJoinType() == JoinRelType.INNER
                || getJoinType() == JoinRelType.LEFT
                || getJoinType() == JoinRelType.SEMI
                || getJoinType() == JoinRelType.ANTI;
        if (buildAllowed == false) {
            return null;
        }
        List<RelTraitSet> inputs = new ArrayList<>(List.of(getLeft().getTraitSet(), getRight().getTraitSet()));
        inputs.set(childId, childTraits);
        return Pair.of(getTraitSet().replace(probeDistribution), inputs);
    }

    @Override
    public DeriveMode getDeriveMode() {
        return DeriveMode.BOTH;
    }

    /** Join execution cost after trait propagation has established a legal physical shape. */
    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(
        org.apache.calcite.plan.RelOptPlanner planner,
        org.apache.calcite.rel.metadata.RelMetadataQuery mq
    ) {
        OpenSearchDistribution selfDist = distributionOf(this);
        if (selfDist == null || selfDist.getType() == org.apache.calcite.rel.RelDistribution.Type.ANY) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        org.apache.calcite.rel.RelDistribution.Type selfType = selfDist.getType();
        OpenSearchDistribution.Locality selfLocality = selfDist.getLocality();
        boolean isSingleton = selfType == org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
        boolean isHashWorker = selfType == org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED
            && selfLocality == OpenSearchDistribution.Locality.WORKER;
        boolean isBroadcastShape = selfType == org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED
            && selfLocality == OpenSearchDistribution.Locality.SHARD;
        if (!isSingleton && !isHashWorker && !isBroadcastShape) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        // passThroughTraits/deriveTraits and the implementation rules establish physical
        // legality. Cost is now only the join work itself. It must not be tiny: in top-down
        // mode the root gather above
        // a distributed join participates in the same global comparison as the input gathers
        // of a coordinator join. Without a strategy-sensitive execution cost, those common
        // large-row transport terms cancel and coordinator execution wins merely because it
        // gathers the small side once instead of broadcasting it, ignoring the parallel join
        // work that broadcast/hash-shuffle actually buy.
        double inputRows = mq.getRowCount(getLeft()) + mq.getRowCount(getRight());
        int parallelism = 1;
        if (isHashWorker && selfDist.getPartitionCount() != null) {
            parallelism = Math.max(1, selfDist.getPartitionCount());
        } else if (isBroadcastShape) {
            // The replicated input carries the number of probe-side workers in the same
            // partitionCount slot used by the broadcast exchange's cost model.
            for (RelNode input : getInputs()) {
                OpenSearchDistribution inputDist = distributionOf(input);
                if (inputDist != null
                    && inputDist.getType() == org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED
                    && inputDist.getPartitionCount() != null) {
                    parallelism = Math.max(1, inputDist.getPartitionCount());
                    break;
                }
            }
        }
        double executionCost = inputRows / parallelism;
        if (isHashWorker) {
            if (joinAlgorithm == JoinAlgorithm.AUTO) {
                // A distribution trait says where the join runs, not how it runs. Top-down
                // deriveTraits can produce WORKER+HASH on the unresolved marker join; do not let
                // that cheaper marker beat the explicit HJ/SMJ implementations emitted by the
                // split rule. AUTO remains valid for joins distributed after CBO, where cost is no
                // longer consulted and ShuffleEnrichment supplies the compatibility fallback.
                return planner.getCostFactory().makeInfiniteCost();
            } else if (joinAlgorithm == JoinAlgorithm.HASH) {
                boolean rowLimitExceeded = hashJoinMaxBuildRows >= 0 && estimatedBuildRows >= hashJoinMaxBuildRows;
                boolean byteLimitExceeded = hashJoinMaxBytesPerWorker > 0 && estimatedBuildBytesPerWorker > hashJoinMaxBytesPerWorker;
                if (rowLimitExceeded || byteLimitExceeded) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
                // HJ is the baseline when memory-safe. Keep a small hash-table construction
                // premium in the same normalized unit as the existing planner cost model.
                executionCost *= 1.02d;
            } else if (joinAlgorithm == JoinAlgorithm.SORT_MERGE) {
                // EnforceSorting materializes the actual sorts. The current OpenSearch cost model
                // is deliberately row-normalized (Volcano compares only its first dimension), so
                // represent sorting/spill as a bounded premium: high enough that safe HJ wins, but
                // not so high that a memory-safe distributed SMJ loses to gathering both sides.
                executionCost *= 1.15d;
            }
        }
        return planner.getCostFactory().makeCost(executionCost, executionCost, 0);
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            org.apache.calcite.plan.RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    // ---- DistributionAware (Option B post-CBO enforcement pass) ----

    /**
     * An equi-join co-partitions on its equi keys: input 0 (left) must deliver
     * {@code WORKER+HASH(leftKeys, N)}, input 1 (right) {@code WORKER+HASH(rightKeys, N)}. A pure-theta
     * join (empty {@code leftKeys}) returns {@code null} — no key to hash-partition on, so it stays
     * coordinator-gathered. Co-partitioning is sound for all of INNER/LEFT/RIGHT/FULL/SEMI/ANTI: a
     * hash-partitioned outer/semi/anti join's null-fill / existence test is partition-local because rows
     * with the same key land in the same partition (standard Spark/Presto). The per-row null semantics
     * live in the worker join operator, not the distribution.
     */
    @Override
    public OpenSearchDistribution requiredInputDistribution(int inputIndex, int partitionCount, OpenSearchDistributionTraitDef traitDef) {
        JoinInfo info = analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            return null;
        }
        if (inputIndex == 0) {
            return traitDef.hash(info.leftKeys, partitionCount);
        }
        if (inputIndex == 1) {
            return traitDef.hash(info.rightKeys, partitionCount);
        }
        return null;
    }

    /**
     * When the left input is hash-partitioned on this join's left equi keys, the join output is
     * {@code WORKER+HASH(leftKeys, N)} — left key columns keep their output positions (left fields come
     * first in the join row type), so a parent keyed on the same column consumes it without a re-shuffle.
     * Anchored on the LEFT side only (the engine convention used by {@code OpenSearchHashJoinSplitRule} and
     * the join's physical trait). Returns {@code null} (output not co-partitionable) when the left input is not
     * hash-partitioned on exactly the left equi keys, or for a pure-theta join.
     */
    @Override
    public OpenSearchDistribution deriveOutputDistribution(
        List<OpenSearchDistribution> childDistributions,
        OpenSearchDistributionTraitDef traitDef
    ) {
        if (childDistributions.size() != 2) {
            return null;
        }
        OpenSearchDistribution leftDist = childDistributions.get(0);
        if (leftDist == null || leftDist.getType() != org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED) {
            return null;
        }
        JoinInfo info = analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            return null;
        }
        // Left input must be hash-partitioned on exactly this join's left equi keys (order-sensitive)
        // for the output-is-left-keys derivation to be sound.
        if (!leftDist.getKeys().equals(info.leftKeys)) {
            return null;
        }
        Integer n = leftDist.getPartitionCount();
        if (n == null) {
            return null;
        }
        return traitDef.hash(info.leftKeys, n);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends)
            .itemIf("joinAlgorithm", joinAlgorithm, joinAlgorithm != JoinAlgorithm.AUTO)
            .itemIf("estimatedBuildRows", estimatedBuildRows, Double.isFinite(estimatedBuildRows))
            .itemIf("estimatedBuildBytesPerWorker", estimatedBuildBytesPerWorker, Double.isFinite(estimatedBuildBytesPerWorker));
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchJoin(
            getCluster(),
            getTraitSet(),
            children.get(0),
            children.get(1),
            getCondition(),
            getJoinType(),
            List.of(backend),
            joinAlgorithm,
            estimatedBuildRows,
            estimatedBuildBytesPerWorker,
            hashJoinMaxBuildRows,
            hashJoinMaxBytesPerWorker
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalJoin.create(
            strippedChildren.get(0),
            strippedChildren.get(1),
            List.of(),
            getCondition(),
            Set.<CorrelationId>of(),
            getJoinType()
        );
    }
}
