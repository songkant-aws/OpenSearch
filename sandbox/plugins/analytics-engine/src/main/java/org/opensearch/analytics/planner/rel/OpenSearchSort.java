/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/**
 * OpenSearch custom Sort carrying viable backend list.
 *
 * @opensearch.internal
 */
public class OpenSearchSort extends Sort implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final boolean perPartition;

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        List<String> viableBackends
    ) {
        this(cluster, traitSet, input, collation, offset, fetch, viableBackends, false);
    }

    public OpenSearchSort(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation,
        RexNode offset,
        RexNode fetch,
        List<String> viableBackends,
        boolean perPartition
    ) {
        super(cluster, traitSet, input, collation, offset, fetch);
        this.viableBackends = viableBackends;
        this.perPartition = perPartition;
    }

    /** True when this Sort runs per-shard (shard-bucket oversampling). */
    public boolean isPerPartition() {
        return perPartition;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Sort doesn't change schema — pass through child's field storage. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new OpenSearchSort(getCluster(), traitSet, input, collation, offset, fetch, viableBackends, perPartition);
    }

    /**
     * Treat our Sort as a concrete physical operator, not a Calcite collation enforcer.
     *
     * <p>Calcite's default classifies a Sort with collation as an enforcer — Volcano then
     * registers it into a {@code required=true} subset that's never marked delivered. That
     * confuses the gather-rule path, which looks for delivered subsets when converting an
     * inner Sort's RelSet to SINGLETON. We don't use Calcite's collation-trait enforcement,
     * so mark the Sort delivered like any other operator.
     */
    @Override
    public boolean isEnforcer() {
        return false;
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null || requiredDistribution.getType() == RelDistribution.Type.ANY) {
            return null;
        }
        boolean global = perPartition == false && (!getCollation().getFieldCollations().isEmpty() || fetch != null || offset != null);
        OpenSearchDistribution outputDistribution = requiredDistribution;
        OpenSearchDistribution inputDistribution = requiredDistribution;
        if (global) {
            if (requiredDistribution.getType() != RelDistribution.Type.SINGLETON) {
                return null;
            }
            OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
            outputDistribution = traitDef.coordSingleton();
            inputDistribution = outputDistribution;
        }
        return Pair.of(getTraitSet().replace(outputDistribution), List.of(getInput().getTraitSet().replace(inputDistribution)));
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childId != 0) {
            return null;
        }
        boolean global = perPartition == false && (!getCollation().getFieldCollations().isEmpty() || fetch != null || offset != null);
        OpenSearchDistribution childDistribution = OpenSearchRelNode.distributionOf(childTraits);
        if (childDistribution == null || childDistribution.getType() == RelDistribution.Type.ANY) {
            return null;
        }
        if (global && childDistribution.getType() != RelDistribution.Type.SINGLETON) {
            return null;
        }
        return Pair.of(getTraitSet().replace(childDistribution), List.of(childTraits));
    }

    /**
     * A collated Sort needs globally-ordered input. Our {@link OpenSearchExchangeReducer}
     * is a concat gather (not a merge exchange), so per-partition sort + ER produces
     * partition-locally ordered rows concatenated in arrival order — wrong. Returning
     * top-down trait contract admits only the coordinator implementation (ER below the
     * Sort, so the Sort sees fully-gathered input).
     *
     * <p>A Sort with no collation AND no fetch/offset is a no-op — skip the gate.
     * A pure LIMIT (fetch != null, no collation) still needs gathering so it applies globally.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (getCollation().getFieldCollations().isEmpty() && fetch == null && offset == null) {
            return planner.getCostFactory().makeTinyCost();
        }
        OpenSearchDistribution distribution = OpenSearchRelNode.distributionOf(getTraitSet());
        if (distribution == null || distribution.getType() != RelDistribution.Type.SINGLETON) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchSort(getCluster(), getTraitSet(), children.getFirst(), getCollation(), offset, fetch, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalSort.create(strippedChildren.getFirst(), getCollation(), offset, fetch);
    }
}
