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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * OpenSearch custom Union carrying viable backend list.
 *
 * <p>Per-column output storage is the intersection of inputs' storage at the same
 * positional index — when all inputs report identical storage we keep it; any
 * divergence (e.g. one branch has a derived literal column, another has a real
 * field reference) collapses to a derived column. Downstream rules that push down
 * to physical storage (Filter, Aggregate) therefore treat post-Union columns as
 * derived unless every branch agrees.
 *
 * @opensearch.internal
 */
public class OpenSearchUnion extends Union implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchUnion(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all, List<String> viableBackends) {
        super(cluster, traitSet, List.of(), inputs, all);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<List<FieldStorageInfo>> perInputStorage = new ArrayList<>(getInputs().size());
        for (RelNode input : getInputs()) {
            RelNode unwrapped = RelNodeUtils.unwrapHep(input);
            if (!(unwrapped instanceof OpenSearchRelNode openSearchInput)) {
                throw new IllegalStateException("Union input is not OpenSearchRelNode: " + unwrapped.getClass().getSimpleName());
            }
            perInputStorage.add(openSearchInput.getOutputFieldStorage());
        }

        int columnCount = getRowType().getFieldCount();
        List<FieldStorageInfo> result = new ArrayList<>(columnCount);
        for (int col = 0; col < columnCount; col++) {
            String fieldName = getRowType().getFieldList().get(col).getName();
            SqlTypeName sqlType = getRowType().getFieldList().get(col).getType().getSqlTypeName();

            FieldStorageInfo first = perInputStorage.getFirst().size() > col ? perInputStorage.getFirst().get(col) : null;
            boolean allMatch = first != null && !first.isDerived();
            if (allMatch) {
                for (int i = 1; i < perInputStorage.size(); i++) {
                    List<FieldStorageInfo> branch = perInputStorage.get(i);
                    if (branch.size() <= col) {
                        allMatch = false;
                        break;
                    }
                    FieldStorageInfo other = branch.get(col);
                    if (other.isDerived()
                        || other.getFieldType() != first.getFieldType()
                        || !other.getDocValueFormats().equals(first.getDocValueFormats())
                        || !other.getIndexFormats().equals(first.getIndexFormats())) {
                        allMatch = false;
                        break;
                    }
                }
            }

            result.add(allMatch ? first : FieldStorageInfo.derivedColumn(fieldName, sqlType));
        }
        return result;
    }

    @Override
    public Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new OpenSearchUnion(getCluster(), traitSet, inputs, all, viableBackends);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null || requiredDistribution.getType() != RelDistribution.Type.SINGLETON) {
            return null;
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
        OpenSearchDistribution singleton = traitDef.coordSingleton();
        List<RelTraitSet> inputTraits = getInputs().stream().map(input -> input.getTraitSet().replace(singleton)).toList();
        return Pair.of(getTraitSet().replace(singleton), inputTraits);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childId < 0 || childId >= getInputs().size()) {
            return null;
        }
        OpenSearchDistribution childDistribution = OpenSearchRelNode.distributionOf(childTraits);
        if (childDistribution == null
            || childDistribution.getType() != RelDistribution.Type.SINGLETON
            || (childDistribution.getLocality() != OpenSearchDistribution.Locality.COORDINATOR
                && childDistribution.getLocality() != OpenSearchDistribution.Locality.SHARD)) {
            return null;
        }
        if (childDistribution.getLocality() == OpenSearchDistribution.Locality.SHARD
            && (Integer.valueOf(1).equals(childDistribution.getShardCount()) == false || childDistribution.getTableId() == null)) {
            return null;
        }
        if (childDistribution.getLocality() == OpenSearchDistribution.Locality.SHARD) {
            for (int i = 0; i < getInputs().size(); i++) {
                if (i != childId
                    && childDistribution.getTableId().equals(OpenSearchRelNode.singleShardTableId(getInputs().get(i))) == false) {
                    return null;
                }
            }
        }
        List<RelTraitSet> inputTraits = new ArrayList<>(
            getInputs().stream().map(input -> input.getTraitSet().replace(childDistribution)).toList()
        );
        inputTraits.set(childId, childTraits);
        return Pair.of(getTraitSet().replace(childDistribution), inputTraits);
    }

    /** Union execution cost after trait propagation establishes its input locality. */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        OpenSearchDistribution selfDist = distributionOf(this);
        if (selfDist == null || selfDist.getType() != RelDistribution.Type.SINGLETON) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        return planner.getCostFactory().makeTinyCost();
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchUnion(getCluster(), getTraitSet(), children, all, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalUnion.create(strippedChildren, all);
    }
}
