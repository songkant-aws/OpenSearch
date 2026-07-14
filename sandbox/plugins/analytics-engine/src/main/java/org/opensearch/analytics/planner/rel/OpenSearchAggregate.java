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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * OpenSearch custom Aggregate carrying viable backend list and per-call annotations.
 *
 * <p>Per-call annotations are kept in a side-map keyed by call index — NOT in
 * {@code AggregateCall#rexList}. Keeping them out of rexList avoids contaminating
 * Calcite's {@code AggCallBinding.preOperands}, which would otherwise corrupt
 * inferReturnType for functions that read {@code getOperandType(0)} (PPL's
 * {@code ARG0_ARRAY} for {@code take} / {@code list} / {@code values}).
 *
 * @opensearch.internal
 */
public class OpenSearchAggregate extends Aggregate implements OpenSearchRelNode, DistributionAware {

    private final List<String> viableBackends;
    private final AggregateMode mode;
    /**
     * Per-call annotations keyed by call index in {@link #getAggCallList()}. May be empty when
     * the aggregate has no annotations yet (pre-marking) or when copied from a Calcite-internal
     * rule that doesn't preserve them. Order is stable for {@link #getAnnotations()} /
     * {@link #copyResolved}.
     */
    private final Map<Integer, AggregateCallAnnotation> callAnnotations;
    /**
     * FINAL-side carrier for literal aggregate-args (e.g. TAKE's N) captured by the
     * split rule from the original SINGLE aggregate's child Project. Empty otherwise.
     * Used by {@code DistributedAggregateRewriter} to re-create the literals as
     * constant columns below FINAL, since the StageInputScan only carries the state.
     */
    private final Map<Integer, List<RexLiteral>> finalExtraLiteralArgs;
    /** Per-call {@link IntermediateField} classification, parallel to {@link #getAggCallList()}; null entry = no SPI decomposition; empty for SINGLE/PARTIAL. */
    private final List<IntermediateField> perCallIntermediateField;

    public OpenSearchAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        AggregateMode mode,
        List<String> viableBackends,
        Map<Integer, AggregateCallAnnotation> callAnnotations
    ) {
        this(cluster, traitSet, input, groupSet, groupSets, aggCalls, mode, viableBackends, callAnnotations, Map.of(), List.of());
    }

    public OpenSearchAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        AggregateMode mode,
        List<String> viableBackends,
        Map<Integer, AggregateCallAnnotation> callAnnotations,
        Map<Integer, List<RexLiteral>> finalExtraLiteralArgs
    ) {
        this(
            cluster,
            traitSet,
            input,
            groupSet,
            groupSets,
            aggCalls,
            mode,
            viableBackends,
            callAnnotations,
            finalExtraLiteralArgs,
            List.of()
        );
    }

    public OpenSearchAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        AggregateMode mode,
        List<String> viableBackends,
        Map<Integer, AggregateCallAnnotation> callAnnotations,
        Map<Integer, List<RexLiteral>> finalExtraLiteralArgs,
        List<IntermediateField> perCallIntermediateField
    ) {
        super(
            cluster,
            traitSet,
            List.of(),
            input,
            frontGroupSetForFinal(groupSet, mode),
            frontGroupSetsForFinal(groupSet, groupSets, mode),
            aggCalls
        );
        this.mode = mode;
        this.viableBackends = viableBackends;
        this.callAnnotations = Map.copyOf(callAnnotations);
        this.finalExtraLiteralArgs = Map.copyOf(finalExtraLiteralArgs);
        // Collections.unmodifiableList — List.copyOf would NPE on the null pass-through entries.
        this.perCallIntermediateField = Collections.unmodifiableList(new ArrayList<>(perCallIntermediateField));
    }

    /**
     * FINAL reads PARTIAL's output, where Calcite has fronted the group keys to {@code 0..n-1}; group
     * on the prefix range so a non-prefix key (e.g. {@code avg(x) by span(y,5)} → {@code {1}}) isn't
     * read as an agg-state column. No-op for SINGLE/PARTIAL (raw input) and already-fronted sets.
     */
    private static ImmutableBitSet frontGroupSetForFinal(ImmutableBitSet groupSet, AggregateMode mode) {
        return mode == AggregateMode.FINAL ? ImmutableBitSet.range(groupSet.cardinality()) : groupSet;
    }

    /**
     * Keeps {@code groupSets} consistent with the fronted {@code groupSet}. PPL only emits simple
     * (single-set) aggregates, so this collapses to one set; revisit if GROUPING SETS is ever added.
     */
    private static List<ImmutableBitSet> frontGroupSetsForFinal(
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        AggregateMode mode
    ) {
        if (mode != AggregateMode.FINAL || groupSets == null || groupSets.isEmpty()) {
            return groupSets;
        }
        return List.of(ImmutableBitSet.range(groupSet.cardinality()));
    }

    /** Builds a FINAL aggregate post-rewrite; clears both stashes so a later {@code copy()} can't replay them. */
    public static OpenSearchAggregate finalAfterRewrite(OpenSearchAggregate prior, RelNode newInput, List<AggregateCall> rebuiltCalls) {
        return new OpenSearchAggregate(
            prior.getCluster(),
            prior.getTraitSet(),
            newInput,
            prior.getGroupSet(),
            prior.getGroupSets(),
            rebuiltCalls,
            AggregateMode.FINAL,
            prior.viableBackends,
            prior.callAnnotations,
            Map.of(),
            List.of()
        );
    }

    public AggregateMode getMode() {
        return mode;
    }

    /** Returns the per-call annotation map (keyed by call index). */
    public Map<Integer, AggregateCallAnnotation> getCallAnnotations() {
        return callAnnotations;
    }

    public Map<Integer, List<RexLiteral>> getFinalExtraLiteralArgs() {
        return finalExtraLiteralArgs;
    }

    /** See {@link #perCallIntermediateField}. */
    public List<IntermediateField> getIntermediateFields() {
        return perCallIntermediateField;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Aggregate output: group-by fields first (inherited from input), then agg results (derived).
     * Group-by fields inherit storage info from the input. Agg results are derived columns.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        List<FieldStorageInfo> inputStorage = (input instanceof OpenSearchRelNode openSearchInput)
            ? openSearchInput.getOutputFieldStorage()
            : List.of();

        List<FieldStorageInfo> outputStorage = new ArrayList<>();

        // Group-by fields: inherit from input
        for (int groupIdx : getGroupSet()) {
            if (groupIdx < inputStorage.size()) {
                outputStorage.add(inputStorage.get(groupIdx));
            }
        }

        // Agg results: derived columns whose physical-deps are the union of arg refs' deps
        // (preserving first-seen order across argList, then rexList).
        for (AggregateCall aggCall : getAggCallList()) {
            LinkedHashSet<String> deps = new LinkedHashSet<>();
            for (int argIdx : aggCall.getArgList()) {
                if (argIdx >= inputStorage.size()) {
                    throw new IllegalStateException(
                        "AggregateCall arg["
                            + argIdx
                            + "] has no matching FieldStorageInfo entry "
                            + "(input only declares "
                            + inputStorage.size()
                            + " columns)"
                    );
                }
                FieldStorageInfo src = inputStorage.get(argIdx);
                if (src.isDerived()) {
                    deps.addAll(src.getDependsOnPhysicalCols());
                } else {
                    deps.add(src.getFieldName());
                }
            }
            for (RexNode rex : aggCall.rexList) {
                deps.addAll(RelNodeUtils.resolvePhysicalDeps(rex, inputStorage));
            }
            outputStorage.add(FieldStorageInfo.derivedColumn(aggCall.getName(), aggCall.getType().getSqlTypeName(), deps));
        }

        return outputStorage;
    }

    @Override
    public Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        return new OpenSearchAggregate(
            getCluster(),
            traitSet,
            input,
            groupSet,
            groupSets,
            aggCalls,
            mode,
            viableBackends,
            callAnnotations,
            finalExtraLiteralArgs,
            perCallIntermediateField
        );
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null || requiredDistribution.getType() == RelDistribution.Type.ANY) {
            return null;
        }
        // SINGLE needs a structural SINGLE -> PARTIAL/FINAL implementation decision. A
        // trait-only copy would make gather-after-SINGLE look legal even though partitioned
        // partial results still need a merge, so leave it to AggregateSplitRule.
        if (mode == AggregateMode.SINGLE) {
            return null;
        }
        if (mode == AggregateMode.PARTIAL && requiredDistribution.getType() == RelDistribution.Type.SINGLETON) {
            return null;
        }
        OpenSearchDistribution inputDistribution = requiredDistribution;
        if (requiredDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            List<Integer> inputKeys = aggregateOutputKeysToInput(requiredDistribution.getKeys());
            if (inputKeys == null) {
                return null;
            }
            OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
            inputDistribution = requiredDistribution.getPartitionCount() == null
                ? traitDef.hashAny(inputKeys)
                : traitDef.hash(inputKeys, requiredDistribution.getPartitionCount());
        } else if (mode == AggregateMode.FINAL && requiredDistribution.getType() == RelDistribution.Type.SINGLETON) {
            OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
            requiredDistribution = traitDef.coordSingleton();
            inputDistribution = requiredDistribution;
        }
        return Pair.of(getTraitSet().replace(requiredDistribution), List.of(getInput().getTraitSet().replace(inputDistribution)));
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childId != 0) {
            return null;
        }
        OpenSearchDistribution childDistribution = OpenSearchRelNode.distributionOf(childTraits);
        if (childDistribution == null || childDistribution.getType() == RelDistribution.Type.ANY) {
            return null;
        }
        if (mode == AggregateMode.SINGLE) {
            if (childDistribution.getType() != RelDistribution.Type.SINGLETON) {
                return null;
            }
            return Pair.of(getTraitSet().replace(childDistribution), List.of(childTraits));
        }
        if (mode == AggregateMode.PARTIAL && childDistribution.getType() == RelDistribution.Type.SINGLETON) {
            return null;
        }
        OpenSearchDistribution outputDistribution = childDistribution;
        if (childDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED) {
            List<Integer> outputKeys = aggregateInputKeysToOutput(childDistribution.getKeys());
            if (outputKeys == null || childDistribution.getPartitionCount() == null) {
                return null;
            }
            OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) childDistribution.getTraitDef();
            outputDistribution = traitDef.hash(outputKeys, childDistribution.getPartitionCount());
        } else if (mode == AggregateMode.FINAL) {
            if (childDistribution.getType() != RelDistribution.Type.SINGLETON
                || childDistribution.getLocality() != OpenSearchDistribution.Locality.COORDINATOR) {
                return null;
            }
        }
        return Pair.of(getTraitSet().replace(outputDistribution), List.of(childTraits));
    }

    private List<Integer> aggregateOutputKeysToInput(List<Integer> outputKeys) {
        List<Integer> groupKeys = getGroupSet().asList();
        List<Integer> inputKeys = new ArrayList<>(outputKeys.size());
        for (int outputKey : outputKeys) {
            if (outputKey < 0 || outputKey >= groupKeys.size()) {
                return null;
            }
            inputKeys.add(groupKeys.get(outputKey));
        }
        return inputKeys;
    }

    private List<Integer> aggregateInputKeysToOutput(List<Integer> inputKeys) {
        List<Integer> groupKeys = getGroupSet().asList();
        List<Integer> outputKeys = new ArrayList<>(inputKeys.size());
        for (int inputKey : inputKeys) {
            int outputKey = groupKeys.indexOf(inputKey);
            if (outputKey < 0) {
                return null;
            }
            outputKeys.add(outputKey);
        }
        return outputKeys;
    }

    /** Aggregate execution cost after top-down traits choose a concrete mode and locality. */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        OpenSearchDistribution selfDistribution = OpenSearchRelNode.distributionOf(getTraitSet());
        if (selfDistribution == null || selfDistribution.getType() == RelDistribution.Type.ANY) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        if (mode == AggregateMode.FINAL) {
            int partitionCount = 1;
            if (selfDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                && selfDistribution.getLocality() == OpenSearchDistribution.Locality.WORKER
                && selfDistribution.getPartitionCount() != null) {
                partitionCount = Math.max(1, selfDistribution.getPartitionCount());
            }
            double finalRows = mq.getRowCount(getInput());
            double finalCost = finalRows / partitionCount;
            return planner.getCostFactory().makeCost(finalCost, finalCost, 0);
        }
        return planner.getCostFactory().makeTinyCost();
    }

    // ---- DistributionAware (Option B post-CBO enforcement pass) ----

    /**
     * A {@code SINGLE} aggregate over a distributable input declares it needs its input hash-partitioned
     * on the group keys, so the enforcement pass can split it into {@code Aggregate(PARTIAL)} on the
     * workers + {@code Aggregate(FINAL)} on the coordinator (the existing {@code OpenSearchAggregateSplitRule}
     * / {@code DistributedAggregateRewriter} machinery the pass reuses):
     * <ul>
     *   <li>non-empty group set → require {@code WORKER+HASH(groupKeys, N)} on the input;</li>
     *   <li>empty group set (e.g. {@code stats sum(x)} no {@code by}) → require {@code COORDINATOR+SINGLETON}:
     *       PARTIAL runs wherever the input is, FINAL merges the ≤N partials at the coordinator. The gather
     *       is bounded (one partial row per partition), so no hash key is needed.</li>
     * </ul>
     * Returns {@code null} (no requirement → input left at its CBO-chosen shape) for non-SINGLE modes, or
     * when the aggregate is not decomposable ({@code STATE_EXPANDING}/{@code DISTINCT}/percentile) — those
     * stay coordinator-centric. The {@code groupSet} of a SINGLE aggregate indexes INPUT columns, so the
     * hash keys are the group-set bits directly.
     */
    @Override
    public OpenSearchDistribution requiredInputDistribution(int inputIndex, int partitionCount, OpenSearchDistributionTraitDef traitDef) {
        if (inputIndex != 0 || mode != AggregateMode.SINGLE) {
            return null;
        }
        if (org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit(this)) {
            return null;
        }
        if (getGroupSet().isEmpty()) {
            // No partition key — the PARTIAL/FINAL split still distributes the work below, but the agg
            // itself gathers its partials to the coordinator. Requiring SINGLETON here is a no-op when the
            // input is already gathered; the distribution win comes from the input's own requirement.
            return traitDef.coordSingleton();
        }
        return traitDef.hash(getGroupSet().asList(), partitionCount);
    }

    /**
     * An aggregate's output is partitioned by its group keys only when it ran distributed (PARTIAL/FINAL)
     * — but in the pre-split SINGLE form the pass hasn't decided that yet, and the FINAL gathers to the
     * coordinator anyway. So we do not advertise a co-partitionable output here (returns {@code null}); a
     * parent that needs a specific partitioning will demand its own exchange. (Aggregate output rarely
     * feeds a co-partition-sensitive parent in PPL; revisit if a join-on-agg-output shape needs it.)
     */
    @Override
    public OpenSearchDistribution deriveOutputDistribution(
        List<OpenSearchDistribution> childDistributions,
        OpenSearchDistributionTraitDef traitDef
    ) {
        return null;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("mode", mode).item("viableBackends", viableBackends);
    }

    @Override
    public List<OperatorAnnotation> getAnnotations() {
        if (callAnnotations.isEmpty()) {
            return List.of();
        }
        List<OperatorAnnotation> annotations = new ArrayList<>(callAnnotations.size());
        for (int i = 0; i < getAggCallList().size(); i++) {
            AggregateCallAnnotation annotation = callAnnotations.get(i);
            if (annotation != null) {
                annotations.add(annotation);
            }
        }
        return annotations;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        // Rebuild the side-map preserving call-index keys, swapping annotation values
        // for the resolved (single-backend-narrowed) variants in the same iteration order
        // getAnnotations() used.
        Map<Integer, AggregateCallAnnotation> rebuilt = new LinkedHashMap<>(callAnnotations.size());
        int annotationIndex = 0;
        for (int i = 0; i < getAggCallList().size(); i++) {
            if (callAnnotations.containsKey(i)) {
                rebuilt.put(i, (AggregateCallAnnotation) resolvedAnnotations.get(annotationIndex++));
            }
        }
        return new OpenSearchAggregate(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            getGroupSet(),
            getGroupSets(),
            getAggCallList(),
            mode,
            List.of(backend),
            rebuilt,
            finalExtraLiteralArgs,
            perCallIntermediateField
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // Annotations live out-of-band; the aggCall list passes through unchanged.
        return LogicalAggregate.create(strippedChildren.getFirst(), List.of(), getGroupSet(), getGroupSets(), getAggCallList());
    }
}
