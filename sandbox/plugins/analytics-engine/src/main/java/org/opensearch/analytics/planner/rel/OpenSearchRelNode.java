/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.List;
import java.util.function.Function;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Marker interface for all OpenSearch custom RelNodes that carry backend assignment
 * and per-column storage metadata.
 *
 * <p>TODO: consider making this an abstract class storing {@code viableBackends} centrally,
 * with a default {@link #copyResolved} that returns {@code this} when already narrowed to
 * a single backend — avoids unnecessary copies during plan forking.
 *
 * <p>TODO: when JMH benchmarks show RelNode copy/strip as a hotspot, consider
 * preserving the original LogicalXxx fragment alongside the marked fragment through
 * the DAG to avoid reconstruction via {@link #copyResolved} and {@link #stripAnnotations}.
 *
 * @opensearch.internal
 */
public interface OpenSearchRelNode extends PhysicalNode {

    /** Returns the OpenSearch distribution carried by {@code traits}, if any. */
    static @Nullable OpenSearchDistribution distributionOf(RelTraitSet traits) {
        for (int i = 0; i < traits.size(); i++) {
            RelTrait trait = traits.getTrait(i);
            if (trait instanceof OpenSearchDistribution distribution) {
                return distribution;
            }
        }
        return null;
    }

    /**
     * Returns the common table id when the subtree can naturally execute at one shard.
     * This is a provenance check, not an enforceable trait: every leaf must originate from
     * the same one-shard table.
     */
    static @Nullable Integer singleShardTableId(RelNode rel) {
        RelNode current = rel instanceof RelSubset subset ? subset.getBestOrOriginal() : rel;
        if (current == null || current == rel && rel instanceof RelSubset) {
            return null;
        }
        OpenSearchDistribution distribution = distributionOf(current.getTraitSet());
        if (distribution != null
            && distribution.getLocality() == OpenSearchDistribution.Locality.SHARD
            && distribution.getType() == org.apache.calcite.rel.RelDistribution.Type.SINGLETON
            && Integer.valueOf(1).equals(distribution.getShardCount())
            && distribution.getTableId() != null) {
            return distribution.getTableId();
        }
        if (current.getInputs().isEmpty()) {
            return null;
        }
        Integer commonTableId = null;
        for (RelNode input : current.getInputs()) {
            Integer tableId = singleShardTableId(input);
            if (tableId == null) {
                return null;
            }
            if (commonTableId == null) {
                commonTableId = tableId;
            } else if (commonTableId.equals(tableId) == false) {
                return null;
            }
        }
        return commonTableId;
    }

    /**
     * Most OpenSearch operators do not propagate physical traits. Operators that are
     * transparent to, require, or derive a distribution override these hooks explicitly.
     * Returning {@code null} is Calcite's contract for "no alternative" and keeps newly
     * introduced operators safe when top-down Volcano optimization is enabled.
     */
    @Override
    default @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return null;
    }

    @Override
    default @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return null;
    }

    /** All backends that could execute this operator, including via delegation. */
    List<String> getViableBackends();

    /** Per-column storage metadata aligned with this node's output row type field order. */
    List<FieldStorageInfo> getOutputFieldStorage();

    /** Returns annotations inside this operator (predicates, calls, expressions). Empty if none. */
    default List<OperatorAnnotation> getAnnotations() {
        return List.of();
    }

    /**
     * Creates a copy with viableBackends narrowed to the given backend and
     * annotations replaced with the resolved versions.
     *
     * <p>{@code children} contains the resolved inputs in the same order as
     * the node's inputs. Single-input operators (Filter, Aggregate, Sort)
     * use {@code children.getFirst()}; future multi-input operators (Join) will
     * use multiple entries.
     *
     * @param backend              the chosen backend for this operator
     * @param children             resolved child RelNodes
     * @param resolvedAnnotations  annotations narrowed to single backends, same order as {@link #getAnnotations()}
     */
    RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations);

    /**
     * Returns a clean standard Calcite RelNode with viableBackends dropped and
     * annotations unwrapped to original expressions. Passed to the backend's
     * {@link FragmentConvertor}.
     *
     * <p>{@code strippedChildren} follows the same ordering convention as
     * {@code children} in {@link #copyResolved}.
     *
     * @param strippedChildren children already stripped
     */
    RelNode stripAnnotations(List<RelNode> strippedChildren);

    /**
     * Returns a clean standard Calcite RelNode with annotations resolved via the given function.
     * The resolver decides per-annotation what to return: the unwrapped original for native
     * annotations, or a placeholder (e.g., {@code delegated_predicate(annotationId)}) for
     * delegated ones.
     *
     * <p>Default delegates to {@link #stripAnnotations(List)} — correct for operators
     * with no annotations (Sort, Scan, ExchangeReducer, StageInputScan).
     *
     * @param strippedChildren    children already stripped
     * @param annotationResolver  maps each annotation to its replacement RexNode
     */
    default RelNode stripAnnotations(List<RelNode> strippedChildren, Function<OperatorAnnotation, RexNode> annotationResolver) {
        return stripAnnotations(strippedChildren);
    }
}
