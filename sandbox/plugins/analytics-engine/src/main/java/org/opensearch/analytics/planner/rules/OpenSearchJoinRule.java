/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.JoinCapability;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HEP marker rewriting {@link LogicalJoin} → {@link OpenSearchJoin}. It leaves
 * distribution unresolved so top-down Volcano can compare coordinator, co-located,
 * hash-shuffle, and broadcast implementations.
 *
 * <p>Accepts INNER / LEFT / RIGHT / FULL / SEMI / ANTI joins of any condition
 * shape — pure equi (hash join), mixed equi + non-equi (hash on the equi keys
 * with the residual riding along as a filter), null-safe equi via
 * {@code IS_NOT_DISTINCT_FROM}, equi with one operand being a {@code RexCall}
 * expression, and pure non-equi (e.g. {@code t1.a < t2.b} alone) — DataFusion
 * picks the appropriate physical strategy (HashJoin / SortMergeJoin /
 * NestedLoopJoin) based on the condition shape downstream.
 *
 * @opensearch.internal
 */
public class OpenSearchJoinRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchJoinRule(PlannerContext context) {
        super(operand(LogicalJoin.class, any()), "OpenSearchJoinRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        return joinType == JoinRelType.INNER
            || joinType == JoinRelType.LEFT
            || joinType == JoinRelType.RIGHT
            || joinType == JoinRelType.FULL
            || joinType == JoinRelType.SEMI
            || joinType == JoinRelType.ANTI;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        // Viable backends = intersection of inputs' viable backends, narrowed to those whose
        // joinCapabilities declare the join's required JoinKind. Inputs are HepRelVertex-
        // wrapped marked nodes by the time this rule fires; bottom-up HEP traversal
        // guarantees they're already in OpenSearchConvention.
        List<String> viableBackends = computeViableBackends(join.getLeft(), join.getRight());
        List<String> candidateBackends = List.copyOf(viableBackends);
        JoinCapability.JoinKind requiredKind = JoinCapability.JoinKind.fromCalcite(join.getJoinType());
        viableBackends.removeIf(backend -> {
            var caps = context.getCapabilityRegistry().getBackend(backend).getCapabilityProvider();
            for (JoinCapability cap : caps.joinCapabilities()) {
                if (cap.kinds().contains(requiredKind)) return false;
            }
            return true;
        });
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException(
                "No backend supports join kind [" + requiredKind + "] among viable backends " + candidateBackends
            );
        }
        // HEP marking only — no ER insertion or strategy selection.
        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        RelNode leftUnwrapped = RelNodeUtils.unwrapHep(join.getLeft());
        RelNode rightUnwrapped = RelNodeUtils.unwrapHep(join.getRight());
        // Leave strategy unresolved. Top-down passThroughTraits can satisfy a coordinator
        // demand, while the broadcast/hash rules add their own physical alternatives.
        RelTraitSet joinTraits = leftUnwrapped.getTraitSet().replace(distTraitDef.any());
        // Pull predicates shared by every OR branch to the top-level conjunction before
        // the split rules call JoinInfo.analyzeCondition(). TPC-H q19 expresses the same
        // equi key in three OR branches; without this normalization Calcite reports no
        // left/right join keys, so both broadcast and hash-shuffle incorrectly treat the
        // join as pure theta and coordinator-centric becomes the only legal alternative.
        RexNode normalizedCondition = RexUtil.pullFactors(join.getCluster().getRexBuilder(), join.getCondition());
        OpenSearchJoin osJoin = new OpenSearchJoin(
            join.getCluster(),
            joinTraits,
            leftUnwrapped,
            rightUnwrapped,
            normalizedCondition,
            join.getJoinType(),
            viableBackends
        );
        call.transformTo(osJoin);
    }

    /** Intersection of viable backends from left and right children. Children may be
     *  {@link HepRelVertex}-wrapped — unwrap to read viableBackends if it's an
     *  {@link OpenSearchRelNode}. onMatch then narrows to backends whose
     *  {@link JoinCapability} declares the join's required kind. */
    private static List<String> computeViableBackends(RelNode left, RelNode right) {
        List<String> leftBackends = viableBackendsOf(left);
        List<String> rightBackends = viableBackendsOf(right);

        Set<String> intersection = new LinkedHashSet<>(leftBackends);
        intersection.retainAll(rightBackends);
        return new ArrayList<>(intersection);
    }

    private static List<String> viableBackendsOf(RelNode rel) {
        if (RelNodeUtils.unwrapHep(rel) instanceof OpenSearchRelNode osNode) {
            return osNode.getViableBackends();
        }
        // Not yet marked — empty list forces the fallback path above.
        return List.of();
    }
}
