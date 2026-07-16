/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.List;

/** Cost tests for legal join implementations produced by top-down trait propagation. */
public class OpenSearchJoinCostTests extends BasePlannerRulesTests {

    private VolcanoPlanner volcano;
    private RelOptCluster volcanoCluster;
    private OpenSearchDistributionTraitDef traitDef;
    private RelOptTable testTable;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        PlannerContext context = buildContext("parquet", /* shardCount */ 3, intFields());
        traitDef = context.getDistributionTraitDef();
        volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(ConventionTraitDef.INSTANCE);
        volcano.addRelTraitDef(traitDef);
        volcanoCluster = RelOptCluster.create(volcano, new RexBuilder(typeFactory));
        testTable = mockTable("test_index", "status", "size");
    }

    public void testUnresolvedDistributionHasInfiniteCost() {
        OpenSearchJoin join = makeJoin(scanWith(traitDef.any()), scanWith(traitDef.any()), traitDef.any());
        assertTrue(costOf(join).isInfinite());
    }

    public void testCoordinatorJoinHasFiniteCost() {
        OpenSearchJoin join = makeJoin(scanWith(traitDef.coordSingleton()), scanWith(traitDef.coordSingleton()), traitDef.coordSingleton());
        assertFalse(costOf(join).isInfinite());
    }

    public void testHashJoinAccountsForParallelExecution() {
        RelOptCost coordinatorCost = costOf(
            makeJoin(scanWith(traitDef.coordSingleton()), scanWith(traitDef.coordSingleton()), traitDef.coordSingleton())
        );
        OpenSearchJoin hashJoin = makeJoin(
            scanWith(traitDef.hash(List.of(0), 4)),
            scanWith(traitDef.hash(List.of(0), 4)),
            traitDef.hash(List.of(0), 4)
        ).withJoinAlgorithm(OpenSearchJoin.JoinAlgorithm.HASH, 100d, 1_000d, 20_000_000L, 512L * 1024 * 1024);
        RelOptCost hashCost = costOf(hashJoin);

        assertFalse(hashCost.isInfinite());
        assertTrue("hash work should be divided over its partitions", hashCost.isLt(coordinatorCost));
    }

    public void testBroadcastJoinAccountsForProbeParallelism() {
        int tableId = testTable.getQualifiedName().hashCode();
        OpenSearchDistribution probe = traitDef.shardRandom(tableId, 4);
        RelOptCost coordinatorCost = costOf(
            makeJoin(scanWith(traitDef.coordSingleton()), scanWith(traitDef.coordSingleton()), traitDef.coordSingleton())
        );
        RelOptCost broadcastCost = costOf(makeJoin(scanWith(probe), scanWith(traitDef.broadcast(4)), traitDef.from(probe)));

        assertFalse(broadcastCost.isInfinite());
        assertTrue("broadcast work should be divided over probe nodes", broadcastCost.isLt(coordinatorCost));
    }

    public void testMemorySafeHashJoinCostsLessThanSortMerge() {
        OpenSearchDistribution hash = traitDef.hash(List.of(0), 4);
        OpenSearchJoin base = makeJoin(scanWith(hash), scanWith(hash), hash);
        OpenSearchJoin hashJoin = base.withJoinAlgorithm(
            OpenSearchJoin.JoinAlgorithm.HASH,
            1_000_000d,
            32d * 1024 * 1024,
            20_000_000L,
            512L * 1024 * 1024
        );
        OpenSearchJoin sortMerge = base.withJoinAlgorithm(
            OpenSearchJoin.JoinAlgorithm.SORT_MERGE,
            1_000_000d,
            32d * 1024 * 1024,
            20_000_000L,
            512L * 1024 * 1024
        );

        assertTrue("HJ should win when its build fits both budgets", costOf(hashJoin).isLt(costOf(sortMerge)));
    }

    public void testHashJoinOverByteBudgetIsIneligible() {
        OpenSearchDistribution hash = traitDef.hash(List.of(0), 4);
        OpenSearchJoin hashJoin = makeJoin(scanWith(hash), scanWith(hash), hash).withJoinAlgorithm(
            OpenSearchJoin.JoinAlgorithm.HASH,
            1_000_000d,
            600d * 1024 * 1024,
            20_000_000L,
            512L * 1024 * 1024
        );

        assertTrue("non-spillable HJ must be rejected when its per-worker hash table exceeds budget", costOf(hashJoin).isInfinite());
    }

    public void testHashJoinWithUnknownBuildStatsIsIneligible() {
        OpenSearchDistribution hash = traitDef.hash(List.of(0), 4);
        OpenSearchJoin hashJoin = makeJoin(scanWith(hash), scanWith(hash), hash).withJoinAlgorithm(
            OpenSearchJoin.JoinAlgorithm.HASH,
            Double.NaN,
            Double.NaN,
            20_000_000L,
            512L * 1024 * 1024
        );

        assertTrue("unknown build statistics must not optimistically select HJ", costOf(hashJoin).isInfinite());
    }

    private OpenSearchTableScan scanWith(OpenSearchDistribution distribution) {
        RelTraitSet traits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(distribution);
        return new OpenSearchTableScan(volcanoCluster, traits, testTable, List.of("mock-parquet"), List.<FieldStorageInfo>of());
    }

    private OpenSearchJoin makeJoin(RelNode left, RelNode right, OpenSearchDistribution distribution) {
        RelTraitSet traits = RelTraitSet.createEmpty().plus(OpenSearchConvention.INSTANCE).plus(distribution);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), left.getRowType().getFieldCount())
        );
        return new OpenSearchJoin(volcanoCluster, traits, left, right, condition, JoinRelType.INNER, List.of("mock-parquet"));
    }

    private RelOptCost costOf(OpenSearchJoin join) {
        return join.computeSelfCost(volcano, RelMetadataQuery.instance());
    }
}
