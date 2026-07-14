/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;
import java.util.Map;

/** Contract tests for Calcite's top-down {@code PhysicalNode} trait hooks. */
public class TopDownTraitPropagationTests extends BasePlannerRulesTests {

    private static final int PARTITIONS = 3;
    private static final List<String> BACKENDS = List.of(MockDataFusionBackend.NAME);

    private OpenSearchDistributionTraitDef traitDef;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        traitDef = buildContext("parquet", intFields()).getDistributionTraitDef();
        cluster.getPlanner().addRelTraitDef(traitDef);
    }

    public void testFilterPassesAndDerivesHashDistribution() {
        TableScan scan = scan("filter_idx");
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeLiteral(0, intType, false)
        );
        OpenSearchFilter filter = new OpenSearchFilter(cluster, traits(traitDef.any()), scan, condition, BACKENDS);
        OpenSearchDistribution hash = traitDef.hash(List.of(0), PARTITIONS);

        Pair<RelTraitSet, List<RelTraitSet>> passed = filter.passThroughTraits(traits(hash));
        assertNotNull(passed);
        assertEquals(hash, distribution(passed.left));
        assertEquals(hash, distribution(passed.right.getFirst()));

        Pair<RelTraitSet, List<RelTraitSet>> derived = filter.deriveTraits(traits(hash), 0);
        assertNotNull(derived);
        assertEquals(hash, distribution(derived.left));
    }

    public void testProjectRemapsHashKeysInBothDirections() {
        TableScan scan = scan("project_idx");
        List<RexNode> projects = List.of(rexBuilder.makeInputRef(scan, 1), rexBuilder.makeInputRef(scan, 0));
        RelDataType outputType = typeFactory.builder()
            .add("size", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();
        OpenSearchProject project = new OpenSearchProject(cluster, traits(traitDef.any()), scan, projects, outputType, BACKENDS);

        Pair<RelTraitSet, List<RelTraitSet>> passed = project.passThroughTraits(traits(traitDef.hash(List.of(0), PARTITIONS)));
        assertNotNull(passed);
        assertEquals(List.of(1), distribution(passed.right.getFirst()).getKeys());

        Pair<RelTraitSet, List<RelTraitSet>> derived = project.deriveTraits(traits(traitDef.hash(List.of(1), PARTITIONS)), 0);
        assertNotNull(derived);
        assertEquals(List.of(0), distribution(derived.left).getKeys());
    }

    public void testJoinPassesSingletonAndDerivesHashRequirements() {
        OpenSearchJoin join = equiJoin();
        Pair<RelTraitSet, List<RelTraitSet>> passed = join.passThroughTraits(traits(traitDef.anySingleton()));
        assertNotNull(passed);
        assertCoordinatorSingleton(distribution(passed.left));
        assertCoordinatorSingleton(distribution(passed.right.get(0)));
        assertCoordinatorSingleton(distribution(passed.right.get(1)));

        OpenSearchDistribution leftHash = traitDef.hash(List.of(0), PARTITIONS);
        Pair<RelTraitSet, List<RelTraitSet>> derived = join.deriveTraits(traits(leftHash), 0);
        assertNotNull(derived);
        assertEquals(leftHash, distribution(derived.left));
        assertEquals(List.of(0), distribution(derived.right.get(1)).getKeys());
        assertEquals(Integer.valueOf(PARTITIONS), distribution(derived.right.get(1)).getPartitionCount());
    }

    public void testJoinDerivesColocatedShardSingleton() {
        TableScan left = scan("same_idx", 1);
        TableScan right = scan("same_idx", 1);
        OpenSearchJoin join = equiJoin(left, right);
        OpenSearchDistribution shard = distribution(left.getTraitSet());

        Pair<RelTraitSet, List<RelTraitSet>> derived = join.deriveTraits(traits(shard), 0);
        assertNotNull(derived);
        assertEquals(shard, distribution(derived.left));
        assertEquals(shard, distribution(derived.right.get(1)));
    }

    public void testPartialAggregateRemapsGroupKey() {
        TableScan scan = scan("agg_idx");
        OpenSearchAggregate partial = new OpenSearchAggregate(
            cluster,
            traits(traitDef.any()),
            scan,
            ImmutableBitSet.of(1),
            null,
            List.of(sumCall(scan)),
            AggregateMode.PARTIAL,
            BACKENDS,
            Map.of()
        );

        Pair<RelTraitSet, List<RelTraitSet>> passed = partial.passThroughTraits(traits(traitDef.hash(List.of(0), PARTITIONS)));
        assertNotNull(passed);
        assertEquals(List.of(1), distribution(passed.right.getFirst()).getKeys());

        Pair<RelTraitSet, List<RelTraitSet>> derived = partial.deriveTraits(traits(traitDef.hash(List.of(1), PARTITIONS)), 0);
        assertNotNull(derived);
        assertEquals(List.of(0), distribution(derived.left).getKeys());
    }

    public void testConventionBuildsMarkedShuffleEnforcer() {
        TableScan scan = scan("enforcer_idx");
        RelNode enforced = OpenSearchConvention.INSTANCE.enforce(scan, traits(traitDef.hash(List.of(0), PARTITIONS)));
        assertTrue(enforced instanceof OpenSearchShuffleExchange);
        assertTrue(enforced.isEnforcer());
    }

    public void testShardSingletonIsDerivedButNotEnforced() {
        TableScan scan = scan("shard_enforcer_idx");
        RelNode enforced = OpenSearchConvention.INSTANCE.enforce(scan, traits(traitDef.shardSingleton(17, 1)));
        assertNull(enforced);
    }

    public void testGlobalSortPassesCoordinatorRequirement() {
        TableScan scan = scan("sort_idx");
        OpenSearchSort sort = new OpenSearchSort(
            cluster,
            traits(traitDef.any()).plus(RelCollations.of(0)),
            scan,
            RelCollations.of(0),
            null,
            null,
            BACKENDS
        );

        Pair<RelTraitSet, List<RelTraitSet>> passed = sort.passThroughTraits(traits(traitDef.anySingleton()));
        assertNotNull(passed);
        assertCoordinatorSingleton(distribution(passed.left));
        assertCoordinatorSingleton(distribution(passed.right.getFirst()));
        Pair<RelTraitSet, List<RelTraitSet>> derived = sort.deriveTraits(traits(traitDef.shardSingleton(17, 1)), 0);
        assertNotNull(derived);
        assertEquals(OpenSearchDistribution.Locality.SHARD, distribution(derived.left).getLocality());
    }

    private OpenSearchJoin equiJoin() {
        TableScan left = scan("left_idx");
        TableScan right = scan("right_idx");
        return equiJoin(left, right);
    }

    private OpenSearchJoin equiJoin(TableScan left, TableScan right) {
        int leftColumns = left.getRowType().getFieldCount();
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, leftColumns)
        );
        return new OpenSearchJoin(cluster, traits(traitDef.any()), left, right, condition, JoinRelType.INNER, BACKENDS);
    }

    private TableScan scan(String name) {
        return scan(name, 2);
    }

    private TableScan scan(String name, int shardCount) {
        RelOptTable table = mockTable(name, "status", "size");
        return OpenSearchTableScan.create(cluster, table, BACKENDS, List.of(), shardCount, traitDef);
    }

    private RelTraitSet traits(OpenSearchDistribution distribution) {
        return cluster.traitSet().plus(OpenSearchConvention.INSTANCE).plus(distribution);
    }

    private OpenSearchDistribution distribution(RelTraitSet traits) {
        return OpenSearchRelNode.distributionOf(traits);
    }

    private static void assertCoordinatorSingleton(OpenSearchDistribution distribution) {
        assertEquals(RelDistribution.Type.SINGLETON, distribution.getType());
        assertEquals(OpenSearchDistribution.Locality.COORDINATOR, distribution.getLocality());
    }
}
