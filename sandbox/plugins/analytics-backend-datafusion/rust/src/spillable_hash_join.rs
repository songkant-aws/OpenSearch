/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Runtime-adaptive, spillable execution for partitioned hash joins.
//!
//! DataFusion's `HashJoinExec` collects the complete build partition before it
//! creates the hash table. That is fast when the planner's estimate is right,
//! but a stale or missing estimate can make the build reservation fail at
//! runtime. This module keeps the normal in-memory path for small builds and
//! switches to a Grace-style hash join when the observed build size or the
//! query memory pool says that continuing in memory is unsafe.
//!
//! The spill path writes the build input once, hash-partitions both sides with
//! DataFusion's own `BatchPartitioner`, and executes one ordinary
//! `HashJoinExec` per disk-backed bucket. Therefore the row-to-bucket mapping is
//! identical on both sides and each bucket releases its hash table before the
//! next one starts.

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit};
use datafusion::execution::TaskContext;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, SpillMetrics,
};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    SpillManager,
};
use futures::{stream, StreamExt, TryStreamExt};

/// At least one MiB is left for a build bucket even when the query is already
/// under severe pressure. The pool reservation remains the final authority.
const MIN_BUILD_TARGET_BYTES: usize = 1024 * 1024;
/// Unknown or unbounded pools still need a finite runtime guard so a bad
/// estimate cannot grow the build side without limit.
const DEFAULT_BUILD_TARGET_BYTES: usize = 512 * 1024 * 1024;
/// Limit the number of simultaneously open bucket files. A bucket that remains
/// too large because of extreme key skew is still protected by DataFusion's
/// memory reservation and fails with a resource error rather than an OOM.
const MAX_SPILL_BUCKETS: usize = 256;

/// Installs [`SpillableHashJoinExec`] around partitioned hash joins.
#[derive(Debug)]
pub(crate) struct SpillableHashJoinOptimizer;

impl PhysicalOptimizerRule for SpillableHashJoinOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            // CollectLeft is used for broadcast joins and has one build shared by
            // several probe partitions. Wrapping it independently per output
            // partition would duplicate the build. Null-aware anti joins require
            // global cross-bucket state, so keep DataFusion's implementation.
            if join.partition_mode() != &PartitionMode::Partitioned || join.null_aware {
                return Ok(Transformed::no(node));
            }

            Ok(Transformed::yes(Arc::new(SpillableHashJoinExec::new(join))))
        })
        .map(|rewritten| rewritten.data)
    }

    fn name(&self) -> &str {
        "spillable_hash_join"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// A runtime-adaptive wrapper around a partitioned [`HashJoinExec`].
#[derive(Debug)]
struct SpillableHashJoinExec {
    original: Arc<HashJoinExec>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    build_target_override: Option<usize>,
}

impl SpillableHashJoinExec {
    fn new(original: &HashJoinExec) -> Self {
        Self {
            original: Arc::new(
                original
                    .builder()
                    .reset_state()
                    .build()
                    .expect("an existing HashJoinExec must rebuild"),
            ),
            properties: Arc::clone(original.properties()),
            metrics: ExecutionPlanMetricsSet::new(),
            build_target_override: None,
        }
    }

    #[cfg(test)]
    fn new_with_build_target(original: &HashJoinExec, build_target_bytes: usize) -> Self {
        let mut exec = Self::new(original);
        exec.build_target_override = Some(build_target_bytes.max(1));
        exec
    }

    fn build_target_bytes(context: &TaskContext) -> usize {
        let pool = &context.runtime_env().memory_pool;
        match pool.memory_limit() {
            MemoryLimit::Finite(limit) => {
                let available = limit.saturating_sub(pool.reserved());
                // Keep headroom for the probe pipeline and for the hash table,
                // whose size is larger than the Arrow build batches themselves.
                (available / 3).max(MIN_BUILD_TARGET_BYTES)
            }
            MemoryLimit::Infinite | MemoryLimit::Unknown => DEFAULT_BUILD_TARGET_BYTES,
        }
    }

    async fn prepare_stream(
        original: Arc<HashJoinExec>,
        metrics: ExecutionPlanMetricsSet,
        partition: usize,
        context: Arc<TaskContext>,
        build_target_override: Option<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let target_bytes =
            build_target_override.unwrap_or_else(|| Self::build_target_bytes(&context));
        let pool = Arc::clone(&context.runtime_env().memory_pool);
        let reservation = MemoryConsumer::new(format!("SpillableHashJoinBuild[{partition}]"))
            .with_can_spill(true)
            .register(&pool);

        let observed_build_bytes =
            MetricBuilder::new(&metrics).gauge("observed_build_bytes", partition);
        let spill_trigger_count =
            MetricBuilder::new(&metrics).counter("spill_trigger_count", partition);
        let spill_bucket_count =
            MetricBuilder::new(&metrics).gauge("spill_bucket_count", partition);
        let spill_metrics = SpillMetrics::new(&metrics, partition);

        let mut build_stream = original.left().execute(partition, Arc::clone(&context))?;
        let mut build_batches = Vec::new();
        let mut build_bytes = 0usize;
        let mut must_spill = false;

        while let Some(batch) = build_stream.next().await {
            let batch = batch?;
            let bytes = batch.get_array_memory_size();
            build_bytes = build_bytes.saturating_add(bytes);
            observed_build_bytes.set(build_bytes);

            // Account for both retained Arrow buffers and the hash table / joined
            // build batch. If the pool refuses even the Arrow reservation, spill
            // immediately rather than letting HashJoinExec reach its OOM path.
            let estimated_working_set = build_bytes.saturating_mul(2);
            if estimated_working_set > target_bytes || reservation.try_grow(bytes).is_err() {
                must_spill = true;
            }
            build_batches.push(batch);
            if must_spill {
                break;
            }
        }

        if must_spill == false {
            drop(reservation);
            let build =
                MemorySourceConfig::try_new_exec(&[build_batches], original.left().schema(), None)?;
            let probe_stream = original.right().execute(partition, Arc::clone(&context))?;
            let probe = one_shot_exec(original.right().schema(), probe_stream)?;
            let join = rebuild_bucket_join(&original, build, probe)?;
            return join.execute(0, context);
        }

        spill_trigger_count.add(1);
        drop(reservation);

        let runtime = context.runtime_env();
        let build_spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            spill_metrics.clone(),
            original.left().schema(),
        );
        let probe_spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            spill_metrics,
            original.right().schema(),
        );

        let mut raw_build =
            build_spill_manager.create_in_progress_file("spillable hash join raw build")?;
        for batch in &build_batches {
            raw_build.append_batch(batch)?;
        }
        build_batches.clear();

        while let Some(batch) = build_stream.next().await {
            let batch = batch?;
            build_bytes = build_bytes.saturating_add(batch.get_array_memory_size());
            observed_build_bytes.set(build_bytes);
            raw_build.append_batch(&batch)?;
        }
        let raw_build = raw_build.finish()?.ok_or_else(|| {
            DataFusionError::Internal(
                "spillable hash join entered spill path without build data".to_string(),
            )
        })?;

        // Size buckets for the expected Arrow + hash-table working set and round
        // up to a power of two for the hash reducer's fast path.
        let required = build_bytes
            .saturating_mul(2)
            .div_ceil(target_bytes.max(1))
            .max(2);
        let bucket_count = required
            .checked_next_power_of_two()
            .unwrap_or(MAX_SPILL_BUCKETS)
            .min(MAX_SPILL_BUCKETS);
        spill_bucket_count.set(bucket_count);

        let left_keys = original
            .on()
            .iter()
            .map(|(left, _)| Arc::clone(left))
            .collect::<Vec<_>>();
        let right_keys = original
            .on()
            .iter()
            .map(|(_, right)| Arc::clone(right))
            .collect::<Vec<_>>();

        let build_files = partition_spill_file(
            raw_build,
            &build_spill_manager,
            left_keys,
            bucket_count,
            partition,
            &metrics,
            "spillable hash join build bucket",
        )
        .await?;

        let probe_stream = original.right().execute(partition, Arc::clone(&context))?;
        let probe_files = partition_stream_to_files(
            probe_stream,
            &probe_spill_manager,
            right_keys,
            bucket_count,
            partition,
            &metrics,
            "spillable hash join probe bucket",
        )
        .await?;

        let mut bucket_streams = Vec::with_capacity(bucket_count);
        for (build_file, probe_file) in build_files.into_iter().zip(probe_files) {
            let build = spill_file_exec(
                original.left().schema(),
                build_spill_manager.clone(),
                build_file,
            )?;
            let probe = spill_file_exec(
                original.right().schema(),
                probe_spill_manager.clone(),
                probe_file,
            )?;
            let join = rebuild_bucket_join(&original, build, probe)?;
            bucket_streams.push(join.execute(0, Arc::clone(&context))?);
        }

        let chained: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            original.schema(),
            stream::iter(bucket_streams).flatten(),
        ));
        if let Some(fetch) = original.fetch() {
            let baseline = BaselineMetrics::new(&metrics, partition);
            Ok(Box::pin(LimitStream::new(
                chained,
                0,
                Some(fetch),
                baseline,
            )))
        } else {
            Ok(chained)
        }
    }
}

impl DisplayAs for SpillableHashJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "SpillableHashJoinExec: mode=Partitioned, join_type={:?}, on={:?}",
                self.original.join_type(),
                self.original.on()
            ),
            DisplayFormatType::TreeRender => write!(f, "spillable_hash_join"),
        }
    }
}

impl ExecutionPlan for SpillableHashJoinExec {
    fn name(&self) -> &str {
        "SpillableHashJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.original.left(), self.original.right()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let rebuilt = self
            .original
            .builder()
            .with_new_children(children)?
            .reset_state()
            .build()?;
        Ok(Arc::new(Self::new(&rebuilt)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Preserve existing behavior when spill is intentionally disabled.
        if context.runtime_env().disk_manager.tmp_files_enabled() == false {
            return self.original.execute(partition, context);
        }

        let future = Self::prepare_stream(
            Arc::clone(&self.original),
            self.metrics.clone(),
            partition,
            context,
            self.build_target_override,
        );
        let lazy = stream::once(future).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), lazy)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fetch(&self) -> Option<usize> {
        self.original.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.original
            .with_fetch(limit)
            .and_then(|plan| plan.downcast_ref::<HashJoinExec>().map(Self::new))
            .map(|plan| Arc::new(plan) as Arc<dyn ExecutionPlan>)
    }
}

fn rebuild_bucket_join(
    original: &HashJoinExec,
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    original
        .builder()
        .with_new_children(vec![build, probe])?
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_fetch(None)
        .reset_state()
        .build_exec()
}

fn one_shot_exec(
    schema: SchemaRef,
    input: SendableRecordBatchStream,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition: Arc<dyn PartitionStream> = Arc::new(OneShotPartition {
        schema: Arc::clone(&schema),
        input: Mutex::new(Some(input)),
    });
    Ok(Arc::new(StreamingTableExec::try_new(
        schema,
        vec![partition],
        None,
        Vec::new(),
        false,
        None,
    )?))
}

struct OneShotPartition {
    schema: SchemaRef,
    input: Mutex<Option<SendableRecordBatchStream>>,
}

impl fmt::Debug for OneShotPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OneShotPartition")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl PartitionStream for OneShotPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.input
            .lock()
            .expect("one-shot partition mutex poisoned")
            .take()
            .unwrap_or_else(|| {
                Box::pin(RecordBatchStreamAdapter::new(
                    Arc::clone(&self.schema),
                    stream::empty(),
                ))
            })
    }
}

fn spill_file_exec(
    schema: SchemaRef,
    manager: SpillManager,
    file: Option<RefCountedTempFile>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let partition: Arc<dyn PartitionStream> = Arc::new(SpillFilePartition {
        schema: Arc::clone(&schema),
        manager,
        file,
    });
    Ok(Arc::new(StreamingTableExec::try_new(
        schema,
        vec![partition],
        None,
        Vec::new(),
        false,
        None,
    )?))
}

#[derive(Debug)]
struct SpillFilePartition {
    schema: SchemaRef,
    manager: SpillManager,
    file: Option<RefCountedTempFile>,
}

impl PartitionStream for SpillFilePartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        match &self.file {
            Some(file) => self
                .manager
                .read_spill_as_stream(file.clone(), None)
                .unwrap_or_else(|error| {
                    Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(&self.schema),
                        stream::once(async move { Err(error) }),
                    ))
                }),
            None => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::empty(),
            )),
        }
    }
}

async fn partition_spill_file(
    file: RefCountedTempFile,
    manager: &SpillManager,
    keys: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    bucket_count: usize,
    input_partition: usize,
    metrics: &ExecutionPlanMetricsSet,
    description: &str,
) -> Result<Vec<Option<RefCountedTempFile>>> {
    let stream = manager.read_spill_as_stream(file, None)?;
    partition_stream_to_files(
        stream,
        manager,
        keys,
        bucket_count,
        input_partition,
        metrics,
        description,
    )
    .await
}

async fn partition_stream_to_files(
    mut input: SendableRecordBatchStream,
    manager: &SpillManager,
    keys: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    bucket_count: usize,
    input_partition: usize,
    metrics: &ExecutionPlanMetricsSet,
    description: &str,
) -> Result<Vec<Option<RefCountedTempFile>>> {
    let timer = MetricBuilder::new(metrics).subset_time("spill_partition_time", input_partition);
    let mut partitioner = BatchPartitioner::new_hash_partitioner(keys, bucket_count, timer)?;
    let mut writers = (0..bucket_count)
        .map(|bucket| manager.create_in_progress_file(&format!("{description} {bucket}")))
        .collect::<Result<Vec<_>>>()?;

    while let Some(batch) = input.next().await {
        partitioner.partition(batch?, |bucket, batch| {
            writers[bucket].append_batch(&batch)?;
            Ok(())
        })?;
    }

    writers
        .into_iter()
        .map(|mut writer| writer.finish())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::Column;

    fn input(
        schema: SchemaRef,
        keys: Vec<i32>,
        values: Vec<i64>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )?)
    }

    #[tokio::test]
    async fn spills_partitioned_hash_join_and_preserves_results() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("left_value", DataType::Int64, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("right_value", DataType::Int64, false),
        ]));

        let left_keys = (0..100).collect::<Vec<i32>>();
        let left_values = (0..100).map(i64::from).collect::<Vec<_>>();
        let right_keys = (50..150).collect::<Vec<i32>>();
        let right_values = (50..150).map(|v| i64::from(v) * 10).collect::<Vec<_>>();
        let left = input(Arc::clone(&left_schema), left_keys, left_values)?;
        let right = input(Arc::clone(&right_schema), right_keys, right_values)?;
        let join = HashJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(Column::new("key", 0)),
                Arc::new(Column::new("key", 0)),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?;

        // The input is deliberately larger than this target, so the test
        // exercises disk partitioning instead of the in-memory fast path.
        let adaptive = SpillableHashJoinExec::new_with_build_target(&join, 512);
        let batches = adaptive
            .execute(0, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await?;
        let output_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert_eq!(output_rows, 50);
        let mut actual = batches
            .iter()
            .flat_map(|batch| {
                let left_key = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("left key");
                let left_value = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("left value");
                let right_key = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("right key");
                let right_value = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("right value");
                (0..batch.num_rows()).map(move |row| {
                    (
                        left_key.value(row),
                        left_value.value(row),
                        right_key.value(row),
                        right_value.value(row),
                    )
                })
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let expected = (50..100)
            .map(|key| (key, i64::from(key), key, i64::from(key) * 10))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);

        let metrics = adaptive.metrics().expect("adaptive join metrics");
        assert_eq!(
            metrics
                .sum_by_name("spill_trigger_count")
                .expect("spill trigger metric")
                .as_usize(),
            1
        );
        assert!(metrics.spilled_bytes().unwrap_or_default() > 0);
        Ok(())
    }
}
