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
//! DataFusion's hash evaluator and an independent Grace seed, and executes one ordinary
//! `HashJoinExec` per disk-backed bucket. Therefore the row-to-bucket mapping is
//! identical on both sides and each bucket releases its hash table before the
//! next one starts.

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute::{take, SortOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_expr_common::utils::evaluate_expressions_to_arrays;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{
    HashJoinExec, PartitionMode, SeededRandomState, SortMergeJoinExec,
};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, SpillMetrics, Time,
};
use datafusion::physical_plan::sorts::sort::SortExec;
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
/// Number of child buckets used when a single Grace bucket is still too large.
/// Recursive repartitioning is deliberately bounded; the final level falls back
/// to DataFusion's spillable sort-merge join instead of risking an allocator OOM.
const RECURSIVE_BUCKET_FANOUT: usize = 4;
const MAX_RECURSIVE_DEPTH: usize = 4;
/// Seed used only for the local Grace repartition. The upstream MPP exchange uses
/// DataFusion's `REPARTITION_RANDOM_STATE` (seed 0); reusing it here can collapse all
/// rows into one bucket when the exchange partition count is a power of two.
/// Keep this separate from both the exchange and HashJoinExec seeds.
const GRACE_HASH_SEED: SeededRandomState = SeededRandomState::with_seed(0x5f37_59df_4a7c_15e9);

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
        // A Grace join emits buckets serially, so it cannot preserve a child's ordering even
        // when the wrapped HashJoinExec advertises one. Keep the partitioning and execution
        // characteristics, but rebuild equivalence properties from the output schema to clear
        // output_ordering. Otherwise a parent Sort/TopK may silently skip a required sort.
        let schema = original.schema();
        let input_properties = original.properties();
        let properties = Arc::new(
            PlanProperties::new(
                datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
                input_properties.output_partitioning().clone(),
                input_properties.emission_type,
                input_properties.boundedness,
            )
            .with_evaluation_type(input_properties.evaluation_type)
            .with_scheduling_type(input_properties.scheduling_type),
        );
        Self {
            original: Arc::new(
                original
                    .builder()
                    .reset_state()
                    .build()
                    .expect("an existing HashJoinExec must rebuild"),
            ),
            properties,
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
            let joined = join.execute(0, Arc::clone(&context))?;
            return apply_fetch(original.fetch(), joined, &metrics, partition);
        }

        spill_trigger_count.add(1);
        drop(reservation);

        let runtime = context.runtime_env();
        let build_spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            spill_metrics_for_side(&metrics, partition, "build"),
            original.left().schema(),
        );
        let probe_spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            spill_metrics_for_side(&metrics, partition, "probe"),
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
            left_keys.clone(),
            bucket_count,
            partition,
            &metrics,
            "spillable hash join build bucket",
            GRACE_HASH_SEED,
        )
        .await?;

        let probe_stream = original.right().execute(partition, Arc::clone(&context))?;
        let probe_files = partition_stream_to_files(
            probe_stream,
            &probe_spill_manager,
            right_keys.clone(),
            bucket_count,
            partition,
            &metrics,
            "spillable hash join probe bucket",
            GRACE_HASH_SEED,
        )
        .await?;

        let bucket_plans = expand_oversized_buckets(
            build_files,
            probe_files,
            target_bytes,
            &build_spill_manager,
            &probe_spill_manager,
            left_keys,
            right_keys,
            partition,
            &metrics,
        )
        .await?;

        // Execute one bucket at a time. The previous implementation created a
        // stream for every bucket eagerly, which kept all file readers and all
        // per-bucket join state alive until the final output batch. This lazy
        // stream releases each bucket before opening the next one.
        let chained = lazy_bucket_stream(
            original.schema(),
            Arc::clone(&original),
            bucket_plans,
            build_spill_manager,
            probe_spill_manager,
            Arc::clone(&context),
        );
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

/** DataFusion's standard spill metrics, labelled per join side for observability. */
fn spill_metrics_for_side(
    metrics: &ExecutionPlanMetricsSet,
    partition: usize,
    side: &'static str,
) -> SpillMetrics {
    SpillMetrics {
        spill_file_count: MetricBuilder::new(metrics)
            .with_new_label("side", side)
            .spill_count(partition),
        spilled_bytes: MetricBuilder::new(metrics)
            .with_new_label("side", side)
            .spilled_bytes(partition),
        spilled_rows: MetricBuilder::new(metrics)
            .with_new_label("side", side)
            .spilled_rows(partition),
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

fn rebuild_sort_merge_join(
    original: &HashJoinExec,
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let join_on = original.on().to_vec();
    // SortMergeJoinExec requires sorted children. Grace files are bucketed, not ordered, so use
    // DataFusion's external SortExec (which can spill independently) before the final hot-bucket
    // merge. Keeping this in the runtime fallback is what makes the fallback correct for arbitrary
    // key distributions, rather than only the single-key test case.
    let left_ordering = LexOrdering::new(
        original
            .on()
            .iter()
            .map(|(left, _)| PhysicalSortExpr {
                expr: Arc::clone(left),
                options: SortOptions::default(),
            })
            .collect::<Vec<_>>(),
    )
    .ok_or_else(|| DataFusionError::Internal("SMJ fallback has no join keys".to_string()))?;
    let right_ordering = LexOrdering::new(
        original
            .on()
            .iter()
            .map(|(_, right)| PhysicalSortExpr {
                expr: Arc::clone(right),
                options: SortOptions::default(),
            })
            .collect::<Vec<_>>(),
    )
    .ok_or_else(|| DataFusionError::Internal("SMJ fallback has no join keys".to_string()))?;
    let join = SortMergeJoinExec::try_new(
        Arc::new(SortExec::new(left_ordering, build)),
        Arc::new(SortExec::new(right_ordering, probe)),
        join_on,
        original.filter().cloned(),
        *original.join_type(),
        vec![SortOptions::default(); original.on().len()],
        original.null_equality(),
    )?;
    Ok(Arc::new(join))
}

#[derive(Debug)]
struct BucketPlan {
    build: Option<RefCountedTempFile>,
    probe: Option<RefCountedTempFile>,
    /// A final hot bucket uses SMJ, whose own sort streams are spillable.
    sort_merge: bool,
}

fn grace_seed(depth: usize) -> SeededRandomState {
    let salt = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul(depth as u64 + 1);
    SeededRandomState::with_seed(GRACE_HASH_SEED.seed().wrapping_add(salt))
}

async fn expand_oversized_buckets(
    build_files: Vec<Option<RefCountedTempFile>>,
    probe_files: Vec<Option<RefCountedTempFile>>,
    target_bytes: usize,
    build_manager: &SpillManager,
    probe_manager: &SpillManager,
    build_keys: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    probe_keys: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    input_partition: usize,
    metrics: &ExecutionPlanMetricsSet,
) -> Result<Vec<BucketPlan>> {
    let recursive_spill_count =
        MetricBuilder::new(metrics).counter("recursive_spill_count", input_partition);
    let sort_merge_fallback_count =
        MetricBuilder::new(metrics).counter("sort_merge_fallback_count", input_partition);
    let mut pending = build_files
        .into_iter()
        .zip(probe_files)
        .map(|(build, probe)| (build, probe, 0usize))
        .collect::<Vec<_>>();
    let mut output = Vec::new();
    let per_bucket_target = (target_bytes / 2).max(MIN_BUILD_TARGET_BYTES) as u64;

    while let Some((build, probe, depth)) = pending.pop() {
        let build_bytes = build
            .as_ref()
            .map(RefCountedTempFile::current_disk_usage)
            .unwrap_or(0);
        if build_bytes <= per_bucket_target || depth >= MAX_RECURSIVE_DEPTH {
            if build_bytes > per_bucket_target {
                sort_merge_fallback_count.add(1);
            }
            output.push(BucketPlan {
                build,
                probe,
                sort_merge: build_bytes > per_bucket_target,
            });
            continue;
        }
        recursive_spill_count.add(1);

        // Split both sides with the same depth-specific seed. A different seed
        // per recursion level prevents a hot bucket from reproducing the exact
        // collision pattern at every level.
        let build_children = match build {
            Some(file) => {
                partition_spill_file(
                    file,
                    build_manager,
                    build_keys.clone(),
                    RECURSIVE_BUCKET_FANOUT,
                    input_partition,
                    metrics,
                    "spillable hash join recursive build bucket",
                    grace_seed(depth + 1),
                )
                .await?
            }
            None => vec![None; RECURSIVE_BUCKET_FANOUT],
        };
        let probe_children = match probe {
            Some(file) => {
                partition_spill_file(
                    file,
                    probe_manager,
                    probe_keys.clone(),
                    RECURSIVE_BUCKET_FANOUT,
                    input_partition,
                    metrics,
                    "spillable hash join recursive probe bucket",
                    grace_seed(depth + 1),
                )
                .await?
            }
            None => vec![None; RECURSIVE_BUCKET_FANOUT],
        };

        for (child_build, child_probe) in build_children.into_iter().zip(probe_children).rev() {
            pending.push((child_build, child_probe, depth + 1));
        }
    }

    output.reverse();
    Ok(output)
}

fn lazy_bucket_stream(
    schema: SchemaRef,
    original: Arc<HashJoinExec>,
    buckets: Vec<BucketPlan>,
    build_manager: SpillManager,
    probe_manager: SpillManager,
    context: Arc<TaskContext>,
) -> SendableRecordBatchStream {
    struct State {
        original: Arc<HashJoinExec>,
        buckets: Vec<BucketPlan>,
        next: usize,
        current: Option<SendableRecordBatchStream>,
        build_manager: SpillManager,
        probe_manager: SpillManager,
        context: Arc<TaskContext>,
    }

    let state = State {
        original,
        buckets,
        next: 0,
        current: None,
        build_manager,
        probe_manager,
        context,
    };
    let stream = stream::try_unfold(state, |mut state| async move {
        loop {
            if let Some(mut current) = state.current.take() {
                match current.next().await {
                    Some(Ok(batch)) => {
                        state.current = Some(current);
                        return Ok(Some((batch, state)));
                    }
                    Some(Err(error)) => return Err(error),
                    None => {}
                }
            }

            let Some(bucket) = state.buckets.get(state.next) else {
                return Ok(None);
            };
            state.next += 1;
            if bucket.build.is_none() && bucket.probe.is_none() {
                continue;
            }

            let build = spill_file_exec(
                state.original.left().schema(),
                state.build_manager.clone(),
                bucket.build.clone(),
            )?;
            let probe = spill_file_exec(
                state.original.right().schema(),
                state.probe_manager.clone(),
                bucket.probe.clone(),
            )?;
            let join = if bucket.sort_merge {
                rebuild_sort_merge_join(&state.original, build, probe)?
            } else {
                rebuild_bucket_join(&state.original, build, probe)?
            };
            state.current = Some(join.execute(0, Arc::clone(&state.context))?);
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
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
    hash_seed: SeededRandomState,
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
        hash_seed,
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
    hash_seed: SeededRandomState,
) -> Result<Vec<Option<RefCountedTempFile>>> {
    let timer = MetricBuilder::new(metrics).subset_time("spill_partition_time", input_partition);
    let mut writers = (0..bucket_count).map(|_| None).collect::<Vec<_>>();

    while let Some(batch) = input.next().await {
        partition_batch_with_seed(
            &batch?,
            &keys,
            bucket_count,
            &hash_seed,
            &timer,
            |bucket, batch| {
                if writers[bucket].is_none() {
                    writers[bucket] =
                        Some(manager.create_in_progress_file(&format!("{description} {bucket}"))?);
                }
                writers[bucket]
                    .as_mut()
                    .expect("writer was initialized above")
                    .append_batch(&batch)?;
                Ok(())
            },
        )?;
    }

    writers
        .into_iter()
        .map(|writer| match writer {
            Some(mut writer) => writer.finish(),
            None => Ok(None),
        })
        .collect()
}

/// Hash-partitions one batch with a seed that is independent from the upstream exchange.
/// DataFusion's `BatchPartitioner` intentionally fixes the repartition seed at zero, so the
/// Grace path performs the same grouped take locally while supplying its own seed.
fn partition_batch_with_seed<F>(
    batch: &RecordBatch,
    keys: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
    bucket_count: usize,
    hash_seed: &SeededRandomState,
    timer: &Time,
    mut emit: F,
) -> Result<()>
where
    F: FnMut(usize, RecordBatch) -> Result<()>,
{
    let _timer = timer.timer();
    let arrays = evaluate_expressions_to_arrays(keys, batch)?;
    let mut hashes = vec![0u64; batch.num_rows()];
    datafusion::common::hash_utils::create_hashes(&arrays, hash_seed.random_state(), &mut hashes)?;
    let mut indices = vec![Vec::<u32>::new(); bucket_count];
    for (row, hash) in hashes.into_iter().enumerate() {
        indices[(hash % bucket_count as u64) as usize].push(row as u32);
    }
    for (bucket, rows) in indices.into_iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let row_indices = UInt32Array::from(rows);
        let columns = batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &row_indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        emit(bucket, RecordBatch::try_new(batch.schema(), columns)?)?;
    }
    Ok(())
}

fn apply_fetch(
    fetch: Option<usize>,
    stream: SendableRecordBatchStream,
    metrics: &ExecutionPlanMetricsSet,
    partition: usize,
) -> Result<SendableRecordBatchStream> {
    match fetch {
        Some(fetch) => Ok(Box::pin(LimitStream::new(
            stream,
            0,
            Some(fetch),
            BaselineMetrics::new(metrics, partition),
        ))),
        None => Ok(stream),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinType;
    use datafusion::physical_expr::expressions::Column;

    #[test]
    fn grace_partition_uses_an_independent_seed() {
        assert_ne!(
            GRACE_HASH_SEED.seed(),
            0,
            "Grace repartition must not reuse exchange seed"
        );
        assert_ne!(
            GRACE_HASH_SEED.seed(),
            12210250226015887276u64,
            "Grace repartition must not reuse HashJoinExec seed"
        );
    }

    #[test]
    fn grace_seed_redistributes_a_four_way_exchange_partition() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let source = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(0..4096))],
        )?;

        // Reproduce one worker's input after a power-of-two (four-way) upstream exchange.
        // With the old seed reuse, every row here would land in Grace buckets congruent to
        // this worker and most buckets would remain empty.
        let exchange_seed = SeededRandomState::with_seed(0);
        let mut exchange_hashes = vec![0u64; source.num_rows()];
        datafusion::common::hash_utils::create_hashes(
            &[Arc::clone(source.column(0))],
            exchange_seed.random_state(),
            &mut exchange_hashes,
        )?;
        let worker_rows = UInt32Array::from(
            exchange_hashes
                .iter()
                .enumerate()
                .filter_map(|(row, hash)| (hash % 4 == 0).then_some(row as u32))
                .collect::<Vec<_>>(),
        );
        let worker_batch = RecordBatch::try_new(
            schema,
            vec![take(source.column(0).as_ref(), &worker_rows, None)?],
        )?;

        let metrics = ExecutionPlanMetricsSet::new();
        let timer = MetricBuilder::new(&metrics).subset_time("test_partition_time", 0);
        let keys: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> =
            vec![Arc::new(Column::new("key", 0))];
        let mut bucket_rows = [0usize; 16];
        partition_batch_with_seed(
            &worker_batch,
            &keys,
            bucket_rows.len(),
            &GRACE_HASH_SEED,
            &timer,
            |bucket, batch| {
                bucket_rows[bucket] += batch.num_rows();
                Ok(())
            },
        )?;

        assert_eq!(bucket_rows.iter().sum::<usize>(), worker_batch.num_rows());
        assert!(
            bucket_rows.iter().filter(|rows| **rows > 0).count() >= 12,
            "independent Grace seed should spread one exchange partition broadly: {bucket_rows:?}"
        );
        Ok(())
    }

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

    fn join_for_type(join_type: JoinType) -> Result<HashJoinExec> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("left_value", DataType::Int64, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("right_value", DataType::Int64, false),
        ]));
        let left = input(
            Arc::clone(&left_schema),
            (0..100).collect(),
            (0..100).map(i64::from).collect(),
        )?;
        let right = input(
            Arc::clone(&right_schema),
            (50..150).collect(),
            (50..150).map(|value| i64::from(value) * 10).collect(),
        )?;
        HashJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(Column::new("key", 0)),
                Arc::new(Column::new("key", 0)),
            )],
            None,
            &join_type,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
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

    #[tokio::test]
    async fn spills_outer_semi_and_anti_join_types() -> Result<()> {
        // Every join type must preserve HashJoinExec's null-extension and filtering semantics when
        // the build/probe inputs are split into Grace buckets. The original test covered only INNER.
        for (join_type, expected_rows, expected_columns) in [
            (JoinType::Left, 100, 4),
            (JoinType::Right, 100, 4),
            (JoinType::Full, 150, 4),
            (JoinType::LeftSemi, 50, 2),
            (JoinType::LeftAnti, 50, 2),
        ] {
            let join = join_for_type(join_type)?;
            let adaptive = SpillableHashJoinExec::new_with_build_target(&join, 512);
            let batches = adaptive
                .execute(0, Arc::new(TaskContext::default()))?
                .try_collect::<Vec<_>>()
                .await?;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected_rows,
                "unexpected row count for {join_type:?}"
            );
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.num_columns() == expected_columns),
                "unexpected schema width for {join_type:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn recursively_repartitions_hot_bucket_and_falls_back_to_sort_merge() -> Result<()> {
        // A single hot key remains in one bucket under every hash seed. The wide build payload
        // makes that bucket exceed the 1 MiB recursive target, exercising the bounded-depth SMJ
        // escape hatch instead of attempting one unbounded hash table.
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let wide = "x".repeat(2048);
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int32Array::from(vec![1; 2_000])),
                Arc::new(StringArray::from(vec![wide; 2_000])),
            ],
        )?;
        let right_batch = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["probe"])),
            ],
        )?;
        let left = MemorySourceConfig::try_new_exec(&[vec![left_batch]], left_schema, None)?;
        let right = MemorySourceConfig::try_new_exec(&[vec![right_batch]], right_schema, None)?;
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
        let adaptive = SpillableHashJoinExec::new_with_build_target(&join, 1);
        let batches = adaptive
            .execute(0, Arc::new(TaskContext::default()))?
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2_000
        );
        let metrics = adaptive.metrics().expect("adaptive join metrics");
        assert!(
            metrics
                .sum_by_name("sort_merge_fallback_count")
                .expect("fallback metric")
                .as_usize()
                > 0
        );
        Ok(())
    }
}
