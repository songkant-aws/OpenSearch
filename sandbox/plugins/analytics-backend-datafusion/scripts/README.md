# DataFusion PPL Parquet Profiling

This folder contains a small harness for profiling the sandbox PPL -> analytics-engine -> DataFusion path with async-profiler.

## Start OpenSearch

From the repository root:

```bash
./gradlew run \
  -PnumNodes=1 \
  -PinstalledPlugins="['arrow-base','composite-engine','parquet-data-format','analytics-engine','analytics-backend-datafusion','test-ppl-frontend']"
```

The plugin list intentionally omits `analytics-backend-lucene` so the analytics backend path resolves to DataFusion for the benchmark query.

For more detailed Rust native frames, especially on Linux EC2, build the native library with debug profile:

```bash
./gradlew run \
  -PrustDebug \
  -PnumNodes=1 \
  -PinstalledPlugins="['arrow-base','composite-engine','parquet-data-format','analytics-engine','analytics-backend-datafusion','test-ppl-frontend']"
```

Release builds also keep line-table symbols (`strip=false`), but `-PrustDebug` gives async-profiler more symbol information at the cost of lower throughput.

## Create a Parquet-Only Index

In another shell:

```bash
sandbox/plugins/analytics-backend-datafusion/scripts/profile_ppl_parquet.sh setup-index
```

The script creates an index with:

```text
index.pluggable.dataformat.enabled=true
index.pluggable.dataformat=composite
index.composite.primary_data_format=parquet
index.composite.secondary_data_formats=[]
```

That configuration writes through the composite pluggable data-format engine with Parquet as the only configured data format.

## Load Sample Data

```bash
DOCS=100000 sandbox/plugins/analytics-backend-datafusion/scripts/profile_ppl_parquet.sh load-sample
```

The sample documents include a `message` text field and a `status` keyword field. The default PPL query runs the `regex` command on `message`.

## Run the Query Once

```bash
sandbox/plugins/analytics-backend-datafusion/scripts/profile_ppl_parquet.sh run-query
```

The request is sent to `POST /_analytics/ppl` with `profile: true`, so the response includes the analytics profile and DataFusion physical-plan metrics.

## Generate a Flamegraph

```bash
PROFILE_SECONDS=120 \
PROFILE_CSTACK=dwarf \
PROFILE_OUTPUT=/tmp/datafusion-ppl-cpu.html \
sandbox/plugins/analytics-backend-datafusion/scripts/profile_ppl_parquet.sh profile
```

If `ASPROF`, `asprof`, or `profiler.sh` is not available, the script downloads async-profiler into `~/.cache/async-profiler`.
The script passes `--cstack dwarf --lib` by default so native frames from `libopensearch_native.so` / DataFusion Rust code are included when the host kernel and symbols allow it.

Useful variants:

```bash
PROFILE_EVENT=wall PROFILE_OUTPUT=/tmp/datafusion-ppl-wall.html \
  sandbox/plugins/analytics-backend-datafusion/scripts/profile_ppl_parquet.sh profile

PROFILE_EVENT=alloc PROFILE_OUTPUT=/tmp/datafusion-ppl-alloc.html \
  sandbox/plugins/analytics-backend-datafusion/scripts/profile_ppl_parquet.sh profile
```

On Linux EC2, CPU profiling may require a permissive `kernel.perf_event_paranoid` setting or running the profiler as the same user that owns the OpenSearch JVM.
If native frames are still collapsed into raw addresses, retry with `PROFILE_CSTACK=fp` and a Rust build that preserves frame pointers, or use `PROFILE_EVENT=wall` to include blocking/waiting native stacks.
