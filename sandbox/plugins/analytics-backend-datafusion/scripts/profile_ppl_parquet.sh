#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
INDEX="${INDEX:-df_parquet_profile}"
SHARDS="${SHARDS:-1}"
REPLICAS="${REPLICAS:-0}"
DOCS="${DOCS:-10000}"
BULK_BATCH="${BULK_BATCH:-1000}"
PROFILE_SECONDS="${PROFILE_SECONDS:-60}"
PROFILE_EVENT="${PROFILE_EVENT:-cpu}"
PROFILE_CSTACK="${PROFILE_CSTACK:-dwarf}"
PROFILE_OUTPUT="${PROFILE_OUTPUT:-${ROOT_DIR}/build/reports/datafusion-ppl-${PROFILE_EVENT}.html}"
ASYNC_PROFILER_VERSION="${ASYNC_PROFILER_VERSION:-4.4}"
ASYNC_PROFILER_CACHE="${ASYNC_PROFILER_CACHE:-${HOME}/.cache/async-profiler}"
PPL_QUERY="${PPL_QUERY:-source=${INDEX} | regex message='error.*datafusion' | stats count() by status}"
QUERY_ITERATIONS="${QUERY_ITERATIONS:-0}"

usage() {
  cat <<'USAGE'
Usage:
  profile_ppl_parquet.sh start-command
  profile_ppl_parquet.sh setup-index
  profile_ppl_parquet.sh load-sample
  profile_ppl_parquet.sh run-query
  profile_ppl_parquet.sh profile

Environment:
  OPENSEARCH_URL        OpenSearch endpoint, default http://localhost:9200
  INDEX                 Target index, default df_parquet_profile
  DOCS                  Sample docs to load, default 10000
  PROFILE_SECONDS       async-profiler duration, default 60
  PROFILE_EVENT         async-profiler event, default cpu
  PROFILE_OUTPUT        Flamegraph HTML output path
  PPL_QUERY             PPL query to run under profile
  ASPROF                Path to asprof/profiler.sh; if absent, this script downloads async-profiler

The generated index is parquet-only:
  index.pluggable.dataformat.enabled = true
  index.pluggable.dataformat = composite
  index.composite.primary_data_format = parquet
  index.composite.secondary_data_formats = []
USAGE
}

curl_json() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" "${OPENSEARCH_URL}${path}" -H 'Content-Type: application/json' --data-binary "$body"
  else
    curl -fsS -X "$method" "${OPENSEARCH_URL}${path}"
  fi
}

start_command() {
  cat <<'CMD'
./gradlew run \
  -PnumNodes=1 \
  -PinstalledPlugins="['arrow-base','arrow-flight-rpc','analytics-engine','composite-engine','analytics-backend-lucene','parquet-data-format','analytics-backend-datafusion','test-ppl-frontend']"
CMD
}

setup_index() {
  curl_json PUT "/_cluster/settings" '{
    "persistent": {
      "cluster.composite.primary_data_format": "parquet",
      "cluster.composite.secondary_data_formats": [],
      "cluster.restrict.composite.dataformat": true
    }
  }'
  echo

  curl_json DELETE "/${INDEX}" >/dev/null 2>&1 || true
  curl_json PUT "/${INDEX}" "{
    \"settings\": {
      \"number_of_shards\": ${SHARDS},
      \"number_of_replicas\": ${REPLICAS},
      \"index.pluggable.dataformat.enabled\": true,
      \"index.pluggable.dataformat\": \"composite\",
      \"index.composite.primary_data_format\": \"parquet\",
      \"index.composite.secondary_data_formats\": []
    },
    \"mappings\": {
      \"properties\": {
        \"message\": { \"type\": \"keyword\", \"index\": false },
        \"status\": { \"type\": \"keyword\", \"index\": false },
        \"value\": { \"type\": \"long\" },
        \"ts\": { \"type\": \"date\" }
      }
    }
  }"
  echo
}

load_sample() {
  local tmp
  tmp="$(mktemp)"
  local loaded=0

  while (( loaded < DOCS )); do
    : > "$tmp"
    local end=$((loaded + BULK_BATCH))
    if (( end > DOCS )); then
      end="$DOCS"
    fi

    local i
    for ((i = loaded; i < end; i++)); do
      printf '{"index":{"_index":"%s"}}\n' "$INDEX" >> "$tmp"
      if (( i % 10 == 0 )); then
        printf '{"message":"error code=%s component=datafusion regex target payload-%s","status":"error","value":%s,"ts":"2026-01-01T00:00:00Z"}\n' "$i" "$i" "$i" >> "$tmp"
      else
        printf '{"message":"ok component=datafusion payload-%s","status":"ok","value":%s,"ts":"2026-01-01T00:00:00Z"}\n' "$i" "$i" >> "$tmp"
      fi
    done

    local bulk_response
    bulk_response="$(curl -fsS -X POST "${OPENSEARCH_URL}/_bulk?refresh=false" -H 'Content-Type: application/x-ndjson' --data-binary "@${tmp}")"
    if [[ "$bulk_response" == *'"errors":true'* || "$bulk_response" == *'"errors": true'* ]]; then
      echo "$bulk_response" >&2
      rm -f "$tmp"
      exit 1
    fi
    loaded="$end"
    echo "loaded ${loaded}/${DOCS}"
  done

  rm -f "$tmp"
  curl_json POST "/${INDEX}/_refresh" >/dev/null
  echo "sample data loaded into ${INDEX}"
}

run_query() {
  curl_json POST "/_analytics/ppl" "{
    \"query\": \"${PPL_QUERY//\"/\\\"}\",
    \"profile\": true
  }"
  echo
}

resolve_asprof() {
  if [[ -n "${ASPROF:-}" && -x "${ASPROF}" ]]; then
    echo "$ASPROF"
    return
  fi

  if command -v asprof >/dev/null 2>&1; then
    command -v asprof
    return
  fi

  if command -v profiler.sh >/dev/null 2>&1; then
    command -v profiler.sh
    return
  fi

  local os arch platform archive dir url
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "$arch" in
    x86_64 | amd64) arch="x64" ;;
    aarch64 | arm64) arch="arm64" ;;
    *) echo "Unsupported async-profiler arch: ${arch}" >&2; exit 1 ;;
  esac
  if [[ "$os" != "linux" && "$os" != "macos" && "$os" != "darwin" ]]; then
    echo "Unsupported async-profiler OS: ${os}" >&2
    exit 1
  fi
  [[ "$os" == "darwin" ]] && os="macos"

  if [[ "$os" == "macos" ]]; then
    platform="macos"
    archive="${ASYNC_PROFILER_CACHE}/async-profiler-${ASYNC_PROFILER_VERSION}-${platform}.zip"
    url="https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/async-profiler-${ASYNC_PROFILER_VERSION}-${platform}.zip"
  else
    platform="${os}-${arch}"
    archive="${ASYNC_PROFILER_CACHE}/async-profiler-${ASYNC_PROFILER_VERSION}-${platform}.tar.gz"
    url="https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/async-profiler-${ASYNC_PROFILER_VERSION}-${platform}.tar.gz"
  fi
  dir="${ASYNC_PROFILER_CACHE}/async-profiler-${ASYNC_PROFILER_VERSION}-${platform}"

  if [[ ! -x "${dir}/bin/asprof" && ! -x "${dir}/profiler.sh" ]]; then
    mkdir -p "$ASYNC_PROFILER_CACHE"
    echo "Downloading async-profiler ${ASYNC_PROFILER_VERSION} from ${url}" >&2
    if ! curl -fL "$url" -o "$archive"; then
      echo "Failed to download async-profiler from ${url}" >&2
      exit 1
    fi
    if [[ "$archive" == *.zip ]]; then
      if ! unzip -q -o "$archive" -d "$ASYNC_PROFILER_CACHE"; then
        echo "Failed to extract ${archive}" >&2
        exit 1
      fi
    else
      if ! tar -xzf "$archive" -C "$ASYNC_PROFILER_CACHE"; then
        echo "Failed to extract ${archive}" >&2
        exit 1
      fi
    fi
  fi

  if [[ -x "${dir}/bin/asprof" ]]; then
    echo "${dir}/bin/asprof"
  elif [[ -x "${dir}/profiler.sh" ]]; then
    echo "${dir}/profiler.sh"
  else
    echo "Could not find asprof/profiler.sh under ${dir}" >&2
    exit 1
  fi
}

opensearch_pid() {
  if [[ -n "${OPENSEARCH_PID:-}" ]]; then
    echo "$OPENSEARCH_PID"
    return
  fi
  pgrep -f 'org.opensearch.bootstrap.OpenSearch' | head -n 1
}

profile_query() {
  local pid asprof iterations
  pid="$(opensearch_pid)"
  if [[ -z "$pid" ]]; then
    echo "Could not find OpenSearch PID. Set OPENSEARCH_PID explicitly." >&2
    exit 1
  fi

  asprof="$(resolve_asprof)"
  mkdir -p "$(dirname "$PROFILE_OUTPUT")"

  echo "Profiling PID ${pid} for ${PROFILE_SECONDS}s; output: ${PROFILE_OUTPUT}"
  "$asprof" -d "$PROFILE_SECONDS" -e "$PROFILE_EVENT" --cstack "$PROFILE_CSTACK" --lib -t -f "$PROFILE_OUTPUT" "$pid" &
  local profiler_pid=$!
  sleep 2

  iterations="$QUERY_ITERATIONS"
  if (( iterations > 0 )); then
    local i
    for ((i = 0; i < iterations; i++)); do
      run_query >/dev/null
    done
  else
    while kill -0 "$profiler_pid" >/dev/null 2>&1; do
      run_query >/dev/null || true
    done
  fi

  wait "$profiler_pid"
  echo "flamegraph written to ${PROFILE_OUTPUT}"
}

case "${1:-}" in
  start-command) start_command ;;
  setup-index) setup_index ;;
  load-sample) load_sample ;;
  run-query) run_query ;;
  profile) profile_query ;;
  -h | --help | help | "") usage ;;
  *) echo "Unknown command: $1" >&2; usage >&2; exit 1 ;;
esac
