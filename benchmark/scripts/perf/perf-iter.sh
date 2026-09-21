#!/usr/bin/env bash
# Profile one engine of the benchmark under perf and emit a flamegraph.
#
# Usage:
#   perf-iter.sh <engine> [<label>] [-- extra benchmark args]
#
# Examples:
#   # baseline upstream profile
#   perf-iter.sh tokio-epoll-uring-upstream baseline
#
#   # after an edit, re-profile with a label
#   perf-iter.sh tokio-epoll-uring-upstream after-fix-1
#
#   # side-ring reference for comparison
#   perf-iter.sh tokio-epoll-uring--no-force-yield ref
#
# Env knobs:
#   PERF_DIR=/ephemeral/perf       output dir
#   NCLIENTS=400                   client count for benchmark
#   FILE_SIZE_MIB=1024             per-client file size
#   BLOCK_SHIFT=13                 2^13 = 8 KiB blocks
#   RUNTIME=15s                    benchmark wall-clock
#   DISK_ACCESS_KIND=direct-io     direct-io | cached-io
#   FREQ=999                       perf sampling Hz
#   BUILD=1                        re-build before running (0 to skip)

set -euo pipefail

ENGINE="${1:?usage: $0 <engine> [<label>] [-- extra args]}"
shift || true
LABEL="${1:-}"
if [ -n "$LABEL" ] && [ "$1" != "--" ]; then
    shift
fi
if [ "${1:-}" = "--" ]; then
    shift
fi
EXTRA_ARGS=("$@")

PERF_DIR="${PERF_DIR:-/ephemeral/perf}"
NCLIENTS="${NCLIENTS:-400}"
FILE_SIZE_MIB="${FILE_SIZE_MIB:-256}"   # 256 MiB × NCLIENTS (400) = 100 GiB working set
BLOCK_SHIFT="${BLOCK_SHIFT:-13}"
RUNTIME="${RUNTIME:-15s}"
DISK_ACCESS_KIND="${DISK_ACCESS_KIND:-direct-io}"
FREQ="${FREQ:-999}"
BUILD="${BUILD:-1}"

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)"

# Per-run tag: <engine>[__label][_<timestamp>]
TS="$(date +%Y%m%d-%H%M%S)"
TAG="${ENGINE}"
[ -n "$LABEL" ] && TAG="${TAG}__${LABEL}"
TAG="${TAG}_${TS}"

mkdir -p "$PERF_DIR"
PERF_DATA="$PERF_DIR/${TAG}.data"
FOLDED="$PERF_DIR/${TAG}.folded"
FLAME="$PERF_DIR/${TAG}.svg"
TOP_TXT="$PERF_DIR/${TAG}.top.txt"
META="$PERF_DIR/${TAG}.meta.txt"

# --- 1. build ---
if [ "$BUILD" = "1" ]; then
    echo "[perf-iter] building benchmark..." >&2
    (cd "$REPO_ROOT" && cargo build --release -p benchmark >&2)
    cp "$REPO_ROOT/target/release/benchmark" /ephemeral/benchmark
fi

# --- 2. translate engine name to CLI subcommand + env vars ---
common_args=(
    --run-duration "$RUNTIME"
    --disable-stats
    "$NCLIENTS" "$FILE_SIZE_MIB" "$BLOCK_SHIFT"
    disk-access no-validate "$DISK_ACCESS_KIND"
)

declare -a engine_env
declare -a engine_cli

case "$ENGINE" in
    tokio-epoll-uring--no-force-yield)
        engine_env=(
            EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT=1
            EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL=1
            EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=0
        )
        engine_cli=(tokio-epoll-uring)
        ;;
    tokio-epoll-uring--force-yield)
        engine_env=(
            EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT=1
            EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL=1
            EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=1
        )
        engine_cli=(tokio-epoll-uring)
        ;;
    tokio-spawn-blocking--*)
        engine_env=()
        engine_cli=(tokio-spawn-blocking "${ENGINE#tokio-spawn-blocking--}")
        ;;
    *)
        engine_env=()
        engine_cli=("$ENGINE")
        ;;
esac

# --- 3. record ---
echo "[perf-iter] recording $TAG (engine=$ENGINE runtime=$RUNTIME nclients=$NCLIENTS)..." >&2
{
    echo "tag:           $TAG"
    echo "engine:        $ENGINE"
    echo "nclients:      $NCLIENTS"
    echo "file_size_mib: $FILE_SIZE_MIB"
    echo "block_shift:   $BLOCK_SHIFT"
    echo "runtime:       $RUNTIME"
    echo "disk_access:   $DISK_ACCESS_KIND"
    echo "freq:          $FREQ"
    echo "engine_env:    ${engine_env[*]}"
    echo "engine_cli:    ${engine_cli[*]}"
    echo "extra_args:    ${EXTRA_ARGS[*]}"
} > "$META"

cd /ephemeral
env "${engine_env[@]}" RUST_LOG=warn \
    perf record \
        -F "$FREQ" \
        --call-graph fp \
        -o "$PERF_DATA" \
        --                                                                      \
        /ephemeral/benchmark \
            "${common_args[@]}" \
            "${engine_cli[@]}" \
            "${EXTRA_ARGS[@]}"

# --- 4. fold + flamegraph ---
echo "[perf-iter] folding stacks..." >&2
perf script -i "$PERF_DATA" --no-inline 2>/dev/null \
    | inferno-collapse-perf \
    > "$FOLDED"

echo "[perf-iter] generating flamegraph -> $FLAME" >&2
inferno-flamegraph --title "$TAG ($ENGINE, $NCLIENTS clients, $DISK_ACCESS_KIND, ${RUNTIME})" \
    --countname samples \
    < "$FOLDED" \
    > "$FLAME"

# --- 5. top hot functions report ---
echo "[perf-iter] top hot functions:" >&2
{
    echo "=== top 30 hot functions (self-time) ==="
    awk '{ n=split($0, parts, ";"); leaf=parts[n]; count=$NF;
           # strip count suffix from leaf
           gsub(/ [0-9]+$/, "", leaf);
           # actual sample count is last token of full line
           # easier: re-parse
        }' /dev/null
    perf report -i "$PERF_DATA" --stdio --no-children --sort symbol \
        --percent-limit 0.3 2>/dev/null | head -60
    echo
    echo "=== top 30 hot stacks (children) ==="
    perf report -i "$PERF_DATA" --stdio --sort dso,symbol \
        --percent-limit 0.5 2>/dev/null | head -50
} > "$TOP_TXT"

echo "[perf-iter] done. artifacts:" >&2
ls -la "$PERF_DATA" "$FOLDED" "$FLAME" "$TOP_TXT" "$META" >&2
echo "tag=$TAG"
