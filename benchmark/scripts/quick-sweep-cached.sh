#!/usr/bin/env bash
# Quick small-working-set sweep with CPU usage capture.
# 400 clients * 10 MiB/client = ~4 GiB working set (fits in 123 GiB RAM).
# Cached-IO so all reads hit page cache after warmup → exposes
# per-engine scheduler/dispatch overhead with NVMe service time
# removed.

set -euo pipefail

OUT_DIR="${OUT_DIR:-/ephemeral/quick-sweep}"
RUNTIME="${RUNTIME:-8s}"
NCLIENTS="${NCLIENTS:-400}"
FILE_SIZE_MIB="${FILE_SIZE_MIB:-10}"
BLOCK_SHIFT=13   # 8 KiB
ENGINES=(
    tokio-epoll-uring--no-force-yield
    tokio-epoll-uring--force-yield
    tokio-epoll-uring-upstream
    tokio-spawn-blocking--512
    tokio-uring
)

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

run_one() {
    local engine="$1"
    local args=(
        --run-duration "$RUNTIME"
        "$NCLIENTS" "$FILE_SIZE_MIB" "$BLOCK_SHIFT"
        disk-access no-validate cached-io
    )
    case "$engine" in
        tokio-epoll-uring--no-force-yield)
            env_prefix=(
                env
                EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT=1
                EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL=1
                EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=0
                RUST_LOG=warn
            )
            cli=(tokio-epoll-uring)
            ;;
        tokio-epoll-uring--force-yield)
            env_prefix=(
                env
                EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT=1
                EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL=1
                EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=1
                RUST_LOG=warn
            )
            cli=(tokio-epoll-uring)
            ;;
        tokio-spawn-blocking--*)
            env_prefix=(env RUST_LOG=warn)
            cli=(tokio-spawn-blocking "${engine#tokio-spawn-blocking--}")
            ;;
        *)
            env_prefix=(env RUST_LOG=warn)
            cli=("$engine")
            ;;
    esac

    # /usr/bin/time -v writes to stderr; capture into rusage_${engine}.txt
    rm -f benchmark.output.json
    "${env_prefix[@]}" /usr/bin/time -v -o "rusage_${engine}.txt" \
        /ephemeral/benchmark "${args[@]}" "${cli[@]}" > "stdout_${engine}.log" 2>&1
    mv benchmark.output.json "out_${engine}.json"
}

echo "=== warmup (std engine, fills page cache) ==="
# Create + warm files. Std engine reads each client's file once → primes the page cache.
env RUST_LOG=warn /ephemeral/benchmark --run-duration 3s \
    "$NCLIENTS" "$FILE_SIZE_MIB" "$BLOCK_SHIFT" \
    disk-access no-validate cached-io std > /dev/null 2>&1 || true
env RUST_LOG=warn /ephemeral/benchmark --run-duration 3s \
    "$NCLIENTS" "$FILE_SIZE_MIB" "$BLOCK_SHIFT" \
    disk-access no-validate cached-io std > /dev/null 2>&1 || true
rm -f benchmark.output.json

echo "=== sweep ==="
for engine in "${ENGINES[@]}"; do
    date '+%H:%M:%S'
    echo "    $engine"
    run_one "$engine"
done
echo "=== done ==="
