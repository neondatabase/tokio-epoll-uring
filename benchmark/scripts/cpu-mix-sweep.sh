#!/usr/bin/env bash
# Direct-IO sweep with CPU work injected after each completed read.
# Target: ~70% per-core utilization (60µs CPU work / op gets us 68%).
# Hypothesis: side-ring's per-thread poller task starves under client-CPU
# load and tail latency blows up; upstream multi-ring degrades better
# because the central Driver::turn drain is a backstop when worker-side
# Path II opportunistic drain doesn't fire.

set -euo pipefail

OUT_DIR="${OUT_DIR:-/ephemeral/cpu-mix-sweep}"
RUNTIME="${RUNTIME:-15s}"
NCLIENTS="${NCLIENTS:-400}"
FILE_SIZE_MIB="${FILE_SIZE_MIB:-256}"
BLOCK_SHIFT=13
CPU_NS="${CPU_NS:-60000}"
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
        --cpu-work-per-op-ns "$CPU_NS"
        "$NCLIENTS" "$FILE_SIZE_MIB" "$BLOCK_SHIFT"
        disk-access no-validate direct-io
    )
    case "$engine" in
        tokio-epoll-uring--no-force-yield)
            env_prefix=(env EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT=1
                EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL=1
                EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=0
                RUST_LOG=warn)
            cli=(tokio-epoll-uring)
            ;;
        tokio-epoll-uring--force-yield)
            env_prefix=(env EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT=1
                EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL=1
                EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=1
                RUST_LOG=warn)
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
    rm -f benchmark.output.json
    "${env_prefix[@]}" /usr/bin/time -v -o "rusage_${engine}.txt" \
        /ephemeral/benchmark "${args[@]}" "${cli[@]}" > "stdout_${engine}.log" 2>&1
    mv benchmark.output.json "out_${engine}.json"
}

echo "=== cpu-mix sweep (cpu_work=${CPU_NS} ns, direct-IO, 100 GiB WS) ==="
for engine in "${ENGINES[@]}"; do
    date '+%H:%M:%S'
    echo "    $engine"
    run_one "$engine"
done
echo "=== done ==="
