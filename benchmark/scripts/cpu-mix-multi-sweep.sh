#!/usr/bin/env bash
# Direct-IO 4-engine comparison at multiple CPU utilization targets.
# no-force-yield is dropped (we don't use it; the previous run showed
# it hangs at shutdown under CPU load).
set -euo pipefail

OUT_DIR="${OUT_DIR:-/ephemeral/cpu-mix-multi}"
RUNTIME="${RUNTIME:-15s}"
NCLIENTS="${NCLIENTS:-400}"
FILE_SIZE_MIB="${FILE_SIZE_MIB:-256}"
BLOCK_SHIFT=13
ENGINES=(
    tokio-epoll-uring--force-yield
    tokio-epoll-uring-upstream
    tokio-spawn-blocking--512
    tokio-uring
)
# (utilization label, cpu_ns)
LEVELS=(
    "30:21000"
    "70:60000"
    "90:95000"
    "95:110000"
)

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

run_one() {
    local engine="$1" cpu_ns="$2" tag="$3"
    local args=(
        --run-duration "$RUNTIME"
        --cpu-work-per-op-ns "$cpu_ns"
        "$NCLIENTS" "$FILE_SIZE_MIB" "$BLOCK_SHIFT"
        disk-access no-validate direct-io
    )
    case "$engine" in
        tokio-epoll-uring--force-yield)
            env_prefix=(env
                EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT=1
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
    "${env_prefix[@]}" /usr/bin/time -v -o "rusage_${tag}_${engine}.txt" \
        /ephemeral/benchmark "${args[@]}" "${cli[@]}" > "stdout_${tag}_${engine}.log" 2>&1
    mv benchmark.output.json "out_${tag}_${engine}.json"
}

for level in "${LEVELS[@]}"; do
    util="${level%%:*}"
    ns="${level##*:}"
    echo "=== ${util}% per-core target (cpu_work=${ns} ns) ==="
    for engine in "${ENGINES[@]}"; do
        date '+%H:%M:%S'
        echo "    $engine"
        run_one "$engine" "$ns" "$util"
    done
done
echo "=== done ==="
