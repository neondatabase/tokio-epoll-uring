#!/usr/bin/env bash
# Same quick sweep as quick-sweep.sh but with O_DIRECT instead of
# cached-IO. Working set = 256 MiB * 400 clients = 100 GiB (~10x host
# RAM, ~25x device cache → every read truly hits NAND).
# Reuses the existing 1024 MiB-per-client files in /ephemeral/direct-io/data;
# setup_files skips because md.len() >= file_size_mib.

set -euo pipefail

OUT_DIR="${OUT_DIR:-/ephemeral/quick-sweep-directio}"
RUNTIME="${RUNTIME:-10s}"
NCLIENTS="${NCLIENTS:-400}"
FILE_SIZE_MIB="${FILE_SIZE_MIB:-256}"
BLOCK_SHIFT=13
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

echo "=== sweep (direct-io, 100 GiB working set) ==="
for engine in "${ENGINES[@]}"; do
    date '+%H:%M:%S'
    echo "    $engine"
    run_one "$engine"
done
echo "=== done ==="
