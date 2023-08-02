#!/usr/bin/env bash

set -euo pipefail
set -x

run() {
	engine="$1"
	common_args=(
		RUST_LOG=warn
		RUST_BACKTRACE=1
		/tmp/benchmark
		--run-duration "$RUNTIME"
		${DISABLE_STATS:-}
		"$NCLIENTS" 100 13 disk-access no-validate cached-io
	)
	case "$engine" in
		tokio-epoll-uring--no-force-yield)
			env EPOLL_URING_PROCESS_URING_ON_SUBMIT=1 \
				EPOLL_URING_PROCESS_URING_ON_QUEUE_FULL=1 \
				EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=0 \
				"${common_args[@]}" "tokio-epoll-uring"
		;;
		tokio-epoll-uring--force-yield)
			env EPOLL_URING_PROCESS_URING_ON_SUBMIT=1 \
				EPOLL_URING_PROCESS_URING_ON_QUEUE_FULL=1 \
				EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL=1 \
				"${common_args[@]}" "tokio-epoll-uring"
		;;
		tokio-spawn-blocking*)
			env "${common_args[@]}" "tokio-spawn-blocking" "${engine#tokio-spawn-blocking--}"
		;;
		*)
			env "${common_args[@]}" "$engine"
			;;
	esac
	if [ "${SAVE_RESULT:-0}" = "1" ]; then
		mv benchmark.output.json "${NCLIENTS}--${engine}.output.json"
	fi
}

kbs_read() {
	iostat -o JSON /dev/nvme1n1 | jq '.sysstat.hosts[0].statistics[0].disk[0].kB_read'
}

page_cache_size_mibs() {
	cat /proc/meminfo  | grep '^Cached:' | awk '{print int($2/1024)}'
}

create_files() {
	EPOLL_URING_FORCE_YIELD=0 RUNTIME=1s SAVE_RESULT=0 DISABLE_STATS=--disable-stats run std
}

ensure_workload_in_page_cache() {
	echo page cache warmup done
	while true; do
		before="$(kbs_read)"
		EPOLL_URING_FORCE_YIELD=0 RUNTIME=10s SAVE_RESULT=0 DISABLE_STATS=--disable-stats run std
		after="$(kbs_read)"
		delta="$((after - before))"
		if [ "$delta" -lt 10 ]; then
			break
		fi
	done
	echo page cache warmup done
}

ensure_page_cache_size_at_least() {
	echo "loading up page cache to size ${TARGET_MIN_MIBS}"
	while true; do
		EPOLL_URING_FORCE_YIELD=0 RUNTIME=10s SAVE_RESULT=0 DISABLE_STATS=--disable-stats run std
		after="$(page_cache_size_mibs)"
		if [ "$after" -ge "${TARGET_MIN_MIBS}" ]; then
			break
		fi
	done
	echo "done"
}

compare_engines=(
	tokio-epoll-uring--no-force-yield
	tokio-epoll-uring--force-yield
	tokio-spawn-blocking--512
	tokio-uring
)

NCLIENTS=400
create_files
echo 3 > /proc/sys/vm/drop_caches
ensure_workload_in_page_cache
for engine in "${compare_engines[@]}"; do
	date # for debugging
	SAVE_RESULT=1 RUNTIME="500k-ios-per-client" run "$engine"
done
unset NCLIENTS

NCLIENTS=1200
create_files
echo 3 > /proc/sys/vm/drop_caches
TARGET_MIN_MIBS=$((58*1024)) ensure_page_cache_size_at_least
for engine in "${compare_engines[@]}"; do
	date # for debugging
	SAVE_RESULT=1 RUNTIME="12k-ios-per-client" run "$engine"
done
unset NCLIENTS

echo benchmarks done
