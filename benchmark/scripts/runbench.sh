#!/usr/bin/env bash

set -euo pipefail
set -x

run() {
	engine="$1"
	PROCESS_URING_ON_SUBMIT=1 PROCESS_URING_ON_QUEUE_FULL=1 YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL="${FORCE_YIELD:-dontcare}" \
		RUST_LOG=warn RUST_BACKTRACE=1 \
		/tmp/christian-filesystem-workers \
		--run-duration "$RUNTIME" \
       		"$NCLIENTS" 100 13 disk-access no-validate cached-io \
               "$engine"	
	if [ "${SAVE_RESULT:-0}" = "1" ]; then
		mv benchmark.output.json "${NCLIENTS}-${FORCE_YIELD}--${engine}.output.json"
	fi
}

kbs_read() {
	iostat -o JSON /dev/nvme1n1 | jq '.sysstat.hosts[0].statistics[0].disk[0].kB_read'
}

page_cache_size_mibs() {
	cat /proc/meminfo  | grep '^Cached:' | awk '{print int($2/1024)}'
}

create_files() {
	FORCE_YIELD=0 RUNTIME=1s SAVE_RESULT=0 run std
}

ensure_workload_in_page_cache() {
	echo page cache warmup done
	while true; do
		before="$(kbs_read)"
		FORCE_YIELD=0 RUNTIME=10s SAVE_RESULT=0 run std
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
		FORCE_YIELD=0 RUNTIME=10s SAVE_RESULT=0 run std
		after="$(page_cache_size_mibs)"
		if [ "$after" -ge "${TARGET_MIN_MIBS}" ]; then
			break
		fi
	done
	echo "done"
}

NCLIENTS=400
TOTAL_IOS="1g-total-ios"
create_files
echo 3 > /proc/sys/vm/drop_caches
ensure_workload_in_page_cache
SAVE_RESULT=1 FORCE_YIELD=0 RUNTIME=20s run tokio-epoll-uring 
SAVE_RESULT=1 FORCE_YIELD=1 RUNTIME=20s run tokio-epoll-uring 

NCLIENTS=1200
create_files
echo 3 > /proc/sys/vm/drop_caches
TARGET_MIN_MIBS=$((58*1024)) ensure_page_cache_size_at_least 
SAVE_RESULT=1 FORCE_YIELD=0 RUNTIME=20s run tokio-epoll-uring 
SAVE_RESULT=1 FORCE_YIELD=1 RUNTIME=20s run tokio-epoll-uring 

echo benchmarks done
