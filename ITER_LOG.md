# TEU upstream backend — perf iteration log

Append-only log driven by the autonomous overnight run. Each iteration is one entry; entries flow top-to-bottom in chronological order. The bottom of the file holds the **rolling hypothesis tree** — the agent's current working theory of where the perf gap comes from and what to attack next.

## Re-orientation protocol (read after context compaction)

```
tail -200 ITER_LOG.md
jj log --no-graph -T 'change_id.short() ++ " " ++ description.first_line() ++ "\n"' -r '..@' 2>&1 | head -30
ls -lt /ephemeral/perf/*.top.txt 2>/dev/null | head -10
cat /home/christian.schwarz/.claude/projects/-home-christian-schwarz-tokio-epoll-uring/memory/MEMORY.md
```

Then resume from "Active hypothesis tree" at the bottom.

## Durable artifacts

- Code: jj-committed on `tokio-upstream-backend` (TEU repo) and `expose-uring-ops` (tokio repo). Never pushed.
- Perf profiles: `/ephemeral/perf/<engine>__<label>_<ts>.{data,folded,svg,top.txt,meta.txt}`
- Benchmark JSONs: `/ephemeral/direct-io/{NCLIENTS}--{engine}.output.json` for the saved sweeps; per-iteration probe runs go in `/ephemeral/perf/` alongside the perf data.
- Files reused: `data/client_<N>.data`, 1024 MiB each, 400 of them already created. With `FILE_SIZE_MIB=256` (default in perf-iter.sh) only the first 256 MiB of each is read → 100 GiB working set. Setup is `O(stat)` on subsequent runs.

## Iteration cycle template

For each iteration, append a block like:

```
### ITER N — <one-line hypothesis>
- date:           <yyyy-mm-dd HH:MM>
- hypothesis:     <why this should help and roughly by how much>
- change:         <file:line summary, or "(profiling only)">
- profile tags:   <tag(s) emitted by perf-iter.sh>
- bench delta (vs ITER 0 baseline): IOPS X% , p50 Yµs, p99 Zµs
- bench delta (vs prev ITER):       IOPS X% , p50 Yµs, p99 Zµs
- conclusion:     accepted | rejected | partial | needs follow-up
- commit:         <jj change_id short> (or "abandoned")
- followups:      <new hypotheses spawned, if any>
```

---

### ITER 0 — baseline profiles + benchmark numbers (no change yet)
- date:           2026-05-21 23:25
- hypothesis:     —
- change:         (profiling only)
- profile tags:   `tokio-epoll-uring-upstream__iter0_20260521-232524`, `tokio-epoll-uring--no-force-yield__iter0_20260521-232551`
- bench (400 clients, 256 MiB/client = 100 GiB, O_DIRECT, 15s):
  - side-ring no-force-yield: **333,062 IOPS** · MiB/s 2,602 · p50 1,124 µs · p99 2,202 µs · p99.9 2,654 µs · p99.99 5,968 µs · p99.999 9,855 µs · max 10,387 µs
  - upstream:                 **123,726 IOPS** · MiB/s 967   · p50 3,090 µs · p99 7,217 µs · p99.9 9,069 µs · p99.99 10,666 µs · p99.999 11,878 µs · max 12,911 µs
  - **gap: 2.69× IOPS, 2.75× p50, 3.28× p99**
- key perf findings:
  - upstream burns **26.37% CPU** in `finish_task_switch → futex_wait → schedule` (futex sleeps under contention)
  - upstream-only hot leaves: `parking_lot::RawMutex::lock_slow` +1.23M samples, `unlock_slow` +710k, kernel `__lock_text_start` +8.2B samples
  - Mutex contention path: ~14% in `register_op` (submit holds lock), ~12% in `Op::poll` (every poll re-locks to inspect Lifecycle)
  - side-ring's 10.6% finish_task_switch is **worker park** (idle), not contention
- conclusion:     **primary bottleneck = the single shared `Mutex<UringContext>` in tokio's io_uring driver.** Every SQE submit, every CQE dispatch, every Op poll re-locks it. With 400 async clients on a multi-thread runtime, this serialises the whole IO path.
- commit:         (no code change)
- followups:
  - H1: jemalloc — orthogonal control, applies to all engines (Christian requested). Cheap.
  - H2: bump `DEFAULT_RING_SIZE` in tokio from 256 → larger (cheap, may reduce submit-side contention via fewer dispatch_completions calls under EBUSY).
  - H3: skip per-submit `is_supported(opcode).await` after first hit (cheap, removes one OnceCell::get + async overhead per op).
  - H4: reduce `dispatch_completions` lock hold time (medium, drain CQEs into local Vec, drop lock, then wake wakers).
  - H5: reduce `Op::poll` lock acquisitions via atomic state hint (medium-hard).
  - H6: **per-worker io_uring** — architectural, large change to tokio. The actual win path.

---

### ITER 1 — jemalloc as global allocator (REJECTED, blocked by pre-existing UB)
- date:           2026-05-21 23:30
- hypothesis:     Christian requested jemalloc to match PageServer's production allocator. Should be net-positive or neutral; uniformly applied to all engines so comparisons stay fair.
- change:         `benchmark/Cargo.toml` add `tikv-jemallocator = "0.6"` dep; `benchmark/src/main.rs` install `#[global_allocator]`.
- result:         **SIGSEGV at shutdown.** Every engine SEGVs (exit 139) right after the last "Client X stopping" log line, before the JSON output is written.
- root cause:     pre-existing UB in benchmark allocator usage. Every engine has:
    ```
    let ptr = std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap());
    Vec::from_raw_parts(ptr, 0, block_size)
    ```
    `Vec<u8>::Drop` frees with `Layout::array::<u8>(block_size)` (align=1), not the original alignment (= block_size). System allocator silently forgives this; jemalloc rejects it as a heap corruption and aborts.
- conclusion:     **rejected for now**; would also reject without jemalloc if you ran under valgrind/MIRI. Logged as a follow-up; jemalloc dep stays in `Cargo.toml` with a clear comment in `main.rs` describing the unblock path.
- commit:         (jemalloc dep retained, allocator install commented out)
- followups:
  - **FU-1:** introduce an `AlignedBuf` wrapper that owns the aligned alloc separately from the Vec view, fix every engine (`benchmark/src/engines/*.rs`), then re-enable jemalloc. Probably 1-2h of work; not on the critical path for the perf demo.

---

### ITER 2 — skip `is_supported(opcode).await` after first hit per opcode
- date:           2026-05-21 23:34
- hypothesis:     Every SQE submit awaits `tokio::io_uring::is_supported(opcode)`. After first init, the OnceCell is already populated so the function returns "instantly" — but it's still an async fn that polls a future and walks the get_or_try_init path. Caching the per-opcode result in an AtomicU64 bitmap in TEU's adapter should skip ~100ns/op + remove the future state machine. Expected: small IOPS gain (1-3%), should be visible in the perf top.
- change:         `tokio-epoll-uring/src/system/backend_upstream.rs:1-25` adds a process-wide `static OPCODE_SUPPORTED_BITMAP: AtomicU64` and shortcuts the probe in `execute_op_upstream` when the bit is already set.
- profile tags:   `tokio-epoll-uring-upstream__iter2_20260521-233429`
- bench delta (vs ITER 0): IOPS 123,726 → **124,417** = +0.6% (within run-to-run noise). p50/p99/tail unchanged.
- perf delta:     overall lock-related leaves slightly down (`unlock_slow` -150k, `register_op` -112k, `futex_wake` -252k samples) — confirming the await *did* cost some CPU. But the net IOPS effect is within noise because we're contention-bound regardless.
- conclusion:     **accepted** (correct change, makes the hot path simpler), but **not** the win we're looking for. The real bottleneck is the Mutex itself, not the probe-per-op.
- commit:         next
- followups:      same as ITER 0 plus H6 (multi-ring) climbs to top priority.

---

(iterations continue below)
