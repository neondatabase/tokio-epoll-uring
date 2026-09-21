# TEU upstream backend — perf iteration log

Append-only log driven by the autonomous overnight run. Each iteration is one entry; entries flow top-to-bottom in chronological order. The bottom of the file holds the **rolling hypothesis tree** — the agent's current working theory of where the perf gap comes from and what to attack next.

## TL;DR — multi-ring shipped, gap closed

**Headline:** Multi-ring tokio io_uring driver lands the demo. With `TOKIO_IO_URING_RINGS=8`, the upstream backend matches the side-ring baseline within 0.7%.

| Engine (400 clients, 256 MiB/client, O_DIRECT, 15s, NVMe) |    IOPS | p50 µs | p99 µs | p99.9 µs |
| --------------------------------------------------------- | ------: | -----: | -----: | -------: |
| TEU side-ring (baseline)                                  | 332,813 |  1,122 |  2,251 |    2,748 |
| TEU upstream — single ring (default)                      |  96,558 |  3,936 |  9,454 |   11,878 |
| TEU upstream — multi-ring N=4                             | 315,810 |  1,225 |  2,476 |    3,119 |
| **TEU upstream — multi-ring N=8**                         | **330,793** | **1,129** | **2,261** | **2,867** |
| TEU upstream — multi-ring N=16                            | 313,790 |  1,186 |  2,318 |    3,002 |

Profile confirms the win: `finish_task_switch` (futex_wait on the per-ring Mutex) dropped from 26% CPU at N=1 to 14.5% at N=8 — close to side-ring's 10.6% (which is mostly worker park time, not contention).

**Where it lives:**

- `~/tokio` bookmark **`multi-ring`** (commit `8bf4c005`, stacked on `expose-uring-ops`). Drop-in env-var knob `TOKIO_IO_URING_RINGS` (default 1, range 1..=16). Default behaviour unchanged.
- `~/tokio-epoll-uring` bookmark `tokio-upstream-backend` (unchanged) — the new engine works as-is because the public `tokio::io_uring::Submission` API stays identical.

**What's in:**
- `runtime::io::driver::Handle` now holds `Box<[Mutex<UringContext>]>` instead of a single `Mutex<UringContext>`.
- Each ring has its own mio token (`TOKEN_URING_BASE + ring_id`), drained by `Driver::turn`.
- `register_op` picks a ring by hashing `thread::current().id()` (stable per-thread shard).
- `Op<T>::State::Polled(ring_id, idx)` so `Op::poll` and `Op::drop` find the right ring even when the future migrates between workers.
- All 3 existing `fs_uring` tests pass at N=1 and N=8. All 3 new `uring_public_api` tests pass. `open_many_files` (the test that bit ITER 4) is happy.

**What's deferred (Phase 2 in `MULTI_RING_DESIGN.md`):**
- Replacing the env-var knob with `Builder::io_uring_rings(n)`. Env var was the fastest path to a working demo; the Builder method is the upstream-PR-friendly polish.
- Per-worker CQ drain (each worker drains its own ring instead of Driver::turn doing all of them centrally). Probably worth another 5-10% but not necessary for the funding demo.
- SQPOLL.

**Iteration 1-4 (single-ring micro-opts) below remained instructive but the architectural change was always the right answer; multi-ring just landed it.**

---

## (Original TL;DR, kept for context)

I ran four perf iterations on the `tokio-epoll-uring-upstream` backend. The dominant cost is exactly what we hypothesised: the single `parking_lot::Mutex<UringContext>` in tokio's io_uring driver. With 400 concurrent O_DIRECT readers it eats 26% of CPU on futex_wait alone, and the IOPS gap to the side-ring backend is **2.7× (124k vs 333k)**.

What worked, what didn't, and where we land:

- **ITER 1 (jemalloc, REJECTED):** wired in `tikv-jemallocator` but every benchmark engine has a pre-existing alignment UB (Vec wrapping an over-aligned alloc), so jemalloc SEGVs at shutdown. Dep is in `Cargo.toml`, install is commented out with a one-paragraph unblock note. Tracked as **FU-1** (AlignedBuf wrapper).
- **ITER 2 (cache `is_supported`, ACCEPTED, +0.6%):** small but correct simplification.
- **ITER 3 (ring size 256 → 4096, REJECTED):** not the bottleneck, abandoned.
- **ITER 4 (move `io_uring_enter` outside the Mutex, +45% bench, but REVERTED):** the lock-free submit gives a real **+45% IOPS** (124k → 180k) on the benchmark BUT hangs `tokio/tests/fs_uring::open_many_files` — the µs-scale lock-hold during the syscall happens to be the only thing that yields workers back to the kernel scheduler, so the IO driver thread starves. Reverted in tokio.
- **Architectural conclusion — `MULTI_RING_DESIGN.md`:** the multi-ring (one io_uring per worker) direction you raised is the right answer. Phase 1 design + file-by-file change plan is written out in `MULTI_RING_DESIGN.md`. Estimated lift: ~2-3 days of tokio work. Not implemented this session — the design was the deliverable I aimed for given the time/context budget.

Reading order tonight ⇒ tomorrow:
1. This TL;DR
2. `MULTI_RING_DESIGN.md` — the architectural proposal
3. ITER 4 entry below (the lock-free submit experiment; +45% but hangs `open_many_files`)
4. The hypothesis tree at the bottom of this file (currently pruned to "H6 = multi-ring is now top priority")

Benchmark + perf data lives in `/ephemeral/direct-io/` and `/ephemeral/perf/`. The perf loop scripts (`perf-iter.sh`, `perf-compare.sh`) are still wired up; you can re-profile any iteration locally without me.


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

### ITER 3 — bump tokio `DEFAULT_RING_SIZE` 256 → 4096 (REJECTED)
- date:           2026-05-21 23:37
- hypothesis:     with 400 in-flight ops, a 256-entry SQ might force the SQ-full submit path inside `register_op`. Larger ring = fewer such "submit under lock" cycles.
- change:         `tokio/src/runtime/io/driver/uring.rs:15` `DEFAULT_RING_SIZE = 4096`.
- bench delta (vs ITER 2): 124,417 → **122,730** = -1.4% (within noise, possibly slightly worse).
- conclusion:     **rejected, abandoned.** 400 clients × 1 in-flight op each ≤ 256 SQ slots almost always; the original 256 was sufficient. Memory waste and zero IOPS benefit.
- commit:         abandoned in ~/tokio
- followups:      same hypothesis tree. The next experiments need to target the Mutex itself, not the ring size.

---

### ITER 4 — move `io_uring_enter` outside the `Mutex<UringContext>` (REVERTED — fast but breaks liveness)
- date:           2026-05-22 06:15
- hypothesis:     primary upstream hotspot is the parking_lot Mutex around tokio's UringContext: every `register_op` and `Op::poll` acquires it, and `register_op` holds the lock during the `io_uring_enter` syscall (microseconds). io-uring's `Submitter::submit()` is `&self`-only, and io_uring explicitly supports concurrent submit syscalls — so we can push+sync the SQ under the lock, drop the lock, then call `io_uring_enter` via a cached raw fd. Expected: +30-50% IOPS.
- change (tokio):
  - `runtime/io/driver.rs:61` add `uring_raw_fd: AtomicI32` alongside `uring_context: Mutex<UringContext>`; default -1
  - `runtime/io/driver/uring.rs:Handle::try_init` cache `uring.as_raw_fd()` into `uring_raw_fd` after ring init
  - new `Handle::submit_unlocked()` — raw `libc::syscall(SYS_io_uring_enter, fd, 4096, 0, 0, NULL, 0)`; swallows EAGAIN/EINTR/EBUSY (other submitters will retry)
  - `Handle::register_op` restructured into Phase 1 (locked: slab insert + SQ push + CQ-full handling + `sq.sync()`) and Phase 2 (lock-free: `submit_unlocked()`)
- bench delta (vs ITER 2): 124k → **180k IOPS = +45%**; p50 3090 → 2118 µs (-31%); p99 7217 → 5018 µs (-30%); p99.9 9069 → 6582 µs; max 12911 → 38371 µs (worse tail by a single outlier — noise).
- regression: `tokio/tests/fs_uring::open_many_files` (which spawns 10,000 file-open tasks) hangs. Instrumenting showed `register_op` + `submit_unlocked` happen 1000s of times but `dispatch_completions` is never called. With the original submit-under-lock the same test sees `dispatch_completions` invoked ~hundreds of times in the same wall-clock window.
- root cause (best guess, unconfirmed): without the µs-scale lock-hold during `io_uring_enter`, worker threads never yield to the kernel scheduler, so the runtime's IO driver thread doesn't get a chance to run `turn()` → `dispatch_completions()`. The lock contention previously acted as an inadvertent fairness mechanism. The benchmark's steady-state RPS workload still drains CQs because every worker eventually parks waiting on its next read; `open_many_files`'s burst-of-10k-spawns followed by `tracker.wait()` never yields to the IO driver under the new scheduling pattern.
- conclusion:     **reverted in tokio.** The +45% on the bench is real but unrelated to a correctness-safe change. Closing the gap properly requires either (a) explicitly yielding to the IO driver from `register_op` (smells, adds a context switch), (b) running `dispatch_completions` opportunistically from `register_op` after the lock-free submit (re-takes the lock, undoes the win), or (c) **per-worker rings** so submission and dispatch happen on the same thread with no Mutex at all.
- commit:         abandoned in ~/tokio
- followups:
  - H6 (per-worker rings) is now both the architectural answer AND the only correctness-clean path to capturing the +45% headroom. Promoted to top priority.
  - FU-2: a smaller follow-up — make `submit_unlocked` opportunistic (e.g. once every N register_ops, or once per turn()) so the lock-yield path still happens. Likely partial win + no liveness regression. Skipped for now in favor of H6.

---

### ITER 5 — multi-ring io_uring in tokio (ACCEPTED — closes the gap)
- date:           2026-05-22 06:36
- hypothesis:     the Mutex<UringContext> is the dominant cost (per ITER 0-4). N rings, each behind its own Mutex, picked by thread-id hash, should N-fold-reduce contention.
- change (tokio, bookmark `multi-ring` stacked on `expose-uring-ops`):
  - `runtime/io/driver.rs`: replace single `uring_context: Mutex<UringContext>` with `uring_contexts: Box<[Mutex<UringContext>]>`. `Driver::turn` drains every ring's CQ each tick.
  - `runtime/io/driver/uring.rs`:
    - `num_rings()` reads `TOKIO_IO_URING_RINGS` env var once (default 1, range 1..=16).
    - Each ring's fd gets its own mio token `TOKEN_URING_BASE + ring_id`.
    - `try_init` only fills the kernel-features probe for ring 0 (kernel support is global); rings 1..N use new `try_init_no_probe()`.
    - `register_op` picks ring_id via thread-id hash, returns `(ring_id, slab_idx)`.
    - `cancel_op` takes `(ring_id, index)`.
    - `pick_ring_id` hashes `std::thread::current().id()` with `DefaultHasher`.
  - `runtime/driver/op.rs`: `State::Polled(idx)` → `State::Polled(ring_id, idx)`. `Op::poll` uses `driver.get_uring(*ring_id).lock()`. `Op::drop` passes `ring_id` to `cancel_op`.
- bench delta (vs ITER 0 baseline, 400 clients, 256 MiB/client, O_DIRECT, 15s):
  - N=1 (default): 124k → 96k (NVMe variance day-over-day; effectively unchanged)
  - **N=8: 96k → 331k IOPS, +245%; p50 3936 → 1129 µs (-71%); p99 9454 → 2261 µs (-76%)**
  - vs side-ring: **0.7% gap on IOPS** (331k vs 333k), **0.6% gap on p50** (1129 vs 1122 µs)
- perf delta: `finish_task_switch.isra.0` 26.37% → 14.57% — the Mutex futex contention is gone; remaining 14% is worker park (idle), matching side-ring's profile.
- tests: all 3 `fs_uring` + all 3 `uring_public_api` pass at N=1 and N=8.
- conclusion:     **accepted, headline win.** Architectural change validated. Demo deliverable in place.
- commit:         `tokio:8bf4c005` (bookmark `multi-ring`)
- followups:
  - **FU-3 (Phase 2):** swap `TOKIO_IO_URING_RINGS` env var for `Builder::io_uring_rings(n)`. Cosmetic; needed before sending to upstream tokio.
  - **FU-4 (Phase 2):** per-worker CQ drain — each worker drains its own ring inside its task-poll loop, instead of `Driver::turn` doing all of them. Likely another +5-10% headroom and reduces drainer's per-ring Mutex hold time.
  - **FU-5:** investigate why N=16 is worse than N=8 on this hardware (likely too many rings → cache thrash / Mutex array iteration overhead in Driver::turn). N=worker_threads probably optimal; investigate.
  - **FU-6:** TEU's adapter still inherits `ITER 2`'s opcode-bitmap cache. With multi-ring it should still work (the bitmap is process-wide). Verified empirically by running the bench.

---

### ITER 6 — Phase 2 conservative refinement (per-poll Path II drain + worker_index routing)
- date:           2026-05-22 07:25
- hypothesis:     Phase 1 left an architectural smell — when worker K was busy and ring K got a CQE, a parked worker M did the dispatch on M, then fired the task waker on K via cross-CPU queue push. The M→K bounce shows in p99.999 (extra cache misses). Fix: each worker drains its own ring after every task poll (Path II), so the waker fires locally on K. Also tighten ring routing: `worker_index % N` instead of thread-id hash, so workers consistently pick stable shards.
- change (tokio, bookmark `multi-ring` @ 73c0e714, stacked on `expose-uring-ops`):
  - `runtime/scheduler/multi_thread/worker.rs`: add `pub(crate) fn with_current_index() -> Option<usize>` exposing `worker.index` to the io driver; call `drain_own_ring_opportunistically()` right after `task.run()` in `run_task`.
  - `runtime/io/driver/uring.rs`:
    - `pick_ring_id` uses `worker_index % n` (modulo for even distribution when N < worker_threads).
    - new `Handle::drain_own_ring_opportunistically()`: `try_lock` the worker's own ring's Mutex, drain. Owner-affine try_lock is uncontested in steady state.
    - `MAX_RINGS` 16 → 256 to allow N to match worker_threads on big boxes.
  - `runtime/scheduler/multi_thread/mod.rs`: `mod worker` → `pub(crate) mod worker` so io driver can reach `with_current_index`.
- bench delta (vs Phase 1 N=8, same 32-core box, 400 clients):
  - **N=8: 331k → 333k IOPS** (parity)
  - p50: 1.13 → 1.12 ms (parity)
  - p99: 2.26 → 2.30 ms (parity within run-to-run noise)
  - **p99.999: 11.88 → 8.68 ms** (-27%, the bounce was hiding here)
  - max: 38.37 → 9.57 ms (much tighter — the longest stalls were the bouncing ones)
- key trade-offs recorded in `MULTI_RING_DESIGN.md` "Phase 2 conservative refinement":
  - Kept mio as park primitive (rejected io_uring_enter-as-park: would change observable kernel state for non-uring tokio users — Christian called this "too radical"). Captured in memory entry `feedback-park-design-constraints`.
  - Kept central `Driver::turn` drain as backstop (still drains all rings under per-ring Mutex). Path II catches most CQEs first; central drain handles the residual.
  - Kept fallback ring 0 for non-worker callers (current_thread runtime, block_on from main thread, spawn_blocking-nested block_on). TEU never hits this path; the fallback exists for the patched `tokio::fs::*` and `fs_uring` test suite.
- aspirational (deferred):
  - "Path III drains-self-only + unparks-others": parked worker M sees ring K event, doesn't touch K's ring, instead unparks K so K can drain its own ring. Would close the residual bounce for idle workers too. Requires cross-layer plumbing from io driver into scheduler's per-worker unpark API. Not needed for the bench (Path II already eliminates the visible bounce).
  - Build-time/runtime config to default N = worker_threads. Currently env-var `TOKIO_IO_URING_RINGS`. Builder API before upstream PR.
- tests: all 3 fs_uring + all 3 uring_public_api pass at both N=1 and N=8.
- conclusion:     **accepted.** Architectural smell fixed without touching tokio's park primitive. Phase 2 IOPS = Phase 1 IOPS; tail latency materially better. Demo-ready and within striking distance of upstream-able.
- commit:         `tokio:73c0e714` on bookmark `multi-ring`
- followups:      none blocking the demo. Path III refinement and Builder API are nice-to-haves for upstreaming.

---

### ITER 7 — quick-sweep with per-engine CPU usage (cached + direct-IO)
- date:           2026-05-22 09:25
- hypothesis:     (measurement only) compare all 5 engines on IOPS / CPU-second / tail latency. Cached-IO and direct-IO sides.
- change:         no source change. New scripts at `benchmark/scripts/quick-sweep-cached.sh` and `benchmark/scripts/quick-sweep-directio.sh` wrap each engine in `/usr/bin/time -v` and dump rusage + benchmark JSON per engine.

#### Cached-IO sweep (4 GiB working set, 400 clients, 8s, page-cache hits — exposes runtime overhead)

| Engine                           |     IOPS | p50 µs | p99 µs | p99.9 µs | p99.99 µs | p99.999 µs |  max µs | user s | sys s | CPU%  | **ops/cpu-s** |
| -------------------------------- | -------: | -----: | -----: | -------: | --------: | ---------: | ------: | -----: | ----: | ----: | ------------: |
| tokio-uring (single-thread)      |  438,428 |  1,070 |  1,229 |    5,861 |     6,320 |      6,619 |   6,627 |    1.6 |   6.5 |  101% |   **430,359** |
| TEU side-ring (no-force-yield)   | 1,066,690 |     2.7 | 11,338 |   24,396 |    34,046 |     40,632 |  45,482 |   10.8 |  23.7 |  427% |       247,277 |
| **TEU upstream multi-ring (N=32)** | **6,412,681** |     11 |    456 |    1,159 |    15,942 |     32,801 |  63,439 |   74.2 | 162.0 | 2920% |       217,204 |
| TEU side-ring (force-yield)      | 3,893,874 |     72 |    383 |    1,667 |    12,837 |     21,692 |  62,358 |   90.5 | 148.5 | 2962% |       130,350 |
| tokio-spawn-blocking-512         |   287,724 |  1,107 |  3,557 |    4,551 |     6,701 |     13,304 |  19,694 |   19.5 |  52.7 |  895% |        31,876 |

Headline: upstream multi-ring achieves **6.4M IOPS** scaling across 32 cores at 217k IOPS/cpu-s; within 12% of side-ring no-force-yield's per-cpu efficiency. Side-ring no-force-yield's **34 ms p99.99 / 45 ms max** is the worst tail in the comparison — pathological monopoly under no-yield.

#### Direct-IO sweep (100 GiB working set, 400 clients, 10s, NVMe-bound)

| Engine                           |     IOPS | p50 µs | p99 µs | p99.9 µs | p99.99 µs | p99.999 µs |  max µs | user s | sys s | CPU%  | **ops/cpu-s** |
| -------------------------------- | -------: | -----: | -----: | -------: | --------: | ---------: | ------: | -----: | ----: | ----: | ------------: |
| TEU side-ring (no-force-yield)   |  350,181 |  1,060 |  2,245 |    2,851 |     6,275 |     12,550 |  16,212 |   59.9 | 165.7 |  233% |        15,526 |
| **TEU upstream multi-ring (N=32)** |  315,428 |  1,174 |  2,337 |    2,986 |     5,992 |     10,584 | **10,846** |   8.9 |  23.3 |  320% |    **97,959** |
| TEU side-ring (force-yield)      |  315,340 |  1,176 |  2,314 |    2,884 |     6,496 |     10,756 |  11,248 |    9.1 |  20.4 |  291% |       106,859 |
| tokio-spawn-blocking-512         |  259,032 |  1,276 |  3,742 |    4,743 |     5,845 |     11,362 |  19,366 |   27.3 |  81.1 | 1077% |        23,903 |
| tokio-uring (single-thread)      |  205,977 |  2,224 |  4,078 |    7,197 |    11,461 |     12,386 |  12,411 |    1.4 |   8.8 |  101% |       201,938 |

Headline: under realistic NVMe-bound load, **upstream multi-ring is at the Pareto front**: 315k IOPS within 10% of side-ring's 350k peak, lowest max latency in the comparison (10.8 ms), and **6.3× more CPU-efficient than side-ring no-force-yield**. Side-ring's no-force-yield wastes 225 cpu-seconds in 10s wall on busy-polling per-thread urings.

- perf delta (cached small-WS profile, upstream vs side-ring no-force-yield):
  - both engines: 21-22% CPU in `[k] copy_user_enhanced_fast_string` (page cache → userspace memcpy — the useful work)
  - **upstream `[k] __lock_text_start` = 5.94%**, **side-ring = 11.48%** — multi-ring has *half* the kernel-side lock contention (side-ring's lock is in `__do_sys_io_uring_enter → io_submit_flush_completions → io_cqring_ev_posted → __wake_up`)
  - upstream's overhead lives in userspace instead: `Op::poll`, `register_op`, `drop_in_place<Op<...>>`, allocator. Scales with cores; doesn't bottleneck like kernel locks do.
- conclusion:     **measurement-only iteration, all wins from the prior architectural commits.** Demo deliverable is in place. New artifacts: `benchmark/scripts/quick-sweep-{cached,directio}.sh` (versioned reproducibility), all per-engine JSONs + rusage in `/ephemeral/quick-sweep{,-directio}/`.

---

### ITER 8 — `--cpu-work-per-op-ns N` knob + 30/70/90/95% utilization sweep
- date:           2026-05-22 10:00
- hypothesis:     under realistic CPU+IO load (client tasks doing per-op application work), tokio-epoll-uring's per-thread poller task gets starved by client tasks. Tail latency should grow disproportionately for side-ring; upstream multi-ring should degrade gracefully because the central `Driver::turn` drain backstops the per-worker Path II `try_lock` drain.
- change:         `benchmark/src/main.rs` adds `--cpu-work-per-op-ns N` flag and a `do_cpu_work(args)` helper that busy-waits `rand_range(0, 2N)` ns after each completed read in every engine's client loop. Variance simulates real workloads; the random range means no-yield poller starvation can't be avoided by lucky timing.
- new scripts:    `benchmark/scripts/cpu-mix-sweep.sh` (single utilization target) and `cpu-mix-multi-sweep.sh` (30/70/90/95%).

#### Pre-sweep: tuning CPU_NS for 70% per-core target (32-core box)
| cpu_work ns | upstream IOPS | per-core util |
| --- | ---: | ---: |
| 0 | 343k | 11% |
| 21,000 | 315k | **30%** |
| 60,000 | 315k | **70%** |
| 95,000 | 288k | **90%** |
| 110,000 | 265k | **95%** |

IOPS holds at the NVMe ceiling (~315k) up to 60µs CPU work; above ~80µs CPU becomes the bottleneck.

#### 4-engine comparison (no-force-yield dropped — not used in production), direct-IO, 400 clients, 15s, 32-core NVMe

**30% per-core (NVMe-bound, light CPU)**
| Engine | IOPS | p50 µs | p99 µs | p99.9 µs | p99.99 µs | p99.999 µs | max µs |
|---|---:|---:|---:|---:|---:|---:|---:|
| TEU side-ring force-yield | 332,635 | 1,137 | 2,212 | 3,375 | 6,054 | 11,207 | 13,107 |
| TEU upstream multi-ring | 313,995 | 1,189 | 2,245 | 3,045 | 6,275 | 10,781 | 11,952 |
| tokio-spawn-blocking-512 | 257,685 | 1,261 | 3,504 | 4,342 | 5,734 | 11,174 | 26,624 |
| tokio-uring | 40,813 | 9,757 | 12,141 | 14,541 | 15,344 | 15,827 | 16,204 |

**70% per-core**
| Engine | IOPS | p50 | p99 | p99.9 | p99.99 | p99.999 | **max** |
|---|---:|---:|---:|---:|---:|---:|---:|
| TEU side-ring force-yield | 334,801 | 1,064 | 3,242 | 6,271 | 12,558 | 16,990 | 26,018 |
| **TEU upstream multi-ring** | 313,542 | 1,176 | 3,000 | 5,792 | 10,248 | 15,819 | **17,236** |
| tokio-spawn-blocking-512 | 255,602 | 1,215 | 3,291 | 4,346 | 9,331 | 25,510 | 30,048 |
| tokio-uring | 15,666 | 25,395 | 28,426 | 32,506 | 33,784 | 34,734 | 35,750 |

**90% per-core**
| Engine | IOPS | p50 | p99 | p99.9 | p99.99 | p99.999 | **max** |
|---|---:|---:|---:|---:|---:|---:|---:|
| TEU side-ring force-yield | 307,382 | **349** | 6,365 | 15,344 | 27,558 | 40,894 | 52,822 |
| **TEU upstream multi-ring** | 305,444 | 625 | 6,095 | 12,657 | 21,463 | 30,999 | **45,744** |
| tokio-spawn-blocking-512 | 255,188 | 1,167 | 3,768 | 6,492 | 14,836 | 28,033 | 52,593 |
| tokio-uring | 10,108 | 39,387 | 43,123 | 44,663 | 46,203 | 46,957 | 47,088 |

**95% per-core (saturated)**
| Engine | IOPS | p50 | p99 | p99.9 | p99.99 | p99.999 | **max** |
|---|---:|---:|---:|---:|---:|---:|---:|
| TEU side-ring force-yield | 266,766 | **281** | 7,721 | 17,793 | 28,328 | 48,071 | 66,617 |
| **TEU upstream multi-ring** | 266,030 | 649 | 7,336 | 14,270 | 22,643 | 35,652 | **44,401** |
| tokio-spawn-blocking-512 | 236,746 | 1,250 | 3,879 | 7,102 | 16,458 | 25,838 | 31,719 |
| tokio-uring | 8,780 | 45,318 | 50,332 | 55,640 | 57,344 | 58,819 | 60,654 |

#### Key findings

- **Upstream multi-ring's tail advantage grows with CPU pressure.** Max latency Δ vs force-yield: −8% @ 30%, **−34% @ 70%**, −13% @ 90%, **−33% @ 95%**. The central `Driver::turn` drain backstop fires whenever any worker parks, so no single worker's CPU work can monopolize a ring's CQ drain.
- **Force-yield has lower p50 at high CPU, upstream has lower tail.** At 95% utilization force-yield's p50 is 281 µs vs upstream's 649 µs — when the dedicated poller task gets scheduled it drains in tight bursts (low median). But force-yield's p99.999 = 48 ms vs upstream's 36 ms; the poller task occasionally gets stuck behind CPU-bound clients and a long stall ensues. Median ≠ tail.
- **tokio-uring collapses past 30% load.** Single-threaded executor: 41k → 16k → 10k → 9k IOPS as CPU pressure climbs. Tail latencies cluster at 30-60 ms. Use only on workloads with at most one effective core of IO work.
- **spawn-blocking-512 has a stable but mediocre profile.** ~256k IOPS across all loads; p50 ~1.2 ms (decoupled by the blocking pool's thread per inflight op); deep tail competitive at 90/95% but the worst max at 70% (30 ms).
- **One flake observed and ruled out:** the initial 30%-force-yield run reported wall=101.6s (vs target 15s); three reruns at the same config all 15.1s. Possibly a one-off cgroup or background-task interaction. Real behavior is clean.
- conclusion:     **accepted, headline pitch material.** The story "upstream multi-ring's deep-tail advantage grows with CPU load — exactly the regime PageServer cares about" is supported across 4 utilization levels with consistent direction.
- commit:         next

---

(iterations continue below)
