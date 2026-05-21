# Multi-ring io_uring for tokio — design proposal

## Status

Design draft, written 2026-05-22 during the overnight perf iteration on the `tokio-epoll-uring-upstream` backend. Companion to:

- `~/tokio-epoll-uring/ITER_LOG.md` — the perf iteration narrative that motivated this design
- `~/tokio-epoll-uring/plan.md` — the original overnight plan
- `~/tokio/tokio` bookmark `expose-uring-ops` — the patched tokio with the public `tokio::io_uring` API this design extends

Not implemented yet. The cheap one-line optimizations attempted in ITER 1-4 either rejected or revealed an architectural ceiling (see ITER 4): tokio's single `Mutex<UringContext>` is the dominant cost on a contended workload, and the obvious lock-free workaround breaks the IO-driver liveness in tests because the µs-scale lock-hold during `io_uring_enter` happens to be the only thing that yields the worker thread back to the scheduler.

## The problem

Tokio's io_uring driver (added 2026 by Alice Ryhl et al.) keeps one ring per runtime instance, behind a `parking_lot::Mutex<UringContext>` in the `runtime::io::driver::Handle`. Every:

- `register_op(entry, waker)` — submission of one SQE plus an `io_uring_enter` syscall
- `Op::poll(cx)` — every awaiting future re-acquires the lock to inspect its slot's `Lifecycle`
- `dispatch_completions()` — drains CQEs and wakes the corresponding wakers

…takes the lock. Under realistic loads (the `tokio-epoll-uring-upstream` benchmark engine running 400 concurrent random-8K O_DIRECT readers on NVMe) the perf profile is dominated by futex contention on this single Mutex:

```
ITER 0 baseline (upstream, 400 clients, 15s, NVMe O_DIRECT):
   26.37%  finish_task_switch.isra.0
            └─ __schedule → futex_wait_queue_me → futex_wait → __x64_sys_futex
   ~14%    register_op (acquires Mutex to insert slab + push SQE + submit)
   ~12%    Op::poll      (acquires Mutex to inspect slot's Lifecycle)
   1.23M samples  parking_lot::raw_mutex::RawMutex::lock_slow
    710k samples  parking_lot::raw_mutex::RawMutex::unlock_slow

By comparison, TEU's side-ring backend (which has one ring per executor
thread via `thread_local_system`) shows only ~10% in `finish_task_switch`,
mostly worker park (idle), not contention.
```

The benchmark result mirrors the profile:

| Engine                                       | IOPS    | p50      | p99      | p99.9    |
| -------------------------------------------- | ------- | -------- | -------- | -------- |
| TEU side-ring (one ring per executor thread) | 333k    | 1.12 ms  | 2.20 ms  | 2.65 ms  |
| TEU upstream  (one shared ring)              | 124k    | 3.09 ms  | 7.22 ms  | 9.07 ms  |
| **Ratio**                                    | **2.7×**| **2.8×** | **3.3×** | **3.4×** |

The remaining IOPS gap **is the Mutex**. The micro-optimization attempts in ITER 1-4 explored:
- caching `is_supported(opcode)` (+0.6%)
- increasing the SQ size (none)
- moving `io_uring_enter` outside the lock (+45% on the bench, but broke `tokio/tests/fs_uring::open_many_files` because the syscall under the lock was the only thing yielding worker threads to the scheduler)

None of them can credibly close the gap without giving up correctness or fairness. The architectural answer is **one ring per worker**.

## Goals

1. **Close the upstream-to-side-ring gap to within 20% on the O_DIRECT 400-client benchmark.** Side-ring achieves ~330k IOPS today; the multi-ring upstream target is ≥ 270k IOPS.
2. **Public API unchanged.** Code that uses `tokio::io_uring::Submission<T>` keeps working byte-identically. The new behaviour is opt-in via the runtime builder.
3. **No surprises for existing tokio users.** Default tokio behaviour does not change. The new mode is gated on `tokio_unstable` + `feature = "io-uring"` + a new builder knob.
4. **Send futures still work.** A `Submission<T>` future polled from a different worker than the one that submitted it must still resolve correctly. (Test: tokio's task scheduler may steal a task to another worker mid-flight.)
5. **No `LocalSet` requirement.** Christian's PageServer uses spawned tasks freely; pinning them to a specific worker is not acceptable.

## Non-goals

- **SQPOLL.** Worth exploring later, but adds a per-ring kernel thread and complicates fairness. Out of scope here.
- **Cross-runtime rings.** Each runtime owns its own set of rings. Two runtimes in the same process are independent.
- **Pluggable scheduling policy.** "One ring per worker thread" is the only knob. No "shard by op hash", no "N rings for M workers" with M > N. Keep it simple.

## Public API (additive on top of `expose-uring-ops`)

```rust
// in tokio::runtime::Builder, gated on cfg_io_uring!
impl Builder {
    /// Use a per-worker `io_uring` instead of a single shared ring.
    /// Default: `false`.
    ///
    /// When enabled, each worker thread of the runtime owns its own
    /// `io_uring` instance. SQE submissions from a task running on
    /// worker N go to worker N's ring; CQEs from worker N's ring are
    /// dispatched on worker N (no cross-thread mutex).
    ///
    /// On the `new_current_thread()` runtime this is equivalent to
    /// the default (single ring), since there is only one worker.
    ///
    /// On the `new_multi_thread()` runtime with `worker_threads(N)`,
    /// this creates `N` rings, plus one fallback ring used by tasks
    /// running in non-worker contexts (`block_on`, blocking pool
    /// callbacks, etc.). The fallback ring keeps the existing
    /// `Mutex<UringContext>` semantics for those minority cases.
    ///
    /// Requires `--cfg tokio_unstable` and `feature = "io-uring"`.
    pub fn io_uring_per_worker(&mut self, enable: bool) -> &mut Self;
}
```

Everything else (`Submission<T>`, `Completable`, `Cancellable`, `CqeResult`, `is_supported`, `is_ready`) stays unchanged. From the consumer's perspective the only difference is performance under contention.

## Internal architecture

### Where the ring lives

The runtime's existing `runtime::io::Handle` keeps `Mutex<UringContext>` (today: one). Add a parallel structure:

```rust
pub(crate) struct Handle {
    // ... existing fields ...

    // Existing: the shared "fallback" ring. Used in single-ring mode
    // (default) and as the fallback in per-worker mode.
    pub(crate) uring_context: Mutex<UringContext>,
    pub(crate) uring_probe: OnceCell<Option<io_uring::Probe>>,

    // New (per-worker mode only): one ring per worker, indexed by
    // worker_id. None if per-worker mode is disabled.
    //
    // Each entry is *unlocked* — only the owning worker writes to it
    // (push SQE, drain CQ). Cross-worker reads/writes go through the
    // narrow set of operations defined below; those use atomic state
    // in `Lifecycle` plus a tiny per-slot mutex *if* needed.
    pub(crate) per_worker_uring: Option<Box<[WorkerUringContext]>>,
}

pub(crate) struct WorkerUringContext {
    // Single-writer (the owning worker); the io_uring crate's
    // SubmissionQueue / CompletionQueue and the Slab<Lifecycle> all
    // live behind UnsafeCell. SAFETY discharged at all access sites by
    // showing `current_worker_id() == self.worker_id`.
    pub(crate) inner: UnsafeCell<UringContext>,
    pub(crate) worker_id: u16,
}
unsafe impl Sync for WorkerUringContext {} // see SAFETY in each method
```

### Where the worker_id comes from

Tokio's multi-thread scheduler already assigns each worker thread a stable index (`runtime::scheduler::multi_thread::worker::Worker::index: usize`). The scheduler's `Context` thread-local exposes it. We can either:

- (A) reach the current worker via a thread-local set at worker startup, or
- (B) add a public accessor `tokio::runtime::current_worker_id() -> Option<u16>` (None outside a worker — e.g. `block_on` on the main thread).

(B) is cleaner. It can be `pub(crate)` to start.

### Submission

`Submission::new(entry, data)` constructs an `Op<T>` future. On first poll, `register_op` runs. Today it always goes to `uring_context`. New shape:

```rust
unsafe fn register_op(&self, entry: Entry, waker: Waker) -> io::Result<usize> {
    if let (Some(rings), Some(wid)) = (
        self.per_worker_uring.as_deref(),
        current_worker_id(),
    ) {
        // SAFETY: only the current worker accesses its own ring's
        // UnsafeCell. `wid == rings[wid].worker_id` is enforced by
        // current_worker_id()'s contract.
        let ctx = unsafe { &mut *rings[wid as usize].inner.get() };
        register_op_into(ctx, entry, waker, /*pack_user_data=*/ wid)
    } else {
        // Fallback: shared ring under Mutex (existing path).
        let mut guard = self.uring_context.lock();
        register_op_into(&mut *guard, entry, waker, /*pack=*/ FALLBACK_WID)
    }
}
```

`pack_user_data` mixes the worker_id into the SQE's `user_data` field. Slab indices need ~24 bits realistically (any reasonable in-flight count); worker_id needs ~6-8 bits (max worker count). Layout:

```
 63                            16 15      0
+--------------------------------+--------+
| slab_index (47 bits)           | wid+1  |
+--------------------------------+--------+
```

`wid+1` so a non-zero low-16 distinguishes per-worker ops from the fallback ring's ops (whose `wid` field stays 0). On the fallback ring we use the existing user_data scheme (slab_index << 16, low bits = 0).

### CQE dispatch

Today: every `Driver::turn()` end-of-loop locks `uring_context` and calls `dispatch_completions()`. New shape:

```rust
// In Driver::turn(), after mio events processed:
if let Some(rings) = handle.per_worker_uring.as_deref() {
    if let Some(wid) = current_worker_id() {
        // SAFETY: only the current worker drains its own ring.
        let ctx = unsafe { &mut *rings[wid as usize].inner.get() };
        ctx.dispatch_completions();
    } else {
        // Non-worker (e.g. the driver thread itself, if separate).
        // Drain the fallback ring only.
        handle.uring_context.lock().dispatch_completions();
    }
} else {
    handle.uring_context.lock().dispatch_completions();
}
```

Each worker's ring is **only** drained by the same worker. This means CQEs produced by worker A's ring only generate wakers fired on worker A. Tokio's task scheduler may then steal that woken task to worker B for the actual poll — that's fine, because `Op::poll` accesses the slab via worker_id encoded in the slot index (decode → lookup the originating worker's ring).

### Op::poll cross-worker access

This is the tricky one. `Op::poll` runs wherever the scheduler decides to poll the task. Today it locks `uring_context` to inspect Lifecycle. Per-worker rings don't have a Mutex, so we need a different scheme.

Two viable approaches:

**(P1) Per-slot atomic state + per-slot mutex for payload.**

Replace `Slab<Lifecycle>` with `Slab<Slot>` where:

```rust
struct Slot {
    state: AtomicU8,       // {Waiting=0, Completed=1, Cancelled=2, Submitted=3}
    payload: parking_lot::Mutex<SlotPayload>,
}
enum SlotPayload {
    Waker(Waker),
    CqeEntry(io_uring::cqueue::Entry),
    CleanupClosure(Box<dyn FnOnce(CqeResult) + Send>),
    Empty,
}
```

- `Op::poll` reads `state` atomically. If `Waiting` and our waker is current (track via a generation counter or by waker pointer comparison), no lock needed. If `Completed`, locks the slot's per-slot mutex to extract the CQE.
- `dispatch_completions` (running on the owning worker, sees its own ring) loads/stores state atomically, only locks the per-slot mutex to wake the stored waker.
- Per-slot mutex contention is rare: at most one polling future and one CQE writer per slot.

Cost: 1 atomic + 1 small Mutex per slot. Slab access (Vec resize) still needs synchronisation — but only on insert, which happens on the owning worker, single-writer. The slab Vec backing memory is mutated only by the owning worker; readers from other workers see a stable indexing scheme (slab indices are stable across resizes).

Actually that last sentence is too optimistic — slab Vec resize relocates memory. Cross-worker readers may hold pointers. We need either (a) `slab` is replaced by a non-relocating backing (e.g. `Box<[MaybeUninit<Slot>]>` with a freelist, sized at runtime startup at e.g. 4096 slots — same as the SQ size), or (b) `Arc<Slot>` and the slab holds `Vec<Arc<Slot>>`. (a) is preferred — fixed footprint, no per-op alloc.

**(P2) Route Op::poll through a one-shot oneshot channel.**

When `register_op` succeeds, return an `Arc<Slot>` to the caller (the `Op<T>` future). Each `Op<T>` polls its `Arc<Slot>` directly without any Mutex (using AtomicWaker pattern: `aw.register(cx.waker())`). `dispatch_completions` wakes via `aw.wake()`. Submission semantics same as (P1).

(P2) is cleaner: no slab indirection on Op::poll, no Mutex anywhere on the poll path. The per-op `Arc<Slot>` alloc is a real cost (was avoided by today's Slab<Lifecycle>), but: under per-worker mode each ring's slab is bounded (e.g. 256 entries = SQ size), so a fixed `Box<[Slot]>` allocated at ring init time is reusable. The `Op<T>` future borrows the slot via raw pointer for its lifetime; the slot doesn't move (fixed allocation).

**Recommendation:** (P2). Simpler invariants, no per-op alloc after init, lock-free poll.

### Cancellation

Today's `Lifecycle::Cancelled(Box<dyn FnOnce(CqeResult) + Send>)` (added in the `expose-uring-ops` patch) is preserved. `Op<T>::drop` stores the cleanup closure into the slot. When the worker's ring delivers the CQE, the worker invokes the closure and frees the slot. Cross-worker semantics: same — the cleanup closure must be `Send`, and it runs on the owning worker.

### Mio integration

Today tokio registers the single uring fd with mio under `TOKEN_WAKEUP`. Per-worker mode: each ring's fd gets its own mio token. Use `TOKEN_URING_BASE + wid` (with `TOKEN_URING_BASE` carved out of mio's token space).

**Or**: each worker has its own mio `Poll`, separate from the runtime-global mio `Poll`. The multi-thread scheduler already has per-worker context; it does NOT today have per-worker mio. Adding it is invasive — the existing single-poll design handles all `AsyncFd`/socket registrations centrally.

**Compromise:** keep the single mio `Poll`, register all N uring fds with distinct tokens. When `Driver::turn` returns events, each event's token identifies which uring's CQEs to drain. The token range `[TOKEN_URING_BASE, TOKEN_URING_BASE + N)` is reserved. The Driver::turn loop already iterates events; add a match arm for the uring token range that drains the corresponding ring's CQ.

This keeps the Driver::turn machinery centralised but allows per-worker CQ ownership. Slight downside: the Driver thread (the one that runs `turn()`) is the *only* one that drains CQs, even though we'd ideally have each worker drain its own. Worker-side drain is an optimisation on top of this — possible but not necessary for the first version.

**Two-stage plan:**
- **Phase 1 (this design):** keep one Driver thread, but use per-worker rings + per-ring tokens + per-worker `current_worker_id()`-based submission. Submission is lock-free per worker. Dispatch is centralised but per-ring (no Mutex contention because each ring's CQ has a single drainer: the Driver thread).
- **Phase 2 (future):** distribute dispatch to each worker via a per-worker mio sub-poll. Complete elimination of cross-worker IO-driver coordination.

Phase 1 alone should be enough to close most of the gap.

## File-by-file change plan (Phase 1)

### tokio side (~/tokio, bookmark `expose-uring-ops-multi-ring`, stacked on `expose-uring-ops`)

1. **`runtime/builder.rs`** — add `io_uring_per_worker: bool` field, `Builder::io_uring_per_worker(bool) -> &mut Self`. Plumbing into the `Driver::new` call.
2. **`runtime/scheduler/multi_thread/worker.rs`** — store `worker_id: u16` in the worker context's thread-local; expose via `pub(crate) fn current_worker_id() -> Option<u16>`.
3. **`runtime/io/driver/uring.rs`** —
   - Split `UringContext` into a "logical" context (slab + ring + fd) plus a top-level dispatcher.
   - Add `WorkerUringContext` (`UnsafeCell<UringContext>` + `worker_id`).
   - In `Handle`, add `per_worker_uring: Option<Box<[WorkerUringContext]>>`.
   - `Handle::register_op` branches on `per_worker_uring.is_some() && current_worker_id().is_some()`.
   - `try_init` allocates the N rings, registers each with mio under `TOKEN_URING_BASE + wid`.
   - `dispatch_completions(wid)` — drain only the ring with the given worker_id.
4. **`runtime/driver/op.rs`** — replace `Slab<Lifecycle>` with a fixed `Box<[Slot]>` per ring. `Op<T>` holds `*const Slot` (or `Arc<Slot>` initially for simplicity). Poll uses `AtomicWaker`.
5. **`runtime/io/driver.rs`** — in `turn()`, after mio events, route each event with `token` in `[TOKEN_URING_BASE, ..)` to the corresponding ring's `dispatch_completions(wid)`.
6. **`tokio/tests/uring_per_worker.rs`** — new test exercising multi-thread runtime + per-worker mode. Verifies:
   - 1000s of concurrent ops across N workers
   - tasks stolen between workers still resolve correctly
   - `Op<T>::drop` mid-flight is safe
   - `open_many_files`-style burst load completes without hangs

### TEU side (~/tokio-epoll-uring, bookmark `tokio-upstream-backend-multi-ring`, stacked on `tokio-upstream-backend`)

Nothing to do — the public `tokio::io_uring` surface is unchanged. The benchmark engine and `execute_op_upstream` continue to work.

Add one runtime probe in `execute_op_upstream` that asserts (at first use) that the multi-ring knob is set on the current runtime, and log if not (so we can detect "user forgot to opt in" at demo time).

### Benchmark

- Add a new engine `tokio-epoll-uring-upstream-multi-ring` that builds the runtime with `io_uring_per_worker(true)`.
- Add it to `runbench.sh`'s `compare_engines` array.
- Run the full sweep; expect IOPS to land near side-ring's 333k.

## Risk register

- **Slot lifetime under cross-worker poll.** `Op<T>` holds a pointer/Arc into the originating worker's slab. If that worker is dropped (e.g. runtime shutdown) before the Op resolves, we UAF. Mitigation: `Arc<Slot>` keeps the slot alive; the *ring* may go away but the slot doesn't. The CQE for an orphaned slot is lost but the cleanup closure still runs on Op::drop.
- **The "fallback ring" path** for non-worker contexts (block_on on main thread, spawn_blocking callbacks). Keep it under the existing Mutex; it's a low-frequency path. But test it under multi-thread + block_on + uring ops to make sure the routing logic doesn't deadlock.
- **mio token space.** Tokio currently uses two tokens (WAKEUP, SIGNAL). Reserving `[TOKEN_URING_BASE, TOKEN_URING_BASE + N)` constrains worker count. Pick `TOKEN_URING_BASE = u32::MAX - 1024` (or similar high value); cap worker count at 1023 implicitly (already true via OS).
- **CQ size scaling.** Each ring's CQ has 2× SQ size. With N rings × 256 SQ each = N × 512 CQ entries. For N=64 (large multi-thread), that's 32k CQ entries × 16 B = 512 KiB. Acceptable.
- **First-call OnceCell probe.** The `is_supported` probe runs once per opcode. With per-worker rings, each ring needs its own probe. Currently the probe is global. Need per-ring probes, or one global probe at startup.
- **Scheduler doesn't yield to IO driver under burst.** This is the bug that bit ITER 4: the IO driver thread can starve if workers don't yield. With per-worker rings + worker-side drain (eventually Phase 2), this resolves naturally. In Phase 1 the IO driver is still central, so the same risk applies. Mitigation: have `register_op` opportunistically call `dispatch_completions` on its own worker's ring before returning Pending. Cost: one CQ check per submit (cheap; CQ head/tail are atomics).

## Demo deliverable shape

When this lands the user-facing demo should be:

1. **Single repo state.** Two stacked bookmarks on `~/tokio` (`expose-uring-ops` + `expose-uring-ops-multi-ring`), two on `~/tokio-epoll-uring` (`tokio-upstream-backend` + `tokio-upstream-backend-multi-ring`).
2. **One benchmark sweep table** comparing side-ring, single-ring upstream, and multi-ring upstream. Expected:

   | Engine            | IOPS    | p50      | p99      |
   | ----------------- | ------- | -------- | -------- |
   | side-ring         | 333k    | 1.12 ms  | 2.20 ms  |
   | upstream (single) | 124k    | 3.09 ms  | 7.22 ms  |
   | upstream (multi)  | ≥270k   | ≤1.5 ms  | ≤3.0 ms  |

3. **One paragraph** on Discord to Alice: "The Mutex was the bottleneck; per-worker rings gets within X% of our side-ring. Want me to draft the PR for `Builder::io_uring_per_worker(bool)` and we can iterate on naming?"

## What's not yet decided

- Worker-side drain (Phase 2) — defer.
- SQPOLL integration — defer.
- Whether `current_worker_id()` should be public — probably yes eventually (useful for other affinity-aware integrations).
- Whether `Slot` should be `Arc<Slot>` (simpler, one alloc per op) or `*const Slot` into a fixed pool (zero per-op alloc, more unsafe). Start with `Arc<Slot>`, optimise later if perf demands.

## Authoring notes

- This design was reached after the overnight perf iteration confirmed the Mutex is the dominant cost. The micro-optimization branch (ITER 1-4 in `ITER_LOG.md`) ruled out cheaper fixes.
- The user (Christian) explicitly asked for this direction. The user's PageServer work informs the constraints: Send futures, no `LocalSet`, jemalloc-friendly, per-core affinity.
