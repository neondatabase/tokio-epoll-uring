# Prototype: switch tokio-epoll-uring to drive tokio's own io_uring

## Context

The user is preparing a Q2 demo to make the case for Q3 funding of an effort to retire tokio-epoll-uring's side-ring architecture and submit io_uring SQEs through tokio's own ring. The Discord chat with tokio maintainer Alice Ryhl (5/8/26) established that the missing tokio piece is "the loop that reads from the CQ needs to be able to handle user-defined ops" — Alice is happy to land a simple version now; the user took the action item.

This plan covers both halves of that prototype:

1. **tokio half**: a `tokio_unstable`-gated public API that lets external crates submit arbitrary SQEs and own their resources across the in-flight window. Designed PR-ready, so the same patch can be upstreamed.
2. **tokio-epoll-uring half**: a second backend that uses tokio's new API, selectable at runtime, with the existing public crate surface byte-identical. The current side-ring backend remains the default so the new backend can be A/B-compared in the same benchmark binary.

Verification: existing `benchmark/scripts/runbench.sh` (400 + 1200 client sweeps) plus a new benchmark engine `tokio-epoll-uring-upstream` so both backends appear side-by-side in the resulting CSVs. Work happens on `/ephemeral` (EC2 InstanceStore NVMe mount), with all changes tracked under `jj` and never pushed.

---

## Part A — tokio: public io_uring submission API

Repo: `~/tokio`, branch `master @ 82fe082e`. Set up with `jj git init` and create topic bookmark `expose-uring-ops`.

Tokio already has the entire mechanism internally; the patch is mostly visibility changes plus a small refactor of the cancellation path so external consumers don't need a tokio change per opcode.

### Files to modify

- `tokio/src/macros/cfg.rs` — split `cfg_io_uring!` (lines 735-748). Today it requires `feature = "fs"`, which is wrong for a non-fs consumer. New shape:
  - `cfg_io_uring!` keeps `tokio_unstable + feature="io-uring" + feature="rt" + target_os="linux"` (drops fs). Gates the driver internals and the new public module.
  - `cfg_io_uring_fs!` adds `feature="fs"`. Gates `tokio/src/io/uring/{open,read,write}.rs` and the `fs/file.rs` + `fs/read_uring.rs` uring paths.
  - Audit each `cfg_io_uring!` call site (`runtime/io/driver.rs:5,54-61`, `runtime/driver.rs:374`, `io/mod.rs:306`) and switch fs-only ones over.

- `tokio/src/runtime/driver/op.rs` — promote internal `Op<T>` to public `Submission<T>`:
  - Make `Completable` `pub`, with `complete(self, cqe: CqeResult) -> Self::Output` and `complete_with_error(self, error: io::Error) -> Self::Output`. Bounds: `Self: Send + 'static`.
  - Replace the `Cancellable` requirement with an optional `Cancellable: Completable { fn on_cancel(self) {} }` trait — almost no consumer needs it; `Drop` on `Self` runs when the slot frees. Current `Open` rescues an FD on cancel; move that logic into `impl Cancellable for Open`.
  - Add `flags: u32` to `CqeResult` (sourced from `cqe.flags()`) so consumers can read `IORING_CQE_F_MORE` etc.
  - Replace the `CancelData` enum stored in `Lifecycle::Cancelled` with `Box<dyn FnOnce(CqeResult) + Send>` — the cleanup closure captures `self: T` at cancellation time. Removes the hard-coded per-opcode enum that today gates new ops behind a tokio change.
  - Rename type: keep `Op<T>` as a `pub(crate)` alias of `Submission<T>` if there are too many existing call sites to change, or do the rename. Either is fine for correctness; the rename is cleaner.

- `tokio/src/runtime/io/driver/uring.rs` — update `dispatch_completions` (lines 65-106) and `Drop for UringContext` (lines 134-173) to invoke the stored cleanup closure on the Cancelled variant. `register_op` (262-296) and `cancel_op` (298-328) stay `pub(crate)` — the public surface goes through `Submission`.

- `tokio/src/runtime/io/uring.rs` — **new file**, the public module:
  ```rust
  pub use crate::runtime::driver::op::{Submission, Completable, Cancellable, CqeResult};
  pub async fn is_supported(opcode: u8) -> std::io::Result<bool>;
  pub fn is_ready(opcode: u8) -> bool;
  ```
  `is_supported` delegates to `Handle::current().inner.driver().io().check_and_init(opcode)` (already at `uring.rs:218`); `is_ready` delegates to `is_uring_ready` (`uring.rs:191`).

- `tokio/src/runtime/io/mod.rs` — `cfg_io_uring! { pub mod uring; }`.

- `tokio/src/lib.rs` — one paragraph under the `tokio_unstable` doc block (~line 427) documenting `tokio::runtime::io::uring`.

### Public API shape

```rust
// tokio::runtime::io::uring
pub trait Completable: Send + 'static {
    type Output;
    fn complete(self, cqe: CqeResult) -> Self::Output;
    fn complete_with_error(self, error: std::io::Error) -> Self::Output;
}

pub trait Cancellable: Completable {
    fn on_cancel(self) {}                                 // default = drop
}

#[derive(Debug)]
#[non_exhaustive]
pub struct CqeResult {
    pub result: std::io::Result<u32>,
    pub flags: u32,
}

pub struct Submission<T: Completable> { /* private */ }

impl<T: Completable> Submission<T> {
    /// # Safety
    /// Every pointer/FD/iovec referenced by `entry` must live until either the
    /// future completes or `data` is dropped by the runtime after the CQE arrives.
    /// The conventional way: own everything inside `data`.
    /// `entry.user_data` is overwritten by the runtime.
    pub unsafe fn new(entry: io_uring::squeue::Entry, data: T) -> Self;
}

impl<T: Completable> Future for Submission<T> { type Output = T::Output; ... }
```

The future is `Send` iff `T: Send` (which the trait requires). No `LocalSet` plumbing needed — submission works from any task on the runtime.

### Tests

`tokio/tests/uring_public_api.rs`, gated on the new `cfg_io_uring!`:

1. `nop_roundtrip` — submit `opcode::Nop`, await, assert `Ok(0)`.
2. `drop_in_flight_does_not_leak` — submit Nop, drop the future immediately, yield; the cleanup closure must run.
3. `readv_dev_zero` — submit a ReadV against `/dev/zero` with a `Box<[u8; 4096]>` owned by `Self`; exercises the buffer-lifetime path.

### Build verification

```
cd ~/tokio
RUSTFLAGS="--cfg tokio_unstable" cargo build -p tokio --features "io-uring rt"
RUSTFLAGS="--cfg tokio_unstable" cargo test -p tokio --features "io-uring rt" --test uring_public_api
```

---

## Part B — tokio-epoll-uring: second backend driven by tokio

Repo: `~/tokio-epoll-uring`, branch `main` (clean). Set up with `jj git init` and create topic bookmark `tokio-upstream-backend`.

### Backend selection

Env var `TOKIO_EPOLL_URING_BACKEND` read once via `OnceCell` in `src/lib.rs` (extend `env_tunables` at lines 97-146 and `assert_no_unknown_env_vars`). Values: `side-ring` (default) and `tokio-upstream`. Process-global; matches the existing `EPOLL_URING_*` env-var pattern.

This keeps `System::launch`, `System::launch_with_metrics`, `thread_local_system`, `Handle`, `SystemHandle`, all op methods, the `Op` trait, errors — **byte-identical**.

### Internal dispatch

`SystemHandleInner` at `src/system/lifecycle/handle.rs:29` becomes a private enum:

```rust
enum SystemHandleInner<M> {
    SideRing { id, submit_side: SubmitSide, per_system_metrics: Arc<M> },
    Upstream { id, per_system_metrics: Arc<M> },
}
```

Each public method on `SystemHandle` matches once and routes. The match is a 1-line cost; the side-ring hot path stays monomorphized over `Op` (no `dyn` boxing). The Upstream variant carries no ring/poller/slots state.

### Upstream backend module

New `src/system/backend_upstream/`:

- `mod.rs` — `pub(crate) async fn execute_op_upstream<O: Op>(op: O) -> (O::Resources, Result<O::Success, Error<O::Error>>)`. Body:
  ```rust
  let sqe = op.make_sqe();
  let adapter = OpCompletable { op };
  let fut = unsafe { tokio::runtime::io::uring::Submission::new(sqe, adapter) };
  fut.await   // returns (Resources, Result<Success, Error>)
  ```
  No `Error::System(SystemShuttingDown)` path — tokio owns the ring's lifecycle.

- `op_adapter.rs` — adapter bridges tokio's `Completable` (CQE-based) to TEU's `Op::on_op_completion(i32)`:
  ```rust
  struct OpCompletable<O: Op> { op: O }

  impl<O: Op> Completable for OpCompletable<O> {
      type Output = (O::Resources, Result<O::Success, O::Error>);
      fn complete(self, cqe: CqeResult) -> Self::Output {
          let res: i32 = match cqe.result {
              Ok(n)  => n as i32,
              Err(e) => -e.raw_os_error().unwrap_or(libc::EIO),
          };
          self.op.on_op_completion(res)
      }
      fn complete_with_error(self, _e: std::io::Error) -> Self::Output {
          // Tokio failed to submit. TEU's failed-submission path drops resources.
          (self.op.on_failed_submission(), Err(/* Error::Op equivalent */))
      }
  }
  ```
  The `i32` round-trip is lossless because `Op::on_op_completion` already decodes negative-errno into `io::Error::from_raw_os_error`.

### `execute_op` refactor

`src/system/submission/op_fut.rs:54-155` — rename body to `execute_op_side_ring`. The `Op` public trait at lines 6-13 is **unchanged**. Add a thin dispatcher on `SystemHandleInner`:

```rust
impl<M: PerSystemMetrics> SystemHandleInner<M> {
    pub(crate) fn dispatch_op<O: Op>(&self, op: O) -> impl Future<Output = (O::Resources, Result<O::Success, Error<O::Error>>)> + Send {
        match self {
            Self::SideRing { submit_side, .. } => Either::Left(execute_op_side_ring(op, submit_side.clone())),
            Self::Upstream { .. }              => Either::Right(execute_op_upstream(op)),
        }
    }
}
```

Each public method in `src/system/lifecycle/handle.rs:104-271` swaps its direct `execute_op` call for `self.inner.dispatch_op(op)`. Lines 121-140 (read), 154-173 (write), and the others all collapse to one-line dispatches.

### Shutdown / lifecycle

`SystemHandle::initiate_shutdown` (`handle.rs:69-76`):
- SideRing arm: today's `inner.shutdown()`.
- Upstream arm: `async {}`. Document in rustdoc that for the upstream backend, ring teardown is the tokio runtime's responsibility.

`SystemHandle::Drop`: SideRing arm runs today's logic; Upstream arm is a no-op.

`thread_local_system()` (`thread_local.rs:7-33`): unchanged source. The per-thread `OnceCell<SystemHandle>` still works; the Upstream variant is essentially a zero-sized inner plus `Arc<M>`. The existing weak-ref drop-test in `tests.rs:36-128` still passes because the `Arc` shape is preserved.

### Metrics

`src/metrics.rs` — `systems_created` / `systems_destroyed` bump in both arms. `observe_slots_submission_queue_depth` is never called from the Upstream backend (no slots); document this in the rustdoc on `PerSystemMetrics`. Don't try to plumb tokio-internal metrics — not stable.

### Cargo / tokio dependency

`tokio-epoll-uring/Cargo.toml` — add a path dependency on `~/tokio` (or `git` ref against a local commit). Enable the `io-uring` feature on tokio. Add `.cargo/config.toml` setting `rustflags = ["--cfg", "tokio_unstable"]` for the workspace so the new tokio API is reachable without polluting every command line.

### Tests

Existing test suite at `src/system/tests.rs` continues to exercise the side-ring backend by default (no env var). Add a new integration test binary (so the env-var setting doesn't race between tests in one process): `tokio-epoll-uring/tests/upstream_smoke.rs`. Sets `TOKIO_EPOLL_URING_BACKEND=tokio-upstream` in the binary's `main` test attrs and runs a small subset — nop, read against `/dev/zero`, drop-while-pending.

---

## Part C — benchmark engine

`benchmark/src/engines/tokio_epoll_uring_upstream.rs` — clone of `engines/tokio_epoll_uring.rs`, with two changes:

1. In `new()`, `std::env::set_var("TOKIO_EPOLL_URING_BACKEND", "tokio-upstream")`. Process-wide is fine because the benchmark binary runs one engine per process.
2. Construct the tokio runtime with whatever opt-in tokio's API requires (likely `Builder::new_multi_thread().enable_all()` with the `io-uring` feature compiled in is enough; the ring lazy-inits on first use via tokio's `check_and_init`).

Wiring:

- `benchmark/src/engines.rs:1-5` — add `pub(crate) mod tokio_epoll_uring_upstream;`.
- `benchmark/src/main.rs:18-22, 151-159, 646-656` — add `EngineKind::TokioEpollUringUpstream` enum variant and a `setup_engine` arm that returns the new engine. CLI name: `tokio-epoll-uring-upstream`.
- `benchmark/scripts/runbench.sh:79-104` — append `tokio-epoll-uring-upstream` to both `compare_engines` loops (the 400-client `500k-ios-per-client` run and the 1200-client `12k-ios-per-client` run). The default `*)` case in `run()` (lines 32-34) already invokes anything by name with the same `common_args`.

The client loop (`thread_local_system().await; handle.read(file, offset, owned_buf).await`) is byte-for-byte identical — the point of the design is that the benchmark engine doesn't know which backend is active.

---

## Critical files

**tokio (PR-shaped):**
- `~/tokio/tokio/src/runtime/driver/op.rs` (Op→Submission, Completable/Cancellable public, CqeResult.flags, Cancelled-as-closure)
- `~/tokio/tokio/src/runtime/io/driver/uring.rs` (closure-call in dispatch & drop)
- `~/tokio/tokio/src/runtime/io/uring.rs` (new public module)
- `~/tokio/tokio/src/runtime/io/mod.rs` (export)
- `~/tokio/tokio/src/macros/cfg.rs` (split cfg_io_uring / cfg_io_uring_fs)
- `~/tokio/tokio/tests/uring_public_api.rs` (new)

**tokio-epoll-uring (backend swap, public surface intact):**
- `~/tokio-epoll-uring/Cargo.toml` + `.cargo/config.toml` (tokio path dep + tokio_unstable)
- `~/tokio-epoll-uring/tokio-epoll-uring/src/lib.rs:97-146` (env var)
- `~/tokio-epoll-uring/tokio-epoll-uring/src/system/lifecycle.rs:101-232` (branch on backend)
- `~/tokio-epoll-uring/tokio-epoll-uring/src/system/lifecycle/handle.rs:25-272` (enum SystemHandleInner, dispatcher, shutdown branch)
- `~/tokio-epoll-uring/tokio-epoll-uring/src/system/submission/op_fut.rs:54-155` (rename to execute_op_side_ring; keep public Op trait)
- `~/tokio-epoll-uring/tokio-epoll-uring/src/system/backend_upstream/{mod,op_adapter}.rs` (new)
- `~/tokio-epoll-uring/tokio-epoll-uring/tests/upstream_smoke.rs` (new)

**benchmark:**
- `~/tokio-epoll-uring/benchmark/src/engines/tokio_epoll_uring_upstream.rs` (new)
- `~/tokio-epoll-uring/benchmark/src/engines.rs:1-5`
- `~/tokio-epoll-uring/benchmark/src/main.rs:18-22,151-159,646-656`
- `~/tokio-epoll-uring/benchmark/scripts/runbench.sh:79-104`

---

## Verification

End-to-end flow, all on `/ephemeral` (NVMe, EC2 InstanceStore):

```bash
# 1. tokio side
cd ~/tokio
jj git init
jj new -m "expose-uring-ops: public Submission<T>+Completable API"
# (edit files per Part A)
RUSTFLAGS="--cfg tokio_unstable" cargo build -p tokio --features "io-uring rt"
RUSTFLAGS="--cfg tokio_unstable" cargo test -p tokio --features "io-uring rt" --test uring_public_api

# 2. tokio-epoll-uring side
cd ~/tokio-epoll-uring
jj git init
jj new -m "tokio-upstream backend"
# (edit files per Part B)
cargo build --all-targets        # both backends compile in
cargo test                       # existing tests, default backend (side-ring)
cargo test --test upstream_smoke # new backend smoke

# 3. benchmark build
cargo build --release -p benchmark --target x86_64-unknown-linux-musl

# 4. run sweep on /ephemeral
cp target/x86_64-unknown-linux-musl/release/benchmark /ephemeral/benchmark
cp benchmark/scripts/{postprocess.py,runbench.sh} /ephemeral/
cd /ephemeral && bash runbench.sh && python3 postprocess.py
# Inspect totals.csv: tokio-epoll-uring-upstream should appear alongside the
# existing engines, with comparable IOPS/latency at the 400-client run and
# competitive (or known-different) behavior at 1200 clients.
```

What "success" looks like for the demo:
- `tokio-epoll-uring-upstream` produces non-zero results without panics or hangs through the full 400-client and 1200-client sweeps.
- p50 / p99 read latency is within ~2× of `tokio-epoll-uring--no-force-yield` on the cached-IO workload. If it's worse than that, that's a useful finding for the funding pitch (specific bottleneck to address in Q3) rather than a blocker.
- Drop-while-pending behavior is correct (no UAF, no leaked FDs) — confirmed by the smoke test and by running the benchmark under ASAN if there's time.

## Risks / known-unknowns

- **Send bounds on the new tokio API.** Plan assumes `Submission<T>` is `Send` when `T: Send`. If tokio's reviewers push for `!Send` (per-worker affine), all `SystemHandle` methods will need a `LocalSet` adapter. The design has this as a single-point-of-change in `execute_op_upstream` so if it surfaces, only the adapter changes.
- **Runtime construction mismatch.** Setting `TOKIO_EPOLL_URING_BACKEND=tokio-upstream` requires the tokio runtime to have the io-uring feature compiled in. If a user sets the env var with a vanilla runtime, `Submission::new`'s `Handle::current().io()` path will not find a ring and will return `ENOSYS` (graceful). The benchmark engine forces the right runtime in its `new()`.
- **`Op::on_op_completion` takes `i32`.** Adapter converts `CqeResult` → `i32` by `Ok(n) ⇒ n as i32`, `Err(e) ⇒ -e.raw_os_error()`. This round-trips because `Op::on_op_completion` already decodes negative-errno into `io::Error::from_raw_os_error`. No information lost for current opcodes; if a future op needs `cqe.flags`, that's a separate `Op2` trait — out of scope.
- **No `pushing` per user constraint.** All work stays local via `jj`. The tokio patch sits on a topic bookmark; the funding demo references the local patch as a path dep, not a PR URL. If the demo lands funding, the tokio patch is upstreamed in Q3.
