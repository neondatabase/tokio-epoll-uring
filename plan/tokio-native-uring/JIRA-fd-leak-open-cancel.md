# tokio-epoll-uring: `OpenAtOp` leaks file descriptors when its future is cancelled after successful kernel completion

## Summary

Cancelling a `tokio_epoll_uring::SystemHandle::open` future (i.e. dropping it) **after** the kernel has already produced the open's CQE causes the resulting file descriptor to leak. The CQE is delivered to the slot but the cancellation path drops the boxed op without inspecting the CQE result, so the FD that the kernel allocated for us is never wrapped in an `OwnedFd` and therefore never `close(2)`d.

This is a pre-existing TEU design issue, found by code review during the Q2 demo prototyping work on switching TEU to drive tokio's own io_uring ring. The new "upstream backend" (multi-ring tokio) inherits the same bug because TEU's `Op` trait does not have a CQE-aware on-cancel hook.

## Reproduction

Spawn N tokio tasks that each `tokio::select! { _ = system.open(path, opts) => {}, _ = timeout => {} }`. If the timeout wins after the kernel has already opened the file but before the polling future runs again, the FD leaks. Observable via `ls /proc/<pid>/fd | wc -l` over time. Aggressively-cancelling-open workloads (config reload, request timeouts) accumulate leaks and eventually hit `EMFILE`.

## Affected code

- **Side-ring backend (the production default):** `tokio-epoll-uring/src/system/slots.rs:519-521`. The `PendingButFutureDropped` variant drops the boxed `Op` when the CQE arrives, never calling `on_op_completion` with the result. For `OpenAtOp` the CQE carries the FD.
- **New upstream backend** (local-only on bookmark `tokio-upstream-backend`): `tokio-epoll-uring/src/system/backend_upstream.rs`. The `OpCompletable<O>` adapter uses the default empty `tokio::io_uring::Cancellable::on_cancel` — identical leak pattern.

## Why it isn't a multi-ring patch regression

The side-ring backend has had this bug since at least the original `PendingButFutureDropped` mechanism shipped. Code review of the Q2 demo prototypes (multi-ring tokio + upstream TEU adapter) surfaced the issue while checking the new backend's cancellation invariants. It is a TEU-wide design gap, not a new regression.

tokio's own internal `Open` op (in the patched tokio at `tokio/src/io/uring/open.rs:32-44`) shows the correct pattern: a custom `Cancellable::on_cancel(self, cqe: CqeResult)` impl that wraps the CQE's FD in `OwnedFd::from_raw_fd(...)` so its `Drop` closes it. The cancellation test `tokio/tests/fs_uring_cancel_open.rs` asserts "leaked ≤ 64" over 128 open+cancel iterations.

## Proposed fix

Add a CQE-aware on-cancel hook to TEU's `Op` trait, e.g.:

```rust
pub trait Op: ... {
    type Resources;
    type Success;
    type Error;
    fn make_sqe(&mut self) -> Entry;
    fn on_failed_submission(self) -> Self::Resources;
    fn on_op_completion(self, res: i32)
        -> (Self::Resources, Result<Self::Success, Self::Error>);
    /// Default: drop self. Override when the kernel allocates a
    /// resource that lives only in the CQE (e.g. Open's returned FD).
    fn on_cancel(self, _res: i32) where Self: Sized {}
}
```

Override for `OpenAtOp`: if `res >= 0`, `unsafe { OwnedFd::from_raw_fd(res); }` — its `Drop` closes the FD. Other ops keep the default (no-op).

Then thread the hook through both backends:

- **Side-ring:** in `slots.rs::process_completion`, when the slot is in `PendingButFutureDropped`, call `op.on_cancel(res)` instead of letting the box drop.
- **Upstream backend:** in `backend_upstream.rs`, implement `Cancellable::on_cancel(self, cqe)` for `OpCompletable<O>` to forward to `self.op.on_cancel(res)`.

## Severity

**Medium.** Leak rate equals (rate of cancelled opens) × (P[kernel completes before drop]). PageServer typically opens persistently with relatively low cancellation rate, so the leak is slow but unbounded. Reaches `EMFILE` after enough cancelled opens at the kernel's `RLIMIT_NOFILE` (typically 1024 or higher in containerised deployments).

## References

- Discovered: Q2 2026 demo prototyping for the multi-ring tokio + upstream-TEU-backend work. See `ITER_LOG.md` ITER 9 in the local `tokio-upstream-backend` bookmark.
- Correct pattern in tokio: `tokio/src/io/uring/open.rs` (master); the test `tokio/tests/fs_uring_cancel_open.rs` asserts the FD-leak-on-cancel bound.
- Affected code (side-ring): `tokio-epoll-uring/src/system/slots.rs`, look for `PendingButFutureDropped`.

## Labels

`tokio-epoll-uring`, `io-uring`, `fd-leak`, `pageserver`
