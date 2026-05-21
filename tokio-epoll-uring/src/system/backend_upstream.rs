//! `tokio_upstream` backend: submits SQEs through tokio's own io_uring ring
//! via the public `tokio::io_uring` API instead of running a side ring.
//!
//! Selected at process start via `TOKIO_EPOLL_URING_BACKEND=tokio-upstream`.
//! The crate's public API is identical to the side-ring backend; the only
//! difference is where SQEs/CQEs flow.

use crate::system::submission::op_fut::{Error, Op, SystemError};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io_uring::{is_ready, is_supported, Cancellable, Completable, CqeResult, Submission};
use uring_common::io_uring;

/// Process-wide bitmap of opcodes we've already confirmed are supported by
/// the running kernel. Bit `n` set ⇒ opcode `n` was successfully probed via
/// `tokio::io_uring::is_supported`, the tokio uring driver is initialised,
/// and `Submission::new` will accept SQEs of this opcode without panicking.
///
/// Why: `tokio::io_uring::is_supported(opcode)` is async because it may
/// perform the one-time `io_uring_setup` / `IORING_REGISTER_PROBE`. After
/// the first hit it's effectively a `OnceCell::get + probe.is_supported`,
/// but it's still an `.await` per SQE which shows up under perf
/// (additional future-state-machine + waker). The hot path runs millions
/// of times per second on busy benchmarks — we cache the per-opcode result
/// in an `AtomicU64` and skip the await once we've seen the opcode
/// supported.
///
/// Opcodes in current kernels are u8 but only ~60 distinct values are
/// defined, so a single u64 holds them all.
static OPCODE_SUPPORTED_BITMAP: AtomicU64 = AtomicU64::new(0);

/// Adapter that turns a tokio-epoll-uring [`Op`] into something tokio's
/// `Submission<T>` can drive.
///
/// tokio passes the full CQE to [`Completable::complete`], but TEU's `Op`
/// trait takes a single `i32` `res`. We convert: a successful CQE becomes
/// the non-negative i32; an io::Error becomes the negated errno. This is
/// lossless because `Op::on_op_completion` already maps negative-errno
/// values to `io::Error::from_raw_os_error(-res)`.
///
/// The upstream backend currently restricts ops to `Error = std::io::Error`
/// because tokio's `complete_with_error(io::Error)` would otherwise be
/// untranslatable into the op's error type. Every op in the crate today
/// already meets this constraint.
struct OpCompletable<O: Op<Error = std::io::Error>> {
    op: O,
}

impl<O: Op<Error = std::io::Error>> Completable for OpCompletable<O> {
    type Output = (O::Resources, Result<O::Success, std::io::Error>);

    fn complete(self, cqe: CqeResult) -> Self::Output {
        let res: i32 = match cqe.result {
            Ok(n) => n as i32,
            Err(e) => -e.raw_os_error().unwrap_or(libc::EIO),
        };
        self.op.on_op_completion(res)
    }

    fn complete_with_error(self, err: std::io::Error) -> Self::Output {
        // Tokio failed to submit the SQE synchronously (e.g. ENOSYS during
        // ring init). Hand resources back to the caller and surface the
        // io::Error in the Result.
        (self.op.on_failed_submission(), Err(err))
    }
}

impl<O: Op<Error = std::io::Error>> Cancellable for OpCompletable<O> {}

/// Drive one operation to completion through tokio's ring.
///
/// Equivalent in semantics to the side-ring `execute_op` (slot acquisition
/// + submission + completion future), but with no side ring or slots —
/// tokio owns those. The future resolves to `(Resources, Result<Success,
/// Error<OpError>>)` identical to the side-ring path.
pub(crate) async fn execute_op_upstream<O>(
    mut op: O,
) -> (O::Resources, Result<O::Success, Error<O::Error>>)
where
    O: Op<Error = std::io::Error> + Send + 'static,
{
    let sqe = op.make_sqe();
    let opcode = sqe_opcode(&sqe);
    // Fast path: we've previously confirmed this opcode is supported and
    // the tokio uring probe has been initialised. Skip the async probe.
    if opcode < 64 && OPCODE_SUPPORTED_BITMAP.load(Ordering::Relaxed) & (1u64 << opcode) != 0 {
        // Already supported.
    } else if opcode < 64 && is_ready(opcode) {
        // tokio's probe was initialised for a different opcode; this one
        // happens to be in the cached probe and supported. Mark and skip.
        OPCODE_SUPPORTED_BITMAP.fetch_or(1u64 << opcode, Ordering::Relaxed);
    } else {
        // First time we see this opcode (or the tokio probe hasn't run
        // yet at all). Pay the async-probe cost once; cache the result.
        let supported = is_supported(opcode).await.unwrap_or(false);
        if !supported {
            return (
                op.on_failed_submission(),
                Err(Error::System(SystemError::SystemShuttingDown)),
            );
        }
        if opcode < 64 {
            OPCODE_SUPPORTED_BITMAP.fetch_or(1u64 << opcode, Ordering::Relaxed);
        }
    }
    let adapter = OpCompletable { op };
    // SAFETY: `op` (now inside `adapter`) owns every kernel-readable
    // resource (FD, buffer) for the entire duration of the operation.
    // tokio holds `adapter` until either the future completes or the CQE
    // arrives after cancellation; both keep the resources alive.
    let (resources, op_result) = unsafe { Submission::new(sqe, adapter) }.await;
    let result = match op_result {
        Ok(s) => Ok(s),
        Err(e) => {
            // We can't easily distinguish "tokio failed to submit" from
            // "kernel returned an error" here — both surface as
            // `Result::Err(io::Error)`. The side-ring backend uses
            // `Error::Op(io::Error)` for kernel-side failures and
            // `Error::System(SystemShuttingDown)` for submission failures.
            // Map ENOSYS specifically to the System variant (the only
            // failure path tokio synthesises today); everything else as
            // Op.
            if e.raw_os_error() == Some(libc::ENOSYS) {
                Err(Error::System(SystemError::SystemShuttingDown))
            } else {
                Err(Error::Op(e))
            }
        }
    };
    (resources, result)
}

/// Extract the opcode byte from an SQE. The opcode is the first byte of
/// the `io_uring_sqe` struct.
fn sqe_opcode(entry: &io_uring::squeue::Entry) -> u8 {
    // The wire layout of io_uring_sqe begins with `__u8 opcode` at byte 0.
    let ptr = entry as *const io_uring::squeue::Entry as *const u8;
    // SAFETY: io-uring's Entry is `#[repr(C)]` over the kernel's sqe layout.
    unsafe { *ptr }
}
