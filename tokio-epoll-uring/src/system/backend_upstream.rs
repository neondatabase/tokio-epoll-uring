//! `tokio_upstream` backend: submits SQEs through tokio's own io_uring ring
//! via the public `tokio::io_uring` API instead of running a side ring.
//!
//! Selected at process start via `TOKIO_EPOLL_URING_BACKEND=tokio-upstream`.
//! The crate's public API is identical to the side-ring backend; the only
//! difference is where SQEs/CQEs flow.

use crate::system::submission::op_fut::{Error, Op, SystemError};
use tokio::io_uring::{is_supported, Cancellable, Completable, CqeResult, Submission};
use uring_common::io_uring;

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
    // Always probe via `is_supported(opcode).await`. This is per-Handle
    // cached inside tokio (via `OnceCell` on `uring_probe`), so the
    // amortized cost is one async-future-poll per opcode per runtime.
    //
    // The previous `OPCODE_SUPPORTED_BITMAP` static was a process-wide
    // bypass — but tokio's `uring_probe` is per-Handle (per-runtime),
    // and consumers like PageServer construct multiple tokio runtimes
    // (e.g. a separate runtime per role). The bitmap would say "this
    // opcode is supported" — set by runtime A's probe — while runtime
    // B's probe was still uninitialised. The downstream
    // `register_op`'s `assert!(self.uring_probe.initialized())` then
    // fires. See pageserver basebackup-handler crash for the
    // motivating bug.
    let supported = is_supported(opcode).await.unwrap_or(false);
    if !supported {
        return (
            op.on_failed_submission(),
            Err(Error::System(SystemError::SystemShuttingDown)),
        );
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
