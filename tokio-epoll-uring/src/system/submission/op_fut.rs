use std::fmt::Display;

/// An io_uring operation and the resources it operates on.
///
/// For each io_uring operation, there is a struct that implements this trait.
pub trait Op: crate::sealed::Sealed + Sized + Send + 'static {
    type Resources;
    type Success;
    type Error;
    fn on_failed_submission(self) -> Self::Resources;
    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>);
    fn make_sqe(&mut self) -> io_uring::squeue::Entry;
}

use crate::system::slots::{self, InflightHandleError};

use super::SubmitSideWeak;

#[derive(Debug, thiserror::Error)]
pub enum OpError {
    #[error("shutting down")]
    SystemShuttingDown,
}

#[derive(thiserror::Error, Debug)]
pub enum Error<T> {
    System(OpError),
    Op(T),
}

impl<T: Display> Display for Error<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::System(e) => {
                if f.alternate() {
                    write!(f, "tokio-epoll-uring: {e:#}")
                } else {
                    write!(f, "tokio-epoll-uring: {e}")
                }
            }
            Error::Op(op) => Display::fmt(op, f),
        }
    }
}

pub(crate) async fn execute_op<O>(
    op: O,
    submit_side: SubmitSideWeak,
) -> (O::Resources, Result<O::Success, Error<O::Error>>)
where
    // FIXME: probably dont need the unpin
    O: Op + Send + 'static + Unpin,
{
    let slot = match submit_side.get_slot().await {
        Some(slot) => slot,
        None => {
            let res = op.on_failed_submission();
            return (res, Err(Error::System(OpError::SystemShuttingDown)));
        }
    };

    let ret = submit_side.with_submit_side_open(|submit_side| {
        let submit_side = match submit_side {
            Some(submit_side) => submit_side,
            None => {
                return Err((
                    op.on_failed_submission(),
                    Err(Error::System(OpError::SystemShuttingDown)),
                ))
            }
        };

        Ok(slot.use_for_op(op, submit_side))
    });

    match ret {
        Err(with_submit_side_err) => with_submit_side_err,
        Ok(use_for_op_ret) => match use_for_op_ret {
            Err((op, slots::UseError::SlotsDropped)) => (
                op.on_failed_submission(),
                Err(Error::System(OpError::SystemShuttingDown)),
            ),
            Ok(inflight) => {
                let (resources, res) = inflight.await;
                // FIXME: this should be an into
                let res = res.map_err(|e| match e {
                    InflightHandleError::Completion(err) => Error::Op(err),
                    InflightHandleError::SlotsDropped => Error::System(OpError::SystemShuttingDown),
                });
                (resources, res)
            }
        },
    }
}
