use std::{fmt::Display, sync::Arc};

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

use crate::system::{
    completion::ProcessCompletionsCause,
    slots::{self, InflightHandleError},
};

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

    static PROCESS_COMPLETIONS_ON_SUBMIT: once_cell::sync::Lazy<bool> =
        once_cell::sync::Lazy::new(|| {
            std::env::var("EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT")
                .map(|v| v == "1")
                .unwrap_or_else(|e| match e {
                    std::env::VarError::NotPresent => true, // default-on
                    std::env::VarError::NotUnicode(_) => {
                        panic!("EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT must be a unicode string")
                    }
                })
        });

    let inflight = submit_side.with_submit_side_open(|submit_side| {

        let submit_side = match submit_side {
            Some(submit_side) => submit_side,
            None => return Err((
                    op.on_failed_submission(),
                    Err(Error::System(OpError::SystemShuttingDown)),
                )),
        };

        // inlined finish-submit

        let (sqe, inflight_op_fut) = match slot.use_for_op(op) {
            Ok((sqe, inflight_op_handle)) => (sqe, inflight_op_handle),
            Err((op, slots::UseError::SlotsDropped)) => {
                return Err((
                    op.on_failed_submission(),
                    Err(Error::System(OpError::SystemShuttingDown)),
                ));
            }
        };

        // this allows us to keep the possible guard in cq_guard because the arc lives on stack
        #[allow(unused_assignments)]
        let mut cq_owned = None;

        let cq_guard = if *PROCESS_COMPLETIONS_ON_SUBMIT {
            let cq = Arc::clone(&submit_side.completion_side);
            cq_owned = Some(cq);
            Some(cq_owned.as_ref().expect("we just set it").lock().unwrap())
        } else {
            None
        };

        if submit_side.submit_raw(sqe).is_err() {
            // TODO: DESIGN: io_uring can deal have more ops inflight than the SQ.
            // So, we could just submit_and_wait here. But, that'd prevent the
            // current executor thread from making progress on other tasks.
            //
            // So, for now, keep SQ size == inflight ops size == Slots size.
            // This potentially limits throughput if SQ size is chosen too small.
            //
            // FIXME: why not just async mutex?
            unreachable!("the `ops` has same size as the SQ, so, if SQ is full, we wouldn't have been able to get this slot");
        }

        if let Some(mut cq) = cq_guard {
            // opportunistically process completion immediately
            // TODO do it during ::poll() as well?
            //
            // FIXME: why are we doing this while holding the SubmitSideOpen
            cq.process_completions(ProcessCompletionsCause::Regular);
        }

        Ok(inflight_op_fut)
    });

    let inflight = match inflight {
        Ok(fut) => fut,
        Err(ret) => return ret,
    };

    // now submitted
    let (resources, res) = inflight.await;
    // FIXME: this should be an into
    let res = res.map_err(|e| match e {
        InflightHandleError::Completion(err) => Error::Op(err),
        InflightHandleError::SlotsDropped => Error::System(OpError::SystemShuttingDown),
    });

    (resources, res)
}
