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

use uring_common::io_uring;

use crate::{
    metrics::PerSystemMetrics,
    system::{
        completion::ProcessCompletionsCause,
        slots::{self, SlotHandle, Slots},
    },
};

use super::{SubmitSideOpenGuard, SubmitSideWeak};

#[derive(Debug, thiserror::Error)]
pub enum SystemError {
    #[error("shutting down")]
    SystemShuttingDown,
}

#[derive(thiserror::Error, Debug)]
pub enum Error<T> {
    System(SystemError),
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

pub(crate) async fn execute_op<O, M>(
    op: O,
    submit_side: SubmitSideWeak,
    slot: Option<SlotHandle>,
    per_system_metrics: Arc<M>,
) -> (O::Resources, Result<O::Success, Error<O::Error>>)
where
    // FIXME: probably dont need the unpin
    O: Op + Send + 'static + Unpin,
    M: PerSystemMetrics,
{
    let open_guard = match submit_side.upgrade_to_open().await {
        Some(open) => open,
        None => {
            return (
                op.on_failed_submission(),
                Err(Error::System(SystemError::SystemShuttingDown)),
            );
        }
    };

    fn do_submit(mut open_guard: SubmitSideOpenGuard, sqe: io_uring::squeue::Entry) {
        if open_guard.submit_raw(sqe).is_err() {
            todo!("1. can this even happen? doesn't io_uring_enter ever not make room?")
        }

        // this allows us to keep the possible guard in cq_guard because the arc lives on stack
        #[allow(unused_assignments)]
        let mut cq_owned = None;

        let cq_guard = if *crate::env_tunables::PROCESS_COMPLETIONS_ON_SUBMIT {
            let cq = Arc::clone(&open_guard.completion_side);
            cq_owned = Some(cq);
            Some(cq_owned.as_ref().expect("we just set it").lock().unwrap())
        } else {
            None
        };
        drop(open_guard); // drop it asap to enable timely shutdown

        if let Some(mut cq) = cq_guard {
            // opportunistically process completion immediately
            // TODO do it during ::poll() as well?
            //
            // FIXME: why are we doing this while holding the SubmitSideOpen
            cq.process_completions(ProcessCompletionsCause::Regular);
        }
    }

    let slot = open_guard.slots.submit_prepare();
    Slots::submit_and_wait(slot, op, |sqe| do_submit(open_guard, sqe)).await
}
