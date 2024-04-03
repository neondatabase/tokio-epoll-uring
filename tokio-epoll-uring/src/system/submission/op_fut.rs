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

use crate::system::{
    completion::ProcessCompletionsCause,
    slots::{self, SlotHandle},
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

pub(crate) async fn execute_op<O>(
    op: O,
    submit_side: SubmitSideWeak,
    slot: Option<SlotHandle>,
) -> (O::Resources, Result<O::Success, Error<O::Error>>)
where
    // FIXME: probably dont need the unpin
    O: Op + Send + 'static + Unpin,
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
        // If PROCESS_COMPLETIONS_ON_SUBMIT is enabled, acquire the completion queue
        // guard before submission so that we, not the poller task, are guaranteed to
        // observe an immediate completion. TODO: the poller task may still be woken
        // up through the epoll of the io_uring fd, investigate whether that actually
        // happens and whether there's a way around it.

        #[allow(unused_assignments)]
        let mut cq_owned = None;
        let cq_guard = if *crate::env_tunables::PROCESS_COMPLETIONS_ON_SUBMIT {
            let cq = Arc::clone(&open_guard.completion_side);
            cq_owned = Some(cq);
            Some(cq_owned.as_ref().expect("we just set it").lock().unwrap())
        } else {
            None
        };

        if open_guard.submit_raw(sqe).is_err() {
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

        drop(open_guard); // drop it asap to enable timely shutdown

        if let Some(mut cq) = cq_guard {
            // opportunistically process completion immediately
            // TODO do it during ::poll() as well?
            cq.process_completions(ProcessCompletionsCause::Regular);
        }
    }

    match slot {
        Some(slot) => slot.use_for_op(op, |sqe| do_submit(open_guard, sqe)).await,
        None => {
            match open_guard.slots.try_get_slot() {
                slots::TryGetSlotResult::Draining => (
                    op.on_failed_submission(),
                    Err(Error::System(SystemError::SystemShuttingDown)),
                ),
                slots::TryGetSlotResult::GotSlot(slot) => {
                    slot.use_for_op(op, |sqe| do_submit(open_guard, sqe)).await
                }
                slots::TryGetSlotResult::NoSlots(later) => {
                    // All slots are taken and we're waiting in line.
                    // If enabled, do some opportunistic completion processing to wake up futures that will release ops slots.
                    // This is in the hope that we'll wake ourselves up.

                    if *crate::env_tunables::PROCESS_COMPLETIONS_ON_QUEUE_FULL {
                        // TODO shouldn't we loop here until we've got a slot? This one-off poll doesn't make much sense.
                        open_guard.submitter.submit().unwrap();
                        open_guard
                            .completion_side
                            .lock()
                            .unwrap()
                            .process_completions(ProcessCompletionsCause::Regular);
                    }
                    let slot = match later.await {
                        Ok(slot) => slot,
                        Err(_dropped) => {
                            return (
                                op.on_failed_submission(),
                                Err(Error::System(SystemError::SystemShuttingDown)),
                            )
                        }
                    };
                    slot.use_for_op(op, |sqe| do_submit(open_guard, sqe)).await
                }
            }
        }
    }
}
