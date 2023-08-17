use std::{fmt::Display, pin::Pin, sync::Arc};

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

use futures::future;

use crate::system::{
    completion::ProcessCompletionsCause,
    slots::{self, SlotHandle},
};

use super::SubmitSideWeak;

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
    let submit_side_weak = submit_side.clone();

    submit_side.with_submit_side_open(|submit_side| {
        let open: &mut super::SubmitSideOpen = match submit_side {
            Some(submit_side) => submit_side,
            None => return Fut::A(async move {(
                    op.on_failed_submission(),
                    Err(Error::System(SystemError::SystemShuttingDown)),
                )}),
        };

        let do_submit = |submit_side: &mut super::SubmitSideOpen, sqe|{
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

            // this allows us to keep the possible guard in cq_guard because the arc lives on stack
            #[allow(unused_assignments)]
            let mut cq_owned = None;

            let cq_guard = if *crate::env_tunables::PROCESS_COMPLETIONS_ON_SUBMIT {
                let cq = Arc::clone(&submit_side.completion_side);
                cq_owned = Some(cq);
                Some(cq_owned.as_ref().expect("we just set it").lock().unwrap())
            } else {
                None
            };

            if let Some(mut cq) = cq_guard {
                // opportunistically process completion immediately
                // TODO do it during ::poll() as well?
                //
                // FIXME: why are we doing this while holding the SubmitSideOpen
                cq.process_completions(ProcessCompletionsCause::Regular);
            }
        };

        match slot {
            Some(slot) => Fut::B({
                slot.use_for_op(op, do_submit, open)
            }),
            None => {
                match open.slots.try_get_slot() {
                    slots::TryGetSlotResult::Draining => {
                        Fut::C(async move { (op.on_failed_submission(), Err(Error::System(SystemError::SystemShuttingDown)))})
                    },
                    slots::TryGetSlotResult::GotSlot(slot) => {
                        Fut::D(async move {
                            submit_side_weak.with_submit_side_open(|submit_side| {
                                let open: &mut super::SubmitSideOpen = match submit_side {
                                    Some(submit_side) => submit_side,
                                    None => return future::Either::Left(async move {(op.on_failed_submission(), Err(Error::System(SystemError::SystemShuttingDown)))}),
                                };
                                future::Either::Right(slot.use_for_op(op, do_submit, open))
                            }).await
                        })
                    },
                    slots::TryGetSlotResult::NoSlots(later) => {
                        // All slots are taken and we're waiting in line.
                        // If enabled, do some opportunistic completion processing to wake up futures that will release ops slots.
                        // This is in the hope that we'll wake ourselves up.

                        if *crate::env_tunables::PROCESS_COMPLETIONS_ON_QUEUE_FULL {
                            // TODO shouldn't we loop here until we've got a slot? This one-off poll doesn't make much sense.
                            open.submitter.submit().unwrap();
                            open.completion_side
                                .lock()
                                .unwrap()
                                .process_completions(ProcessCompletionsCause::Regular);
                        }
                        Fut::E(async move {
                            let slot = match later.await {
                                Ok(slot) => slot,
                                Err(_dropped) => return (op.on_failed_submission(), Err(Error::System(SystemError::SystemShuttingDown))),
                            };
                            submit_side_weak.with_submit_side_open(|submit_side| {
                                let open: &mut super::SubmitSideOpen = match submit_side {
                                    Some(submit_side) => submit_side,
                                    None => return future::Either::Left(async move {(op.on_failed_submission(), Err(Error::System(SystemError::SystemShuttingDown)))}),
                                };
                                future::Either::Right(slot.use_for_op(op, do_submit, open))
                            }).await
                        })
                    }
                }
            }
        }
    }).await
}

// Used by `execute_op` to avoid boxing the future returned by the `with_submit_side` closure.
enum Fut<Output, A, B, C, D, E>
where
    A: std::future::Future<Output = Output>,
    B: std::future::Future<Output = Output>,
    C: std::future::Future<Output = Output>,
    D: std::future::Future<Output = Output>,
    E: std::future::Future<Output = Output>,
{
    A(A),
    B(B),
    C(C),
    D(D),
    E(E),
}
impl<Output, A, B, C, D, E> std::future::Future for Fut<Output, A, B, C, D, E>
where
    A: std::future::Future<Output = Output>,
    B: std::future::Future<Output = Output>,
    C: std::future::Future<Output = Output>,
    D: std::future::Future<Output = Output>,
    E: std::future::Future<Output = Output>,
{
    type Output = Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unsafe {
            match self.get_unchecked_mut() {
                Self::A(x) => Pin::new_unchecked(x).poll(cx),
                Self::B(x) => Pin::new_unchecked(x).poll(cx),
                Self::C(x) => Pin::new_unchecked(x).poll(cx),
                Self::D(x) => Pin::new_unchecked(x).poll(cx),
                Self::E(x) => Pin::new_unchecked(x).poll(cx),
            }
        }
    }
}
