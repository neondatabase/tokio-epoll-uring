use std::{fmt::Display, ops::ControlFlow, sync::Arc};

use futures::FutureExt;
use tokio::sync::oneshot;

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
    slots::{self, InflightHandle, InflightHandleError, SlotHandle, TryGetSlotResult},
    submission::{SubmitError, SubmitSideOpen},
};

use super::SubmitSideWeak;

/// Generic [`Future`](std::future::Future) that represents an [`Op`], created by a method on [`crate::Ops`].
pub struct OpFut<O>
where
    O: Op + Send + 'static,
{
    state: OpFutState<O>,
}
pub(crate) enum OpFutState<O>
where
    O: Op + Send + 'static,
{
    Undefined,
    WaitForSlot {
        submit_side_weak: SubmitSideWeak,
        waiter: oneshot::Receiver<SlotHandle>,
        make_op: O,
    },
    Submitted(InflightHandle<O>),
    Err {
        make_op: O,
        err: OpError,
    },
    ReadyPolled,
}
#[derive(Debug, thiserror::Error)]
pub enum OpError {
    #[error("shutting down")]
    SystemShuttingDown,
    #[error("system is shut down")]
    SystemIsShutDown,
}

pub(crate) fn finish_submit<O>(
    op: O,
    unsafe_slot: SlotHandle,
    submit_side_open: &mut SubmitSideOpen,
) -> Result<InflightHandle<O>, (O, OpError)>
where
    O: Op + Send + 'static + Unpin,
{
    let (sqe, inflight_op_fut) = match unsafe_slot.use_for_op(op) {
        Ok((sqe, inflight_op_handle)) => (sqe, inflight_op_handle),
        Err((op, slots::UseError::SlotsDropped)) => {
            return Err((op, OpError::SystemIsShutDown));
        }
    };

    lazy_static::lazy_static! {
        static ref PROCESS_URING_ON_SUBMIT: bool =
            std::env::var("EPOLL_URING_PROCESS_URING_ON_SUBMIT")
                .map(|v| v == "1")
                .unwrap_or_else(|e| match e {
                    std::env::VarError::NotPresent => true, // default-on
                    std::env::VarError::NotUnicode(_) => panic!("EPOLL_URING_PROCESS_URING_ON_SUBMIT must be a unicode string"),
                });
    }
    // if we're going to process completions immediately, get the lock on the CQ so that
    // we are guaranteed to process completions before the poller task
    {
        let mut cq_owned = None;
        let cq_guard = if *PROCESS_URING_ON_SUBMIT {
            let cq = Arc::clone(&submit_side_open.completion_side);
            cq_owned = Some(cq);
            Some(cq_owned.as_ref().expect("we just set it").lock().unwrap())
        } else {
            None
        };
        assert_eq!(cq_owned.is_none(), cq_guard.is_none());

        match submit_side_open.submit_raw(sqe) {
            Ok(()) => {}
            Err(SubmitError::QueueFull) => {
                // TODO: DESIGN: io_uring can deal have more ops inflight than the SQ.
                // So, we could just submit_and_wait here. But, that'd prevent the
                // current executor thread from making progress on other tasks.
                //
                // So, for now, keep SQ size == inflight ops size == Slots size.
                // This potentially limits throughput if SQ size is chosen too small.
                unreachable!("the `ops` has same size as the SQ, so, if SQ is full, we wouldn't have been able to get this slot");
            }
        }

        if let Some(mut cq) = cq_guard {
            assert!(*PROCESS_URING_ON_SUBMIT);
            // opportunistically process completion immediately
            // TODO do it during ::poll() as well?
            cq.process_completions(ProcessCompletionsCause::Regular);
        } else {
            assert!(!*PROCESS_URING_ON_SUBMIT);
        }
    }

    Ok(inflight_op_fut)
}

impl<O> OpFut<O>
where
    O: Op + Send + Unpin + 'static,
{
    fn new_err(make_op: O, err: OpError) -> Self {
        OpFut {
            state: OpFutState::Err { make_op, err },
        }
    }

    pub(crate) fn new(make_op: O, submit_side: SubmitSideWeak) -> Self {
        submit_side.with_submit_side_open(|submit_side_open| {
            match submit_side_open {
                None => OpFut::new_err(make_op, OpError::SystemShuttingDown),
                Some(submit_side_open) => {
                    match submit_side_open.slots.try_get_slot() {
                        TryGetSlotResult::Draining => {
                            Self::new_err(make_op, OpError::SystemShuttingDown)
                        }
                        TryGetSlotResult::GotSlot(unsafe_slot) => {
                            let fut = match finish_submit(make_op, unsafe_slot, submit_side_open) {
                                Ok(fut) => fut,
                                Err((make_op, err)) => {
                                    return Self::new_err(make_op, err);
                                }
                            };
                            OpFut {
                                state: OpFutState::Submitted(fut),
                            }
                        }
                        TryGetSlotResult::NoSlots(waiter) => {
                            // All slots are taken and we're waiting in line.
                            // If enabled, do some opportunistic completion processing to wake up futures that will release ops slots.
                            // This is in the hope that we'll wake ourselves up.

                            lazy_static::lazy_static! {
                                static ref PROCESS_URING_ON_QUEUE_FULL: bool =
                                    std::env::var("EPOLL_URING_PROCESS_URING_ON_QUEUE_FULL")
                                        .map(|v| v == "1")
                                        .unwrap_or_else(|e| match e {
                                            std::env::VarError::NotPresent => true, // default-on
                                            std::env::VarError::NotUnicode(_) => panic!("EPOLL_URING_PROCESS_URING_ON_QUEUE_FULL must be a unicode string"),
                                        });
                            }
                            if *PROCESS_URING_ON_QUEUE_FULL {
                                // TODO shouldn't we loop here until we've got a slot? This one-off poll doesn't make much sense.
                                submit_side_open.submitter.submit().unwrap();
                                submit_side_open
                                    .completion_side
                                    .lock()
                                    .unwrap()
                                    .process_completions(ProcessCompletionsCause::Regular);
                            }
                            OpFut {
                                state: OpFutState::WaitForSlot {
                                    submit_side_weak: submit_side_open.myself.clone(),
                                    waiter,
                                    make_op,
                                },
                            }
                        }
                    }
                },
            }
        })
    }
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

impl<O> std::future::Future for OpFut<O>
where
    O: Op + Send + 'static + Unpin,
{
    type Output = (O::Resources, Result<O::Success, Error<O::Error>>);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut myself = self.as_mut();
        loop {
            let cur = std::mem::replace(&mut myself.state, OpFutState::Undefined);
            match cur {
                OpFutState::Undefined => unreachable!(),
                OpFutState::ReadyPolled => unreachable!(),
                OpFutState::Err { make_op, err } => {
                    return std::task::Poll::Ready((
                        make_op.on_failed_submission(),
                        Err(Error::System(err)),
                    ));
                }
                OpFutState::WaitForSlot {
                    submit_side_weak,
                    mut waiter,
                    make_op: op,
                } => match waiter.poll_unpin(cx) {
                    std::task::Poll::Pending => {
                        myself.state = OpFutState::WaitForSlot {
                            submit_side_weak,
                            waiter,
                            make_op: op,
                        };
                        return std::task::Poll::Pending;
                    }
                    std::task::Poll::Ready(Ok(unsafe_slot)) => {
                        let res =
                            submit_side_weak.with_submit_side_open(|maybe_submit_side_open| {
                                match maybe_submit_side_open {
                                    Some(submit_side_open) => {
                                        match finish_submit(op, unsafe_slot, submit_side_open) {
                                            Ok(fut) => {
                                                myself.state = OpFutState::Submitted(fut);
                                                ControlFlow::Continue(())
                                            }
                                            Err((op, err)) => {
                                                myself.state = OpFutState::ReadyPolled;
                                                ControlFlow::Break(std::task::Poll::Ready((
                                                    op.on_failed_submission(),
                                                    Err(Error::System(err)),
                                                )))
                                            }
                                        }
                                    }
                                    None => {
                                        myself.state = OpFutState::ReadyPolled;
                                        ControlFlow::Break(std::task::Poll::Ready((
                                            op.on_failed_submission(),
                                            Err(Error::System(OpError::SystemIsShutDown)),
                                        )))
                                    }
                                }
                            });
                        match res {
                            ControlFlow::Continue(()) => continue,
                            ControlFlow::Break(ret) => return ret,
                        }
                    }
                    std::task::Poll::Ready(Err(_sender_dropped)) => {
                        myself.state = OpFutState::ReadyPolled;
                        return std::task::Poll::Ready((
                            op.on_failed_submission(),
                            Err(Error::System(OpError::SystemIsShutDown)),
                        ));
                    }
                },
                OpFutState::Submitted(mut submit_fut) => match submit_fut.poll_unpin(cx) {
                    std::task::Poll::Ready((resources, res)) => {
                        myself.state = OpFutState::ReadyPolled;
                        match res {
                            Ok(success) => return std::task::Poll::Ready((resources, Ok(success))),
                            Err(err) => match err {
                                InflightHandleError::Completion(err) => {
                                    return std::task::Poll::Ready((
                                        resources,
                                        Err(Error::Op(err)),
                                    ));
                                }
                                InflightHandleError::SlotsDropped => {
                                    return std::task::Poll::Ready((
                                        resources,
                                        Err(Error::System(OpError::SystemIsShutDown)),
                                    ))
                                }
                            },
                        }
                    }
                    std::task::Poll::Pending => {
                        myself.state = OpFutState::Submitted(submit_fut);
                        return std::task::Poll::Pending;
                    }
                },
            }
        }
    }
}
