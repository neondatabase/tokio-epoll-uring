pub mod nop;
pub mod read;

use std::sync::{Arc, Mutex, Weak};

use futures::{Future, FutureExt};
use tokio::sync::oneshot;

use crate::{
    system::{
        completion::ProcessCompletionsCause,
        slots::{self, InflightOpHandle, InflightOpHandleError, SlotHandle, TryGetSlotResult},
        submission::{SubmitError, SubmitSide, SubmitSideInner, SubmitSideOpen},
    },
    ResourcesOwnedByKernel, SubmitSideProvider,
};

pub(crate) enum OpFut<L, P, O>
where
    L: Future<Output = P> + Unpin + Send + 'static,
    P: SubmitSideProvider + 'static,
    O: OpTrait + Send + 'static,
{
    Undefined,
    NeedLaunch {
        system_launcher: L,
        make_op: O,
    },
    WaitForSlot {
        submit_side_weak: Weak<Mutex<SubmitSideInner>>,
        waiter: oneshot::Receiver<SlotHandle>,
        make_op: O,
    },
    Submitted(InflightOpHandle<O>),
    ReadyPolled,
}

pub(crate) trait OpTrait: ResourcesOwnedByKernel + Sized + Send + 'static {
    fn make_sqe(&mut self) -> io_uring::squeue::Entry;
    fn into_fut<L, P>(self, launcher: L) -> OpFut<L, P, Self>
    where
        L: Future<Output = P> + Unpin + Send + 'static,
        P: SubmitSideProvider + 'static,
    {
        OpFut::NeedLaunch {
            system_launcher: launcher,
            make_op: self,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError {
    #[error("shutting down")]
    SystemShuttingDown,
    #[error("system is shut down")]
    SystemIsShutDown,
}

pub(crate) fn finish_submit<O>(
    op: O,
    unsafe_slot: SlotHandle,
    submit_side_open: &mut SubmitSideOpen,
) -> Result<InflightOpHandle<O>, (O, OpError)>
where
    O: OpTrait + Send + 'static + Unpin,
{
    let (sqe, inflight_op_fut) = match unsafe_slot.use_for_op(op) {
        Ok((sqe, inflight_op_handle)) => (sqe, inflight_op_handle),
        Err((op, slots::UseError::SlotsDropped)) => {
            return Err((op, OpError::SystemIsShutDown));
        }
    };

    lazy_static::lazy_static! {
        static ref PROCESS_URING_ON_SUBMIT: bool =
            std::env::var("PROCESS_URING_ON_SUBMIT")
                .map(|v| v == "1")
                .unwrap_or_else(|e| match e {
                    std::env::VarError::NotPresent => false,
                    std::env::VarError::NotUnicode(_) => panic!("PROCESS_URING_ON_SUBMIT must be a unicode string"),
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

impl<L, P, O> std::future::Future for OpFut<L, P, O>
where
    L: Future<Output = P> + Unpin + Send + 'static,
    P: SubmitSideProvider + 'static,
    O: OpTrait + Send + 'static + Unpin,
{
    type Output = Result<O::OpResult, (O, OpError)>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut myself = self.as_mut();
        loop {
            let cur = std::mem::replace(&mut *myself, OpFut::Undefined);
            match cur {
                OpFut::Undefined => unreachable!(),
                OpFut::ReadyPolled => unreachable!(),
                OpFut::NeedLaunch {
                    mut system_launcher,
                    make_op,
                } => match system_launcher.poll_unpin(cx) {
                    std::task::Poll::Pending => {
                        *myself = OpFut::NeedLaunch {
                            system_launcher,
                            make_op,
                        };
                        return std::task::Poll::Pending;
                    }
                    std::task::Poll::Ready(provider) => {
                        enum Action<R> {
                            Continue,
                            Return(R),
                        }
                        let action = provider.with_submit_side(|submit_side| {
                            let SubmitSide(inner) = submit_side;
                            let mut inner_guard = inner.lock().unwrap();
                            let submit_side_open = match &mut *inner_guard {
                                SubmitSideInner::Open(open) => open,
                                SubmitSideInner::Plugged => todo!(),
                                SubmitSideInner::Undefined => unreachable!(),
                            };

                            let mut ops_guard = submit_side_open.slots.inner.lock().unwrap();
                            match ops_guard.try_get_slot() {
                                TryGetSlotResult::Draining => {
                                    *myself = OpFut::ReadyPolled;
                                    return Action::Return(Err(
                                        (make_op, OpError::SystemShuttingDown),
                                    ));
                                }
                                TryGetSlotResult::GotSlot(unsafe_slot) => {
                                    drop(ops_guard);
                                    let fut = match finish_submit(make_op, unsafe_slot, submit_side_open) {
                                        Ok(fut) => fut,
                                        Err((make_op, err)) => {
                                            *myself = OpFut::ReadyPolled;
                                            return Action::Return(Err((make_op, err)));
                                        },
                                    };
                                    *myself = OpFut::Submitted(fut);
                                    Action::Continue
                                }
                                TryGetSlotResult::NoSlots(waiter) => {
                                    // All slots are taken and we're waiting in line.
                                    // If enabled, do some opportunistic completion processing to wake up futures that will release ops slots.
                                    // This is in the hope that we'll wake ourselves up.
                                    drop(ops_guard); // so that  process_completions() can take it

                                    lazy_static::lazy_static! {
                                        static ref PROCESS_URING_ON_QUEUE_FULL: bool =
                                            std::env::var("PROCESS_URING_ON_QUEUE_FULL")
                                                .map(|v| v == "1")
                                                .unwrap_or_else(|e| match e {
                                                    std::env::VarError::NotPresent => false,
                                                    std::env::VarError::NotUnicode(_) => panic!("PROCESS_URING_ON_QUEUE_FULL must be a unicode string"),
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
                                    *myself = OpFut::WaitForSlot {
                                        submit_side_weak: Arc::downgrade(&inner),
                                        waiter,
                                        make_op,
                                    };
                                    Action::Continue
                                }
                            }
                        });
                        match action {
                            Action::Continue => continue,
                            Action::Return(output) => return std::task::Poll::Ready(output),
                        }
                    }
                },
                OpFut::WaitForSlot {
                    submit_side_weak,
                    mut waiter,
                    make_op: op,
                } => match waiter.poll_unpin(cx) {
                    std::task::Poll::Pending => {
                        *myself = OpFut::WaitForSlot {
                            submit_side_weak,
                            waiter,
                            make_op: op,
                        };
                        return std::task::Poll::Pending;
                    }
                    std::task::Poll::Ready(Ok(unsafe_slot)) => {
                        let submit_side = match submit_side_weak.upgrade() {
                            Some(submit_side) => submit_side,
                            None => {
                                *myself = OpFut::ReadyPolled;
                                unsafe_slot.try_return();
                                *myself = OpFut::ReadyPolled;
                                return std::task::Poll::Ready(Err((
                                    op,
                                    OpError::SystemIsShutDown,
                                )));
                            }
                        };
                        let mut inner_guard = submit_side.lock().unwrap();
                        let submit_side_open = match &mut *inner_guard {
                            SubmitSideInner::Open(open) => open,
                            SubmitSideInner::Plugged => todo!(),
                            SubmitSideInner::Undefined => unreachable!(),
                        };
                        match finish_submit(op, unsafe_slot, submit_side_open) {
                            Ok(fut) => {
                                *myself = OpFut::Submitted(fut);
                                continue;
                            }
                            Err((op, err)) => {
                                *myself = OpFut::ReadyPolled;
                                return std::task::Poll::Ready(Err((op, err)));
                            }
                        }
                    }
                    std::task::Poll::Ready(Err(_e)) => {
                        *myself = OpFut::ReadyPolled;
                        return std::task::Poll::Ready(Err((op, OpError::SystemIsShutDown)));
                    }
                },
                OpFut::Submitted(mut submit_fut) => match submit_fut.poll_unpin(cx) {
                    std::task::Poll::Ready(Ok(op_result)) => {
                        let output: O::OpResult = op_result;
                        *myself = OpFut::ReadyPolled;
                        return std::task::Poll::Ready(Ok(output));
                    }
                    std::task::Poll::Ready(Err((
                        resource,
                        InflightOpHandleError::SlotsDropped,
                    ))) => {
                        *myself = OpFut::ReadyPolled;
                        return std::task::Poll::Ready(Err((resource, OpError::SystemIsShutDown)));
                    }
                    std::task::Poll::Pending => {
                        *myself = OpFut::Submitted(submit_fut);
                        return std::task::Poll::Pending;
                    }
                },
            }
        }
    }
}
