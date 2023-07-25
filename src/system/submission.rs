use std::sync::{Arc, Mutex, Weak};

use io_uring::{SubmissionQueue, Submitter};
use tracing::trace;

use crate::{
    system::{OpState, OpsInner},
    ResourcesOwnedByKernel,
};

use super::{completion::CompletionSide, OpStateInner, Ops};

pub(crate) struct SubmitSideNewArgs {
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    pub(crate) id: usize,
    pub(crate) submitter: Submitter<'static>,
    pub(crate) sq: SubmissionQueue<'static>,
    pub(crate) ops: Arc<Mutex<Ops>>,
    pub(crate) completion_side: Arc<Mutex<CompletionSide>>,
    pub(crate) waiters_tx:
        tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<UnsafeOpsSlotHandle>>,
}

impl SubmitSide {
    pub(crate) fn new(args: SubmitSideNewArgs) -> SubmitSide {
        let SubmitSideNewArgs {
            id,
            submitter,
            sq,
            ops,
            completion_side,
            waiters_tx,
        } = args;
        SubmitSide(Arc::new_cyclic(|myself| {
            Mutex::new(SubmitSideInner::Open(SubmitSideOpen {
                id,
                submitter,
                sq,
                ops,
                completion_side: Arc::clone(&completion_side),
                waiters_tx,
                myself: Weak::clone(myself),
            }))
        }))
    }
}

impl SubmitSideOpen {
    pub fn get_ops_slot(&self) -> GetOpsSlotFut {
        return GetOpsSlotFut {
            state: GetOpsSlotFutState::NotPolled {
                submit_side_weak: Weak::clone(&self.myself),
                ops_weak: Arc::downgrade(&self.ops),
            },
        };
    }
}

impl SubmitSideOpen {
    fn submit_raw(&mut self, sqe: io_uring::squeue::Entry) -> std::result::Result<(), SubmitError> {
        self.sq.sync();
        match unsafe { self.sq.push(&sqe) } {
            Ok(()) => {}
            Err(_queue_full) => {
                return Err(SubmitError::QueueFull);
            }
        }
        self.sq.sync();
        self.submitter.submit().unwrap();
        Ok(())
    }
}

pub(crate) struct UnsafeOpsSlotHandle {
    pub(crate) ops_weak: Weak<Mutex<Ops>>,
    pub(crate) idx: usize,
}

#[derive(Debug, thiserror::Error)]
// TODO: all of these are the same cause, i.e., system shutdown
pub(crate) enum UnsafeOpsSlotHandleSubmitError {
    #[error("submit side is plugged")]
    SubmitSidePlugged,
    #[error("ops draining")]
    OpsDraining,
    #[error("ops dropped")]
    OpsDropped,
}

impl UnsafeOpsSlotHandle {
    fn submit<R, MakeSqe>(
        self,
        submit_side: Arc<Mutex<SubmitSideInner>>,
        mut rsrc: R,
        make_sqe: MakeSqe,
    ) -> Result<InflightOpHandle<R>, (Self, R, UnsafeOpsSlotHandleSubmitError)>
    where
        R: ResourcesOwnedByKernel + Send + 'static,
        MakeSqe: FnOnce(&mut R) -> io_uring::squeue::Entry,
    {
        let sqe = make_sqe(&mut rsrc);
        let sqe = sqe.user_data(u64::try_from(self.idx).unwrap());

        let mut submit_side_guard = submit_side.lock().unwrap();
        let submit_side_open = match &mut *submit_side_guard {
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::Plugged => {
                return Err((
                    self,
                    rsrc,
                    UnsafeOpsSlotHandleSubmitError::SubmitSidePlugged,
                ));
            }
            SubmitSideInner::Undefined => todo!(),
        };

        let ops = match Weak::upgrade(&self.ops_weak) {
            Some(ops) => ops,
            None => {
                return Err((self, rsrc, UnsafeOpsSlotHandleSubmitError::OpsDropped));
            }
        };
        let ops_guard = ops.lock().unwrap();
        let mut ops_inner_guard = ops_guard.inner.lock().unwrap();
        let ops_open = match &mut *ops_inner_guard {
            OpsInner::Undefined => unreachable!(),
            OpsInner::Open(open) => open,
            OpsInner::Draining(_) => {
                drop(ops_inner_guard);
                drop(ops_guard);
                return Err((self, rsrc, UnsafeOpsSlotHandleSubmitError::OpsDraining));
            }
        };
        assert!(ops_open.storage[self.idx].is_none()); // TODO turn Option into tri-state for better semantics: NotTaken, SlotLive, Submitted
        ops_open.storage[self.idx] =
            Some(OpState(Mutex::new(OpStateInner::Pending { waker: None })));
        // drop mutexes, process_completions() below may need to grab it
        drop(ops_inner_guard);
        drop(ops_guard);

        // if we're going to process completions immediately, get the lock on the CQ so that
        // we are guaranteed to process completions before the poller task
        {
            lazy_static::lazy_static! {
                static ref PROCESS_URING_ON_SUBMIT: bool =
                    std::env::var("PROCESS_URING_ON_SUBMIT")
                        .map(|v| v == "1")
                        .unwrap_or_else(|e| match e {
                            std::env::VarError::NotPresent => false,
                            std::env::VarError::NotUnicode(_) => panic!("PROCESS_URING_ON_SUBMIT must be a unicode string"),
                        });
            }

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
                    // So, for now, keep SQ size == inflight ops size.
                    // This potentially limits throughput if SQ size is chosen too small.
                    unreachable!("the `ops` has same size as the SQ, so, if SQ is full, we wouldn't have been able to get this slot");
                }
            }

            if let Some(mut cq) = cq_guard {
                assert!(*PROCESS_URING_ON_SUBMIT);
                // opportunistically process completion immediately
                // TODO do it during ::poll() as well?
                cq.process_completions();
            } else {
                assert!(!*PROCESS_URING_ON_SUBMIT);
            }
        }
        Ok(InflightOpHandle {
            resources_owned_by_kernel: Some(rsrc),
            state: InflightOpHandleState::Inflight {
                slot: self,
                poll_count: 0,
            },
        })
    }
}

impl UnsafeOpsSlotHandle {
    fn poll(self, cx: &mut std::task::Context<'_>) -> OwnedSlotPollResult {
        let ops = match Weak::upgrade(&self.ops_weak) {
            Some(ops) => ops,
            None => {
                return OwnedSlotPollResult::OpsDropped;
            }
        };
        let mut ops_guard = ops.lock().unwrap();
        let mut ops_inner_guard = ops_guard.inner.lock().unwrap();
        let storage = match &mut *ops_inner_guard {
            OpsInner::Undefined => unreachable!(),
            OpsInner::Open(inner) => &mut inner.storage,
            OpsInner::Draining(inner) => &mut inner.storage,
        };
        let op_state = &mut storage[self.idx];
        let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();

        let cur: OpStateInner = std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
        match cur {
            OpStateInner::Undefined => panic!("future is in undefined state"),
            OpStateInner::Pending {
                waker: _, // don't recycle wakers, it may be from a different Context than the current `cx`
            } => {
                trace!("op is still pending, storing waker in it");

                *op_state_inner = OpStateInner::Pending {
                    waker: Some(cx.waker().clone()),
                };
                drop(op_state_inner);
                drop(ops_inner_guard);
                drop(ops_guard);
                return OwnedSlotPollResult::Pending(self);
            }
            OpStateInner::PendingButFutureDropped { .. } => {
                unreachable!("if it's dropped, it's not pollable")
            }
            OpStateInner::Ready { result: res } => {
                trace!("op is ready, returning resources to user");
                *op_state_inner = OpStateInner::ReadyConsumed;
                drop(op_state_inner);
                drop(ops_inner_guard);
                ops_guard.return_slot_and_wake(self.idx);
                return OwnedSlotPollResult::Ready(res);
            }
            OpStateInner::ReadyConsumed => {
                unreachable!("only we set this state, and return PolL::Ready() when we do it")
            }
        }
    }
}

pub struct NotInflightSlotHandle {
    state: NotInflightSlotHandleState,
}

enum NotInflightSlotHandleState {
    Usable {
        submit_side_weak: Weak<Mutex<SubmitSideInner>>,
        slot: UnsafeOpsSlotHandle,
    },
    Used,
    Error {
        slot: UnsafeOpsSlotHandle,
    },
    Dropped,
}

pub(crate) struct NotInflightSlotHandleSubmitError<R>
where
    R: ResourcesOwnedByKernel + Send + 'static,
{
    pub(crate) kind: NotInflightSlotHandleSubmitErrorKind,
    pub(crate) rsrc: R,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum NotInflightSlotHandleSubmitErrorKind {
    #[error("submit side is already dropped")]
    SubmitSideDropped,
    #[error("submit error")]
    SubmitError(#[source] UnsafeOpsSlotHandleSubmitError),
}

impl NotInflightSlotHandle {
    pub(crate) fn submit<R, MakeSqe>(
        mut self,
        rsrc: R,
        make_sqe: MakeSqe,
    ) -> Result<InflightOpHandle<R>, NotInflightSlotHandleSubmitError<R>>
    where
        R: ResourcesOwnedByKernel + Send + 'static,
        MakeSqe: FnOnce(&mut R) -> io_uring::squeue::Entry,
    {
        let cur = std::mem::replace(&mut self.state, NotInflightSlotHandleState::Used);
        match cur {
            NotInflightSlotHandleState::Usable {
                submit_side_weak,
                slot,
            } => {
                let submit_side = match submit_side_weak.upgrade() {
                    Some(submit_side) => submit_side,
                    None => {
                        self.state = NotInflightSlotHandleState::Error { slot };
                        return Err(NotInflightSlotHandleSubmitError {
                            kind: NotInflightSlotHandleSubmitErrorKind::SubmitSideDropped,
                            rsrc,
                        });
                    }
                };
                match slot.submit(submit_side, rsrc, make_sqe) {
                    Ok(inflight_op_handle) => Ok(inflight_op_handle),
                    Err((slot, rsrc, err)) => {
                        self.state = NotInflightSlotHandleState::Error { slot };
                        return Err(NotInflightSlotHandleSubmitError {
                            rsrc,
                            kind: NotInflightSlotHandleSubmitErrorKind::SubmitError(err),
                        });
                    }
                }
            }
            NotInflightSlotHandleState::Used => unreachable!("implementation error"),
            NotInflightSlotHandleState::Error { .. } => unreachable!("implementation error"),
            NotInflightSlotHandleState::Dropped => unreachable!("implementation error"),
        }
    }
}

impl Drop for NotInflightSlotHandle {
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, NotInflightSlotHandleState::Dropped);
        match cur {
            NotInflightSlotHandleState::Error { slot }
            | NotInflightSlotHandleState::Usable {
                slot,
                submit_side_weak: _,
            } => {
                let ops = match Weak::upgrade(&slot.ops_weak) {
                    Some(ops) => ops,
                    None => return,
                };
                let mut ops_guard = ops.lock().unwrap();
                ops_guard.return_slot_and_wake(slot.idx);
            }
            NotInflightSlotHandleState::Used => (),
            NotInflightSlotHandleState::Dropped => unreachable!("implementation error"),
        }
    }
}

pub(crate) struct GetOpsSlotFut {
    state: GetOpsSlotFutState,
}
enum GetOpsSlotFutState {
    Undefined,
    NotPolled {
        submit_side_weak: Weak<Mutex<SubmitSideInner>>,
        ops_weak: Weak<Mutex<Ops>>,
    },
    EnqueuedWaiter {
        submit_side_weak: Weak<Mutex<SubmitSideInner>>,
        waiter: tokio::sync::oneshot::Receiver<UnsafeOpsSlotHandle>,
    },
    ReadyPolled,
}

impl GetOpsSlotFut {
    #[allow(dead_code)]
    pub(crate) fn state_discriminant_str(&self) -> &'static str {
        match self.state {
            GetOpsSlotFutState::Undefined => "Undefined",
            GetOpsSlotFutState::NotPolled { .. } => "NotPolled",
            GetOpsSlotFutState::EnqueuedWaiter { .. } => "EnqueuedWaiter",
            GetOpsSlotFutState::ReadyPolled => "ReadyPolled",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GetOpsSlotError {
    #[error("ops is draining")]
    OpsDraining,
    #[error("ops dropped")]
    OpsDropped,
    #[error("submit side got plugged while doing opportunistic completion processing to get submit slot")]
    SubmitSidePluggedWhileDoingOpportunisticCompletionProcessingToGetSubmitSlot,
    #[error("submit side got dropped while doing opportunistic completion processing to get submit slot")]
    SubmitSideDropped,
    #[error(
        "waiters got dropped while doing opportunistic completion processing to get submit slot"
    )]
    WaitersRxDropped,
}

impl std::future::Future for GetOpsSlotFut {
    type Output = Result<NotInflightSlotHandle, GetOpsSlotError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {
            let cur = std::mem::replace(&mut self.state, GetOpsSlotFutState::Undefined);
            match cur {
                GetOpsSlotFutState::Undefined => unreachable!("implementation error"),
                GetOpsSlotFutState::NotPolled {
                    ops_weak,
                    submit_side_weak,
                } => {
                    let ops = match Weak::upgrade(&ops_weak) {
                        Some(ops) => ops,
                        None => {
                            self.state = GetOpsSlotFutState::ReadyPolled;
                            return std::task::Poll::Ready(Err(GetOpsSlotError::OpsDropped));
                        }
                    };
                    let ops_guard = ops.lock().unwrap();
                    let mut ops_inner_guard = ops_guard.inner.lock().unwrap();
                    let open = match &mut *ops_inner_guard {
                        OpsInner::Undefined => unreachable!(),
                        OpsInner::Open(open) => open,
                        OpsInner::Draining(_) => {
                            self.state = GetOpsSlotFutState::ReadyPolled;
                            return std::task::Poll::Ready(Err(GetOpsSlotError::OpsDraining));
                        }
                    };
                    match open.unused_indices.pop() {
                        Some(idx) => {
                            self.state = GetOpsSlotFutState::ReadyPolled;
                            drop(ops_inner_guard);
                            drop(ops_guard);
                            return std::task::Poll::Ready(Ok(NotInflightSlotHandle {
                                state: NotInflightSlotHandleState::Usable {
                                    submit_side_weak,
                                    slot: UnsafeOpsSlotHandle { ops_weak, idx },
                                },
                            }));
                        }
                        None => {
                            // all slots are taken.
                            // do some opportunistic completion processing to wake up futures that will release ops slots
                            // then yield to executor
                            drop(ops_inner_guard);
                            drop(ops_guard); // so that  process_completions() can take it
                            let submit_side = match Weak::upgrade(&submit_side_weak) {
                                Some(submit_side) => submit_side,
                                None => {
                                    self.state = GetOpsSlotFutState::ReadyPolled;
                                    return std::task::Poll::Ready(Err(
                                        GetOpsSlotError::SubmitSideDropped,
                                    ));
                                }
                            };
                            let submit_side_inner_guard = submit_side.lock().unwrap();
                            let submit_side_open = match &*submit_side_inner_guard {
                                SubmitSideInner::Open(open) => open,
                                SubmitSideInner::Plugged => {
                                    self.state = GetOpsSlotFutState::ReadyPolled;
                                    return std::task::Poll::Ready(Err(GetOpsSlotError::SubmitSidePluggedWhileDoingOpportunisticCompletionProcessingToGetSubmitSlot));
                                }
                                SubmitSideInner::Undefined => unreachable!(),
                            };

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
                                    .process_completions();
                            }
                            let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                            match submit_side_open.waiters_tx.send(wake_up_tx) {
                                Ok(()) => (),
                                Err(tokio::sync::mpsc::error::SendError(_)) => {
                                    self.state = GetOpsSlotFutState::ReadyPolled;
                                    return std::task::Poll::Ready(Err(
                                        GetOpsSlotError::WaitersRxDropped,
                                    ));
                                }
                            }
                            self.state = GetOpsSlotFutState::EnqueuedWaiter {
                                submit_side_weak,
                                waiter: wake_up_rx,
                            };
                            continue;
                        }
                    }
                }
                GetOpsSlotFutState::EnqueuedWaiter {
                    mut waiter,
                    submit_side_weak,
                } => {
                    match {
                        let waiter = std::pin::Pin::new(&mut waiter);
                        waiter.poll(cx)
                    }{
                        std::task::Poll::Ready(res) => match res {
                            Ok(slot_handle) => {
                                self.state = GetOpsSlotFutState::ReadyPolled;
                                return std::task::Poll::Ready(
                                    Ok(NotInflightSlotHandle {
                                         state: NotInflightSlotHandleState::Usable {
                                            submit_side_weak,
                                            slot: slot_handle
                                        }}));
                            }
                            Err(_waiter_dropped) => unreachable!("system dropped before all GetOpsSlotFut were dropped; type system should prevent this"),
                        },
                        std::task::Poll::Pending => {
                            self.state = GetOpsSlotFutState::EnqueuedWaiter{ waiter, submit_side_weak};
                            return std::task::Poll::Pending;
                        },
                    }
                }
                GetOpsSlotFutState::ReadyPolled => {
                    panic!("must not poll future after observing ready")
                }
            }
        }
    }
}

enum OwnedSlotPollResult {
    Ready(i32),
    Pending(UnsafeOpsSlotHandle),
    OpsDropped,
}

#[derive(Clone)]
pub struct SubmitSide(pub(crate) Arc<Mutex<SubmitSideInner>>);

pub(crate) enum SubmitSideInner {
    Open(SubmitSideOpen),
    Plugged,
    Undefined,
}

pub(crate) struct SubmitSideOpen {
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    id: usize,
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
    ops: Arc<Mutex<Ops>>,
    completion_side: Arc<Mutex<CompletionSide>>,
    waiters_tx:
        tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<UnsafeOpsSlotHandle>>,
    myself: Weak<Mutex<SubmitSideInner>>,
}

impl SubmitSideOpen {
    pub(crate) fn deconstruct(self) -> (Submitter<'static>, SubmissionQueue<'static>) {
        let SubmitSideOpen { submitter, sq, .. } = self;
        (submitter, sq)
    }
}

unsafe impl Send for SubmitSideOpen {}
unsafe impl Sync for SubmitSideOpen {}

pub enum PlugError {
    AlreadyPlugged,
}
impl SubmitSideInner {
    pub(crate) fn plug(&mut self) -> Result<SubmitSideOpen, PlugError> {
        let cur = std::mem::replace(self, SubmitSideInner::Undefined);
        match cur {
            SubmitSideInner::Undefined => panic!("implementation error"),
            SubmitSideInner::Open(open) => {
                *self = SubmitSideInner::Plugged;
                Ok(open)
            }
            SubmitSideInner::Plugged => Err(PlugError::AlreadyPlugged),
        }
    }
    pub(crate) fn must_open(&mut self) -> &mut SubmitSideOpen {
        match self {
            SubmitSideInner::Undefined => panic!("implementation error"),
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::Plugged => {
                panic!("submit side is plugged, likely because poller task is dead")
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("queue full")]
    QueueFull,
}

pub(crate) struct InflightOpHandle<R: ResourcesOwnedByKernel + Send + 'static> {
    resources_owned_by_kernel: Option<R>, // beocmes None in `drop()`, Some otherwise
    state: InflightOpHandleState,
}

enum InflightOpHandleState {
    Undefined,
    Inflight {
        slot: UnsafeOpsSlotHandle,
        poll_count: usize,
    },
    DoneButYieldingToExecutorForFairness {
        result: i32,
    },
    DoneAndPolled,
    Dropped,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InflightOpHandleError {
    #[error("ops dropped")]
    OpsDropped,
}

impl<R: ResourcesOwnedByKernel + Send + Unpin> std::future::Future for InflightOpHandle<R> {
    type Output = Result<R::OpResult, (R, InflightOpHandleError)>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let cur = std::mem::replace(&mut self.state, InflightOpHandleState::Undefined);
        match cur {
            InflightOpHandleState::Undefined => unreachable!("implementation error"),
            InflightOpHandleState::DoneAndPolled => {
                panic!("must not poll future after observing ready")
            }
            InflightOpHandleState::Dropped => unreachable!("implementation error"),
            InflightOpHandleState::Inflight { slot, poll_count } => {
                let res = match slot.poll(cx) {
                    OwnedSlotPollResult::Ready(res) => res,
                    OwnedSlotPollResult::Pending(slot) => {
                        self.state = InflightOpHandleState::Inflight {
                            slot,
                            poll_count: poll_count + 1,
                        };
                        return std::task::Poll::Pending;
                    }
                    OwnedSlotPollResult::OpsDropped => {
                        self.state = InflightOpHandleState::DoneAndPolled;
                        // SAFETY:
                        // This future has an outdated view of the system; it shut down in the meantime.
                        // Shutdown makes sure that all inflight ops complete, so,
                        // these resources are no longer owned by the kernel and can be returned as an error.
                        #[allow(unused_unsafe)]
                        unsafe {
                            let resources_owned_by_kernel =
                                self.resources_owned_by_kernel.take().unwrap();
                            return std::task::Poll::Ready(Err((
                                resources_owned_by_kernel,
                                InflightOpHandleError::OpsDropped,
                            )));
                        }
                    }
                };

                let mut rsrc_mut = unsafe {
                    self.as_mut()
                        .map_unchecked_mut(|myself| &mut myself.resources_owned_by_kernel)
                };
                let rsrc = rsrc_mut.take().expect("we only take() it in drop(), and evidently drop() hasn't happened yet because we're executing a method on self");
                drop(rsrc_mut);

                lazy_static::lazy_static! {
                    static ref YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL: bool =
                        std::env::var("YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL")
                            .map(|v| v == "1")
                            .unwrap_or_else(|e| match e {
                                std::env::VarError::NotPresent => false,
                                std::env::VarError::NotUnicode(_) => panic!("YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL must be a unicode string"),
                            });
                }

                if poll_count == 0 && *YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL {
                    let fut = tokio::task::yield_now();
                    tokio::pin!(fut);
                    match fut.poll(cx) {
                        std::task::Poll::Pending => {
                            self.state =
                                InflightOpHandleState::DoneButYieldingToExecutorForFairness {
                                    result: res,
                                };
                            let replaced = self.resources_owned_by_kernel.replace(rsrc);
                            assert!(replaced.is_none(), "we just took it above");
                            return std::task::Poll::Pending;
                        }
                        std::task::Poll::Ready(()) => {
                            // fallthrough
                        }
                    }
                }
                self.state = InflightOpHandleState::DoneAndPolled;
                return std::task::Poll::Ready(Ok(rsrc.on_op_completion(res)));
            }
            InflightOpHandleState::DoneButYieldingToExecutorForFairness { result } => {
                self.state = InflightOpHandleState::DoneAndPolled;
                return std::task::Poll::Ready(Ok(self
                    .resources_owned_by_kernel
                    .take()
                    .unwrap()
                    .on_op_completion(result)));
            }
        }
    }
}

impl<R> Drop for InflightOpHandle<R>
where
    R: ResourcesOwnedByKernel + Send + 'static,
{
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, InflightOpHandleState::Dropped);
        match cur {
            InflightOpHandleState::Undefined => unreachable!("future is in undefined state"),
            InflightOpHandleState::Dropped => {
                unreachable!("future is in dropped state, but we're in drop() right now")
            }
            InflightOpHandleState::DoneAndPolled => (),
            InflightOpHandleState::DoneButYieldingToExecutorForFairness { .. } => (),
            InflightOpHandleState::Inflight {
                slot,
                poll_count: _,
            } => {
                let ops = match Weak::upgrade(&slot.ops_weak) {
                    Some(ops) => ops,
                    None => {
                        // SAFETY:
                        // This future has an outdated view of the system; it shut down in the meantime.
                        // Shutdown makes sure that all inflight ops complete, so, it is safe to drop the resources owned by kernel at this point.
                        #[allow(unused_unsafe)]
                        unsafe {
                            let resources_owned_by_kernel = self.resources_owned_by_kernel.take();
                            drop(resources_owned_by_kernel);
                        }
                        return;
                    }
                };
                let mut ops_guard = ops.lock().unwrap();
                let mut ops_inner_guard = ops_guard.inner.lock().unwrap();
                let storage = match &mut *ops_inner_guard {
                    OpsInner::Undefined => unreachable!(),
                    OpsInner::Open(inner) => &mut inner.storage,
                    OpsInner::Draining(inner) => &mut inner.storage,
                };
                let op_state = &mut storage[slot.idx];
                let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();
                let cur = std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
                match cur {
                    OpStateInner::Undefined => unreachable!("implementation error"),
                    OpStateInner::Pending { .. } => {
                        // Up until now, Self held the resources that the uring op is operating on.
                        // Now Self is getting dropped, but the uring op is still ongoing.
                        // We must prevent the resources from getting dropped, otherwise the kernel will operate on the dropped resource.
                        // NB: the most concerning resource is the memory buffer into which a read-style uring op will write / from which a write-style uring will read.
                        let rsrc = self
                            .resources_owned_by_kernel
                            .take()
                            .expect("we only take() during drop, which is here");
                        // Use Box for type erasure.
                        // Type erasure is necessary because the ResourcesOwnedByKernel trait has an associated type "OpResult",
                        // and we don't want the system to be generic over it.
                        // Since dropping of inflight IOs is generally rare, the allocations should be fine.
                        // Could optimize by making erause a trait method on ResourcesOwnedByKernel; it could then use a slab allcoator or similar.
                        let rsrc: Box<dyn std::any::Any + Send> = Box::new(rsrc);
                        *op_state_inner = OpStateInner::PendingButFutureDropped {
                            resources_owned_by_kernel: rsrc,
                        };
                        drop(op_state_inner);
                    }
                    OpStateInner::Ready { result: _ } => {
                        // The op completed and called the waker that would eventually cause this future to be polled
                        // and transition from Inflight to one of the Done states. But this future got dropped first.
                        // So, it's our job to drop the slot.
                        drop(op_state_inner);
                        drop(ops_inner_guard);
                        ops_guard.return_slot_and_wake(slot.idx);
                    }
                    OpStateInner::ReadyConsumed => (),
                    OpStateInner::PendingButFutureDropped { .. } => {
                        unreachable!("above is the only transition into this state, and this function only runs once")
                    }
                }
            }
        }
    }
}
