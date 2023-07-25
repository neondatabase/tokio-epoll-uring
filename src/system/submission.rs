use std::sync::{Arc, Mutex, Weak};

use io_uring::{SubmissionQueue, Submitter};
use tracing::trace;

use crate::{system::OpState, ResourcesOwnedByKernel};

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
                completion_side: Arc::clone(&completion_side),
                ops,
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
                submit_side: Weak::upgrade(&self.myself).expect("we're executing on myself"),
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
    pub(crate) ops: Arc<Mutex<Ops>>,
    pub(crate) idx: usize,
    // only used in some modes
    pub(crate) submit_side: Arc<Mutex<SubmitSideInner>>,
}

enum UnsafeOpsSlotHandleSubmitError {
    SubmitSidePlugged(UnsafeOpsSlotHandle),
}

impl UnsafeOpsSlotHandle {
    fn submit<R, MakeSqe>(
        self,
        mut rsrc: R,
        make_sqe: MakeSqe,
    ) -> Result<InflightOpHandle<R>, UnsafeOpsSlotHandleSubmitError>
    where
        R: ResourcesOwnedByKernel + Send + 'static,
        MakeSqe: FnOnce(&mut R) -> io_uring::squeue::Entry,
    {
        let sqe = make_sqe(&mut rsrc);
        let sqe = sqe.user_data(u64::try_from(self.idx).unwrap());

        let mut submit_side_guard = self.submit_side.lock().unwrap();
        let submit_side_open = match &mut *submit_side_guard {
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::Plugged => {
                drop(submit_side_guard);
                return Err(UnsafeOpsSlotHandleSubmitError::SubmitSidePlugged(self));
            }
            SubmitSideInner::Undefined => panic!("implementation error"),
        };

        let mut ops_guard = submit_side_open.ops.lock().unwrap();
        assert!(ops_guard.storage[self.idx].is_none()); // TODO turn Option into tri-state for better semantics: NotTaken, SlotLive, Submitted
        ops_guard.storage[self.idx] =
            Some(OpState(Mutex::new(OpStateInner::Pending { waker: None })));
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
        drop(submit_side_guard);
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
    fn return_slot_and_wake(self) {
        let UnsafeOpsSlotHandle {
            submit_side,
            ops,
            idx,
        } = self;
        let mut ops_guard = ops.lock().unwrap();
        ops_guard.storage[idx] = None;
        ops_guard.return_slot_and_wake(submit_side, self.idx);
        drop(ops_guard);
    }
}

impl UnsafeOpsSlotHandle {
    fn poll(self, cx: &mut std::task::Context<'_>) -> OwnedSlotPollResult {
        let mut ops_guard = self.ops.lock().unwrap();
        let op_state = &mut ops_guard.storage[self.idx];
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
                drop(ops_guard);
                return OwnedSlotPollResult::Pending(self);
            }
            OpStateInner::PendingButFutureDropped { .. } => {
                unreachable!("if it's dropped, it's not pollable")
            }
            OpStateInner::ReadyButFutureDropped => {
                unreachable!("if it's dropped, it's not pollable")
            }
            OpStateInner::Ready { result: res } => {
                trace!("op is ready, returning resources to user");
                drop(op_state_inner);
                drop(ops_guard);
                // important to drop the ops_guard to avoid deadlock in return_slot_and_wake
                self.return_slot_and_wake();
                return OwnedSlotPollResult::Ready(res);
            }
        }
    }
}

pub struct NotInflightSlotHandle {
    state: NotInflightSlotHandleState,
}

enum NotInflightSlotHandleState {
    Usable { slot: UnsafeOpsSlotHandle },
    Used,
    Dropped,
}

impl NotInflightSlotHandle {
    pub(crate) fn submit<R, MakeSqe>(mut self, rsrc: R, make_sqe: MakeSqe) -> InflightOpHandle<R>
    where
        R: ResourcesOwnedByKernel + Send + 'static,
        MakeSqe: FnOnce(&mut R) -> io_uring::squeue::Entry,
    {
        let cur = std::mem::replace(&mut self.state, NotInflightSlotHandleState::Used);
        match cur {
            NotInflightSlotHandleState::Usable { slot } => {
                match slot.submit(rsrc, make_sqe) {
                    Ok(inflight_op_handle) => inflight_op_handle,
                    Err(UnsafeOpsSlotHandleSubmitError::SubmitSidePlugged(slot)) => {
                        // put back so we run the Drop handler
                        self.state = NotInflightSlotHandleState::Usable { slot };
                        panic!("cannot use slot for submission, SubmitSide is already plugged")
                    }
                }
            }
            NotInflightSlotHandleState::Used => unreachable!("implementation error"),
            NotInflightSlotHandleState::Dropped => unreachable!("implementation error"),
        }
    }
}

impl Drop for NotInflightSlotHandle {
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, NotInflightSlotHandleState::Dropped);
        match cur {
            NotInflightSlotHandleState::Usable { slot } => {
                slot.return_slot_and_wake();
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
        submit_side: Arc<Mutex<SubmitSideInner>>,
    },
    EnqueuedWaiter(tokio::sync::oneshot::Receiver<UnsafeOpsSlotHandle>),
    ReadyPolled,
}

impl GetOpsSlotFut {
    #[allow(dead_code)]
    pub(crate) fn state_discriminant_str(&self) -> &'static str {
        match self.state {
            GetOpsSlotFutState::Undefined => "Undefined",
            GetOpsSlotFutState::NotPolled { .. } => "NotPolled",
            GetOpsSlotFutState::EnqueuedWaiter(_) => "EnqueuedWaiter",
            GetOpsSlotFutState::ReadyPolled => "ReadyPolled",
        }
    }
}

impl std::future::Future for GetOpsSlotFut {
    type Output = NotInflightSlotHandle;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        loop {
            let cur = std::mem::replace(&mut self.state, GetOpsSlotFutState::Undefined);
            match cur {
                GetOpsSlotFutState::Undefined => unreachable!("implementation error"),
                GetOpsSlotFutState::NotPolled { submit_side } => {
                    let mut submit_side_guard = submit_side.lock().unwrap();
                    let submit_side_open = submit_side_guard.must_open();
                    let mut ops_guard = submit_side_open.ops.lock().unwrap();
                    match ops_guard.unused_indices.pop() {
                        Some(idx) => {
                            drop(ops_guard);
                            let ops = Arc::clone(&submit_side_open.ops);
                            drop(submit_side_guard);
                            self.state = GetOpsSlotFutState::ReadyPolled;
                            return std::task::Poll::Ready(NotInflightSlotHandle {
                                state: NotInflightSlotHandleState::Usable {
                                    slot: UnsafeOpsSlotHandle {
                                        submit_side,
                                        ops,
                                        idx,
                                    },
                                },
                            });
                        }
                        None => {
                            // all slots are taken.
                            // do some opportunistic completion processing to wake up futures that will release ops slots
                            // then yield to executor
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
                                    .process_completions();
                            }
                            let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                            match submit_side_open.waiters_tx.send(wake_up_tx) {
                                Ok(()) => (),
                                Err(tokio::sync::mpsc::error::SendError(_)) => {
                                    todo!("can this happen? poller would be dead")
                                }
                            }
                            self.state = GetOpsSlotFutState::EnqueuedWaiter(wake_up_rx);
                            continue;
                        }
                    }
                }
                GetOpsSlotFutState::EnqueuedWaiter(mut waiter) => {
                    match {
                        let waiter = std::pin::Pin::new(&mut waiter);
                        waiter.poll(cx)
                    }{
                        std::task::Poll::Ready(res) => match res {
                            Ok(slot_handle) => {
                                self.state = GetOpsSlotFutState::ReadyPolled;
                                return std::task::Poll::Ready(NotInflightSlotHandle { state: NotInflightSlotHandleState::Usable { slot: slot_handle }});
                            }
                            Err(_waiter_dropped) => unreachable!("system dropped before all GetOpsSlotFut were dropped; type system should prevent this"),
                        },
                        std::task::Poll::Pending => {
                            self.state = GetOpsSlotFutState::EnqueuedWaiter(waiter);
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

pub(crate) enum SubmitError {
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

impl<R: ResourcesOwnedByKernel + Send + Unpin> std::future::Future for InflightOpHandle<R> {
    type Output = R::OpResult;

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
                return std::task::Poll::Ready(rsrc.on_op_completion(res));
            }
            InflightOpHandleState::DoneButYieldingToExecutorForFairness { result } => {
                self.state = InflightOpHandleState::DoneAndPolled;
                return std::task::Poll::Ready(
                    self.resources_owned_by_kernel
                        .take()
                        .unwrap()
                        .on_op_completion(result),
                );
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
                let mut submit_side_guard = slot.submit_side.lock().unwrap();
                let submit_side_open = submit_side_guard.must_open();
                let mut ops_guard = submit_side_open.ops.lock().unwrap();
                let op_state = &mut ops_guard.storage[slot.idx];
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
                        drop(ops_guard);
                        drop(submit_side_guard);
                        slot.return_slot_and_wake();
                    }
                    OpStateInner::PendingButFutureDropped { .. } => {
                        unreachable!("above is the only transition into this state, and this function only runs once")
                    }
                    OpStateInner::ReadyButFutureDropped => {
                        unreachable!("this is the future, and it's dropping, but not yet dropped");
                    }
                }
            }
        }
    }
}
