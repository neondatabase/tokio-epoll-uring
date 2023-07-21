use std::{
    collections::HashMap,
    future::Future,
    os::fd::AsRawFd,
    sync::{Arc, Mutex, Weak},
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tracing::{debug, info, info_span, trace, Instrument};

pub(crate) struct System {
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    id: usize,
    split_uring: *mut io_uring::IoUring,
    pub(crate) submit_side: Arc<Mutex<SubmitSideInner>>,
}

// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Send for System {}
// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Sync for System {}

pub(crate) struct SystemHandle {
    pub(crate) submit_side: SubmitSide,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl System {
    pub(crate) fn new() -> SystemHandle {
        static POLLER_TASK_ID: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let id = POLLER_TASK_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // TODO: this unbounded channel is the root of all evil: unbounded queue for IOPS; should provie app option to back-pressure instead.
        let (waiters_tx, waiters_rx) = tokio::sync::mpsc::unbounded_channel();
        let ops = Arc::new(Mutex::new(Ops {
            id,
            storage: array_macro::array![_ => None; RING_SIZE as usize],
            unused_indices: (0..RING_SIZE.try_into().unwrap()).collect(),
            waiters_rx,
        }));
        let uring = Box::into_raw(Box::new(io_uring::IoUring::new(RING_SIZE).unwrap()));
        let uring_fd = unsafe { (*uring).as_raw_fd() };
        let (submitter, sq, cq) = unsafe { (&mut *uring).split() };

        let send_sync_completion_queue = Arc::new(Mutex::new(CompletionSide {
            id,
            cq,
            ops: ops.clone(),
            submit_side: Weak::new(),
        }));

        let submit_side = Arc::new_cyclic(|myself| {
            Mutex::new(SubmitSideInner::Open(SubmitSideOpen {
                id,
                submitter,
                sq,
                completion_side: Arc::clone(&send_sync_completion_queue),
                ops,
                waiters_tx,
                myself: Weak::clone(myself),
            }))
        });
        let system = System {
            id,
            split_uring: uring,
            submit_side: Arc::clone(&submit_side),
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        setup_poller_task(
            id,
            uring_fd,
            Arc::clone(&send_sync_completion_queue),
            system,
            shutdown_rx,
        );
        SystemHandle {
            submit_side: SubmitSide(submit_side),
            shutdown_tx,
        }
    }
}

impl SystemHandle {
    // TODO: use compile-time tricks to ensure the system is always `shutdown()` and never Dropped.
    pub fn shutdown(self) {
        let SystemHandle {
            submit_side,
            shutdown_tx,
        } = self;
        drop(submit_side);
        drop(shutdown_tx); // this will cause the poller task to shut down
    }
}

mod private {
    pub trait Sealed {}
    impl Sealed for &'_ crate::system_lifecycle::BorrowBased {}
    impl Sealed for crate::system_lifecycle::ThreadLocal {}
}

pub trait SubmitSideProvider: private::Sealed + Unpin + Copy {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(self, f: F) -> R;
}

struct OpState(Mutex<OpStateInner>);

enum OpStateInner {
    Undefined,
    Pending {
        waker: Option<std::task::Waker>, // None if it hasn't been polled yet
    },
    PendingButFutureDropped {
        /// When a future gets dropped while the Op is still running, it gets Box'ed  This is a Box'ed `ResourcesOwnedByKernel`
        resources_owned_by_kernel: Box<dyn std::any::Any + Send>,
    },
    ReadyButFutureDropped,
    Ready {
        result: i32,
    },
}

impl OpStateInner {
    fn discriminant_str(&self) -> &'static str {
        match self {
            OpStateInner::Undefined => "Undefined",
            OpStateInner::Pending { .. } => "Pending",
            OpStateInner::PendingButFutureDropped { .. } => "PendingButFutureDropped",
            OpStateInner::ReadyButFutureDropped => "ReadyButFutureDropped",
            OpStateInner::Ready { .. } => "Ready",
        }
    }
}

struct CompletionSide {
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    id: usize,
    cq: CompletionQueue<'static>,
    ops: Arc<Mutex<Ops>>,
    submit_side: Weak<Mutex<SubmitSideInner>>,
}

unsafe impl Send for CompletionSide {}

impl CompletionSide {
    fn process_completions(&mut self) {
        let cq = &mut self.cq;
        cq.sync();
        for cqe in &mut *cq {
            trace!("got cqe: {:?}", cqe);
            let idx: u64 = unsafe { std::mem::transmute(cqe.user_data()) };
            let mut ops_guard = self.ops.lock().unwrap();
            let op_state = &mut ops_guard.storage[idx as usize];
            let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();
            let cur = std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
            match cur {
                OpStateInner::Undefined => unreachable!("implementation error"),
                OpStateInner::Pending { waker } => {
                    *op_state_inner = OpStateInner::Ready {
                        result: cqe.result(),
                    };
                    drop(op_state_inner);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
                OpStateInner::PendingButFutureDropped {
                    resources_owned_by_kernel,
                } => {
                    *op_state_inner = OpStateInner::ReadyButFutureDropped;
                    drop(op_state_inner);
                    *op_state = None;
                    drop(op_state);
                    drop(resources_owned_by_kernel);
                    let submit_side = Weak::upgrade(&self.submit_side)
                        .expect("completion gets shut down after submission");
                    ops_guard.return_slot_and_wake(submit_side, idx as usize);
                }
                OpStateInner::ReadyButFutureDropped => {
                    unreachable!("can't be ready twice")
                }
                OpStateInner::Ready { .. } => {
                    unreachable!("can't be ready twice")
                }
            }
        }
        cq.sync();
    }
}

struct Ops {
    id: usize,
    storage: [Option<OpState>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    waiters_rx:
        tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<UnsafeOpsSlotHandle>>,
}

impl Ops {
    fn return_slot_and_wake(&mut self, submit_side: Arc<Mutex<SubmitSideInner>>, idx: usize) {
        assert!(self.storage[idx].is_none());
        loop {
            match self.waiters_rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    break;
                }
                Ok(waiter) => {
                    match waiter.send(UnsafeOpsSlotHandle {
                        submit_side: Arc::clone(&submit_side),
                        idx,
                    }) {
                        Ok(()) => {
                            trace!(system = self.id, "handed `idx` to a waiter");
                            return;
                        }
                        Err(_) => {
                            // the future requesting wakeup got dropped. wake up next one
                            continue;
                        }
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    panic!("SubmitSide got dropped while there were pending ops; we drain it, this shouldn't happen")
                }
            }
        }
        trace!(
            system = self.id,
            "no waiters, returning `idx` to unused_indices"
        );
        self.unused_indices.push(idx);
    }
}

const RING_SIZE: u32 = 128;

#[derive(Clone)]
pub struct SubmitSide(Arc<Mutex<SubmitSideInner>>);

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

impl SubmitSideInner {
    fn plug(&mut self) -> SubmitSideOpen {
        let cur = std::mem::replace(self, SubmitSideInner::Undefined);
        match cur {
            SubmitSideInner::Undefined => panic!("implementation error"),
            SubmitSideInner::Open(open) => {
                *self = SubmitSideInner::Plugged;
                open
            }
            SubmitSideInner::Plugged => panic!("must only plug once"),
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

unsafe impl Send for SubmitSideInner {}

pub(crate) enum SubmitError {
    QueueFull,
}

pub struct GetOpsSlotFut {
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

pub struct NotInflightSlotHandle {
    state: NotInflightSlotHandleState,
}

struct UnsafeOpsSlotHandle {
    submit_side: Arc<Mutex<SubmitSideInner>>,
    idx: usize,
}

enum NotInflightSlotHandleState {
    Usable { slot: UnsafeOpsSlotHandle },
    Used,
    Dropped,
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

impl SubmitSideOpen {
    pub fn get_ops_slot(&self) -> GetOpsSlotFut {
        return GetOpsSlotFut {
            state: GetOpsSlotFutState::NotPolled {
                submit_side: Weak::upgrade(&self.myself).expect("we're executing on myself"),
            },
        };
    }
}

impl Future for GetOpsSlotFut {
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
                            drop(submit_side_open);
                            drop(submit_side_guard);
                            self.state = GetOpsSlotFutState::ReadyPolled;
                            return std::task::Poll::Ready(NotInflightSlotHandle {
                                state: NotInflightSlotHandleState::Usable {
                                    slot: UnsafeOpsSlotHandle { submit_side, idx },
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

impl NotInflightSlotHandle {
    pub(crate) fn submit<R, MakeSqe>(mut self, rsrc: R, make_sqe: MakeSqe) -> InflightOpHandle<R>
    where
        R: ResourcesOwnedByKernel + Send + 'static,
        MakeSqe: FnOnce(&mut R) -> io_uring::squeue::Entry,
    {
        let cur = std::mem::replace(&mut self.state, NotInflightSlotHandleState::Used);
        match cur {
            NotInflightSlotHandleState::Usable { slot } => slot.submit(rsrc, make_sqe),
            NotInflightSlotHandleState::Used => unreachable!("implementation error"),
            NotInflightSlotHandleState::Dropped => unreachable!("implementation error"),
        }
    }
}

impl UnsafeOpsSlotHandle {
    fn submit<R, MakeSqe>(self, mut rsrc: R, make_sqe: MakeSqe) -> InflightOpHandle<R>
    where
        R: ResourcesOwnedByKernel + Send + 'static,
        MakeSqe: FnOnce(&mut R) -> io_uring::squeue::Entry,
    {
        let sqe = make_sqe(&mut rsrc);
        let sqe = sqe.user_data(u64::try_from(self.idx).unwrap());

        let mut submit_side_guard = self.submit_side.lock().unwrap();
        let submit_side_open = submit_side_guard.must_open();

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
        InflightOpHandle {
            resources_owned_by_kernel: Some(rsrc),
            state: InflightOpHandleState::Inflight {
                slot: self,
                poll_count: 0,
            },
        }
    }
}

impl UnsafeOpsSlotHandle {
    fn return_slot_and_wake(self) {
        let UnsafeOpsSlotHandle { submit_side, idx } = self;
        let mut submit_side_guard = submit_side.lock().unwrap();
        let submit_side_open = submit_side_guard.must_open();
        let mut ops_guard = submit_side_open.ops.lock().unwrap();
        assert!(ops_guard.storage[idx].is_some());
        ops_guard.storage[idx] = None;
        ops_guard.return_slot_and_wake(Arc::clone(&submit_side), self.idx);
        drop(ops_guard);
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

enum OwnedSlotPollResult {
    Ready(i32),
    Pending(UnsafeOpsSlotHandle),
}

impl UnsafeOpsSlotHandle {
    fn poll(self, cx: &mut std::task::Context<'_>) -> OwnedSlotPollResult {
        let mut submit_side_guard = self.submit_side.lock().unwrap();
        let submit_side_open = submit_side_guard.must_open();
        let mut ops_guard = submit_side_open.ops.lock().unwrap();
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
                drop(op_state);
                drop(ops_guard);
                drop(submit_side_open);
                drop(submit_side_guard);
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
                drop(submit_side_open);
                drop(submit_side_guard);
                // important to drop the ops_guard to avoid deadlock in return_slot_and_wake
                self.return_slot_and_wake();
                return OwnedSlotPollResult::Ready(res);
            }
        }
    }
}

pub(crate) trait ResourcesOwnedByKernel {
    type OpResult;
    fn on_op_completion(self, res: i32) -> Self::OpResult;
}

impl<R: ResourcesOwnedByKernel + Send + Unpin> Future for InflightOpHandle<R> {
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
                        drop(op_state);
                        drop(ops_guard);
                        drop(submit_side_open);
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

fn setup_poller_task(
    id: usize,
    uring_fd: std::os::fd::RawFd,
    completion_side: Arc<Mutex<CompletionSide>>,
    system: System,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let _ = tokio::task::spawn(async move {
        scopeguard::defer!({
            info!("poller task is exiting");
        });
        let give_back_cq_on_drop = {
            |system, cq| {
                scopeguard::guard((system, cq), move |(system, completion_side)| {
                    tracing::info!("poller task shutdown guard spawning shutdown thread");
                    std::thread::Builder::new().name("shutdown-thread".to_owned()).spawn(move || {
                        tracing::info!("poller task shutdown thread start");
                        scopeguard::defer_on_success! {tracing::info!("poller task shutdown thread end")};
                        scopeguard::defer_on_unwind! {tracing::error!("poller task shutdown thread panic")};

                        // Prevent new ops from being submitted.
                        // Wait for all inflight ops to finish.
                        // Unsplit the uring by
                        // 1. By dropping Submitter, SubmissionQueue, CompletionQueue
                        // 2. Box::from_raw'ing the IoUring stored as a raw pointer in the System struct
                        // Drop the IoUring struct, cleaning up the underlying kernel resources.

                        let system = system; // needed to move the System, which we Unsafe-imp'ed Send
                        let System {
                            id: _,
                            split_uring,
                            submit_side,
                        } = system;

                        // Prevent new ops from being submitted.
                        let mut submit_side_guard = submit_side.lock().unwrap(); // TODO this could be an RwLock?
                        let SubmitSideOpen {
                            id: _,
                            submitter,
                            sq,
                            ops: _,
                            completion_side: submit_sides_completion_side,
                            waiters_tx: _,
                            myself: _,
                        } = submit_side_guard.plug();
                        drop(submit_side_guard);
                        assert!(
                            Arc::ptr_eq(&submit_sides_completion_side, &completion_side),
                            "internal inconsistency about System's completion side Arc and "
                        ); // ptr_eq is safe because these are not trait objects
                           // drop stuff we already have
                        drop(submit_sides_completion_side);

                        // Wait for all inflight ops to finish.
                        let completion_side = Arc::try_unwrap(completion_side).ok().expect(
                            "we plugged the SubmitSide, so, all refs to CompletionSide are gone",
                        );
                        let mut completion_side = Mutex::into_inner(completion_side).unwrap();

                        loop {
                            let ops_guard = completion_side.ops.lock().unwrap();
                            if ops_guard.unused_indices.len() == RING_SIZE as usize {
                                break;
                            }
                            drop(ops_guard);
                            completion_side.process_completions();
                            std::thread::yield_now();
                        }

                        // Unsplit the uring
                        let CompletionSide {
                            id: _,
                            cq,
                            ops,
                            submit_side: _,
                        } = completion_side;
                        // compile-time-ensure we've got the owned types here by declaring the types explicitly
                        let mut sq: SubmissionQueue<'_> = sq;
                        let submitter: Submitter<'_> = submitter;
                        let mut cq: CompletionQueue<'_> = cq;
                        // We now own all the parts from the IoUring::split() again.
                        // Some final assertions, then drop them all, unleak the IoUring, and drop it as well.
                        // That cleans up the SQ, CQs, registrations, etc.
                        cq.sync();
                        assert_eq!(cq.len(), 0, "cqe: {:?}", cq.next());
                        sq.sync();
                        assert_eq!(sq.len(), 0);
                        assert_eq!(
                            ops.try_lock().unwrap().unused_indices.len(),
                            RING_SIZE.try_into().unwrap()
                        );
                        drop(cq);
                        drop(sq);
                        drop(submitter);
                        let uring = unsafe { Box::from_raw(split_uring) };

                        // Drop the IoUring struct, cleaning up the underlying kernel resources.
                        drop(uring);
                    }).unwrap();
                })
            }
        };
        let mut cq = Some(give_back_cq_on_drop(system, completion_side));

        info!(
            "launching poller task on thread id {:?}",
            std::thread::current().id()
        );

        tokio::pin!(shutdown);
        let fd = tokio::io::unix::AsyncFd::new(uring_fd).unwrap();
        loop {
            let mut is_timeout_wakeup;
            // See fd.read() API docs for recipe for this code block.
            loop {
                is_timeout_wakeup = false;
                let mut guard = tokio::select! {
                    ready_res = fd.ready(tokio::io::Interest::READABLE) => {
                        ready_res.unwrap()
                    }
                    _ = &mut shutdown  => {
                        trace!("poller task got shutdown signal");
                        return; // shutdown_guard will do the cleanup
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        is_timeout_wakeup = true;
                        break;
                    }
                };
                if !guard.ready().is_readable() {
                    trace!("spurious wakeup");
                    continue;
                }
                guard.clear_ready_matching(tokio::io::Ready::READABLE);
                break;
            }

            let (system, unprotected_cq_arc) =
                scopeguard::ScopeGuard::into_inner(cq.take().unwrap());
            let mut unprotected_cq = unprotected_cq_arc.lock().unwrap();
            unprotected_cq.process_completions(); // todo: catch_unwind to enable orderly shutdown? or at least abort if it panics?
            if is_timeout_wakeup {
                let ops = unprotected_cq.ops.lock().unwrap();
                let mut by_state_discr = HashMap::new();
                for s in &ops.storage {
                    match s {
                        Some(opstate) => {
                            let opstate_inner = opstate.0.lock().unwrap();
                            let state_discr = opstate_inner.discriminant_str();
                            by_state_discr
                                .entry(state_discr)
                                .and_modify(|v| *v += 1)
                                .or_insert(1);
                        }
                        None => {
                            by_state_discr.entry("None").and_modify(|v| *v += 1).or_insert(1);
                        },
                    }
                }
                debug!(
                    "poller task got timeout: ops free slots = {} by_ops_state: {:?}",
                    ops.unused_indices.len(),
                    by_state_discr
                );
            }
            drop(unprotected_cq);
            cq = Some(give_back_cq_on_drop(system, unprotected_cq_arc));
        }
    }.instrument(info_span!(parent: None, "poller_task", system=id )));
}

impl SubmitSide {
    pub(crate) async fn submit<R, MakeSqe>(self, rsrc: R, make_sqe: MakeSqe) -> R::OpResult
    where
        R: ResourcesOwnedByKernel + Send + Unpin + 'static,
        MakeSqe: FnOnce(&mut R) -> io_uring::squeue::Entry,
    {
        let slot_fut = {
            let SubmitSide(inner) = self;
            let mut submit_side_guard = inner.lock().unwrap();
            let submit_side_open = submit_side_guard.must_open();
            submit_side_open.get_ops_slot()
        };
        let slot = slot_fut.await;
        slot.submit(rsrc, make_sqe).await
    }
}
