use std::{
    collections::HashMap,
    future::Future,
    os::fd::AsRawFd,
    sync::{Arc, Mutex, Weak},
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tokio::sync::oneshot;
use tracing::{debug, info_span, trace, Instrument};

use crate::shutdown_request::WaitForShutdownResult;

pub(crate) struct System {
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    id: usize,
    split_uring: *mut io_uring::IoUring,
    // poller_heartbeat: (), // TODO
}

// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Send for System {}
// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Sync for System {}

impl System {
    pub(crate) async fn launch() -> SystemHandle {
        static POLLER_TASK_ID: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let id = POLLER_TASK_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // TODO: this unbounded channel is the root of all evil: unbounded queue for IOPS; should provie app option to back-pressure instead.
        let (waiters_tx, waiters_rx) = tokio::sync::mpsc::unbounded_channel();
        let ops = Arc::new_cyclic(|myself| {
            Mutex::new(Ops {
                id,
                storage: array_macro::array![_ => None; RING_SIZE as usize],
                unused_indices: (0..RING_SIZE.try_into().unwrap()).collect(),
                waiters_rx,
                myself: Weak::clone(&myself),
            })
        });
        let uring = Box::into_raw(Box::new(io_uring::IoUring::new(RING_SIZE).unwrap()));
        let uring_fd = unsafe { (*uring).as_raw_fd() };
        let (submitter, sq, cq) = unsafe { (&mut *uring).split() };

        let completion_side = Arc::new(Mutex::new(CompletionSide {
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
                completion_side: Arc::clone(&completion_side),
                ops,
                waiters_tx,
                myself: Weak::clone(myself),
            }))
        });
        let system = System {
            id,
            split_uring: uring,
        };
        let (shutdown_tx, shutdown_rx) = crate::shutdown_request::new();
        let poller_task_state = Arc::new(Mutex::new(PollerState::RunningInTask(Arc::new(
            Mutex::new(PollerStateInner {
                id,
                uring_fd,
                completion_side,
                system,
                shutdown_rx,
            }),
        ))));
        let (poller_ready_tx, poller_ready_rx) = oneshot::channel();
        tokio::task::spawn(poller_task(id, poller_task_state, poller_ready_tx));
        poller_ready_rx.await;
        SystemHandle {
            state: SystemHandleState::KeepSystemAlive(SystemHandleLive {
                id,
                submit_side: SubmitSide(submit_side),
                shutdown_tx,
            }),
        }
    }
}

pub struct SystemHandle {
    pub(crate) state: SystemHandleState,
}

pub(crate) enum SystemHandleState {
    KeepSystemAlive(SystemHandleLive),
    ExplicitShutdownRequestOngoing,
    ExplicitShutdownRequestDone,
    ImplicitShutdownRequestThroughDropOngoing,
    ImplicitShutdownRequestThroughDropDone,
}
pub(crate) struct SystemHandleLive {
    #[allow(dead_code)]
    id: usize,
    pub(crate) submit_side: SubmitSide,
    shutdown_tx: crate::shutdown_request::Sender<ShutdownRequest>,
}

impl Drop for SystemHandle {
    fn drop(&mut self) {
        let cur = std::mem::replace(
            &mut self.state,
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing,
        );
        match cur {
            SystemHandleState::ExplicitShutdownRequestOngoing => {
                panic!("implementation error, likely panic during explicit shutdown")
            }
            SystemHandleState::ExplicitShutdownRequestDone => (), // nothing to do
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing => {
                unreachable!("only this function sets that state, and it gets called exactly once")
            }
            SystemHandleState::ImplicitShutdownRequestThroughDropDone => {
                unreachable!("only this function sets that state, and it gets called exactly once")
            }
            SystemHandleState::KeepSystemAlive(live) => {
                let _ = live.shutdown(); // we don't care about the result
                self.state = SystemHandleState::ImplicitShutdownRequestThroughDropDone;
            }
        }
    }
}

impl SystemHandle {
    /// Initiate system shtudown and return a future to await completion of shutdown.
    ///
    /// If this call returns Ok(), it is guaranteed that no new operations
    /// will be allowed to start (TODO: not true right now, since poller task plugs the queue).
    /// Currently, such attempts will cause a panic but we may change the behavior
    /// to make it return a custom [`std::io::Error`] instead.
    ///
    /// Shutdown must wait for all in-flight operations to finish..
    /// Shutdown may spawn an `std::thread` for this purpose.
    /// It would generally be unsafe to have "force shutdown" after a timeout because
    /// in io_uring, the kernel owns resources such as memory buffers while operations are in flight.
    pub fn shutdown(mut self) -> impl Future<Output = ()> + Send + Unpin {
        let cur = std::mem::replace(
            &mut self.state,
            SystemHandleState::ExplicitShutdownRequestOngoing,
        );
        match cur {
            SystemHandleState::ExplicitShutdownRequestOngoing => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::ExplicitShutdownRequestDone => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::ImplicitShutdownRequestThroughDropDone => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::KeepSystemAlive(live) => {
                let ret = live.shutdown();
                self.state = SystemHandleState::ExplicitShutdownRequestDone;
                ret
            }
        }
    }
}

impl SystemHandleState {
    pub(crate) fn guaranteed_live(&self) -> &SystemHandleLive {
        match self {
            SystemHandleState::ExplicitShutdownRequestOngoing => unreachable!("caller guarantees that system handle is live, but it is in state ExplicitShutdownRequestOngoing"),
            SystemHandleState::ExplicitShutdownRequestDone => unreachable!("caller guarantees that system handle is live, but it is in state ExplicitShutdownRequestDone"),
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing => unreachable!("caller guarantees that system handle is live, but it is in state ImplicitShutdownRequestThroughDrop"),
            SystemHandleState::ImplicitShutdownRequestThroughDropDone => unreachable!("caller guarantees that system handle is live, but it is in state ImplicitShutdownRequestThroughDropDone"),
            SystemHandleState::KeepSystemAlive(live) => live,
        }
    }
}

struct WaitShutdownFut {
    done_rx: tokio::sync::oneshot::Receiver<()>,
}

impl Future for WaitShutdownFut {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let done_rx = &mut self.done_rx;
        let done_rx = std::pin::Pin::new(done_rx);
        match done_rx.poll(cx) {
            std::task::Poll::Ready(res) => match res {
                Ok(()) => std::task::Poll::Ready(()),
                Err(_) => panic!("implementation error: poller must not die before SystemHandle"),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl SystemHandleLive {
    fn shutdown(self) -> impl Future<Output = ()> + Send + Unpin {
        let SystemHandleLive {
            id: _,
            submit_side,
            shutdown_tx,
        } = self;
        let mut submit_side_guard = submit_side.0.lock().unwrap();
        let open_state = match submit_side_guard.plug() {
            Ok(open_state) => open_state,
            Err(PlugError::AlreadyPlugged) => panic!(
                "implementation error: its solely the SystemHandle's job to plug the submit side"
            ),
        };
        drop(submit_side_guard);
        drop(submit_side);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let req = ShutdownRequest {
            open_state,
            done_tx,
        };
        shutdown_tx.shutdown(req);
        WaitShutdownFut { done_rx }
    }
}

// mod private {
//     pub trait Sealed {}
//     impl Sealed for &'_ crate::system_lifecycle::BorrowBased {}
//     impl Sealed for crate::system_lifecycle::ThreadLocal {}
//     impl Sealed for &'_ super::SystemHandle {}
// }

impl SubmitSideProvider for &'_ SystemHandle {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(self, f: F) -> R {
        f(self.state.guaranteed_live().submit_side.clone())
    }
}

pub trait SubmitSideProvider: Unpin {
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
    myself: Weak<Mutex<Ops>>,
}

impl Ops {
    // TODO: why do we need the submit_side here?
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
                        ops: Weak::upgrade(&self.myself).expect("we're executing on myself"),
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
                    // submit side is plugged already
                    break;
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

unsafe impl Send for SubmitSideOpen {}

enum PlugError {
    AlreadyPlugged,
}
impl SubmitSideInner {
    fn plug(&mut self) -> Result<SubmitSideOpen, PlugError> {
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
    ops: Arc<Mutex<Ops>>,
    idx: usize,
    // only used in some modes
    submit_side: Arc<Mutex<SubmitSideInner>>,
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
                            let ops = Arc::clone(&submit_side_open.ops);
                            drop(submit_side_open);
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
                drop(op_state);
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

struct ShutdownRequest {
    done_tx: tokio::sync::oneshot::Sender<()>,
    open_state: SubmitSideOpen,
}

enum PollerState {
    Undefined,
    RunningInTask(Arc<Mutex<PollerStateInner>>),
    SwitchingFromTaskToThread,
    RunningInThread(Arc<Mutex<PollerStateInner>>),
    ShuttingDown,
    ShutDown,
}

impl std::fmt::Debug for PollerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollerState::Undefined => write!(f, "Undefined"),
            PollerState::RunningInTask(_) => write!(f, "RunningInTask"),
            PollerState::SwitchingFromTaskToThread => write!(f, "SwitchingFromTaskToThread"),
            PollerState::RunningInThread(_) => write!(f, "RunningInThread"),
            PollerState::ShuttingDown => write!(f, "ShuttingDown"),
            PollerState::ShutDown => write!(f, "ShutDown"),
        }
    }
}

struct PollerStateInner {
    id: usize,
    uring_fd: std::os::fd::RawFd,
    completion_side: Arc<Mutex<CompletionSide>>,
    system: System,
    shutdown_rx: crate::shutdown_request::Receiver<ShutdownRequest>,
}

async fn poller_task(id: usize, state: Arc<Mutex<PollerState>>, poller_ready: oneshot::Sender<()>) {
    let _switch_to_thread_if_task_gets_dropped =
        scopeguard::guard(Arc::clone(&state), move |state| {
            let mut state_guard = state.lock().unwrap();
            let cur = std::mem::replace(&mut *state_guard, PollerState::Undefined);
            let inner = match cur {
                PollerState::ShutDown => {
                    // we're done
                    *state_guard = PollerState::ShutDown;
                    return;
                }
                PollerState::RunningInTask(inner) => {
                    tracing::info!("poller task is getting dropped, switching to thread");
                    *state_guard = PollerState::SwitchingFromTaskToThread;
                    inner
                }
                PollerState::SwitchingFromTaskToThread
                | PollerState::RunningInThread(_)
                | PollerState::ShuttingDown
                | PollerState::Undefined => unreachable!("unexpected state: {cur:?}"),
            };
            let state_clone = Arc::clone(&state);
            std::thread::Builder::new()
                .name(format!("{}-shutdown-thread", id))
                .spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .enable_io()
                        .build()
                        .unwrap()
                        .block_on(async move {
                            let state = state_clone;
                            let mut state_guard = state.lock().unwrap();
                            let cur = std::mem::replace(&mut *state_guard, PollerState::Undefined);
                            match cur {
                                PollerState::SwitchingFromTaskToThread => {
                                    *state_guard = PollerState::RunningInThread(inner);
                                    drop(state_guard);
                                    poller_impl(state)
                                        .instrument(info_span!("poller_thread", system=%id))
                                        .await
                                }
                                PollerState::Undefined
                                | PollerState::RunningInTask(_)
                                | PollerState::RunningInThread(_)
                                | PollerState::ShuttingDown
                                | PollerState::ShutDown => {
                                    unreachable!("unexpected state: {cur:x?}")
                                }
                            }
                        })
                })
                .unwrap();
        });
    let _ = poller_ready.send(());
    poller_impl(state)
        .instrument(info_span!("poller_task", system=%id))
        .await;
}

async fn poller_impl(state: Arc<Mutex<PollerState>>) {
    let inner = {
        let state_guard = state.lock().unwrap();
        match &*state_guard {
            PollerState::Undefined | PollerState::SwitchingFromTaskToThread => {
                panic!("implementation error")
            }
            PollerState::ShutDown => {
                unreachable!("if poller_impl_impl shuts shuts down, we never get back here, caller guarantees it")
            }
            PollerState::ShuttingDown => {
                unreachable!(
                    "only this function transitions into that state, and then never revisits"
                )
            }
            PollerState::RunningInTask(inner) | PollerState::RunningInThread(inner) => {
                Arc::clone(&inner)
            }
        }
    };
    let shutdown_req = poller_impl_impl(Arc::clone(&state), Arc::clone(&inner)).await;
    let cur = std::mem::replace(&mut *state.lock().unwrap(), PollerState::ShuttingDown);

    // Prevent new ops from being submitted.
    // Wait for all inflight ops to finish.
    // Unsplit the uring by
    // 1. By dropping Submitter, SubmissionQueue, CompletionQueue
    // 2. Box::from_raw'ing the IoUring stored as a raw pointer in the System struct
    // Drop the IoUring struct, cleaning up the underlying kernel resources.

    // Prevent new ops from being submitted:
    // => SystemHandle::drop / SystemHandle::shutdown already plugged the submit side. Nothing to do.

    // Wait for all inflight ops to finish.
    loop {
        {
            let inner_guard = inner.lock().unwrap();
            let mut completion_side_guard = inner_guard.completion_side.lock().unwrap();
            let ops_guard = completion_side_guard.ops.lock().unwrap();
            if ops_guard.unused_indices.len() == RING_SIZE as usize {
                break;
            }
            drop(ops_guard);
            completion_side_guard.process_completions();
        }
        // if we get preempted here, e.g., because the runtime is getting dropped, then the task is in state ShuttingDown.
        // The scopeguard in the caller will spawn a thread to re-call us and resume here
        tokio::task::yield_now().await;
    }

    // no await preemption from here on, enforce it through the closure
    (move || {
        drop(inner); // this should make us the only owner
        let inner_owned: PollerStateInner = match cur {
            PollerState::RunningInTask(inner) | PollerState::RunningInThread(inner) => {
                let owned = Arc::try_unwrap(inner)
                    .ok()
                    .expect("we replaced the state with ShuttingDown, so, we're the only owner");
                Mutex::into_inner(owned).unwrap()
            }
            PollerState::Undefined => todo!(),
            PollerState::SwitchingFromTaskToThread => todo!(),
            PollerState::ShuttingDown => todo!(),
            PollerState::ShutDown => todo!(),
        };
        poller_impl_finish_shutdown(inner_owned, shutdown_req);
        *state.lock().unwrap() = PollerState::ShutDown;
    })()
}

async fn poller_impl_impl(
    state: Arc<Mutex<PollerState>>,
    inner: Arc<Mutex<PollerStateInner>>,
) -> ShutdownRequest {
    let (uring_fd, completion_side, shutdown_rx) = {
        let mut inner_guard = inner.lock().unwrap();
        let PollerStateInner {
            id: _,
            uring_fd,
            completion_side,
            system,
            shutdown_rx: ref mut shutdown,
        } = &mut *inner_guard;
        (*uring_fd, Arc::clone(completion_side), shutdown.clone())
    };

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
                rx = shutdown_rx.wait_for_shutdown_request()  => {
                    match rx {
                        WaitForShutdownResult::ExplicitRequest(req) => {
                            return req;
                        }
                        WaitForShutdownResult::ExplicitRequestObservedEarlier => {
                            panic!("once we observe a shutdown request, we return it and the caller does through with shutdown, without a chance for the executor to intervene")
                        }
                        WaitForShutdownResult::SenderDropped => {
                            panic!("implementation error: SystemHandle _must_ send shutdown request");
                        }
                    }
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

        let mut completion_side_guard = completion_side.lock().unwrap();
        completion_side_guard.process_completions(); // todo: catch_unwind to enable orderly shutdown? or at least abort if it panics?
        if is_timeout_wakeup {
            let ops = completion_side_guard.ops.lock().unwrap();
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
                        by_state_discr
                            .entry("None")
                            .and_modify(|v| *v += 1)
                            .or_insert(1);
                    }
                }
            }
            debug!(
                "poller task got timeout: ops free slots = {} by_ops_state: {:?}",
                ops.unused_indices.len(),
                by_state_discr
            );
        }
        drop(completion_side_guard);
    }
}

fn poller_impl_finish_shutdown(inner_owned: PollerStateInner, req: ShutdownRequest) {
    tracing::info!("poller shutdown start");
    scopeguard::defer_on_success! {tracing::info!("poller shutdown end")};
    scopeguard::defer_on_unwind! {tracing::error!("poller shutdown panic")};

    let PollerStateInner {
        id: _,
        uring_fd: _,
        completion_side,
        system,
        shutdown_rx: _,
    } = inner_owned;

    let system = system; // needed to move the System, which we Unsafe-imp'ed Send
    let System { id: _, split_uring } = system;

    let ShutdownRequest {
        done_tx,
        open_state,
    } = req;

    let SubmitSideOpen {
        id: _,
        submitter,
        sq,
        ops: _,
        completion_side: submit_sides_completion_side,
        waiters_tx: _,
        myself: _,
    } = open_state;
    assert!(
        Arc::ptr_eq(&submit_sides_completion_side, &completion_side),
        "internal inconsistency about System's completion side Arc and "
    ); // ptr_eq is safe because these are not trait objects
       // drop stuff we already have
    drop(submit_sides_completion_side);
    let completion_side = Arc::try_unwrap(completion_side)
        .ok()
        .expect("we plugged the SubmitSide, so, all refs to CompletionSide are gone");
    let mut completion_side = Mutex::into_inner(completion_side).unwrap();

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

    // notify about completed shutdown;
    // ignore send errors, interest may be gone if it's implicit shutdown through SystemHandle::drop
    let _ = done_tx.send(());
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

#[cfg(test)]
mod submit_side_tests {
    use tracing::trace;

    use crate::SubmitSideProvider;

    use super::{InflightOpHandle, NotInflightSlotHandle};
    struct MockOp {}
    fn submit_mock_op(slot: NotInflightSlotHandle) -> InflightOpHandle<MockOp> {
        impl super::ResourcesOwnedByKernel for MockOp {
            type OpResult = ();
            fn on_op_completion(self, _res: i32) -> Self::OpResult {}
        }
        let submit_fut = slot.submit(MockOp {}, |_| io_uring::opcode::Nop::new().build());
        submit_fut
    }

    #[tokio::test]
    async fn shutdown_waits_for_ongoing_ops() {
        // tracing_subscriber::fmt::init();

        let system = crate::launch_shared().await;
        let slot = system
            .clone()
            .with_submit_side(|submit_side| {
                let mut guard = submit_side.0.lock().unwrap();
                let guard = guard.must_open();
                guard.get_ops_slot()
            })
            .await;
        let submit_fut = submit_mock_op(slot);
        let shutdown_done = system.shutdown();
        tokio::pin!(shutdown_done);
        tokio::select! {
            // TODO don't rely on timing
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            _ = &mut shutdown_done => {
                panic!("shutdown should not complete until submit_fut is done");
            }
        }
        println!("waiting submit_fut");
        let _: () = submit_fut.await;
        println!("submit_fut is done");
        tokio::select! {
            // TODO don't rely on timing
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                panic!("shutdown should complete after submit_fut is done");
            }
            _ = &mut shutdown_done => { }
        }
    }

    #[test]
    fn poller_task_dropped_during_shutdown() {
        // tracing_subscriber::fmt::init();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let (slot, shutdown_done) = rt.block_on(async move {
            let system = crate::launch_shared().await;
            let slot = system
                .clone()
                .with_submit_side(|submit_side| {
                    let mut guard = submit_side.0.lock().unwrap();
                    let guard = guard.must_open();
                    guard.get_ops_slot()
                })
                .await;
            // When we send shutdown, the slot will keep the poller task in the shutdown process_completions loop.
            // Then we'll drop the rt, which causes the poller task to get dropped.
            // We should observe a transition to RunningInThread.
            let shutdown_done = system.shutdown();
            (slot, shutdown_done)
        });
        rt.shutdown_background();

        let new_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        new_rt.block_on(async move {
            let mut shutdown_done = shutdown_done;
            tokio::select! {
                // TODO don't rely on timing
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => { }
                _ = &mut shutdown_done => {
                    panic!("shutdown should not complete until submit_fut is done");
                }
            }
            // clear the slot
            drop(slot);

            tokio::select! {
                // TODO don't rely on timing
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    panic!("shutdown should complete after submit_fut is done");
                }
                _ = &mut shutdown_done => { }
            }
        });
    }
}
