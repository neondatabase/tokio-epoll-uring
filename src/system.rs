use std::{
    collections::HashMap,
    future::Future,
    os::fd::AsRawFd,
    sync::{Arc, Mutex, Weak},
};

use futures::FutureExt;
use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tokio::sync::{self, broadcast, mpsc, oneshot};
use tracing::{debug, info, info_span, trace, Instrument};

use crate::{
    shutdown_request::WaitForShutdownResult,
    thread_local_system_handle::ThreadLocalSubmitSideProvider, SharedSystemHandle,
};

/// The live system. Not constructible or accessible by user code. Use [`SystemHandle`] to interact.
pub struct System {
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

pub struct Launch {
    id: usize,
    state: LaunchState,
}

enum LaunchState {
    Init {
        // TODO: cfg test
        poller_preempt: Option<PollerTesting>,
    },
    WaitForPollerTaskToStart {
        poller_ready_rx: oneshot::Receiver<()>,
        system_handle_not_safe_for_use_yet: SystemHandle,
    },
    Launched,
    Undefined,
}

impl System {
    pub fn launch() -> Launch {
        static POLLER_TASK_ID: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let id = POLLER_TASK_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Launch {
            id,
            state: LaunchState::Init {
                poller_preempt: None,
            },
        }
    }
    #[cfg(test)]
    fn launch_with_testing(poller_preempt: PollerTesting) -> Launch {
        static POLLER_TASK_ID: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let id = POLLER_TASK_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Launch {
            id,
            state: LaunchState::Init {
                poller_preempt: Some(poller_preempt),
            },
        }
    }
}

impl Future for Launch {
    type Output = SystemHandle;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let id = self.id;
        let myself = self.get_mut();
        loop {
            let cur = { std::mem::replace(&mut myself.state, LaunchState::Undefined) };
            match cur {
                LaunchState::Undefined => unreachable!("implementation error"),
                LaunchState::Init { poller_preempt } => {
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
                    let poller_task_state = Arc::new(Mutex::new(Poller {
                        id,
                        state: PollerState::RunningInTask(Arc::new(Mutex::new(PollerStateInner {
                            uring_fd,
                            completion_side,
                            system,
                            shutdown_rx,
                        }))),
                    }));
                    let (poller_ready_tx, poller_ready_rx) = oneshot::channel();
                    tokio::task::spawn(poller_task(
                        Arc::clone(&poller_task_state),
                        poller_ready_tx,
                        poller_preempt,
                    ));
                    let system_handle_not_safe_for_use_yet = SystemHandle {
                        state: SystemHandleState::KeepSystemAlive(SystemHandleLive {
                            id,
                            submit_side: SubmitSide(submit_side),
                            shutdown_tx,
                        }),
                    };
                    myself.state = LaunchState::WaitForPollerTaskToStart {
                        poller_ready_rx,
                        system_handle_not_safe_for_use_yet,
                    };
                    continue;
                }
                LaunchState::WaitForPollerTaskToStart {
                    mut poller_ready_rx,
                    system_handle_not_safe_for_use_yet,
                } => match poller_ready_rx.poll_unpin(cx) {
                    std::task::Poll::Ready(Ok(())) => {
                        myself.state = LaunchState::Launched;
                        return std::task::Poll::Ready(system_handle_not_safe_for_use_yet);
                    }
                    std::task::Poll::Ready(Err(_)) => {
                        // TODO can this happen?
                        panic!("implementation error: poller task must not die before SystemHandle")
                    }
                    std::task::Poll::Pending => {
                        myself.state = LaunchState::WaitForPollerTaskToStart {
                            poller_ready_rx,
                            system_handle_not_safe_for_use_yet,
                        };
                        return std::task::Poll::Pending;
                    }
                },
                LaunchState::Launched => {
                    unreachable!("we already returned Ready(SystemHandle) above, polling after is not allowed");
                }
            }
        }
    }
}

/// An indirection to allow [`crate::ops`] to be generic over which [`System`] to use.
///
/// Check the "Implementors" section for the list of available [`System`]s.
/// Check any [`crate::ops`] function or the crate's examples to see where you need this.
///
/// The name of this trait is subject to debate.
pub trait SystemLauncher<P>: Future<Output = P>
where
    P: SubmitSideProvider,
{
}
impl<'a> SystemLauncher<&'a SystemHandle> for std::future::Ready<&'a SystemHandle> {}
impl SystemLauncher<ThreadLocalSubmitSideProvider> for crate::ThreadLocalSystemLauncher {}
impl SystemLauncher<SharedSystemHandle> for std::future::Ready<SharedSystemHandle> {}

/// Owned handle to the [`System`] launched by [`SystemLauncher`].
///
/// The only use of this handle is to shut down the [`System`].
/// Call [`initiate_shutdown`](SystemHandle::initiate_shutdown) for explicit shutdown with ability to wait for shutdown completion.
///
/// Alternatively, `drop` will also request shutdown, but not wait for completion of shutdown.
///
/// This handle is [`Send`] but not [`Clone`].
/// If you need to share it between threads, use [`crate::SharedSystemHandle`].
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
    pub fn initiate_shutdown(mut self) -> impl Future<Output = ()> + Send + Unpin {
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

unsafe impl Send for SubmitSideOpen {}
unsafe impl Sync for SubmitSideOpen {}

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

struct ShutdownRequest {
    done_tx: tokio::sync::oneshot::Sender<()>,
    open_state: SubmitSideOpen,
}

struct Poller {
    id: usize,
    state: PollerState,
}

enum PollerState {
    Undefined,
    RunningInTask(Arc<Mutex<PollerStateInner>>),
    RunningInThread(Arc<Mutex<PollerStateInner>>),
    ShuttingDownPreemptible(Arc<Mutex<PollerStateInner>>, Arc<ShutdownRequest>),
    ShuttingDownNoMorePreemptible,
    ShutDown,
}

impl std::fmt::Debug for PollerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollerState::Undefined => write!(f, "Undefined"),
            PollerState::RunningInTask(_) => write!(f, "RunningInTask"),
            PollerState::RunningInThread(_) => write!(f, "RunningInThread"),
            PollerState::ShuttingDownPreemptible(_, _) => write!(f, "ShuttingDownPreemptible"),
            PollerState::ShuttingDownNoMorePreemptible => {
                write!(f, "ShuttingDownNoMorePreemptible")
            }
            PollerState::ShutDown => write!(f, "ShutDown"),
        }
    }
}

struct PollerStateInner {
    uring_fd: std::os::fd::RawFd,
    completion_side: Arc<Mutex<CompletionSide>>,
    system: System,
    shutdown_rx: crate::shutdown_request::Receiver<ShutdownRequest>,
}

struct PollerTesting {
    shutdown_loop_reached_tx: tokio::sync::mpsc::UnboundedSender<Arc<Mutex<Poller>>>,
    preempt_outer_rx: oneshot::Receiver<()>,
    preempt_during_epoll_rx: oneshot::Receiver<()>,
    poller_switch_to_thread_done_tx: oneshot::Sender<Arc<Mutex<Poller>>>,
}

async fn poller_task(
    poller: Arc<Mutex<Poller>>,
    poller_ready: oneshot::Sender<()>,
    testing: Option<PollerTesting>,
) {
    let id = poller.lock().unwrap().id;
    let preempt_outer_rx;
    let poller_switch_to_thread_done;
    let shutdown_loop_reached;
    let preempt_during_epoll_rx;
    match testing {
        None => {
            preempt_outer_rx = None;
            poller_switch_to_thread_done = None;
            shutdown_loop_reached = None;
            preempt_during_epoll_rx = None;
        }
        Some(testing) => {
            preempt_outer_rx = Some(testing.preempt_outer_rx);
            poller_switch_to_thread_done = Some(testing.poller_switch_to_thread_done_tx);
            shutdown_loop_reached = Some(testing.shutdown_loop_reached_tx);
            preempt_during_epoll_rx = Some(testing.preempt_during_epoll_rx);
        }
    }
    let switch_to_thread_if_task_gets_dropped = scopeguard::guard(
        (Arc::clone(&poller), poller_switch_to_thread_done),
        |(poller, poller_switch_to_thread_done)| {
            let shutdown_loop_reached = shutdown_loop_reached.clone();
            let span = info_span!("poller_task_scopeguard", system=%id);
            let _entered = span.enter(); // safe to use here because there's no more .await
            let mut poller_guard = poller.lock().unwrap();
            let cur = std::mem::replace(&mut poller_guard.state, PollerState::Undefined);
            match cur {
                PollerState::ShutDown => {
                    // we're done
                    poller_guard.state = PollerState::ShutDown;
                    return;
                }
                x @ PollerState::ShuttingDownPreemptible(_, _) => {
                    tracing::info!("poller task dropped while shutting down");
                    poller_guard.state = x;
                }
                PollerState::RunningInTask(inner) => {
                    tracing::info!("poller task dropped while running poller_impl_impl");
                    poller_guard.state = PollerState::RunningInThread(inner);
                }
                PollerState::RunningInThread(_)
                | PollerState::Undefined
                | PollerState::ShuttingDownNoMorePreemptible => {
                    unreachable!("unexpected state: {cur:?}")
                }
            }
            let poller_clone = Arc::clone(&poller);
            std::thread::Builder::new()
                .name(format!("{}-poller-thread", id))
                .spawn(move || {
                    let span = info_span!("poller_thread", system=%id);
                    let _entered = span.enter(); // safe to use here because we use new_current_thread
                    info!("poller thread running");
                    tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .enable_io()
                        .build()
                        .unwrap()
                        .block_on(async move {
                            let poller = poller_clone;
                            let mut poller_guard = poller.lock().unwrap();
                            let cur =
                                std::mem::replace(&mut poller_guard.state, PollerState::Undefined);
                            match cur {
                                x @ PollerState::RunningInThread(_)
                                | x @ PollerState::ShuttingDownPreemptible(_, _) => {
                                    poller_guard.state = x;
                                    drop(poller_guard);
                                    if let Some(tx) = poller_switch_to_thread_done {
                                        // receiver must ensure that clone doesn't outlive the try_unwrap during shutdown
                                        tx.send(Arc::clone(&poller)).ok().unwrap();
                                    }
                                    poller_impl(poller, None, shutdown_loop_reached.clone())
                                        .instrument(info_span!("poller_thread", system=%id))
                                        .await
                                }
                                PollerState::Undefined
                                | PollerState::RunningInTask(_)
                                | PollerState::ShuttingDownNoMorePreemptible
                                | PollerState::ShutDown => {
                                    unreachable!("unexpected state: {cur:x?}")
                                }
                            }
                        })
                })
                .unwrap();
        },
    );
    // scopeguard is installed, call the launch complete
    let _ = poller_ready.send(());
    let (preempt_during_epoll_doit_tx, preempt_during_epoll_doit_rx) = broadcast::channel(1);
    let (preempt_due_to_preempt_durign_epoll_tx, mut preempt_due_to_preempt_during_epoll_rx) =
        mpsc::unbounded_channel();
    if let Some(rx) = preempt_during_epoll_rx {
        tokio::spawn(async move {
            let _ = rx.await;
            preempt_during_epoll_doit_tx
                .send(preempt_due_to_preempt_durign_epoll_tx)
                .unwrap();
        });
    }
    tokio::select! {
        _ = poller_impl(Arc::clone(&poller), Some(preempt_during_epoll_doit_rx) , shutdown_loop_reached.clone()).instrument(info_span!("poller_task", system=%id)) => {},
        _ = preempt_due_to_preempt_during_epoll_rx.recv() => { },
        _ = async move { match preempt_outer_rx {
            Some(preempt) => {
                preempt.await
            },
            None => {
                futures::future::pending().await
            },
        } } => { },
    }

    // just to make it abundantely clear that scopeguard is _also_ involved on regular control flow
    drop(switch_to_thread_if_task_gets_dropped);
}

async fn poller_impl(
    poller: Arc<Mutex<Poller>>,
    preempt_in_epoll: Option<sync::broadcast::Receiver<mpsc::UnboundedSender<()>>>,
    shutdown_loop_reached: Option<tokio::sync::mpsc::UnboundedSender<Arc<Mutex<Poller>>>>,
) {
    info!("poller_impl running");
    // Run poller_impl_impl. It only returns if we got a shutdown request.
    // If we get cancelled at an await point, the caller's scope-guard will spawn an OS thread and re-run this function.
    // So, keep book about our state.
    let (inner_shared, maybe_shutdown_req_shared) = {
        let mut poller_guard = poller.lock().unwrap();
        let cur = std::mem::replace(&mut poller_guard.state, PollerState::Undefined);
        match cur {
            PollerState::Undefined => {
                panic!("implementation error")
            }
            PollerState::ShuttingDownNoMorePreemptible => unreachable!(),
            PollerState::ShutDown => {
                unreachable!("if poller_impl_impl shuts shuts down, we never get back here, caller guarantees it")
            }
            PollerState::ShuttingDownPreemptible(inner, req) => {
                let inner_clone = Arc::clone(&inner);
                let req_clone = Arc::clone(&req);
                poller_guard.state = PollerState::ShuttingDownPreemptible(inner, req);
                (inner_clone, Some(req_clone))
            }
            PollerState::RunningInTask(inner) => {
                let clone = Arc::clone(&inner);
                poller_guard.state = PollerState::RunningInTask(inner);
                (clone, None)
            }
            PollerState::RunningInThread(inner) => {
                let clone = Arc::clone(&inner);
                poller_guard.state = PollerState::RunningInThread(inner);
                (clone, None)
            }
        }
    };
    let shutdown_req_shared = match maybe_shutdown_req_shared {
        None => {
            let shutdown_req: ShutdownRequest = tokio::select! {
                req = poller_impl_impl(Arc::clone(&inner_shared), preempt_in_epoll) => { req },
            };
            let shared = Arc::new(shutdown_req);
            poller.lock().unwrap().state = PollerState::ShuttingDownPreemptible(
                Arc::clone(&inner_shared),
                Arc::clone(&shared),
            );
            shared
        }
        Some(shared) => shared,
    };

    // We got the shutdown request; what do we need to do?
    // 1. Prevent new ops from being submitted.
    // 2. Wait for all inflight ops to finish.
    // 3. Unsplit the uring by
    //      3.1. By dropping Submitter, SubmissionQueue, CompletionQueue
    //      3 2. Box::from_raw'ing the IoUring stored as a raw pointer in the System struct
    // 4. Drop the IoUring struct, cleaning up the underlying kernel resources.

    // 1. Prevent new ops from being submitted:
    // Already plugged the sumit side in SystemHandle::drop / SystemHandle::shutdown;

    // Wait for all inflight ops to finish.
    if let Some(tx) = shutdown_loop_reached {
        // receiver must ensure that clone doesn't outlive the try_unwrap during shutdown
        tx.send(Arc::clone(&poller)).ok().unwrap();
    }
    loop {
        {
            let inner_guard = inner_shared.lock().unwrap();
            let mut completion_side_guard = inner_guard.completion_side.lock().unwrap();
            let ops_guard = completion_side_guard.ops.lock().unwrap();
            if ops_guard.unused_indices.len() == RING_SIZE as usize {
                break;
            }
            drop(ops_guard);
            completion_side_guard.process_completions();
        }
        // If we get cancelled here, e.g., because the runtime is getting dropped,
        // the Poller is in state ShuttingDown.
        // The scopeguard in our caller will spawn an OS thread and re-run this function.
        tokio::task::yield_now().await;
    }

    // From here on, we cannot let ourselves be cancelled at an `.await` anymore.
    // (See comment on `yield_now().await` above why preemption is safe)
    // Use a closure to enforce it.
    (move || {
        let mut poller_guard = poller.lock().unwrap();
        let cur = std::mem::replace(
            &mut poller_guard.state,
            PollerState::ShuttingDownNoMorePreemptible,
        );
        drop(poller_guard);
        match cur {
            x @ PollerState::RunningInTask(_)
            | x @ PollerState::RunningInThread(_)
            | x @ PollerState::ShutDown
            | x @ PollerState::ShuttingDownNoMorePreemptible
            | x @ PollerState::Undefined => unreachable!("unexpected state: {x:?}"),
            PollerState::ShuttingDownPreemptible(inner, req) => {
                assert!(Arc::ptr_eq(&inner_shared, &inner));
                assert!(Arc::ptr_eq(&shutdown_req_shared, &req));
                drop(inner_shared); // this should make `poller` the only owner, needed for try_unwrap below
                drop(shutdown_req_shared); // this should make `poller` the only owner, needed for try_unwrap below
                let owned = Arc::try_unwrap(inner)
                    .ok()
                    .expect("we replaced the state with ShuttingDown, so, we're the only owner");
                let inner_owned = Mutex::into_inner(owned).unwrap();
                let req = Arc::try_unwrap(req)
                    .ok()
                    .expect("we replaced the state with ShuttingDown, so, we're the only owner");
                poller_impl_finish_shutdown(inner_owned, req);
            }
        };
        poller.lock().unwrap().state = PollerState::ShutDown;
        tracing::info!("poller finished shutdown");
    })()
}

async fn poller_impl_impl(
    inner: Arc<Mutex<PollerStateInner>>,
    mut preempt_in_epoll: Option<tokio::sync::broadcast::Receiver<mpsc::UnboundedSender<()>>>,
) -> ShutdownRequest {
    let (uring_fd, completion_side, shutdown_rx) = {
        let mut inner_guard = inner.lock().unwrap();
        let PollerStateInner {
            uring_fd,
            completion_side,
            system: _,
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
                _ = async {
                    match &mut preempt_in_epoll {
                        Some(preempt) => {
                            let tell_caller = preempt.resubscribe().recv().await.unwrap();
                            tell_caller.send(()).ok().unwrap();
                            futures::future::pending::<()>().await;
                            unreachable!("we should get dropped at above .await point");
                        },
                        None => {
                            futures::future::pending().await
                        },
                    }
                } => {
                    unreachable!("see above");
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
    let completion_side = Mutex::into_inner(completion_side).unwrap();

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

#[cfg(test)]
mod submit_side_tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use crate::{SharedSystemHandle, System};

    use super::{InflightOpHandle, NotInflightSlotHandle, PollerTesting, SubmitSideProvider};
    struct MockOp {}
    fn submit_mock_op(slot: NotInflightSlotHandle) -> InflightOpHandle<MockOp> {
        impl super::ResourcesOwnedByKernel for MockOp {
            type OpResult = ();
            fn on_op_completion(self, _res: i32) -> Self::OpResult {}
        }
        let submit_fut = slot.submit(MockOp {}, |_| io_uring::opcode::Nop::new().build());
        submit_fut
    }

    // TODO: turn into a does-not-compile test
    // #[tokio::test]
    // async fn get_slot_panics_if_used_after_shutdown() {
    //     let handle = crate::launch_owned().await;
    //     handle.shutdown().await;
    //     // handle.
    //     // .with_submit_side(|submit_side| {
    //     //     let mut guard = submit_side.0.lock().unwrap();
    //     //     let guard = guard.must_open();
    //     //     guard.get_ops_slot()
    //     // })
    //     // .await;
    // }

    #[tokio::test]
    async fn submit_panics_after_shutdown() {
        let system = SharedSystemHandle::launch().await;

        // get a slot
        let slot = system
            .clone()
            .with_submit_side(|submit_side| {
                let mut guard = submit_side.0.lock().unwrap();
                let guard = guard.must_open();
                guard.get_ops_slot()
            })
            .await;

        let (shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel::<()>();
        let jh = tokio::spawn(async move {
            shutdown_started_rx.await.unwrap();
            assert_panic::assert_panic! {
                { let _ = submit_mock_op(slot); },
                &str,
                "cannot use slot for submission, SubmitSide is already plugged"
            };
        });
        let wait_shutdown = system.initiate_shutdown();
        shutdown_started_tx.send(()).unwrap();
        jh.await.unwrap();
        wait_shutdown.await;
    }

    #[tokio::test]
    async fn shutdown_waits_for_ongoing_ops() {
        // tracing_subscriber::fmt::init();

        let system = SharedSystemHandle::launch().await;
        let slot = system
            .clone()
            .with_submit_side(|submit_side| {
                let mut guard = submit_side.0.lock().unwrap();
                let guard = guard.must_open();
                guard.get_ops_slot()
            })
            .await;
        let submit_fut = submit_mock_op(slot);
        let shutdown_done = system.initiate_shutdown();
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
    fn poller_task_dropped_during_shutdown_switches_to_thread() {
        // tracing_subscriber::fmt::init();

        // multi-thread runtime because we need to wait for preempt_done_rx in this thread.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let (poller_switch_to_thread_done_tx, poller_switch_to_thread_done_rx) =
            tokio::sync::oneshot::channel();
        let (shutdown_loop_reached_tx, mut shutdown_loop_reached_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (preempt_outer_tx, preempt_outer_rx) = tokio::sync::oneshot::channel();
        let (_preempt_during_epoll_tx, preempt_during_epoll_rx) = tokio::sync::oneshot::channel();
        let testing = PollerTesting {
            shutdown_loop_reached_tx,
            preempt_outer_rx,
            poller_switch_to_thread_done_tx,
            preempt_during_epoll_rx,
        };
        let (slot, shutdown_done) = rt.block_on(async move {
            let system = System::launch_with_testing(testing).await;
            let slot = system
                .with_submit_side(|submit_side| {
                    let mut guard = submit_side.0.lock().unwrap();
                    let guard = guard.must_open();
                    guard.get_ops_slot()
                })
                .await;
            // When we send shutdown, the slot will keep the poller task in the shutdown process_completions loop.
            // Then we'll use the preempt_tx to cancel the poller task future.
            // We should observe a transition to RunningInThread.
            let shutdown_done_fut = system.initiate_shutdown();
            (slot, shutdown_done_fut)
        });
        let poller = shutdown_loop_reached_rx.blocking_recv().unwrap();
        assert!(
            matches!(
                poller.lock().unwrap().state,
                super::PollerState::ShuttingDownPreemptible(_, _)
            ),
            " state is {:?}",
            poller.lock().unwrap().state
        );
        preempt_outer_tx.send(()).ok().unwrap();
        drop(poller);
        let poller = poller_switch_to_thread_done_rx.blocking_recv().unwrap();
        assert!(
            matches!(
                poller.lock().unwrap().state,
                super::PollerState::ShuttingDownPreemptible(_, _)
            ),
            " state is {:?}",
            poller.lock().unwrap().state
        );
        drop(poller);
        // NB: both above drop(poller) are crucial so that the shutdown loop's Arc::try_unwrap will succeed

        // quick check to ensure the shutdown_done future is not ready yet;
        // we shouldn't signal shutdown_done until end of shutdown, even if we switch to the thread

        let second_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        second_rt.block_on(async move {
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

    #[test]
    fn poller_task_dropped_during_epoll_switches_to_thread() {
        // tracing_subscriber::fmt::init();

        // multi-thread runtime because we need to wait for preempt_done_rx in this thread.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let (poller_switch_to_thread_done_tx, poller_switch_to_thread_done_rx) =
            tokio::sync::oneshot::channel();
        let (shutdown_loop_reached_tx, _shutdown_loop_reached_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (_preempt_outer_tx, preempt_outer_rx) = tokio::sync::oneshot::channel();
        let (preempt_during_epoll_tx, preempt_during_epoll_rx) = tokio::sync::oneshot::channel();
        let testing = PollerTesting {
            shutdown_loop_reached_tx,
            preempt_outer_rx,
            poller_switch_to_thread_done_tx,
            preempt_during_epoll_rx,
        };

        let (read_task_jh, mut writer) = rt.block_on(async move {
            let (reader, writer) = os_pipe::pipe().unwrap();
            let jh = tokio::spawn(async move {
                let system = System::launch_with_testing(testing).await;
                let reader =
                    unsafe { OwnedFd::from_raw_fd(nix::unistd::dup(reader.as_raw_fd()).unwrap()) };
                let buf = vec![0; 1];
                let (reader, buf, res) =
                    crate::read(std::future::ready(&system), reader, 0, buf).await;
                res.unwrap();
                assert_eq!(buf, &[1]);
                // now we know it has reached the epoll loop once
                // next call is the one to be interrupted
                let (_, buf, res) = crate::read(std::future::ready(&system), reader, 0, buf).await;
                res.unwrap();
                assert_eq!(buf, &[2]);
                system
            });
            (jh, writer)
        });
        use std::io::Write;
        // unblock first read
        writer.write_all(&[1]).unwrap();
        preempt_during_epoll_tx.send(()).unwrap();

        let poller = poller_switch_to_thread_done_rx.blocking_recv().unwrap();
        assert!(
            matches!(
                poller.lock().unwrap().state,
                super::PollerState::RunningInThread(_)
            ),
            " state is {:?}",
            poller.lock().unwrap().state
        );
        drop(poller);
        // NB: above drop(poller) is crucial so that the shutdown loop's Arc::try_unwrap will succeed

        assert!(!read_task_jh.is_finished());

        writer.write_all(&[2]).unwrap();

        let second_rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        second_rt.block_on(async move {
            let system = tokio::select! {
                // TODO don't rely on timing
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    panic!("read should complete as soon as we write the second byte")
                }
                system = read_task_jh => { system.unwrap() }
            };

            // ensure system shutdown works if we're in the thread, not task
            system.initiate_shutdown().await;
        });
    }

    #[tokio::test]
    async fn drop_system_handle() {
        let system = System::launch().await;
        drop(system);
    }
}
