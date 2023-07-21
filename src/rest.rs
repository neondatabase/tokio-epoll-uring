use std::{
    future::Future,
    os::fd::{AsRawFd, OwnedFd},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::ready,
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tracing::{info, trace};

enum PreadvCompletionFutState<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    Created {
        file: OwnedFd,
        offset: u64,
    },
    WaitingForOpSlot {
        file: OwnedFd,
        offset: u64,
        wakeup: GetOpsSlotFut,
    },
    Submitted {
        file: OwnedFd,
        inflight_op_handle: InflightOpHandle<B>,
    },
    ReadyPolled,
    Undefined,
    Dropped,
}

impl<B> PreadvCompletionFutState<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    fn discriminant_str(&self) -> &'static str {
        match self {
            PreadvCompletionFutState::WaitingForOpSlot { wakeup, .. } => match wakeup {
                GetOpsSlotFut {
                    state: GetOpsSlotFutState::Undefined,
                } => "waiting_for_op_slot(Undefined)",
                GetOpsSlotFut {
                    state: GetOpsSlotFutState::NotPolled { .. },
                } => "waiting_for_op_slot(NotPolled)",
                GetOpsSlotFut {
                    state: GetOpsSlotFutState::EnqueuedWaiter(_),
                } => "waiting_for_op_slot(EnqueuedWaiter)",
                GetOpsSlotFut {
                    state: GetOpsSlotFutState::ReadyPolled,
                } => "waiting_for_op_slot(ReadyPolled)",
            },
            PreadvCompletionFutState::Created { .. } => "created",
            PreadvCompletionFutState::Submitted { .. } => "submitted",
            PreadvCompletionFutState::ReadyPolled => "ready_polled",
            PreadvCompletionFutState::Undefined => "undefined",
            PreadvCompletionFutState::Dropped => "dropped",
        }
    }
}

pub(crate) struct System {
    split_uring: *mut io_uring::IoUring,
    pub(crate) submit_side: Arc<Mutex<SubmitSide>>,
    rx_completion_queue_from_poller_task:
        std::sync::mpsc::Receiver<Arc<Mutex<SendSyncCompletionQueue>>>,
}

// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Send for System {}
// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Sync for System {}

impl System {
    pub(crate) fn new() -> Self {
        // TODO: this unbounded channel is the root of all evil: unbounded queue for IOPS; should provie app option to back-pressure instead.
        let (waiters_tx, waiters_rx) = tokio::sync::mpsc::unbounded_channel();
        let preallocated_completions = Arc::new(Mutex::new(Ops {
            storage: array_macro::array![_ => None; RING_SIZE as usize],
            unused_indices: (0..RING_SIZE.try_into().unwrap()).collect(),
            waiters_rx,
        }));
        let uring = Box::into_raw(Box::new(io_uring::IoUring::new(RING_SIZE).unwrap()));
        let uring_fd = unsafe { (*uring).as_raw_fd() };
        let (submitter, sq, cq) = unsafe { (&mut *uring).split() };

        let send_sync_completion_queue = Arc::new(Mutex::new(SendSyncCompletionQueue {
            cq,
            seen_poison: false,
            ops: preallocated_completions.clone(),
            submit_side: Weak::new(),
        }));
        let rx_completion_queue_from_poller_task =
            setup_poller_task(uring_fd, Arc::clone(&send_sync_completion_queue));
        let submit_side = Arc::new_cyclic(|myself| {
            Mutex::new(SubmitSide {
                submitter,
                sq,
                cq: send_sync_completion_queue,
                ops: preallocated_completions,
                waiters_tx,
                myself: Weak::clone(myself),
            })
        });
        System {
            split_uring: uring,
            rx_completion_queue_from_poller_task,
            submit_side,
        }
    }

    // TODO: use compile-time tricks to ensure the system is always `shutdown()` and never Dropped.
    pub fn shutdown(self) {
        let System {
            split_uring,
            submit_side,
            rx_completion_queue_from_poller_task,
        } = self;
        trace!("start dropping thread-local state");
        // case (1) current thread is getting stopped but poller is still running; it'll exit once it sees the poison.
        // case (2) poller got stopped, e.g., because the executor is shutting down; we'll wait for the poison.
        //
        // in both cases,

        let mut cq = None;
        loop {
            let entry = io_uring::opcode::Nop::new()
                .build()
                // drain existing ops before scheduling the poison
                .flags(io_uring::squeue::Flags::IO_DRAIN)
                .user_data(u64::MAX);
            match {
                let submit_side = &mut *submit_side.lock().unwrap();
                submit_side.submit_raw(entry)
            } {
                Ok(_) => break,
                Err(SubmitError::QueueFull) => {
                    // poller may be stopping
                    let cq = cq.get_or_insert_with(|| {
                        rx_completion_queue_from_poller_task.recv().unwrap()
                    });
                    match cq.lock().unwrap().process_completions() {
                        Ok(()) => continue, // retry submit poison
                        Err(ProcessCompletionsErr::PoisonPill) => {
                            unreachable!("only we send poison pills")
                        }
                    }
                }
            }
        }
        let cq = cq.unwrap_or_else(|| rx_completion_queue_from_poller_task.recv().unwrap());
        let mut cq_locked = cq.lock().unwrap();
        if !cq_locked.seen_poison {
            // this is case (2)
            loop {
                match cq_locked.process_completions() {
                    Ok(()) => continue,
                    Err(ProcessCompletionsErr::PoisonPill) => break,
                }
            }
        }
        assert!(cq_locked.seen_poison);
        drop(cq_locked);

        // XXX: we still hold the weak reference in `myself` and in the SendSyncCompletionQueue
        let submit_side = Arc::try_unwrap(submit_side).ok().expect(
            "we've shut down the system, so, we're the only ones with a reference to submit_side",
        );
        let submit_side = Mutex::into_inner(submit_side).unwrap();
        let SubmitSide {
            submitter,
            sq,
            ops: preallocated_completions,
            cq: cq_arc_from_submit_side,
            waiters_tx: _,
            myself: _,
        } = submit_side;
        assert!(Arc::ptr_eq(&cq_arc_from_submit_side, &cq)); // ptr_eq is safe because these are not trait objects
        drop(cq_arc_from_submit_side);
        // cq can have at most refcount 3 at a time:
        // - the poller thread
        // - the thread-local storage
        // - the thread that's currently in PreadvCompletion::poll, helping the executor thread with completion processing
        let cq = Arc::try_unwrap(cq)
            .ok()
            .expect("at this point, cq is not referenced by anything else anymore");
        let cq = Mutex::into_inner(cq).unwrap();
        let SendSyncCompletionQueue {
            seen_poison: _,
            cq,
            ops: _,
            submit_side: _,
        } = cq;

        let submitter: Submitter<'_> = submitter;
        let mut sq: SubmissionQueue<'_> = sq;
        let mut cq: CompletionQueue<'_> = cq;
        // We now own all the parts from the IoUring::split() again.
        // Some final assertions, then drop them all, unleak the IoUring, and drop it as well.
        // That cleans up the SQ, CQs, registrations, etc.
        cq.sync();
        assert_eq!(cq.len(), 0, "cqe: {:?}", cq.next());
        sq.sync();
        assert_eq!(sq.len(), 0);
        assert_eq!(
            preallocated_completions
                .try_lock()
                .unwrap()
                .unused_indices
                .len(),
            RING_SIZE.try_into().unwrap()
        );
        drop(cq);
        drop(sq);
        drop(submitter);
        let uring = unsafe { Box::from_raw(split_uring) };
        drop(uring);
    }
}

pub(crate) trait SystemTrait: Unpin + Copy {
    fn with_submit_side<F: FnOnce(&mut SubmitSide) -> R, R>(self, f: F) -> R;
}

pub(crate) struct PreadvCompletionFut<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> {
    system: S,
    state: PreadvCompletionFutState<B>,
    buf: Option<B>, // beocmes None in `drop()`, Some otherwise
    // make it !Send because the `idx` is thread-local
    _types: std::marker::PhantomData<B>,
}

struct OpState(Mutex<OpStateInner>);

enum OpStateInner {
    Undefined,
    Pending {
        waker: Option<std::task::Waker>, // None if it hasn't been polled yet
    },
    PendingButFutureDropped {
        _buffer_owned: Box<dyn std::any::Any + Send>,
    },
    ReadyButFutureDropped,
    Ready {
        result: i32,
    },
}

impl<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> PreadvCompletionFut<S, B> {
    pub fn new(system: S, file: OwnedFd, offset: u64, buf: B) -> Self {
        PreadvCompletionFut {
            system,
            buf: Some(buf),
            _types: std::marker::PhantomData,
            state: PreadvCompletionFutState::Created { file, offset },
        }
    }
}

pub(crate) type PreadvOutput<B> = (OwnedFd, B, std::io::Result<usize>);

impl<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> std::future::Future
    for PreadvCompletionFut<S, B>
{
    type Output = PreadvOutput<B>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        trace!(
            "polling future in state {:?}",
            self.state.discriminant_str()
        );
        loop {
            let cur = std::mem::replace(&mut self.state, PreadvCompletionFutState::Undefined);
            match cur {
                PreadvCompletionFutState::Undefined => {
                    unreachable!("future is in undefined state")
                }
                PreadvCompletionFutState::Dropped => {
                    unreachable!("future is in dropped state, but we're in poll() right now, so, can't be dropped")
                }
                PreadvCompletionFutState::ReadyPolled => {
                    panic!("must not poll future after observing ready")
                }
                PreadvCompletionFutState::Created { file, offset } => {
                    self.state = PreadvCompletionFutState::WaitingForOpSlot {
                        file,
                        offset,
                        wakeup: self
                            .system
                            .with_submit_side(|submit_side| submit_side.get_ops_slot()),
                    };
                    continue;
                }
                PreadvCompletionFutState::WaitingForOpSlot {
                    mut wakeup,
                    file,
                    offset,
                } => {
                    match {
                        let wakeup = std::pin::Pin::new(&mut wakeup);
                        wakeup.poll(cx)
                    } {
                        std::task::Poll::Ready(slot_handle) => {
                            trace!("was woken up from waiting for an available op slot");
                            let mut buf = self.buf.take().unwrap();
                            let iov = libc::iovec {
                                iov_base: buf.stable_mut_ptr() as *mut libc::c_void,
                                iov_len: buf.bytes_total(),
                            };
                            let inflight_op_handle =
                                slot_handle.submit(buf, |buf| {
                                    io_uring::opcode::Readv::new(
                                        io_uring::types::Fd(file.as_raw_fd()),
                                        &iov as *const _,
                                        1,
                                    )
                                    .offset(offset)
                                    .build()
                                });
                            self.state = PreadvCompletionFutState::Submitted {
                                file,
                                inflight_op_handle,
                            };
                            continue; // so we poll for Submitted
                        }
                        std::task::Poll::Pending => {
                            trace!("not woken up to check for an available op slot yet");
                            self.state = PreadvCompletionFutState::WaitingForOpSlot {
                                file,
                                offset,
                                wakeup,
                            };
                            return std::task::Poll::Pending;
                        }
                    }
                }
                PreadvCompletionFutState::Submitted {
                    file,
                    mut inflight_op_handle,
                } => {
                    trace!("checking op state to see if it's ready, storing waker in it if not");
                    match {
                        let pinned = Pin::new(&mut inflight_op_handle);
                        pinned.poll(cx)
                    } {
                        std::task::Poll::Ready((buf, res)) => {
                            self.state = PreadvCompletionFutState::ReadyPolled;
                            return std::task::Poll::Ready((file, buf, res));
                        }
                        std::task::Poll::Pending => {
                            self.state = PreadvCompletionFutState::Submitted {
                                file,
                                inflight_op_handle,
                            };
                            return std::task::Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> Drop for PreadvCompletionFut<S, B> {
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, PreadvCompletionFutState::Dropped);
        match cur {
            PreadvCompletionFutState::Undefined => unreachable!("future is in undefined state"),
            PreadvCompletionFutState::Dropped => {
                unreachable!("future is in dropped state, but we're in drop() right now")
            }
            PreadvCompletionFutState::Created { .. } => (),
            PreadvCompletionFutState::WaitingForOpSlot { .. } => (),
            PreadvCompletionFutState::ReadyPolled => (),
            PreadvCompletionFutState::Submitted {
                inflight_op_handle, ..
            } => {
                drop(inflight_op_handle) // dropping moves ownership of the buffer from inflight_op_handle to the OpState
            }
        }
    }
}

struct SendSyncCompletionQueue {
    seen_poison: bool,
    cq: CompletionQueue<'static>,
    ops: Arc<Mutex<Ops>>,
    submit_side: Weak<Mutex<SubmitSide>>,
}

unsafe impl Send for SendSyncCompletionQueue {}

enum ProcessCompletionsErr {
    PoisonPill,
}

impl SendSyncCompletionQueue {
    fn process_completions(&mut self) -> std::result::Result<(), ProcessCompletionsErr> {
        let cq = &mut self.cq;
        cq.sync();
        for cqe in &mut *cq {
            trace!("got cqe: {:?}", cqe);
            let idx: u64 = unsafe { std::mem::transmute(cqe.user_data()) };
            if idx == u64::MAX {
                self.seen_poison = true;
                continue; // TODO assert it's the last one and break?
            }
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
                OpStateInner::PendingButFutureDropped { _buffer_owned } => {
                    *op_state_inner = OpStateInner::ReadyButFutureDropped;
                    drop(op_state_inner);
                    *op_state = None;
                    drop(op_state);
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
        if self.seen_poison {
            assert_eq!(cq.len(), 0);
            assert!(cq.next().is_none());
            Err(ProcessCompletionsErr::PoisonPill)
        } else {
            Ok(())
        }
    }
}

struct Ops {
    storage: [Option<OpState>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    waiters_rx:
        tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<UnsafeOpsSlotHandle>>,
}

impl Ops {
    fn return_slot_and_wake(&mut self, submit_side: Arc<Mutex<SubmitSide>>, idx: usize) {
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
                            trace!("handed `idx` to a waiter");
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
        trace!("no waiters, returning `idx` to unused_indices");
        self.unused_indices.push(idx);
    }
}

const RING_SIZE: u32 = 128;

pub(crate) struct SubmitSide {
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
    ops: Arc<Mutex<Ops>>,
    // wake_poller_tx: tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<()>>,
    cq: Arc<Mutex<SendSyncCompletionQueue>>,
    waiters_tx:
        tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<UnsafeOpsSlotHandle>>,
    myself: Weak<Mutex<SubmitSide>>,
}

unsafe impl Send for SubmitSide {}

pub(crate) enum SubmitError {
    QueueFull,
}

pub struct GetOpsSlotFut {
    state: GetOpsSlotFutState,
}
enum GetOpsSlotFutState {
    Undefined,
    NotPolled { submit_side: Arc<Mutex<SubmitSide>> },
    EnqueuedWaiter(tokio::sync::oneshot::Receiver<UnsafeOpsSlotHandle>),
    ReadyPolled,
}

pub struct NotInflightSlotHandle {
    state: NotInflightSlotHandleState,
}

struct UnsafeOpsSlotHandle {
    submit_side: Arc<Mutex<SubmitSide>>,
    idx: usize,
}

enum NotInflightSlotHandleState {
    Usable { slot: UnsafeOpsSlotHandle },
    Used,
    Dropped,
}

impl SubmitSide {
    pub fn get_ops_slot(&mut self) -> GetOpsSlotFut {
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
        let cur = std::mem::replace(&mut self.state, GetOpsSlotFutState::Undefined);
        match cur {
            GetOpsSlotFutState::Undefined => unreachable!("implementation error"),
            GetOpsSlotFutState::NotPolled { submit_side } => {
                let submit_side_guard = submit_side.lock().unwrap();
                let mut ops_guard = submit_side_guard.ops.lock().unwrap();
                match ops_guard.unused_indices.pop() {
                    Some(idx) => {
                        drop(ops_guard);
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
                            submit_side_guard.submitter.submit().unwrap();
                            match submit_side_guard.cq.lock().unwrap().process_completions() {
                                Ok(()) => (),
                                Err(ProcessCompletionsErr::PoisonPill) => {
                                    unreachable!("the thread-local destructor is the only one that sends them, and we're currently using that thread-local, so, it can't have been sent")
                                }
                            }
                        }
                        let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                        match submit_side_guard.waiters_tx.send(wake_up_tx) {
                            Ok(()) => (),
                            Err(tokio::sync::mpsc::error::SendError(_)) => {
                                todo!("can this happen? poller would be dead")
                            }
                        }
                        self.state = GetOpsSlotFutState::EnqueuedWaiter(wake_up_rx);
                        return std::task::Poll::Pending;
                    }
                }
            }
            GetOpsSlotFutState::EnqueuedWaiter(waiter) => {
                tokio::pin!(waiter);
                match ready!(waiter.poll(cx)) {
                    Ok(slot_handle) => {
                        self.state = GetOpsSlotFutState::ReadyPolled;
                        std::task::Poll::Ready(NotInflightSlotHandle { state: NotInflightSlotHandleState::Usable { slot: slot_handle }})
                    }
                    Err(_waiter_dropped) => unreachable!("system dropped before all GetOpsSlotFut were dropped; type system should prevent this"),
                }
            }
            GetOpsSlotFutState::ReadyPolled => {
                panic!("must not poll future after observing ready")
            }
        }
    }
}

struct InflightOpHandle<B: tokio_uring::buf::IoBufMut + Send> {
    buf: Option<B>, // beocmes None in `drop()`, Some otherwise
    state: InflightOpHandleState,
}

enum InflightOpHandleState {
    Undefined,
    Submitted {
        slot: UnsafeOpsSlotHandle,
        poll_count: usize,
    },
    ReadyButYieldingToExecutorForFairness {
        result: std::io::Result<usize>,
    },
    ReadyPolled,
    Dropped,
}

impl NotInflightSlotHandle {
    fn submit<B, MakeSqe>(mut self, buf: B, make_sqe: MakeSqe) -> InflightOpHandle<B>
    where
        B: tokio_uring::buf::IoBufMut + Send + 'static,
        MakeSqe: FnOnce(&mut B) -> io_uring::squeue::Entry,
    {
        let cur = std::mem::replace(&mut self.state, NotInflightSlotHandleState::Used);
        match cur {
            NotInflightSlotHandleState::Usable { slot } => slot.submit(buf, make_sqe),
            NotInflightSlotHandleState::Used => unreachable!("implementation error"),
            NotInflightSlotHandleState::Dropped => unreachable!("implementation error"),
        }
    }
}

impl UnsafeOpsSlotHandle {
    fn submit<B, MakeSqe>(self, mut buf: B, make_sqe: MakeSqe) -> InflightOpHandle<B>
    where
        B: tokio_uring::buf::IoBufMut + Send + 'static,
        MakeSqe: FnOnce(&mut B) -> io_uring::squeue::Entry,
    {
        let sqe = make_sqe(&mut buf);
        let sqe = sqe.user_data(u64::try_from(self.idx).unwrap());

        let mut submit_side_guard = self.submit_side.lock().unwrap();

        let mut ops_guard = submit_side_guard.ops.lock().unwrap();
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
                let cq = Arc::clone(&submit_side_guard.cq);
                cq_owned = Some(cq);
                Some(cq_owned.as_ref().expect("we just set it").lock().unwrap())
            } else {
                None
            };
            assert_eq!(cq_owned.is_none(), cq_guard.is_none());

            match submit_side_guard.submit_raw(sqe) {
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
                match cq.process_completions() {
                    Ok(()) => {}
                    Err(ProcessCompletionsErr::PoisonPill) => {
                        unreachable!("the thread-local destructor is the only one that sends them, and we're currently using that thread-local, so, it can't have been sent");
                    }
                }
            } else {
                assert!(!*PROCESS_URING_ON_SUBMIT);
            }
        }
        drop(submit_side_guard);
        InflightOpHandle {
            buf: Some(buf),
            state: InflightOpHandleState::Submitted {
                slot: self,
                poll_count: 0,
            },
        }
    }
}

impl UnsafeOpsSlotHandle {
    fn return_slot_and_wake(self) {
        let UnsafeOpsSlotHandle { submit_side, idx } = self;
        let submit_side_guard = submit_side.lock().unwrap();
        let mut ops_guard = submit_side_guard.ops.lock().unwrap();
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

impl SubmitSide {
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
        let submit_side = self.submit_side.lock().unwrap();
        let mut ops_guard = submit_side.ops.lock().unwrap();
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
                drop(submit_side);
                return OwnedSlotPollResult::Pending(self);
            }
            OpStateInner::PendingButFutureDropped { .. } => {
                unreachable!("if it's dropped, it's not pollable")
            }
            OpStateInner::ReadyButFutureDropped => {
                unreachable!("if it's dropped, it's not pollable")
            }
            OpStateInner::Ready { result: res } => {
                trace!("op is ready, returning file and buffer to user");
                drop(op_state_inner);
                drop(ops_guard);
                drop(submit_side);
                self.return_slot_and_wake();
                return OwnedSlotPollResult::Ready(res);
            }
        }
    }
}

impl<B: tokio_uring::buf::IoBufMut + Send> Future for InflightOpHandle<B> {
    type Output = (B, std::io::Result<usize>);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let cur = std::mem::replace(&mut self.state, InflightOpHandleState::Undefined);
        match cur {
            InflightOpHandleState::Undefined => unreachable!("implementation error"),
            InflightOpHandleState::ReadyPolled => {
                panic!("must not poll future after observing ready")
            }
            InflightOpHandleState::Dropped => unreachable!("implementation error"),
            InflightOpHandleState::Submitted { slot, poll_count } => {
                let res = match slot.poll(cx) {
                    OwnedSlotPollResult::Ready(res) => res,
                    OwnedSlotPollResult::Pending(slot) => {
                        self.state = InflightOpHandleState::Submitted {
                            slot,
                            poll_count: poll_count + 1,
                        };
                        return std::task::Poll::Pending;
                    }
                };

                let mut buf_mut =
                    unsafe { self.as_mut().map_unchecked_mut(|myself| &mut myself.buf) };
                let mut buf = buf_mut.take().expect("we only take() it in drop(), and evidently drop() hasn't happened yet because we're executing a method on self");
                drop(buf_mut);
                // https://man.archlinux.org/man/io_uring_prep_read.3.en
                let res = if res < 0 {
                    Err(std::io::Error::from_raw_os_error(-res))
                } else {
                    unsafe { buf.set_init(res as usize) };
                    Ok(res as usize)
                };

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
                                InflightOpHandleState::ReadyButYieldingToExecutorForFairness {
                                    result: res,
                                };
                            let replaced = self.buf.replace(buf);
                            assert!(replaced.is_none(), "we just took it above");
                            return std::task::Poll::Pending;
                        }
                        std::task::Poll::Ready(()) => {
                            // fallthrough
                        }
                    }
                }
                self.state = InflightOpHandleState::ReadyPolled;
                return std::task::Poll::Ready((buf, res));
            }
            InflightOpHandleState::ReadyButYieldingToExecutorForFairness { result } => {
                self.state = InflightOpHandleState::ReadyPolled;
                return std::task::Poll::Ready((self.buf.take().unwrap(), result));
            }
        }
    }
}

impl<B> Drop for InflightOpHandle<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, InflightOpHandleState::Dropped);
        match cur {
            InflightOpHandleState::Undefined => unreachable!("future is in undefined state"),
            InflightOpHandleState::Dropped => {
                unreachable!("future is in dropped state, but we're in drop() right now")
            }
            InflightOpHandleState::ReadyPolled => (),
            InflightOpHandleState::ReadyButYieldingToExecutorForFairness { .. } => (),
            InflightOpHandleState::Submitted {
                slot,
                poll_count: _,
            } => {
                // buffer must be kept alive until the operation is complete, even if we lose interest
                let buf = self
                    .buf
                    .take()
                    .expect("we only take() during drop, which is here");
                slot.move_buf_and_slot_ownership_to_system(buf);
            }
        }
    }
}

impl UnsafeOpsSlotHandle {
    fn move_buf_and_slot_ownership_to_system<B>(self, buf: B)
    where
        B: tokio_uring::buf::IoBufMut + Send + 'static,
    {
        let _buffer_owned: Box<dyn std::any::Any + Send> = Box::new(buf);
        let submit_side = self.submit_side.lock().unwrap();
        let mut ops_guard = submit_side.ops.lock().unwrap();
        let op_state = &mut ops_guard.storage[self.idx];
        let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();
        let cur = std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
        match cur {
            OpStateInner::Undefined => unreachable!("implementation error"),
            OpStateInner::Pending { .. } => {
                *op_state_inner = OpStateInner::PendingButFutureDropped { _buffer_owned };
                drop(op_state_inner);
            }
            OpStateInner::PendingButFutureDropped { .. } => {
                unreachable!("implementation error")
            }
            OpStateInner::Ready { .. } => {
                unreachable!("implementation error")
            }
            OpStateInner::ReadyButFutureDropped => {
                unreachable!("implementation error")
            }
        }
    }
}

fn setup_poller_task(
    uring_fd: std::os::fd::RawFd,
    cq: Arc<Mutex<SendSyncCompletionQueue>>,
) -> std::sync::mpsc::Receiver<Arc<Mutex<SendSyncCompletionQueue>>> {
    let (giveback_cq_tx, giveback_cq_rx) = std::sync::mpsc::sync_channel(1);

    let fut = async move {
        scopeguard::defer!({
            info!("poller task is exiting");
        });
        let give_back_cq_on_drop = |cq| {
            scopeguard::guard(cq, |cq| {
                giveback_cq_tx.send(cq).unwrap();
            })
        };
        let mut cq = Some(give_back_cq_on_drop(cq));

        info!(
            "launching poller task on thread id {:?}",
            std::thread::current().id()
        );

        let fd = tokio::io::unix::AsyncFd::new(uring_fd).unwrap();
        loop {
            // See fd.read() API docs for recipe for this code block.
            loop {
                let mut guard = fd.ready(tokio::io::Interest::READABLE).await.unwrap();
                if !guard.ready().is_readable() {
                    trace!("spurious wakeup");
                    continue;
                }
                guard.clear_ready_matching(tokio::io::Ready::READABLE);
                break;
            }

            let unprotected_cq_arc = scopeguard::ScopeGuard::into_inner(cq.take().unwrap());
            let mut unprotected_cq = unprotected_cq_arc.lock().unwrap();
            let res = unprotected_cq.process_completions(); // todo: catch_unwind?
            drop(unprotected_cq);
            cq = Some(give_back_cq_on_drop(unprotected_cq_arc));
            match res {
                Ok(()) => {}
                Err(ProcessCompletionsErr::PoisonPill) => {
                    info!("poller observed poison pill");
                    break;
                }
            }
        }
    };

    lazy_static::lazy_static! {
        static ref POLLER_TASK_UNCONSTRAINED: bool = std::env::var("POLLER_TASK_UNCONSTRAINED")
            .map(|v| v == "1")
            .unwrap_or_else(|e| match e {
                std::env::VarError::NotPresent => false,
                std::env::VarError::NotUnicode(_) => panic!("POLLER_TASK_UNCONSTRAINED must be a unicode string"),
            });
    }
    if *POLLER_TASK_UNCONSTRAINED {
        tokio::task::spawn(tokio::task::unconstrained(fut));
    } else {
        tokio::task::spawn(fut);
    }

    giveback_cq_rx
}
