use std::{
    os::fd::{AsRawFd, OwnedFd},
    sync::{Arc, Mutex},
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tracing::{info, trace};

enum ThreadLocalStateInner {
    NotUsed,
    Used {
        split_uring: *mut io_uring::IoUring,
        submit_side: SubmitSide,
        rx_completion_queue_from_poller_task:
            std::sync::mpsc::Receiver<Arc<Mutex<SendSyncCompletionQueue>>>,
    },
    Dropped,
}
struct ThreadLocalState(ThreadLocalStateInner);

thread_local! {
    static THREAD_LOCAL: std::cell::RefCell<ThreadLocalState> = std::cell::RefCell::new(ThreadLocalState(ThreadLocalStateInner::NotUsed));
}

fn with_this_executor_threads_submit_side<F: FnOnce(&mut SubmitSide) -> R, R>(f: F) -> R {
    THREAD_LOCAL.with(|local_state| {
        let mut local_state = local_state.borrow_mut();
        loop {
            match &mut local_state.0 {
                ThreadLocalStateInner::NotUsed => {
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
                    let cq = Arc::new(Mutex::new(SendSyncCompletionQueue {
                        cq,
                        seen_poison: false,
                        ops: preallocated_completions.clone(),
                    }));
                    let rx_completion_queue_from_poller_task =
                        setup_poller_task(uring_fd, Arc::clone(&cq));
                    let submit_side = SubmitSide {
                        submitter,
                        sq,
                        cq,
                        ops: preallocated_completions,
                        waiters_tx,
                    };
                    *local_state = ThreadLocalState(ThreadLocalStateInner::Used {
                        split_uring: uring,
                        rx_completion_queue_from_poller_task,
                        submit_side,
                    });
                    continue;
                }
                // fast path
                ThreadLocalStateInner::Used { submit_side, .. } => break f(submit_side),
                ThreadLocalStateInner::Dropped => {
                    unreachable!("threat-local can't be dropped while executing")
                }
            }
        }
    })
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        let cur: ThreadLocalStateInner =
            std::mem::replace(&mut self.0, ThreadLocalStateInner::Dropped);
        match cur {
            ThreadLocalStateInner::NotUsed => {}
            ThreadLocalStateInner::Used {
                split_uring,
                mut submit_side,
                rx_completion_queue_from_poller_task,
            } => {
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
                    match submit_side.submit(entry) {
                        Ok(_preallocated_submission_idx) => break,
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
                let SubmitSide {
                    submitter,
                    sq,
                    ops: preallocated_completions,
                    cq: cq_arc_from_submit_side,
                    waiters_tx: _,
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
            ThreadLocalStateInner::Dropped => {
                unreachable!("ThreadLocalState::drop() had already been called in the past");
            }
        }
    }
}

type PreadvOutput<B> = (OwnedFd, B, std::io::Result<usize>);

pub fn preadv<B: tokio_uring::buf::IoBufMut + Send>(
    file: OwnedFd,
    offset: u64,
    buf: B,
) -> impl std::future::Future<Output = PreadvOutput<B>> + Send {
    let fut = PreadvCompletionFut {
        buf: Some(buf),
        _types: std::marker::PhantomData,
        state: PreadvCompletionFutState::WaitingForOpSlot {
            file,
            offset,
            wakeup: None,
        },
    };
    fut
}

enum PreadvCompletionFutState {
    WaitingForOpSlot {
        file: OwnedFd,
        offset: u64,
        wakeup: Option<tokio::sync::oneshot::Receiver<()>>,
    },
    Submitted {
        ops: Arc<Mutex<Ops>>,
        idx: usize, // into the preallocated_completions
    },
    ReadyPolled,
    Undefined,
    Dropped,
}
struct PreadvCompletionFut<B: tokio_uring::buf::IoBufMut + Send> {
    state: PreadvCompletionFutState,
    buf: Option<B>, // beocmes None in `drop()`, Some otherwise
    // make it !Send because the `idx` is thread-local
    _types: std::marker::PhantomData<B>,
}

struct OpState(Mutex<OpStateInner>);

enum OpStateInner {
    Undefined,
    Pending {
        file_currently_owned_by_kernel: OwnedFd,
        waker: Option<std::task::Waker>, // None if it hasn't been polled yet
    },
    PendingButFutureDropped {
        _buffer_owned: Box<dyn std::any::Any + Send>,
    },
    ReadyButFutureDropped,
    Ready {
        file_ready_to_be_taken_back: OwnedFd,
        result: i32,
    },
}

impl<B: tokio_uring::buf::IoBufMut + Send> PreadvCompletionFut<B> {
    fn assert_not_undefined(&self) {
        assert!(!matches!(self.state, PreadvCompletionFutState::Undefined));
    }
    fn ty_get_ops_slot(&mut self) -> bool {
        let cur = std::mem::replace(&mut self.state, PreadvCompletionFutState::Undefined);
        let PreadvCompletionFutState::WaitingForOpSlot { file, offset , wakeup } = cur  else {
            panic!("implementation error");
        };
        assert!(
            wakeup.is_none(),
            "if wakeup is Some, we should be polling this future"
        );
        with_this_executor_threads_submit_side(move |submit_side| {
            let (mut ops_guard, idx) = loop {
                let mut ops_guard = submit_side.ops.lock().unwrap();
                match ops_guard.unused_indices.pop() {
                    Some(idx) => break (ops_guard, idx),
                    None => {
                        // all slots are taken.
                        // do some opportunistic completion processing to wake up futures that will release ops slots
                        // then yield to executor
                        drop(ops_guard); // so that  process_completions() can take it

                        submit_side.submitter.submit().unwrap();
                        match submit_side.cq.lock().unwrap().process_completions() {
                            Ok(()) => (),
                            Err(ProcessCompletionsErr::PoisonPill) => {
                                unreachable!("the thread-local destructor is the only one that sends them, and we're currently using that thread-local, so, it can't have been sent")
                            }
                        }

                        let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                        match submit_side.waiters_tx.send(wake_up_tx) {
                            Ok(()) => (),
                            Err(tokio::sync::mpsc::error::SendError(_)) => {
                                todo!("can this happen? poller would be dead")
                            }
                        }
                        self.state = PreadvCompletionFutState::WaitingForOpSlot {
                            file,
                            offset,
                            wakeup: Some(wake_up_rx),
                        };
                        return false;
                    }
                }
            };
            let buf = self.buf.as_mut().unwrap();

            let iov = libc::iovec {
                iov_base: buf.stable_mut_ptr() as *mut libc::c_void,
                iov_len: buf.bytes_total(),
            };

            let sqe = io_uring::opcode::Readv::new(
                io_uring::types::Fd(file.as_raw_fd()),
                &iov as *const _,
                1,
            )
            .offset(offset)
            .build()
            .user_data(idx.try_into().unwrap());

            ops_guard.storage[idx] = Some(OpState(Mutex::new(OpStateInner::Pending {
                waker: None,
                file_currently_owned_by_kernel: file,
            })));

            drop(ops_guard); // to remove mut borrow on submit_side, needed for .submit()
            match submit_side.submit(sqe) {
                Ok(()) => {}
                Err(SubmitError::QueueFull) => {
                    unreachable!("the preallocated_completions has same size as the SQ")
                }
            }
            // opportunistically process completion immediately
            // TODO do it during poll as well?
            // match submit_side.cq.lock().unwrap().process_completions() {
            //     Ok(()) => {}
            //     Err(ProcessCompletionsErr::PoisonPill) => {
            //         unreachable!("the thread-local destructor is the only one that sends them, and we're currently using that thread-local, so, it can't have been sent");
            //     }
            // }

            self.state = PreadvCompletionFutState::Submitted {
                ops: Arc::clone(&submit_side.ops),
                idx,
            };
            return true;
        })
    }
}

impl<B: tokio_uring::buf::IoBufMut + Send> std::future::Future for PreadvCompletionFut<B> {
    type Output = PreadvOutput<B>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
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
                PreadvCompletionFutState::WaitingForOpSlot {
                    mut wakeup,
                    file,
                    offset,
                } => match wakeup.take() {
                    Some(mut wakeup) => {
                        match {
                            let wakeup = std::pin::Pin::new(&mut wakeup);
                            wakeup.poll(cx)
                        } {
                            std::task::Poll::Ready(res) => {
                                match res {
                                    Ok(()) => {}
                                    Err(_sender_dropped) => {
                                        // the waiters_rx got dropped, or, our wakeup request got dropped
                                        todo!("can the poller task die before a future?")
                                    }
                                }
                                self.state = PreadvCompletionFutState::WaitingForOpSlot {
                                    file,
                                    offset,
                                    wakeup: None,
                                };
                                if self.ty_get_ops_slot() {
                                    assert!(matches!(
                                        self.state,
                                        PreadvCompletionFutState::Submitted { .. }
                                    ));
                                    continue; // So we opportunistically poll for Submitted
                                } else {
                                    assert!(matches!(
                                        self.state,
                                        PreadvCompletionFutState::WaitingForOpSlot {
                                            wakeup: Some(_),
                                            ..
                                        }
                                    ));
                                    continue; // So we poll the wakeup, and get the right waker if it's not done by the time we poll
                                }
                            }
                            std::task::Poll::Pending => {
                                self.state = PreadvCompletionFutState::WaitingForOpSlot {
                                    file,
                                    offset,
                                    wakeup: Some(wakeup),
                                };
                                return std::task::Poll::Pending;
                            }
                        }
                    }
                    None => {
                        self.state = PreadvCompletionFutState::WaitingForOpSlot {
                            file,
                            offset,
                            wakeup: None,
                        };
                        if self.ty_get_ops_slot() {
                            assert!(matches!(
                                self.state,
                                PreadvCompletionFutState::Submitted { .. }
                            ));
                            continue; // So we opportunistically poll for Submitted
                        } else {
                            assert!(matches!(
                                self.state,
                                PreadvCompletionFutState::WaitingForOpSlot {
                                    wakeup: Some(_),
                                    ..
                                }
                            ));
                            continue; // So we poll the wakeup, and get the right waker if it's not done by the time we poll
                        }
                    }
                },
                PreadvCompletionFutState::Submitted { ops, idx } => {
                    let mut ops_guard = ops.lock().unwrap();
                    let op_state = &mut ops_guard.storage[idx];
                    let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();

                    let cur: OpStateInner =
                        std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
                    match cur {
                        OpStateInner::Undefined => panic!("future is in undefined state"),
                        OpStateInner::Pending {
                            file_currently_owned_by_kernel,
                            waker: _, // don't recycle wakers, it may be from a different Context than the current `cx`
                        } => {
                            *op_state_inner = OpStateInner::Pending {
                                file_currently_owned_by_kernel,
                                waker: Some(cx.waker().clone()),
                            };
                            drop(op_state_inner);
                            drop(op_state);
                            drop(ops_guard);
                            self.state = PreadvCompletionFutState::Submitted { ops, idx };
                            return std::task::Poll::Pending;
                        }
                        OpStateInner::PendingButFutureDropped { .. } => {
                            unreachable!("if it's dropped, it's not pollable")
                        }
                        OpStateInner::ReadyButFutureDropped => {
                            unreachable!("if it's dropped, it's not pollable")
                        }
                        OpStateInner::Ready {
                            result: res,
                            file_ready_to_be_taken_back,
                        } => {
                            drop(op_state_inner);
                            *op_state = None;
                            ops_guard.return_slot_and_wake(idx);
                            drop(ops_guard);
                            let mut buf_mut = unsafe {
                                self.as_mut().map_unchecked_mut(|myself| &mut myself.buf)
                            };
                            let mut buf = buf_mut.take().expect("we only take() it in drop(), and evidently drop() hasn't happened yet because we're executing a method on self");
                            drop(buf_mut);
                            // https://man.archlinux.org/man/io_uring_prep_read.3.en
                            let res = if res < 0 {
                                Err(std::io::Error::from_raw_os_error(-res))
                            } else {
                                unsafe { buf.set_init(res as usize) };
                                Ok(res as usize)
                            };
                            self.state = PreadvCompletionFutState::ReadyPolled;
                            return std::task::Poll::Ready((file_ready_to_be_taken_back, buf, res));
                        }
                    }
                }
            }
        }
    }
}

impl<B: tokio_uring::buf::IoBufMut + Send> Drop for PreadvCompletionFut<B> {
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, PreadvCompletionFutState::Dropped);
        match cur {
            PreadvCompletionFutState::Undefined => unreachable!("future is in undefined state"),
            PreadvCompletionFutState::Dropped => {
                unreachable!("future is in dropped state, but we're in drop() right now")
            }
            PreadvCompletionFutState::WaitingForOpSlot { .. } => (),
            PreadvCompletionFutState::ReadyPolled => (),
            PreadvCompletionFutState::Submitted { ops, idx } => {
                let mut ops_guard = ops.lock().unwrap();
                let op_state = &mut ops_guard.storage[idx];
                let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();

                let cur = std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
                match cur {
                    OpStateInner::Undefined => {
                        unreachable!("operation is in undefined state")
                    }
                    OpStateInner::Ready { .. } => {
                        drop(op_state_inner);
                        *op_state = None;
                        ops_guard.return_slot_and_wake(idx);
                    }
                    OpStateInner::Pending { .. } => {
                        // keep the buffer alive by storing it in `ops`
                        let buf = self
                            .buf
                            .take()
                            .expect("we only take() during drop, which is here");
                        // we can't have B as a type parameter in the preallocated_completions, drop it
                        let _buffer_owned: Box<dyn std::any::Any + Send> = Box::new(buf);
                        *op_state_inner = OpStateInner::PendingButFutureDropped { _buffer_owned };
                    }
                    OpStateInner::PendingButFutureDropped { .. } => {
                        unreachable!("future is being dropped right now")
                    }
                    OpStateInner::ReadyButFutureDropped => {
                        unreachable!("we are only dropping now, completion handler can't ahve observed us in PendingButFutureDropped state")
                    }
                }
            }
        }
    }
}

struct SendSyncCompletionQueue {
    seen_poison: bool,
    cq: CompletionQueue<'static>,
    ops: Arc<Mutex<Ops>>,
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
                OpStateInner::Pending {
                    file_currently_owned_by_kernel,
                    waker,
                } => {
                    *op_state_inner = OpStateInner::Ready {
                        result: cqe.result(),
                        file_ready_to_be_taken_back: file_currently_owned_by_kernel,
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
                    ops_guard.return_slot_and_wake(idx as usize);
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
    waiters_rx: tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
}

impl Ops {
    fn return_slot_and_wake(&mut self, idx: usize) {
        assert!(self.storage[idx].is_none());
        self.unused_indices.push(idx);
        loop {
            match self.waiters_rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    break;
                }
                Ok(waiter) => {
                    match waiter.send(()) {
                        Ok(()) => {
                            break;
                        }
                        Err(()) => {
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
    }
}

const RING_SIZE: u32 = 128;

struct SubmitSide {
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
    ops: Arc<Mutex<Ops>>,
    // wake_poller_tx: tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<()>>,
    cq: Arc<Mutex<SendSyncCompletionQueue>>,
    waiters_tx: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
}

unsafe impl Send for SubmitSide {}

enum SubmitError {
    QueueFull,
}

impl SubmitSide {
    fn submit(&mut self, sqe: io_uring::squeue::Entry) -> std::result::Result<(), SubmitError> {
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

fn setup_poller_task(
    uring_fd: std::os::fd::RawFd,
    cq: Arc<Mutex<SendSyncCompletionQueue>>,
) -> std::sync::mpsc::Receiver<Arc<Mutex<SendSyncCompletionQueue>>> {
    let (giveback_cq_tx, giveback_cq_rx) = std::sync::mpsc::sync_channel(1);

    tokio::task::spawn(async move {
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
                    info!("spurious wakeup");
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
    });

    giveback_cq_rx
}
