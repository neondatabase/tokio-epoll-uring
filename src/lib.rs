use std::{
    os::fd::AsRawFd,
    sync::{Arc, Mutex},
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tracing::{info, instrument, trace};

enum ThreadLocalStateInner {
    NotUsed,
    Used {
        submit_side: SubmitSide,
        rx_completion_queue_from_poller_task: std::sync::mpsc::Receiver<SendSyncCompletionQueue>,
    },
    ShuttingDown,
}
struct ThreadLocalState(ThreadLocalStateInner);

thread_local! {
    static THREAD_LOCAL: std::cell::RefCell<Arc<Mutex<ThreadLocalState>>> = std::cell::RefCell::new(Arc::new(Mutex::new(ThreadLocalState(ThreadLocalStateInner::NotUsed))));
}

fn get_this_executor_threads_submit_side() -> std::io::Result<SubmitSide> {
    THREAD_LOCAL.with(|local_state| {
        let local_state = local_state.borrow_mut();
        let mut local_state = local_state.lock().unwrap();
        Ok(match &local_state.0 {
            ThreadLocalStateInner::NotUsed => {
                let uring = Box::leak(Box::new(io_uring::IoUring::new(128).unwrap()));
                let (mut submitter, sq, cq) = uring.split();
                let rx = setup_poller_task(
                    &mut submitter,
                    SendSyncCompletionQueue {
                        cq,
                        seen_poison: false,
                    },
                );
                let submit_side = SubmitSide {
                    inner: Arc::new(Mutex::new(SubmitSideInner { submitter, sq })),
                };
                *local_state = ThreadLocalState(ThreadLocalStateInner::Used {
                    rx_completion_queue_from_poller_task: rx,
                    submit_side: submit_side.clone(),
                });
                submit_side
            }
            ThreadLocalStateInner::Used { submit_side, .. } => submit_side.clone(),
            ThreadLocalStateInner::ShuttingDown => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "thread local uring is shutting down",
                ));
            }
        })
    })
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        let cur: ThreadLocalStateInner =
            std::mem::replace(&mut self.0, ThreadLocalStateInner::ShuttingDown);
        match cur {
            ThreadLocalStateInner::NotUsed => {}
            ThreadLocalStateInner::Used {
                submit_side,
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
                        .user_data(0x80_00_00_00_00_00_00_00);
                    match submit_side.submit(&entry) {
                        Ok(()) => break,
                        Err(SubmitError::QueueFull) => {
                            // poller may be stopping
                            let cq = cq.get_or_insert_with(|| {
                                rx_completion_queue_from_poller_task.recv().unwrap()
                            });
                            match cq.process_completions() {
                                Ok(()) => continue, // retry submit poison
                                Err(ProcessCompletionsErr::PoisonPill) => {
                                    unreachable!("only we send poison pills")
                                }
                            }
                        }
                    }
                }
                let cq =
                    cq.get_or_insert_with(|| rx_completion_queue_from_poller_task.recv().unwrap());

                if !cq.seen_poison {
                    // this is case (2)
                    loop {
                        match cq.process_completions() {
                            Ok(()) => continue,
                            Err(ProcessCompletionsErr::PoisonPill) => break,
                        }
                    }
                }

                let inner = Arc::try_unwrap(submit_side.inner)
                    .ok()
                    .expect("thread locals should never be shared");
                let SubmitSideInner { submitter, mut sq } = Mutex::into_inner(inner).ok().unwrap();
                // the band of submitter, sq, and cq is back together; some final assertions, then drop them all.
                // the last one to drop will close the io_ring fd.
                cq.cq.sync();
                assert_eq!(cq.cq.len(), 0, "cqe: {:?}", cq.cq.next());
                sq.sync();
                assert_eq!(sq.len(), 0);
                drop(cq);
                drop(sq);
                drop(submitter);
            }
            ThreadLocalStateInner::ShuttingDown => {
                panic!("ThreadLocalState::drop() called twice");
            }
        }
    }
}

type PreadvOutput<F> = (F, Vec<u8>, std::io::Result<usize>);

pub fn preadv<F: AsRawFd + 'static>(
    file: F,
    offset: u64,
    buf: Vec<u8>,
) -> impl std::future::Future<Output = PreadvOutput<F>> {
    PreadvCompletionFut {
        state: Arc::new(PreadvCompletionFutState(Mutex::new(
            PreadvCompletionFutStateInner::TrySubmission { file, offset, buf },
        ))),
    }
}

enum DoneSubState<F: AsRawFd> {
    Ready(PreadvOutput<F>),
    Taken,
}

struct PreadvCompletionFutState<T: AsRawFd>(Mutex<PreadvCompletionFutStateInner<T>>);
enum PreadvCompletionFutStateInner<F: AsRawFd> {
    Undefined,
    TrySubmission {
        file: F,
        offset: u64,
        buf: Vec<u8>,
    },
    Pending {
        file_currently_owned_by_kernel: F,
        buf_currently_owned_by_kernel: Vec<u8>,
        waker: std::task::Waker,
    },
    Done(DoneSubState<F>),
}

struct PreadvCompletionFut<F: AsRawFd> {
    state: Arc<PreadvCompletionFutState<F>>,
}

impl<F: AsRawFd + 'static> std::future::Future for PreadvCompletionFut<F> {
    type Output = (F, Vec<u8>, std::io::Result<usize>);

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut state = self.state.0.lock().unwrap();
        let cur = std::mem::replace(&mut *state, PreadvCompletionFutStateInner::Undefined);
        match cur {
            PreadvCompletionFutStateInner::Undefined => panic!("future is in undefined state"),
            PreadvCompletionFutStateInner::TrySubmission { file, offset, buf } => {
                let submit_side = match get_this_executor_threads_submit_side() {
                    Ok(local) => local,
                    Err(e) => {
                        *state = PreadvCompletionFutStateInner::Done(DoneSubState::Taken);
                        return std::task::Poll::Ready((file, buf, Err(e)));
                    }
                };

                let completion = Box::new(Completion {
                    op_future_state: Arc::clone(&self.state) as Arc<dyn OpFuture>,
                });
                let user_data = Box::into_raw(completion) as u64;
                assert!(
                    user_data & 0x80_00_00_00_00_00_00_00 == 0,
                    "highest bit is reserved for posion pill"
                );

                let iov = libc::iovec {
                    iov_base: buf.as_ptr() as *mut libc::c_void,
                    iov_len: buf.len(),
                };

                let sqe = io_uring::opcode::Readv::new(
                    io_uring::types::Fd(file.as_raw_fd()),
                    &iov as *const _,
                    1,
                )
                .offset(offset)
                .build()
                .user_data(user_data);

                match submit_side.submit(&sqe) {
                    Ok(()) => (),
                    Err(e) => {
                        *state = PreadvCompletionFutStateInner::Done(DoneSubState::Taken);
                        let err = match e {
                            SubmitError::QueueFull => {
                                todo!()
                            }
                        };
                        return std::task::Poll::Ready((file, buf, Err(err)));
                    }
                }

                *state = PreadvCompletionFutStateInner::Pending {
                    waker: cx.waker().clone(),
                    buf_currently_owned_by_kernel: buf,
                    file_currently_owned_by_kernel: file,
                };
                drop(state);

                std::task::Poll::Pending
            }
            x @ PreadvCompletionFutStateInner::Pending { .. } => {
                *state = x;
                std::task::Poll::Pending
            }
            PreadvCompletionFutStateInner::Done(DoneSubState::Ready(res)) => {
                *state = PreadvCompletionFutStateInner::Done(DoneSubState::Taken);
                std::task::Poll::Ready(res)
            }
            PreadvCompletionFutStateInner::Done(DoneSubState::Taken) => {
                panic!("polled after completion")
            }
        }
    }
}

trait OpFuture {
    fn on_completion(&self, res: i32);
}

struct Completion {
    op_future_state: Arc<dyn OpFuture>,
}

fn process_completion(user_data: u64, res: i32) {
    let completion: Box<Completion> = unsafe { Box::from_raw(std::mem::transmute(user_data)) };
    completion.op_future_state.on_completion(res);
}

impl<F: AsRawFd> OpFuture for PreadvCompletionFutState<F> {
    fn on_completion(&self, res: i32) {
        let mut state = self.0.lock().unwrap();
        let cur = std::mem::replace(&mut *state, PreadvCompletionFutStateInner::Undefined);
        match cur {
            PreadvCompletionFutStateInner::Undefined => panic!("future is in undefined state"),
            PreadvCompletionFutStateInner::TrySubmission { .. } => {
                panic!("future is in try submission state")
            }
            PreadvCompletionFutStateInner::Done(_) => panic!("future is in done state"),
            PreadvCompletionFutStateInner::Pending {
                file_currently_owned_by_kernel,
                buf_currently_owned_by_kernel,
                waker,
            } => {
                *state = PreadvCompletionFutStateInner::Done(DoneSubState::Ready((
                    file_currently_owned_by_kernel,
                    buf_currently_owned_by_kernel,
                    // https://man.archlinux.org/man/io_uring_prep_read.3.en
                    if res < 0 {
                        Err(std::io::Error::from_raw_os_error(-res))
                    } else {
                        Ok(res as usize)
                    },
                )));
                drop(state);
                waker.wake();
            }
        }
    }
}

struct SendSyncCompletionQueue {
    seen_poison: bool,
    cq: CompletionQueue<'static>,
}

unsafe impl Send for SendSyncCompletionQueue {}
unsafe impl Sync for SendSyncCompletionQueue {}

enum ProcessCompletionsErr {
    PoisonPill,
}

impl SendSyncCompletionQueue {
    fn process_completions(&mut self) -> std::result::Result<(), ProcessCompletionsErr> {
        let cq = &mut self.cq;
        cq.sync();
        for cqe in &mut *cq {
            trace!("got cqe: {:?}", cqe);
            if cqe.user_data() == 0x80_00_00_00_00_00_00_00 {
                trace!("got poison pill");
                self.seen_poison = true;
                break;
            }
            process_completion(cqe.user_data(), cqe.result());
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

struct SubmitSideInner {
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
}

struct SubmitSide {
    inner: Arc<Mutex<SubmitSideInner>>,
}

unsafe impl Send for SubmitSide {}
unsafe impl Sync for SubmitSide {}

impl Clone for SubmitSide {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

enum SubmitError {
    QueueFull,
}

impl SubmitSide {
    fn submit(&self, sqe: &io_uring::squeue::Entry) -> std::result::Result<(), SubmitError> {
        let mut submit_side = self.inner.try_lock().unwrap();
        match unsafe { submit_side.sq.push(&sqe) } {
            Ok(()) => {}
            Err(_queue_full) => {
                return Err(SubmitError::QueueFull);
            }
        }
        submit_side.sq.sync();
        submit_side.submitter.submit().unwrap();
        Ok(())
    }
}

fn setup_poller_task(
    submitter: &Submitter<'_>,
    cq: SendSyncCompletionQueue,
) -> std::sync::mpsc::Receiver<SendSyncCompletionQueue> {
    let eventfd = eventfd::EventFD::new(0, eventfd::EfdFlags::EFD_NONBLOCK).unwrap();

    submitter.register_eventfd(eventfd.as_raw_fd()).unwrap();

    let (tx, rx) = std::sync::mpsc::sync_channel(1);

    tokio::task::spawn(tokio::task::unconstrained(async move {
        let give_back_cq_on_drop = |cq| {
            scopeguard::guard(cq, |cq| {
                tx.send(cq).unwrap();
            })
        };
        let mut cq = Some(give_back_cq_on_drop(cq));

        trace!("launching poller task");

        let fd = tokio::io::unix::AsyncFd::new(eventfd).unwrap();
        loop {
            // See fd.read() API docs for recipe for this code block.
            loop {
                let mut guard = fd.ready(tokio::io::Interest::READABLE).await.unwrap();
                if !guard.ready().is_readable() {
                    info!("spurious wakeup");
                    continue;
                }
                match fd.get_ref().read() {
                    Ok(val) => {
                        assert!(val > 0);
                        // info!("read: {val:?}");
                        continue;
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // info!("would block");
                        guard.clear_ready_matching(tokio::io::Ready::READABLE);
                        break;
                    }
                    Err(e) => panic!("{:?}", e),
                }
            }
            trace!("eventfd ready, processing completions");

            let mut unprotected_cq = scopeguard::ScopeGuard::into_inner(cq.take().unwrap());
            let res = unprotected_cq.process_completions();
            cq = Some(give_back_cq_on_drop(unprotected_cq));
            match res {
                Ok(()) => {}
                Err(ProcessCompletionsErr::PoisonPill) => {
                    info!("poller observed poison pill");
                    break; // drops the cq
                }
            }
        }
    }));

    rx
}
