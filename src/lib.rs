use std::{
    os::fd::AsRawFd,
    sync::{Arc, Mutex},
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tracing::info;

enum ThreadLocalState {
    NotUsed,
    Used(SubmitSide),
    ShuttingDown,
}

thread_local! {
    static THREAD_LOCAL: std::cell::RefCell<Arc<Mutex<ThreadLocalState>>> = std::cell::RefCell::new(Arc::new(Mutex::new(ThreadLocalState::NotUsed)));
}

fn get_this_executor_threads_submit_side() -> std::io::Result<SubmitSide> {
    THREAD_LOCAL.with(|local_state| {
        let local_state = local_state.borrow_mut();
        let mut local_state = local_state.lock().unwrap();
        Ok(match &*local_state {
            ThreadLocalState::NotUsed => {
                let uring = Box::leak(Box::new(io_uring::IoUring::new(128).unwrap()));
                let (mut submitter, sq, cq) = uring.split();
                setup_poller_task(&mut submitter, SendSyncCompletionQueue { cq });
                let submit_side = SubmitSide {
                    inner: Arc::new(Mutex::new(SubmitSideInner { submitter, sq })),
                };
                *local_state = ThreadLocalState::Used(submit_side.clone());
                submit_side
            }
            ThreadLocalState::Used(submit_side) => submit_side.clone(),
            ThreadLocalState::ShuttingDown => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "thread local uring is shutting down",
                ));
            }
        })
    })
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
                .user_data(Box::into_raw(completion) as u64);

                *state = PreadvCompletionFutStateInner::Pending {
                    waker: cx.waker().clone(),
                    buf_currently_owned_by_kernel: buf,
                    file_currently_owned_by_kernel: file,
                };
                drop(state);

                let mut submit_side = submit_side.inner.try_lock().unwrap();
                match unsafe { submit_side.sq.push(&sqe) } {
                    Ok(()) => {}
                    Err(_queue_full) => {
                        // TODO: unleak
                        return std::task::Poll::Pending;
                    }
                }
                submit_side.sq.sync();
                submit_side.submitter.submit().unwrap();

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
    cq: CompletionQueue<'static>,
}

unsafe impl Send for SendSyncCompletionQueue {}
unsafe impl Sync for SendSyncCompletionQueue {}

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

fn setup_poller_task(submitter: &Submitter<'_>, cq: SendSyncCompletionQueue) {
    let eventfd = eventfd::EventFD::new(0, eventfd::EfdFlags::EFD_NONBLOCK).unwrap();

    submitter.register_eventfd(eventfd.as_raw_fd()).unwrap();

    tokio::task::spawn(tokio::task::unconstrained(async move {
        let mut cq = cq;

        info!("launching poller task");

        let fd = tokio::io::unix::AsyncFd::new(eventfd).unwrap();
        loop {
            // info!("Reaper waiting for eventfd");
            // See read() API docs for recipe for this code block.
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
            info!("eventfd ready");

            cq.cq.sync();
            for cqe in &mut cq.cq {
                info!("got cqe: {:?}", cqe);
                process_completion(cqe.user_data(), cqe.result());
            }
            cq.cq.sync();
        }
    }));
}
