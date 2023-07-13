use std::{
    os::fd::AsRawFd,
    sync::{Arc, Mutex},
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tracing::{debug, info};

enum ThreadLocalStateInner {
    NotUsed,
    Used {
        split_uring: *mut io_uring::IoUring,
        submit_side: SubmitSide,
        rx_completion_queue_from_poller_task: std::sync::mpsc::Receiver<SendSyncCompletionQueue>,
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
                    let uring = Box::into_raw(Box::new(io_uring::IoUring::new(128).unwrap()));
                    let uring_fd = unsafe { (*uring).as_raw_fd() };
                    let (submitter, sq, cq) = unsafe { (&mut *uring).split() };
                    let rx = setup_poller_task(
                        uring_fd,
                        SendSyncCompletionQueue {
                            cq,
                            seen_poison: false,
                        },
                    );
                    let submit_side = SubmitSide { submitter, sq };
                    *local_state = ThreadLocalState(ThreadLocalStateInner::Used {
                        split_uring: uring,
                        rx_completion_queue_from_poller_task: rx,
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
                let mut cq =
                    cq.unwrap_or_else(|| rx_completion_queue_from_poller_task.recv().unwrap());

                if !cq.seen_poison {
                    // this is case (2)
                    loop {
                        match cq.process_completions() {
                            Ok(()) => continue,
                            Err(ProcessCompletionsErr::PoisonPill) => break,
                        }
                    }
                }
                assert!(cq.seen_poison);

                let SubmitSide { submitter, sq } = submit_side;
                let SendSyncCompletionQueue { seen_poison: _, cq } = cq;
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

type PreadvOutput<F, B> = (F, B, std::io::Result<usize>);

pub fn preadv<F: AsRawFd + 'static, B: tokio_uring::buf::IoBufMut>(
    file: F,
    offset: u64,
    buf: B,
) -> impl std::future::Future<Output = PreadvOutput<F, B>> {
    PreadvCompletionFut {
        state: Arc::new(PreadvCompletionFutState(Mutex::new(
            PreadvCompletionFutStateInner::TrySubmission { file, offset, buf },
        ))),
    }
}

enum DoneSubState<F: AsRawFd, B: tokio_uring::buf::IoBufMut> {
    Ready(PreadvOutput<F, B>),
    Taken,
}

struct PreadvCompletionFutState<T: AsRawFd, B: tokio_uring::buf::IoBufMut>(
    Mutex<PreadvCompletionFutStateInner<T, B>>,
);
enum PreadvCompletionFutStateInner<F: AsRawFd, B: tokio_uring::buf::IoBufMut> {
    Undefined,
    TrySubmission {
        file: F,
        offset: u64,
        buf: B,
    },
    Pending {
        file_currently_owned_by_kernel: F,
        buf_currently_owned_by_kernel: B,
        waker: std::task::Waker,
    },
    Done(DoneSubState<F, B>),
}

struct PreadvCompletionFut<F: AsRawFd, B: tokio_uring::buf::IoBufMut> {
    state: Arc<PreadvCompletionFutState<F, B>>,
}

impl<F: AsRawFd + 'static, B: tokio_uring::buf::IoBufMut> std::future::Future
    for PreadvCompletionFut<F, B>
{
    type Output = PreadvOutput<F, B>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut state = self.state.0.lock().unwrap();
        let cur = std::mem::replace(&mut *state, PreadvCompletionFutStateInner::Undefined);
        match cur {
            PreadvCompletionFutStateInner::Undefined => panic!("future is in undefined state"),
            PreadvCompletionFutStateInner::TrySubmission {
                file,
                offset,
                mut buf,
            } => {
                let completion = Box::new(Completion {
                    op_future_state: Arc::clone(&self.state) as Arc<dyn OpFuture>,
                });
                let user_data = Box::into_raw(completion) as u64;
                assert!(
                    user_data & 0x80_00_00_00_00_00_00_00 == 0,
                    "highest bit is reserved for posion pill"
                );

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
                .user_data(user_data);

                with_this_executor_threads_submit_side(|submit_side| {
                    match submit_side.submit(&sqe) {
                        Ok(()) => {
                            *state = PreadvCompletionFutStateInner::Pending {
                                waker: cx.waker().clone(),
                                buf_currently_owned_by_kernel: buf,
                                file_currently_owned_by_kernel: file,
                            };
                            return std::task::Poll::Pending;
                        }
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
                })
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

impl<F: AsRawFd, B: tokio_uring::buf::IoBufMut> OpFuture for PreadvCompletionFutState<F, B> {
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
                mut buf_currently_owned_by_kernel,
                waker,
            } => {
                // https://man.archlinux.org/man/io_uring_prep_read.3.en
                let res = if res < 0 {
                    Err(std::io::Error::from_raw_os_error(-res))
                } else {
                    unsafe { buf_currently_owned_by_kernel.set_init(res as usize) };
                    Ok(res as usize)
                };
                *state = PreadvCompletionFutStateInner::Done(DoneSubState::Ready((
                    file_currently_owned_by_kernel,
                    buf_currently_owned_by_kernel,
                    res,
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

enum ProcessCompletionsErr {
    PoisonPill,
}

impl SendSyncCompletionQueue {
    fn process_completions(&mut self) -> std::result::Result<(), ProcessCompletionsErr> {
        let cq = &mut self.cq;
        cq.sync();
        for cqe in &mut *cq {
            debug!("got cqe: {:?}", cqe);
            if cqe.user_data() == 0x80_00_00_00_00_00_00_00 {
                debug!("got poison pill");
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

struct SubmitSide {
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
}

unsafe impl Send for SubmitSide {}

enum SubmitError {
    QueueFull,
}

impl SubmitSide {
    fn submit(&mut self, sqe: &io_uring::squeue::Entry) -> std::result::Result<(), SubmitError> {
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
    cq: SendSyncCompletionQueue,
) -> std::sync::mpsc::Receiver<SendSyncCompletionQueue> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);

    tokio::task::spawn(tokio::task::unconstrained(async move {
        let give_back_cq_on_drop = |cq| {
            scopeguard::guard(cq, |cq| {
                tx.send(cq).unwrap();
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
