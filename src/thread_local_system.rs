use std::{
    os::fd::{AsRawFd, OwnedFd},
    sync::{Arc, Mutex, Weak},
};

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use tracing::trace;

use crate::{
    setup_poller_task, Ops, PreadvCompletionFut, PreadvCompletionFutState, PreadvOutput,
    ProcessCompletionsErr, SendSyncCompletionQueue, SubmitError, SubmitSide, RING_SIZE,
};

#[derive(Clone, Copy)]
pub struct ThreadLocalSystem;

enum ThreadLocalStateInner {
    NotUsed,
    Used(System),
    Dropped,
}
struct ThreadLocalState(ThreadLocalStateInner);

thread_local! {
    static THREAD_LOCAL: std::cell::RefCell<ThreadLocalState> = std::cell::RefCell::new(ThreadLocalState(ThreadLocalStateInner::NotUsed));
}

struct System {
    split_uring: *mut io_uring::IoUring,
    submit_side: Arc<Mutex<SubmitSide>>,
    rx_completion_queue_from_poller_task:
        std::sync::mpsc::Receiver<Arc<Mutex<SendSyncCompletionQueue>>>,
}

impl super::SystemTrait for ThreadLocalSystem {
    fn with_submit_side<F: FnOnce(&mut crate::SubmitSide) -> R, R>(self, f: F) -> R {
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
                        let uring =
                            Box::into_raw(Box::new(io_uring::IoUring::new(RING_SIZE).unwrap()));
                        let uring_fd = unsafe { (*uring).as_raw_fd() };
                        let (submitter, sq, cq) = unsafe { (&mut *uring).split() };

                        let send_sync_completion_queue =
                            Arc::new(Mutex::new(SendSyncCompletionQueue {
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
                        *local_state = ThreadLocalState(ThreadLocalStateInner::Used(System {
                            split_uring: uring,
                            rx_completion_queue_from_poller_task,
                            submit_side,
                        }));
                        continue;
                    }
                    // fast path
                    ThreadLocalStateInner::Used(System { submit_side, .. }) => {
                        break f(&mut *submit_side.lock().unwrap())
                    }
                    ThreadLocalStateInner::Dropped => {
                        unreachable!("threat-local can't be dropped while executing")
                    }
                }
            }
        })
    }
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        let cur: ThreadLocalStateInner =
            std::mem::replace(&mut self.0, ThreadLocalStateInner::Dropped);
        match cur {
            ThreadLocalStateInner::NotUsed => {}
            ThreadLocalStateInner::Used(System {
                split_uring,
                submit_side,
                rx_completion_queue_from_poller_task,
            }) => {
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
                        submit_side.submit(entry)
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
                let submit_side = Arc::try_unwrap(submit_side).ok().expect("we've shut down the system, so, we're the only ones with a reference to submit_side");
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
            ThreadLocalStateInner::Dropped => {
                unreachable!("ThreadLocalState::drop() had already been called in the past");
            }
        }
    }
}

impl ThreadLocalSystem {
    pub fn preadv<B: tokio_uring::buf::IoBufMut + Send>(
        file: OwnedFd,
        offset: u64,
        buf: B,
    ) -> impl std::future::Future<Output = PreadvOutput<B>> + Send {
        let fut = PreadvCompletionFut {
            system: ThreadLocalSystem,
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
}
