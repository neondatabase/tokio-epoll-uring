use std::{
    collections::{HashSet, VecDeque},
    os::fd::AsRawFd,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};

pub mod handle;

use futures::FutureExt;
use io_uring::{CompletionQueue, SubmissionQueue, Submitter};

use crate::system::{submission::SubmitSideOpen, Ops, RING_SIZE};

use super::{
    completion::{CompletionSide, Poller, PollerNewArgs, PollerTesting},
    lifecycle::handle::{SystemHandle, SystemHandleLive, SystemHandleState},
    submission::{SubmitSide, SubmitSideNewArgs},
    OpsInner, OpsInnerOpen,
};

/// The live system. Not constructible or accessible by user code. Use [`SystemHandle`] to interact.
pub struct System {
    #[allow(dead_code)]
    id: usize,
    split_uring: *mut io_uring::IoUring,
    // poller_heartbeat: (), // TODO
}

// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Send for System {}
// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Sync for System {}

/// See [`System::launch`].
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
        poller_ready_fut: Pin<
            Box<
                dyn Send
                    + std::future::Future<Output = crate::shutdown_request::Sender<ShutdownRequest>>,
            >,
        >,
        submit_side: SubmitSide,
    },
    Launched,
    Undefined,
}

impl System {
    /// Returns a future that, when polled, sets up an io_uring instance
    /// and a corresponding *poller task* on the current tokio runtime.
    /// The future resolves to a [`SystemHandle`] that can be used to
    /// interact with the system.
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
    pub(crate) fn launch_with_testing(poller_preempt: PollerTesting) -> Launch {
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

impl std::future::Future for Launch {
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
                    let ops = Arc::new_cyclic(|myself| {
                        Mutex::new(Ops {
                            id,
                            inner: Arc::new(Mutex::new(OpsInner::Open(Box::new(OpsInnerOpen {
                                id,
                                storage: array_macro::array![_ => None; RING_SIZE as usize],
                                unused_indices: (0..RING_SIZE.try_into().unwrap()).collect(),
                                waiters: VecDeque::new(),
                                myself: Weak::clone(&myself),
                            })))),
                        })
                    });
                    let uring = Box::into_raw(Box::new(io_uring::IoUring::new(RING_SIZE).unwrap()));
                    let uring_fd = unsafe { (*uring).as_raw_fd() };
                    let (submitter, sq, cq) = unsafe { (&mut *uring).split() };

                    let completion_side =
                        Arc::new(Mutex::new(CompletionSide::new(id, cq, ops.clone())));

                    let submit_side = SubmitSide::new(SubmitSideNewArgs {
                        id,
                        submitter,
                        sq,
                        ops: Arc::clone(&ops),
                        completion_side: Arc::clone(&completion_side),
                    });
                    let system = System {
                        id,
                        split_uring: uring,
                    };
                    let poller_ready_fut = Poller::launch(PollerNewArgs {
                        id,
                        uring_fd,
                        completion_side,
                        system,
                        ops,
                        preempt: poller_preempt,
                    });
                    myself.state = LaunchState::WaitForPollerTaskToStart {
                        poller_ready_fut: Box::pin(poller_ready_fut),
                        submit_side,
                    };
                    continue;
                }
                LaunchState::WaitForPollerTaskToStart {
                    mut poller_ready_fut,
                    submit_side,
                } => match poller_ready_fut.poll_unpin(cx) {
                    std::task::Poll::Ready(poller_shutdown_tx) => {
                        myself.state = LaunchState::Launched;
                        let system_handle = SystemHandle {
                            state: SystemHandleState::KeepSystemAlive(SystemHandleLive {
                                id,
                                submit_side,
                                shutdown_tx: poller_shutdown_tx,
                            }),
                        };
                        return std::task::Poll::Ready(system_handle);
                    }
                    std::task::Poll::Pending => {
                        myself.state = LaunchState::WaitForPollerTaskToStart {
                            poller_ready_fut,
                            submit_side,
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

pub(crate) struct ShutdownRequest {
    pub done_tx: tokio::sync::oneshot::Sender<()>,
    pub open_state: SubmitSideOpen,
}

pub(crate) fn poller_impl_finish_shutdown(
    system: System,
    ops: Arc<Mutex<Ops>>,
    completion_side: Arc<Mutex<CompletionSide>>,
    req: ShutdownRequest,
) {
    tracing::info!("poller shutdown start");
    scopeguard::defer_on_success! {tracing::info!("poller shutdown end")};
    scopeguard::defer_on_unwind! {tracing::error!("poller shutdown panic")};

    let System { id: _, split_uring } = { system };

    let ShutdownRequest {
        done_tx,
        open_state,
    } = req;

    let (submitter, sq) = open_state.deconstruct();
    let completion_side = Arc::try_unwrap(completion_side)
        .ok()
        .expect("we plugged the SubmitSide, so, all refs to CompletionSide are gone");
    let completion_side = Mutex::into_inner(completion_side).unwrap();

    // Unsplit the uring
    // explicit type to ensure we get compile-time-errors if we don't have the owned io_uring crate types
    let mut cq: CompletionQueue<'_> = completion_side.deconstruct();
    let mut sq: SubmissionQueue<'_> = sq;
    let submitter: Submitter<'_> = submitter;
    // We now own all the parts from the IoUring::split() again.
    // Some final assertions, then drop them all, unleak the IoUring, and drop it as well.
    // That cleans up the SQ, CQs, registrations, etc.
    cq.sync();
    assert_eq!(cq.len(), 0, "cqe: {:?}", cq.next());
    sq.sync();
    assert_eq!(sq.len(), 0);
    {
        // NB about ops: we can't Arc::try_unwrap().unwrap() it here because
        // there may still be OpFut's that hold a Weak ref to it, and they may
        // try to upgrade the Weak ref to an Arc at any time.
        // Such OpFuts' slot will be in OpState::Ready, so, they're going to
        // drop their upgraded Arc quite soon.
        // So, it's guaranteed that `ops` will be dropped quite soon after we
        // drop our Arc.

        let ops_guard = ops.lock().unwrap();
        let ops_inner = ops_guard.inner.lock().unwrap();
        let inner = match &*ops_inner {
            OpsInner::Undefined => unreachable!(),
            OpsInner::Open(_open) => panic!("we should be Draining by now"),
            OpsInner::Draining(inner) => inner,
        };
        let slots_owned_by_user_space = inner.slots_owned_by_user_space().collect::<HashSet<_>>();
        let unused_indices = inner
            .unused_indices
            .iter()
            .cloned()
            .collect::<HashSet<usize>>();
        // at this time, all slots must be either in unused_indices (their state is None) or they must be in Ready state
        assert_eq!(
            inner.slots_owned_by_user_space().count(),
            RING_SIZE.try_into().unwrap()
        );
        assert!(unused_indices.is_subset(&slots_owned_by_user_space));
        // drop our arc
        drop(ops_inner);
        drop(ops_guard);
        drop(ops);
    }
    // final assertions done, do the unsplitting
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
