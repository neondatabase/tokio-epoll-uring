use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use io_uring::CompletionQueue;
use tokio::sync::{self, broadcast, mpsc, oneshot};
use tracing::{debug, info, info_span, trace, Instrument};

use crate::util::oneshot_nonconsuming;

use super::{
    lifecycle::{ShutdownRequest, System},
    slots::{CoOwnerCompletionSide, CoOwnerPoller, Slots},
};

pub(crate) struct CompletionSide {
    #[allow(dead_code)]
    id: usize,
    cq: CompletionQueue<'static>,
    slots: Slots<CoOwnerCompletionSide>,
}

unsafe impl Send for CompletionSide {}

pub(crate) enum ProcessCompletionsCause {
    Regular,
    Shutdown,
}

impl CompletionSide {
    pub(crate) fn new(
        id: usize,
        cq: CompletionQueue<'static>,
        ops: Slots<CoOwnerCompletionSide>,
    ) -> Self {
        Self { id, cq, slots: ops }
    }
    pub(crate) fn deconstruct(self) -> CompletionQueue<'static> {
        let Self {
            id: _,
            cq,
            slots: _,
        } = { self };
        cq
    }
    pub(crate) fn process_completions(&mut self, _cause: ProcessCompletionsCause) {
        self.cq.sync();
        self.slots.process_completions(&mut self.cq);
        self.cq.sync();
    }
}

pub(crate) struct Poller {
    id: usize,
    state: PollerState,
}

pub(crate) struct PollerNewArgs {
    pub id: usize,
    pub uring_fd: std::os::fd::RawFd,
    pub completion_side: Arc<Mutex<CompletionSide>>,
    pub system: System,
    pub(crate) slots: Slots<CoOwnerPoller>,
    pub testing: Option<PollerTesting>,
}

impl Poller {
    pub(crate) async fn launch(
        args: PollerNewArgs,
    ) -> oneshot_nonconsuming::SendOnce<ShutdownRequest> {
        let PollerNewArgs {
            id,
            uring_fd,
            completion_side,
            system,
            slots: ops,
            testing,
        } = args;
        let (shutdown_tx, shutdown_rx) = oneshot_nonconsuming::channel();
        let poller_task_state = Arc::new(Mutex::new(Poller {
            id,
            state: PollerState::RunningInTask(Arc::new(Mutex::new(PollerStateInner {
                uring_fd,
                completion_side,
                system,
                slots: ops,
                shutdown_rx,
            }))),
        }));
        let (poller_ready_tx, poller_ready_rx) = oneshot::channel();
        tokio::task::spawn(poller_task(
            Arc::clone(&poller_task_state),
            poller_ready_tx,
            testing,
        ));
        poller_ready_rx
            .await
            // TODO make launch fallible and propagate this error
            .expect("poller task must not die during startup");
        shutdown_tx
    }
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
    pub slots: Slots<CoOwnerPoller>,
    shutdown_rx: oneshot_nonconsuming::Receiver<ShutdownRequest>,
}

pub(crate) struct PollerTesting {
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
    // 1. Prevent new ops from being submitted and wait for all inflight ops to finish.
    // 2. Unsplit the uring by
    //      2.1. By dropping Submitter, SubmissionQueue, CompletionQueue
    //      2 2. Box::from_raw'ing the IoUring stored as a raw pointer in the System struct
    // 3. Drop the IoUring struct, cleaning up the underlying kernel resources.

    if let Some(tx) = shutdown_loop_reached {
        // receiver must ensure that clone doesn't outlive the try_unwrap during shutdown
        tx.send(Arc::clone(&poller)).ok().unwrap();
    }

    // 1. Prevent new ops from being submitted and wait for all inflight ops to finish.
    // `SystemHandleInner::shutdown` already plugged the sumit side & transitioned Ops to `Draining` state.
    // So, all that's left is to wait for pending count to reach 0.
    loop {
        {
            let inner_guard = inner_shared.lock().unwrap();
            let mut completion_side_guard = inner_guard.completion_side.lock().unwrap();
            let pending_count = completion_side_guard.slots.pending_slot_count();
            debug!(pending_count, "waiting for pending operations to complete");
            if pending_count == 0 {
                break;
            }
            completion_side_guard.process_completions(ProcessCompletionsCause::Shutdown);
            drop(completion_side_guard);
            drop(inner_guard);
        }
        // If we get cancelled here, e.g., because the runtime is getting dropped,
        // the Poller is in state ShuttingDown.
        // The scopeguard in our caller will spawn an OS thread and re-run this function.
        // TODO instead of timeout, reconfigure the slots to wake us up here.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // From here on, we cannot let ourselves be cancelled at an `.await` anymore.
    // (See comment on `yield_now().await` above why cancellation is safe earlier.)
    // Use a closure to enforce it.
    {
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

                let PollerStateInner {
                    uring_fd: _,
                    completion_side,
                    system,
                    slots: ops,
                    shutdown_rx: _,
                } = { inner_owned }; // scope to make the `x: _` destructuring drop.

                crate::system::lifecycle::poller_impl_finish_shutdown(
                    system,
                    ops,
                    completion_side,
                    req,
                );
            }
        };
        poller.lock().unwrap().state = PollerState::ShutDown;
        tracing::info!("poller finished shutdown");
    }
}

async fn poller_impl_impl(
    inner: Arc<Mutex<PollerStateInner>>,
    mut preempt_in_epoll: Option<tokio::sync::broadcast::Receiver<mpsc::UnboundedSender<()>>>,
) -> ShutdownRequest {
    let (uring_fd, completion_side, mut shutdown_rx) = {
        let mut inner_guard = inner.lock().unwrap();
        let PollerStateInner {
            uring_fd,
            completion_side,
            system: _,
            slots: _,
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
                rx = shutdown_rx.recv()  => {
                    match rx {
                        crate::util::oneshot_nonconsuming::RecvResult::FirstRecv(req) => {
                            tracing::debug!("got explicit shutdown request");
                            return req;
                        }
                        crate::util::oneshot_nonconsuming::RecvResult::NotFirstRecv => {
                            panic!("once we observe a shutdown request, we return it and the caller does through with shutdown, without a chance for the executor to intervene")
                        }
                        crate::util::oneshot_nonconsuming::RecvResult::SenderDropped => {
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
        completion_side_guard.process_completions(ProcessCompletionsCause::Regular); // todo: catch_unwind to enable orderly shutdown? or at least abort if it panics?
        if is_timeout_wakeup {
            completion_side_guard.slots.poller_timeout_debug_dump();
        }
        drop(completion_side_guard);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
    };

    use tracing::info;

    use crate::{
        system::{
            completion::{PollerState, PollerTesting},
            lifecycle::System,
            test_util::shared_system_handle::SharedSystemHandle,
        },
        Ops,
    };

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

        let (reader, mut writer) = os_pipe::pipe().unwrap();
        let reader = unsafe { OwnedFd::from_raw_fd(nix::unistd::dup(reader.as_raw_fd()).unwrap()) };

        let (system, read_fut) = rt.block_on(async move {
            let system = SharedSystemHandle::launch_with_testing(testing).await;
            let mut read_fut = Box::pin(system.read(reader, 0, vec![1]));
            tokio::select! {
                _ = &mut read_fut => { panic!("we haven't written to the pipe yet") },
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    info!("by now the operation should be submitted");
                    (system, read_fut)
                }
            }
        });

        // When we send shutdown, the slot will keep the poller task in the shutdown process_completions loop.
        // Then we'll use the preempt_tx to cancel the poller task future.
        // We should observe a transition to RunningInThread.
        let shutdown_done_fut = system.initiate_shutdown();

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
            let mut shutdown_done_fut = shutdown_done_fut;
            tokio::select! {
                // TODO don't rely on timing
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => { }
                _ = &mut shutdown_done_fut => {
                    panic!("shutdown should not complete until submit_fut is done");
                }
            }

            // now unblock the read
            writer.write_all(&[1]).unwrap();

            tokio::select! {
                // TODO don't rely on timing
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    panic!("shutdown should complete after submit_fut is done");
                }
                _ = &mut shutdown_done_fut => { }
            }
        });

        let ((_, _), res) = second_rt.block_on(read_fut);
        let err = res.expect_err("when poller signals shutdown_done, it has dropped the Ops Arc; read_fut only holds a Weak to it and will fail to upgrade");
        assert_eq!(
            format!("{:#}", err),
            "tokio-epoll-uring: system is shut down"
        );
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
                let ((reader, buf), res) = system.read(reader, 0, buf).await;
                res.unwrap();
                assert_eq!(buf, &[1]);
                // now we know it has reached the epoll loop once
                // next call is the one to be interrupted
                let ((_, buf), res) = system.read(reader, 0, buf).await;
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
                PollerState::RunningInThread(_)
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
}
