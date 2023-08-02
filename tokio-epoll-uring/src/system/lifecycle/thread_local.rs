//! Lazily-launched [`System`] thread-local to current tokio executor thread.

use std::{
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::Poll,
};

use futures::FutureExt;

use crate::{
    system::submission::op_fut::{Op, OpFut},
    System, SystemHandle,
};

/// Submit [`crate::Ops`] to a lazily launched [`System`]
/// that is thread-local to the current executor thread.
///
/// ```
/// async {
///     use crate::tokio_epoll_uring::Ops;
///     let file = std::fs::File::open("/dev/null").unwrap();
///     let file: std::os::fd::OwnedFd = file.into();
///     let buf = vec![0; 2048];
///     let ((_, _), res) =
///     tokio_epoll_uring::with_thread_local_system(|system| {
///         system.read(file, 0, buf)
///     })
///     .await;
/// };
/// ```
///
pub fn with_thread_local_system<F, O>(make_op: F) -> ThreadLocalOpFut<F, O>
where
    O: Op + Unpin + Send,
    F: Send + 'static + FnOnce(&'_ SystemHandle) -> OpFut<O>,
{
    ThreadLocalOpFut {
        state: ThreadLocalOpFutState::TryAgainThreadLocal { make_op },
    }
}

pub struct ThreadLocalOpFut<F, O>
where
    O: Op + Unpin + Send,
    F: Send + 'static + FnOnce(&'_ SystemHandle) -> OpFut<O>,
{
    state: ThreadLocalOpFutState<F, O>,
}

enum ThreadLocalOpFutState<F, O>
where
    O: Op + Unpin + Send,
    F: Send + 'static + FnOnce(&'_ SystemHandle) -> OpFut<O>,
{
    Undefined,
    WaitForThreadLocal {
        make_op: F,
        try_again_rx: Pin<Box<dyn Send + std::future::Future<Output = ()>>>,
    },
    TryAgainThreadLocal {
        make_op: F,
    },
    Submitted(OpFut<O>),
    ReadyPolled,
}

impl<F, O> std::future::Future for ThreadLocalOpFut<F, O>
where
    O: Op + Unpin + Send,
    F: Unpin + Send + 'static + FnOnce(&'_ SystemHandle) -> OpFut<O>,
{
    type Output = (
        O::Resources,
        Result<O::Success, crate::system::submission::op_fut::Error<O::Error>>,
    );

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut myself = self.as_mut();
        loop {
            let cur = std::mem::replace(&mut myself.state, ThreadLocalOpFutState::Undefined);
            match cur {
                ThreadLocalOpFutState::Undefined => unreachable!(),
                ThreadLocalOpFutState::TryAgainThreadLocal { make_op } => {
                    let res = try_with_thread_local_submit_side(make_op);
                    match res {
                        TryThreadLocalSubmitResult::Submitted(op) => {
                            myself.state = ThreadLocalOpFutState::Submitted(op);
                        }
                        TryThreadLocalSubmitResult::WaitingForThreadLocal(f, waiter) => {
                            myself.state = ThreadLocalOpFutState::WaitForThreadLocal {
                                make_op: f,
                                try_again_rx: waiter,
                            }
                        }
                    }
                    continue;
                }
                ThreadLocalOpFutState::WaitForThreadLocal {
                    make_op,
                    mut try_again_rx,
                } => match try_again_rx.poll_unpin(cx) {
                    std::task::Poll::Ready(()) => {
                        myself.state = ThreadLocalOpFutState::TryAgainThreadLocal { make_op };
                        continue;
                    }
                    std::task::Poll::Pending => {
                        myself.state = ThreadLocalOpFutState::WaitForThreadLocal {
                            make_op,
                            try_again_rx,
                        };
                        return Poll::Pending;
                    }
                },
                ThreadLocalOpFutState::Submitted(mut op) => match op.poll_unpin(cx) {
                    std::task::Poll::Ready(ready) => {
                        myself.state = ThreadLocalOpFutState::ReadyPolled;
                        return Poll::Ready(ready);
                    }
                    std::task::Poll::Pending => {
                        myself.state = ThreadLocalOpFutState::Submitted(op);
                        return Poll::Pending;
                    }
                },
                ThreadLocalOpFutState::ReadyPolled => {
                    unreachable!("must not poll future after it returned Ready");
                }
            }
        }
    }
}

enum ThreadLocalState {
    Uninit,
    Launching(tokio::sync::broadcast::Sender<()>),
    Launched(SystemHandle),
    Undefined,
}

thread_local! {
    static THREAD_LOCAL: std::sync::Arc<Mutex<ThreadLocalState>> = Arc::new(Mutex::new(ThreadLocalState::Uninit));
}

enum TryThreadLocalSubmitResult<F, O>
where
    O: Op + Unpin + Send,
    F: Unpin + 'static + FnOnce(&'_ SystemHandle) -> OpFut<O>,
{
    Submitted(OpFut<O>),
    WaitingForThreadLocal(F, Pin<Box<dyn Send + std::future::Future<Output = ()>>>),
}

fn try_with_thread_local_submit_side<O, F>(f: F) -> TryThreadLocalSubmitResult<F, O>
where
    O: Op + Unpin + Send,
    F: Unpin + 'static + FnOnce(&'_ SystemHandle) -> OpFut<O>,
{
    THREAD_LOCAL.with(move |local_state_arc| {
        let mut local_state = local_state_arc.lock().unwrap();
        let cur = std::mem::replace(&mut *local_state, ThreadLocalState::Undefined);
        match cur {
            ThreadLocalState::Undefined => unreachable!(),
            ThreadLocalState::Uninit => {
                let weak_self = Arc::downgrade(local_state_arc);
                let (tx, mut rx) = tokio::sync::broadcast::channel(1);
                *local_state = ThreadLocalState::Launching(tx);
                tokio::spawn(async move {
                    let system_handle = System::launch().await;
                    match Weak::upgrade(&weak_self) {
                        None => {
                            // thread / thread-local got destroyed while we were launching
                            drop(system_handle);
                        }
                        Some(local_state_arc) => {
                            let mut local_state_guard = local_state_arc.lock().unwrap();
                            let cur = std::mem::replace(
                                &mut *local_state_guard,
                                ThreadLocalState::Undefined,
                            );
                            let tx = match cur {
                                ThreadLocalState::Launching(tx) => {
                                    *local_state_guard = ThreadLocalState::Launched(system_handle);
                                    tx
                                }
                                ThreadLocalState::Uninit
                                | ThreadLocalState::Launched(_)
                                | ThreadLocalState::Undefined => unreachable!(),
                            };
                            drop(local_state_guard);
                            drop(tx);
                        }
                    }
                });
                TryThreadLocalSubmitResult::WaitingForThreadLocal(
                    f,
                    Box::pin(async move {
                        let _ = rx.recv();
                    }),
                )
            }
            ThreadLocalState::Launching(tx) => {
                let mut rx = tx.subscribe();
                *local_state = ThreadLocalState::Launching(tx);
                TryThreadLocalSubmitResult::WaitingForThreadLocal(
                    f,
                    Box::pin(async move {
                        let _ = rx.recv().await;
                    }),
                )
            }
            ThreadLocalState::Launched(system) => {
                let res = f(&system);
                *local_state = ThreadLocalState::Launched(system);
                TryThreadLocalSubmitResult::Submitted(res)
            }
        }
    })
}
