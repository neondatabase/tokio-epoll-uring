//! Lazily-launched [`System`] thread-local to current tokio executor thread.

use std::cell::RefCell;

use crate::{System, SystemHandle};

enum State {
    NotStarted,
    Launching(tokio::sync::broadcast::Sender<()>),
    Launched(SystemHandle),
}

thread_local! {
    static THREAD_LOCAL: RefCell<State> = RefCell::new(State::NotStarted);
}

pub async fn with_thread_local_system<'a, 'b, F, O, R>(f: F) -> R
where
    F: FnOnce(&mut SystemHandle) -> O,
    O: std::future::Future<Output = R> + 'b,
    'b: 'a,
{
    let mut f = Some(f);
    loop {
        enum Outcome<A, Y, R>
        where
            A: std::future::Future<Output = Y>,
        {
            ObservedNotStartedDoStartLaunch(A),
            ObservedAlreadyLaunching(tokio::sync::broadcast::Receiver<()>),
            Launched(R),
        }
        let wait_launched = THREAD_LOCAL.with(|x| {
            let mut borrow = x.borrow_mut();
            match &mut *borrow {
                State::NotStarted => {
                    drop(borrow);
                    let (tx, _rx) = tokio::sync::broadcast::channel(1);
                    x.replace(State::Launching(tx));
                    let fut = async move {
                        let launched_by_us = System::launch().await;
                        THREAD_LOCAL.with(|x| {
                            let mut borrow = x.borrow_mut();
                            match &mut *borrow {
                                // The likely case: we remained on the same thread where we started launching.
                                State::Launching(_notify_waiters) => {
                                    drop(borrow);
                                    x.replace(State::Launched(launched_by_us));
                                    // above replace drops the _notify_waiters
                                }
                                // We were moved to another thread, and the other thread hasn't started launching yet.
                                State::NotStarted => {
                                    drop(borrow);
                                    x.replace(State::Launched(launched_by_us));
                                }
                                // We were moved to another thread, and the other thread already had a system running.
                                // Use the other thread's system and drop the one we launched. Sad but unavoidable.
                                // Should be quite rare though.
                                State::Launched(_other_threads_system) => {
                                    drop(launched_by_us);
                                }
                            };
                        });
                    };
                    Outcome::ObservedNotStartedDoStartLaunch(fut)
                }
                State::Launching(notify_waiters) => {
                    Outcome::ObservedAlreadyLaunching(notify_waiters.subscribe())
                }
                State::Launched(handle) => Outcome::Launched((f.take().unwrap())(handle)),
            }
        });
        match wait_launched {
            Outcome::ObservedNotStartedDoStartLaunch(fut) => {
                fut.await;
                continue;
            }
            Outcome::ObservedAlreadyLaunching(mut wait) => {
                let _ = wait.recv().await;
                continue;
            }
            Outcome::Launched(op_fut) => {
                return op_fut.await;
            }
        }
    }
}

pub struct Handle(tokio::sync::OwnedMutexGuard<tokio::sync::OnceCell<SystemHandle>>);

impl std::ops::Deref for Handle {
    type Target = SystemHandle;

    fn deref(&self) -> &Self::Target {
        self.0
            .get()
            .expect("must be already initialized when using this")
    }
}

impl std::ops::DerefMut for Handle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .get_mut()
            .expect("must be already initialized when using this")
    }
}
