//! Lazily-launched [`System`] thread-local to current tokio executor thread.

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
};

use futures::future;

use crate::{system::submission::SubmitSide, System, SystemHandle};

enum State {
    NotStarted,
    Launching,
    Launched(SystemHandle),
}

thread_local! {
    static THREAD_LOCAL: RefCell<State> = RefCell::new(State::NotStarted);
}

pub async fn with_thread_local_system<'a, 'b, F, O, FO>(f: F) -> FO
where
    F: FnOnce(&mut SystemHandle) -> O,
    O: std::future::Future<Output = FO> + 'b,
    'b: 'a,
{
    let mut f = Some(f);
    loop {
        enum Outcome<A, Y, R>
        where
            A: std::future::Future<Output = Y>,
        {
            DidStartLaunch(A),
            AlreadyLaunching,
            Launched(R),
        }
        let wait_launched = THREAD_LOCAL.with(|x| {
            let mut borrow = x.borrow_mut();
            match &mut *borrow {
                State::NotStarted => {
                    drop(borrow);
                    x.replace(State::Launching);
                    let fut = async move {
                        let handle = System::launch().await;
                        THREAD_LOCAL.with(|x| {
                            let mut borrow = x.borrow_mut();
                            match &mut *borrow {
                                State::Launching => {
                                    drop(borrow);
                                    x.replace(State::Launched(handle));
                                }
                                _ => todo!(),
                            };
                        });
                    };
                    Outcome::DidStartLaunch(fut)
                }
                State::Launching => Outcome::AlreadyLaunching,
                State::Launched(handle) => Outcome::Launched((f.take().unwrap())(handle)),
            }
        });
        match wait_launched {
            Outcome::DidStartLaunch(fut) => {
                fut.await;
                continue;
            }
            Outcome::AlreadyLaunching => {
                todo!()
            }
            Outcome::Launched(fut) => {
                return fut.await;
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
