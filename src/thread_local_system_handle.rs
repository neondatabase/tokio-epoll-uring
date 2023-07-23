use std::{marker::PhantomData, task::Poll};

use futures::FutureExt;

use crate::system::{Launch, SubmitSide, SubmitSideProvider, System, SystemHandle};

#[derive(Clone, Copy)]
pub struct ThreadLocalSystemHandle;

enum ThreadLocalStateInner {
    Uninit,
    Launching(Launch),
    Launched(SystemHandle),
    Dropped,
}
struct ThreadLocalState(ThreadLocalStateInner);

impl std::fmt::Debug for ThreadLocalStateInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ThreadLocalStateInner::Uninit => f.write_str("Uninit"),
            ThreadLocalStateInner::Launching(_) => f.write_str("Launching"),
            ThreadLocalStateInner::Launched(_) => f.write_str("Launched"),
            ThreadLocalStateInner::Dropped => f.write_str("Dropped"),
        }
    }
}

thread_local! {
    static THREAD_LOCAL: std::cell::RefCell<ThreadLocalState> = std::cell::RefCell::new(ThreadLocalState(ThreadLocalStateInner::Uninit));
}

pub struct ThreadLocalSubmitSideProvider {
    _not_send_because_thread_local: PhantomData<*const ()>,
}

impl ThreadLocalSubmitSideProvider {
    fn new() -> Self {
        Self {
            _not_send_because_thread_local: PhantomData,
        }
    }
}

impl std::future::Future for ThreadLocalSystemHandle {
    type Output = ThreadLocalSubmitSideProvider;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        THREAD_LOCAL.with(|local_state| {
            let mut local_state = local_state.borrow_mut();
            loop {
                match &mut local_state.0 {
                    ThreadLocalStateInner::Uninit => {
                        let f = System::launch();
                        *local_state = ThreadLocalState(ThreadLocalStateInner::Launching(f));
                        continue;
                    }
                    ThreadLocalStateInner::Launching(f) => {
                        if let Poll::Ready(system) = f.poll_unpin(cx) {
                            *local_state =
                                ThreadLocalState(ThreadLocalStateInner::Launched(system));
                            continue;
                        } else {
                            return Poll::Pending;
                        }
                    }
                    // fast path
                    ThreadLocalStateInner::Launched(_system) => {
                        return Poll::Ready(ThreadLocalSubmitSideProvider::new())
                    }
                    ThreadLocalStateInner::Dropped => {
                        unreachable!("threat-local can't be dropped while executing")
                    }
                }
            }
        })
    }
}

impl SubmitSideProvider for ThreadLocalSubmitSideProvider {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(self, f: F) -> R {
        THREAD_LOCAL.with(|local_state| {
            let mut local_state = local_state.borrow_mut();
            match &mut local_state.0 {
                ThreadLocalStateInner::Launched(system) => system.with_submit_side(f),
                st @ ThreadLocalStateInner::Uninit |
                st @  ThreadLocalStateInner::Launching(_) |
                st @ ThreadLocalStateInner::Dropped
                 => {
                    unreachable!("this type is only constructed after the thread-local state is `Launched` and the !Send-ness of Self means we won't move to another thread in the meantime; {st:?}");
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
            ThreadLocalStateInner::Uninit => {}
            ThreadLocalStateInner::Launching(launch) => {
                // on implicit shutdown, we don't care about when it finishes
                let _ = launch;
            }
            ThreadLocalStateInner::Launched(system) => {
                // on implicit shutdown, we don't care about when it finishes
                let _ = system.initiate_shutdown();
            }
            ThreadLocalStateInner::Dropped => {
                unreachable!("ThreadLocalState::drop() had already been called in the past");
            }
        }
    }
}
