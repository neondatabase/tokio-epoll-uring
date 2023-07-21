use std::os::fd::OwnedFd;

use crate::{
    preadv::{PreadvCompletionFut, PreadvOutput},
    rest::{SubmitSide, System, SystemLifecycleManager},
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

impl SystemLifecycleManager for ThreadLocalSystem {
    fn with_submit_side<F: FnOnce(&mut SubmitSide) -> R, R>(self, f: F) -> R {
        THREAD_LOCAL.with(|local_state| {
            let mut local_state = local_state.borrow_mut();
            loop {
                match &mut local_state.0 {
                    ThreadLocalStateInner::NotUsed => {
                        *local_state = ThreadLocalState(ThreadLocalStateInner::Used(
                            crate::rest::System::new(),
                        ));
                    }
                    // fast path
                    ThreadLocalStateInner::Used(system) => {
                        break f(&mut *system.submit_side.lock().unwrap())
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
            ThreadLocalStateInner::Used(system) => {
                system.shutdown();
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
        PreadvCompletionFut::new(ThreadLocalSystem, file, offset, buf)
    }
}