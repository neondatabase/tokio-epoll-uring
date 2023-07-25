use std::sync::{Arc, Mutex, Weak};

pub(super) mod completion;
pub(super) mod lifecycle;
pub(super) mod submission;
#[cfg(test)]
mod tests;

use tracing::trace;

use self::submission::{SubmitSideInner, UnsafeOpsSlotHandle};

pub(crate) const RING_SIZE: u32 = 128;

struct OpState(Mutex<OpStateInner>);

enum OpStateInner {
    Undefined,
    Pending {
        waker: Option<std::task::Waker>, // None if it hasn't been polled yet
    },
    PendingButFutureDropped {
        /// When a future gets dropped while the Op is still running, it gets Box'ed  This is a Box'ed `ResourcesOwnedByKernel`
        resources_owned_by_kernel: Box<dyn std::any::Any + Send>,
    },
    ReadyButFutureDropped,
    Ready {
        result: i32,
    },
}

pub struct Ops {
    id: usize,
    storage: [Option<OpState>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    waiters_rx:
        tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<UnsafeOpsSlotHandle>>,
    myself: Weak<Mutex<Ops>>,
}

impl OpStateInner {
    fn discriminant_str(&self) -> &'static str {
        match self {
            OpStateInner::Undefined => "Undefined",
            OpStateInner::Pending { .. } => "Pending",
            OpStateInner::PendingButFutureDropped { .. } => "PendingButFutureDropped",
            OpStateInner::ReadyButFutureDropped => "ReadyButFutureDropped",
            OpStateInner::Ready { .. } => "Ready",
        }
    }
}

impl Ops {
    // TODO: why do we need the submit_side here?
    fn return_slot_and_wake(&mut self, submit_side: Arc<Mutex<SubmitSideInner>>, idx: usize) {
        assert!(self.storage[idx].is_none());
        loop {
            match self.waiters_rx.try_recv() {
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    break;
                }
                Ok(waiter) => {
                    match waiter.send(UnsafeOpsSlotHandle {
                        submit_side: Arc::clone(&submit_side),
                        ops: Weak::upgrade(&self.myself).expect("we're executing on myself"),
                        idx,
                    }) {
                        Ok(()) => {
                            trace!(system = self.id, "handed `idx` to a waiter");
                            return;
                        }
                        Err(_) => {
                            // the future requesting wakeup got dropped. wake up next one
                            continue;
                        }
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    // submit side is plugged already
                    break;
                }
            }
        }
        trace!(
            system = self.id,
            "no waiters, returning `idx` to unused_indices"
        );
        self.unused_indices.push(idx);
    }
}
