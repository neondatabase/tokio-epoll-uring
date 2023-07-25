use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};

pub(super) mod completion;
pub(super) mod lifecycle;
pub(super) mod submission;
#[cfg(test)]
mod tests;

use tokio::sync::oneshot;
use tracing::trace;

use self::submission::UnsafeOpsSlotHandle;

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
    Ready {
        result: i32,
    },
    ReadyConsumed,
}

impl OpStateInner {
    fn discriminant_str(&self) -> &'static str {
        match self {
            OpStateInner::Undefined => "Undefined",
            OpStateInner::Pending { .. } => "Pending",
            OpStateInner::PendingButFutureDropped { .. } => "PendingButFutureDropped",
            OpStateInner::Ready { .. } => "Ready",
            OpStateInner::ReadyConsumed => "ReadyConsumed",
        }
    }
}

pub struct Ops {
    id: usize,
    inner: Arc<Mutex<OpsInner>>,
}

enum OpsInner {
    Open(Box<OpsInnerOpen>),
    Draining(Box<OpsInnerDraining>),
    Undefined,
}

struct OpsInnerOpen {
    id: usize,
    storage: [Option<OpState>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    waiters: VecDeque<tokio::sync::oneshot::Sender<UnsafeOpsSlotHandle>>,
    myself: Weak<Mutex<Ops>>,
}

struct OpsInnerDraining {
    #[allow(dead_code)]
    id: usize,
    storage: [Option<OpState>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
}

enum TryGetSlotResult {
    GotSlot(UnsafeOpsSlotHandle),
    NoSlots(oneshot::Receiver<UnsafeOpsSlotHandle>),
    Draining,
}

impl Ops {
    fn try_get_slot(&mut self) -> TryGetSlotResult {
        let mut ops_inner_guard = self.inner.lock().unwrap();
        let open = match &mut *ops_inner_guard {
            OpsInner::Undefined => unreachable!(),
            OpsInner::Open(open) => open,
            OpsInner::Draining(_) => {
                return TryGetSlotResult::Draining;
            }
        };
        match open.unused_indices.pop() {
            Some(idx) => TryGetSlotResult::GotSlot({
                UnsafeOpsSlotHandle {
                    ops_weak: open.myself.clone(),
                    idx,
                }
            }),
            None => {
                let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                open.waiters.push_back(wake_up_tx);
                TryGetSlotResult::NoSlots(wake_up_rx)
            }
        }
    }

    #[tracing::instrument(skip_all, fields(system=%self.id, idx=%idx))]
    fn return_slot_and_wake(&mut self, idx: usize) {
        fn return_slot(op_state_ref: &mut Option<OpState>) {
            match op_state_ref {
                None => (),
                Some(some) => {
                    let op_state_inner_guard = some.0.lock().unwrap();
                    match &*op_state_inner_guard {
                        OpStateInner::Undefined => unreachable!(),
                        OpStateInner::Pending { .. }
                        | OpStateInner::PendingButFutureDropped { .. } => {
                            panic!("implementation error: potential memory unsafetiy: we must not return a slot that is still pending  {:?}", op_state_inner_guard.discriminant_str());
                        }
                        OpStateInner::Ready { .. } | OpStateInner::ReadyConsumed => {
                            drop(op_state_inner_guard);
                            *op_state_ref = None;
                        }
                    }
                }
            }
        }

        let mut inner_guard = self.inner.lock().unwrap();
        match &mut *inner_guard {
            OpsInner::Undefined => unreachable!(),
            OpsInner::Open(inner) => {
                return_slot(&mut inner.storage[idx]);
                while let Some(waiter) = inner.waiters.pop_front() {
                    match waiter.send(UnsafeOpsSlotHandle {
                        ops_weak: Weak::clone(&inner.myself),
                        idx,
                    }) {
                        Ok(()) => {
                            trace!("handed `idx` to a waiter");
                            return;
                        }
                        Err(_) => {
                            // the future requesting wakeup got dropped. wake up next one
                            continue;
                        }
                    }
                }
                trace!("no waiters, returning idx to unused_indices");
                inner.unused_indices.push(idx);
            }
            OpsInner::Draining(inner) => {
                return_slot(&mut inner.storage[idx]);
                trace!("draining, returning idx to unused_indices");
                inner.unused_indices.push(idx);
            }
        }
    }
}
