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
use tracing::{trace, Level};

use crate::ResourcesOwnedByKernel;

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
}

impl OpStateInner {
    fn discriminant_str(&self) -> &'static str {
        match self {
            OpStateInner::Undefined => "Undefined",
            OpStateInner::Pending { .. } => "Pending",
            OpStateInner::PendingButFutureDropped { .. } => "PendingButFutureDropped",
            OpStateInner::Ready { .. } => "Ready",
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

pub(crate) enum TryGetSlotResult {
    GotSlot(UnsafeOpsSlotHandle),
    NoSlots(oneshot::Receiver<UnsafeOpsSlotHandle>),
    Draining,
}

impl Ops {
    pub(crate) fn try_get_slot(&mut self) -> TryGetSlotResult {
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

    #[tracing::instrument(skip_all, level=Level::TRACE, fields(system=%self.id, idx=%idx))]
    pub(crate) fn return_slot(&mut self, idx: usize) {
        fn clear_slot(op_state_ref: &mut Option<OpState>) {
            match op_state_ref {
                None => (),
                Some(some) => {
                    let op_state_inner_guard = some.0.lock().unwrap();
                    match &*op_state_inner_guard {
                        OpStateInner::Undefined => unreachable!(),
                        OpStateInner::Pending { .. }
                        | OpStateInner::PendingButFutureDropped { .. } => {
                            panic!("implementation error: potential memory unsafety: we must not return a slot that is still pending  {:?}", op_state_inner_guard.discriminant_str());
                        }
                        OpStateInner::Ready { .. } => {
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
                clear_slot(&mut inner.storage[idx]);
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
                clear_slot(&mut inner.storage[idx]);
                trace!("draining, returning idx to unused_indices");
                inner.unused_indices.push(idx);
            }
        }
    }
}

pub(crate) struct UnsafeOpsSlotHandle {
    pub(crate) ops_weak: Weak<Mutex<Ops>>,
    pub(crate) idx: usize,
}

#[derive(Debug, thiserror::Error)]
// TODO: all of these are the same cause, i.e., system shutdown
pub(crate) enum UnsafeOpsSlotHandleSubmitError {
    #[error("ops draining")]
    OpsDraining,
    #[error("ops dropped")]
    OpsDropped,
}

impl UnsafeOpsSlotHandle {
    pub(crate) fn claim<R>(
        self,
        rsrc: R,
    ) -> Result<InflightOpHandle<R>, (R, UnsafeOpsSlotHandleSubmitError)>
    where
        R: ResourcesOwnedByKernel + Send + 'static,
    {
        let ops = match Weak::upgrade(&self.ops_weak) {
            Some(ops) => ops,
            None => {
                return Err((rsrc, UnsafeOpsSlotHandleSubmitError::OpsDropped));
            }
        };
        let mut ops_guard = ops.lock().unwrap();
        let mut ops_inner_guard = ops_guard.inner.lock().unwrap();
        let ops_open = match &mut *ops_inner_guard {
            OpsInner::Undefined => unreachable!(),
            OpsInner::Open(open) => open,
            OpsInner::Draining(_) => {
                drop(ops_inner_guard);
                ops_guard.return_slot(self.idx);
                return Err((rsrc, UnsafeOpsSlotHandleSubmitError::OpsDraining));
            }
        };
        assert!(ops_open.storage[self.idx].is_none()); // TODO turn Option into tri-state for better semantics: NotTaken, SlotLive, Submitted
        ops_open.storage[self.idx] =
            Some(OpState(Mutex::new(OpStateInner::Pending { waker: None })));
        // drop mutexes, process_completions() below may need to grab it
        drop(ops_inner_guard);
        drop(ops_guard);

        Ok(InflightOpHandle {
            resources_owned_by_kernel: Some(rsrc),
            state: InflightOpHandleState::Inflight {
                slot: self,
                poll_count: 0,
            },
        })
    }
}

impl OpsInnerDraining {
    fn slots_owned_by_user_space(&self) -> impl Iterator<Item = usize> + '_ {
        self.storage
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                None => Some(idx),
                Some(op_state) => {
                    let op_state_inner = op_state.0.lock().unwrap();
                    match &*op_state_inner {
                        OpStateInner::Undefined => unreachable!(),
                        OpStateInner::Pending { .. } => None,
                        OpStateInner::PendingButFutureDropped { .. } => None,
                        OpStateInner::Ready { .. } => Some(idx),
                    }
                }
            })
    }
}

pub(crate) struct InflightOpHandle<R: ResourcesOwnedByKernel + Send + 'static> {
    resources_owned_by_kernel: Option<R>, // beocmes None in `drop()`, Some otherwise
    state: InflightOpHandleState,
}

enum InflightOpHandleState {
    Undefined,
    Inflight {
        slot: UnsafeOpsSlotHandle,
        poll_count: usize,
    },
    DoneButYieldingToExecutorForFairness {
        result: i32,
    },
    DoneAndPolled,
    Dropped,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InflightOpHandleError {
    #[error("ops dropped")]
    OpsDropped,
}

enum OwnedSlotPollResult {
    Ready(i32),
    Pending(UnsafeOpsSlotHandle),
    ShutDown,
}

impl UnsafeOpsSlotHandle {
    fn poll(self, cx: &mut std::task::Context<'_>) -> OwnedSlotPollResult {
        let ops = match Weak::upgrade(&self.ops_weak) {
            Some(ops) => ops,
            None => {
                return OwnedSlotPollResult::ShutDown;
            }
        };
        let mut ops_guard = ops.lock().unwrap();
        let mut ops_inner_guard = ops_guard.inner.lock().unwrap();
        let storage = match &mut *ops_inner_guard {
            OpsInner::Undefined => unreachable!(),
            OpsInner::Open(inner) => &mut inner.storage,
            OpsInner::Draining(inner) => &mut inner.storage,
        };
        let op_state = &mut storage[self.idx];
        let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();

        let cur: OpStateInner = std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
        match cur {
            OpStateInner::Undefined => panic!("future is in undefined state"),
            OpStateInner::Pending {
                waker: _, // don't recycle wakers, it may be from a different Context than the current `cx`
            } => {
                trace!("op is still pending, storing waker in it");
                *op_state_inner = OpStateInner::Pending {
                    waker: Some(cx.waker().clone()),
                };
                drop(op_state_inner);
                drop(ops_inner_guard);
                drop(ops_guard);
                return OwnedSlotPollResult::Pending(self);
            }
            OpStateInner::PendingButFutureDropped { .. } => {
                unreachable!("if it's dropped, it's not pollable")
            }
            OpStateInner::Ready { result: res } => {
                trace!("op is ready, returning resources to user");
                *op_state_inner = OpStateInner::Ready { result: res };
                drop(op_state_inner);
                drop(ops_inner_guard);
                ops_guard.return_slot(self.idx);
                return OwnedSlotPollResult::Ready(res);
            }
        }
    }
}

impl<R: ResourcesOwnedByKernel + Send + Unpin> std::future::Future for InflightOpHandle<R> {
    type Output = Result<R::OpResult, (R, InflightOpHandleError)>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let cur = std::mem::replace(&mut self.state, InflightOpHandleState::Undefined);
        match cur {
            InflightOpHandleState::Undefined => unreachable!("implementation error"),
            InflightOpHandleState::DoneAndPolled => {
                panic!("must not poll future after observing ready")
            }
            InflightOpHandleState::Dropped => unreachable!("implementation error"),
            InflightOpHandleState::Inflight { slot, poll_count } => {
                let res = match slot.poll(cx) {
                    OwnedSlotPollResult::Ready(res) => res,
                    OwnedSlotPollResult::Pending(slot) => {
                        self.state = InflightOpHandleState::Inflight {
                            slot,
                            poll_count: poll_count + 1,
                        };
                        return std::task::Poll::Pending;
                    }
                    OwnedSlotPollResult::ShutDown => {
                        self.state = InflightOpHandleState::DoneAndPolled;
                        // SAFETY:
                        // This future has an outdated view of the system; it shut down in the meantime.
                        // Shutdown makes sure that all inflight ops complete, so,
                        // these resources are no longer owned by the kernel and can be returned as an error.
                        #[allow(unused_unsafe)]
                        unsafe {
                            let resources_owned_by_kernel =
                                self.resources_owned_by_kernel.take().unwrap();
                            return std::task::Poll::Ready(Err((
                                resources_owned_by_kernel,
                                InflightOpHandleError::OpsDropped,
                            )));
                        }
                    }
                };

                let mut rsrc_mut = unsafe {
                    self.as_mut()
                        .map_unchecked_mut(|myself| &mut myself.resources_owned_by_kernel)
                };
                let rsrc = rsrc_mut.take().expect("we only take() it in drop(), and evidently drop() hasn't happened yet because we're executing a method on self");
                drop(rsrc_mut);

                lazy_static::lazy_static! {
                    static ref YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL: bool =
                        std::env::var("YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL")
                            .map(|v| v == "1")
                            .unwrap_or_else(|e| match e {
                                std::env::VarError::NotPresent => false,
                                std::env::VarError::NotUnicode(_) => panic!("YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL must be a unicode string"),
                            });
                }

                if poll_count == 0 && *YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL {
                    let fut = tokio::task::yield_now();
                    tokio::pin!(fut);
                    match fut.poll(cx) {
                        std::task::Poll::Pending => {
                            self.state =
                                InflightOpHandleState::DoneButYieldingToExecutorForFairness {
                                    result: res,
                                };
                            let replaced = self.resources_owned_by_kernel.replace(rsrc);
                            assert!(replaced.is_none(), "we just took it above");
                            return std::task::Poll::Pending;
                        }
                        std::task::Poll::Ready(()) => {
                            // fallthrough
                        }
                    }
                }
                self.state = InflightOpHandleState::DoneAndPolled;
                return std::task::Poll::Ready(Ok(rsrc.on_op_completion(res)));
            }
            InflightOpHandleState::DoneButYieldingToExecutorForFairness { result } => {
                self.state = InflightOpHandleState::DoneAndPolled;
                return std::task::Poll::Ready(Ok(self
                    .resources_owned_by_kernel
                    .take()
                    .unwrap()
                    .on_op_completion(result)));
            }
        }
    }
}

impl<R> Drop for InflightOpHandle<R>
where
    R: ResourcesOwnedByKernel + Send + 'static,
{
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, InflightOpHandleState::Dropped);
        match cur {
            InflightOpHandleState::Undefined => unreachable!("future is in undefined state"),
            InflightOpHandleState::Dropped => {
                unreachable!("future is in dropped state, but we're in drop() right now")
            }
            InflightOpHandleState::DoneAndPolled => (),
            InflightOpHandleState::DoneButYieldingToExecutorForFairness { .. } => (),
            InflightOpHandleState::Inflight {
                slot,
                poll_count: _,
            } => {
                let ops = match Weak::upgrade(&slot.ops_weak) {
                    Some(ops) => ops,
                    None => {
                        // SAFETY:
                        // This future has an outdated view of the system; it shut down in the meantime.
                        // Shutdown makes sure that all inflight ops complete, so, it is safe to drop the resources owned by kernel at this point.
                        #[allow(unused_unsafe)]
                        unsafe {
                            let resources_owned_by_kernel = self.resources_owned_by_kernel.take();
                            drop(resources_owned_by_kernel);
                        }
                        return;
                    }
                };
                let mut ops_guard = ops.lock().unwrap();
                let mut ops_inner_guard = ops_guard.inner.lock().unwrap();
                let storage = match &mut *ops_inner_guard {
                    OpsInner::Undefined => unreachable!(),
                    OpsInner::Open(inner) => &mut inner.storage,
                    OpsInner::Draining(inner) => &mut inner.storage,
                };
                let op_state = &mut storage[slot.idx];
                let mut op_state_inner = op_state.as_ref().unwrap().0.lock().unwrap();
                let cur = std::mem::replace(&mut *op_state_inner, OpStateInner::Undefined);
                match cur {
                    OpStateInner::Undefined => unreachable!("implementation error"),
                    OpStateInner::Pending { .. } => {
                        // Up until now, Self held the resources that the uring op is operating on.
                        // Now Self is getting dropped, but the uring op is still ongoing.
                        // We must prevent the resources from getting dropped, otherwise the kernel will operate on the dropped resource.
                        // NB: the most concerning resource is the memory buffer into which a read-style uring op will write / from which a write-style uring will read.
                        let rsrc = self
                            .resources_owned_by_kernel
                            .take()
                            .expect("we only take() during drop, which is here");
                        // Use Box for type erasure.
                        // Type erasure is necessary because the ResourcesOwnedByKernel trait has an associated type "OpResult",
                        // and we don't want the system to be generic over it.
                        // Since dropping of inflight IOs is generally rare, the allocations should be fine.
                        // Could optimize by making erause a trait method on ResourcesOwnedByKernel; it could then use a slab allcoator or similar.
                        let rsrc: Box<dyn std::any::Any + Send> = Box::new(rsrc);
                        *op_state_inner = OpStateInner::PendingButFutureDropped {
                            resources_owned_by_kernel: rsrc,
                        };
                        drop(op_state_inner);
                    }
                    OpStateInner::Ready { result: _ } => {
                        // The op completed and called the waker that would eventually cause this future to be polled
                        // and transition from Inflight to one of the Done states. But this future got dropped first.
                        // So, it's our job to drop the slot.
                        drop(op_state_inner);
                        drop(ops_inner_guard);
                        ops_guard.return_slot(slot.idx);
                    }
                    OpStateInner::PendingButFutureDropped { .. } => {
                        unreachable!("above is the only transition into this state, and this function only runs once")
                    }
                }
            }
        }
    }
}
