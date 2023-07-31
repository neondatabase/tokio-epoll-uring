//! Structure to keep track of in-flight operations.
//!
//! There is one [`Slots`] instance per [`crate::System`].
//!
//! An in-flight io_uring operation occupies a slot in a [`Slots`] instance.
//!
//! # Usage
//!
//! The consumer of this module is [`crate::ops::OpFut::new`].
//! The role that this module plays in the op submission process:
//!
//! 1. Consumer gets a handle to a slot using [`Slots::try_get_slot`].
//! 2. Consumer uses the slot, getting back an [`InflightHandle`].
//! 3. Consumer submits the io_uring op to the [`SubmissionSide`].
//! 4. Consumer `await`s the the [`InflightHandle`].
//!
//! The [`InflightHandle`] future that enforces correct
//! ownership of the resources that the io_uring operation operates on.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::oneshot;
use tracing::{debug, trace};

use super::{submission::op_fut::Op, RING_SIZE};

pub(crate) trait SlotsCoOwner {}

pub(crate) struct CoOwnerSubmitSide;
impl SlotsCoOwner for CoOwnerSubmitSide {}
pub(crate) struct CoOwnerCompletionSide;
impl SlotsCoOwner for CoOwnerCompletionSide {}

pub(crate) struct CoOwnerPoller;
impl SlotsCoOwner for CoOwnerPoller {}

pub(crate) struct Slots<O: SlotsCoOwner> {
    _marker: std::marker::PhantomData<O>,
    #[allow(dead_code)]
    id: usize,
    inner: Arc<Mutex<SlotsInner>>,
}

#[derive(Clone)]
pub(crate) struct SlotsWeak {
    #[allow(dead_code)]
    id: usize,
    inner_weak: Weak<Mutex<SlotsInner>>,
}

enum SlotsInner {
    Open(Box<SlotsInnerOpen>),
    Draining(Box<SlotsInnerDraining>),
    Undefined,
}

struct SlotsInnerOpen {
    id: usize,
    storage: [Option<Slot>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    waiters: VecDeque<tokio::sync::oneshot::Sender<SlotHandle>>,
    myself: SlotsWeak,
}

struct SlotsInnerDraining {
    #[allow(dead_code)]
    id: usize,
    storage: [Option<Slot>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
}

pub(crate) struct SlotHandle {
    slots_weak: SlotsWeak,
    idx: usize,
}

enum Slot {
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

pub(super) fn new(
    id: usize,
) -> (
    Slots<CoOwnerSubmitSide>,
    Slots<CoOwnerCompletionSide>,
    Slots<CoOwnerPoller>,
) {
    let inner = Arc::new_cyclic(|inner_weak| {
        Mutex::new(SlotsInner::Open(Box::new(SlotsInnerOpen {
            id,
            storage: array_macro::array![_ => None; RING_SIZE as usize],
            unused_indices: (0..RING_SIZE.try_into().unwrap()).collect(),
            waiters: VecDeque::new(),
            myself: SlotsWeak {
                id,
                inner_weak: inner_weak.clone(),
            },
        })))
    });
    (
        Slots {
            _marker: std::marker::PhantomData,
            id,
            inner: Arc::clone(&inner),
        },
        Slots {
            _marker: std::marker::PhantomData,
            id,
            inner: Arc::clone(&inner),
        },
        Slots {
            _marker: std::marker::PhantomData,
            id,
            inner: Arc::clone(&inner),
        },
    )
}

impl SlotsWeak {
    fn try_upgrade_mut<F, R>(&self, f: F) -> Result<R, ()>
    where
        F: FnOnce(&mut SlotsInner) -> R,
    {
        match Weak::upgrade(&self.inner_weak) {
            Some(inner_strong) => {
                let mut inner_guard = inner_strong.lock().unwrap();
                Ok(f(&mut *inner_guard))
            }
            None => Err(()),
        }
    }
}

impl SlotsInner {
    fn return_slot(&mut self, idx: usize) {
        fn clear_slot(slot_storage_ref: &mut Option<Slot>) {
            match slot_storage_ref {
                None => (),
                Some(slot_ref) => match slot_ref {
                    Slot::Undefined => unreachable!(),
                    Slot::Pending { .. } | Slot::PendingButFutureDropped { .. } => {
                        panic!("implementation error: potential memory unsafety: we must not return a slot that is still pending  {:?}", slot_ref.discriminant_str());
                    }
                    Slot::Ready { .. } => {
                        *slot_storage_ref = None;
                    }
                },
            }
        }

        match self {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(inner) => {
                clear_slot(&mut inner.storage[idx]);
                while let Some(waiter) = inner.waiters.pop_front() {
                    match waiter.send(SlotHandle {
                        slots_weak: inner.myself.clone(),
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
            SlotsInner::Draining(inner) => {
                clear_slot(&mut inner.storage[idx]);
                trace!("draining, returning idx to unused_indices");
                inner.unused_indices.push(idx);
            }
        }
    }
}

impl<O: SlotsCoOwner> Slots<O> {
    pub(super) fn poller_timeout_debug_dump(&self) {
        let inner = self.inner.lock().unwrap();
        // TODO: only do this if some env var is set?
        let (storage, unused_indices) = match &*inner {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(inner) => (&inner.storage, &inner.unused_indices),
            SlotsInner::Draining(inner) => (&inner.storage, &inner.unused_indices),
        };
        let mut by_state_discr = HashMap::new();
        for s in storage {
            match s {
                Some(slot) => {
                    let discr = slot.discriminant_str();
                    by_state_discr
                        .entry(discr)
                        .and_modify(|v| *v += 1)
                        .or_insert(1);
                }
                None => {
                    by_state_discr
                        .entry("None")
                        .and_modify(|v| *v += 1)
                        .or_insert(1);
                }
            }
        }
        debug!(
            "poller task got timeout: free slots = {} by state: {:?}",
            unused_indices.len(),
            by_state_discr
        );
    }
}

impl Slots<CoOwnerCompletionSide> {
    pub(super) fn process_completions(
        &mut self,
        cqes: impl Iterator<Item = io_uring::cqueue::Entry>,
    ) {
        let mut inner_guard = self.inner.lock().unwrap();
        for cqe in cqes {
            inner_guard.process_completion(cqe);
        }
    }
}

impl SlotsInner {
    fn process_completion(&mut self, cqe: io_uring::cqueue::Entry) {
        let idx: u64 = cqe.user_data();
        let idx = usize::try_from(idx).unwrap();

        let storage = match self {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(inner) => &mut inner.storage,
            SlotsInner::Draining(inner) => &mut inner.storage,
        };
        let slot = &mut storage[usize::try_from(idx).unwrap()];
        let slot = slot.as_mut().unwrap();
        let cur = std::mem::replace(&mut *slot, Slot::Undefined);
        match cur {
            Slot::Undefined => unreachable!("implementation error"),
            Slot::Pending { waker } => {
                *slot = Slot::Ready {
                    result: cqe.result(),
                };
                if let Some(waker) = waker {
                    trace!("waking up future");
                    waker.wake();
                }
            }
            Slot::PendingButFutureDropped {
                resources_owned_by_kernel,
            } => {
                drop(resources_owned_by_kernel);
                *slot = Slot::Ready {
                    result: cqe.result(),
                };
                self.return_slot(idx as usize);
            }
            Slot::Ready { .. } => {
                unreachable!(
                    "completions only come in once: {:?}",
                    cur.discriminant_str()
                )
            }
        }
    }
}

impl Slots<CoOwnerSubmitSide> {
    pub(super) fn set_draining(&self) {
        let mut inner_guard = self.inner.lock().unwrap();
        let cur = std::mem::replace(&mut *inner_guard, SlotsInner::Undefined);
        match cur {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(open) => {
                let SlotsInnerOpen {
                    id,
                    storage,
                    unused_indices,
                    waiters: _, // cancels all waiters
                    myself: _,
                } = *open;
                *inner_guard = SlotsInner::Draining(Box::new(SlotsInnerDraining {
                    id,
                    storage,
                    unused_indices,
                }));
            }
            SlotsInner::Draining(_draining) => {
                panic!("implementation error: must only call set_draining once")
            }
        }
    }
}

impl Slots<CoOwnerCompletionSide> {
    pub(super) fn pending_slot_count(&self) -> usize {
        let ring_size = usize::try_from(RING_SIZE).unwrap();
        let mut inner_guard = self.inner.lock().unwrap();
        let cur = std::mem::replace(&mut *inner_guard, SlotsInner::Undefined);
        match cur {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(_open) => {
                panic!("implementation error: must only call this method after set_draining")
            }
            SlotsInner::Draining(draining) => {
                let pending_count = ring_size - draining.slots_owned_by_user_space().count();
                *inner_guard = SlotsInner::Draining(draining);
                return pending_count;
            }
        }
    }
}

impl<O: SlotsCoOwner> Slots<O> {
    pub(super) fn shutdown_assertions(&self) {
        let ops_inner_guard = self.inner.lock().unwrap();
        let inner = match &*ops_inner_guard {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(_open) => panic!("we should be Draining by now"),
            SlotsInner::Draining(inner) => inner,
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
    }
}

pub(crate) enum TryGetSlotResult {
    GotSlot(SlotHandle),
    NoSlots(oneshot::Receiver<SlotHandle>),
    Draining,
}

impl Slots<CoOwnerSubmitSide> {
    pub(crate) fn try_get_slot(&self) -> TryGetSlotResult {
        let mut inner_guard = self.inner.lock().unwrap();
        let open = match &mut *inner_guard {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(open) => open,
            SlotsInner::Draining(_) => {
                return TryGetSlotResult::Draining;
            }
        };
        match open.unused_indices.pop() {
            Some(idx) => TryGetSlotResult::GotSlot({
                SlotHandle {
                    slots_weak: open.myself.clone(),
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
}

#[derive(Debug, thiserror::Error)]
// TODO: all of these are the same cause, i.e., system shutdown
pub(crate) enum UseError {
    #[error("slots dropped, the handle was stale, system is likely shutting/shut down")]
    SlotsDropped,
}

impl SlotHandle {
    pub(crate) fn use_for_op<O>(
        self,
        mut op: O,
    ) -> Result<(io_uring::squeue::Entry, InflightHandle<O>), (O, UseError)>
    where
        O: Op + Send + 'static,
    {
        let sqe = op.make_sqe();
        let sqe = sqe.user_data(u64::try_from(self.idx).unwrap());

        let res = self.slots_weak.try_upgrade_mut(|inner| match inner {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(open) => {
                assert!(open.storage[self.idx].is_none()); // TODO turn Option into tri-state for better semantics: NotTaken, SlotLive, Submitted
                open.storage[self.idx] = Some(Slot::Pending { waker: None });
            }
            SlotsInner::Draining(_) => {
                inner.return_slot(self.idx);
            }
        });
        match res {
            Err(()) => {
                return Err((op, UseError::SlotsDropped));
            }
            Ok(()) => (),
        };
        Ok((
            sqe,
            InflightHandle {
                resources_owned_by_kernel: Some(op),
                state: InflightHandleState::Inflight {
                    slot: self,
                    poll_count: 0,
                },
            },
        ))
    }
}

impl SlotsInnerDraining {
    pub(super) fn slots_owned_by_user_space(&self) -> impl Iterator<Item = usize> + '_ {
        self.storage
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                None => Some(idx),
                Some(slot_ref) => match slot_ref {
                    Slot::Undefined => unreachable!(),
                    Slot::Pending { .. } => None,
                    Slot::PendingButFutureDropped { .. } => None,
                    Slot::Ready { .. } => Some(idx),
                },
            })
    }
}

pub(crate) struct InflightHandle<O: Op + Send + 'static> {
    // TODO: misnomer
    resources_owned_by_kernel: Option<O>, // beocmes None in `drop()`, Some otherwise
    state: InflightHandleState,
}

enum InflightHandleState {
    Undefined,
    Inflight { slot: SlotHandle, poll_count: usize },
    DoneButYieldingToExecutorForFairness { result: i32 },
    DoneAndPolled,
    Dropped,
}

pub(crate) enum InflightHandleError<O: Op> {
    SlotsDropped,
    Completion(O::Error),
}

impl<O: Op + Send + Unpin> std::future::Future for InflightHandle<O> {
    type Output = (O::Resources, Result<O::Success, InflightHandleError<O>>);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let cur = std::mem::replace(&mut self.state, InflightHandleState::Undefined);
        match cur {
            InflightHandleState::Undefined => unreachable!("implementation error"),
            InflightHandleState::DoneAndPolled => {
                panic!("must not poll future after observing ready")
            }
            InflightHandleState::Dropped => unreachable!("implementation error"),
            InflightHandleState::Inflight { slot, poll_count } => {
                let res = match slot.poll_inflight(cx) {
                    InflightSlotPollResult::Ready(res) => res,
                    InflightSlotPollResult::Pending(slot) => {
                        self.state = InflightHandleState::Inflight {
                            slot,
                            poll_count: poll_count + 1,
                        };
                        return std::task::Poll::Pending;
                    }
                    InflightSlotPollResult::ShutDown => {
                        self.state = InflightHandleState::DoneAndPolled;
                        // SAFETY:
                        // This future has an outdated view of the system; it shut down in the meantime.
                        // Shutdown makes sure that all inflight ops complete, so,
                        // these resources are no longer owned by the kernel and can be returned as an error.
                        #[allow(unused_unsafe)]
                        unsafe {
                            let resources_owned_by_kernel =
                                self.resources_owned_by_kernel.take().unwrap();
                            return std::task::Poll::Ready((
                                resources_owned_by_kernel.on_failed_submission(),
                                Err(InflightHandleError::SlotsDropped),
                            ));
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
                                InflightHandleState::DoneButYieldingToExecutorForFairness {
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
                self.state = InflightHandleState::DoneAndPolled;
                let (resources, res) = rsrc.on_op_completion(res);
                return std::task::Poll::Ready((
                    resources,
                    res.map_err(InflightHandleError::Completion),
                ));
            }
            InflightHandleState::DoneButYieldingToExecutorForFairness { result } => {
                self.state = InflightHandleState::DoneAndPolled;
                let rsrc = self.resources_owned_by_kernel.take().unwrap();
                let (resources, res) = rsrc.on_op_completion(result);
                return std::task::Poll::Ready((
                    resources,
                    res.map_err(InflightHandleError::Completion),
                ));
            }
        }
    }
}

enum InflightSlotPollResult {
    Ready(i32),
    Pending(SlotHandle),
    ShutDown,
}

impl SlotHandle {
    fn poll_inflight(self, cx: &mut std::task::Context<'_>) -> InflightSlotPollResult {
        let slots_weak = self.slots_weak.clone();
        let res = slots_weak.try_upgrade_mut(move |inner| {
            let storage = match inner {
                SlotsInner::Undefined => unreachable!(),
                SlotsInner::Open(inner) => &mut inner.storage,
                SlotsInner::Draining(inner) => &mut inner.storage,
            };

            let slot_storage_ref = &mut storage[self.idx];
            let slot_mut = slot_storage_ref.as_mut().unwrap();

            let cur: Slot = std::mem::replace(&mut *slot_mut, Slot::Undefined);
            match cur {
                Slot::Undefined => panic!("future is in undefined state"),
                Slot::Pending {
                    waker: _, // don't recycle wakers, it may be from a different Context than the current `cx`
                } => {
                    trace!("op is still pending, storing waker in it");
                    *slot_mut = Slot::Pending {
                        waker: Some(cx.waker().clone()),
                    };
                    return InflightSlotPollResult::Pending(self);
                }
                Slot::PendingButFutureDropped { .. } => {
                    unreachable!("if it's dropped, it's not pollable")
                }
                Slot::Ready { result: res } => {
                    trace!("op is ready, returning resources to user");
                    *slot_mut = Slot::Ready { result: res };
                    inner.return_slot(self.idx);
                    return InflightSlotPollResult::Ready(res);
                }
            }
        });
        match res {
            Err(()) => {
                return InflightSlotPollResult::ShutDown;
            }
            Ok(res) => res,
        }
    }
}

// SAFETY:
// If the future gets dropped, we must ensure that the resources (memory, first and foremost)
// stay alive until we get the operation's cqe. Otherwise we risk memory corruption by hands of the kernel.
impl<O> Drop for InflightHandle<O>
where
    O: Op + Send + 'static,
{
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, InflightHandleState::Dropped);
        match cur {
            InflightHandleState::Undefined => unreachable!("future is in undefined state"),
            InflightHandleState::Dropped => {
                unreachable!("future is in dropped state, but we're in drop() right now")
            }
            InflightHandleState::DoneAndPolled => (),
            InflightHandleState::DoneButYieldingToExecutorForFairness { .. } => (),
            InflightHandleState::Inflight {
                slot,
                poll_count: _,
            } => {
                let res = slot.slots_weak.try_upgrade_mut(|inner| {
                    let storage = match inner {
                        SlotsInner::Undefined => unreachable!(),
                        SlotsInner::Open(inner) => &mut inner.storage,
                        SlotsInner::Draining(inner) => &mut inner.storage,
                    };
                    let slot_storage_mut = &mut storage[slot.idx];
                    let slot_mut = slot_storage_mut.as_mut().unwrap();
                    let cur = std::mem::replace(&mut *slot_mut, Slot::Undefined);
                    match cur {
                        Slot::Undefined => unreachable!("implementation error"),
                        Slot::Pending { .. } => {
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
                            *slot_mut = Slot::PendingButFutureDropped {
                                resources_owned_by_kernel: rsrc,
                            };
                        }
                        Slot::Ready { result } => {
                            // The op completed and called the waker that would eventually cause this future to be polled
                            // and transition from Inflight to one of the Done states. But this future got dropped first.
                            // So, it's our job to drop the slot.
                            *slot_mut = Slot::Ready { result };
                            inner.return_slot(slot.idx);
                        }
                        Slot::PendingButFutureDropped { .. } => {
                            unreachable!("above is the only transition into this state, and this function only runs once")
                        }
                    }
                });
                match res {
                    Ok(()) => (),
                    Err(()) => {
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
                }
            }
        }
    }
}

impl Slot {
    pub(super) fn discriminant_str(&self) -> &'static str {
        match self {
            Slot::Undefined => "Undefined",
            Slot::Pending { .. } => "Pending",
            Slot::PendingButFutureDropped { .. } => "PendingButFutureDropped",
            Slot::Ready { .. } => "Ready",
        }
    }
}
