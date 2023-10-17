//! Structure to keep track of in-flight operations.
//!
//! [`Slots`] serves the following purposes:
//!
//! - Have a place to which we can transfer ownership of the resources (FD, buffer)
//!   if the future gets dropped while op is still in flight.
//! - Heep track of what ops are in flight so during system shutdown we know when we're done.
//! - Limit queue depth & provide means for a task to wait until it's the task's turn.
//!   The queue depth limit is currently hard-coded to [`crate::system::RING_SIZE`].
//!   The wait-until-it's-our-turn is implemented by the `tokio::sync::oneshot` returned by
//!   [`Slots::try_get_slot`].
//!
//!
//! There is one [`Slots`] instance per [`crate::System`].
//!
//! An in-flight io_uring operation occupies a slot in a [`Slots`] instance.
//!
//! The consumer of this module is [`crate::system::submission::op_fut::execute_op`].
//! The two important that it uses are:
//! - get the slot using [`Slots::try_get_slot`].
//! - use the slot (and submit the op to the kernel) using [`SlotHandle::use_for_op`]
//!
//! [`SlotHandle::use_for_op`] enforces correct ownership of the resources that the
//! io_uring operation operates on.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::poll_fn,
    sync::{Arc, Mutex, Weak},
    task::Poll,
};

use tokio::sync::oneshot;
use tracing::{debug, trace};

use crate::system::submission::op_fut::Error;

use super::{
    submission::op_fut::{Op, SystemError},
    RING_SIZE,
};

pub(super) mod co_owner {
    pub const SUBMIT_SIDE: usize = 0;
    pub const COMPLETION_SIDE: usize = 1;
    pub const POLLER: usize = 2;
    pub const NUM_CO_OWNERS: usize = 3;
}

/// See module-level comment [`crate::system::slots`].
pub(crate) struct Slots<const O: usize> {
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

struct SlotsInner {
    #[allow(dead_code)]
    id: usize,
    storage: [Option<Slot>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    co_owner_live: [bool; co_owner::NUM_CO_OWNERS],
    state: SlotsInnerState,
}

enum SlotsInnerState {
    Open {
        myself: SlotsWeak,
        // FIXME: this is a basic channel right? could be a tokio::sync::mpsc::channel(1) instead
        waiters: VecDeque<tokio::sync::oneshot::Sender<SlotHandle>>,
    },
    Draining,
}

pub(crate) struct SlotHandle {
    // FIXME: why is this weak?
    slots_weak: SlotsWeak,
    idx: usize,
}

enum Slot {
    Pending {
        waker: Option<std::task::Waker>, // None if it hasn't been polled yet
    },
    PendingButFutureDropped {
        /// When a future gets dropped while the Op is still running, it gets Box'ed  This is a Box'ed `ResourcesOwnedByKernel`
        _resources_owned_by_kernel: Box<dyn std::any::Any + Send>,
    },
    Ready {
        result: i32,
    },
}

pub(super) fn new(
    id: usize,
) -> (
    Slots<{ co_owner::SUBMIT_SIDE }>,
    Slots<{ co_owner::COMPLETION_SIDE }>,
    Slots<{ co_owner::POLLER }>,
) {
    let inner = Arc::new_cyclic(|inner_weak| {
        Mutex::new(SlotsInner {
            id,
            storage: {
                const NONE: Option<Slot> = None;
                [NONE; RING_SIZE as usize]
            },
            unused_indices: (0..RING_SIZE.try_into().unwrap()).collect(),
            co_owner_live: [false; co_owner::NUM_CO_OWNERS],
            state: SlotsInnerState::Open {
                waiters: VecDeque::new(),
                myself: SlotsWeak {
                    id,
                    inner_weak: inner_weak.clone(),
                },
            },
        })
    });
    fn make_co_owner<const O: usize>(inner: &Arc<Mutex<SlotsInner>>) -> Slots<O> {
        let mut guard = inner.lock().unwrap();
        guard.co_owner_live[O] = true;
        Slots {
            id: guard.id,
            inner: Arc::clone(inner),
        }
    }
    (
        make_co_owner::<{ co_owner::SUBMIT_SIDE }>(&inner),
        make_co_owner::<{ co_owner::COMPLETION_SIDE }>(&inner),
        make_co_owner::<{ co_owner::POLLER }>(&inner),
    )
}

impl<const O: usize> Drop for Slots<O> {
    fn drop(&mut self) {
        let lock_res = self.inner.lock();
        match lock_res {
            Ok(mut guard) => {
                guard.co_owner_live[O] = false;
            }
            Err(mut poison) => {
                let guard = poison.get_mut();
                guard.co_owner_live[O] = false;
            }
        }
    }
}

impl SlotsWeak {
    fn try_upgrade_mut<F, R>(&self, f: F) -> Result<R, ()>
    where
        F: FnOnce(&mut SlotsInner) -> R,
    {
        match Weak::upgrade(&self.inner_weak) {
            Some(inner_strong) => {
                let mut inner_guard = inner_strong.lock().unwrap();
                Ok(f(&mut inner_guard))
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
                    Slot::Pending { .. } | Slot::PendingButFutureDropped { .. } => {
                        panic!("implementation error: potential memory unsafety: we must not return a slot that is still pending  {:?}", slot_ref.discriminant_str());
                    }
                    Slot::Ready { .. } => {
                        *slot_storage_ref = None;
                    }
                },
            }
        }

        match &mut self.state {
            SlotsInnerState::Open { myself, waiters } => {
                clear_slot(&mut self.storage[idx]);
                while let Some(waiter) = waiters.pop_front() {
                    match waiter.send(SlotHandle {
                        slots_weak: myself.clone(),
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
                self.unused_indices.push(idx);
            }
            SlotsInnerState::Draining => {
                clear_slot(&mut self.storage[idx]);
                trace!("draining, returning idx to unused_indices");
                self.unused_indices.push(idx);
            }
        }
    }
}

impl<const O: usize> Slots<O> {
    pub(super) fn poller_timeout_debug_dump(&self) {
        let inner = self.inner.lock().unwrap();
        // TODO: only do this if some env var is set?
        let (storage, unused_indices) = (&inner.storage, &inner.unused_indices);
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

impl Slots<{ co_owner::COMPLETION_SIDE }> {
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

        let storage = &mut self.storage;
        let slot = &mut storage[idx];
        let slot = slot.as_mut().unwrap();
        match slot {
            Slot::Pending { waker } => {
                let waker = waker.take();
                *slot = Slot::Ready {
                    result: cqe.result(),
                };
                if let Some(waker) = waker {
                    trace!("waking up future");
                    waker.wake();
                }
            }
            Slot::PendingButFutureDropped {
                _resources_owned_by_kernel,
            } => {
                *slot = Slot::Ready {
                    result: cqe.result(),
                };
                self.return_slot(idx);
            }
            Slot::Ready { .. } => {
                unreachable!(
                    "completions only come in once: {:?}",
                    slot.discriminant_str()
                )
            }
        }
    }
}

impl Slots<{ co_owner::COMPLETION_SIDE }> {
    pub(super) fn transition_to_draining(&self) {
        let mut inner_guard = self.inner.lock().unwrap();
        match &mut inner_guard.state {
            SlotsInnerState::Open {
                myself: _,
                waiters: _,
            } => {
                // this assignment here drops `waiters`,
                // thereby making all of the op futures return with a shutdown error
                inner_guard.state = SlotsInnerState::Draining;
            }
            SlotsInnerState::Draining => {}
        }
    }
}

impl Slots<{ co_owner::COMPLETION_SIDE }> {
    pub(super) fn pending_slot_count(&self) -> usize {
        let ring_size = usize::try_from(RING_SIZE).unwrap();
        let inner_guard = self.inner.lock().unwrap();
        match inner_guard.state {
            SlotsInnerState::Open { .. } => {
                panic!("implementation error: must only call this method after set_draining")
            }
            SlotsInnerState::Draining => {
                ring_size - inner_guard.slots_owned_by_user_space().count()
            }
        }
    }
}

impl<const O: usize> Slots<O> {
    pub(super) fn shutdown_assertions(self) {
        let inner_guard = self.inner.lock().unwrap();
        match &inner_guard.state {
            SlotsInnerState::Open { .. } => panic!("we should be Draining by now"),
            SlotsInnerState::Draining => (),
        };
        let slots_owned_by_user_space = inner_guard
            .slots_owned_by_user_space()
            .collect::<HashSet<_>>();
        let unused_indices = inner_guard
            .unused_indices
            .iter()
            .cloned()
            .collect::<HashSet<usize>>();
        // at this time, all slots must be either in unused_indices (their state is None) or they must be in Ready state
        assert_eq!(
            inner_guard.slots_owned_by_user_space().count(),
            RING_SIZE.try_into().unwrap()
        );
        assert!(unused_indices.is_subset(&slots_owned_by_user_space));

        // assert the calling owner is the only remaining owner
        let mut expected_co_owner_live = [false; co_owner::NUM_CO_OWNERS];
        expected_co_owner_live[O] = true;
        assert_eq!(inner_guard.co_owner_live, expected_co_owner_live);
    }
}

pub(crate) enum TryGetSlotResult {
    GotSlot(SlotHandle),
    NoSlots(oneshot::Receiver<SlotHandle>),
    Draining,
}

impl Slots<{ co_owner::SUBMIT_SIDE }> {
    pub(crate) fn try_get_slot(&self) -> TryGetSlotResult {
        let mut inner_guard = self.inner.lock().unwrap();
        let inner = &mut *inner_guard;
        match &mut inner.state {
            SlotsInnerState::Draining => TryGetSlotResult::Draining,
            SlotsInnerState::Open { myself, waiters } => match inner.unused_indices.pop() {
                Some(idx) => TryGetSlotResult::GotSlot({
                    SlotHandle {
                        slots_weak: myself.clone(),
                        idx,
                    }
                }),
                None => {
                    let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                    waiters.push_back(wake_up_tx);
                    TryGetSlotResult::NoSlots(wake_up_rx)
                }
            },
        }
    }
}

impl SlotHandle {
    pub(crate) fn use_for_op<O, S>(
        self,
        mut op: O,
        do_submit: S,
    ) -> impl std::future::Future<Output = (O::Resources, Result<O::Success, Error<O::Error>>)>
    where
        O: Op + Send + 'static,
        S: FnOnce(io_uring::squeue::Entry),
    {
        let sqe = op.make_sqe();
        let sqe = sqe.user_data(u64::try_from(self.idx).unwrap());

        let res = self.slots_weak.try_upgrade_mut(|inner| match inner.state {
            SlotsInnerState::Open { .. } => {
                assert!(inner.storage[self.idx].is_none()); // TODO turn Option into tri-state for better semantics: NotTaken, SlotLive, Submitted
                inner.storage[self.idx] = Some(Slot::Pending { waker: None });
            }
            SlotsInnerState::Draining => {
                inner.return_slot(self.idx);
            }
        });
        let Ok(()) = res else {
            return futures::future::Either::Left(async move {
                (
                    op.on_failed_submission(),
                    Err(Error::<O::Error>::System(SystemError::SystemShuttingDown)),
                )
            });
        };

        do_submit(sqe);

        futures::future::Either::Right(self.wait_for_completion(op))
    }

    async fn wait_for_completion<O: Op + Send + 'static>(
        self,
        op: O,
    ) -> (O::Resources, Result<O::Success, Error<O::Error>>) {
        let slot = self;
        let op = std::sync::Mutex::new(Some(op));

        // If this future gets dropped _before_ the op completes, we need to make sure
        // that the resources owned by the kernel continue to live until the op completes.
        // Otherwise, the kernel will operate on the dropped resource. The most concerning
        // case are memory buffers which would be use-after-freed by the kernel. For example,
        // for a read uring op, the kernel could write into the buffer that has been freed and/or re-used.
        //
        // If this futures gets dropped _after_ the op completes but before this future
        // is poll()ed, we need to return the slot in addition to freeing the resources.
        scopeguard::defer! {
            if op.lock().unwrap().is_none() {
                // fast-path to avoid the try_upgrade_mut() call
                return;
            }
            let res = slot.slots_weak.try_upgrade_mut(|inner| {
                let Some(op) = op.lock().unwrap().take() else {
                    return;
                };
                let storage = &mut inner.storage;
                let slot_storage_mut = &mut storage[slot.idx];
                let slot_mut = slot_storage_mut.as_mut().unwrap();
                match &mut *slot_mut {
                    Slot::Pending { .. } => {
                        // The resource needs to be kept alive until the op completes.
                        // So, move it into the Slot.
                        // `process_completion` will drop the box and return the slot
                        // once it observes the completion.
                        *slot_mut = Slot::PendingButFutureDropped {
                            _resources_owned_by_kernel: Box::new(op),
                        };
                    }
                    Slot::Ready { result } => {
                        // The op completed and called the waker that would eventually cause this future to be polled
                        // and transition from Inflight to one of the Done states. But this future got dropped first.
                        // So, it's our job to drop the slot.
                        *slot_mut = Slot::Ready { result: *result };
                        inner.return_slot(slot.idx);
                        // SAFETY:
                        // The op is ready, hence the resources aren't onwed by the kernel anymore.
                        #[allow(unused_unsafe)]
                        unsafe {
                            drop(op);
                        }
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
                        let Some(op) = op.lock().unwrap().take() else {
                            return;
                        };
                        drop(op);
                    }
                }
            }
        };

        // Now that we've set up the scope guard, get to business.
        // Inspect the slot to check whether the poller task already processed the completion.
        // If it has, good for us.
        // If not, set up a oneshot to notify us. (TODO: in the hand-rolled futures, this was simply a std::task::Waker, now it's a oneshot.)
        enum InspectSlotResult {
            AlreadyDone(i32),
            NeedToWait,
            ShutDown,
        }
        let inspect_slot_res = poll_fn(|cx| {
            let inspect_slot_res = slot.slots_weak.try_upgrade_mut(move |inner| {
                let storage = &mut inner.storage;
                let slot_storage_ref = &mut storage[slot.idx];
                let slot_mut = slot_storage_ref.as_mut().unwrap();

                match &mut *slot_mut {
                    Slot::Pending { waker } => {
                        trace!("op is still pending, storing waker in it");
                        let waker_mut_ref = waker.get_or_insert_with(|| cx.waker().clone());
                        if !cx.waker().will_wake(waker_mut_ref) {
                            waker.replace(cx.waker().clone());
                        }
                        InspectSlotResult::NeedToWait
                    }
                    Slot::PendingButFutureDropped { .. } => {
                        unreachable!("if it's dropped, it's not pollable")
                    }
                    Slot::Ready { result: res } => {
                        trace!("op is ready, returning resources to user");
                        let res = *res;
                        inner.return_slot(slot.idx);
                        InspectSlotResult::AlreadyDone(res)
                    }
                }
            });
            let inspect_slot_res = match inspect_slot_res {
                Err(()) => InspectSlotResult::ShutDown,
                Ok(res) => res,
            };
            match inspect_slot_res {
                InspectSlotResult::NeedToWait => Poll::Pending,
                x => Poll::Ready(x),
            }
        })
        .await;
        let res: i32;
        let was_ready_on_first_poll: bool;
        match inspect_slot_res {
            InspectSlotResult::AlreadyDone(r) => {
                res = r;
                was_ready_on_first_poll = true;
            }
            InspectSlotResult::NeedToWait => {
                unreachable!()
            }
            InspectSlotResult::ShutDown => {
                // SAFETY:
                // This future has an outdated view of the system; it shut down in the meantime.
                // Shutdown makes sure that all inflight ops complete, so,
                // these resources are no longer owned by the kernel and can be returned as an error.
                #[allow(unused_unsafe)]
                unsafe {
                    let op = op.lock().unwrap().take().unwrap();
                    return (
                        op.on_failed_submission(),
                        Err(Error::System(SystemError::SystemShuttingDown)),
                    );
                }
            }
        };

        if was_ready_on_first_poll && *crate::env_tunables::YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL
        {
            tokio::task::yield_now().await;
        }

        // SAFETY:
        // We got a result, so, kernel is done with the operation and ownership is back with us.
        #[allow(unused_unsafe)]
        let (resources, res) = unsafe {
            let op = op.lock().unwrap().take().expect("we only take() it in drop(), and evidently drop() hasn't happened yet because we're executing a method on self");
            op.on_op_completion(res)
        };
        (resources, res.map_err(Error::Op))
    }
}

impl SlotsInner {
    pub(super) fn slots_owned_by_user_space(&self) -> impl Iterator<Item = usize> + '_ {
        self.storage
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                None => Some(idx),
                Some(slot_ref) => match slot_ref {
                    Slot::Pending { .. } => None,
                    Slot::PendingButFutureDropped { .. } => None,
                    Slot::Ready { .. } => Some(idx),
                },
            })
    }
}

impl Slot {
    pub(super) fn discriminant_str(&self) -> &'static str {
        match self {
            Slot::Pending { .. } => "Pending",
            Slot::PendingButFutureDropped { .. } => "PendingButFutureDropped",
            Slot::Ready { .. } => "Ready",
        }
    }
}
