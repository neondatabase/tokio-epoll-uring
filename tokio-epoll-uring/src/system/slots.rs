//! Structure to keep track of in-flight operations.
//!
//! [`Slots`] serves the following purposes:
//!
//! - Own each submitted operation and its resources (FD, buffer) until the CQE
//!   is processed, independently of the lifetime of the operation future.
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
//! [`SlotHandle::use_for_op`] moves the operation into the slot before submission.
//! Completion sends the typed output to the future through a one-shot channel;
//! if the future was dropped, the output is dropped by the completion side.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::{poll_fn, Future},
    pin::pin,
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::oneshot;
use tracing::{debug, trace};
use uring_common::io_uring;

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
    #[cfg(test)]
    testing: SlotsTesting,
}

#[cfg(test)]
pub(crate) struct SlotsTesting {
    pub(crate) test_on_wake: Box<
        dyn Send
            + Sync
            + Fn() -> Option<tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>>,
    >,
}

#[cfg(not(test))]
#[derive(Default)]
pub(crate) struct SlotsTesting;

#[cfg(test)]
impl Default for SlotsTesting {
    fn default() -> Self {
        Self {
            test_on_wake: Box::new(|| None),
        }
    }
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
    // Some while this handle represents an unconsumed slot reservation. Once
    // an operation is installed in storage, ownership moves to the slot and
    // this is set to None.
    idx: Option<usize>,
    #[cfg(test)]
    test_on_wake:
        std::sync::Mutex<Option<tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>>>,
}

enum Slot {
    Pending {
        completion: Box<dyn PendingCompletion>,
    },
}

trait PendingCompletion: Send + 'static {
    fn complete(self: Box<Self>, res: i32);
}

struct PendingCompletionImpl<O: Op> {
    op: O,
    result_tx: oneshot::Sender<UseForOpOutput<O>>,
}

impl<O: Op> PendingCompletion for PendingCompletionImpl<O> {
    fn complete(self: Box<Self>, res: i32) {
        let PendingCompletionImpl { op, result_tx } = *self;
        let (resources, result) = op.on_op_completion(res);
        // If the operation future was cancelled, sending returns ownership of
        // the normal completion output. Dropping it performs the same cleanup
        // as dropping a successfully returned value (notably OwnedFd::drop).
        drop(result_tx.send((resources, result.map_err(Error::Op))));
    }
}

pub(super) fn new(
    id: usize,
    #[allow(unused_variables)] testing: SlotsTesting,
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
            #[cfg(test)]
            testing,
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
                Some(slot_ref) => panic!(
                    "implementation error: potential memory unsafety: we must not return a slot that is still pending  {:?}",
                    slot_ref.discriminant_str()
                ),
            }
        }
        match &mut self.state {
            SlotsInnerState::Open { myself, waiters } => {
                clear_slot(&mut self.storage[idx]);
                while let Some(waiter) = waiters.pop_front() {
                    match waiter.send(SlotHandle {
                        slots_weak: myself.clone(),
                        idx: Some(idx),
                        #[cfg(test)]
                        test_on_wake: Mutex::new((self.testing.test_on_wake)()),
                    }) {
                        Ok(()) => {
                            trace!("handed `idx` to a waiter");
                            return;
                        }
                        Err(mut rejected) => {
                            // We still own `idx` and will offer it to the next
                            // waiter. Disarm the rejected handle before it is
                            // dropped while the slots mutex is held.
                            rejected.idx.take();
                            // the future requesting wakeup got dropped. wake up next one
                            continue;
                        }
                    }
                }
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
        debug!(
            "poller task got timeout: free slots = {} by state: {state:?}",
            unused_indices.len(),
            state = {
                // Note: This non-trivial piece of code is inside the debug! macro so that it
                // doesn't get executed when tracing level is set to ignore debug events. If
                // you want to move it out, use tracing::enabled to still avoid the overhead.
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
                by_state_discr
            }
        );
    }
}

impl Slots<{ co_owner::COMPLETION_SIDE }> {
    pub(super) fn process_completions(
        &mut self,
        cqes: impl Iterator<Item = io_uring::cqueue::Entry>,
    ) {
        for cqe in cqes {
            let (completion, res) = {
                let mut inner_guard = self.inner.lock().unwrap();
                inner_guard.take_completion(cqe)
            };
            // Do operation-specific completion and drop user resources without
            // holding the slots mutex. Destructors are allowed to call back
            // into the system.
            completion.complete(res);
        }
    }
}

impl SlotsInner {
    fn take_completion(
        &mut self,
        cqe: io_uring::cqueue::Entry,
    ) -> (Box<dyn PendingCompletion>, i32) {
        let idx: u64 = cqe.user_data();
        let idx = usize::try_from(idx).unwrap();
        let Slot::Pending { completion } = self.storage[idx]
            .take()
            .expect("completion must refer to a submitted operation");
        self.return_slot(idx);
        (completion, cqe.result())
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
        // Once every CQE has been processed, all slots have been returned.
        assert_eq!(
            inner_guard.slots_owned_by_user_space().count(),
            RING_SIZE.try_into().unwrap()
        );
        assert_eq!(unused_indices, slots_owned_by_user_space);

        // assert the calling owner is the only remaining owner
        let mut expected_co_owner_live = [false; co_owner::NUM_CO_OWNERS];
        expected_co_owner_live[O] = true;
        assert_eq!(inner_guard.co_owner_live, expected_co_owner_live);
    }
}

pub(crate) enum TryGetSlotResult {
    GotSlot {
        slot: SlotHandle,
        queue_depth: u64,
    },
    NoSlots {
        later: oneshot::Receiver<SlotHandle>,
        queue_depth: u64,
    },
    Draining,
}

impl Slots<{ co_owner::SUBMIT_SIDE }> {
    pub(crate) fn try_get_slot(&self) -> TryGetSlotResult {
        let mut inner_guard = self.inner.lock().unwrap();
        let inner = &mut *inner_guard;
        match &mut inner.state {
            SlotsInnerState::Draining => TryGetSlotResult::Draining,
            SlotsInnerState::Open { myself, waiters } => {
                let num_in_use_slots = RING_SIZE as u64 - inner.unused_indices.len() as u64;
                match inner.unused_indices.pop() {
                    Some(idx) => TryGetSlotResult::GotSlot {
                        slot: SlotHandle {
                            slots_weak: myself.clone(),
                            idx: Some(idx),
                            #[cfg(test)]
                            test_on_wake: Mutex::new((inner.testing.test_on_wake)()),
                        },
                        queue_depth: num_in_use_slots,
                    },
                    None => {
                        let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                        let num_waiters = waiters.len() as u64;
                        waiters.push_back(wake_up_tx);
                        TryGetSlotResult::NoSlots {
                            later: wake_up_rx,
                            queue_depth: num_in_use_slots + num_waiters,
                        }
                    }
                }
            }
        }
    }
}

type UseForOpOutput<O> = (
    <O as Op>::Resources,
    Result<<O as Op>::Success, Error<<O as Op>::Error>>,
);

impl SlotHandle {
    pub(crate) fn use_for_op<O, S>(
        mut self,
        mut op: O,
        do_submit: S,
    ) -> impl std::future::Future<Output = UseForOpOutput<O>>
    where
        O: Op + Send + 'static,
        S: FnOnce(io_uring::squeue::Entry),
    {
        let idx = self.idx.expect("slot reservation must be live");
        let sqe = op.make_sqe();
        let sqe = sqe.user_data(u64::try_from(idx).unwrap());

        let (result_tx, result_rx) = oneshot::channel();
        let mut pending = Some(PendingCompletionImpl { op, result_tx });

        let res = self.slots_weak.try_upgrade_mut(|inner| match inner.state {
            SlotsInnerState::Open { .. } => {
                assert!(inner.storage[idx].is_none()); // TODO turn Option into tri-state for better semantics: NotTaken, SlotLive, Submitted
                inner.storage[idx] = Some(Slot::Pending {
                    completion: Box::new(pending.take().unwrap()),
                });
            }
            SlotsInnerState::Draining => {}
        });
        if let Some(PendingCompletionImpl { op, .. }) = pending.take() {
            // Either the slots allocation has already disappeared, or the
            // system entered Draining after handing this SlotHandle out.
            return futures::future::Either::Left(async move {
                (
                    op.on_failed_submission(),
                    Err(Error::<O::Error>::System(SystemError::SystemShuttingDown)),
                )
            });
        }
        debug_assert!(res.is_ok());
        // The slot now owns the operation and will return the index on CQE.
        self.idx.take();

        do_submit(sqe);

        futures::future::Either::Right(self.wait_for_completion::<O>(result_rx))
    }

    async fn wait_for_completion<O: Op + Send + 'static>(
        self,
        result_rx: oneshot::Receiver<UseForOpOutput<O>>,
    ) -> (O::Resources, Result<O::Success, Error<O::Error>>) {
        let mut result_rx = pin!(result_rx);
        let mut poll_count = 0;
        let poll_res = poll_fn(|cx| {
            poll_count += 1;
            result_rx.as_mut().poll(cx)
        })
        .await
        .expect("the slot must complete every successfully submitted operation");
        assert!(poll_count >= 1);
        #[cfg(test)]
        {
            let on_wake = { self.test_on_wake.lock().unwrap().take() };
            if let Some(on_wake) = on_wake {
                let (tx, rx) = tokio::sync::oneshot::channel();
                on_wake.send(tx).unwrap();
                rx.await.unwrap();
            }
        }
        if poll_count == 1 && *crate::env_tunables::YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL {
            tokio::task::yield_now().await;
        }
        poll_res
    }
}

impl Drop for SlotHandle {
    fn drop(&mut self) {
        let Some(idx) = self.idx.take() else {
            return;
        };
        let _ = self.slots_weak.try_upgrade_mut(|inner| {
            assert!(
                inner.storage[idx].is_none(),
                "an unconsumed reservation must not own an operation"
            );
            inner.return_slot(idx);
        });
    }
}

impl SlotsInner {
    pub(super) fn slots_owned_by_user_space(&self) -> impl Iterator<Item = usize> + '_ {
        self.storage
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| x.is_none().then_some(idx))
    }
}

impl Slot {
    pub(super) fn discriminant_str(&self) -> &'static str {
        match self {
            Slot::Pending { .. } => "Pending",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        system::slots::{SlotsTesting, TryGetSlotResult},
        System,
    };

    #[test]
    fn cancelled_delivered_reservation_is_returned() {
        let (submit_side, _completion_side, _poller) = super::new(1, SlotsTesting::default());
        let mut reservations = Vec::new();
        for _ in 0..super::RING_SIZE {
            let TryGetSlotResult::GotSlot { slot, .. } = submit_side.try_get_slot() else {
                panic!("expected an available slot");
            };
            reservations.push(slot);
        }

        let TryGetSlotResult::NoSlots { later, .. } = submit_side.try_get_slot() else {
            panic!("expected to wait after reserving every slot");
        };

        // Returning one reservation successfully delivers a new SlotHandle
        // into `later`. Cancelling before repolling drops that queued handle.
        drop(reservations.pop());
        drop(later);

        let TryGetSlotResult::GotSlot { slot, .. } = submit_side.try_get_slot() else {
            panic!("the cancelled reservation must be available again");
        };
        drop(slot);
        drop(reservations);

        assert_eq!(
            submit_side.inner.lock().unwrap().unused_indices.len(),
            super::RING_SIZE as usize
        );
    }

    // Regression-test for issue https://github.com/neondatabase/tokio-epoll-uring/issues/37
    #[tokio::test]
    async fn test_wait_for_completion_drop_behavior() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Arc::new(Mutex::new(Some(tx)));
        let system = System::launch_with_testing(
            None,
            Some(SlotsTesting {
                test_on_wake: Box::new(move || {
                    Some(
                        tx.lock()
                            .unwrap()
                            .take()
                            .expect("should only be called once, we only submit one nop here"),
                    )
                }),
            }),
            &crate::metrics::GLOBAL_STORAGE,
            Arc::new(()),
        )
        .await
        .unwrap();
        let nop = tokio::spawn(system.nop());
        let at_yield_point: tokio::sync::oneshot::Sender<()> = rx.await.unwrap();
        nop.abort();
        let Err(join_err) = nop.await else {
            panic!("expecting join error after abort");
        };
        assert!(join_err.is_cancelled());
        assert!(
            at_yield_point.is_closed(),
            "abort drops the nop op, and hence the oneshot receiver"
        );
    }
}
