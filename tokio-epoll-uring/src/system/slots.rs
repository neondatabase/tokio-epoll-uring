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
    storage: [Slot; RING_SIZE as usize],
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
        waiters: VecDeque<SlotWaiter>,
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

enum SlotWaiter {
    Tokio(oneshot::Sender<SlotHandle>),
    #[cfg(test)]
    Test(Box<dyn FnOnce(SlotHandle) -> Result<(), SlotHandle> + Send>),
}

impl SlotWaiter {
    fn send(self, slot: SlotHandle) -> Result<(), SlotHandle> {
        match self {
            SlotWaiter::Tokio(sender) => sender.send(slot),
            #[cfg(test)]
            SlotWaiter::Test(sender) => sender(slot),
        }
    }
}

struct PreparedDelivery {
    waiter: SlotWaiter,
    #[cfg(test)]
    test_on_wake: Option<tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>>,
}

enum Slot {
    Free,
    Reserved,
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
            storage: std::array::from_fn(|_| Slot::Free),
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

    fn return_reservation(&self, idx: usize) {
        loop {
            let waiter = match self.try_upgrade_mut(|inner| inner.prepare_return(idx)) {
                Ok(waiter) => waiter,
                Err(()) => return,
            };
            let Some(delivery) = waiter else {
                return;
            };

            let handle = SlotHandle {
                slots_weak: self.clone(),
                idx: Some(idx),
                #[cfg(test)]
                test_on_wake: Mutex::new(delivery.test_on_wake),
            };
            match delivery.waiter.send(handle) {
                Ok(()) => {
                    // Sending relinquishes this return loop's ownership. The
                    // receiver may already have consumed or dropped the handle.
                    trace!(idx, "handed slot reservation to a waiter");
                    return;
                }
                Err(mut rejected) => {
                    let recovered = rejected
                        .idx
                        .take()
                        .expect("a rejected handle must own its reservation");
                    debug_assert_eq!(recovered, idx);
                    // Retry from the mutex-protected state transition. Shutdown
                    // may have changed Open to Draining while send ran.
                }
            }
        }
    }
}

impl SlotsInner {
    /// Prepare a reservation return while holding the slots mutex. Delivery
    /// itself must happen after releasing the mutex because sending can destroy
    /// the delivered SlotHandle synchronously.
    fn prepare_return(&mut self, idx: usize) -> Option<PreparedDelivery> {
        assert!(
            matches!(self.storage[idx], Slot::Reserved),
            "only a reserved slot can be returned; slot {idx} is {}",
            self.storage[idx].discriminant_str()
        );
        match &mut self.state {
            SlotsInnerState::Open { waiters, .. } => match waiters.pop_front() {
                Some(waiter) => Some(PreparedDelivery {
                    waiter,
                    #[cfg(test)]
                    test_on_wake: (self.testing.test_on_wake)(),
                }),
                None => {
                    self.storage[idx] = Slot::Free;
                    self.unused_indices.push(idx);
                    None
                }
            },
            SlotsInnerState::Draining => {
                self.storage[idx] = Slot::Free;
                self.unused_indices.push(idx);
                None
            }
        }
    }
}

impl<const O: usize> Slots<O> {
    fn slots_weak(&self) -> SlotsWeak {
        SlotsWeak {
            id: self.id,
            inner_weak: Arc::downgrade(&self.inner),
        }
    }

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
                    let discr = s.discriminant_str();
                    by_state_discr
                        .entry(discr)
                        .and_modify(|v| *v += 1)
                        .or_insert(1);
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
            let (idx, completion, res) = {
                let mut inner_guard = self.inner.lock().unwrap();
                inner_guard.take_completion(cqe)
            };
            self.slots_weak().return_reservation(idx);
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
    ) -> (usize, Box<dyn PendingCompletion>, i32) {
        let idx: u64 = cqe.user_data();
        let idx = usize::try_from(idx).unwrap();
        let slot = std::mem::replace(&mut self.storage[idx], Slot::Reserved);
        let Slot::Pending { completion } = slot else {
            panic!("completion must refer to a pending operation")
        };
        (idx, completion, cqe.result())
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
    /// Count every slot that shutdown must wait for: both submitted operations
    /// and reservations that have not yet been submitted or relinquished.
    pub(super) fn outstanding_slot_count(&self) -> usize {
        let inner_guard = self.inner.lock().unwrap();
        match inner_guard.state {
            SlotsInnerState::Open { .. } => {
                panic!("implementation error: must only call this method after set_draining")
            }
            SlotsInnerState::Draining => inner_guard
                .storage
                .iter()
                .filter(|slot| !matches!(slot, Slot::Free))
                .count(),
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
        let unused_indices = inner_guard
            .unused_indices
            .iter()
            .cloned()
            .collect::<HashSet<usize>>();
        assert_eq!(
            inner_guard.unused_indices.len(),
            unused_indices.len(),
            "unused_indices contains duplicate returns"
        );
        assert!(unused_indices.iter().all(|idx| *idx < RING_SIZE as usize));
        for (idx, slot) in inner_guard.storage.iter().enumerate() {
            assert!(matches!(slot, Slot::Free), "slot {idx} is not free");
            assert!(
                unused_indices.contains(&idx),
                "free slot {idx} is unavailable"
            );
        }
        assert_eq!(unused_indices.len(), RING_SIZE as usize);

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
                    Some(idx) => {
                        assert!(matches!(inner.storage[idx], Slot::Free));
                        inner.storage[idx] = Slot::Reserved;
                        TryGetSlotResult::GotSlot {
                            slot: SlotHandle {
                                slots_weak: myself.clone(),
                                idx: Some(idx),
                                #[cfg(test)]
                                test_on_wake: Mutex::new((inner.testing.test_on_wake)()),
                            },
                            queue_depth: num_in_use_slots,
                        }
                    }
                    None => {
                        let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                        let num_waiters = waiters.len() as u64;
                        waiters.push_back(SlotWaiter::Tokio(wake_up_tx));
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
                assert!(matches!(inner.storage[idx], Slot::Reserved));
                inner.storage[idx] = Slot::Pending {
                    completion: Box::new(pending.take().unwrap()),
                };
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
        self.slots_weak.return_reservation(idx);
    }
}

impl Slot {
    pub(super) fn discriminant_str(&self) -> &'static str {
        match self {
            Slot::Free => "Free",
            Slot::Reserved => "Reserved",
            Slot::Pending { .. } => "Pending",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };

    use crate::{
        system::slots::{SlotsTesting, TryGetSlotResult},
        System,
    };

    fn assert_all_slots_free<const O: usize>(slots: &super::Slots<O>) {
        let inner = slots.inner.lock().unwrap();
        let unique = inner
            .unused_indices
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(inner.unused_indices.len(), super::RING_SIZE as usize);
        assert_eq!(unique.len(), super::RING_SIZE as usize);
        assert!(unique.iter().all(|idx| *idx < super::RING_SIZE as usize));
        assert!(inner
            .storage
            .iter()
            .all(|slot| matches!(slot, super::Slot::Free)));
    }

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

        assert_all_slots_free(&submit_side);
    }

    #[test]
    fn successful_delivery_may_destroy_reservation_inside_send() {
        let (submit_side, _completion_side, _poller) = super::new(1, SlotsTesting::default());
        let mut reservations = Vec::new();
        for _ in 0..super::RING_SIZE {
            let TryGetSlotResult::GotSlot { slot, .. } = submit_side.try_get_slot() else {
                panic!("expected an available slot");
            };
            reservations.push(slot);
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_sender = Arc::clone(&calls);
        let inner = Arc::clone(&submit_side.inner);
        let test_waiter = super::SlotWaiter::Test(Box::new(move |mut slot| {
            // A send is allowed to synchronously destroy the delivered value.
            // Verify that delivery did not retain the slots mutex before
            // exercising that re-entrant SlotHandle::drop path.
            let guard = match inner.try_lock() {
                Ok(guard) => guard,
                Err(error) => {
                    // Avoid calling the locking destructor while reporting a
                    // lock violation, which would hang the regression test.
                    slot.idx.take();
                    panic!("slot mutex is locked during delivery: {error}");
                }
            };
            drop(guard);
            calls_for_sender.fetch_add(1, Ordering::SeqCst);
            drop(slot);
            Ok(())
        }));
        {
            let mut inner = submit_side.inner.lock().unwrap();
            let super::SlotsInnerState::Open { waiters, .. } = &mut inner.state else {
                panic!("slots unexpectedly draining");
            };
            waiters.push_back(test_waiter);
        }

        // Returning this reservation invokes the test sender. It destroys the
        // delivered handle before reporting success, so the recursive return
        // must complete and the outer delivery loop must stop exactly once.
        drop(reservations.pop());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(reservations);

        assert_all_slots_free(&submit_side);
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
