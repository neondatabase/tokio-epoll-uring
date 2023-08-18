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

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};

use tracing::{debug, trace};

use crate::system::submission::op_fut::Error;

use super::{
    submission::op_fut::{Fut, Op, SystemError},
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

enum SlotsInner {
    Open(Box<SlotsInnerOpen>),
    Draining(Box<SlotsInnerDraining>),
    Drained,
    Undefined,
}

struct SlotsInnerOpen {
    id: usize,
    storage: [Option<Slot>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    // FIXME: this is a basic channel right? could be a tokio::sync::mpsc::channel(1) instead
    waiters: VecDeque<tokio::sync::oneshot::Sender<usize>>,
    myself: SlotsWeak,
    co_owner_live: [bool; co_owner::NUM_CO_OWNERS],
    sq: SubmissionQueue<'static>,
    submitter: Submitter<'static>,
    cq: CompletionQueue<'static>,
    split_uring: *mut io_uring::IoUring,
}

struct SlotsInnerDraining {
    #[allow(dead_code)]
    id: usize,
    storage: [Option<Slot>; RING_SIZE as usize],
    unused_indices: Vec<usize>,
    co_owner_live: [bool; co_owner::NUM_CO_OWNERS],
    sq: SubmissionQueue<'static>,
    submitter: Submitter<'static>,
    cq: CompletionQueue<'static>,
    split_uring: *mut io_uring::IoUring,
}

unsafe impl Send for SlotsInnerOpen {}
unsafe impl Sync for SlotsInnerOpen {}

unsafe impl Send for SlotsInnerDraining {}
unsafe impl Sync for SlotsInnerDraining {}

pub(crate) struct SlotHandle {
    // FIXME: why is this weak?
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
    sq: SubmissionQueue<'static>,
    submitter: Submitter<'static>,
    cq: CompletionQueue<'static>,
    split_uring: *mut io_uring::IoUring,
) -> (
    Slots<{ co_owner::SUBMIT_SIDE }>,
    Slots<{ co_owner::COMPLETION_SIDE }>,
    Slots<{ co_owner::POLLER }>,
) {
    let inner = Arc::new_cyclic(|inner_weak| {
        Mutex::new(SlotsInner::Open(Box::new(SlotsInnerOpen {
            id,
            storage: {
                const NONE: Option<Slot> = None;
                [NONE; RING_SIZE as usize]
            },
            unused_indices: (0..RING_SIZE.try_into().unwrap()).collect(),
            waiters: VecDeque::new(),
            myself: SlotsWeak {
                id,
                inner_weak: inner_weak.clone(),
            },
            co_owner_live: [false; co_owner::NUM_CO_OWNERS],
            submitter,
            sq,
            cq,
            split_uring,
        })))
    });
    fn make_co_owner<const O: usize>(inner: &Arc<Mutex<SlotsInner>>) -> Slots<O> {
        let mut guard = inner.lock().unwrap();
        let SlotsInner::Open(open) = &mut *guard else {
            panic!("we just created it like this above");
        };
        let SlotsInnerOpen {
            id, co_owner_live, ..
        } = &mut **open;
        co_owner_live[O] = true;
        Slots {
            id: *id,
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
            Ok(mut guard) => match &mut *guard {
                SlotsInner::Undefined => (),
                SlotsInner::Open(open) => open.co_owner_live[O] = false,
                SlotsInner::Draining(draining) => draining.co_owner_live[O] = false,
                SlotsInner::Drained => (),
            },
            Err(mut poison) => match &mut **poison.get_mut() {
                SlotsInner::Open(open) => open.co_owner_live[O] = false,
                SlotsInner::Draining(draining) => draining.co_owner_live[O] = false,
                SlotsInner::Undefined => (),
                SlotsInner::Drained => (),
            },
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

impl SlotsInner {}

impl<const O: usize> Slots<O> {
    pub(super) fn poller_timeout_debug_dump(&self) {
        let inner = self.inner.lock().unwrap();
        // TODO: only do this if some env var is set?
        let (storage, unused_indices) = match &*inner {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(inner) => (&inner.storage, &inner.unused_indices),
            SlotsInner::Draining(inner) => (&inner.storage, &inner.unused_indices),
            SlotsInner::Drained => unreachable!(),
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

impl Slots<{ co_owner::COMPLETION_SIDE }> {
    pub(super) fn process_completions(&mut self) {
        let mut inner_guard = self.inner.lock().unwrap();
        match &mut *inner_guard {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(inner) => inner.process_completions(),
            SlotsInner::Draining(inner) => inner.process_completions(),
            SlotsInner::Drained => panic!("should not be called after draining done"),
        }
    }
}

trait ProcessCompletions {
    fn storage(&mut self) -> &mut [Option<Slot>];
    fn cq(&mut self) -> &mut io_uring::CompletionQueue<'static>;
    fn process_completions(&mut self) {
        self.cq().sync();
        loop {
            let Some(cqe) = self.cq().next() else { break; };

            let idx: u64 = cqe.user_data();
            let idx = usize::try_from(idx).unwrap();

            let storage = self.storage();
            let slot_storage_ref = &mut storage[idx];
            let slot = slot_storage_ref.as_mut().unwrap();
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
                    self.return_slot(idx);
                }
                Slot::Ready { .. } => {
                    unreachable!(
                        "completions only come in once: {:?}",
                        cur.discriminant_str()
                    )
                }
            }
        }
        self.cq().sync();
    }

    fn return_slot(&mut self, idx: usize) {
        let storage = self.storage();
        let slot_storage_ref = &mut storage[idx];
        match slot_storage_ref {
            None => (),
            Some(slot_ref) => match slot_ref {
                Slot::Undefined => unreachable!(),
                Slot::Pending { .. } | Slot::PendingButFutureDropped { .. } => {
                    panic!("implementation error: potential memory unsafety: we must not clear a slot that is still pending  {:?}", slot_ref.discriminant_str());
                }
                Slot::Ready { .. } => {
                    *slot_storage_ref = None;
                }
            },
        }
        self.return_slot_impl(idx);
    }

    fn return_slot_impl(&mut self, idx: usize);
}

impl ProcessCompletions for SlotsInnerOpen {
    fn storage(&mut self) -> &mut [Option<Slot>] {
        &mut self.storage
    }
    fn cq(&mut self) -> &mut io_uring::CompletionQueue<'static> {
        &mut self.cq
    }
    fn return_slot_impl(&mut self, idx: usize) {
        while let Some(waiter) = self.waiters.pop_front() {
            match waiter.send(idx) {
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
}

impl ProcessCompletions for SlotsInnerDraining {
    fn storage(&mut self) -> &mut [Option<Slot>] {
        &mut self.storage
    }
    fn cq(&mut self) -> &mut io_uring::CompletionQueue<'static> {
        &mut self.cq
    }
    fn return_slot_impl(&mut self, idx: usize) {
        trace!("draining, returning idx to unused_indices");
        self.unused_indices.push(idx);
    }
}

impl SlotsInnerOpen {
    fn submit_raw(&mut self, sqe: io_uring::squeue::Entry) -> std::result::Result<(), SubmitError> {
        self.sq.sync();
        match unsafe { self.sq.push(&sqe) } {
            Ok(()) => {}
            Err(_queue_full) => {
                return Err(SubmitError::QueueFull);
            }
        }
        self.sq.sync();
        self.submitter.submit().unwrap();
        Ok(())
    }
}

impl Slots<{ co_owner::SUBMIT_SIDE }> {
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
                    co_owner_live,
                    sq,
                    cq,
                    submitter,
                    split_uring,
                } = *open;
                *inner_guard = SlotsInner::Draining(Box::new(SlotsInnerDraining {
                    id,
                    storage,
                    unused_indices,
                    co_owner_live,
                    sq,
                    cq,
                    submitter,
                    split_uring,
                }));
            }
            SlotsInner::Draining(_draining) => {
                panic!("implementation error: must only call set_draining once")
            }
            SlotsInner::Drained => panic!("implementation error: must only call set_draining once"),
        }
    }
}

impl Slots<{ co_owner::COMPLETION_SIDE }> {
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
                pending_count
            }
            SlotsInner::Drained => unreachable!(),
        }
    }
}

impl<const O: usize> Slots<O> {
    pub(super) fn shutdown(self) -> Box<io_uring::IoUring> {
        let mut ops_inner_guard = self.inner.lock().unwrap();
        let before = std::mem::replace(&mut *ops_inner_guard, SlotsInner::Drained);
        let inner = match before {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(_open) => panic!("we should be Draining by now"),
            SlotsInner::Drained => panic!("we never get back here"),
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

        let SlotsInnerDraining {
            id: _,
            storage: _,
            unused_indices: _,
            co_owner_live: _,
            mut sq,
            submitter,
            mut cq,
            split_uring,
        } = *inner;

        // We now own all the parts from the IoUring::split() again.
        // Some final assertions, then drop them all, unleak the IoUring, and drop it as well.
        // That cleans up the SQ, CQs, registrations, etc.
        cq.sync();
        assert_eq!(cq.len(), 0, "cqe: {:?}", cq.next());
        sq.sync();
        assert_eq!(sq.len(), 0);

        #[allow(clippy::drop_non_drop)]
        {
            drop(cq);
            drop(sq);
            drop(submitter);
        }
        let uring: Box<io_uring::IoUring> = unsafe { Box::from_raw(split_uring) };
        uring
    }
}

impl Slots<{ co_owner::SUBMIT_SIDE }> {
    pub(crate) fn submit<'s, 'f, O>(
        &self,
        op: O,
    ) -> impl std::future::Future<Output = (O::Resources, Result<O::Success, Error<O::Error>>)> + 'f
    where
        O: Op + Send + 'static,
        'f: 's,
    {
        let mut inner_guard = self.inner.lock().unwrap();
        let open = match &mut *inner_guard {
            SlotsInner::Undefined => unreachable!(),
            SlotsInner::Open(open) => open,
            SlotsInner::Draining(_) | SlotsInner::Drained => {
                return Fut::<_, _, _, _>::A(async move {
                    (
                        op.on_failed_submission(),
                        Err(Error::System(SystemError::SystemShuttingDown)),
                    )
                });
            }
        };

        fn use_for_op<'s, 'f, O>(
            open: &'s mut SlotsInnerOpen,
            idx: usize,
            mut op: O,
        ) -> impl std::future::Future<Output = (O::Resources, Result<O::Success, Error<O::Error>>)> + 'f
        where
            O: Op + Send + 'static,
            'f: 's,
        {
            let sqe = op.make_sqe();
            let sqe = sqe.user_data(u64::try_from(idx).unwrap());

            assert!(open.storage[idx].is_none()); // TODO turn Option into tri-state for better semantics: NotTaken, SlotLive, Submitted
            open.storage[idx] = Some(Slot::Pending { waker: None });
            {
                if open.submit_raw(sqe).is_err() {
                    // TODO: DESIGN: io_uring can deal have more ops inflight than the SQ.
                    // So, we could just submit_and_wait here. But, that'd prevent the
                    // current executor thread from making progress on other tasks.
                    //
                    // So, for now, keep SQ size == inflight ops size == Slots size.
                    // This potentially limits throughput if SQ size is chosen too small.
                    //
                    // FIXME: why not just async mutex?
                    unreachable!("the `ops` has same size as the SQ, so, if SQ is full, we wouldn't have been able to get this slot");
                }

                if *crate::env_tunables::PROCESS_COMPLETIONS_ON_SUBMIT {
                    // opportunistically process completion immediately
                    // TODO do it during ::poll() as well?
                    //
                    // FIXME: why are we doing this while holding the SubmitSideOpen
                    open.process_completions();
                }
            }

            SlotHandle {
                idx,
                slots_weak: open.myself.clone(),
            }
            .wait_for_completion(op)
        }

        match open.unused_indices.pop() {
            Some(idx) => Fut::B(use_for_op(open, idx, op)),
            None => {
                let (wake_up_tx, wake_up_rx) = tokio::sync::oneshot::channel();
                open.waiters.push_back(wake_up_tx);

                if *crate::env_tunables::PROCESS_COMPLETIONS_ON_QUEUE_FULL {
                    // TODO shouldn't we loop here until we've got a slot? This one-off poll doesn't make much sense.
                    open.submitter.submit().unwrap();
                    open.process_completions();
                }

                let myself = open.myself.clone();

                Fut::C(async move {
                    let idx = match wake_up_rx.await {
                        Ok(idx) => idx,
                        Err(_) => {
                            return (
                                op.on_failed_submission(),
                                Err(Error::System(SystemError::SystemShuttingDown)),
                            );
                        }
                    };
                    let inner = Weak::upgrade(&myself.inner_weak)
                        .expect("we got an idx, so, we know the system is still alive");
                    let fut = {
                        let mut inner_guard = inner.lock().unwrap();
                        match &mut *inner_guard {
                            SlotsInner::Undefined => unreachable!(),
                            SlotsInner::Open(open) => use_for_op(open, idx, op),
                            SlotsInner::Draining(_) | SlotsInner::Drained => {
                                return (
                                    op.on_failed_submission(),
                                    Err(Error::System(SystemError::SystemShuttingDown)),
                                );
                            }
                        }
                    };
                    fut.await
                })
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("queue full")]
    QueueFull,
}

impl Slots<{ co_owner::SUBMIT_SIDE }> {}

impl SlotHandle {
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
                let inner: &mut dyn ProcessCompletions = match inner {
                    SlotsInner::Undefined => unreachable!(),
                    SlotsInner::Open(inner) => &mut **inner,
                    SlotsInner::Draining(inner) => &mut **inner,
                    SlotsInner::Drained => unreachable!(),
                };
                let storage = inner.storage();
                let slot_storage_mut = &mut storage[slot.idx];
                let slot_mut = slot_storage_mut.as_mut().unwrap();
                let cur = std::mem::replace(&mut *slot_mut, Slot::Undefined);
                match cur {
                    Slot::Undefined => unreachable!("implementation error"),
                    Slot::Pending { .. } => {
                        // The resource needs to be kept alive until the op completes.
                        // So, move it into the Slot.
                        // `process_completion` will drop the box and return the slot
                        // once it observes the completion.
                        *slot_mut = Slot::PendingButFutureDropped {
                            resources_owned_by_kernel: Box::new(op),
                        };
                    }
                    Slot::Ready { result } => {
                        // The op completed and called the waker that would eventually cause this future to be polled
                        // and transition from Inflight to one of the Done states. But this future got dropped first.
                        // So, it's our job to drop the slot.
                        *slot_mut = Slot::Ready { result };
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
                let inner: &mut dyn ProcessCompletions = match inner {
                    SlotsInner::Undefined => unreachable!(),
                    SlotsInner::Open(inner) => &mut **inner,
                    SlotsInner::Draining(inner) => &mut **inner,
                    SlotsInner::Drained => unreachable!(),
                };

                let storage = inner.storage();
                let slot_storage_ref = &mut storage[slot.idx];
                let slot_mut = slot_storage_ref.as_mut().unwrap();

                let cur: Slot = std::mem::replace(&mut *slot_mut, Slot::Undefined);
                match cur {
                    Slot::Undefined => panic!("slot is in undefined state"),
                    Slot::Pending { mut waker } => {
                        trace!("op is still pending, storing waker in it");
                        let waker_mut_ref = waker.get_or_insert_with(|| cx.waker().clone());
                        if !cx.waker().will_wake(waker_mut_ref) {
                            *slot_mut = Slot::Pending {
                                waker: Some(cx.waker().clone()),
                            };
                        } else {
                            *slot_mut = Slot::Pending { waker };
                        }
                        InspectSlotResult::NeedToWait
                    }
                    Slot::PendingButFutureDropped { .. } => {
                        unreachable!("if it's dropped, it's not pollable")
                    }
                    Slot::Ready { result: res } => {
                        trace!("op is ready, returning resources to user");
                        *slot_mut = Slot::Ready { result: res };
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
