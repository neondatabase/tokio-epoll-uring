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
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};

use tracing::trace;
use uring_common::io_uring;

use crate::{metrics::PerSystemMetrics, system::submission::op_fut::Error};

use super::submission::op_fut::Op;

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
struct SlotsInner {
    #[allow(dead_code)]
    id: usize,
    inuse_slot_count: Arc<AtomicU64>,
    co_owner_live: [bool; co_owner::NUM_CO_OWNERS],
    state: SlotsInnerState,
    #[cfg(test)]
    testing: SlotsTesting,
}

#[cfg(test)]
pub(crate) struct SlotsTesting {}

#[cfg(not(test))]
#[derive(Default)]
pub(crate) struct SlotsTesting;

#[cfg(test)]
impl Default for SlotsTesting {
    fn default() -> Self {
        Self {}
    }
}

enum SlotsInnerState {
    Open {},
    Draining,
}

pub(crate) struct SlotHandle {
    slot: Arc<Mutex<Slot>>,
}

struct Slot {
    state: SlotState,
}

enum SlotState {
    NotSubmitted {
        inuse_slot_count: Arc<AtomicU64>,
    },
    Pending {
        inuse_slot_count: Arc<AtomicU64>,
        waker: Option<std::task::Waker>, // None if it hasn't been polled yet
    },
    PendingButFutureDropped {
        inuse_slot_count: Arc<AtomicU64>,
        /// When a future gets dropped while the Op is still running, it gets Box'ed  This is a Box'ed `ResourcesOwnedByKernel`
        _resources_owned_by_kernel: Box<dyn std::any::Any + Send>,
    },
    Ready {
        result: i32,
    },
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
            co_owner_live: [false; co_owner::NUM_CO_OWNERS],
            inuse_slot_count: Arc::new(AtomicU64::new(0)),
            state: SlotsInnerState::Open {},
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
        let slot_arc_ptr: u64 = cqe.user_data();
        // SAFETY: we leaked one reference in `use_for_op`, this here is unleaking it
        let slot: Arc<std::sync::Mutex<Slot>> = unsafe { Arc::from_raw(slot_arc_ptr as *const _) };
        let mut slot_lock_guard = slot.lock().unwrap();
        let slot = &mut *slot_lock_guard;
        match &mut slot.state {
            SlotState::NotSubmitted { inuse_slot_count } => {
                panic!("implementation error: completion received for unsubmitted slot")
            }
            SlotState::Pending {
                inuse_slot_count,
                waker,
            } => {
                let res = inuse_slot_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                assert!(res > 0, "{res}");

                let waker = waker.take();
                slot.state = SlotState::Ready {
                    result: cqe.result(),
                };
                if let Some(waker) = waker {
                    trace!("waking up future");
                    waker.wake();
                }
                // The slot will be returned by `wait_for_completion`.
            }
            SlotState::PendingButFutureDropped {
                inuse_slot_count,
                _resources_owned_by_kernel,
            } => {
                let res = inuse_slot_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                assert!(res > 0);

                slot.state = SlotState::Ready {
                    result: cqe.result(),
                };
            }
            SlotState::Ready { .. } => {
                unreachable!(
                    "completions only come in once: {:?}",
                    slot.state.discriminant_str()
                )
            }
        }
    }
}

impl Slots<{ co_owner::COMPLETION_SIDE }> {
    pub(super) fn transition_to_draining(&self) {
        let mut inner_guard = self.inner.lock().unwrap();
        match &mut inner_guard.state {
            SlotsInnerState::Open {} => {
                // this assignment here drops `waiters`,
                // thereby making all of the op futures return with a shutdown error
                inner_guard.state = SlotsInnerState::Draining;
            }
            SlotsInnerState::Draining => {}
        }
    }
}

impl Slots<{ co_owner::COMPLETION_SIDE }> {
    pub(super) fn inuse_slot_count(&self) -> u64 {
        let inner_guard = self.inner.lock().unwrap();
        let inuse_slot_count = inner_guard.inuse_slot_count.load(Ordering::SeqCst);
        match inner_guard.state {
            SlotsInnerState::Open { .. } => {
                panic!("implementation error: must only call this method after set_draining")
            }
            SlotsInnerState::Draining => inuse_slot_count,
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

        // assert the calling owner is the only remaining owner
        let mut expected_co_owner_live = [false; co_owner::NUM_CO_OWNERS];
        expected_co_owner_live[O] = true;
        assert_eq!(inner_guard.co_owner_live, expected_co_owner_live);
    }
}

type UseForOpOutput<O> = (
    <O as Op>::Resources,
    Result<<O as Op>::Success, Error<<O as Op>::Error>>,
);

impl Slots<{ co_owner::SUBMIT_SIDE }> {
    pub(crate) fn submit_prepare<M>(&self, per_system_metrics: Arc<M>) -> SlotHandle
    where
        M: PerSystemMetrics,
    {
        let slot = Arc::new(Mutex::new({
            Slot {
                state: SlotState::NotSubmitted {
                    inuse_slot_count: {
                        let inuse_slot_count = {
                            let inner_guard = self.inner.lock().unwrap();
                            Arc::clone(&inner_guard.inuse_slot_count)
                        };
                        let queue_depth =
                            inuse_slot_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        per_system_metrics.observe_slots_submission_queue_depth(queue_depth);
                        inuse_slot_count
                    },
                },
            }
        }));
        SlotHandle { slot }
    }

    pub(crate) fn submit_and_wait<O, S>(
        slot_handle: SlotHandle,
        mut op: O,
        do_submit: S,
    ) -> impl Future<Output = UseForOpOutput<O>>
    where
        O: Op + Send + 'static,
        S: FnOnce(io_uring::squeue::Entry),
    {
        let SlotHandle { slot } = slot_handle;
        let mut slot_lock_guard = slot.lock().unwrap();
        let slot_mut = &mut *slot_lock_guard;
        match &slot_mut.state {
            SlotState::NotSubmitted { inuse_slot_count } => {
                slot_mut.state = SlotState::Pending {
                    inuse_slot_count: Arc::clone(inuse_slot_count),
                    waker: None,
                };
            }
            SlotState::Pending { .. } => {
                unreachable!()
            }
            SlotState::PendingButFutureDropped { .. } => {
                unreachable!()
            }
            SlotState::Ready { .. } => {
                unreachable!()
            }
        }
        drop(slot_lock_guard);
        let slot_arc_ptr = Arc::into_raw(Arc::clone(&slot));
        let sqe = op.make_sqe();
        let sqe = sqe.user_data(slot_arc_ptr as *const _ as u64);
        do_submit(sqe);
        Self::wait_for_completion(slot, op)
    }

    async fn wait_for_completion<O>(slot: Arc<Mutex<Slot>>, op: O) -> UseForOpOutput<O>
    where
        O: Op + Send + 'static,
    {
        // invariant: op.is_some() <=> we haven't observed the poll_fn below complete yet
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
            let Some(op) = op.lock().unwrap().take() else {
                // this is the normal case: the op completed, waker got woken, poll_fn below returned Ready
                return;
            };
            let mut slot_lock_guard = slot.lock().unwrap();
            let slot_mut = &mut *slot_lock_guard;

            match &slot_mut.state {
                SlotState::NotSubmitted { inuse_slot_count } => unreachable!("we call this function only after transitioning the slot out of this state"),
                SlotState::Pending { inuse_slot_count, waker: _ } => {
                    // The resource needs to be kept alive until the op completes.
                    // So, move it into the Slot.
                    // `process_completion` will drop the box and return the slot
                    // once it observes the completion.
                    slot_mut.state = SlotState::PendingButFutureDropped {
                        inuse_slot_count: Arc::clone(inuse_slot_count),
                        _resources_owned_by_kernel: Box::new(op),
                    };
                }
                SlotState::Ready { result } => {
                    // The op completed and called the waker that would eventually cause this future to be polled
                    // and transition from Inflight to one of the Done states. But this future got dropped first.
                    // So, it's our job to drop the slot.
                    slot_mut.state = SlotState::Ready { result: *result };
                    // SAFETY:
                    // The op is ready, hence the resources aren't onwed by the kernel anymore.
                    #[allow(unused_unsafe)]
                    unsafe {
                        drop(op);
                    }
                }
                SlotState::PendingButFutureDropped { .. } => {
                    unreachable!("above is the only transition into this state, and this function only runs once")
                }
            }
        };

        // Now that we've set up the scope guard, get to business.
        // Inspect the slot to check whether the poller task already processed the completion.
        // If it has, good for us.
        // If not, store a waker in the slot so the poller task will wake us up to poll again
        // and observe the SlotState::Ready then.
        //
        // If we get cancelled in the meantime (i.e., this future gets dropped), the scopeguard
        // will make sure the resources stay alive until the op is complete.
        let mut poll_count = 0;
        let poll_res = poll_fn(|cx| {
            poll_count += 1;
            let mut slot_lock_guard = slot.lock().unwrap();
            let slot_mut = &mut *slot_lock_guard;

            match &mut slot_mut.state {
                SlotState::NotSubmitted { .. } => {
                    unreachable!(
                        "we call this function only after transitioning the slot out of this state"
                    )
                }
                SlotState::Pending {
                    inuse_slot_count,
                    waker,
                } => {
                    trace!("op is still pending, storing waker in it");
                    let waker_mut_ref = waker.get_or_insert_with(|| cx.waker().clone());
                    if !cx.waker().will_wake(waker_mut_ref) {
                        waker.replace(cx.waker().clone());
                    }
                    Poll::Pending
                }
                SlotState::PendingButFutureDropped { .. } => {
                    unreachable!("if it's dropped, it's not pollable")
                }
                SlotState::Ready { result: res } => {
                    trace!("op is ready, returning resources to user");
                    let res = *res;
                    // SAFETY: the slot is ready, so, ownership is back with userspace.
                    #[allow(unused_unsafe)]
                    unsafe {
                        let op = op.lock().unwrap().take().unwrap();
                        let (resources, res) = op.on_op_completion(res);
                        Poll::Ready((resources, res.map_err(Error::Op)))
                    }
                }
            }
        })
        .await;
        assert!(poll_count >= 1);
        if poll_count == 1 && *crate::env_tunables::YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL {
            tokio::task::yield_now().await;
        }
        poll_res
    }
}

impl SlotState {
    pub(super) fn discriminant_str(&self) -> &'static str {
        match self {
            SlotState::NotSubmitted { .. } => "NotSubmitted",
            SlotState::Pending { .. } => "Pending",
            SlotState::PendingButFutureDropped { .. } => "PendingButFutureDropped",
            SlotState::Ready { .. } => "Ready",
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        match &self.state {
            SlotState::NotSubmitted { inuse_slot_count } => {
                // If it hasn't been submitted, e.g. because we gave up while
                // waiting for the SQ to unclog, it's the drop handler's job
                // to decrement the inuse_slot_count.
                let prev = inuse_slot_count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                assert!(prev > 0);
            }
            SlotState::Pending { .. } => {
                unreachable!("we only drop a Slot after it's been processed by the poller task")
            }
            SlotState::PendingButFutureDropped {
                inuse_slot_count,
                _resources_owned_by_kernel,
            } => {
                unreachable!("we only drop a Slot after it's been processed by the poller task")
            }
            SlotState::Ready { .. } => {
                trace!("dropping a Slot that's already Ready");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metrics::PerSystemMetrics;
    use system::test_util::{self, FOREVER};

    use crate::*;

    #[tokio::test(start_paused = true)]
    async fn test_submission_queue_depth_metric() {
        struct Metrics {
            observations: std::sync::Mutex<Vec<u64>>,
        }
        impl PerSystemMetrics for Metrics {
            fn observe_slots_submission_queue_depth(&self, queue_depth: u64) {
                self.observations.lock().unwrap().push(queue_depth);
            }
        }

        let metrics = Metrics {
            observations: std::sync::Mutex::new(Vec::new()),
        };
        let metrics = Arc::new(metrics);
        let system = Arc::new(
            System::launch_with_metrics(Arc::clone(&metrics))
                .await
                .unwrap(),
        );

        // basics
        let (_, res) = system.nop().await;
        res.unwrap();
        let (_, res) = system.nop().await;
        res.unwrap();
        {
            let mut observations = metrics.observations.lock().unwrap();
            assert_eq!(observations.len(), 2);
            assert_eq!(&observations[..], &[0, 0]);
            observations.clear();
        }

        // Build some queue depth.
        let timerfd = test_util::timerfd::oneshot(FOREVER);
        let timerfd = Arc::new(timerfd);

        let read1 = test_util::timerfd::read(Arc::clone(&timerfd), Arc::clone(&system));
        let read2 = test_util::timerfd::read(Arc::clone(&timerfd), Arc::clone(&system));
        let read3 = test_util::timerfd::read(Arc::clone(&timerfd), Arc::clone(&system));

        tokio::select! {
            _ = read1 => { panic!("timer should not be firing") }
            _ = read2 => { panic!("timer should not be firing") }
            _ = read3 => { panic!("timer should not be firing") }
            _ = tokio::time::sleep(FOREVER) => {}
        }

        {
            let mut observations = metrics.observations.lock().unwrap();
            assert_eq!(observations.len(), 3);
            assert_eq!(&observations[..], &[0, 1, 2]);
            observations.clear();
        }
    }
}
