use std::{fmt::Display, sync::Arc};

use futures::stream::FuturesOrdered;

/// An io_uring operation and the resources it operates on.
///
/// For each io_uring operation, there is a struct that implements this trait.
pub trait Op: crate::sealed::Sealed + Sized + Send + 'static {
    type Resources;
    type Success;
    type Error;
    fn on_failed_submission(self) -> Self::Resources;
    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>);
    fn make_sqe(&mut self) -> io_uring::squeue::Entry;
}

use uring_common::io_uring;

use crate::{
    metrics::PerSystemMetrics,
    system::{completion::ProcessCompletionsCause, slots, slots::SlotHandle},
};

use super::{SubmitSideOpenGuard, SubmitSideWeak};

#[derive(Debug, thiserror::Error)]
pub enum SystemError {
    #[error("shutting down")]
    SystemShuttingDown,
}

#[derive(thiserror::Error, Debug)]
pub enum Error<T> {
    System(SystemError),
    Op(T),
}

impl<T: Display> Display for Error<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::System(e) => {
                if f.alternate() {
                    write!(f, "tokio-epoll-uring: {e:#}")
                } else {
                    write!(f, "tokio-epoll-uring: {e}")
                }
            }
            Error::Op(op) => Display::fmt(op, f),
        }
    }
}

pub(crate) async fn execute_op<O, M>(
    op: O,
    submit_side: SubmitSideWeak,
    per_system_metrics: Arc<M>,
) -> (O::Resources, Result<O::Success, Error<O::Error>>)
where
    // FIXME: probably dont need the unpin
    O: Op + Send + 'static + Unpin,
    M: PerSystemMetrics,
{
    let results = 
	execute_ops(std::iter::once(op), submit_side, per_system_metrics)
	.await;
    results.into_iter().next().unwrap()
}

pub(crate) async fn execute_ops<O, M>(
    ops_iter: impl Iterator<Item = O>,
    submit_side: SubmitSideWeak,
    per_system_metrics: Arc<M>,
) -> Vec<(O::Resources, Result<O::Success, Error<O::Error>>)>
where
    // FIXME: probably dont need the unpin
    O: Op + Send + 'static + Unpin,
    M: PerSystemMetrics,
{
    let mut open_guard = match submit_side.upgrade_to_open().await {
        Some(open) => open,
        None => {
	    let mut futs = Vec::new();
	    for op in ops_iter {
		let err = Error::System(SystemError::SystemShuttingDown);
                futs.push(wait_or_immediate_result(op, Err(err)).await);
            };
	    return futs;
        }
    };

    fn do_submit(open_guard: &mut SubmitSideOpenGuard, sqe: io_uring::squeue::Entry) {
        if open_guard.submit_raw(sqe).is_err() {
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

        // this allows us to keep the possible guard in cq_guard because the arc lives on stack
        #[allow(unused_assignments)]
        let mut cq_owned = None;

        let cq_guard = if *crate::env_tunables::PROCESS_COMPLETIONS_ON_SUBMIT {
            let cq = Arc::clone(&open_guard.completion_side);
            cq_owned = Some(cq);
            Some(cq_owned.as_ref().expect("we just set it").lock().unwrap())
        } else {
            None
        };

        if let Some(mut cq) = cq_guard {
            // opportunistically process completion immediately
            // TODO do it during ::poll() as well?
            //
            // FIXME: why are we doing this while holding the SubmitSideOpen
            cq.process_completions(ProcessCompletionsCause::Regular);
        }
    }

    let mut result_futs = FuturesOrdered::new();

    for mut op in ops_iter {
        let mut slot = match open_guard.slots.try_get_slot() {
            slots::TryGetSlotResult::Draining => {
                result_futs.push_back(wait_or_immediate_result(
                    op,
                    Err(Error::System(SystemError::SystemShuttingDown)),
                ));
                continue;
            }
            slots::TryGetSlotResult::GotSlot { slot, queue_depth } => {
                per_system_metrics
                    .as_ref()
                    .observe_slots_submission_queue_depth(queue_depth);
                slot
            }
            slots::TryGetSlotResult::NoSlots { later, queue_depth } => {
                // All slots are taken and we're waiting in line.
                // If enabled, do some opportunistic completion processing to wake up futures that will release ops slots.
                // This is in the hope that we'll wake ourselves up.

                per_system_metrics
                    .as_ref()
                    .observe_slots_submission_queue_depth(queue_depth);
                if *crate::env_tunables::PROCESS_COMPLETIONS_ON_QUEUE_FULL {
                    // TODO shouldn't we loop here until we've got a slot? This one-off poll doesn't make much sense.
                    open_guard.submitter.submit().unwrap();
                    open_guard
                        .completion_side
                        .lock()
                        .unwrap()
                        .process_completions(ProcessCompletionsCause::Regular);
                }
                let slot = match later.await {
                    Ok(slot) => slot,
                    Err(_dropped) => {
                        result_futs.push_back(wait_or_immediate_result(
                            op,
                            Err(Error::System(SystemError::SystemShuttingDown)),
                        ));
                        continue;
                    }
                };
                slot
            }
        };

        let fut = match slot.use_for_op(&mut op, |sqe| do_submit(&mut open_guard, sqe)) {
	    Ok(()) => wait_or_immediate_result(op, Ok(slot)),
	    Err(err) => wait_or_immediate_result(op, Err(Error::System(err))),
	};
        result_futs.push_back(fut);
    }

    // drop it to enable timely shutdown
    drop(open_guard);

    let mut results = Vec::new();
    use futures::StreamExt;
    while let Some(res) = result_futs.next().await {
	results.push(res);
    }
    results
}

async fn wait_or_immediate_result<O>(
    op: O,
    submit_result: Result<SlotHandle, Error<O::Error>>,
) -> (O::Resources, Result<O::Success, Error<O::Error>>)
where
    O: Op + Send + 'static,
{
    match submit_result {
        Ok(mut slot) => slot.wait_for_completion(op).await,
        Err(err) => (op.on_failed_submission(), Err(err)),
    }
}
