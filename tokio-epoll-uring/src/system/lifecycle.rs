use std::{
    os::fd::AsRawFd,
    sync::{Arc, Mutex},
};

pub mod handle;
pub mod thread_local;

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};

use crate::system::{submission::SubmitSideOpen, RING_SIZE};

use super::{
    completion::{CompletionSide, Poller, PollerNewArgs, PollerTesting},
    lifecycle::handle::SystemHandle,
    slots::{self, Slots},
    submission::{SubmitSide, SubmitSideNewArgs},
};

/// A running `tokio_epoll_uring` system. Use [`Self::launch`] to start, then [`SystemHandle`] to interact.
pub struct System {
    #[allow(dead_code)]
    id: usize,
    split_uring: *mut io_uring::IoUring,
    // poller_heartbeat: (), // TODO
}

// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Send for System {}
// SAFETY: we never use the raw IoUring pointer and it's not thread-local or anything like that.
unsafe impl Sync for System {}

static SYSTEM_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl System {
    /// Returns a future that, when polled, sets up an io_uring instance
    /// and a corresponding tokio-epoll-uring *poller task* on the current tokio runtime.
    /// The future resolves to a [`SystemHandle`] that can be used to
    /// interact with the system.
    ///
    /// The concept of *poller task* is described in [`crate::doc::design`].
    pub async fn launch() -> SystemHandle {
        Self::launch_with_testing(None).await
    }

    pub(crate) async fn launch_with_testing(testing: Option<PollerTesting>) -> SystemHandle {
        let id = SYSTEM_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // TODO: this unbounded channel is the root of all evil: unbounded queue for IOPS; should provie app option to back-pressure instead.
        let (submit_side, poller_ready_fut) = {
            let (slots_submit_side, slots_completion_side, slots_poller) = super::slots::new(id);
            let uring = Box::into_raw(Box::new(io_uring::IoUring::new(RING_SIZE).unwrap()));
            let uring_fd = unsafe { (*uring).as_raw_fd() };
            let (submitter, sq, cq) = unsafe { (*uring).split() };

            let completion_side = Arc::new(Mutex::new(CompletionSide::new(
                id,
                cq,
                slots_completion_side,
            )));

            let submit_side = SubmitSide::new(SubmitSideNewArgs {
                id,
                submitter,
                sq,
                slots: slots_submit_side,
                completion_side: Arc::clone(&completion_side),
            });
            let system = System {
                id,
                split_uring: uring,
            };
            let poller_ready_fut = Poller::launch(PollerNewArgs {
                id,
                uring_fd,
                completion_side,
                system,
                slots: slots_poller,
                testing,
            });
            (submit_side, poller_ready_fut)
        };

        let poller_shutdown_tx = poller_ready_fut.await;

        SystemHandle::new(id, submit_side, poller_shutdown_tx)
    }
}

pub(crate) struct ShutdownRequest {
    pub done_tx: tokio::sync::oneshot::Sender<()>,
    pub open_state: SubmitSideOpen,
}

pub(crate) fn poller_impl_finish_shutdown(
    system: System,
    ops: Slots<{ slots::co_owner::POLLER }>,
    completion_side: Arc<Mutex<CompletionSide>>,
    req: ShutdownRequest,
) {
    tracing::info!("poller shutdown start");
    scopeguard::defer_on_success! {tracing::info!("poller shutdown end")};
    scopeguard::defer_on_unwind! {tracing::error!("poller shutdown panic")};

    let System { id: _, split_uring } = { system };

    let ShutdownRequest {
        done_tx,
        open_state,
    } = req;

    let (submitter, sq) = open_state.deconstruct();
    let completion_side = Arc::try_unwrap(completion_side)
        .ok()
        .expect("we plugged the SubmitSide, so, all refs to CompletionSide are gone");
    let completion_side = Mutex::into_inner(completion_side).unwrap();

    // Unsplit the uring
    // explicit type to ensure we get compile-time-errors if we don't have the owned io_uring crate types
    let mut cq: CompletionQueue<'_> = completion_side.deconstruct();
    let mut sq: SubmissionQueue<'_> = sq;
    let submitter: Submitter<'_> = submitter;
    // We now own all the parts from the IoUring::split() again.
    // Some final assertions, then drop them all, unleak the IoUring, and drop it as well.
    // That cleans up the SQ, CQs, registrations, etc.
    cq.sync();
    assert_eq!(cq.len(), 0, "cqe: {:?}", cq.next());
    sq.sync();
    assert_eq!(sq.len(), 0);
    {
        // NB about ops: we can't Arc::try_unwrap().unwrap() it here because
        // there may still be OpFut's that hold a Weak ref to it, and they may
        // try to upgrade the Weak ref to an Arc at any time.
        // Such OpFuts' slot will be in OpState::Ready, so, they're going to
        // drop their upgraded Arc quite soon.
        // So, it's guaranteed that `ops` will be dropped quite soon after we
        // drop our Arc.
        ops.shutdown_assertions();
    }
    // final assertions done, do the unsplitting.
    // There's no special meaning to these `drop`s here, it's just nice to see
    // and drop them all in one place, right before get back the IoUring struct
    // that they referenced.
    #[allow(clippy::drop_non_drop)]
    {
        drop(cq);
        drop(sq);
        drop(submitter);
    }
    let uring: Box<io_uring::IoUring> = unsafe { Box::from_raw(split_uring) };

    // Drop the IoUring struct, cleaning up the underlying kernel resources.
    drop(uring);

    // notify about completed shutdown;
    // ignore send errors, interest may be gone if it's implicit shutdown through SystemHandle::drop
    let _ = done_tx.send(());
}
