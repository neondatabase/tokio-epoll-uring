use std::{
    os::fd::AsRawFd,
    sync::{Arc, Mutex},
};

pub mod handle;
pub mod thread_local;

use crate::system::{submission::SubmitSide, RING_SIZE};

use super::{
    completion::{CompletionSide, Poller, PollerNewArgs, PollerTesting},
    lifecycle::handle::SystemHandle,
    slots::{self, Slots},
};

/// A running `tokio_epoll_uring` system. Use [`Self::launch`] to start, then [`SystemHandle`] to interact.
pub struct System {
    #[allow(dead_code)]
    id: usize,
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
            let uring = Box::into_raw(Box::new(io_uring::IoUring::new(RING_SIZE).unwrap()));
            let uring_fd = unsafe { (*uring).as_raw_fd() };
            let (submitter, sq, cq) = unsafe { (*uring).split() };

            let (slots_submit_side, slots_completion_side, slots_poller) =
                super::slots::new(id, sq, submitter, cq, uring);

            let completion_side =
                Arc::new(Mutex::new(CompletionSide::new(id, slots_completion_side)));

            let submit_side = SubmitSide::new(id, slots_submit_side);
            let system = System { id };
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
}

pub(crate) fn poller_impl_finish_shutdown(
    system: System,
    slots: Slots<{ slots::co_owner::POLLER }>,
    completion_side: Arc<Mutex<CompletionSide>>,
    req: ShutdownRequest,
) {
    tracing::info!("poller shutdown start");
    scopeguard::defer_on_success! {tracing::info!("poller shutdown end")};
    scopeguard::defer_on_unwind! {tracing::error!("poller shutdown panic")};

    let System { id: _ } = { system };

    let ShutdownRequest { done_tx } = req;

    let completion_side = Arc::try_unwrap(completion_side)
        .ok()
        .expect("we plugged the SubmitSide, so, all refs to CompletionSide are gone");
    let _completion_side = Mutex::into_inner(completion_side).unwrap();

    // Unsplit the uring
    // explicit type to ensure we get compile-time-errors if we don't have the owned io_uring crate types
    let ring = slots.shutdown();
    drop(ring);

    // notify about completed shutdown;
    // ignore send errors, interest may be gone if it's implicit shutdown through SystemHandle::drop
    let _ = done_tx.send(());
}
