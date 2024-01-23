use std::{
    os::fd::AsRawFd,
    sync::{Arc, Mutex},
};

pub mod handle;
pub mod thread_local;

use io_uring::{CompletionQueue, SubmissionQueue, Submitter};
use uring_common::io_uring;

use crate::{
    system::{completion::ShutdownRequestImpl, RING_SIZE},
    util::oneshot_nonconsuming,
};

use super::{
    completion::{CompletionSide, Poller, PollerNewArgs, PollerTesting},
    lifecycle::handle::SystemHandle,
    slots::{self, Slots},
    submission::{SubmitSide, SubmitSideInner, SubmitSideNewArgs},
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

        let (submit_side, poller_ready_fut) = {
            // TODO: should we mlock `slots`? io_uring mmap is mlocked, slots are equally important for the system to function;
            let (slots_submit_side, slots_completion_side, slots_poller) = super::slots::new(id);

            let uring = Box::new(
                io_uring::IoUring::builder()
                    // Don't fork-inherit the MAP_SHARED memory mapping of the SQs and CQs with any child process.
                    // Rationale: for an individual io_uring instance, tokio-epoll-uring assumes explusive ownership
                    // of the user-space part of the io_uring user/kernel interface.
                    //
                    // For example, we rely on Arc<Mutex<>> to protect the `SubmitSideInner`.
                    // A child process would have its cloned `Arc<Mutex<>>` but operate on the same MAP_SHARED memory mapping.
                    // Disaster would ensure.
                    .dontfork()
                    .build(RING_SIZE)
                    .unwrap(),
            );
            let flags_set_by_kernel = nix::fcntl::FdFlag::from_bits_truncate(
                nix::fcntl::fcntl(uring.as_raw_fd(), nix::fcntl::FcntlArg::F_GETFD).unwrap(),
            );
            assert!(flags_set_by_kernel.contains(nix::fcntl::FdFlag::FD_CLOEXEC),
                // NB: the kernel does this https://sourcegraph.com/github.com/torvalds/linux@v5.10/-/blob/fs/io_uring.c?L9195-9201
                "for an individual io_uring instance, tokio-epoll-uring assumes explusive ownership \
                 of the user-space part of the io_uring user/kernel interface; sharing it with another \
                 process is unsafe; actually we want to prevent fork'ing, but, there's not flag for that, \
                 so at least guard somewhat against the most common use of forking, which is fork+exec.");

            // Ensure the kernel's io_uring features are sufficient for tokio-epoll-uring / it's use case in Neon Pageserver.
            //
            // Kernel 5.10 (Debian bullseye)      https://sourcegraph.com/github.com/torvalds/linux@v5.10/-/blob/fs/io_uring.c?L9368-9371
            // =>      features     SINGLE_MMAP | NODROP | SUBMIT_STABLE | RW_CUR_POS | CUR_PERSONALITY | FAST_POLL | POLL_32BITS;
            // Kernel 6.1 (Debian bookworm)       https://sourcegraph.com/github.com/torvalds/linux@v6.1/-/blob/io_uring/io_uring.c?L3534:16-3534:39
            // => adds features     SQPOLL_NONFIXED | EXT_ARG | NATIVE_WORKERS | RSRC_TAGS | CQE_SKIP | LINKED_FILE;
            // Kernel 6.7 (no Debian release yet) https://sourcegraph.com/github.com/torvalds/linux@v6.7/-/blob/io_uring/io_uring.c?L4054-4060
            // => adds features     REG_REG_RING;
            //
            // Best man page rendering for features on the web: https://man.archlinux.org/man/io_uring_setup.2.en
            //
            // NODROP:
            //  - https://github.com/golang/go/issues/31908#issuecomment-571651387
            //  - also https://kernel.dk/io_uring-whatsnew.pdf
            assert!(uring.params().is_feature_nodrop(), "tokio-epoll-uring has len(slots) == len(sq) == len(cq), so, in theory we don't care, but, better not worry");
            // SUBMIT_STABLE: https://man.archlinux.org/man/io_uring_submit.3.en#NOTES ;
            // TODO need to check each op whether it needs this, then safeguard in future ops
            assert!(uring.params().is_feature_submit_stable(), "we can always put things into resources to avoid depending on this feature, but we haven't thought about it so far, so, let's require it");
            // RW_CUR_POS: we don't expose current-file-position-style APIs currently.
            // CUR_PERSONALITY:
            // - not a problem in Neon's pageserver use case, we don't change personality / seteuid / whatnot
            // FAST_POLL: "just" a performance feature, not relevant to assert here
            // POLL_32BITS: we don't expose epoll; if we ever do, I suppose there'd be a submission error with the higher bits flags, so, we'd notice.
            // SQPOLL_NONFIXED: we don't support fixed files at this time
            // EXT_ARG: we don't use this functionality currently (io_uring_enter timeout sounds interesting though)
            // NATIVE_WORKERS: has a performance aspect, and a security/privileges aspect
            // - https://kernel.dk/axboe-kr2022.pdf (generally a good presentation)
            // => security/privileges aspect: same argument as for CUR_PERSONALITY
            // => performance aspect: we don't care about that here
            // RSRC_TAGS: we don't use fixed FDs / buffers
            // CQE_SKIP: we don't use flag IOSQE_CQE_SKIP_SUCCESS, unlikely the current design every will
            // LINKED_FILE: we don't use IOSQE_IO_LINK nor IOSQE_IO_HARDLINK aka linked io_uring operations; so, this doesn't matter
            // REG_REG_RING: we don't use fixed FDs (and hence not IORING_REGISTER_USE_REGISTERED_RING), so, this doesn't matter.

            let uring = Box::into_raw(uring);

            let uring_fd = unsafe { (*uring).as_raw_fd() };
            let (submitter, sq, cq) = unsafe { (*uring).split() };

            let completion_side = Arc::new(Mutex::new(CompletionSide::new(
                id,
                cq,
                slots_completion_side,
            )));

            // TODO: this unbounded channel is the root of all evil: unbounded queue for IOPS; should provie app option to back-pressure instead.
            let (shutdown_tx, shutdown_rx) = oneshot_nonconsuming::channel();

            let submit_side = SubmitSide::new(SubmitSideNewArgs {
                id,
                submitter,
                sq,
                slots: slots_submit_side,
                completion_side: Arc::clone(&completion_side),
                shutdown_tx,
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
                shutdown_rx,
            });
            (submit_side, poller_ready_fut)
        };

        poller_ready_fut.await;

        SystemHandle::new(id, submit_side)
    }
}

pub(crate) struct ShutdownRequest {
    pub done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub submit_side_inner: Arc<tokio::sync::Mutex<SubmitSideInner>>,
}

pub(crate) fn poller_impl_finish_shutdown(
    system: System,
    ops: Slots<{ slots::co_owner::POLLER }>,
    completion_side: Arc<Mutex<CompletionSide>>,
    shutdown_request: ShutdownRequestImpl,
) {
    tracing::info!("poller shutdown start");
    scopeguard::defer_on_success! {tracing::info!("poller shutdown end")};
    scopeguard::defer_on_unwind! {tracing::error!("poller shutdown panic")};

    let System { id: _, split_uring } = { system };

    let ShutdownRequestImpl {
        mut done_tx,
        submit_side_open,
    } = { shutdown_request };

    let (submitter, sq) = submit_side_open.deconstruct();
    let completion_side = Arc::try_unwrap(completion_side).ok().unwrap();
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
    if let Some(done_tx) = done_tx.take() {
        let _ = done_tx.send(());
    }
}
