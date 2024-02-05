//! Owned handle to an explicitly [`System::launch`](crate::System::launch)ed system.

use futures::FutureExt;
use std::{os::fd::OwnedFd, path::Path, task::ready};
use uring_common::{buf::BoundedBufMut, io_fd::IoFd};

use crate::{
    ops::{
        fsync::FsyncOp,
        open_at::OpenAtOp,
        read::ReadOp,
        statx::{StatxOp, SubmittingBox},
    },
    system::submission::{op_fut::execute_op, SubmitSide},
};

/// Owned handle to the [`System`](crate::System) created by [`System::launch`](crate::System::launch).
///
/// The only use of this handle is to shut down the [`System`](crate::System).
/// Call [`initiate_shutdown`](SystemHandle::initiate_shutdown) for explicit shutdown with ability to wait for shutdown completion.
///
/// Alternatively, `drop` will also request shutdown, but not wait for completion of shutdown.
///
/// This handle is [`Send`] but not [`Clone`].
/// While it's possible to wrap it in an `Arc<Mutex<_>>`, you probably want to look into [`crate::thread_local_system`] instead.
pub struct SystemHandle {
    inner: Option<SystemHandleInner>,
}

struct SystemHandleInner {
    #[allow(dead_code)]
    pub(super) id: usize,
    pub(crate) submit_side: SubmitSide,
}

impl SystemHandle {
    pub(crate) fn new(id: usize, submit_side: SubmitSide) -> Self {
        SystemHandle {
            inner: Some(SystemHandleInner { id, submit_side }),
        }
    }

    /// Initiate system shtudown and return a future to await completion of shutdown.
    ///
    /// It is not necessary to poll the returned future to initiate shutdown; it is
    /// just to await completion of orderly shutdown.
    ///
    /// After the call to this function returns, it is guaranteed that all subsequent attempts
    /// to start new operations will fail with a custom [`std::io::Error`].
    ///
    /// Operations started before we initiated shutdown that have not been submitted
    /// to the kernel yet will fail in the same way.
    ///
    /// Operations started before we initiated shutdown that *have* been submitted to the kernel
    /// remain active. It is safe to drop future that awaits the operation, but
    /// operation itself is not cancelled. When dropping, ownership of buffers or other
    /// resources that are owned by the kernel while the operation is in-flight moves to the
    /// [`System`](crate::System) until the operation completes.
    /// (TODO: use io_uring features to cancel in-flight operations).
    ///
    /// If the poller task gets cancelled, e.g., because the tokio runtime is being shut down,
    /// the shutdown procedure makes sure to continue in a new `std::thread`.
    ///
    /// So, it is safe to drop the tokio runtime on which the poller task runs.
    pub fn initiate_shutdown(mut self) -> impl std::future::Future<Output = ()> + Send {
        let inner = self
            .inner
            .take()
            .expect("we only consume here and during Drop");
        inner.shutdown()
    }
}

struct WaitShutdownFut {
    done_rx: tokio::sync::oneshot::Receiver<()>,
}

impl std::future::Future for WaitShutdownFut {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let done_rx = &mut self.done_rx;
        match ready!(done_rx.poll_unpin(cx)) {
            Ok(()) => std::task::Poll::Ready(()),
            Err(_) => panic!("implementation error: poller must not die before SystemHandle"),
        }
    }
}

impl SystemHandleInner {
    fn shutdown(self) -> impl std::future::Future<Output = ()> + Send {
        self.submit_side.shutdown()
    }
}

impl crate::SystemHandle {
    pub fn nop(
        &self,
    ) -> impl std::future::Future<
        Output = (
            (),
            Result<(), crate::system::submission::op_fut::Error<std::io::Error>>,
        ),
    > {
        let op = crate::ops::nop::Nop {};
        let inner = self.inner.as_ref().unwrap();
        execute_op(op, inner.submit_side.weak(), None)
    }
    pub fn read<F: IoFd + Send, B: BoundedBufMut + Send>(
        &self,
        file: F,
        offset: u64,
        buf: B,
    ) -> impl std::future::Future<
        Output = (
            (F, B),
            Result<usize, crate::system::submission::op_fut::Error<std::io::Error>>,
        ),
    > {
        let op = ReadOp { file, offset, buf };
        let inner = self.inner.as_ref().unwrap();
        execute_op(op, inner.submit_side.weak(), None)
    }
    pub fn open<P: AsRef<Path>>(
        &self,
        path: P,
        options: &crate::ops::open_at::OpenOptions,
    ) -> impl std::future::Future<
        Output = Result<OwnedFd, crate::system::submission::op_fut::Error<std::io::Error>>,
    > {
        let op = match OpenAtOp::new_cwd(path.as_ref(), options) {
            Ok(op) => op,
            Err(e) => {
                return futures::future::Either::Left(async move {
                    Err(crate::system::submission::op_fut::Error::Op(e))
                })
            }
        };
        let inner = self.inner.as_ref().unwrap();
        let weak = inner.submit_side.weak();
        futures::future::Either::Right(async move {
            let (_, res) = execute_op(op, weak, None).await;
            res
        })
    }

    pub async fn fsync<F: IoFd + Send>(
        &self,
        file: F,
    ) -> (
        F,
        Result<(), crate::system::submission::op_fut::Error<std::io::Error>>,
    ) {
        let op = FsyncOp {
            file,
            flags: uring_common::io_uring::types::FsyncFlags::empty(),
        };
        let inner = self.inner.as_ref().unwrap();
        execute_op(op, inner.submit_side.weak(), None).await
    }

    pub async fn fdatasync<F: IoFd + Send>(
        &self,
        file: F,
    ) -> (
        F,
        Result<(), crate::system::submission::op_fut::Error<std::io::Error>>,
    ) {
        let op = FsyncOp {
            file,
            flags: uring_common::io_uring::types::FsyncFlags::DATASYNC,
        };
        let inner = self.inner.as_ref().unwrap();
        execute_op(op, inner.submit_side.weak(), None).await
    }

    pub async fn statx<F: IoFd + Send>(
        &self,
        file: F,
    ) -> (
        F,
        Result<
            Box<uring_common::libc::statx>,
            crate::system::submission::op_fut::Error<std::io::Error>,
        >,
    ) {
        // TODO: avoid the allocation? allow callers to provide their own buffer?
        let buf: Box<uring_common::libc::statx> = Box::new(
            // TODO replace with Box<MaybeUninit>, https://github.com/rust-lang/rust/issues/63291
            // SAFETY: we only use the memory if the fstat succeeds, should be using MaybeUninit here.
            unsafe { std::mem::zeroed() },
        );
        let op = StatxOp::ByFileDescriptor {
            file,
            statxbuf: SubmittingBox::NotSubmitting(buf),
        };
        let inner = self.inner.as_ref().unwrap();
        let (resources, result) = execute_op(op, inner.submit_side.weak(), None).await;
        let crate::ops::statx::Resources::ByFileDescriptor { file, statxbuf } = resources;
        match result {
            Ok(()) => (file, Ok(statxbuf)),
            Err(e) => (file, Err(e)),
        }
    }
}
