//! Owned handle to an explicitly [`System::launch`]ed system.

use std::os::fd::OwnedFd;

use tokio_uring::buf::IoBufMut;

use crate::{
    ops::read::ReadOp,
    system::submission::{op_fut::OpFut, SubmitSide},
    Ops,
};

use super::ShutdownRequest;

/// Owned handle to the [`System`](crate::System) created by [`System::launch`](crate::System::launch).
///
/// The only use of this handle is to shut down the [`System`](crate::System).
/// Call [`initiate_shutdown`](SystemHandle::initiate_shutdown) for explicit shutdown with ability to wait for shutdown completion.
///
/// Alternatively, `drop` will also request shutdown, but not wait for completion of shutdown.
///
/// This handle is [`Send`] but not [`Clone`].
/// While it's possible to wrap it in an `Arc<Mutex<_>>`, you probably want to look into [`crate::with_thread_local_system`] instead.
pub struct SystemHandle {
    inner: Option<SystemHandleInner>,
}

struct SystemHandleInner {
    #[allow(dead_code)]
    pub(super) id: usize,
    pub(crate) submit_side: SubmitSide,
    pub(super) shutdown_tx: crate::util::oneshot_nonconsuming::SendOnce<ShutdownRequest>,
}

impl Drop for SystemHandle {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = inner.shutdown(); // we don't care about the result
        }
    }
}

impl SystemHandle {
    pub(crate) fn new(
        id: usize,
        submit_side: SubmitSide,
        shutdown_tx: crate::util::oneshot_nonconsuming::SendOnce<ShutdownRequest>,
    ) -> Self {
        SystemHandle {
            inner: Some(SystemHandleInner {
                id,
                submit_side,
                shutdown_tx,
            }),
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
    pub fn initiate_shutdown(mut self) -> impl std::future::Future<Output = ()> + Send + Unpin {
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
        let done_rx = std::pin::Pin::new(done_rx);
        match done_rx.poll(cx) {
            std::task::Poll::Ready(res) => match res {
                Ok(()) => std::task::Poll::Ready(()),
                Err(_) => panic!("implementation error: poller must not die before SystemHandle"),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl SystemHandleInner {
    fn shutdown(self) -> impl std::future::Future<Output = ()> + Send + Unpin {
        let SystemHandleInner {
            id: _,
            submit_side,
            shutdown_tx,
        } = self;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let req = ShutdownRequest {
            done_tx,
            open_state: submit_side.plug(),
        };
        shutdown_tx
            .send(req)
            .ok()
            .expect("implementation error: poller task must not die before SystemHandle");
        WaitShutdownFut { done_rx }
    }
}

impl Ops for crate::SystemHandle {
    fn nop(&self) -> OpFut<crate::ops::nop::Nop> {
        let op = crate::ops::nop::Nop {};
        let inner = self.inner.as_ref().unwrap();
        OpFut::new(op, inner.submit_side.weak())
    }
    fn read<B: IoBufMut + Send>(&self, file: OwnedFd, offset: u64, buf: B) -> OpFut<ReadOp<B>> {
        let op = ReadOp { file, offset, buf };
        let inner = self.inner.as_ref().unwrap();
        OpFut::new(op, inner.submit_side.weak())
    }
}