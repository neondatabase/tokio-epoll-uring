//! Owned handle to an explicitly [`System::launch`]ed system.

use std::os::fd::OwnedFd;

use tokio_uring::buf::IoBufMut;

use crate::{
    ops::{read::ReadOp, OpFut},
    system::submission::PlugError,
    Ops,
};

use super::{ShutdownRequest, SubmitSide};

/// Owned handle to the [`System`](crate::System) created by [`System::launch`](crate::System::launch).
///
/// The only use of this handle is to shut down the [`System`](crate::System).
/// Call [`initiate_shutdown`](SystemHandle::initiate_shutdown) for explicit shutdown with ability to wait for shutdown completion.
///
/// Alternatively, `drop` will also request shutdown, but not wait for completion of shutdown.
///
/// This handle is [`Send`] but not [`Clone`].
/// If you need to share it between threads, use [`SharedSystemHandle`](crate::SharedSystemHandle).
pub struct SystemHandle {
    pub(crate) state: SystemHandleState,
}

pub(crate) enum SystemHandleState {
    KeepSystemAlive(SystemHandleLive),
    ExplicitShutdownRequestOngoing,
    ExplicitShutdownRequestDone,
    ImplicitShutdownRequestThroughDropOngoing,
    ImplicitShutdownRequestThroughDropDone,
}
pub(crate) struct SystemHandleLive {
    #[allow(dead_code)]
    pub(super) id: usize,
    pub(crate) submit_side: SubmitSide,
    pub(super) shutdown_tx: crate::util::oneshot_nonconsuming::SendOnce<ShutdownRequest>,
}

impl Drop for SystemHandle {
    fn drop(&mut self) {
        let cur = std::mem::replace(
            &mut self.state,
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing,
        );
        match cur {
            SystemHandleState::ExplicitShutdownRequestOngoing => {
                panic!("implementation error, likely panic during explicit shutdown")
            }
            SystemHandleState::ExplicitShutdownRequestDone => (), // nothing to do
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing => {
                unreachable!("only this function sets that state, and it gets called exactly once")
            }
            SystemHandleState::ImplicitShutdownRequestThroughDropDone => {
                unreachable!("only this function sets that state, and it gets called exactly once")
            }
            SystemHandleState::KeepSystemAlive(live) => {
                let _ = live.shutdown(); // we don't care about the result
                self.state = SystemHandleState::ImplicitShutdownRequestThroughDropDone;
            }
        }
    }
}

impl SystemHandle {
    /// Initiate system shtudown and return a future to await completion of shutdown.
    ///
    /// It is not necessary to poll the returned future to initiate shutdown; it is
    /// just to await completion of orderly shutdown.
    ///
    /// After the call to this function returns, it is guaranteed that all subsequent attempts
    /// to start new operations will fail with a custom [`std::io::Error`].
    /// I.e., subsequent `crate::read().await` will fail with an error.
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
        let cur = std::mem::replace(
            &mut self.state,
            SystemHandleState::ExplicitShutdownRequestOngoing,
        );
        match cur {
            SystemHandleState::ExplicitShutdownRequestOngoing => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::ExplicitShutdownRequestDone => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::ImplicitShutdownRequestThroughDropDone => {
                unreachable!("this function consumes self")
            }
            SystemHandleState::KeepSystemAlive(live) => {
                let ret = live.shutdown();
                self.state = SystemHandleState::ExplicitShutdownRequestDone;
                ret
            }
        }
    }
}

impl SystemHandleState {
    pub(crate) fn guaranteed_live(&self) -> &SystemHandleLive {
        match self {
            SystemHandleState::ExplicitShutdownRequestOngoing => unreachable!("caller guarantees that system handle is live, but it is in state ExplicitShutdownRequestOngoing"),
            SystemHandleState::ExplicitShutdownRequestDone => unreachable!("caller guarantees that system handle is live, but it is in state ExplicitShutdownRequestDone"),
            SystemHandleState::ImplicitShutdownRequestThroughDropOngoing => unreachable!("caller guarantees that system handle is live, but it is in state ImplicitShutdownRequestThroughDrop"),
            SystemHandleState::ImplicitShutdownRequestThroughDropDone => unreachable!("caller guarantees that system handle is live, but it is in state ImplicitShutdownRequestThroughDropDone"),
            SystemHandleState::KeepSystemAlive(live) => live,
        }
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

impl SystemHandleLive {
    fn shutdown(self) -> impl std::future::Future<Output = ()> + Send + Unpin {
        let SystemHandleLive {
            id: _,
            submit_side,
            shutdown_tx,
        } = self;
        let mut submit_side_guard = submit_side.0.lock().unwrap();
        let open_state = match submit_side_guard.plug() {
            Ok(open_state) => open_state,
            Err(PlugError::AlreadyPlugged) => panic!(
                "implementation error: its solely the SystemHandle's job to plug the submit side"
            ),
        };
        drop(submit_side_guard);
        drop(submit_side);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let req = ShutdownRequest {
            open_state,
            done_tx,
        };
        shutdown_tx
            .send(req)
            .ok()
            .expect("implementation error: poller task must not die before SystemHandle");
        WaitShutdownFut { done_rx }
    }
}

impl SystemHandle {
    pub(crate) fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(&self, f: F) -> R {
        f(self.state.guaranteed_live().submit_side.clone())
    }
}

impl Ops for SystemHandle {
    fn read<B: IoBufMut + Send>(&self, file: OwnedFd, offset: u64, buf: B) -> OpFut<ReadOp<B>> {
        let op = ReadOp { file, offset, buf };
        self.with_submit_side(|submit_side| OpFut::new(op, submit_side))
    }
}
