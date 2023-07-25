use crate::system::submission::PlugError;

use super::{ShutdownRequest, SubmitSide};

/// Owned handle to the [`System`] launched by [`SystemLauncher`].
///
/// The only use of this handle is to shut down the [`System`].
/// Call [`initiate_shutdown`](SystemHandle::initiate_shutdown) for explicit shutdown with ability to wait for shutdown completion.
///
/// Alternatively, `drop` will also request shutdown, but not wait for completion of shutdown.
///
/// This handle is [`Send`] but not [`Clone`].
/// If you need to share it between threads, use [`crate::SharedSystemHandle`].
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
    pub(super) shutdown_tx: crate::shutdown_request::Sender<ShutdownRequest>,
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
    /// If this call returns Ok(), it is guaranteed that no new operations
    /// will be allowed to start (TODO: not true right now, since poller task plugs the queue).
    /// Currently, such attempts will cause a panic but we may change the behavior
    /// to make it return a custom [`std::io::Error`] instead.
    ///
    /// Shutdown must wait for all in-flight operations to finish..
    /// Shutdown may spawn an `std::thread` for this purpose.
    /// It would generally be unsafe to have "force shutdown" after a timeout because
    /// in io_uring, the kernel owns resources such as memory buffers while operations are in flight.
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
        shutdown_tx.shutdown(req);
        WaitShutdownFut { done_rx }
    }
}
