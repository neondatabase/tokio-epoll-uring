use std::sync::{Arc, RwLock};

use futures::Future;

use crate::{system::submission::op_fut::OpFut, Ops, System, SystemHandle};

/// [`Clone`]-able wrapper around [`SystemHandle`] for sharing between threads / tokio tasks.
///
/// The downside over [`SystemHandle`] is that shutdown is no longer modeled through the type system, so,
/// it's up to the user to ensure that shutdown is initiated at the right time.
/// If they fail, we currently panic.
#[derive(Clone)]
pub struct SharedSystemHandle(Arc<RwLock<Option<SystemHandle>>>);

impl Ops for SharedSystemHandle {
    fn nop(&self) -> OpFut<crate::ops::nop::Nop> {
        let guard = self.0.read().unwrap();
        let guard = guard
            .as_ref()
            .expect("SharedSystemHandle is shut down, cannot submit new operations");
        guard.nop()
    }
    fn read<B: tokio_uring::buf::IoBufMut + Send>(
        &self,
        file: std::os::fd::OwnedFd,
        offset: u64,
        buf: B,
    ) -> OpFut<crate::ops::read::ReadOp<B>> {
        let guard = self.0.read().unwrap();
        let guard = guard
            .as_ref()
            .expect("SharedSystemHandle is shut down, cannot submit new operations");
        guard.read(file, offset, buf)
    }
}

#[cfg(test)]
use crate::system::completion::PollerTesting;

impl SharedSystemHandle {
    pub async fn launch() -> Self {
        let handle = System::launch().await;
        Self(Arc::new(RwLock::new(Some(handle))))
    }

    #[cfg(test)]
    pub(crate) async fn launch_with_testing(poller_testing: PollerTesting) -> Self {
        let handle = System::launch_with_testing(poller_testing).await;
        Self(Arc::new(RwLock::new(Some(handle))))
    }

    /// First shutdown call will succeed, subsequent ones on other [`Clone`]s of `self` will panic.
    ///
    /// For more details, see [`crate::SystemHandle::initiate_shutdown`].
    ///
    /// TODO: change API to return an error, using [`Arc::try_unwrap`] or similar?
    pub fn initiate_shutdown(self) -> impl Future<Output = ()> + Send + Unpin {
        self.0
            .write()
            .unwrap()
            .take()
            .expect("SharedSystemHandle already shut down")
            .initiate_shutdown()
    }
}