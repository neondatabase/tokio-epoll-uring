use std::sync::{Arc, RwLock};

use futures::Future;

use crate::{
    system::{lifecycle::handle::SystemHandleState, submission::SubmitSide},
    SubmitSideProvider, System, SystemHandle,
};

/// [`Clone`]-able wrapper around [`SystemHandle`] for sharing between threads / tokio tasks.
///
/// The downside is that shutdown is no longer modeled through the type system, so,
/// it's up to the user to ensure that shutdown is initiated at the right time.
/// If they fail, we currently panic.
#[derive(Clone)]
pub struct SharedSystemHandle(Arc<RwLock<Option<SystemHandle>>>);

impl SubmitSideProvider for SharedSystemHandle {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(self, f: F) -> R {
        f({
            let guard = self.0.read().unwrap();
            let guard = guard
                .as_ref()
                .expect("SharedSystemHandle is shut down, cannot submit new operations");
            match &guard.state {
                SystemHandleState::KeepSystemAlive(inner) => inner.submit_side.clone(),
                SystemHandleState::ExplicitShutdownRequestOngoing
                | SystemHandleState::ExplicitShutdownRequestDone
                | SystemHandleState::ImplicitShutdownRequestThroughDropOngoing
                | SystemHandleState::ImplicitShutdownRequestThroughDropDone => {
                    unreachable!(
                        "the .take() in fn shutdown plus the RwLock prevent us from reaching here"
                    )
                }
            }
        })
    }
}

impl SharedSystemHandle {
    pub async fn launch() -> Self {
        let handle = System::launch().await;
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
