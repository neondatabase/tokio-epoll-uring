use std::sync::{Arc, RwLock};

use futures::Future;

use crate::system::{SubmitSide, SystemHandle, SystemHandleState};
use crate::system::{SubmitSideProvider, System};

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

    /// Plug the submission queue; new operation submissions will cause panics.
    /// Existing operations will be allowed to complete.
    /// Returns a oneshot receiver that will be signalled when all operations have completed.
    ///
    /// This function panics if it's called more than once (i.e., on another clone of the wrapped handle).
    pub fn initiate_shutdown(self) -> impl Future<Output = ()> + Send + Unpin {
        self.0
            .write()
            .unwrap()
            .take()
            .expect("SharedSystemHandle already shut down")
            .shutdown()
    }
}
