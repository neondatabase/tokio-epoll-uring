use std::sync::{Arc, RwLock};

use crate::system::{SubmitSide, SystemHandle};
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
            guard.submit_side.clone()
        })
    }
}

impl SharedSystemHandle {
    pub fn launch() -> Self {
        Self(Arc::new(RwLock::new(Some(System::launch()))))
    }

    /// Plug the submission queue; new operation submissions will cause panics.
    /// Existing operations will be allowed to complete.
    /// Returns a oneshot receiver that will be signalled when all operations have completed.
    ///
    /// This function panics if it's called more than once (i.e., on another clone of the wrapped handle).
    pub fn shutdown(self) -> tokio::sync::oneshot::Receiver<()> {
        self.0
            .write()
            .unwrap()
            .take()
            .expect("SharedSystemHandle already shut down")
            .shutdown()
    }
}
