//! Artificially limits futures lifetime to ensure we don't shutdown system before all futures are gone.

use std::sync::{Arc, Mutex};

use crate::system::{SubmitSide, SystemHandle};
use crate::system::{SubmitSideProvider, System};

pub struct SystemLifecycle {
    system_handle: Arc<Mutex<Option<SystemHandle>>>,
}

impl SubmitSideProvider for &'_ SystemLifecycle {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(self, f: F) -> R {
        f({
            let guard = self.system_handle.lock().unwrap();
            let guard = guard.as_ref().unwrap();
            guard.submit_side.clone()
        })
    }
}

impl Drop for SystemLifecycle {
    fn drop(&mut self) {
        self.system_handle
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .shutdown();
    }
}

impl SystemLifecycle {
    pub fn new() -> Self {
        Self {
            system_handle: Arc::new(Mutex::new(Some(System::new()))),
        }
    }
}
