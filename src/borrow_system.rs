//! Artificially limits futures lifetime to ensure we don't shutdown system before all futures are gone.

use std::{
    os::fd::OwnedFd,
    sync::{Arc, Mutex},
};

use crate::preadv::{PreadvCompletionFut, PreadvOutput};

use crate::rest::{SubmitSide, System, SystemLifecycleManager};

pub struct BorrowSystem {
    system: Arc<Mutex<Option<System>>>,
}

impl SystemLifecycleManager for &'_ BorrowSystem {
    fn with_submit_side<F: FnOnce(&mut SubmitSide) -> R, R>(self, f: F) -> R {
        f(&mut *self
            .system
            .lock()
            .unwrap()
            .as_mut()
            .expect("not Drop'ed yet")
            .submit_side
            .lock()
            .unwrap())
    }
}

impl Drop for BorrowSystem {
    fn drop(&mut self) {
        self.system.lock().unwrap().take().unwrap().shutdown();
    }
}

impl BorrowSystem {
    pub fn new() -> Self {
        Self {
            system: Arc::new(Mutex::new(Some(System::new()))),
        }
    }
    pub fn preadv<B: tokio_uring::buf::IoBufMut + Send>(
        &self,
        file: OwnedFd,
        offset: u64,
        buf: B,
    ) -> impl std::future::Future<Output = PreadvOutput<B>> + Send + '_ {
        PreadvCompletionFut::new(self, file, offset, buf)
    }
}
