//! Artificially limits futures lifetime to ensure we don't shutdown system before all futures are gone.

use std::{
    os::fd::OwnedFd,
    sync::{Arc, Mutex},
};

use crate::system::{SubmitSide, SystemHandle};
use crate::system::{System, SystemLifecycleManager};

pub struct BorrowSystem {
    system_handle: Arc<Mutex<Option<SystemHandle>>>,
}

impl SystemLifecycleManager for &'_ BorrowSystem {
    fn with_submit_side<F: FnOnce(Arc<Mutex<SubmitSide>>) -> R, R>(self, f: F) -> R {
        f({
            let guard = self.system_handle.lock().unwrap();
            let guard = guard.as_ref().unwrap();
            guard.submit_side.clone()
        })
    }
}

impl Drop for BorrowSystem {
    fn drop(&mut self) {
        self.system_handle
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .shutdown();
    }
}

impl BorrowSystem {
    pub fn new() -> Self {
        Self {
            system_handle: Arc::new(Mutex::new(Some(System::new()))),
        }
    }
    pub async fn preadv<B: tokio_uring::buf::IoBufMut + Send>(
        &self,
        file: OwnedFd,
        offset: u64,
        buf: B,
    ) -> crate::ops::PreadvOutput<B> {
        crate::ops::preadv(self, file, offset, buf).await
    }
}
