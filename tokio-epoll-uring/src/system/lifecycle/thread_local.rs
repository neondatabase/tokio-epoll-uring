//! Lazily-launched [`System`] thread-local to current tokio executor thread.

use std::sync::Arc;

use crate::{System, SystemHandle};

thread_local! {
    static THREAD_LOCAL: std::sync::Arc<tokio::sync::Mutex<tokio::sync::OnceCell<SystemHandle>>> = Arc::new(
        tokio::sync::Mutex::const_new(
        tokio::sync::OnceCell::const_new()));
}

pub async fn thread_local_system() -> Handle {
    let arc = THREAD_LOCAL.with(|arc| arc.clone());

    let guard = arc.lock_owned().await;
    guard.get_or_init(System::launch).await;

    Handle(guard)
}

pub struct Handle(tokio::sync::OwnedMutexGuard<tokio::sync::OnceCell<SystemHandle>>);

impl std::ops::Deref for Handle {
    type Target = SystemHandle;

    fn deref(&self) -> &Self::Target {
        self.0
            .get()
            .expect("must be already initialized when using this")
    }
}

impl std::ops::DerefMut for Handle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
            .get_mut()
            .expect("must be already initialized when using this")
    }
}
