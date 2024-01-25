//! Lazily-launched [`System`] thread-local to current tokio executor thread.

use std::sync::Arc;

use crate::{System, SystemHandle};

thread_local! {
    static THREAD_LOCAL: std::sync::Arc<tokio::sync::OnceCell<SystemHandle>> = Arc::new(tokio::sync::OnceCell::const_new());
}

/// Panics if we cannot [`System::launch`].
pub async fn thread_local_system() -> Handle {
    let arc = THREAD_LOCAL.with(|arc| arc.clone());

    let _ = arc
        .get_or_init(|| async { System::launch().await.unwrap() })
        .await;

    Handle(arc)
}

#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::OnceCell<SystemHandle>>);

impl std::ops::Deref for Handle {
    type Target = SystemHandle;

    fn deref(&self) -> &Self::Target {
        self.0
            .get()
            .expect("must be already initialized when using this")
    }
}
