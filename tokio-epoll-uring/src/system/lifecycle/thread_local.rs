//! Lazily-launched [`System`] thread-local to current tokio executor thread.

use std::sync::Arc;

use crate::{system::submission::op_fut::Op, System, SystemHandle};

thread_local! {
    static THREAD_LOCAL: std::sync::Arc<tokio::sync::OnceCell<SystemHandle>> = Arc::new(tokio::sync::OnceCell::const_new());
}

/// Submit [`crate::Ops`] to a lazily launched [`System`]
/// that is thread-local to the current executor thread.
///
/// ```
/// async {
///     use crate::tokio_epoll_uring::Ops;
///     let file = std::fs::File::open("/dev/null").unwrap();
///     let file: std::os::fd::OwnedFd = file.into();
///     let buf = vec![0; 2048];
///     let ((_, _), res) =
///     tokio_epoll_uring::with_thread_local_system(|system| {
///         system.read(file, 0, buf)
///     })
///     .await;
/// };
/// ```
///
pub async fn with_thread_local_system<F, O>(make_op: F) -> <OpFut<O> as std::future::Future>::Output
where
    O: Op + Unpin + Send,
    F: Send + 'static + FnOnce(&'_ SystemHandle) -> OpFut<O>,
{
    let arc = THREAD_LOCAL.with(|arc| arc.clone());

    let system = arc.get_or_init(System::launch).await;

    let op = make_op(system);

    op.await
}
