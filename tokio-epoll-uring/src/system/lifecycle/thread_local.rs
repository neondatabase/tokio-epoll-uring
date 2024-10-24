//! Lazily-launched [`System`] thread-local to current tokio executor thread.

use std::sync::Arc;

use crate::{System, SystemHandle};

thread_local! {
    static THREAD_LOCAL: std::sync::Arc<tokio::sync::OnceCell<SystemHandle<()>>> = Arc::new(tokio::sync::OnceCell::const_new());
}

/// Panics if we cannot [`System::launch`].
pub async fn thread_local_system() -> Handle {
    let arc = THREAD_LOCAL.with(|arc| arc.clone());

    let _ = arc
        .get_or_init(|| async { System::launch(Arc::new(())).await.unwrap() })
        .await;

    Handle(arc)
}

#[derive(Clone)]
pub struct Handle(Arc<tokio::sync::OnceCell<SystemHandle<()>>>);

impl std::ops::Deref for Handle {
    type Target = SystemHandle<()>;

    fn deref(&self) -> &Self::Target {
        self.0
            .get()
            .expect("must be already initialized when using this")
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    #[test]
    fn test_block_on_inside_spawn_blocking_1() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let weak = rt.block_on(async move {
            tokio::task::spawn_blocking(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let system = crate::thread_local_system().await;
                    let ((), res) = system.nop().await;
                    res.unwrap();
                    Arc::downgrade(&system.0)
                })
            })
            .await
            .unwrap()
        });

        drop(rt);

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn test_block_on_inside_spawn_blocking_2() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let task = rt.spawn(async move {
            tokio::task::spawn_blocking(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let system = crate::thread_local_system().await;
                    let ((), res) = system.nop().await;
                    res.unwrap();
                    Arc::downgrade(&system.0)
                })
            })
            .await
            .unwrap()
        });

        let weak = rt.block_on(task).unwrap();

        drop(rt);

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn test_block_on_inside_spawn_blocking_3() {
        let keepalive = Duration::from_secs(4);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_keep_alive(keepalive)
            .build()
            .unwrap();

        let task = rt.spawn(async move {
            tokio::task::spawn_blocking(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let system = crate::thread_local_system().await;
                    let ((), res) = system.nop().await;
                    res.unwrap();
                    Arc::downgrade(&system.0)
                })
            })
            .await
            .unwrap()
        });

        let weak = rt.block_on(task).unwrap();

        // immediately after should succeed
        // (technically this makes the test sensitive to CPU timing but 4 secs is a long time)
        assert!(weak.upgrade().is_some());

        // add 1 second slack time to make the test less sensitive to CPU timing
        std::thread::sleep(keepalive + Duration::from_secs(1));

        // the `rt` should have downsized its blocking pool after `keepalive`
        assert!(weak.upgrade().is_none());

        drop(rt);
    }
}
