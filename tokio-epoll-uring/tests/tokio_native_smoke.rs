//! Smoke tests for the `Backend::TokioNative` backend.
//!
//! Runs in its own integration-test binary so the global `set_default_backend`
//! state doesn't conflict with other tests (which assume the default
//! side-ring backend).

use std::os::fd::OwnedFd;

fn install_tokio_native_backend() {
    // Idempotent within a process. Call before any tokio-epoll-uring System
    // is launched.
    let _ = tokio_epoll_uring::set_default_backend(tokio_epoll_uring::Backend::TokioNative);
}

#[test]
fn tokio_native_nop_roundtrip() {
    install_tokio_native_backend();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let system = tokio_epoll_uring::System::launch().await.unwrap();
        let ((), result) = system.nop().await;
        result.unwrap();
    });
}

#[test]
fn tokio_native_read_dev_zero() {
    install_tokio_native_backend();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let system = tokio_epoll_uring::System::launch().await.unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open("/dev/zero")
            .unwrap();
        let fd: OwnedFd = file.into();
        let buf = vec![0xABu8; 4096];
        let ((_fd, buf), res) = system.read(fd, 0, buf).await;
        let n = res.unwrap();
        assert_eq!(n, 4096);
        assert!(buf.iter().all(|&b| b == 0));
    });
}

#[test]
fn tokio_native_drop_in_flight() {
    install_tokio_native_backend();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let system = tokio_epoll_uring::System::launch().await.unwrap();

        // Fire 32 nops and drop the futures immediately. The upstream
        // backend should hand each op's resources off to tokio's cleanup
        // closure, which drops them when the CQE arrives.
        for _ in 0..32 {
            let fut = system.nop();
            drop(fut);
        }
        // Give tokio a chance to drain.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    });
}
