use std::{
    io::Write,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    time::Duration,
};

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{ResourcesOwnedByKernel, SharedSystemHandle, SubmitSideProvider, System};

use super::submission::{
    InflightOpHandle, NotInflightSlotHandle, NotInflightSlotHandleSubmitErrorKind,
};

struct MockOp {}
fn submit_mock_op(
    slot: NotInflightSlotHandle,
) -> Result<InflightOpHandle<MockOp>, NotInflightSlotHandleSubmitErrorKind> {
    impl ResourcesOwnedByKernel for MockOp {
        type OpResult = ();
        fn on_op_completion(self, _res: i32) -> Self::OpResult {}
    }
    slot.submit(MockOp {}, |_| io_uring::opcode::Nop::new().build())
        .map_err(|err| err.kind)
}

// TODO: turn into a does-not-compile test
// #[tokio::test]
// async fn get_slot_panics_if_used_after_shutdown() {
//     let handle = crate::launch_owned().await;
//     handle.shutdown().await;
//     // handle.
//     // .with_submit_side(|submit_side| {
//     //     let mut guard = submit_side.0.lock().unwrap();
//     //     let guard = guard.must_open();
//     //     guard.get_ops_slot()
//     // })
//     // .await;
// }

#[tokio::test]
async fn submit_errors_after_shutdown() {
    // tracing_subscriber::fmt::init();

    let system = SharedSystemHandle::launch().await;

    // get a slot
    let slot = system
        .clone()
        .with_submit_side(|submit_side| {
            let mut guard = submit_side.0.lock().unwrap();
            let guard = guard.must_open();
            guard.get_ops_slot()
        })
        .await
        .unwrap();

    let (shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel::<()>();
    let jh = tokio::spawn(async move {
        shutdown_started_rx.await.unwrap();
        match submit_mock_op(slot) {
            Ok(_inflight) => panic!("submissions should fail after shutdown initiated"),
            Err(NotInflightSlotHandleSubmitErrorKind::SubmitSideDropped) => {}
        }
    });
    let wait_shutdown = system.initiate_shutdown();
    shutdown_started_tx.send(()).unwrap();
    jh.await.unwrap();
    info!("waiting for shutdown to complete");
    wait_shutdown.await;
}

#[tokio::test]
async fn shutdown_waits_for_ongoing_ops() {
    // tracing_subscriber::fmt::init();

    let system = SharedSystemHandle::launch().await;
    let slot = system
        .clone()
        .with_submit_side(|submit_side| {
            let mut guard = submit_side.0.lock().unwrap();
            let guard = guard.must_open();
            guard.get_ops_slot()
        })
        .await
        .unwrap();
    let submit_fut = submit_mock_op(slot).unwrap();
    let shutdown_done = system.initiate_shutdown();
    tokio::pin!(shutdown_done);
    tokio::select! {
        // TODO don't rely on timing
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        _ = &mut shutdown_done => {
            panic!("shutdown should not complete until submit_fut is done");
        }
    }
    println!("waiting submit_fut");
    let _: () = submit_fut.await;
    println!("submit_fut is done");
    tokio::select! {
        // TODO don't rely on timing
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("shutdown should complete after submit_fut is done");
        }
        _ = &mut shutdown_done => { }
    }
}

#[tokio::test]
async fn drop_system_handle() {
    let system = System::launch().await;
    drop(system);
}

#[tokio::test]
async fn op_state_pending_but_future_dropped() {
    // Get the op slot into state PendingButFutureDropped
    // then let process_completions run and see what happens.

    let system = SharedSystemHandle::launch().await;

    let (reader, mut writer) = os_pipe::pipe().unwrap();
    let reader = unsafe { OwnedFd::from_raw_fd(nix::unistd::dup(reader.as_raw_fd()).unwrap()) };

    let buf = vec![0; 1];
    let mut read_fut = Box::pin(crate::read(
        std::future::ready(system.clone()),
        reader,
        0,
        buf,
    ));
    let stop_polling_read_fut = CancellationToken::new();
    let jh = tokio::spawn({
        let stop_polling_read_fut = stop_polling_read_fut.clone();
        async move {
            tokio::select! {
                _ = &mut read_fut => { unreachable!("we don't write to the pipe") }
                _ = stop_polling_read_fut.cancelled() => {
                    read_fut
                }
            }
        }
    });

    // TODO don't rely on timing for read_fut to reach Pending state
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!jh.is_finished());
    stop_polling_read_fut.cancel();
    let read_fut = jh.await.unwrap();

    // assert!(matches!(read_fut), ...) it's an `async fn`, can't match :(

    drop(read_fut);
    // op should be in state PendingButFutureDropped by now

    // wake up poller task to process completions
    writer.write_all(&[1]).unwrap();

    system.initiate_shutdown().await;
}

#[tokio::test]
async fn basic() {
    let system = SharedSystemHandle::launch().await;

    let (reader, mut writer) = os_pipe::pipe().unwrap();
    let reader = unsafe { OwnedFd::from_raw_fd(nix::unistd::dup(reader.as_raw_fd()).unwrap()) };

    writer.write_all(&[1]).unwrap();

    let buf = vec![0; 1];
    let (_, buf, res) = crate::read(std::future::ready(system.clone()), reader, 0, buf).await;
    let sz = res.unwrap();
    assert_eq!(sz, 1);
    assert_eq!(buf, vec![1]);

    system.initiate_shutdown().await;
}
