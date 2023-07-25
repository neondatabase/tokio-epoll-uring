use crate::{ResourcesOwnedByKernel, SharedSystemHandle, System, SubmitSideProvider};

use super::submission::{InflightOpHandle, NotInflightSlotHandle};

struct MockOp {}
fn submit_mock_op(slot: NotInflightSlotHandle) -> InflightOpHandle<MockOp> {
    impl ResourcesOwnedByKernel for MockOp {
        type OpResult = ();
        fn on_op_completion(self, _res: i32) -> Self::OpResult {}
    }
    let submit_fut = slot.submit(MockOp {}, |_| io_uring::opcode::Nop::new().build());
    submit_fut
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
async fn submit_panics_after_shutdown() {
    let system = SharedSystemHandle::launch().await;

    // get a slot
    let slot = system
        .clone()
        .with_submit_side(|submit_side| {
            let mut guard = submit_side.0.lock().unwrap();
            let guard = guard.must_open();
            guard.get_ops_slot()
        })
        .await;

    let (shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel::<()>();
    let jh = tokio::spawn(async move {
        shutdown_started_rx.await.unwrap();
        assert_panic::assert_panic! {
            { let _ = submit_mock_op(slot); },
            &str,
            "cannot use slot for submission, SubmitSide is already plugged"
        };
    });
    let wait_shutdown = system.initiate_shutdown();
    shutdown_started_tx.send(()).unwrap();
    jh.await.unwrap();
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
        .await;
    let submit_fut = submit_mock_op(slot);
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
