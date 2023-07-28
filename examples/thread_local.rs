use std::{
    ops::ControlFlow,
    sync::{Arc, Mutex, Weak},
};

use tokio_epoll_uring::{new_read, ops::OpFut, SubmitSideProvider, System, SystemHandle};

enum ThreadLocalState {
    Uninit,
    Launching(tokio::sync::broadcast::Sender<()>),
    Launched(SystemHandle),
    Undefined,
}

thread_local! {
    static THREAD_LOCAL: std::sync::Arc<Mutex<ThreadLocalState>> = Arc::new(Mutex::new(ThreadLocalState::Uninit));
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();
    tracing::info!("starting");

    let mut jhs = Vec::new();
    for _ in 0..8 {
        jhs.push(tokio::spawn(async move {
            let mut file = tempfile::tempfile().unwrap();
            {
                use std::io::Write;
                file.write_all(&[23u8].repeat(1024)).unwrap();
                file.write_all(&[42u8].repeat(1024)).unwrap();
                file.write_all(&[67u8].repeat(1024)).unwrap();
            }

            let buf = vec![0; 2048];
            let mut read_op_storage = Some(new_read(file.into(), 512, buf));
            let fut = loop {
                let read_op = read_op_storage.take().unwrap();
                let cf = THREAD_LOCAL.with(move |local_state_arc| {
                    let mut local_state = local_state_arc.lock().unwrap();
                    let cur = std::mem::replace(&mut *local_state, ThreadLocalState::Undefined);
                    match cur {
                        ThreadLocalState::Undefined => unreachable!(),
                        ThreadLocalState::Uninit => {
                            let weak_self = Arc::downgrade(local_state_arc);
                            let (tx, rx) = tokio::sync::broadcast::channel(1);
                            *local_state = ThreadLocalState::Launching(tx);
                            tokio::spawn(async move {
                                let system_handle = System::launch().await;
                                match Weak::upgrade(&weak_self) {
                                    None => {
                                        drop(system_handle);
                                    }
                                    Some(local_state_arc) => {
                                        let mut local_state_guard = local_state_arc.lock().unwrap();
                                        let cur = std::mem::replace(
                                            &mut *local_state_guard,
                                            ThreadLocalState::Undefined,
                                        );
                                        let tx = match cur {
                                            ThreadLocalState::Launching(tx) => {
                                                *local_state_guard =
                                                    ThreadLocalState::Launched(system_handle);
                                                tx
                                            }
                                            ThreadLocalState::Uninit
                                            | ThreadLocalState::Launched(_)
                                            | ThreadLocalState::Undefined => unreachable!(),
                                        };
                                        drop(local_state_guard);
                                        drop(tx);
                                    }
                                }
                            });
                            ControlFlow::Continue((read_op, rx))
                        }
                        ThreadLocalState::Launching(tx) => {
                            ControlFlow::Continue((read_op, tx.subscribe()))
                        }
                        ThreadLocalState::Launched(system) => ControlFlow::Break(
                            system.with_submit_side(|submit_side| OpFut::new(read_op, submit_side)),
                        ),
                    }
                });
                match cf {
                    ControlFlow::Break(fut) => break fut,
                    ControlFlow::Continue((read_op, mut waiter)) => {
                        // this may move us to another thread; doesn't matter, we start over there
                        let _ = waiter.recv().await;
                        let replaced = read_op_storage.replace(read_op);
                        debug_assert!(replaced.is_none());
                        continue;
                    }
                }
            };
            let ((_file, buf), res) = fut.await;
            let read = res.unwrap();
            assert_eq!(read, 2048, "not expecting short read");
            assert_eq!(&buf[0..512], &[23u8; 512]);
            assert_eq!(&buf[512..512 + 1024], &[42u8; 1024]);
            assert_eq!(&buf[512 + 1024..512 + 1024 + 512], &[67u8; 512]);
        }));
    }

    for jh in jhs {
        jh.await.unwrap();
    }
}
