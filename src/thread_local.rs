use std::{
    ops::ControlFlow,
    os::fd::OwnedFd,
    sync::{Arc, Mutex, Weak},
};

use tokio_uring::buf::IoBufMut;

use crate::{
    ops::{read::ReadOp, OpFut, OpTrait},
    SubmitSideProvider, System, SystemHandle,
};

enum ThreadLocalState {
    Uninit,
    Launching(tokio::sync::broadcast::Sender<()>),
    Launched(SystemHandle),
    Undefined,
}

thread_local! {
    static THREAD_LOCAL: std::sync::Arc<Mutex<ThreadLocalState>> = Arc::new(Mutex::new(ThreadLocalState::Uninit));
}

pub async fn submit<O>(
    op: O,
) -> (
    O::Resources,
    Result<O::Success, crate::ops::Error<O::Error>>,
)
where
    O: OpTrait + Send + Unpin,
{
    let mut read_op_storage = Some(op);
    loop {
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
                                // thread / thread-local got destroyed while we were launching
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
                ThreadLocalState::Launching(tx) => ControlFlow::Continue((read_op, tx.subscribe())),
                ThreadLocalState::Launched(system) => ControlFlow::Break(
                    system.with_submit_side(|submit_side| OpFut::new(read_op, submit_side)),
                ),
            }
        });
        match cf {
            ControlFlow::Break(fut) => break fut.await,
            ControlFlow::Continue((read_op, mut waiter)) => {
                // this may move us to another thread; doesn't matter, we start over there
                let _ = waiter.recv().await;
                let replaced = read_op_storage.replace(read_op);
                debug_assert!(replaced.is_none());
                continue;
            }
        }
    }
}

pub async fn read<B: IoBufMut + Send>(
    file: OwnedFd,
    offset: u64,
    buf: B,
) -> (
    (OwnedFd, B),
    Result<usize, crate::ops::Error<std::io::Error>>,
) {
    let op = ReadOp { file, offset, buf };
    submit(op).await
}
