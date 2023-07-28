// use std::{
//     ops::ControlFlow,
//     os::fd::OwnedFd,
//     pin::Pin,
//     sync::{Arc, Mutex, Weak},
// };

// use tokio_uring::buf::IoBufMut;

// use crate::{
//     ops::{read::ReadOp, OpFut, OpTrait},
//     Ops, SubmitSideProvider, System, SystemHandle,
// };

// pub struct ThreadLocal;

// impl Ops for ThreadLocal {
//     fn read<B: IoBufMut + Send>(self, file: OwnedFd, offset: u64, buf: B) -> OpFut<ReadOp<B>> {
//         let op = ReadOp { file, offset, buf };
//         make_op_fut(op)
//     }
// }

// enum ThreadLocalState {
//     Uninit,
//     Launching(tokio::sync::broadcast::Sender<()>),
//     Launched(SystemHandle),
//     Undefined,
// }

// thread_local! {
//     static THREAD_LOCAL: std::sync::Arc<Mutex<ThreadLocalState>> = Arc::new(Mutex::new(ThreadLocalState::Uninit));
// }

// pub(crate) enum TrySubmitResult<O: OpTrait + Send + Unpin, R> {
//     Submitted(R),
//     WaitingForThreadLocal(O, Pin<Box<dyn std::future::Future<Output = ()>>>),
// }

// pub fn make_op_fut<O, R>(op: O) -> TrySubmitResult<O, R>
// where
//     O: OpTrait + Send + Unpin,
// {
//     THREAD_LOCAL.with(move |local_state_arc| {
//         let mut local_state = local_state_arc.lock().unwrap();
//         let cur = std::mem::replace(&mut *local_state, ThreadLocalState::Undefined);
//         match cur {
//             ThreadLocalState::Undefined => unreachable!(),
//             ThreadLocalState::Uninit => {
//                 let weak_self = Arc::downgrade(local_state_arc);
//                 let (tx, rx) = tokio::sync::broadcast::channel(1);
//                 *local_state = ThreadLocalState::Launching(tx);
//                 tokio::spawn(async move {
//                     let system_handle = System::launch().await;
//                     match Weak::upgrade(&weak_self) {
//                         None => {
//                             // thread / thread-local got destroyed while we were launching
//                             drop(system_handle);
//                         }
//                         Some(local_state_arc) => {
//                             let mut local_state_guard = local_state_arc.lock().unwrap();
//                             let cur = std::mem::replace(
//                                 &mut *local_state_guard,
//                                 ThreadLocalState::Undefined,
//                             );
//                             let tx = match cur {
//                                 ThreadLocalState::Launching(tx) => {
//                                     *local_state_guard = ThreadLocalState::Launched(system_handle);
//                                     tx
//                                 }
//                                 ThreadLocalState::Uninit
//                                 | ThreadLocalState::Launched(_)
//                                 | ThreadLocalState::Undefined => unreachable!(),
//                             };
//                             drop(local_state_guard);
//                             drop(tx);
//                         }
//                     }
//                 });
//                 TrySubmitResult::WaitingForThreadLocal(
//                     op,
//                     Box::pin(async move {
//                         let _ = rx.recv();
//                     }),
//                 )
//             }
//             ThreadLocalState::Launching(tx) => TrySubmitResult::WaitingForThreadLocal(
//                 op,
//                 Box::pin(async move {
//                     let _ = tx.subscribe();
//                 }),
//             ),
//             ThreadLocalState::Launched(system) => TrySubmitResult::Submitted(
//                 TrySubmitResult::Submitted(system.with_submit_side(|submit_side| OpFut::new(op, submit_side))),
//             ),
//         }
//     })
// }
