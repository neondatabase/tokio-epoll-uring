use std::{
    borrow::Cow,
    os::fd::{AsRawFd, OwnedFd},
    pin::Pin,
};

use tracing::trace;

use crate::rest::{GetOpsSlotFut, InflightOpHandle, SystemTrait};

enum PreadvCompletionFutState<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    Created {
        file: OwnedFd,
        offset: u64,
    },
    WaitingForOpSlot {
        file: OwnedFd,
        offset: u64,
        wakeup: GetOpsSlotFut,
    },
    Submitted {
        file: OwnedFd,
        inflight_op_handle: InflightOpHandle<B>,
    },
    ReadyPolled,
    Undefined,
    Dropped,
}

pub(crate) struct PreadvCompletionFut<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> {
    system: S,
    state: PreadvCompletionFutState<B>,
    buf: Option<B>, // beocmes None in `drop()`, Some otherwise
    // make it !Send because the `idx` is thread-local
    _types: std::marker::PhantomData<B>,
}

impl<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> PreadvCompletionFut<S, B> {
    pub fn new(system: S, file: OwnedFd, offset: u64, buf: B) -> Self {
        PreadvCompletionFut {
            system,
            buf: Some(buf),
            _types: std::marker::PhantomData,
            state: PreadvCompletionFutState::Created { file, offset },
        }
    }
}

pub(crate) type PreadvOutput<B> = (OwnedFd, B, std::io::Result<usize>);

impl<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> std::future::Future
    for PreadvCompletionFut<S, B>
{
    type Output = PreadvOutput<B>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        trace!(
            "polling future in state {:?}",
            self.state.discriminant_str()
        );
        loop {
            let cur = std::mem::replace(&mut self.state, PreadvCompletionFutState::Undefined);
            match cur {
                PreadvCompletionFutState::Undefined => {
                    unreachable!("future is in undefined state")
                }
                PreadvCompletionFutState::Dropped => {
                    unreachable!("future is in dropped state, but we're in poll() right now, so, can't be dropped")
                }
                PreadvCompletionFutState::ReadyPolled => {
                    panic!("must not poll future after observing ready")
                }
                PreadvCompletionFutState::Created { file, offset } => {
                    self.state = PreadvCompletionFutState::WaitingForOpSlot {
                        file,
                        offset,
                        wakeup: self
                            .system
                            .with_submit_side(|submit_side| submit_side.get_ops_slot()),
                    };
                    continue;
                }
                PreadvCompletionFutState::WaitingForOpSlot {
                    mut wakeup,
                    file,
                    offset,
                } => {
                    match {
                        let wakeup = std::pin::Pin::new(&mut wakeup);
                        wakeup.poll(cx)
                    } {
                        std::task::Poll::Ready(slot_handle) => {
                            trace!("was woken up from waiting for an available op slot");
                            let mut buf = self.buf.take().unwrap();
                            let iov = libc::iovec {
                                iov_base: buf.stable_mut_ptr() as *mut libc::c_void,
                                iov_len: buf.bytes_total(),
                            };
                            let inflight_op_handle = slot_handle.submit(buf, |buf| {
                                io_uring::opcode::Readv::new(
                                    io_uring::types::Fd(file.as_raw_fd()),
                                    &iov as *const _,
                                    1,
                                )
                                .offset(offset)
                                .build()
                            });
                            self.state = PreadvCompletionFutState::Submitted {
                                file,
                                inflight_op_handle,
                            };
                            continue; // so we poll for Submitted
                        }
                        std::task::Poll::Pending => {
                            trace!("not woken up to check for an available op slot yet");
                            self.state = PreadvCompletionFutState::WaitingForOpSlot {
                                file,
                                offset,
                                wakeup,
                            };
                            return std::task::Poll::Pending;
                        }
                    }
                }
                PreadvCompletionFutState::Submitted {
                    file,
                    mut inflight_op_handle,
                } => {
                    trace!("checking op state to see if it's ready, storing waker in it if not");
                    match {
                        let pinned = Pin::new(&mut inflight_op_handle);
                        pinned.poll(cx)
                    } {
                        std::task::Poll::Ready((buf, res)) => {
                            self.state = PreadvCompletionFutState::ReadyPolled;
                            return std::task::Poll::Ready((file, buf, res));
                        }
                        std::task::Poll::Pending => {
                            self.state = PreadvCompletionFutState::Submitted {
                                file,
                                inflight_op_handle,
                            };
                            return std::task::Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> Drop for PreadvCompletionFut<S, B> {
    fn drop(&mut self) {
        let cur = std::mem::replace(&mut self.state, PreadvCompletionFutState::Dropped);
        match cur {
            PreadvCompletionFutState::Undefined => unreachable!("future is in undefined state"),
            PreadvCompletionFutState::Dropped => {
                unreachable!("future is in dropped state, but we're in drop() right now")
            }
            PreadvCompletionFutState::Created { .. } => (),
            PreadvCompletionFutState::WaitingForOpSlot { .. } => (),
            PreadvCompletionFutState::ReadyPolled => (),
            PreadvCompletionFutState::Submitted {
                inflight_op_handle, ..
            } => {
                drop(inflight_op_handle) // dropping moves ownership of the buffer from inflight_op_handle to the OpState
            }
        }
    }
}
impl<B> PreadvCompletionFutState<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    fn discriminant_str(&self) -> Cow<'static, str> {
        Cow::Borrowed(match self {
            PreadvCompletionFutState::WaitingForOpSlot { wakeup, .. } => {
                return Cow::Owned(format!(
                    "waiting_for_op_slot({})",
                    wakeup.state_discriminant_str()
                ));
            }
            PreadvCompletionFutState::Created { .. } => "created",
            PreadvCompletionFutState::Submitted { .. } => "submitted",
            PreadvCompletionFutState::ReadyPolled => "ready_polled",
            PreadvCompletionFutState::Undefined => "undefined",
            PreadvCompletionFutState::Dropped => "dropped",
        })
    }
}
