use std::{
    borrow::Cow,
    os::fd::{AsRawFd, OwnedFd},
    pin::Pin,
};

use tracing::trace;

use crate::rest::{GetOpsSlotFut, InflightOpHandle, ResourcesOwnedByOp, SystemTrait};

enum PreadvCompletionFutState<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    Created {
        file: OwnedFd,
        offset: u64,
        buf: B,
    },
    WaitingForOpSlot {
        file: OwnedFd,
        offset: u64,
        buf: B,
        wakeup: GetOpsSlotFut,
    },
    Submitted {
        inflight_op_handle: InflightOpHandle<Preadv<B>>,
    },
    ReadyPolled,
    Undefined,
    Dropped,
}

pub(crate) struct PreadvCompletionFut<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> {
    system: S,
    state: PreadvCompletionFutState<B>,
}

impl<S: SystemTrait, B: tokio_uring::buf::IoBufMut + Send> PreadvCompletionFut<S, B> {
    pub fn new(system: S, file: OwnedFd, offset: u64, buf: B) -> Self {
        PreadvCompletionFut {
            system,
            state: PreadvCompletionFutState::Created { file, offset, buf },
        }
    }
}

struct Preadv<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    file: OwnedFd,
    buf: B,
}

impl<B> ResourcesOwnedByOp for Preadv<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    type OpResult = (OwnedFd, B, std::io::Result<usize>);

    fn on_op_completion(mut self, res: i32) -> Self::OpResult {
        // https://man.archlinux.org/man/io_uring_prep_read.3.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            unsafe { tokio_uring::buf::IoBufMut::set_init(&mut self.buf, res as usize) };
            Ok(res as usize)
        };
        (self.file, self.buf, res)
    }
}

pub(crate) type PreadvOutput<B> = (OwnedFd, B, std::io::Result<usize>);

impl<S, B> std::future::Future for PreadvCompletionFut<S, B>
where
    S: SystemTrait,
    B: tokio_uring::buf::IoBufMut + Send,
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
                PreadvCompletionFutState::Created { file, offset, buf } => {
                    self.state = PreadvCompletionFutState::WaitingForOpSlot {
                        file,
                        offset,
                        buf,
                        wakeup: self
                            .system
                            .with_submit_side(|submit_side| submit_side.get_ops_slot()),
                    };
                    continue;
                }
                PreadvCompletionFutState::WaitingForOpSlot {
                    mut wakeup,
                    buf,
                    file,
                    offset,
                } => {
                    match {
                        let wakeup = std::pin::Pin::new(&mut wakeup);
                        wakeup.poll(cx)
                    } {
                        std::task::Poll::Ready(slot_handle) => {
                            trace!("was woken up from waiting for an available op slot");
                            let inflight_op_handle =
                                slot_handle.submit(Preadv { file, buf }, |preadv| {
                                    io_uring::opcode::Read::new(
                                        io_uring::types::Fd(preadv.file.as_raw_fd()),
                                        preadv.buf.stable_mut_ptr(),
                                        preadv.buf.bytes_total() as _,
                                    )
                                    .offset(offset)
                                    .build()
                                });
                            self.state = PreadvCompletionFutState::Submitted { inflight_op_handle };
                            continue; // so we poll for Submitted
                        }
                        std::task::Poll::Pending => {
                            trace!("not woken up to check for an available op slot yet");
                            self.state = PreadvCompletionFutState::WaitingForOpSlot {
                                file,
                                offset,
                                buf,
                                wakeup,
                            };
                            return std::task::Poll::Pending;
                        }
                    }
                }
                PreadvCompletionFutState::Submitted {
                    mut inflight_op_handle,
                } => {
                    trace!("checking op state to see if it's ready, storing waker in it if not");
                    match {
                        let pinned = Pin::new(&mut inflight_op_handle);
                        pinned.poll(cx)
                    } {
                        std::task::Poll::Ready(output) => {
                            self.state = PreadvCompletionFutState::ReadyPolled;
                            return std::task::Poll::Ready(output);
                        }
                        std::task::Poll::Pending => {
                            self.state = PreadvCompletionFutState::Submitted { inflight_op_handle };
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
