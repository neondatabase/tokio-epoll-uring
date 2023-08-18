use std::{fmt::Display, pin::Pin, sync::Arc};

/// An io_uring operation and the resources it operates on.
///
/// For each io_uring operation, there is a struct that implements this trait.
pub trait Op: crate::sealed::Sealed + Sized + Send + 'static {
    type Resources;
    type Success;
    type Error;
    fn on_failed_submission(self) -> Self::Resources;
    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>);
    fn make_sqe(&mut self) -> io_uring::squeue::Entry;
}

use futures::future;

use crate::system::{
    completion::ProcessCompletionsCause,
    slots::{self, SlotHandle},
};

use super::SubmitSide;

#[derive(Debug, thiserror::Error)]
pub enum SystemError {
    #[error("shutting down")]
    SystemShuttingDown,
}

#[derive(thiserror::Error, Debug)]
pub enum Error<T> {
    System(SystemError),
    Op(T),
}

impl<T: Display> Display for Error<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::System(e) => {
                if f.alternate() {
                    write!(f, "tokio-epoll-uring: {e:#}")
                } else {
                    write!(f, "tokio-epoll-uring: {e}")
                }
            }
            Error::Op(op) => Display::fmt(op, f),
        }
    }
}

pub(crate) fn execute_op<O>(
    op: O,
    open_guard: &mut SubmitSide,
    slot: Option<SlotHandle>,
) -> impl std::future::Future<Output = (O::Resources, Result<O::Success, Error<O::Error>>)>
where
    // FIXME: probably dont need the unpin
    O: Op + Send + 'static + Unpin,
{
    open_guard.slots.submit(op)
    // match slot {
    //     Some(slot) => Fut::A(slot.use_for_op(op)),
    //     None => {
    //         match open_guard.slots.try_get_slot() {
    //             slots::TryGetSlotResult::Draining => Fut::B(async move {
    //                 (
    //                     op.on_failed_submission(),
    //                     Err(Error::System(SystemError::SystemShuttingDown)),
    //                 )
    //             }),
    //             slots::TryGetSlotResult::GotSlot(slot) => Fut::C(slot.use_for_op(op)),
    //             slots::TryGetSlotResult::NoSlots(later) => {
    //                 // All slots are taken and we're waiting in line.
    //                 // If enabled, do some opportunistic completion processing to wake up futures that will release ops slots.
    //                 // This is in the hope that we'll wake ourselves up.

    //                 Fut::D(async move {
    //                     let slot = match later.await {
    //                         Ok(slot) => slot,
    //                         Err(_dropped) => {
    //                             return (
    //                                 op.on_failed_submission(),
    //                                 Err(Error::System(SystemError::SystemShuttingDown)),
    //                             )
    //                         }
    //                     };
    //                     slot.use_for_op(op).await
    //                 })
    //             }
    //         }
    //     }
    // }
}

// Used by `execute_op` to avoid boxing the future returned by the `with_submit_side` closure.
pub(crate) enum Fut<Output, A, B, C, D>
where
    A: std::future::Future<Output = Output>,
    B: std::future::Future<Output = Output>,
    C: std::future::Future<Output = Output>,
    D: std::future::Future<Output = Output>,
{
    A(A),
    B(B),
    C(C),
    D(D),
}
impl<Output, A, B, C, D> std::future::Future for Fut<Output, A, B, C, D>
where
    A: std::future::Future<Output = Output>,
    B: std::future::Future<Output = Output>,
    C: std::future::Future<Output = Output>,
    D: std::future::Future<Output = Output>,
{
    type Output = Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unsafe {
            match self.get_unchecked_mut() {
                Self::A(x) => Pin::new_unchecked(x).poll(cx),
                Self::B(x) => Pin::new_unchecked(x).poll(cx),
                Self::C(x) => Pin::new_unchecked(x).poll(cx),
                Self::D(x) => Pin::new_unchecked(x).poll(cx),
            }
        }
    }
}
