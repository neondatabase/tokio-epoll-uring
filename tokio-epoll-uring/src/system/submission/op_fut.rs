use std::{fmt::Display, pin::Pin};

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

use crate::system::slots::SlotHandle;

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
    _slot: Option<SlotHandle>,
) -> impl std::future::Future<Output = (O::Resources, Result<O::Success, Error<O::Error>>)>
where
    // FIXME: probably dont need the unpin
    O: Op + Send + 'static + Unpin,
{
    open_guard.slots.submit(op)
}

// Like futures::future::Either, but with 3 variants
pub(crate) enum Fut<Output, A, B, C>
where
    A: std::future::Future<Output = Output>,
    B: std::future::Future<Output = Output>,
    C: std::future::Future<Output = Output>,
{
    A(A),
    B(B),
    C(C),
}
impl<Output, A, B, C> std::future::Future for Fut<Output, A, B, C>
where
    A: std::future::Future<Output = Output>,
    B: std::future::Future<Output = Output>,
    C: std::future::Future<Output = Output>,
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
            }
        }
    }
}
