//! Parent module for the [`crate::Ops`] trait and returned futures.

use std::{os::fd::OwnedFd, pin::Pin};

use tokio_uring::buf::IoBufMut;

#[doc(inline)]
pub use crate::system::submission::op_fut::Op;

pub mod nop;
pub mod read;

/// The io_uring operations supported by this crate.
///
/// Use directly on any of the "Implementors" (see below)
/// or inside the closure passed to [`crate::with_thread_local_system`].
pub trait Ops {
    /// See <https://man.archlinux.org/man/extra/liburing/io_uring_prep_nop.3.en>.
    fn nop(
        &self,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = (
                        (),
                        Result<(), crate::system::submission::op_fut::Error<std::io::Error>>,
                    ),
                >
                + 'static
                + Send,
        >,
    >;
    /// Read up to `buf.bytes_total()` bytes from the given `file` at given `offset` into `buf`.
    ///
    /// The output of the future is an `std::io::Result`, with `Ok(usize)` indicating the number of bytes read into `buf`.
    ///
    /// See also <https://man.archlinux.org/man/extra/liburing/io_uring_prep_read.3.en>
    fn read<B: IoBufMut + Send>(
        &self,
        file: OwnedFd,
        offset: u64,
        buf: B,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = (
                        (OwnedFd, B),
                        Result<usize, crate::system::submission::op_fut::Error<std::io::Error>>,
                    ),
                >
                + 'static
                + Send,
        >,
    >;
}
