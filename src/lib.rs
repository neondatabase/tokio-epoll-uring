//! This crate enables high-performance use of Linux's `io_uring` from vanilla `tokio`.
//!
//! # Usage
//!
//! 1. Launch a [`System`].
//! 2. Call the method for the desired io_uring operation.
//!    The [`Future`](std::future::Future) that is returned by the method
//!    *owns* the resources required by the operation.
//! 4. Await the future returned by the invocation.
//! 5. Get back the resources and a result.
//! 6. Inspect the result and proceed accordingly.
//!
//! Transfer of ownership of resources is required with io_uring because
//! the kernel owns the resources while the operation is in flight.
//! This is different from `std` and `tokio` IO traits, which typically *borrow* resources.
//! We can't do borrowing with io_uring because the kernel owns
//! the resources while the operation is in flight.
//! More background: withoutboats' [notes on io_uring](https://without.boats/blog/io-uring/).
//!
//! ## Example
//!
//! ```
//! #[tokio::main]
//! async fn main() {
//!   use tokio_epoll_uring::prelude::*;
//!   use tokio_epoll_uring::System;
//!
//!   // Launch the uring system.
//!   let system = System::launch().await;
//!
//!   let file = std::fs::File::open("/dev/zero").unwrap();
//!   let fd: std::os::fd::OwnedFd = file.into();
//!   let buf = vec![1; 1024];
//!   // call & op fn and await the future; passing resource ownership
//!   let ((fd, buf), res) = system.read(fd, 0, buf).await;
//!   // we got back the resources and a result
//!   assert!(res.is_ok());
//!   assert_eq!(buf, vec![0; 1024]);
//! }
//! ```
//!
//! Check out more examples in the `./examples` directory.
//!
//! # Motivation, Design, Benchmarks
//!
//! See [`crate::doc`].

pub mod doc;

pub mod prelude {
    pub use crate::Ops;
    pub use tokio_uring::buf::IoBuf;
    pub use tokio_uring::buf::IoBufMut;
}

/// The operations that this crate supports. Use these as an entrypoint to learn the API.
pub mod ops;
use std::os::fd::OwnedFd;

mod system;
use ops::read::ReadOp;
use ops::OpFut;
pub use system::lifecycle::handle::SystemHandle;
pub use system::lifecycle::System;

pub(crate) mod util;

mod shared_system_handle;
pub use shared_system_handle::SharedSystemHandle;

use system::submission::SubmitSide;

use tokio_uring::buf::IoBufMut;

impl SubmitSideProvider for SystemHandle {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(&self, f: F) -> R {
        f(self.state.guaranteed_live().submit_side.clone())
    }
}

pub trait SubmitSideProvider: Unpin + Sized {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(&self, f: F) -> R;
}

pub trait Ops {
    fn read<B: IoBufMut + Send>(&self, file: OwnedFd, offset: u64, buf: B) -> OpFut<ReadOp<B>>;
}

impl<P: SubmitSideProvider> Ops for P {
    fn read<B: IoBufMut + Send>(&self, file: OwnedFd, offset: u64, buf: B) -> OpFut<ReadOp<B>> {
        let op = ReadOp { file, offset, buf };
        self.with_submit_side(|submit_side| OpFut::new(op, submit_side))
    }
}

pub trait ResourcesOwnedByKernel {
    type Resources;
    type Success;
    type Error;
    fn on_failed_submission(self) -> Self::Resources;
    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>);
}
