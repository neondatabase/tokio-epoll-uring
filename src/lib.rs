//! This crate enables high-performance use of Linux's `io_uring` from vanilla `tokio`.
//!
//! # Usage
//!
//! 1. Launch a [`System`] to get a [`SystemHandle`].
//! 2. On the handle, call the [`Ops`] method that corresponds to the desired io_uring operation.
//!    The method/returned future will *own* resources such as buffers and file descriptors
//!    until the operation completes.
//! 4. Await the future returned by the invocation.
//! 5. Get back the resources and a result.
//! 6. Inspect the result and proceed accordingly.
//!
//! Transfer of ownership of resources is required with io_uring because
//! the kernel owns the resources while the operation is in flight.
//! This is different from `std` and `tokio` IO traits, which typically *borrow* resources.
//! More background: withoutboats's [notes on io_uring](https://without.boats/blog/io-uring/).
//!
//! ## Example 1: Explicitly Launched System
//!
//! ```rust
//! #[tokio::main]
//! async fn main() {
//!   use tokio_epoll_uring::Ops;
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
//! ## Example 2: Thread-Local System
//!
//! On a multi-core system, you'll want a separate [`System`] per executor thread to minimize the need for coordination during submission.
//! The [`with_thread_local_system`] provides an out-of-the-box solution to lazily launch a [`System`] for the current executor thread.
//!
//! ```rust
//! #[tokio::main]
//! async fn main() {
//!     let mut tasks = Vec::new();
//!     for i in (0..100) {
//!         tasks.push(tokio::spawn(mytask(i)));
//!     }
//!     for jh in tasks {
//!         jh.await;
//!     }
//! }
//!
//! async fn mytask(i: usize) {
//!     let file = std::fs::File::open("/dev/zero").unwrap();
//!     let fd: std::os::fd::OwnedFd = file.into();
//!     let buf = vec![1; 1024];
//!     let ((_, _), res) = tokio_epoll_uring::with_thread_local_system(|system| {
//!         use tokio_epoll_uring::Ops;
//!         system.read(fd, 0, buf)
//!     }).await;
//!     println!("task {i} result: {res:?}");
//! }
//! ```
//!
//! # Motivation, Design, Benchmarks
//!
//! See [`crate::doc`].

pub(crate) mod sealed {
    pub trait Sealed {}
}

pub mod doc;

pub mod ops;
pub use ops::Ops;

mod system;

pub use system::lifecycle::handle::SystemHandle;
pub use system::lifecycle::thread_local::with_thread_local_system;
pub use system::lifecycle::System;

pub(crate) mod util;

// pub mod shared_system_handle;
