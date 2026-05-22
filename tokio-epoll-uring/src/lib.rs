//! This crate enables high-performance use of Linux's `io_uring` from vanilla `tokio`.
//!
//! # Usage
//!
//! 1. Launch a [`System`] to get a [`SystemHandle`].
//! 2. On the handle, call method that corresponds to the desired io_uring operation.
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
//!   // Launch the uring system.
//!   let system = tokio_epoll_uring::System::launch().await.unwrap();
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
//! The [`thread_local_system`] provides an out-of-the-box solution to lazily launch a [`System`] for the current executor thread.
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
//!     let system = tokio_epoll_uring::thread_local_system().await;
//!     let ((_, _), res) = system.read(fd, 0, buf).await;
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

#[cfg(target_os = "linux")]
pub mod ops;

#[cfg(target_os = "linux")]
mod system;

#[cfg(target_os = "linux")]
pub use {
    crate::system::submission::op_fut::Error,
    system::lifecycle::handle::SystemHandle,
    system::lifecycle::thread_local::{thread_local_system, Handle},
    system::lifecycle::LaunchResult,
    system::lifecycle::System,
    system::submission::op_fut::Error as SystemError,
};

pub use uring_common::buf::{BoundedBuf, BoundedBufMut, IoBuf, IoBufMut, Slice};
pub use uring_common::io_fd::IoFd;

pub(crate) mod util;

pub mod metrics;

/// Which io_uring backend powers a [`System`].
///
/// Set process-wide via [`set_default_backend`] before launching any [`System`].
/// Defaults to [`Backend::SideRing`] if never set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// tokio-epoll-uring runs its own io_uring ring on a dedicated poller task
    /// driven by epoll on the ring's fd. The historical default.
    SideRing,
    /// SQEs are submitted through tokio's own ring via the public
    /// `tokio::io_uring` API. No side ring, no poller, no slots — tokio owns
    /// the ring and SQ/CQ. Requires the tokio runtime to be built with the
    /// `io-uring` feature enabled.
    TokioNative,
}

static DEFAULT_BACKEND: std::sync::OnceLock<Backend> = std::sync::OnceLock::new();

/// Returned by [`set_default_backend`] when a *different* backend was already set.
#[derive(Debug, Clone, Copy)]
pub struct BackendAlreadySet {
    pub existing: Backend,
    pub attempted: Backend,
}

impl std::fmt::Display for BackendAlreadySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tokio-epoll-uring default backend already set to {:?}, cannot change to {:?}",
            self.existing, self.attempted
        )
    }
}

impl std::error::Error for BackendAlreadySet {}

/// Set the backend used for [`System`]s launched after this call. Call once at
/// process startup, before the first [`System::launch`] or
/// [`thread_local_system`] usage.
///
/// Idempotent if called multiple times with the same value. Returns
/// [`BackendAlreadySet`] if a *different* backend was already set.
pub fn set_default_backend(backend: Backend) -> Result<(), BackendAlreadySet> {
    match DEFAULT_BACKEND.set(backend) {
        Ok(()) => Ok(()),
        Err(_) => {
            let existing = *DEFAULT_BACKEND.get().expect("set above");
            if existing == backend {
                Ok(())
            } else {
                Err(BackendAlreadySet {
                    existing,
                    attempted: backend,
                })
            }
        }
    }
}

/// The configured default backend, or [`Backend::SideRing`] if
/// [`set_default_backend`] was never called.
pub fn default_backend() -> Backend {
    DEFAULT_BACKEND.get().copied().unwrap_or(Backend::SideRing)
}

#[doc(hidden)]
pub mod env_tunables {
    pub(crate) static YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL: once_cell::sync::Lazy<bool> =
        once_cell::sync::Lazy::new(|| {
            std::env::var("EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL")
            .map(|v| v == "1")
            .unwrap_or_else(|e| match e {
                std::env::VarError::NotPresent => true, // default-on
                std::env::VarError::NotUnicode(_) => panic!("EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL must be a unicode string"),
            })
        });
    pub(crate) static PROCESS_COMPLETIONS_ON_QUEUE_FULL: once_cell::sync::Lazy<bool> =
        once_cell::sync::Lazy::new(|| {
            std::env::var("EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL")
                .map(|v| v == "1")
                .unwrap_or_else(|e| match e {
                    std::env::VarError::NotPresent => true, // default-on
                    std::env::VarError::NotUnicode(_) => panic!(
                        "EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL must be a unicode string"
                    ),
                })
        });
    pub(crate) static PROCESS_COMPLETIONS_ON_SUBMIT: once_cell::sync::Lazy<bool> =
        once_cell::sync::Lazy::new(|| {
            std::env::var("EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT")
                .map(|v| v == "1")
                .unwrap_or_else(|e| match e {
                    std::env::VarError::NotPresent => true, // default-on
                    std::env::VarError::NotUnicode(_) => {
                        panic!("EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT must be a unicode string")
                    }
                })
        });
    pub fn assert_no_unknown_env_vars() {
        std::env::vars()
            .filter_map(|(v, _)| {
                if v.starts_with("EPOLL_URING_") {
                    Some(v)
                } else {
                    None
                }
            })
            .for_each(|v| match v.as_str() {
                "EPOLL_URING_YIELD_TO_EXECUTOR_IF_READY_ON_FIRST_POLL"
                | "EPOLL_URING_PROCESS_COMPLETIONS_ON_QUEUE_FULL"
                | "EPOLL_URING_PROCESS_COMPLETIONS_ON_SUBMIT" => {}
                x => panic!("env var starts with EPOLL_URING but is not an env_tunable: {x:?}"),
            });
    }
}
