use std::os::fd::{AsRawFd, OwnedFd, RawFd};

/// An `io-uring` compatible file file-descriptor-wrapping struct.
///
/// This trait is implemented by structs that hold ownership of a file descriptor.
///
/// Think of this as [`crate::buf::IoBuf`], but for file descriptors.
pub trait IoFd: Unpin + 'static {
    /// # Safety
    ///
    /// The implementation must ensure that, while the runtime
    /// owns the value, the fd returned by this method
    /// 1. remains valid (i.e., is not closed),
    /// 2. points to the same kernel resource.
    unsafe fn as_fd(&self) -> RawFd;
}

impl IoFd for OwnedFd {
    unsafe fn as_fd(&self) -> RawFd {
        // SAFETY: `OwnedFd` is the definition of the requirements in the trait method.
        self.as_raw_fd()
    }
}

impl IoFd for std::fs::File {
    unsafe fn as_fd(&self) -> RawFd {
        // SAFETY: `File` is the definition of the requirements in the trait method.
        self.as_raw_fd()
    }
}

pub trait IoFdMut: IoFd {
    /// # Safety
    ///
    /// In addition to the requirements in [`IoFd::as_fd`],
    /// the implementation must ensure that the state of the kernel resource
    /// referenced by the file descriptor does not change.
    ///
    /// The reason for this requirement is that the runtime may be
    /// using the file descriptor in an operation the modifies the kernel resource,
    /// and the higher-level APIs of the runtime may use `&mut`-ness to prevent race conditions.
    ///
    /// For example, the higher-level API may require `&mut self` for `seek` because `seek`
    /// modifies the cursor position of the underlying file kernel resource.
    /// This ensures through the type system that it's not possible to `seek` and `read`
    /// concurrently.
    unsafe fn as_fd_mut(&mut self) -> RawFd;
}

impl IoFdMut for OwnedFd {
    unsafe fn as_fd_mut(&mut self) -> RawFd {
        // Safety: If we have a `&mut` ref to an `OwnedFd`, Rust type system guarantees this is the only reference.
        self.as_raw_fd()
    }
}

impl IoFdMut for std::fs::File {
    unsafe fn as_fd_mut(&mut self) -> RawFd {
        // Safety: If we have a `&mut` ref to an `std::fs::File`, Rust type system guarantees this is the only reference.
        self.as_raw_fd()
    }
}
