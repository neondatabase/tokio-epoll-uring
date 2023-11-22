// It's an ext trait to avoid changes in `open_options.rs`.
//
// See also: https://github.com/neondatabase/tokio-uring/pull/1

use std::io;

use crate::open_options::OpenOptions;

/// Extension trait to allow re-use of [`OpenOptions`] in other crates that
/// build on top of [`io_uring`].
pub trait OpenOptionsIoUringExt {
    /// Turn `self` into an [`::io_uring::opcode::OpenAt`] SQE for the given `path`.
    ///
    /// # Safety
    ///
    /// The returned SQE stores the provided `path` pointer.
    /// The caller must ensure that it remains valid until either the returned
    /// SQE is dropped without being submitted, or if the SQE is submitted until
    /// the corresponding completion is observed.
    unsafe fn as_openat_sqe(
        &self,
        path: *const libc::c_char,
    ) -> io::Result<io_uring::squeue::Entry>;
}

impl OpenOptionsIoUringExt for OpenOptions {
    unsafe fn as_openat_sqe(
        &self,
        path: *const libc::c_char,
    ) -> io::Result<io_uring::squeue::Entry> {
        use io_uring::{opcode, types};
        let flags = libc::O_CLOEXEC
            | self.access_mode()?
            | self.creation_mode()?
            | (self.custom_flags & !libc::O_ACCMODE);

        Ok(opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path)
            .flags(flags)
            .mode(self.mode)
            .build())
    }
}
