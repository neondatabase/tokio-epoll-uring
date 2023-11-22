use crate::system::submission::op_fut::Op;
use std::ffi::CString;
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::prelude::OsStrExt;
use std::path::Path;

use uring_common::io_uring;
pub use uring_common::open_options::OpenOptions;

pub struct OpenAtOp {
    dir_fd: Option<OwnedFd>,
    _path: CString, // need to keep it alive for the lifetime of the operation
    sqe: io_uring::squeue::Entry,
}

impl OpenAtOp {
    pub(crate) fn new_cwd(path: &Path, options: &OpenOptions) -> std::io::Result<Self> {
        let path = CString::new(path.as_os_str().as_bytes())?;
        // SAFETY: we keep `path` alive inside the OpenAtOp, so, the pointer stored in the SQE remains valid.
        let sqe = unsafe {
            uring_common::open_options_io_uring_ext::OpenOptionsIoUringExt::as_openat_sqe(
                options,
                path.as_ptr(),
            )
        }?;
        Ok(OpenAtOp {
            dir_fd: None,
            _path: path,
            sqe,
        })
    }
}

impl crate::sealed::Sealed for OpenAtOp {}

impl Op for OpenAtOp {
    type Resources = Option<OwnedFd>;
    type Success = OwnedFd;
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        self.sqe.clone()
    }

    fn on_failed_submission(self) -> Self::Resources {
        self.dir_fd
    }

    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        // https://man.archlinux.org/man/io_uring_prep_openat.3.en
        // and https://man.archlinux.org/man/openat.2.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(unsafe { OwnedFd::from_raw_fd(res) })
        };
        (self.dir_fd, res)
    }
}
