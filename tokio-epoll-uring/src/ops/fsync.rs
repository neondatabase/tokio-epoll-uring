use std::os::fd::AsRawFd;

use uring_common::{
    io_fd::IoFd,
    io_uring::{self},
};

use crate::system::submission::op_fut::Op;

pub struct FsyncOp<F>
where
    F: IoFd + Send,
{
    pub(crate) file: F,
    pub(crate) flags: io_uring::types::FsyncFlags,
}

impl<F> crate::sealed::Sealed for FsyncOp<F> where F: IoFd + Send {}

impl<F> Op for FsyncOp<F>
where
    F: IoFd + Send,
{
    type Resources = F;
    type Success = ();
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Fsync::new(io_uring::types::Fd(
            // SAFETY: we hold `F` in self, and if `self` is dropped, we hand the fd to the
            // `System` to keep it live until the operation completes.
            #[allow(unused_unsafe)]
            unsafe {
                self.file.as_fd().as_raw_fd()
            },
        ))
        .flags(self.flags)
        .build()
    }

    fn on_failed_submission(self) -> Self::Resources {
        self.file
    }

    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        // https://man.archlinux.org/man/extra/liburing/io_uring_prep_fsync.3.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(())
        };
        (self.file, res)
    }
}
