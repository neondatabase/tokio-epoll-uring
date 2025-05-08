use std::os::fd::AsRawFd;

use nix::fcntl::FallocateFlags;
use uring_common::{
    io_fd::IoFd,
    io_uring::{self},
};

use crate::system::submission::op_fut::Op;

pub struct FallocateOp<F>
where
    F: IoFd + Send,
{
    pub(crate) file: F,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) mode: FallocateFlags,
}

impl<F> crate::sealed::Sealed for FallocateOp<F> where F: IoFd + Send {}

impl<F> Op for FallocateOp<F>
where
    F: IoFd + Send,
{
    type Resources = F;
    type Success = ();
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Fallocate::new(
            io_uring::types::Fd(
                // SAFETY: we hold `F` in self, and if `self` is dropped, we hand the fd to the
                // `System` to keep it live until the operation completes.
                #[allow(unused_unsafe)]
                unsafe {
                    self.file.as_fd().as_raw_fd()
                },
            ),
            self.len,
        )
        .offset(self.offset)
        .mode(self.mode.bits())
        .build()
    }

    fn on_failed_submission(self) -> Self::Resources {
        self.file
    }

    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(())
        };
        (self.file, res)
    }
}
