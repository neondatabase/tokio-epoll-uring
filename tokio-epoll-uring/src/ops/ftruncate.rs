use uring_common::io_fd::IoFd;

use crate::{ops::fallocate::FallocateOp, system::submission::op_fut::Op};

pub struct FtruncateOp<F>
where
    F: IoFd + Send,
{
    pub(crate) file: F,
    pub(crate) len: u64,
}

impl<F> crate::sealed::Sealed for FtruncateOp<F> where F: IoFd + Send {}

impl<F> From<FtruncateOp<F>> for FallocateOp<F>
where
    F: IoFd + Send,
{
    fn from(op: FtruncateOp<F>) -> Self {
        FallocateOp {
            file: op.file,
            offset: 0,
            len: op.len,
            mode: 0, // 0 means regular fallocate, which is equivalent to ftruncate
        }
    }
}

impl<F> Op for FtruncateOp<F>
where
    F: IoFd + Send,
{
    type Resources = F;
    type Success = ();
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> uring_common::io_uring::squeue::Entry {
        let mut fallocate_op = FallocateOp {
            file: unsafe { std::ptr::read(&self.file) }, // Read without dropping
            offset: 0,
            len: self.len,
            mode: 0,
        };

        std::mem::forget(unsafe { std::ptr::read(&self.file) });

        fallocate_op.make_sqe()
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
