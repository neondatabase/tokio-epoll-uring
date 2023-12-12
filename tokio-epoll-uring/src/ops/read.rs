use std::os::fd::AsRawFd;

use uring_common::{buf::BoundedBufMut, io_fd::IoFd, io_uring};

use crate::system::submission::op_fut::Op;

pub struct ReadOp<F, B>
where
    F: IoFd + Send,
    B: BoundedBufMut + Send,
{
    pub(crate) file: F,
    pub(crate) offset: u64,
    pub(crate) buf: B,
}

impl<F, B> crate::sealed::Sealed for ReadOp<F, B>
where
    F: IoFd + Send,
    B: BoundedBufMut + Send,
{
}

impl<F, B> Op for ReadOp<F, B>
where
    F: IoFd + Send,
    B: BoundedBufMut + Send,
{
    type Resources = (F, B);
    type Success = usize;
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Read::new(
            io_uring::types::Fd(
                // SAFETY: we hold `F` in self, and if `self` is dropped, we hand the fd to the
                // `System` to keep it live until the operation completes.
                #[allow(unused_unsafe)]
                unsafe {
                    self.file.as_fd().as_raw_fd()
                },
            ),
            self.buf.stable_mut_ptr(),
            self.buf.bytes_total() as _,
        )
        .offset(self.offset)
        .build()
    }

    fn on_failed_submission(self) -> Self::Resources {
        (self.file, self.buf)
    }

    fn on_op_completion(
        mut self,
        res: i32,
    ) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        // https://man.archlinux.org/man/io_uring_prep_read.3.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            unsafe { BoundedBufMut::set_init(&mut self.buf, res as usize) };
            Ok(res as usize)
        };
        ((self.file, self.buf), res)
    }
}
