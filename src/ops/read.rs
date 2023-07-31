use std::os::fd::{AsRawFd, OwnedFd};

use crate::system::submission::op_fut::OpTrait;

pub struct ReadOp<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    pub(crate) file: OwnedFd,
    pub(crate) offset: u64,
    pub(crate) buf: B,
}

impl<B> OpTrait for ReadOp<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    type Resources = (OwnedFd, B);
    type Success = usize;
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Read::new(
            io_uring::types::Fd(self.file.as_raw_fd()),
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
            unsafe { tokio_uring::buf::IoBufMut::set_init(&mut self.buf, res as usize) };
            Ok(res as usize)
        };
        ((self.file, self.buf), res)
    }
}
