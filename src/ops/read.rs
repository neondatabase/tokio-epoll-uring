use std::os::fd::{AsRawFd, OwnedFd};

use crate::system::{ResourcesOwnedByKernel, SubmitSideProvider};

use super::OpTrait;

/// Read up to `buf.len()` bytes from a `file` into `buf` at the given `offset`.
pub async fn read<'a, L, P, B>(
    system_launcher: L,
    file: OwnedFd,
    offset: u64,
    buf: B,
) -> (OwnedFd, B, std::io::Result<usize>)
where
    L: std::future::Future<Output = P> + Unpin,
    P: SubmitSideProvider,
    B: tokio_uring::buf::IoBufMut + Send + 'a,
{
    ReadOp { file, offset, buf }.into_fut(system_launcher).await
}

struct ReadOp<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    file: OwnedFd,
    offset: u64,
    buf: B,
}

impl<B> OpTrait for ReadOp<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Read::new(
            io_uring::types::Fd(self.file.as_raw_fd()),
            self.buf.stable_mut_ptr(),
            self.buf.bytes_total() as _,
        )
        .offset(self.offset)
        .build()
    }
}

impl<B> ResourcesOwnedByKernel for ReadOp<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    type OpResult = (OwnedFd, B, std::io::Result<usize>);

    fn on_op_completion(mut self, res: i32) -> Self::OpResult {
        // https://man.archlinux.org/man/io_uring_prep_read.3.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            unsafe { tokio_uring::buf::IoBufMut::set_init(&mut self.buf, res as usize) };
            Ok(res as usize)
        };
        (self.file, self.buf, res)
    }
}
