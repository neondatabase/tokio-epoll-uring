use std::os::fd::AsRawFd;

use uring_common::{buf::BoundedBuf, io_fd::IoFd, io_uring};

use crate::system::submission::op_fut::Op;

pub struct WriteOp<F, B>
where
    F: IoFd + Send,
    B: BoundedBuf + Send,
{
    pub(crate) file: F,
    pub(crate) offset: u64,
    pub(crate) buf: B,
    pub(crate) system_id: usize,
    pub(crate) thread_id: std::thread::ThreadId,
    pub(crate) thread_name: Option<String>,
}

impl<F, B> crate::sealed::Sealed for WriteOp<F, B>
where
    F: IoFd + Send,
    B: BoundedBuf + Send,
{
}

impl<F, B> Op for WriteOp<F, B>
where
    F: IoFd + Send,
    B: BoundedBuf + Send,
{
    type Resources = (F, B);
    type Success = usize;
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Write::new(
            io_uring::types::Fd(
                // SAFETY: we hold `F` in self, and if `self` is dropped, we hand the fd to the
                // `System` to keep it live until the operation completes.
                #[allow(unused_unsafe)]
                unsafe {
                    self.file.as_fd().as_raw_fd()
                },
            ),
            self.buf.stable_ptr(),
            self.buf.bytes_init() as _,
        )
        .offset(self.offset)
        .build()
    }

    fn on_failed_submission(self) -> Self::Resources {
        (self.file, self.buf)
    }

    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        // https://man.archlinux.org/man/extra/liburing/io_uring_prep_write.3.en
        let res = if res < 0 {
            if -res == nix::libc::ECANCELED {
                tracing::warn!(%self.system_id, sub_thread_id=?self.thread_id, sub_thread_name=?self.thread_name, cur_thread_id=?std::thread::current().id(), cur_thread_name=?std::thread::current().name(), "ECANCELED returned, this is unexpected")
            }
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(res as usize)
        };
        ((self.file, self.buf), res)
    }
}
