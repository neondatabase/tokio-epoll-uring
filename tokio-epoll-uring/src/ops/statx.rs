use std::os::fd::AsRawFd;
use uring_common::libc;

use uring_common::{
    io_fd::IoFd,
    io_uring::{self},
};

use crate::system::submission::op_fut::Op;

// See `https://man.archlinux.org/man/statx.2.en#Invoking_%3Cb%3Estatx%3C/b%3E():`
// to understand why there are different variants and why they're named the way they are.
pub enum StatxOp<F>
where
    F: IoFd + Send,
{
    ByFileDescriptor {
        file: F,
        statxbuf: Submitting<
            Box<uring_common::io_uring::types::statx>,
            *mut uring_common::io_uring::types::statx,
        >,
    },
}

impl<F> StatxOp<F>
where
    F: IoFd + Send,
{
    // Do the equivalent of fstat.
    pub fn new_fstat(file: F, statxbuf: Box<uring_common::io_uring::types::statx>) -> StatxOp<F> {
        StatxOp::ByFileDescriptor {
            file,
            statxbuf: Submitting::No(statxbuf),
        }
    }
}

pub enum Resources<F>
where
    F: IoFd + Send,
{
    ByFileDescriptor {
        file: F,
        statxbuf: Box<uring_common::io_uring::types::statx>,
    },
}

// TODO: refine the `Op` trait so we encode this state in the typesystem
enum Submitting<A, B> {
    No(A),
    Yes(B),
}

/// SAFETY: we only needs this because we store the pointer while Submitting::Yes
unsafe impl<F> Send for StatxOp<F> where F: IoFd + Send {}

impl<F> crate::sealed::Sealed for StatxOp<F> where F: IoFd + Send {}

impl<F> Op for StatxOp<F>
where
    F: IoFd + Send,
{
    type Resources = Resources<F>;
    type Success = ();
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        // See https://man.archlinux.org/man/statx.2.en#Invoking_%3Cb%3Estatx%3C/b%3E():
        match self {
            StatxOp::ByFileDescriptor { file, statxbuf } => {
                let fd = io_uring::types::Fd(
                    // SAFETY: we hold `F` in self, and if `self` is dropped, we hand the fd to the
                    // `System` to keep it live until the operation completes.
                    #[allow(unused_unsafe)]
                    unsafe {
                        file.as_fd().as_raw_fd()
                    },
                );
                // SAFETY: by Box::into_raw'ing the statxbuf box, the memory won't be re-used
                // until we Box::from_raw it in `on_failed_submission` or `on_op_completion`
                let statxbuf: *mut uring_common::io_uring::types::statx = match statxbuf {
                    Submitting::No(statxbuf_box) => Box::into_raw(statxbuf_box),
                    Submitting::Yes(statxbuf_ptr) => {
                        unreachable!("make_sqe is only called once")
                    }
                };
                // This is equivalent to what rust std 1.75 does if statx is supported
                io_uring::opcode::Statx::new(fd, b"\0" as *const _, statxbuf)
                    .flags(libc::AT_EMPTY_PATH | libc::AT_STATX_SYNC_AS_STAT)
                    .mask(uring_common::libc::STATX_ALL)
                    .build()
            }
        }
    }

    fn on_failed_submission(self) -> Self::Resources {
        self.on_ownership_back_with_userspace()
    }

    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        // https://man.archlinux.org/man/io_uring_prep_statx.3.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(())
        };
        (self.on_ownership_back_with_userspace(), res)
    }
}

impl<F> StatxOp<F>
where
    F: IoFd + Send,
{
    fn on_ownership_back_with_userspace(self) -> Resources<F> {
        match self {
            StatxOp::ByFileDescriptor { file, statxbuf } => {
                let statxbuf = match statxbuf {
                    Submitting::No(_) => unreachable!("only called after make_sqe"),
                    Submitting::Yes(statxbuf_ptr) => {
                        // SAFETY: the `System` guarantees that when it calls us here,
                        // ownership of the resources is with us.
                        unsafe { Box::from_raw(statxbuf_ptr) }
                    }
                };
                Resources::ByFileDescriptor { file, statxbuf }
            }
        }
    }
}
