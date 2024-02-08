use crate::system::submission::op_fut::Op;
use crate::util::submitting_box::SubmittingBox;
use std::{mem::MaybeUninit, os::fd::AsRawFd};
use uring_common::libc;
pub use uring_common::libc::statx;
use uring_common::{
    io_fd::IoFd,
    io_uring::{self},
};

pub(crate) fn op<F>(
    resources: Resources<F>,
) -> impl Op<Resources = Resources<F>, Success = (), Error = std::io::Error>
where
    F: IoFd + Send,
{
    match resources {
        Resources::ByFileDescriptor { file, statxbuf } => StatxOp::ByFileDescriptor {
            file,
            statxbuf: SubmittingBox::new(statxbuf),
        },
    }
}

pub enum Resources<F>
where
    F: IoFd + Send,
{
    ByFileDescriptor {
        file: F,
        statxbuf: Box<MaybeUninit<uring_common::libc::statx>>,
    },
}

// See `https://man.archlinux.org/man/statx.2.en#Invoking_%3Cb%3Estatx%3C/b%3E():`
// to understand why there are different variants and why they're named the way they are.
enum StatxOp<F>
where
    F: IoFd + Send,
{
    ByFileDescriptor {
        file: F,
        statxbuf: SubmittingBox<uring_common::libc::statx>,
    },
}

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
                // This is equivalent to what rust std 1.75 does if statx is supported
                io_uring::opcode::Statx::new(
                    fd,
                    // SAFETY: static byte string is zero-terminated.
                    unsafe { std::ffi::CStr::from_bytes_with_nul_unchecked(b"\0").as_ptr() },
                    // Yes, this cast is what the io_uring / tokio-uring crates currently do as well.
                    // Don't understand why io_uring crate just doesn't take a `libc::statx` directly.
                    // https://github.com/tokio-rs/tokio-uring/blob/c4320fa2e7b146b28ad921ae25b552a0894c9697/src/io/statx.rs#L47-L61
                    statxbuf.start_submitting() as *mut uring_common::io_uring::types::statx,
                )
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
                // SAFETY: the `System` guarantees that when it calls us here,
                // ownership of the resources is with us.
                let statxbuf = unsafe { statxbuf.ownership_back_in_userspace() };
                Resources::ByFileDescriptor { file, statxbuf }
            }
        }
    }
}
