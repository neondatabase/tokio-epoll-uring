use core::panic;
use std::os::fd::AsRawFd;
use uring_common::libc;

use uring_common::{
    io_fd::IoFd,
    io_uring::{self},
};

use crate::system::submission::op_fut::Op;

pub use uring_common::libc::statx;

// See `https://man.archlinux.org/man/statx.2.en#Invoking_%3Cb%3Estatx%3C/b%3E():`
// to understand why there are different variants and why they're named the way they are.
pub enum StatxOp<F>
where
    F: IoFd + Send,
{
    ByFileDescriptor {
        file: F,
        statxbuf: SubmittingBox<uring_common::libc::statx>,
    },
}

impl<F> StatxOp<F>
where
    F: IoFd + Send,
{
    // Do the equivalent of fstat.
    pub fn new_fstat(file: F, statxbuf: Box<uring_common::libc::statx>) -> StatxOp<F> {
        StatxOp::ByFileDescriptor {
            file,
            statxbuf: SubmittingBox::NotSubmitting(statxbuf),
        }
    }
}

#[non_exhaustive]
pub enum Resources<F>
where
    F: IoFd + Send,
{
    ByFileDescriptor {
        file: F,
        statxbuf: Box<uring_common::libc::statx>,
    },
}

// TODO: refine the `Op` trait so we encode this state in the typesystem as a typestate
pub enum SubmittingBox<A>
where
    A: 'static,
{
    NotSubmitting(Box<A>),
    Submitting(*mut A),
    Undefined,
}

impl<A> SubmittingBox<A> {
    fn start_submitting(&mut self) -> &'static mut A {
        match std::mem::replace(self, Self::Undefined) {
            SubmittingBox::NotSubmitting(v) => {
                let leaked = Box::leak(v);
                *self = Self::Submitting(leaked as *mut _);
                leaked
            }
            SubmittingBox::Submitting(_) => {
                panic!("must not call this function more than once without ownership_back_in_userspace() inbetween")
            }
            Self::Undefined => {
                panic!("implementation error; did we panic earlier in the ::Submitting case?")
            }
        }
    }

    /// # Safety
    ///
    /// Callers must ensure that userspace, and in particular, _the caller_ has again exclusive ownership
    /// over the memory.
    unsafe fn ownership_back_in_userspace(mut self) -> Box<A> {
        match std::mem::replace(&mut self, SubmittingBox::Undefined) {
            SubmittingBox::NotSubmitting(_) => {
                panic!("must not call this function without prior call to start_submitting()")
            }
            SubmittingBox::Submitting(leaked) => Box::from_raw(leaked),
            SubmittingBox::Undefined => todo!(),
        }
    }
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
                // This is equivalent to what rust std 1.75 does if statx is supported
                io_uring::opcode::Statx::new(
                    fd,
                    b"\0" as *const _,
                    // Yes, this cast is what the io_uring / tokio-uring crates currently do as well.
                    // Don't understand why io_uring crate just doesn't take a `libc::statx` directly.
                    // https://github.com/tokio-rs/tokio-uring/blob/c4320fa2e7b146b28ad921ae25b552a0894c9697/src/io/statx.rs#L47-L61
                    statxbuf.start_submitting() as *mut uring_common::libc::statx
                        as *mut uring_common::io_uring::types::statx,
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
