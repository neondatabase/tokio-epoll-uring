use std::os::fd::{AsRawFd, OwnedFd};

use futures::{Future, FutureExt};

use crate::system::{ResourcesOwnedByKernel, SubmitSide, SubmitSideProvider};

enum ReadFut<L, P, B>
where
    P: SubmitSideProvider,
    L: Future<Output = P>,
    B: tokio_uring::buf::IoBufMut + Send,
{
    Undefined,
    NeedLaunch {
        system_launcher: L,
        file: OwnedFd,
        offset: u64,
        buf: B,
    },
    WaitSlot {
        slot_fut: crate::system::GetOpsSlotFut,
        file: OwnedFd,
        offset: u64,
        buf: B,
    },
    Submitted(crate::system::InflightOpHandle<Read<B>>),
    ReadyPolled,
}

pub fn read<L, P, B>(
    system_launcher: L,
    file: OwnedFd,
    offset: u64,
    buf: B,
) -> impl Future<Output = (OwnedFd, B, std::io::Result<usize>)>
where
    P: SubmitSideProvider,
    L: Future<Output = P> + Unpin,
    B: tokio_uring::buf::IoBufMut + Send,
{
    ReadFut::<L, P, B>::NeedLaunch {
        system_launcher,
        file: file,
        offset: offset,
        buf: buf,
    }
}

impl<L, P, B> std::future::Future for ReadFut<L, P, B>
where
    P: SubmitSideProvider,
    L: Future<Output = P> + Unpin,
    B: tokio_uring::buf::IoBufMut + Send,
{
    type Output = (OwnedFd, B, std::io::Result<usize>);

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut myself = self.as_mut();
        loop {
            let cur = std::mem::replace(&mut *myself, ReadFut::Undefined);
            match cur {
                ReadFut::Undefined => todo!(),
                ReadFut::ReadyPolled => unreachable!(),
                ReadFut::NeedLaunch {
                    mut system_launcher,
                    file,
                    offset,
                    buf,
                } => match system_launcher.poll_unpin(cx) {
                    std::task::Poll::Ready(submit_provider) => {
                        let slot_fut = submit_provider.with_submit_side(|submit_side| {
                            let SubmitSide(inner) = submit_side;
                            let mut submit_side_guard = inner.lock().unwrap();
                            let submit_side_open = submit_side_guard.must_open();
                            submit_side_open.get_ops_slot()
                        });
                        *myself = ReadFut::WaitSlot {
                            slot_fut,
                            file,
                            offset,
                            buf,
                        };
                        continue;
                    }
                    std::task::Poll::Pending => {
                        *myself = ReadFut::NeedLaunch {
                            system_launcher,
                            file,
                            offset,
                            buf,
                        };
                        return std::task::Poll::Pending;
                    }
                },
                ReadFut::WaitSlot {
                    mut slot_fut,
                    file,
                    offset,
                    buf,
                } => match slot_fut.poll_unpin(cx) {
                    std::task::Poll::Ready(slot) => {
                        let submit_fut = slot.submit(Read { file, buf }, |preadv| {
                            io_uring::opcode::Read::new(
                                io_uring::types::Fd(preadv.file.as_raw_fd()),
                                preadv.buf.stable_mut_ptr(),
                                preadv.buf.bytes_total() as _,
                            )
                            .offset(offset)
                            .build()
                        });
                        *myself = ReadFut::Submitted(submit_fut);
                        continue;
                    }
                    std::task::Poll::Pending => {
                        *myself = ReadFut::WaitSlot {
                            slot_fut,
                            file,
                            offset,
                            buf,
                        };
                        return std::task::Poll::Pending;
                    }
                },
                ReadFut::Submitted(mut submit_fut) => match submit_fut.poll_unpin(cx) {
                    std::task::Poll::Ready((file, buf, res)) => {
                        *myself = ReadFut::ReadyPolled;
                        return std::task::Poll::Ready((file, buf, res));
                    }
                    std::task::Poll::Pending => {
                        *myself = ReadFut::Submitted(submit_fut);
                        return std::task::Poll::Pending;
                    }
                },
            }
        }
    }
}

struct Read<B>
where
    B: tokio_uring::buf::IoBufMut + Send,
{
    file: OwnedFd,
    buf: B,
}

impl<B> ResourcesOwnedByKernel for Read<B>
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
