use std::os::fd::{AsRawFd, OwnedFd};

use crate::system::{ResourcesOwnedByKernel, SystemLifecycleManager};

pub(crate) type PreadvOutput<B> = (OwnedFd, B, std::io::Result<usize>);

pub(crate) async fn read<S, B>(system: S, file: OwnedFd, offset: u64, buf: B) -> PreadvOutput<B>
where
    S: SystemLifecycleManager,
    B: tokio_uring::buf::IoBufMut + Send,
{
    let slot_handle = system
        .with_submit_side(|submit_side| {
            let mut submit_side_guard = submit_side.lock().unwrap();
            let submit_side = submit_side_guard.must_open();
            submit_side.get_ops_slot()
        })
        .await;
    let inflight_op_handle = slot_handle.submit(Read { file, buf }, |preadv| {
        io_uring::opcode::Read::new(
            io_uring::types::Fd(preadv.file.as_raw_fd()),
            preadv.buf.stable_mut_ptr(),
            preadv.buf.bytes_total() as _,
        )
        .offset(offset)
        .build()
    });
    inflight_op_handle.await
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
