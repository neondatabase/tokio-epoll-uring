use std::os::fd::OwnedFd;

use tokio_uring::buf::IoBufMut;

use crate::{system::submission::op_fut::OpFut, Ops};

pub mod nop;
pub mod read;

pub(crate) use self::read::ReadOp;

impl Ops for crate::SystemHandle {
    fn nop(&self) -> OpFut<self::nop::Nop> {
        let op = nop::Nop {};
        OpFut::new(op, self.state.guaranteed_live().submit_side.clone())
    }
    fn read<B: IoBufMut + Send>(&self, file: OwnedFd, offset: u64, buf: B) -> OpFut<ReadOp<B>> {
        let op = ReadOp { file, offset, buf };
        OpFut::new(op, self.state.guaranteed_live().submit_side.clone())
    }
}
