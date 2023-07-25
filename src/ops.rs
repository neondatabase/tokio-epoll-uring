pub mod read;

use futures::{Future, FutureExt};

use crate::{
    system::submission::{
        GetOpsSlotError, GetOpsSlotFut, InflightOpHandle, NotInflightSlotHandleSubmitError,
        NotInflightSlotHandleSubmitErrorKind, SubmitSide,
    },
    ResourcesOwnedByKernel, SubmitSideProvider,
};

pub(crate) enum OpFut<L, P, O>
where
    L: Future<Output = P> + Unpin,
    P: SubmitSideProvider,
    O: OpTrait + Send + 'static,
{
    Undefined,
    NeedLaunch { system_launcher: L, make_op: O },
    NeedSlot { slot_fut: GetOpsSlotFut, make_op: O },
    Submitted(InflightOpHandle<O>),
    ReadyPolled,
}

pub(crate) trait OpTrait: ResourcesOwnedByKernel + Sized + Send + 'static {
    fn make_sqe(&mut self) -> io_uring::squeue::Entry;
    fn into_fut<L, P>(self, launcher: L) -> OpFut<L, P, Self>
    where
        L: Future<Output = P> + Unpin,
        P: SubmitSideProvider,
    {
        OpFut::NeedLaunch {
            system_launcher: launcher,
            make_op: self,
        }
    }
}

pub(crate) enum OpSubmitError<O>
where
    O: OpTrait + Send + 'static,
{
    GetOpsSlotError(O, GetOpsSlotError),
    SubmitError(O, NotInflightSlotHandleSubmitErrorKind),
}

impl<L, P, O> std::future::Future for OpFut<L, P, O>
where
    L: Future<Output = P> + Unpin,
    P: SubmitSideProvider,
    O: OpTrait + Send + 'static + Unpin,
{
    type Output = Result<O::OpResult, OpSubmitError<O>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut myself = self.as_mut();
        loop {
            let cur = std::mem::replace(&mut *myself, OpFut::Undefined);
            match cur {
                OpFut::Undefined => todo!(),
                OpFut::ReadyPolled => unreachable!(),
                OpFut::NeedLaunch {
                    mut system_launcher,
                    make_op,
                } => match system_launcher.poll_unpin(cx) {
                    std::task::Poll::Ready(submit_provider) => {
                        let slot_fut = submit_provider.with_submit_side(|submit_side| {
                            let SubmitSide(inner) = submit_side;
                            let mut submit_side_guard = inner.lock().unwrap();
                            let submit_side_open = submit_side_guard.must_open();
                            submit_side_open.get_ops_slot()
                        });
                        *myself = OpFut::NeedSlot { slot_fut, make_op };
                        continue;
                    }
                    std::task::Poll::Pending => {
                        *myself = OpFut::NeedLaunch {
                            system_launcher,
                            make_op,
                        };
                        return std::task::Poll::Pending;
                    }
                },
                OpFut::NeedSlot {
                    mut slot_fut,
                    make_op,
                } => match slot_fut.poll_unpin(cx) {
                    std::task::Poll::Pending => {
                        *myself = OpFut::NeedSlot { slot_fut, make_op };
                        return std::task::Poll::Pending;
                    }
                    std::task::Poll::Ready(Ok(not_inflight_slot_handle)) => {
                        let submit_fut =
                            match not_inflight_slot_handle.submit(make_op, |op| op.make_sqe()) {
                                Ok(submit_fut) => submit_fut,
                                Err(NotInflightSlotHandleSubmitError { rsrc, kind }) => {
                                    *myself = OpFut::ReadyPolled;
                                    return std::task::Poll::Ready(Err(
                                        OpSubmitError::SubmitError(rsrc, kind),
                                    ));
                                }
                            };
                        *myself = OpFut::Submitted(submit_fut);
                        continue;
                    }
                    std::task::Poll::Ready(Err(e)) => {
                        *myself = OpFut::ReadyPolled;
                        return std::task::Poll::Ready(Err(OpSubmitError::GetOpsSlotError(
                            make_op, e,
                        )));
                    }
                },
                OpFut::Submitted(mut submit_fut) => match submit_fut.poll_unpin(cx) {
                    std::task::Poll::Ready(output) => {
                        let output: O::OpResult = output;
                        *myself = OpFut::ReadyPolled;
                        return std::task::Poll::Ready(Ok(output));
                    }
                    std::task::Poll::Pending => {
                        *myself = OpFut::Submitted(submit_fut);
                        return std::task::Poll::Pending;
                    }
                },
            }
        }
    }
}
