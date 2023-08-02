pub(crate) mod op_fut;

use std::sync::{Arc, Mutex, Weak};

use io_uring::{SubmissionQueue, Submitter};
use tokio_util::either::Either;

use super::{
    completion::CompletionSide,
    slots::{self, SlotHandle, Slots, TryGetSlotResult},
};

pub(crate) struct SubmitSideNewArgs {
    pub(crate) id: usize,
    pub(crate) submitter: Submitter<'static>,
    pub(crate) sq: SubmissionQueue<'static>,
    pub(crate) slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
    pub(crate) completion_side: Arc<Mutex<CompletionSide>>,
}

pub(crate) struct SubmitSide {
    // This is the only long-lived strong reference to the `SubmitSideInner`.
    inner: Arc<Mutex<SubmitSideInner>>,
}

impl SubmitSide {
    pub(crate) fn new(args: SubmitSideNewArgs) -> SubmitSide {
        let SubmitSideNewArgs {
            id,
            submitter,
            sq,
            slots: ops,
            completion_side,
        } = args;
        SubmitSide {
            inner: Arc::new(Mutex::new(SubmitSideInner::Open(SubmitSideOpen {
                id,
                submitter,
                sq,
                slots: ops,
                completion_side: Arc::clone(&completion_side),
            }))),
        }
    }
}

impl SubmitSideOpen {
    pub(crate) fn submit_raw(
        &mut self,
        sqe: io_uring::squeue::Entry,
    ) -> std::result::Result<(), SubmitError> {
        self.sq.sync();
        match unsafe { self.sq.push(&sqe) } {
            Ok(()) => {}
            Err(_queue_full) => {
                return Err(SubmitError::QueueFull);
            }
        }
        self.sq.sync();
        self.submitter.submit().unwrap();
        Ok(())
    }
}

pub struct SubmitSideWeak(Weak<Mutex<SubmitSideInner>>);

impl SubmitSideWeak {
    pub(crate) fn with_submit_side_open<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&mut SubmitSideOpen>) -> R,
    {
        let submit_side = match self.0.upgrade() {
            Some(submit_side) => submit_side,
            None => return f(None),
        };
        SubmitSide { inner: submit_side }.with_submit_side_open(f)
    }

    pub(crate) async fn get_slot(&self) -> Option<SlotHandle> {
        let maybe_fut = self.with_submit_side_open(|submit_side_open| match submit_side_open {
            None => None,
            Some(open) => match open.slots.try_get_slot() {
                TryGetSlotResult::Draining => None,
                TryGetSlotResult::GotSlot(slot) => Some(Either::Left(async move { Ok(slot) })),
                TryGetSlotResult::NoSlots(later) => Some(Either::Right(later)),
            },
        });

        if let Some(maybe_fut) = maybe_fut {
            maybe_fut.await.ok()
        } else {
            None
        }
    }
}

impl SubmitSide {
    pub(crate) fn with_submit_side_open<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&mut SubmitSideOpen>) -> R,
    {
        let mut inner_guard = self.inner.lock().unwrap();
        let submit_side_open = match &mut *inner_guard {
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::Plugged => return f(None),
        };
        f(Some(submit_side_open))
    }
}

pub(crate) enum SubmitSideInner {
    Open(SubmitSideOpen),
    Plugged,
}

pub(crate) struct SubmitSideOpen {
    #[allow(dead_code)]
    id: usize,
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
    slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
    completion_side: Arc<Mutex<CompletionSide>>,
}

impl SubmitSide {
    pub(crate) fn weak(&self) -> SubmitSideWeak {
        SubmitSideWeak(Arc::downgrade(&self.inner))
    }
    pub(crate) fn plug(self) -> SubmitSideOpen {
        let mut inner = self.inner.lock().unwrap();
        let cur = std::mem::replace(&mut *inner, SubmitSideInner::Plugged);
        match cur {
            SubmitSideInner::Open(open) => {
                open.slots.set_draining();
                open
            }
            SubmitSideInner::Plugged => unreachable!(),
        }
    }
}

impl SubmitSideOpen {
    pub(crate) fn deconstruct(self) -> (Submitter<'static>, SubmissionQueue<'static>) {
        let SubmitSideOpen { submitter, sq, .. } = self;
        (submitter, sq)
    }
}

unsafe impl Send for SubmitSideOpen {}
unsafe impl Sync for SubmitSideOpen {}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("queue full")]
    QueueFull,
}
