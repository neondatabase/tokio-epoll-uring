pub(crate) mod op_fut;

use std::sync::{Arc, Mutex, Weak};

use io_uring::{SubmissionQueue, Submitter};

use super::{
    completion::CompletionSide,
    slots::{CoOwnerSubmitSide, Slots},
};

pub(crate) struct SubmitSideNewArgs {
    pub(crate) id: usize,
    pub(crate) submitter: Submitter<'static>,
    pub(crate) sq: SubmissionQueue<'static>,
    pub(crate) slots: Slots<CoOwnerSubmitSide>,
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
            inner: Arc::new_cyclic(|myself| {
                Mutex::new(SubmitSideInner::Open(SubmitSideOpen {
                    id,
                    submitter,
                    sq,
                    slots: ops,
                    completion_side: Arc::clone(&completion_side),
                    myself: SubmitSideWeak(Weak::clone(&myself)),
                }))
            }),
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
    pub(crate) fn clone(&self) -> Self {
        SubmitSideWeak(Weak::clone(&self.0))
    }
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
    slots: Slots<CoOwnerSubmitSide>,
    completion_side: Arc<Mutex<CompletionSide>>,
    myself: SubmitSideWeak,
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