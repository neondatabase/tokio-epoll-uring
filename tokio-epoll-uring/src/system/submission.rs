pub(crate) mod op_fut;

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, Weak},
};

use io_uring::{SubmissionQueue, Submitter};

use super::{
    completion::CompletionSide,
    slots::{self, Slots},
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
    inner: Arc<tokio::sync::Mutex<SubmitSideInner>>,
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
            inner: Arc::new(tokio::sync::Mutex::new(SubmitSideInner::Open(
                SubmitSideOpen {
                    id,
                    submitter,
                    sq,
                    slots: ops,
                    completion_side: Arc::clone(&completion_side),
                },
            ))),
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

#[derive(Clone)]
pub struct SubmitSideWeak(Weak<tokio::sync::Mutex<SubmitSideInner>>);

impl SubmitSideWeak {
    pub(crate) async fn with_submit_side_open(&self) -> Option<SubmitSideOpenGuard> {
        let inner = match self.0.upgrade() {
            Some(inner) => inner,
            None => return None,
        };
        let mut inner_guard = inner.lock_owned().await;
        match &mut *inner_guard {
            SubmitSideInner::Open(_) => Some(SubmitSideOpenGuard(inner_guard)),
            SubmitSideInner::Plugged => None,
        }
    }
}

pub(crate) struct SubmitSideOpenGuard(tokio::sync::OwnedMutexGuard<SubmitSideInner>);

impl Deref for SubmitSideOpenGuard {
    type Target = SubmitSideOpen;
    fn deref(&self) -> &Self::Target {
        match &*self.0 {
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::Plugged => unreachable!(),
        }
    }
}

impl DerefMut for SubmitSideOpenGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut *self.0 {
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::Plugged => unreachable!(),
        }
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
        let mut inner = self.inner.blocking_lock(); // TODO: is there deadlock risk by using this?
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
