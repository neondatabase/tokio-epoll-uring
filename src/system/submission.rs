pub(crate) mod op_fut;

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex, Weak},
};

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

pub(crate) trait SubmitSideCoOwner {}

pub(crate) struct SubmitSideCoOwnerHandle;
impl SubmitSideCoOwner for SubmitSideCoOwnerHandle {}

pub(crate) struct CoOwnedSubmitSide<C: SubmitSideCoOwner> {
    _marker: PhantomData<C>,
    inner: Arc<Mutex<SubmitSideInner>>,
}

impl<C: SubmitSideCoOwner> CoOwnedSubmitSide<C> {
    pub(crate) fn new(args: SubmitSideNewArgs) -> CoOwnedSubmitSide<C> {
        let SubmitSideNewArgs {
            id,
            submitter,
            sq,
            slots: ops,
            completion_side,
        } = args;
        CoOwnedSubmitSide {
            _marker: PhantomData,
            inner: Arc::new_cyclic(|myself| {
                Mutex::new(SubmitSideInner::Open(SubmitSideOpen {
                    id,
                    submitter,
                    sq,
                    slots: ops,
                    completion_side: Arc::clone(&completion_side),
                    myself: Weak::clone(&myself),
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

pub(crate) enum SubmitSideInner {
    Open(SubmitSideOpen),
    Plugged,
    Undefined,
}

pub(crate) struct SubmitSideOpen {
    #[allow(dead_code)]
    id: usize,
    pub(crate) submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
    pub(crate) slots: Slots<CoOwnerSubmitSide>,
    pub(crate) completion_side: Arc<Mutex<CompletionSide>>,
    myself: Weak<Mutex<SubmitSideInner>>,
}

impl SubmitSideOpen {
    pub(crate) fn deconstruct(self) -> (Submitter<'static>, SubmissionQueue<'static>) {
        let SubmitSideOpen { submitter, sq, .. } = self;
        (submitter, sq)
    }
}

unsafe impl Send for SubmitSideOpen {}
unsafe impl Sync for SubmitSideOpen {}

pub enum PlugError {
    AlreadyPlugged,
}
impl SubmitSideInner {
    pub(crate) fn plug(&mut self) -> Result<SubmitSideOpen, PlugError> {
        let cur = std::mem::replace(self, SubmitSideInner::Undefined);
        match cur {
            SubmitSideInner::Undefined => panic!("implementation error"),
            SubmitSideInner::Open(open) => {
                *self = SubmitSideInner::Plugged;
                Ok(open)
            }
            SubmitSideInner::Plugged => Err(PlugError::AlreadyPlugged),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("queue full")]
    QueueFull,
}
