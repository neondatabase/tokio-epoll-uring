pub(crate) mod op_fut;

use std::sync::{Arc, Mutex};

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
            id,
            submitter,
            sq,
            slots: ops,
            completion_side: Arc::clone(&completion_side),
        }
    }
}

impl SubmitSide {
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

pub(crate) struct SubmitSide {
    #[allow(dead_code)]
    id: usize,
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
    slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
    completion_side: Arc<Mutex<CompletionSide>>,
}

impl SubmitSide {
    pub(crate) fn deconstruct(self) -> (Submitter<'static>, SubmissionQueue<'static>) {
        let SubmitSide { submitter, sq, .. } = self;
        (submitter, sq)
    }
}

unsafe impl Send for SubmitSide {}
unsafe impl Sync for SubmitSide {}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("queue full")]
    QueueFull,
}
