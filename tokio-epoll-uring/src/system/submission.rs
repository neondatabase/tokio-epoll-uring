pub(crate) mod op_fut;

use std::sync::{Arc, Mutex};

use io_uring::{SubmissionQueue, Submitter};

use super::{
    completion::CompletionSide,
    slots::{self, Slots},
};

pub(crate) struct SubmitSideNewArgs {
    pub(crate) id: usize,
    pub(crate) slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
    pub(crate) completion_side: Arc<Mutex<CompletionSide>>,
}

impl SubmitSide {
    pub(crate) fn new(args: SubmitSideNewArgs) -> SubmitSide {
        let SubmitSideNewArgs {
            id,
            slots: ops,
            completion_side,
        } = args;
        SubmitSide {
            id,
            slots: ops,
            completion_side: Arc::clone(&completion_side),
        }
    }
}

pub(crate) struct SubmitSide {
    #[allow(dead_code)]
    id: usize,
    slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
    completion_side: Arc<Mutex<CompletionSide>>,
}

impl Drop for SubmitSide {
    fn drop(&mut self) {
        self.slots.set_draining()
    }
}
