pub(crate) mod op_fut;

use super::slots::{self, Slots};

impl SubmitSide {
    pub(crate) fn new(id: usize, slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>) -> SubmitSide {
        SubmitSide { id, slots }
    }
}

pub(crate) struct SubmitSide {
    #[allow(dead_code)]
    id: usize,
    slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
}

impl Drop for SubmitSide {
    fn drop(&mut self) {
        self.slots.set_draining()
    }
}
