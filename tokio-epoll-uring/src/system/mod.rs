pub(super) mod completion;
pub(super) mod lifecycle;
pub(crate) mod slots;
pub(super) mod submission;
#[cfg(test)]
pub(crate) mod test_util;
#[cfg(test)]
pub(crate) mod tests;

pub(crate) const RING_SIZE: u32 = 128;

pub mod kernel_support;
