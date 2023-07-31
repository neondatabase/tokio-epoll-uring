pub(super) mod completion;
pub(super) mod lifecycle;
pub(crate) mod slots;
pub(super) mod submission;
#[cfg(test)]
mod test_util;
#[cfg(test)]
mod tests;

pub(crate) const RING_SIZE: u32 = 128;
