use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};

pub(super) mod completion;
pub(super) mod lifecycle;
pub(super) mod submission;
#[cfg(test)]
mod tests;
pub(crate) mod slots;

use tokio::sync::oneshot;
use tracing::trace;

use crate::ResourcesOwnedByKernel;

pub(crate) const RING_SIZE: u32 = 128;
