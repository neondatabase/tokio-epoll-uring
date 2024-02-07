use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static SYSTEMS_CREATED: AtomicU64 = AtomicU64::new(0);
pub(crate) static SYSTEMS_DESTROYED: AtomicU64 = AtomicU64::new(0);

#[non_exhaustive]
pub struct GlobalMetrics {
    pub systems_created: u64,
    pub systems_destroyed: u64,
}

pub fn global() -> GlobalMetrics {
    GlobalMetrics {
        systems_created: SYSTEMS_CREATED.load(Ordering::Relaxed),
        systems_destroyed: SYSTEMS_DESTROYED.load(Ordering::Relaxed),
    }
}
