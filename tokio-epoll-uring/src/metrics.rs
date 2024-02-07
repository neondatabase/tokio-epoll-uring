use std::sync::atomic::{AtomicU64, Ordering};

#[non_exhaustive]
pub struct Metrics {
    pub systems_created: u64,
    pub systems_destroyed: u64,
}

pub(crate) struct MetricsStorage {
    pub(crate) systems_created: AtomicU64,
    pub(crate) systems_destroyed: AtomicU64,
}

impl MetricsStorage {
    pub(crate) const fn new_const() -> Self {
        MetricsStorage {
            systems_created: AtomicU64::new(0),
            systems_destroyed: AtomicU64::new(0),
        }
    }
}

impl MetricsStorage {
    fn make_pub(&self) -> Metrics {
        Metrics {
            systems_created: GLOBAL_STORAGE.systems_created.load(Ordering::Relaxed),
            systems_destroyed: GLOBAL_STORAGE.systems_destroyed.load(Ordering::Relaxed),
        }
    }
}

pub(crate) static GLOBAL_STORAGE: MetricsStorage = MetricsStorage::new_const();

pub fn global() -> Metrics {
    GLOBAL_STORAGE.make_pub()
}
