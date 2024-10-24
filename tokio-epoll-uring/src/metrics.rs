use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, Weak,
    },
};

pub struct GlobalMetrics {
    pub systems_created: u64,
    pub systems_destroyed: u64,
}

pub(crate) struct GlobalMetricsStorage {
    pub(crate) systems_created: AtomicU64,
    pub(crate) systems_destroyed: AtomicU64,
}

impl GlobalMetricsStorage {
    pub(crate) const fn new_const() -> Self {
        GlobalMetricsStorage {
            systems_created: AtomicU64::new(0),
            systems_destroyed: AtomicU64::new(0),
        }
    }
}

impl GlobalMetricsStorage {
    fn make_pub(&self) -> GlobalMetrics {
        GlobalMetrics {
            systems_created: GLOBAL_STORAGE.systems_created.load(Ordering::Relaxed),
            systems_destroyed: GLOBAL_STORAGE.systems_destroyed.load(Ordering::Relaxed),
        }
    }
}

pub(crate) static GLOBAL_STORAGE: GlobalMetricsStorage = GlobalMetricsStorage::new_const();

pub fn global() -> GlobalMetrics {
    GLOBAL_STORAGE.make_pub()
}

pub struct PerSystemMetricsStorage<M: PerSystemMetrics>(Mutex<HashMap<usize, Weak<M>>>);

impl<M: PerSystemMetrics> PerSystemMetricsStorage<M> {
    pub fn new() -> Self {
        PerSystemMetricsStorage(Mutex::new(HashMap::new()))
    }

    pub(crate) fn insert(&self, id: usize, metrics: Weak<M>) {
        let mut g = self.0.lock().unwrap();
        g.insert(id, metrics);
    }

    pub(crate) fn remove(&self, id: usize) {
        let mut g = self.0.lock().unwrap();
        g.remove(&id);
    }
}

/// A trait for recording per-system tokio-epoll-uring metrics.
pub trait PerSystemMetrics {
    fn record_slots_submission_queue_depth(&self, queue_depth: u64);
}

impl PerSystemMetrics for () {
    fn record_slots_submission_queue_depth(&self, _queue_depth: u64) {}
}
