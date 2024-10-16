use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
};

use once_cell::sync::Lazy;

pub struct Metrics {
    pub systems_created: u64,
    pub systems_destroyed: u64,
    pub waiters_queue_depth: Vec<u64>,
}

pub(crate) struct MetricsStorage {
    pub(crate) systems_created: AtomicU64,
    pub(crate) systems_destroyed: AtomicU64,
    /// Waiters queue depth for active system, indexed by system id.
    pub(crate) waiters_queue_depth: Lazy<RwLock<HashMap<usize, AtomicU64>>>,
}

impl MetricsStorage {
    pub(crate) const fn new_const() -> Self {
        MetricsStorage {
            systems_created: AtomicU64::new(0),
            systems_destroyed: AtomicU64::new(0),
            waiters_queue_depth: Lazy::new(|| RwLock::new(HashMap::new())),
        }
    }
}

impl MetricsStorage {
    /// Updates metrics at system creation.
    pub(crate) fn new_system(&self, id: usize) {
        self.systems_created.fetch_add(1, Ordering::Relaxed);
        {
            let mut g = self.waiters_queue_depth.write().unwrap();
            g.insert(id, AtomicU64::new(0));
        }
    }

    /// Updates metrics at system destruction.
    pub(crate) fn destroy_system(&self, id: usize) {
        self.systems_destroyed.fetch_add(1, Ordering::Relaxed);
        {
            let mut g = self.waiters_queue_depth.write().unwrap();
            g.remove(&id);
        }
    }

    /// Updates waiters queue depth metric for system with `id`.
    pub(crate) fn update_waiters_queue_depth(&self, id: usize, depth: u64) {
        let g = self.waiters_queue_depth.read().unwrap();
        g.get(&id).unwrap().store(depth, Ordering::Relaxed);
    }

    fn make_pub(&self) -> Metrics {
        Metrics {
            systems_created: GLOBAL_STORAGE.systems_created.load(Ordering::Relaxed),
            systems_destroyed: GLOBAL_STORAGE.systems_destroyed.load(Ordering::Relaxed),
            waiters_queue_depth: {
                let g = GLOBAL_STORAGE.waiters_queue_depth.read().unwrap();
                g.values().map(|x| x.load(Ordering::Relaxed)).collect()
            },
        }
    }
}

pub(crate) static GLOBAL_STORAGE: MetricsStorage = MetricsStorage::new_const();

pub fn global() -> Metrics {
    GLOBAL_STORAGE.make_pub()
}
