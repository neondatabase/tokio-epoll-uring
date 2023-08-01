use rand::Rng;
use std::{
    alloc::Layout,
    ops::ControlFlow,
    os::fd::{FromRawFd, IntoRawFd},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tracing::info;

use crate::{Args, ClientWork, ClientWorkKind, Engine, EngineRunResult, StatsState};
pub(crate) struct EngineTokioUring {}

impl EngineTokioUring {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Engine for EngineTokioUring {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) -> EngineRunResult {
        tokio_uring::start(async move {
            let mut handles = Vec::new();
            assert_eq!(works.len(), args.num_clients.get() as usize);
            let all_client_tasks_spawned =
                Arc::new(tokio::sync::Barrier::new(args.num_clients.get() as usize));
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let all_client_tasks_spawned = Arc::clone(&all_client_tasks_spawned);
                let clients_ready = Arc::clone(&clients_ready);
                handles.push(tokio_uring::spawn({
                    let args = Arc::clone(&args);
                    async move {
                        clients_ready.wait().await;
                        let start = std::time::Instant::now();
                        Self::client(
                            i,
                            Arc::clone(&args),
                            work,
                            &stop,
                            stats_state,
                            all_client_tasks_spawned,
                        )
                        .await;
                        start.elapsed()
                    }
                }));
            }
            let mut client_run_times = Vec::new();
            for handle in handles {
                let run_time = handle.await.unwrap();
                client_run_times.push(run_time);
            }
            EngineRunResult { client_run_times }
        })
    }
}

impl EngineTokioUring {
    async fn client(
        i: u64,
        args: Arc<Args>,
        work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
        all_client_tasks_spawned: Arc<tokio::sync::Barrier>,
    ) {
        tracing::info!("Client {i} starting");
        all_client_tasks_spawned.wait().await;
        let block_size = 1 << args.block_size_shift.get();

        let ClientWork { ops_left, kind } = work;
        enum WorkKind {
            DiskAccess { file: tokio_uring::fs::File },
        }
        let kind = match kind {
            ClientWorkKind::DiskAccess { file, validate } => {
                if validate {
                    unimplemented!()
                }
                let raw_fd = file.into_raw_fd();
                WorkKind::DiskAccess {
                    file: unsafe { tokio_uring::fs::File::from_raw_fd(raw_fd) },
                }
            }
            ClientWorkKind::NoWork {} => unimplemented!(),
            ClientWorkKind::TimerFdSetStateAndRead { .. } => unimplemented!(),
        };

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

        // alloc aligned to make O_DIRECT work
        let buf = unsafe {
            let ptr = std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap());
            assert!(!ptr.is_null());
            Vec::from_raw_parts(ptr, 0, block_size)
        };
        let mut loop_buf = Some(buf);

        let block_size_u64: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            let ControlFlow::Continue(()) = ops_left.take_one_op() else {
                break;
            };

            // simulate Timeline::layers.read().await
            let _guard = rwlock.read().await;

            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;
            let start = std::time::Instant::now();
            match &kind {
                WorkKind::DiskAccess { file } => {
                    let buf = loop_buf.take().unwrap();
                    let (res, buf) = file.read_at(buf, offset_in_file).await;
                    let read = res.unwrap();
                    assert_eq!(read, buf.len());
                    assert_eq!(read, block_size);
                    let replaced = loop_buf.replace(buf);
                    assert!(replaced.is_none());
                }
            }
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            stats_state.record_iop_latency(usize::try_from(i).unwrap(), start.elapsed());
        }
        info!("Client {i} stopping");
    }
}
