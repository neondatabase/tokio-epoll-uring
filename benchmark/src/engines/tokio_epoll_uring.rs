use rand::Rng;
use std::{
    alloc::Layout,
    ops::ControlFlow,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::prelude::FileExt,
    },
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio_epoll_uring::Ops;
use tracing::{debug, info};

use crate::{Args, ClientWork, ClientWorkKind, Engine, EngineRunResult, StatsState};

pub(crate) struct EngineTokioEpollUring {
    rt: tokio::runtime::Runtime,
}

impl EngineTokioEpollUring {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            // .worker_threads(1) // useful for debugging
            .enable_all()
            .build()
            .unwrap();
        Self { rt }
    }
}

impl Engine for EngineTokioEpollUring {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) -> EngineRunResult {
        let EngineTokioEpollUring { rt } = *self;
        let rt = Arc::new(rt);

        rt.block_on(async move {
            let mut handles = Vec::new();
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let clients_ready = Arc::clone(&clients_ready);
                handles.push(tokio::spawn({
                    let args = Arc::clone(&args);
                    async move {
                        clients_ready.wait().await;
                        let start = std::time::Instant::now();
                        Self::client(i, &args, work, &stop, stats_state).await;
                        start.elapsed()
                    }
                }));
            }
            // task that prints periodically which clients have exited
            let stopped_handles = Arc::new(
                (0..handles.len())
                    .map(|_| AtomicBool::new(false))
                    .collect::<Vec<_>>(),
            );
            let stop_stopped_task_status_task = Arc::new(AtomicBool::new(false));
            let stopped_task_status_task = tokio::spawn({
                let stopped_handles = Arc::clone(&stopped_handles);
                let stop_clients = Arc::clone(&stop);
                let stop_stopped_task_status_task = Arc::clone(&stop_stopped_task_status_task);
                async move {
                    'outer: while !stop_stopped_task_status_task.load(Ordering::Relaxed) {
                        // don't print until `stop` is set
                        while !stop_clients.load(Ordering::Relaxed) {
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                            debug!("waiting for clients to stop");
                            continue 'outer;
                        }
                        // log list of not-stopped clients every second
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        let stopped = stopped_handles
                            .iter()
                            .map(|x| x.load(Ordering::Relaxed))
                            .filter(|x| *x)
                            .count();
                        let not_stopped = stopped_handles
                            .iter()
                            .enumerate()
                            .filter(|(_, state)| state.load(Ordering::Relaxed) == false)
                            .map(|(i, _)| i)
                            .collect::<Vec<usize>>();
                        let total = stopped_handles.len();
                        info!("handles stopped {stopped} total {total}");
                        info!("  not stopped: {not_stopped:?}",)
                    }
                }
            });
            let mut client_run_times = Vec::new();
            for (i, handle) in handles.into_iter().enumerate() {
                info!("awaiting client {i}");
                let runtime = handle.await.unwrap();
                stopped_handles[i].store(true, Ordering::Relaxed);
                client_run_times.push(runtime);
            }
            stop_stopped_task_status_task.store(true, Ordering::Relaxed);
            info!("awaiting stopped_task_status_task");
            stopped_task_status_task.await.unwrap();
            info!("stopped_task_status_task stopped");
            EngineRunResult { client_run_times }
        })
    }
}

impl EngineTokioEpollUring {
    #[tracing::instrument(skip_all, level="trace", fields(client=%i))]
    async fn client(
        i: u64,
        args: &Args,
        work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        // tokio::time::sleep(Duration::from_secs(i)).await;
        // tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));

        #[derive(Copy, Clone)]
        enum ClientWorkFd {
            DiskAccess { raw_fd: RawFd, validate: bool },
            TimerFd(RawFd, Duration),
            NoWork,
        }

        let fd = match work.kind {
            ClientWorkKind::DiskAccess { file, validate } => ClientWorkFd::DiskAccess {
                raw_fd: file.into_raw_fd(),
                validate,
            },
            ClientWorkKind::TimerFdSetStateAndRead { timerfd, duration } => {
                let ret = ClientWorkFd::TimerFd(timerfd.as_raw_fd(), duration);
                std::mem::forget(timerfd); // they don't support into_raw_fd
                ret
            }
            ClientWorkKind::NoWork {} => ClientWorkFd::NoWork,
        };

        // alloc aligned to make O_DIRECT work
        let buf = unsafe {
            let ptr = std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap());
            assert!(!ptr.is_null());
            Vec::from_raw_parts(ptr, 0, block_size)
        };
        let mut loop_buf = Some(buf);

        let validate_buf = unsafe {
            let ptr = std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap());
            assert!(!ptr.is_null());
            Vec::from_raw_parts(ptr, 0, block_size)
        };
        let mut loop_validate_buf = Some(validate_buf);

        let block_size_u64: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            let ControlFlow::Continue(()) = work.ops_left.take_one_op() else {
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
            match fd {
                ClientWorkFd::DiskAccess {
                    raw_fd: file_fd,
                    validate,
                } => {
                    let owned_buf = loop_buf.take().unwrap();
                    let file = unsafe { OwnedFd::from_raw_fd(file_fd) };
                    // We use it to get one io_uring submission & completion ring per core / executor thread.
                    // The thread-local rings are not great if there's block_in_place in the codebase. It's fine here.
                    // Ideally we'd have one submission ring per core and a single completion ring, because, completion
                    // wakes up the task but we don't know which runtime it is one.
                    // (Even more ideal: a runtime that is io_uring-aware and keeps tasks that wait for wakeup from a completion
                    //  affine to a completion queue somehow... The design space is big.)

                    let ((file, owned_buf), res) =
                        tokio_epoll_uring::with_thread_local_system(move |system| {
                            system.read(file, offset_in_file, owned_buf)
                        })
                        .await;
                    let count = res.unwrap();
                    assert_eq!(count, owned_buf.len());
                    assert_eq!(count, block_size);

                    if validate {
                        let mut owned_validate_buf = loop_validate_buf.take().unwrap();
                        owned_validate_buf.resize(block_size, 0);
                        let std_file = unsafe { std::fs::File::from_raw_fd(file.as_raw_fd()) };
                        let nread = std_file
                            .read_at(&mut owned_validate_buf, offset_in_file)
                            .unwrap();
                        assert_eq!(nread, block_size);
                        assert_eq!(owned_buf, owned_validate_buf);
                        loop_validate_buf = Some(owned_validate_buf);
                        std_file.into_raw_fd(); // we used as_raw_fd above, don't make the Drop of std_file close the fd
                    }
                    loop_buf = Some(owned_buf);
                    file.into_raw_fd(); // so that it's there for next iteration
                }
                ClientWorkFd::TimerFd(_timerfd, _duration) => {
                    unimplemented!()
                }
                ClientWorkFd::NoWork => (),
            }
            // TODO: can this dealock with rendezvous channel, i.e., queue_depth=0?

            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            stats_state.record_iop_latency(usize::try_from(i).unwrap(), start.elapsed());
        }
        info!("Client {i} stopping");
    }
}
