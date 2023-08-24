use rand::Rng;
use std::{
    alloc::Layout,
    num::NonZeroUsize,
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
use tracing::info;

use crate::{Args, ClientWork, ClientWorkKind, Engine, EngineRunResult, StatsState};

pub(crate) struct EngineTokioSpawnBlocking {
    rt: tokio::runtime::Runtime,
}

impl EngineTokioSpawnBlocking {
    pub(crate) fn new(max_blocking_threads: NonZeroUsize) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(max_blocking_threads.get())
            .build()
            .unwrap();
        Self { rt }
    }
}

impl Engine for EngineTokioSpawnBlocking {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) -> EngineRunResult {
        let rt = &self.rt;
        rt.block_on(async {
            let mut handles = Vec::new();
            assert_eq!(works.len(), args.num_clients.get() as usize);
            for (i, work) in (0..args.num_clients.get()).zip(works) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let clients_ready = Arc::clone(&clients_ready);
                handles.push(tokio::spawn({
                    let args = Arc::clone(&args);
                    async move {
                        clients_ready.wait().await;
                        Self::client(i, Arc::clone(&args), work, &stop, stats_state).await;
                        std::time::Instant::now()
                    }
                }));
            }
            let mut client_finish_times = Vec::new();
            for handle in handles {
                let run_time = handle.await.unwrap();
                client_finish_times.push(run_time);
            }
            EngineRunResult {
                client_finish_times,
            }
        })
    }
}

impl EngineTokioSpawnBlocking {
    async fn client(
        i: u64,
        args: Arc<Args>,
        work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

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
        let buf = {
            let buf_ptr = unsafe {
                std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap())
            };
            assert!(!buf_ptr.is_null());
            #[derive(Clone, Copy)]
            struct SendPtr(*mut u8);
            impl SendPtr {
                fn as_mut_ptr(&self) -> *mut u8 {
                    self.0
                }
            }
            unsafe impl Send for SendPtr {} // the thread spawned in the loop below doesn't outlive this function (we're polled t completion)
            unsafe impl Sync for SendPtr {} // the loop below ensures only one thread accesses it at a time
            SendPtr(buf_ptr)
        };
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
            tokio::task::spawn_blocking(move || {
                match fd {
                    ClientWorkFd::DiskAccess {
                        raw_fd: file_fd,
                        validate,
                    } => {
                        if validate {
                            unimplemented!()
                        }
                        let buf: &mut [u8] =
                            unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), block_size) };
                        let file = unsafe { std::fs::File::from_raw_fd(file_fd) };
                        file.read_at(buf, offset_in_file).unwrap();
                        file.into_raw_fd(); // so that it's there for next iteration
                    }
                    ClientWorkFd::TimerFd(timerfd, duration) => {
                        let mut fd = unsafe { timerfd::TimerFd::from_raw_fd(timerfd) };
                        fd.set_state(
                            timerfd::TimerState::Oneshot(duration),
                            timerfd::SetTimeFlags::Default,
                        );
                        fd.read();
                        let owned: OwnedFd = fd.into();
                        std::mem::forget(owned);
                    }
                    ClientWorkFd::NoWork => {}
                }
            })
            .await
            .unwrap();
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            stats_state.record_iop_latency(usize::try_from(i).unwrap(), start.elapsed());
        }
        info!("Client {i} stopping");
    }
}
