use rand::Rng;
use std::{
    alloc::Layout,
    ops::ControlFlow,
    os::unix::prelude::FileExt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tracing::info;

use crate::{Args, ClientWork, ClientWorkKind, Engine, EngineRunResult, StatsState};

pub(crate) struct EngineStd {}
impl Engine for EngineStd {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) -> EngineRunResult {
        let myself = Arc::new(*self);
        std::thread::scope(|scope| {
            assert_eq!(works.len(), args.num_clients.get() as usize);
            let mut jhs = Vec::with_capacity(args.num_clients.get() as usize);
            for (i, work) in (0..args.num_clients.get()).zip(works) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let myself = Arc::clone(&myself);
                jhs.push(scope.spawn({
                    let args = Arc::clone(&args);
                    let clients_ready = Arc::clone(&clients_ready);
                    move || {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(clients_ready.wait());
                        myself.client(i, Arc::clone(&args), work, &stop, stats_state);
                        std::time::Instant::now()
                    }
                }));
            }
            let client_run_times = jhs.into_iter().map(|jh| jh.join().unwrap()).collect();
            EngineRunResult {
                client_finish_times: client_run_times,
            }
        })
    }
}
impl EngineStd {
    fn client(
        &self,
        i: u64,
        args: Arc<Args>,
        mut work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();
        // alloc aligned to make O_DIRECT work
        let buf =
            unsafe { std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap()) };
        assert!(!buf.is_null());
        let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf, block_size) };
        let block_size: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            let ControlFlow::Continue(()) = work.ops_left.take_one_op() else {
                break;
            };

            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size))
                * block_size;
            let start = std::time::Instant::now();
            match &mut work.kind {
                ClientWorkKind::DiskAccess { file, validate } => {
                    if *validate {
                        unimplemented!()
                    }
                    self.read_iter(i, &args, file, offset_in_file, buf);
                }
                ClientWorkKind::TimerFdSetStateAndRead { timerfd, duration } => {
                    timerfd.set_state(
                        timerfd::TimerState::Oneshot(*duration),
                        timerfd::SetTimeFlags::Default,
                    );
                    timerfd.read();
                }
                ClientWorkKind::NoWork {} => {}
            }
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            stats_state.record_iop_latency(usize::try_from(i).unwrap(), start.elapsed());
        }
        info!("Client {i} stopping");
    }

    #[inline(always)]
    fn read_iter(
        &self,
        _client_num: u64,
        _args: &Args,
        file: &std::fs::File,
        offset: u64,
        buf: &mut [u8],
    ) {
        file.read_at(buf, offset).unwrap();
        // TODO: verify
    }
}
