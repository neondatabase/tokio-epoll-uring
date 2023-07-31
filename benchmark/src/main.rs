use std::{
    alloc::Layout,
    collections::HashMap,
    io::{Seek, Write},
    num::NonZeroU64,
    ops::ControlFlow,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::prelude::FileExt,
    },
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use clap::Parser;
use crossbeam_utils::CachePadded;
use itertools::Itertools;
use rand::{Rng, RngCore};
use serde_with::serde_as;
use tokio_epoll_uring::Ops;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(serde::Serialize, clap::Parser, Clone)]
struct Args {
    num_clients: NonZeroU64,
    file_size_mib: NonZeroU64,
    block_size_shift: NonZeroU64,
    #[clap(long, default_value = "until-ctrl-c")]
    run_duration: RunDuration,
    #[clap(subcommand)]
    work_kind: WorkKind,
}

#[derive(Clone, serde::Serialize)]
enum RunDuration {
    UntilCtrlC,
    FixedDuration(Duration),
    FixedTotalIoCount(u64),
}

impl FromStr for RunDuration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "until-ctrl-c" => Ok(RunDuration::UntilCtrlC),
            x if x.ends_with("total-ios") => {
                let stripped = &s[..s.len() - "total-ios".len()];
                let (stripped, multiplier) = if stripped.ends_with("k-") {
                    (&stripped[..stripped.len() - 2], 1000)
                } else if stripped.ends_with("m-") {
                    (&stripped[..stripped.len() - 2], 1000 * 1000)
                } else if stripped.ends_with("g-") {
                    (&stripped[..stripped.len() - 2], 1000 * 1000 * 1000)
                } else {
                    (stripped, 1)
                };
                match stripped.parse::<NonZeroU64>() {
                    Ok(n) => Ok(RunDuration::FixedTotalIoCount(n.get() * multiplier)),
                    Err(e) => Err(format!("invalid io count: {e}: {s:?}")),
                }
            }
            x => match humantime::parse_duration(x) {
                Ok(d) => Ok(RunDuration::FixedDuration(d)),
                Err(e) => Err(format!("invalid duration: {e}: {s:?}")),
            },
        }
    }
}

#[derive(Clone, Copy, clap::ValueEnum, serde::Serialize)]
enum ValidateMode {
    NoValidate,
    Validate,
}

#[derive(Clone, Copy, clap::Subcommand, serde::Serialize)]
enum WorkKind {
    DiskAccess {
        validate: ValidateMode,
        #[clap(subcommand)]
        disk_access_kind: DiskAccessKind,
    },
    TimerFd {
        #[clap(subcommand)]
        expiration_mode: TimerFdExperiationModeKind,
    },
    NoWork {
        #[clap(subcommand)]
        engine: EngineKind,
    },
}

#[derive(Clone, Copy, clap::Subcommand, serde::Serialize)]
enum TimerFdExperiationModeKind {
    Oneshot {
        micros: NonZeroU64,
        #[clap(subcommand)]
        engine: EngineKind,
    },
}

impl WorkKind {
    fn engine(&self) -> &EngineKind {
        match self {
            WorkKind::TimerFd { expiration_mode } => match expiration_mode {
                TimerFdExperiationModeKind::Oneshot { engine, .. } => engine,
            },
            WorkKind::DiskAccess {
                disk_access_kind, ..
            } => match disk_access_kind {
                DiskAccessKind::DirectIo { engine } => engine,
                DiskAccessKind::CachedIo { engine } => engine,
            },
            WorkKind::NoWork { engine } => engine,
        }
    }
}

#[derive(Copy, Clone, clap::Subcommand, serde::Serialize)]
enum DiskAccessKind {
    DirectIo {
        #[clap(subcommand)]
        engine: EngineKind,
    },
    CachedIo {
        #[clap(subcommand)]
        engine: EngineKind,
    },
}

#[derive(Clone, Copy, clap::Subcommand, serde::Serialize)]
enum EngineKind {
    Std,
    TokioOnExecutorThread,
    TokioSpawnBlocking {
        spawn_blocking_pool_size: NonZeroU64,
    },
    TokioEpollUring,
}

struct EngineStd {}

struct EngineTokioSpawnBlocking {
    rt: tokio::runtime::Runtime,
}

struct StatsState {
    reads_in_last_second: Vec<crossbeam_utils::CachePadded<AtomicU64>>,
    latencies_histo: Vec<crossbeam_utils::CachePadded<Mutex<hdrhistogram::Histogram<u64>>>>,
}

impl StatsState {
    fn make_latency_histogram() -> hdrhistogram::Histogram<u64> {
        hdrhistogram::Histogram::new_with_bounds(1, 10_000_000, 3).unwrap()
    }
    fn record_iop_latency(&self, client_num: usize, latency: Duration) {
        let mut h = self.latencies_histo[client_num].lock().unwrap();
        h.record(u64::try_from(latency.as_micros()).unwrap())
            .unwrap();
    }
}

trait Engine {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        reads_in_last_second: Arc<StatsState>,
    );
}

const MONITOR_PERIOD: Duration = Duration::from_secs(1);

fn main() {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .with_env_filter({
            tracing_subscriber::EnvFilter::try_from_default_env()
                .expect("must set RUST_LOG variable")
        })
        .init();

    let args: Arc<Args> = Arc::new(Args::parse());

    let stop_engine = Arc::new(AtomicBool::new(false));
    let stop_monitor = CancellationToken::new();

    let works = setup_client_works(&args);

    let engine = setup_engine(&args.work_kind.engine());

    let stats_state = Arc::new(StatsState {
        reads_in_last_second: (0..works.len())
            .into_iter()
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect(),
        latencies_histo: (0..works.len())
            .into_iter()
            .map(|_| CachePadded::new(Mutex::new(StatsState::make_latency_histogram())))
            .collect(),
    });

    match args.run_duration {
        RunDuration::UntilCtrlC => {}
        RunDuration::FixedDuration(duration) => {
            let stop_engine = Arc::clone(&stop_engine);
            std::thread::spawn(move || {
                std::thread::sleep(duration);
                info!("configured runtime expired, setting stop flag");
                stop_engine.store(true, Ordering::Relaxed);
            });
        }
        RunDuration::FixedTotalIoCount(_) => {
            // done earlier in setup_client_works
        }
    }

    ctrlc::set_handler({
        let stop_engine = Arc::clone(&stop_engine);
        move || {
            info!("ctrl-c, setting stop flag");
            if stop_engine.fetch_or(true, Ordering::Relaxed) {
                error!("stop flag was already set, aborting");
                std::process::abort();
            } else {
                info!("first ctrl-c, stop flag set");
            }
        }
    })
    .unwrap();

    let clients_and_monitor_ready = Arc::new(tokio::sync::Barrier::new(works.len() + 1));

    let monitor = std::thread::Builder::new()
        .name("monitor".to_owned())
        .spawn({
            let stats_state = Arc::clone(&stats_state);
            let args = Arc::clone(&args);
            let clients_and_monitor_ready = Arc::clone(&clients_and_monitor_ready);

            struct AggregatedStats {
                start: std::time::Instant,
                op_count: u64,
                op_size: u64,
                latencies_histo: hdrhistogram::Histogram<u64>,
            }
            const LATENCY_PERCENTILES: [f64; 7] = [50.0, 90.0, 99.0, 99.9, 99.99, 99.999, 99.9999];
            fn latency_percentiles_serialize<S>(
                values: &[u64; LATENCY_PERCENTILES.len()],
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serde::Serialize::serialize(
                    &LATENCY_PERCENTILES
                        .iter()
                        .map(|p| format!("p{p}"))
                        .zip(values.iter().cloned())
                        .collect::<HashMap<_, _>>(),
                    serializer,
                )
            }
            #[serde_as]
            #[derive(serde::Serialize)]
            struct AggregatedStatsSummary {
                #[serde_as(as = "serde_with::DurationMicroSeconds")]
                elapsed_us: std::time::Duration,
                throughput_iops: f64,
                throughput_bw_mibps: f64,
                latency_min_us: u64,
                latency_mean_us: f64,
                latency_max_us: u64,
                #[serde(serialize_with = "latency_percentiles_serialize")]
                latency_percentiles: [u64; LATENCY_PERCENTILES.len()],
            }

            impl std::fmt::Display for AggregatedStatsSummary {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(
                        f,
                        "t{:.2} TP: iops={:.0} bw={:.2} LAT(us): min={} mean={:.0} max={} {}",
                        self.elapsed_us.as_secs_f64(),
                        self.throughput_iops,
                        self.throughput_bw_mibps,
                        self.latency_min_us,
                        self.latency_mean_us,
                        self.latency_max_us,
                        self.latency_percentiles
                            .iter()
                            .zip(LATENCY_PERCENTILES.iter())
                            .map(|(v, p)| format!("p{p}={v}"))
                            .join(" "),
                    )
                }
            }
            impl AggregatedStats {
                fn new(op_size: u64) -> Self {
                    Self {
                        start: std::time::Instant::now(),
                        op_count: 0,
                        op_size,
                        latencies_histo: StatsState::make_latency_histogram(),
                    }
                }
                fn reset(&mut self, start: std::time::Instant) {
                    self.start = start;
                    self.op_count = 0;
                    self.latencies_histo.clear();
                }
                fn summary_since_start(&self) -> AggregatedStatsSummary {
                    let elapsed = self.start.elapsed();
                    let elapsed_secs = elapsed.as_secs_f64();
                    let histo = &self.latencies_histo;
                    AggregatedStatsSummary {
                        elapsed_us: elapsed,
                        throughput_iops: (self.op_count as f64) / elapsed_secs,
                        throughput_bw_mibps: (self.op_count as f64) * ((self.op_size) as f64)
                            / ((1 << 20) as f64)
                            / elapsed_secs,
                        latency_min_us: histo.min(),
                        latency_mean_us: histo.mean(),
                        latency_max_us: histo.max(),
                        latency_percentiles: {
                            let mut values = [0; LATENCY_PERCENTILES.len()];
                            for (i, value_ref) in values.iter_mut().enumerate() {
                                *value_ref = histo.value_at_percentile(LATENCY_PERCENTILES[i]);
                            }
                            values
                        },
                    }
                }
            }

            #[derive(serde::Serialize)]
            struct BenchmarkOutput {
                args: Args,
                sorted_per_task_total_reads: Vec<u64>,
                totals: Vec<AggregatedStatsSummary>,
            }

            let stop_monitor = stop_monitor.clone();
            move || {
                let mut per_task_total_reads = HashMap::new();
                let op_size = 1 << args.block_size_shift.get();
                let mut total = AggregatedStats::new(op_size);
                let mut total_summaries = Vec::new();
                let mut this_round = AggregatedStats::new(op_size);

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .unwrap();

                rt.block_on(clients_and_monitor_ready.wait());

                let mut ticker = rt.block_on(async move { tokio::time::interval(MONITOR_PERIOD) });

                let mut exit = false;
                while !exit {
                    this_round.reset(std::time::Instant::now());
                    rt.block_on(async {
                        let ticker = &mut ticker;
                        tokio::select! {
                            _ = ticker.tick() => {}
                            _ = stop_monitor.cancelled() => {
                                exit = true;
                            }
                        };
                    });

                    for (client, counter) in stats_state.reads_in_last_second.iter().enumerate() {
                        let task_reads_in_last_second = counter.swap(0, Ordering::Relaxed);
                        let per_task = per_task_total_reads.entry(client).or_insert(0);
                        *per_task += task_reads_in_last_second;
                        this_round.op_count += task_reads_in_last_second;
                        total.op_count += task_reads_in_last_second;
                    }
                    for h in &stats_state.latencies_histo {
                        let mut h = h.lock().unwrap();
                        total.latencies_histo += &*h;
                        this_round.latencies_histo += &*h;
                        h.clear();
                    }

                    let this_round_summary = this_round.summary_since_start();
                    let total_summary = total.summary_since_start();

                    info!("{this_round_summary}");
                    info!("{total_summary}");

                    total_summaries.push(total_summary);
                }

                info!("monitor shutting down");

                // dump per-task total reads into a json file.
                // Useful to judge fairness (i.e., did each client task get about the same nubmer of ops in a time-based run).
                //
                // command line to get something for copy-paste into google sheets:
                //  ssh neon-devvm-mbp ssh testinstance sudo cat /mnt/per_task_total_reads.json | jq '.[]' | pbcopy
                let sorted_per_task_total_reads =
                    per_task_total_reads.values().cloned().sorted().collect();
                let output = BenchmarkOutput {
                    args: args.as_ref().clone(),
                    sorted_per_task_total_reads,
                    totals: total_summaries,
                };
                let outpath = std::path::PathBuf::from("benchmark.output.json");
                info!("writing results to to {:?}", outpath);
                std::fs::write(&outpath, serde_json::to_string(&output).unwrap()).unwrap();

                let total_summary = total.summary_since_start();
                info!("total: {}", total_summary);
            }
        })
        .unwrap();

    engine.run(
        args.clone(),
        works,
        clients_and_monitor_ready,
        stop_engine,
        stats_state,
    );
    stop_monitor.cancel();
    monitor.join().unwrap();
}

#[derive(Clone)]
struct OpsLeft(Option<Arc<AtomicI64>>);

struct ClientWork {
    ops_left: OpsLeft,
    kind: ClientWorkKind,
}

impl OpsLeft {
    fn take_one_op(&self) -> ControlFlow<()> {
        match &self.0 {
            None => (),
            Some(ops_left) => {
                let ops_left = ops_left.fetch_sub(1, Ordering::Relaxed);
                if ops_left < 0 {
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }
}

enum ClientWorkKind {
    DiskAccess {
        file: std::fs::File,
        validate: bool,
    },
    TimerFdSetStateAndRead {
        timerfd: timerfd::TimerFd,
        duration: Duration,
    },
    NoWork {},
}

fn setup_client_works(args: &Args) -> Vec<ClientWork> {
    let ops_left = OpsLeft(match args.run_duration {
        RunDuration::UntilCtrlC => None,
        RunDuration::FixedDuration(_) => None,
        RunDuration::FixedTotalIoCount(total_io_count) => Some(Arc::new(AtomicI64::new(
            i64::try_from(total_io_count).unwrap(),
        ))),
    });
    match &args.work_kind {
        WorkKind::DiskAccess {
            disk_access_kind,
            validate,
        } => {
            setup_files(&args, disk_access_kind);
            // assert invariant and open files
            let mut client_files = Vec::new();
            for i in 0..args.num_clients.get() {
                let file_path = data_file_path(args, i);
                let md = std::fs::metadata(&file_path).unwrap();
                assert!(md.len() >= args.file_size_mib.get() * 1024 * 1024);

                let file = open_file_direct_io(
                    disk_access_kind,
                    OpenFileMode::Read,
                    &data_file_path(args, i),
                );
                client_files.push(ClientWork {
                    ops_left: ops_left.clone(),
                    kind: ClientWorkKind::DiskAccess {
                        file,
                        validate: match validate {
                            ValidateMode::NoValidate => false,
                            ValidateMode::Validate => true,
                        },
                    },
                });
            }
            client_files
        }
        WorkKind::TimerFd { expiration_mode } => (0..args.num_clients.get())
            .map(|_| match expiration_mode {
                TimerFdExperiationModeKind::Oneshot { micros, engine: _ } => ClientWork {
                    ops_left: ops_left.clone(),
                    kind: ClientWorkKind::TimerFdSetStateAndRead {
                        timerfd: {
                            timerfd::TimerFd::new_custom(timerfd::ClockId::Monotonic, false, true)
                                .unwrap()
                        },
                        duration: Duration::from_micros(micros.get()),
                    },
                },
            })
            .collect(),
        WorkKind::NoWork { engine: _ } => (0..args.num_clients.get())
            .map(|_| ClientWork {
                ops_left: ops_left.clone(),
                kind: ClientWorkKind::NoWork {},
            })
            .collect(),
    }
}

fn data_dir(_args: &Args) -> PathBuf {
    std::path::PathBuf::from("data")
}
fn data_file_path(_args: &Args, client_num: u64) -> PathBuf {
    std::path::PathBuf::from("data").join(format!("client_{}.data", client_num))
}

fn alloc_self_aligned_buffer(size: usize) -> *mut u8 {
    let buf_ptr = unsafe { std::alloc::alloc(Layout::from_size_align(size, size).unwrap()) };
    assert!(!buf_ptr.is_null());
    buf_ptr
}

fn setup_files(args: &Args, disk_access_kind: &DiskAccessKind) {
    let data_dir = data_dir(args);
    std::fs::create_dir_all(&data_dir).unwrap();
    std::thread::scope(|scope| {
        for i in 0..args.num_clients.get() {
            let file_path = data_file_path(args, i);
            let (append_offset, append_megs) = match std::fs::metadata(&file_path) {
                Ok(md) => {
                    if md.len() >= args.file_size_mib.get() * 1024 * 1024 {
                        (0, 0)
                    } else {
                        info!("File {:?} exists but has wrong size", file_path);
                        let rounded_down_megs = md.len() / (1024 * 1024);
                        let rounded_down_offset = rounded_down_megs * 1024 * 1024;
                        let append_megs = args
                            .file_size_mib
                            .get()
                            .checked_sub(rounded_down_megs)
                            .unwrap();
                        (rounded_down_offset, append_megs)
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (0, args.file_size_mib.get()),
                Err(e) => panic!("Error while checking file {:?}: {}", file_path, e),
            };
            if append_megs == 0 {
                continue;
            }
            let mut file =
                open_file_direct_io(disk_access_kind, OpenFileMode::WriteNoTruncate, &file_path);
            file.seek(std::io::SeekFrom::Start(append_offset)).unwrap();

            // fill the file with pseudo-random data
            scope.spawn(move || {
                let chunk = alloc_self_aligned_buffer(1 << 20);
                let chunk = unsafe { std::slice::from_raw_parts_mut(chunk, 1 << 20) };
                for _ in 0..append_megs {
                    rand::thread_rng().fill_bytes(chunk);
                    file.write_all(&chunk).unwrap();
                }
            });
        }
    });
}

fn setup_engine(engine_kind: &EngineKind) -> Box<dyn Engine> {
    match engine_kind {
        EngineKind::Std => Box::new(EngineStd {}),
        EngineKind::TokioOnExecutorThread => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            Box::new(EngineTokioOnExecutorThread { rt })
        }
        EngineKind::TokioSpawnBlocking {
            spawn_blocking_pool_size,
        } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .max_blocking_threads(spawn_blocking_pool_size.get() as usize)
                .build()
                .unwrap();
            Box::new(EngineTokioSpawnBlocking { rt })
        }
        EngineKind::TokioEpollUring => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                // .worker_threads(1) // useful for debugging
                .enable_all()
                .build()
                .unwrap();
            Box::new(EngineTokioEpollUring { rt })
        }
    }
}

enum OpenFileMode {
    Read,
    WriteNoTruncate,
}

fn open_file_direct_io(
    disk_access_kind: &DiskAccessKind,
    mode: OpenFileMode,
    path: &Path,
) -> std::fs::File {
    let (read, write) = match mode {
        OpenFileMode::Read => (true, false),
        OpenFileMode::WriteNoTruncate => (false, true),
    };
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::prelude::OpenOptionsExt;
        let mut options = std::fs::OpenOptions::new();
        options.read(read).write(write).create(write);
        match disk_access_kind {
            DiskAccessKind::DirectIo { engine: _ } => {
                options.custom_flags(libc::O_DIRECT);
            }
            DiskAccessKind::CachedIo { engine: _ } => {}
        }
        options.open(path).unwrap()
    }

    // https://github.com/axboe/fio/issues/48
    // summarized in https://github.com/ronomon/direct-io/issues/1#issuecomment-360331547
    // => macOS does not support O_DIRECT, but we can use fcntl to set F_NOCACHE
    // If the file pages are in the page cache, this has no effect though.

    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let file = std::fs::OpenOptions::new()
            .read(read)
            .write(write)
            .create(write)
            .open(path)
            .unwrap();
        match args.direct_io {
            WorkKind::DirectIo => {
                let fd = file.as_raw_fd();
                let res = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
                assert_eq!(res, 0);
            }
            WorkKind::CachedIo => {}
        }
        file
    }
}
impl Engine for EngineStd {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let myself = Arc::new(*self);
        std::thread::scope(|scope| {
            assert_eq!(works.len(), args.num_clients.get() as usize);
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let myself = Arc::clone(&myself);
                scope.spawn({
                    let args = Arc::clone(&args);
                    let clients_ready = Arc::clone(&clients_ready);
                    move || {
                        tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap()
                            .block_on(clients_ready.wait());
                        myself.client(i, Arc::clone(&args), work, &stop, stats_state)
                    }
                });
            }
        });
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
        file: &mut std::fs::File,
        offset: u64,
        buf: &mut [u8],
    ) {
        file.read_at(buf, offset).unwrap();
        // TODO: verify
    }
}

struct EngineTokioOnExecutorThread {
    rt: tokio::runtime::Runtime,
}

impl Engine for EngineTokioOnExecutorThread {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let rt = &self.rt;
        rt.block_on(async {
            let mut handles = Vec::new();
            assert_eq!(works.len(), args.num_clients.get() as usize);
            let all_client_tasks_spawned =
                Arc::new(tokio::sync::Barrier::new(args.num_clients.get() as usize));
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let all_client_tasks_spawned = Arc::clone(&all_client_tasks_spawned);
                let clients_ready = Arc::clone(&clients_ready);
                handles.push(tokio::spawn({
                    let args = Arc::clone(&args);
                    async move {
                        clients_ready.wait().await;
                        Self::client(
                            i,
                            Arc::clone(&args),
                            work,
                            &stop,
                            stats_state,
                            all_client_tasks_spawned,
                        )
                        .await
                    }
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
}

impl EngineTokioOnExecutorThread {
    async fn client(
        i: u64,
        args: Arc<Args>,
        mut work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
        all_client_tasks_spawned: Arc<tokio::sync::Barrier>,
    ) {
        tracing::info!("Client {i} starting");
        all_client_tasks_spawned.wait().await;
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

        // alloc aligned to make O_DIRECT work
        let buf = {
            let buf_ptr = unsafe {
                std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap())
            };
            assert!(!buf_ptr.is_null());
            let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf_ptr, block_size) };
            buf
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
            match &mut work.kind {
                ClientWorkKind::DiskAccess { file, validate } => {
                    if *validate {
                        unimplemented!()
                    }
                    file.read_at(buf, offset_in_file).unwrap();
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
}

impl Engine for EngineTokioSpawnBlocking {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let rt = &self.rt;
        rt.block_on(async {
            let mut handles = Vec::new();
            assert_eq!(works.len(), args.num_clients.get() as usize);
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let clients_ready = Arc::clone(&clients_ready);
                handles.push(tokio::spawn({
                    let args = Arc::clone(&args);
                    async move {
                        clients_ready.wait().await;
                        Self::client(i, Arc::clone(&args), work, &stop, stats_state).await
                    }
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
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
            unsafe impl Send for SendPtr {} // the thread spawned in the loop below doesn't outlive this function (we're polled t completion)
            unsafe impl Sync for SendPtr {} // the loop below ensures only one thread accesses it at a time
            let buf = SendPtr(buf_ptr);
            // extra scope so that it doesn't outlive any await points, it's not Send, only the SendPtr wrapper is
            buf
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
                        let buf = buf;
                        let buf: &mut [u8] =
                            unsafe { std::slice::from_raw_parts_mut(buf.0, block_size) };
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

struct EngineTokioFlume {
    rt: tokio::runtime::Runtime,
    num_workers: NonZeroU64,
    queue_depth: NonZeroU64,
}

impl Engine for EngineTokioFlume {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let myself = Arc::new(*self);
        let (worker_tx, work_rx) = flume::bounded(myself.queue_depth.get() as usize);
        let mut handles = Vec::new();
        for _ in 0..myself.num_workers.get() {
            let work_rx = work_rx.clone();
            let handle = std::thread::spawn(move || Self::worker(work_rx));
            handles.push(handle);
        }
        // workers get stopped by the worker_tx being dropped
        scopeguard::defer!(for handle in handles {
            handle.join().unwrap();
        });
        myself.rt.block_on(async {
            let mut handles = Vec::new();
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let worker_tx = worker_tx.clone();
                let myself = Arc::clone(&myself);
                let clients_ready = Arc::clone(&clients_ready);
                handles.push(tokio::spawn({
                    let args = Arc::clone(&args);
                    async move {
                        clients_ready.wait().await;
                        myself
                            .client(worker_tx, i, &args, work, &stop, stats_state)
                            .await
                    }
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
        drop(worker_tx); // stops the workers
    }
}

type FlumeWork = Box<dyn FnOnce() -> std::io::Result<()> + Send + 'static>;

struct FlumeWorkRequest {
    work: FlumeWork,
    response: tokio::sync::oneshot::Sender<std::io::Result<()>>,
}

impl EngineTokioFlume {
    async fn client(
        &self,
        worker_tx: flume::Sender<FlumeWorkRequest>,
        i: u64,
        args: &Args,
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
            unsafe impl Send for SendPtr {} // the thread spawned in the loop below doesn't outlive this function (we're polled t completion)
            unsafe impl Sync for SendPtr {} // the loop below ensures only one thread accesses it at a time
            let buf = SendPtr(buf_ptr);
            // extra scope so that it doesn't outlive any await points, it's not Send, only the SendPtr wrapper is
            buf
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
            let work = Box::new(move || {
                match fd {
                    ClientWorkFd::DiskAccess {
                        raw_fd: file_fd,
                        validate,
                    } => {
                        if validate {
                            unimplemented!()
                        }
                        let buf = buf;
                        let buf: &mut [u8] =
                            unsafe { std::slice::from_raw_parts_mut(buf.0, block_size) };
                        let file = unsafe { std::fs::File::from_raw_fd(file_fd) };
                        file.read_at(buf, offset_in_file).unwrap();
                        file.into_raw_fd(); // so that it's there for next iteration
                        Ok(())
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
                        Ok(())
                    }
                    ClientWorkFd::NoWork => Ok(()),
                }
            });
            // TODO: can this dealock with rendezvous channel, i.e., queue_depth=0?
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let start = std::time::Instant::now();
            worker_tx
                .send_async(FlumeWorkRequest {
                    work,
                    response: response_tx,
                })
                .await
                .unwrap();
            response_rx
                .await
                .expect("rx flume")
                .expect("not expecting io errors");
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            stats_state.record_iop_latency(usize::try_from(i).unwrap(), start.elapsed());
        }
        info!("Client {i} stopping");
    }
    fn worker(rx: flume::Receiver<FlumeWorkRequest>) {
        loop {
            let FlumeWorkRequest { work, response } = match rx.recv() {
                Ok(w) => w,
                Err(flume::RecvError::Disconnected) => {
                    info!("Worker stopping");
                    return;
                }
            };
            let res = work();
            match response.send(res) {
                Ok(()) => (),
                Err(x) => {
                    error!("Failed to send response: {:?}", x);
                }
            }
        }
    }
}

struct EngineTokioEpollUring {
    rt: tokio::runtime::Runtime,
}

impl Engine for EngineTokioEpollUring {
    fn run(
        self: Box<Self>,
        args: Arc<Args>,
        works: Vec<ClientWork>,
        clients_ready: Arc<tokio::sync::Barrier>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
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
                        Self::client(i, &args, work, &stop, stats_state).await
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
            for (i, handle) in handles.into_iter().enumerate() {
                info!("awaiting client {i}");
                handle.await.unwrap();
                stopped_handles[i].store(true, Ordering::Relaxed);
            }
            stop_stopped_task_status_task.store(true, Ordering::Relaxed);
            info!("awaiting stopped_task_status_task");
            stopped_task_status_task.await.unwrap();
            info!("stopped_task_status_task stopped");
        });
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
