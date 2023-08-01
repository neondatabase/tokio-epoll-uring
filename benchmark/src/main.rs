use std::{
    alloc::Layout,
    collections::HashMap,
    io::{Seek, Write},
    num::{NonZeroU64, NonZeroUsize},
    ops::ControlFlow,
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
use engines::{
    std_thread::EngineStd, tokio_epoll_uring::EngineTokioEpollUring,
    tokio_on_executor_thread::EngineTokioOnExecutorThread,
    tokio_spawn_blocking::EngineTokioSpawnBlocking, tokio_uring::EngineTokioUring,
};
use hdrhistogram::Counter;
use itertools::Itertools;
use rand::RngCore;
use serde_with::serde_as;
use tracing::{error, info};

mod engines;

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
    FixedPerClientIoCount(u64),
}

impl FromStr for RunDuration {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "until-ctrl-c" => Ok(RunDuration::UntilCtrlC),
            x if x.ends_with("ios-total") => {
                let stripped = &s[..s.len() - "ios-total".len()];
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
            x if x.ends_with("ios-per-client") => {
                let stripped = &s[..s.len() - "ios-per-client".len()];
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
                    Ok(n) => Ok(RunDuration::FixedPerClientIoCount(n.get() * multiplier)),
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
        spawn_blocking_pool_size: NonZeroUsize,
    },
    TokioEpollUring,
    TokioUring,
}

struct StatsState {
    reads_in_last_second: Vec<crossbeam_utils::CachePadded<AtomicU64>>,
    latencies_histo: Vec<crossbeam_utils::CachePadded<Mutex<hdrhistogram::Histogram<u64>>>>,
}

impl StatsState {
    fn make_latency_histogram() -> hdrhistogram::Histogram<u64> {
        hdrhistogram::Histogram::new_with_bounds(1, 1_000_000_000, 3).unwrap()
    }
    fn record_iop_latency(&self, client_num: usize, latency: Duration) {
        let mut h = self.latencies_histo[client_num].lock().unwrap();
        h.record(u64::try_from(latency.as_nanos()).unwrap())
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
    ) -> EngineRunResult;
}

struct EngineRunResult {
    client_run_times: Vec<Duration>,
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
    let (stop_monitor_tx, mut stop_monitor_rx) = tokio::sync::oneshot::channel::<EngineRunResult>();

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
        RunDuration::FixedPerClientIoCount(_) => {
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
                values: &[f64; LATENCY_PERCENTILES.len()],
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
                latency_min_us: f64,
                latency_mean_us: f64,
                latency_max_us: f64,
                #[serde(serialize_with = "latency_percentiles_serialize")]
                latency_percentiles: [f64; LATENCY_PERCENTILES.len()],
            }

            impl std::fmt::Display for AggregatedStatsSummary {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(
                        f,
                        "t{:.2} TP: iops={:.0} bw={:.2} LAT(us): min={:.0} mean={:.0} max={:.0} {}",
                        self.elapsed_us.as_secs_f64(),
                        self.throughput_iops,
                        self.throughput_bw_mibps,
                        self.latency_min_us,
                        self.latency_mean_us,
                        self.latency_max_us,
                        self.latency_percentiles
                            .iter()
                            .zip(LATENCY_PERCENTILES.iter())
                            .map(|(v, p)| format!("p{p}={v:.0}"))
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
                        latency_min_us: histo.min().as_f64() / 1000.0,
                        latency_mean_us: histo.mean() / 1000.0,
                        latency_max_us: histo.max().as_f64() / 1000.0,
                        latency_percentiles: {
                            let mut values = [0.0; LATENCY_PERCENTILES.len()];
                            for (i, value_ref) in values.iter_mut().enumerate() {
                                *value_ref =
                                    histo.value_at_percentile(LATENCY_PERCENTILES[i]).as_f64()
                                        / 1000.0;
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
                sorted_per_task_runtimes_secs: Vec<f64>,
                totals: Vec<AggregatedStatsSummary>,
            }

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

                let mut exit: Option<EngineRunResult> = None;
                while exit.is_none() {
                    this_round.reset(std::time::Instant::now());
                    rt.block_on(async {
                        let ticker = &mut ticker;
                        tokio::select! {
                            _ = ticker.tick() => {}
                            msg = &mut stop_monitor_rx => {
                                exit = Some(msg.unwrap());
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
                let exit = exit.unwrap();

                info!("monitor shutting down");

                // dump per-task total reads into a json file.
                // Useful to judge fairness (i.e., did each client task get about the same nubmer of ops in a time-based run).
                //
                // command line to get something for copy-paste into google sheets:
                //  ssh neon-devvm-mbp ssh testinstance sudo cat /mnt/per_task_total_reads.json | jq '.[]' | pbcopy
                let sorted_per_task_total_reads =
                    per_task_total_reads.values().cloned().sorted().collect();
                let sorted_per_task_runtimes_secs = exit
                    .client_run_times
                    .into_iter()
                    .sorted()
                    .map(|d| d.as_secs_f64())
                    .collect();
                let output = BenchmarkOutput {
                    args: args.as_ref().clone(),
                    sorted_per_task_total_reads,
                    sorted_per_task_runtimes_secs,
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

    let res = engine.run(
        args.clone(),
        works,
        clients_and_monitor_ready,
        stop_engine,
        stats_state,
    );
    stop_monitor_tx
        .send(res)
        .ok()
        .expect("monitor must not exit by itself");
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
                if ops_left <= 0 {
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
    let mut fixed_total_io_count_ops_left = None;
    let mut get_ops_left_for_client = || {
        match args.run_duration {
            RunDuration::UntilCtrlC => OpsLeft(None),
            RunDuration::FixedDuration(_) => OpsLeft(None),
            RunDuration::FixedTotalIoCount(total_io_count) => {
                let shared = fixed_total_io_count_ops_left.get_or_insert_with(|| {
                    Arc::new(AtomicI64::new(i64::try_from(total_io_count).unwrap()))
                });
                OpsLeft(Some(Arc::clone(shared)))
            }
            RunDuration::FixedPerClientIoCount(per_client) => {
                // create a separate OpsLeft per client
                OpsLeft(Some(Arc::new(AtomicI64::new(
                    i64::try_from(per_client).unwrap(),
                ))))
            }
        }
    };
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
                    ops_left: get_ops_left_for_client(),
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
                    ops_left: get_ops_left_for_client(),
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
                ops_left: get_ops_left_for_client(),
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
        EngineKind::TokioOnExecutorThread => Box::new(EngineTokioOnExecutorThread::new()),
        EngineKind::TokioSpawnBlocking {
            spawn_blocking_pool_size,
        } => Box::new(EngineTokioSpawnBlocking::new(*spawn_blocking_pool_size)),
        EngineKind::TokioEpollUring => Box::new(EngineTokioEpollUring::new()),
        EngineKind::TokioUring => Box::new(EngineTokioUring::new()),
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
}
