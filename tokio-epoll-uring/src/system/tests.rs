use std::{
    io::Write,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use crate::{
    metrics::MetricsStorage, system::test_util::shared_system_handle::SharedSystemHandle, System,
};

// TODO: turn into a does-not-compile test
// #[tokio::test]
// async fn get_slot_panics_if_used_after_shutdown() {
//     let handle = crate::launch_owned().await;
//     handle.shutdown().await;
//     // handle.
//     // .with_submit_side(|submit_side| {
//     //     let mut guard = submit_side.0.lock().unwrap();
//     //     let guard = guard.must_open();
//     //     guard.get_ops_slot()
//     // })
//     // .await;
// }

#[tokio::test]
async fn drop_system_handle() {
    let system = System::launch().await;
    drop(system);
}

#[tokio::test]
async fn op_state_pending_but_future_dropped() {
    // Get the op slot into state PendingButFutureDropped
    // then let process_completions run and see what happens.

    let system = SharedSystemHandle::launch().await.unwrap();

    let (reader, mut writer) = os_pipe::pipe().unwrap();
    let reader = unsafe { OwnedFd::from_raw_fd(nix::unistd::dup(reader.as_raw_fd()).unwrap()) };

    let buf = vec![0; 1];
    let mut read_fut = Box::pin(system.read(reader, 0, buf));
    let stop_polling_read_fut = CancellationToken::new();
    let jh = tokio::spawn({
        let stop_polling_read_fut = stop_polling_read_fut.clone();
        #[allow(clippy::async_yields_async)]
        async move {
            tokio::select! {
                _ = &mut read_fut => { unreachable!("we don't write to the pipe") }
                _ = stop_polling_read_fut.cancelled() => {
                    read_fut
                }
            }
        }
    });

    // TODO don't rely on timing for read_fut to reach Pending state
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!jh.is_finished());
    stop_polling_read_fut.cancel();
    let read_fut = jh.await.unwrap();

    // assert!(matches!(read_fut), ...) it's an `async fn`, can't match :(

    drop(read_fut);
    // op should be in state PendingButFutureDropped by now

    // wake up poller task to process completions
    writer.write_all(&[1]).unwrap();

    system.initiate_shutdown().await;
}

#[tokio::test]
async fn basic() {
    let system = SharedSystemHandle::launch().await.unwrap();

    let (reader, mut writer) = os_pipe::pipe().unwrap();
    let reader = unsafe { OwnedFd::from_raw_fd(nix::unistd::dup(reader.as_raw_fd()).unwrap()) };

    writer.write_all(&[1]).unwrap();

    let buf = vec![0; 1];
    let ((_, buf), res) = system.read(reader, 0, buf).await;
    let sz = res.unwrap();
    assert_eq!(sz, 1);
    assert_eq!(buf, vec![1]);

    system.initiate_shutdown().await;
}

// This test changes & observes process-wide state.
// To avoid requiring cargo nextest / --test-threads 1, we do some trickery.
// TODO: find means to avoid this trickery / make it more robust.
#[tokio::test]
async fn hitting_memlock_limit_does_not_panic() {
    let max_number_of_systems_spawned_by_other_tests: usize = 100; // other tests affect VmLck as well.

    let (soft, hard) =
        nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_MEMLOCK).unwrap();
    let expect_system_memlock_usage = 16 * 1024; // TODO: depends on RING_SIZE
                                                 // lower the softlimit such that the test will complete quickly but also don't
                                                 // lower it so much that other tests will fail
    let temp_softlimit: u64 =
        2 * (max_number_of_systems_spawned_by_other_tests as u64) * expect_system_memlock_usage;
    assert!(temp_softlimit <= hard);
    nix::sys::resource::setrlimit(
        nix::sys::resource::Resource::RLIMIT_MEMLOCK,
        temp_softlimit,
        hard,
    )
    .unwrap();
    scopeguard::defer!({
        nix::sys::resource::setrlimit(nix::sys::resource::Resource::RLIMIT_MEMLOCK, soft, hard)
            .unwrap();
    });

    let get_vm_lck = || {
        let s = std::fs::read_to_string("/proc/self/status").unwrap();
        let mut iter = s.lines().filter_map(|line| {
            let (pre, suff) = line.split_once(':')?;
            if pre != "VmLck" {
                return None;
            }
            let (num, unit) = {
                let comps: Vec<_> = suff.split_whitespace().collect();
                assert_eq!(comps.len(), 2);
                (comps[0], comps[1])
            };
            assert_eq!(unit, "kB");
            let num: u64 = num.parse().unwrap();
            Some(num * 1024)
        });
        let first = iter.next().unwrap();
        assert!(iter.next().is_none());
        first
    };

    let mut systems = Vec::new();
    let mut vm_lck_observations = vec![];
    loop {
        let res = System::launch().await;
        vm_lck_observations.push(get_vm_lck());
        match res {
            Ok(system) => {
                // use the uring in case the memory is allocated lazily
                let ((), res) = system.nop().await;
                res.unwrap();
                systems.push(system); // keep alive until end of test

                // Pass the test if our kernel
                // is recent enough that SQ and CQ aren't accounted as locked memory.
                // E.g., on 5.10 LTS kernels < 5.10.162 (and generally mainline kernels < 5.12),
                // io_uring will account the memory of the CQ and SQ as locked.
                // More details: https://github.com/neondatabase/neon/issues/6373#issuecomment-1905814391
                if vm_lck_observations.len() > max_number_of_systems_spawned_by_other_tests {
                    let mut sorted = vm_lck_observations.clone();
                    sorted.sort();
                    let remainder = &sorted[max_number_of_systems_spawned_by_other_tests..];
                    if remainder.len() < 2 {
                        continue;
                    }
                    // we should see a trend line
                    let min = remainder.iter().min();
                    let max = remainder.iter().max();
                    if min == max {
                        println!("it seems like CQ and SQ aren't accounted as locked memory by the kernel");
                        println!("VmLock observations: {vm_lck_observations:?}");
                        return;
                    } else {
                        // strong monotonicity
                        let mut last = remainder[0];
                        for i in &remainder[1..] {
                            assert!(last < *i);
                            last = *i;
                        }
                    }
                }
            }
            Err(e) => match e {
                crate::system::lifecycle::LaunchResult::IoUringBuild(e) => {
                    assert_eq!(e.kind(), std::io::ErrorKind::OutOfMemory);
                    // run this test with --test-threads=1 or nextest to get predictable results for systems.len() under a given ulimit
                    println!("hit limit after {} iterations", systems.len(),);
                    return;
                }
            },
        }
    }
}

#[test]
fn test_metrics() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let metrics = Box::leak(Box::new(MetricsStorage::new_const()));
    let metrics_ptr = metrics as *mut _;
    let system = rt
        .block_on(System::launch_with_testing(None, None, metrics))
        .unwrap();
    assert_eq!(
        1,
        metrics
            .systems_created
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    assert_eq!(
        0,
        metrics
            .systems_destroyed
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    rt.block_on(system.initiate_shutdown());

    assert_eq!(
        1,
        metrics
            .systems_created
            .load(std::sync::atomic::Ordering::Relaxed)
    );
    assert_eq!(
        1,
        metrics
            .systems_destroyed
            .load(std::sync::atomic::Ordering::Relaxed)
    );

    // SAFETY: we shut down the system, nothing references the `metrics`
    drop(unsafe { Box::from_raw(metrics_ptr) });
}

#[tokio::test]
async fn test_statx() {
    let system = System::launch().await.unwrap();

    let tempdir = tempfile::tempdir().unwrap();

    let file_path = tempdir.path().join("some_file");
    let content = b"some content";
    std::fs::write(&file_path, content).unwrap();

    let std_file = std::fs::File::open(&file_path).unwrap();
    let fd = OwnedFd::from(std_file);

    // happy path
    let (fd, res) = system.statx(fd).await;
    let stat = res.expect("we know it exists");
    assert_eq!(content.len() as u64, stat.stx_size);

    std::fs::remove_file(&file_path).unwrap();

    // can do statx on unlinked file
    let (fd, res) = system.statx(fd).await;
    let stat = res.expect("we know it exists");
    assert_eq!(content.len() as u64, stat.stx_size);

    drop(fd);

    // TODO: once we add statx with pathname instead of file descriptor,
    // ensure we get NotFound back when the file doesn't exist.
}

#[tokio::test]
async fn test_write() {
    let system = System::launch().await.unwrap();

    let tempdir = tempfile::tempdir().unwrap();

    let file_path = tempdir.path().join("some_file");
    let std_file = std::fs::File::create(&file_path).unwrap();
    let fd = OwnedFd::from(std_file);

    let write1 = b"some";
    let write2 = b"content";
    let ((fd, _), res) = system.write(fd, 0, write1.to_vec()).await;
    res.unwrap();

    assert_eq!(&write1[..], &std::fs::read(&file_path).unwrap());

    // make sure there's no hidden file cursor underneath, i.e., that it's really write_at
    let ((fd, _), res) = system.write(fd, 2, write2.to_vec()).await;
    res.unwrap();

    assert_eq!(
        {
            let mut expect = vec![];
            expect.extend_from_slice(&write1[0..2]);
            expect.extend(write2);
            expect
        },
        std::fs::read(&file_path).unwrap()
    );

    drop(fd);
}
