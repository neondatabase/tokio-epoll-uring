use std::{
    io::Write,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::Arc,
    time::Duration,
};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::task::{unconstrained, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    metrics::GlobalMetricsStorage,
    system::{
        test_util::{shared_system_handle::SharedSystemHandle, timerfd, FOREVER},
        RING_SIZE,
    },
    System,
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
    let metrics = Box::leak(Box::new(GlobalMetricsStorage::new_const()));
    let metrics_ptr = metrics as *mut _;
    let system = rt
        .block_on(System::launch_with_testing(
            None,
            None,
            metrics,
            Arc::new(()),
        ))
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

/// Scenario: More tasks than slots; each tasks `.await`s one operation at a time.
///
/// NB: In this test, we use the pattern of `select! { ..., sleep(2 seconds) }` to drive op futures
/// to the point where they are enqueued and occupy a slot. This will become flaky if that takes
/// more than 2 seconds. A more deterministic way to do it would be to use
/// #[tokio::test(start_paused=true)].
#[tokio::test]
async fn test_slot_exhaustion_behavior_when_op_future_gets_dropped() {
    let system = System::launch().await.unwrap();
    let system = Arc::new(system);

    // rack up 3*RING_SIZE tasks that wait forever
    let mut submitted_or_enqueued = Vec::new();
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut tasks = JoinSet::new();
    for _ in 0..3 * RING_SIZE {
        let system = system.clone();
        let fd = Arc::new(timerfd::oneshot(FOREVER));
        let cancel = cancel.child_token();
        let (tx, rx) = tokio::sync::oneshot::channel();
        submitted_or_enqueued.push(rx);
        tasks.spawn(async move {
            let fut = timerfd::must_read(Arc::clone(&fd), &system);
            let mut fut = std::pin::pin!(fut);
            tokio::select! {
                biased; // to ensure we poll system.read() before notifying the test task
                _ = &mut fut => {
                    unreachable!("timerfd only fires in far future")
                }
                _ = tokio::time::sleep(Duration::from_secs(2)) => { }
            }
            tx.send(()).expect("test bug");
            tokio::select! {
                _ = &mut fut => {
                    unreachable!()
                }
                _ = cancel.cancelled() => { drop(fut); fd }
            }
        });
    }

    for rx in submitted_or_enqueued {
        rx.await.expect("test bug");
    }

    // all the futures have been submitted, drop them
    cancel.cancel();
    let mut timerfds = Vec::new();
    while let Some(res) = tasks.join_next().await {
        let timerfd = res.unwrap();
        timerfds.push(timerfd);
    }

    // the slots are still blocked on the timerfd
    // TODO: assert that directly
    // assert it by starting a new read and check that that read will be Pending forever
    let fire_in = Duration::from_secs(1);
    let fd = timerfd::oneshot(fire_in);
    tokio::time::sleep(2 * fire_in).await;
    let fut = timerfd::must_read(fd, system.clone());
    let mut fut = std::pin::pin!(fut);
    tokio::select! {
        biased; // ensure future gets queued first
        _ = &mut fut => {
            panic!("future shouldn't be ready because all slots are still used")
        }
        _ = tokio::time::sleep(Duration::from_secs(2)) => { }
    }

    // unblock the tasks by firing their timerfds sooner, otherwise shutdown hangs forever
    for timerfd in timerfds {
        timerfd.set(Duration::from_millis(1));
    }

    // our read should complete because unblocking of the tasks
    // frees up slots
    let _: () = fut.await;

    Arc::into_inner(system).unwrap().initiate_shutdown().await;
}

/// Scenario: a single tasks creates many futures that get submitted
/// and hence occupy a slot, but the future never gets polled to completion,
/// even though the io_uring-level operation has long completed.
///
/// The current behavior is that the operation waits for a slot to
/// become available, i.e., it never completes.
///
/// NB: In this test, we use the pattern of `select! { ..., sleep(2 seconds) }` to drive op futures
/// to the point where they are enqueued and occupy a slot. This will become flaky if that takes
/// more than 2 seconds. A more deterministic way to do it would be to use
/// #[tokio::test(start_paused=true)].
#[tokio::test]
async fn test_slot_exhaustion_behavior_when_op_completes_but_future_does_not_get_polled() {
    let system = Arc::new(System::launch().await.unwrap());

    // Use up all slots.
    let mut reads = FuturesUnordered::new();
    let mut timerfds = Vec::new();
    for _ in 0..RING_SIZE {
        let oneshot = timerfd::oneshot(FOREVER);
        let oneshot = Arc::new(oneshot);
        let mut fut = Box::pin(tokio::task::unconstrained(timerfd::must_read(
            oneshot.clone(),
            system.clone(),
        )));
        let res = futures::poll!(&mut fut);
        assert!(res.is_pending());
        reads.push(fut);
        timerfds.push(oneshot);
    }

    // An additional op will now wait forever for a free slot.
    let mut nop = Box::pin(unconstrained(system.nop()));
    tokio::select! {
        biased; // ensure future gets queued first
        res = &mut nop => {
            panic!("nop shouldn't be able to get a slot because all slots are already used: {res:?}")
        }
        _ = tokio::time::sleep(Duration::from_secs(2)) => { }
    }

    // make the io_uring operations complete
    for timerfd in timerfds {
        timerfd.set(Duration::from_millis(1));
    }

    // despite the completed io_uring operations, our nop future is still waiting for a slot
    tokio::select! {
        biased; // ensure future gets queued first
        res = &mut nop => {
            panic!("nop shouldn't be able to get a slot because all slots are still used: {res:?}")
        }
        _ = tokio::time::sleep(Duration::from_secs(2)) => { }
    }

    //
    // Cleanup
    //
    while let Some(()) = reads.next().await {}

    // nop can now get a slot because the read futs have been polled to completion
    let ((), res) = nop.await;
    res.unwrap();

    Arc::into_inner(system).unwrap().initiate_shutdown().await;
}

#[test]
fn repro_ecancelled1() {
    let timerfd_timeout = Duration::from_secs(10);
    let fd = Arc::new(timerfd::oneshot(timerfd_timeout));
    let started_at = std::time::Instant::now();
    let timerfd_fires_at = started_at + timerfd_timeout;

    let system = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Create a system on t1
        let system = Arc::new(rt.block_on(rt.spawn(System::launch())).unwrap().unwrap());
        // this moves the poller task to a dedicated thread
        // That itself is not the point of this test, but, I want to isolate the reason for the ECANCELLED
        // to the fact that the _submitting_ thread died, not some polling thread changed.
        drop(rt);
        system
    })
    .join()
    .unwrap();
    println!("launched system");

    let fut = std::thread::spawn({
        let system = Arc::clone(&system);
        let fd = Arc::clone(&fd);
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let submission = rt.spawn({
                async move {
                    let mut fut = Box::pin(timerfd::read(fd, system));
                    tokio::select! {
                        biased; // to ensure we poll system.read() before notifying the test task
                        _ = &mut fut => {
                            unreachable!("timerfd only fires in far future")
                        }
                        _ = tokio::time::sleep(Duration::from_secs(2)) => { }
                    }
                    fut
                }
            });
            println!("waiting for submission");
            let fut = rt.block_on(submission).unwrap();
            println!("submitted future");
            // now the io_uring op is submitted from this thread
            // kill this thread by exiting
            return fut;
        }
    })
    .join()
    .unwrap();

    // Wait for the completion to arrive.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    println!("poll fut to completion from another thread, expecting it to return ECANCELLED");
    assert!(
        // Ensure that there's plenty of time left before the timerfd fires
        std::time::Instant::now() + Duration::from_secs(5) < timerfd_fires_at,
        "self-check: test is timing dependent"
    );
    let res = rt.block_on(fut);
    let Err(crate::Error::Op(err)) = res else {
        panic!("expected ECANCELLED, got {res:?}");
    };
    assert_eq!(err.raw_os_error(), Some(nix::libc::ECANCELED));

    // explicit drop to prove that it's not happening because these here get dropped too early
    drop(system);
    drop(fd);

    /*

        christian@neon-hetzner-dev-christian:[~/ext/linux]: sudo bpftrace -e 'kfunc:io_setup_async_rw { printf("punting\n%s\n\n%s\n\n", kstack(),ustack(perf)); }'


        christian@neon-hetzner-dev-christian:[~/src/tokio-epoll-uring]: RUSTFLAGS="-C force-frame-pointers=yes" cargo nextest run repro_ecancelled1 --nocapture


        prints

        christian@neon-hetzner-dev-christian:[~/ext/linux]: sudo bpftrace -e 'kfunc:io_setup_async_rw { printf("punting\n%s\n\n%s\n\n", kstack(),ustack(perf)); }'
    Attaching 1 probe...
    punting

            bpf_prog_2a8ff7a400fd9d3f_kfunc_vmlinux_io_setup_async_rw_1+581
            bpf_prog_2a8ff7a400fd9d3f_kfunc_vmlinux_io_setup_async_rw_1+581
            bpf_trampoline_6442501231+76
            io_setup_async_rw+5
            __io_read+1228
            io_read+17
            io_issue_sqe+102
            io_submit_sqes+508
            __do_sys_io_uring_enter+961
            do_syscall_64+85
            entry_SYSCALL_64_after_hwframe+110



            7f3a094967d9 syscall+25 (/usr/lib/x86_64-linux-gnu/libc.so.6)
            55ff5245761a io_uring::submit::Submitter::enter::h8e7418076f4e3daf+154 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff524576f9 io_uring::submit::Submitter::submit_and_wait::h0cb10fdfcf74ff57+153 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff521b4119 io_uring::submit::Submitter::submit::h174293a4e995fe83+25 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff521e300b tokio_epoll_uring::system::submission::SubmitSideOpen::submit_raw::hdbf53b2ccdf4dae0+91 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff52324a4a tokio_epoll_uring::system::submission::op_fut::execute_op::_$u7b$$u7b$closure$u7d$$u7d$::do_submit::h460469a4f26fddf9+106 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff52202131 tokio_epoll_uring::system::submission::op_fut::execute_op::_$u7b$$u7b$closure$u7d$$u7d$::_$u7b$$u7b$closure$u7d$$u7d$::h790ec3a893ee4969+17 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff522c6908 tokio_epoll_uring::system::slots::SlotHandle::use_for_op::he96fb03d0d4dd92f+472 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff52201358 tokio_epoll_uring::system::submission::op_fut::execute_op::_$u7b$$u7b$closure$u7d$$u7d$::hf941b7df7b031edd+2056 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff521c3063 tokio_epoll_uring::system::test_util::timerfd::read::_$u7b$$u7b$closure$u7d$$u7d$::hae598cd02f16dd4c+611 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff52204e4d _$LT$core..pin..Pin$LT$P$GT$$u20$as$u20$core..future..future..Future$GT$::poll::hffd177561cdb0d6d+45 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff52204458 _$LT$$RF$mut$u20$F$u20$as$u20$core..future..future..Future$GT$::poll::h819b0895bf6d8d70+56 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff5230c171 tokio_epoll_uring::system::tests::repro_ecancelled1::_$u7b$$u7b$closure$u7d$$u7d$::_$u7b$$u7b$closure$u7d$$u7d$::h7239074173d05ef1+433 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff5232470d _$LT$tokio..future..poll_fn..PollFn$LT$F$GT$$u20$as$u20$core..future..future..Future$GT$::poll::hc2f7b658bf8b95c1+29 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff5230be7c tokio_epoll_uring::system::tests::repro_ecancelled1::_$u7b$$u7b$closure$u7d$$u7d$::h19d2bd1bc6aca8c0+860 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff52234901 tokio::runtime::task::core::Core$LT$T$C$S$GT$::poll::_$u7b$$u7b$closure$u7d$$u7d$::h3dcc966053b03442+129 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff5224c659 tokio::loom::std::unsafe_cell::UnsafeCell$LT$T$GT$::with_mut::hf6adee37fa99ff16+89 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)
            55ff52233832 tokio::runtime::task::core::Core$LT$T$C$S$GT$::poll::hcec0f2b32d3bbda4+34 (/home/christian/src/tokio-epoll-uring/target/debug/deps/tokio_epoll_uring-2bef0c28c90241cb)


            The kernel code that is punting to an async worker is

                if (force_nonblock) {
            /* If the file doesn't support async, just async punt */
            if (unlikely(!io_file_supports_nowait(req))) {
                ret = io_setup_async_rw(req, iovec, s, true);
                return ret ?: -EAGAIN;
            }

            the io_setup_async_rw is then dispatching to
                    .prep_async		= io_readv_prep_async,

            that function returns ... TODO

            we come back, return from thi; io_read is just one of many psosible .issue functions; this is where we end up in io_issue_sqe

                ret = def->issue(req, issue_flags);

            I think the commen case is that io_setup_async_rw returns 0 and so we return -EAGAIN

            christian@neon-hetzner-dev-christian:[~/ext/linux]: sudo bpftrace -e 'kfunc:io_setup_async_rw { printf("punting\n%s\n\n%s\n\n", kstack(),ustack(perf)); } kretfunc:io_read { printf("returning from io_read value %d\n", retval); }'

            Yep, it prints
            returning from io_read value -11

            ok, but -EAGAIN isn't handled specially by io_issue_sqe, (it's not IOU_OK) and so we return 0 fro io_issue_sqe

            go one step up to io_queue_sqe, and I think we call io_queue_async

            christian@neon-hetzner-dev-christian:[~/ext/linux]: sudo bpftrace -e 'kfunc:io_setup_async_rw { printf("punting\n%s\n\n%s\n\n", kstack(),ustack(perf)); } kretfunc:io_read { printf("returning from io_read value %d\n", retval); } kfunc:io_queue_async { printf("%s\n", probe); }'

            yep, prints
                returning from io_read value -11
                kfunc:vmlinux:io_queue_async

            we can ignore the linking stuff; what does io_arm_poll_handler return?

            christian@neon-hetzner-dev-christian:[~/ext/linux]: sudo bpftrace -e 'kfunc:io_setup_async_rw { printf("punting\n%s\n\n%s\n\n", kstack(),ustack(perf)); } kretfunc:io_read { printf("returning from io_read value %d\n", retval); } kfunc:io_queue_async { printf("%s\n", probe); } kretprobe:io_arm_poll_handler{ printf("%s returns %d\n", probe, retval); }'

            returning from io_read value -11
            kfunc:vmlinux:io_queue_async
            kretprobe:io_arm_poll_handler returns 0

            that is  IO_APOLL_OK, ok, when does it return that? on the happy path only great

            my gist of that function is that the file_operations->pol function will be used somehow

                .poll		= timerfd_poll,

            browing around a bit, timerfd_poll is called first sycnhoronusly as part of io_arm_poll_handler;

            anyway, this is timerfd, we don't use it in Pageserver; I assume the ECANCELLED we saw in pageserver was an ext4 write because it's a `ephemral_file_buffered_writer`

           reading the kernel code for those, these also get punted, esp if
           1. they need to allocate and to make the allocation we need to do IO to load allcoator state
           2. we're not O_DIRECT and need to evict some other page first

           If we're using O_DIRECT, then it's mostly about fallocate blocking or not. We're not doing that in pageserver (we should though)

           So, it can reasonably happen.

         */
}
