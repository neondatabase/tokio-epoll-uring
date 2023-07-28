//! This crate enables high-performance use of Linux's `io_uring` from vanilla `tokio`.
//!
//! # Usage
//!
//! 1. Get a [`SystemLauncher`].
//! 2. Pick your io operation's `async fn`. Currently, we only support [`read`].
//! 3. Call the `async fn`, passing it the [`SystemLauncher`] you got in step 1
//!   and the **resources** that the operation operates on.
//!  Crucially, resources include the owned buffer.
//!  The buffer must implement [`tokio_uring::buf::IoBufMut`].
//!  Best to just use [`Vec<u8>`] for starters.
//!  (No, we're not using [`tokio_uring`] under the hood, we just like their trait and ready-made impls.)
//! 4. Await the future returned by the invocation.
//! 5. Get back the resources and a result.
//! 6. Inspect the result, and interpret the buffer accordingly.
//!
//! The "passing of resources" is required with io_uring because
//! the kernel owns the resources while the operation is in flight.
//! This is different from Rust's `std::io::Read` traits and also
//! from `tokio::io::AsyncRead`, which just take `&mut` refs to
//! the memory buffers they operate on.
//!
//! We can't do it like that with io_uring because the kernel owns
//! the resources, including crucially the memory buffer, while the
//! operation is in flight. Hence, the APIs of this crate use Rust's
//! ownership system to enforce this.
//! More background: withoutboats' [notes on io_uring](https://without.boats/blog/io-uring/).
//!
//! ## Example
//!
//! ```
//! #[tokio::main]
//! async fn main() {
//!#  use std::os::fd::OwnedFd;
//!   use tokio_uring::buf::IoBufMut;
//!   let file = std::fs::File::open("/dev/zero").unwrap();
//!   let fd: OwnedFd = file.into();
//!   let buf = vec![1; 1024];
//!   let (fd, buf, res) =
//!        tokio_epoll_uring::read(
//!             tokio_epoll_uring::ThreadLocalSystemLauncher,
//!             fd,
//!             0,
//!             buf
//!        ).await;
//!   assert!(res.is_ok());
//!   assert_eq!(buf, vec![0; 1024]);
//! }
//! ```
//!
//! Check out more examples in the `./examples` directory.
//!
//! # Motivation / Background
//!
//! The motivation for this crate was to allow for efficient kernel-buffered reads from async Rust.
//! "Efficiency" here means **low CPU and latency overhead**.
//! "Kernel-buffered" means that we don't use O_DIRECT or similar, just regular `read` syscalls that
//! go through the page cache.
//!
//! `tokio::fs` is not well-suited for this task because it is just a wrapper around `spawn_blocking`.
//! `spawn_blocking` is backed by a thread pool and hence comes with OS thread context switching overhead.
//! For example, a single `tokio::fs::File::read().await` will, at the very least, context switch from
//! executor thread to spawned thread, then back to executor thread once the `read()` call completes.
//! Context switches have different CPU overhead, depending on how NUMA-close the target thread is.
//! But even in the most optimal case, the two context switches will consume
//! a medium amount of single-digit microseconds of CPU time.
//!
//! That overhead is justified if the `read()` operation will block because it hits misses the page cache and has to read from the disk.
//! For example, a page-cache-missing 8k random `read()` an `i4i.2xlarge` EC2 instance takes `~110us`.
//! In this case, `tokio::fs` is a better choice than blocking the executor thread through sync IO.
//!
//! But, a page-cache-*hit* 8k random `read()` on the same instance takes `~1us`.
//! In this case, `tokio::fs` is clearly a bad choice than blocking the executor thread through sync IO.
//!
//! The problem is that we don't know ahead of time if it will be a hit or miss.
//! So, we must use the same code path for both cases.
//! And, we can't use `tokio::fs` for both cases because it's a bad choice for the "hit" case.
//! But we can't use synchronous IO either because it would block the executor thread for too long if it's a `miss`.
//!
//! Now, one could argue to just switch to [`tokio_uring`](https://docs.rs/tokio-uring/latest/tokio_uring/).
//! It's a new async Rust runtime that uses [`io_uring`](https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html)
//! instead of [`epoll`](https://manpages.debian.org/unstable/manpages/epoll.7.en.html) under the hood.
//! Pretty cool, if only ...
//! * ... it was as widely used as `tokio`, so one could be sure that it's production-ready and
//! * ... [Rust allowed us to be generic over async runtimes](https://github.com/rust-lang/wg-async/issues/45),
//!   so all our code that depends on vanilla `tokio` won't be broken at runtime or compile time.
//!
//! So, the next best idea is to use `tokio` and `tokio_uring` within the same process like so:
//! - Use `tokio` by default.
//! - From within our future running on the `tokio` executor thread:
//!   - create `tokio_uring` futures,
//!   - spawn a task on the `tokio_uring` runtime to drive the future to completion, and
//!   - use a oneshot channel to connect the task with our `tokio` future.
//!
//! The above works functionally but still has context-switching-induced performance problems.
//! Specifically, the thread(s) that processes the uring completions are part of the `tokio_uring` runtime
//! and hence not the same threads as the `tokio` runtime's executor threads where we're awaiting the
//! oneshot channel receiver.
//! So, delivery of the completion is again a context switch.
//! (It's also an outside-of-runtime `wake()` for `tokio`, which allegedly is more expensive than
//!  intra-runtime `wake()`s. Make sense, but I have yet to measure it in isolation).
//!
//! # Design
//!
//! Enter this crate.
//!
//! The core insights behind this crate are:
//! * We can use Linux's new-ish `io_uring` facility to submit kernel-buffered reads from async Rust.
//! * To wait for completions, `io_uring` supports `epoll`ing of the file descriptor that represents an `io_uring` instance
//!   (This is a lesser-known but supported feature of io_uring).
//! * Vanilla `tokio` supports  `epoll`ing arbitrary file descriptors in async Rust via [`tokio::io::unix::AsyncFd`].
//!
//! Combine the above, and the result is this library:
//!
//! 1. Once: Create an io_uring instance.
//! 2. Once: spawn a vanilla `tokio::task` that `epoll`s the io_uring instance for completions.
//!    We call this task the **poller task**.
//! 3. For each IO:
//!  * Submit the IO operation to the io_uring instance without waiting for completions (`io_uring_enter` without `IORING_ENTER_GETEVENTS`).
//!  * Implement a `Future` that gets woken up by **poller task** once the operation completes.
//!
//! The core advantages compared to the various sketches in the *Motivation* section are:
//! * We can issue kernel-buffered read system from async Rust without risking blocking the executor thread.
//! * We avoid OS thread context switching in favor of intra-runtime tokio-task-switching, which has lower latency and lower CPU overhead.
//! * We can keep using vanilla `tokio`, as our design sits completely on top of it.
//!
//! There is an extension to make this design more scalable on multi-core machines:
//! we can use one `io_uring` per executor thread. The vehicle for this _must_ be lazily-initialized
//! thread-locals, as `tokio` doesn't provide enough OS thread lifecycle hooks to do it another way.
//! While it's quite ugly, it avoids synchronizing on the submission path with significant performance benefits.
//! For completions, we continue to have one *poller task* per io_uring instance, spawned during
//! the lazy initialization of the thread-local.
//!
//! ## Critique
//!
//! We shouldn't put too much lipstick on the pig:
//!
//! The tokio runtime has no idea of the high priority that the *poller task* has in the overall system.
//! If the system is under heavy load, the turnaround time for the *poller task* in the tokio scheduler will grow.
//! This means completion processing gets delayed, thereby transitively delaying `wake()` of the futures that issued these operations.
//! It would be beneficial to prioritize the *poller task* if the io_uring fd becomes EPOLLIN.
//!
//! Further, in the one-io_uring-per-executor-thread extension to the design:
//!   1. There is no way to pin the *poller task* to the OS thread as the io_uring it corresponds to.
//!      This means the *poller task*s float freely among the executor OS threads.
//!      In a scenario where a read hits the page cache, the *poller task* will thus run on a different OS thread
//!      than where the read op was issued. So, tokio will have to do an (intra-runtime) cross-OS-thread `wake()`.
//!      Which is more expensive than if the *poller task* were on the same OS thread as the task that issued the read op.
//!      Sadly, there is [little hope](https://discord.com/channels/500028886025895936/500336333500448798/1131667951657955481)
//!      that OS-thread-affinity features will ever appear in vanilla tokio.
//!      It makes sense from an abstraction-cleanliness perspective, though.
//!   2. The `block_in_place` facility repurposes the current executor thread as a spawn_blocking-type thread.
//!      If we already created an io_uring instance in thread thread-local, that io_uring instance will now sit around useless
//!      until the thread exits or becomes a regular executor thread again. Tokio may also spawn new executor threads
//!      in the meantime, and these will have new io_uring instances in their thread-locals.
//!      So, we could end up with many more io_uring instances than executor threads in the system
//!      if `block_in_place` is used a lot. It's not a safety or liveness problem per se, and tokio
//!      _will_ eventually stop unused spawn_blocking-type threads, thereby cleaning up the unused io_urings.
//!      But it's an ugly wrinkle that wouldn't be necessary if we could fork `tokio` to integrate `epoll`ed `io_uring` more deeply.
//!
//! However, despite these flaws, the benchmarks below show that the design is a vast improvement over [`tokio::fs`].
//!
//! # Benchmarks
//!
//! ## Fully-Page-Cache-Hit 8k Random Reads
//!
//! On an `i4i.2xlarge` EC2 instance, we get 2.5 million 8k random read IOPS per second from the page cache.
//! These are 8-vCPU machines. Hence we have 8 executor threads.
//! The benchmark used 400 concurrent tokio tasks, each `read().await`ing 8k bytes at random offsets in per-task files.
//! The total working set is ca. 40 GiB, and we made sure it was completely inside the page cache during the benchmark
//! (no read IOs to the disk during the benchmark).
//!
//! We have some env-var tunables to maximize fairness.
//! Enabling them yields 1.8 million 8k random reads per second at nearly 100% fairness.
//! Fairness is measured by the spread of total achieved IOPS per task over the benchmark run.
//! A lower spread means better fairness.
//!
//! The bottleneck in any of the above configurations is CPU time.
//! We reach 100% CPU utilization with far less than 400 concurrent tokio tasks.
//! The 400 are for stress-testing fairness.
//! Flamegraphs show > 75% of CPU time spent in the kernel doing the actual work.
//!
//! Regarding latency jitter, the maximally fair configuration has mean latency of 204us and a p99.99 latency of 793us.
//! The most performant configuration had a mean of 2us, p99.99 of 19us; which implies very-long-tail outliers.
//!
//! More investigation is needed to understand what is going on there, as the 2.5mio vs 1.8mio advantage in throughput is attractive.
//!
//! For comparison, `tokio::fs` will do just about ca 370k IOPS; I didn't measure latency jitter.
//!
//! ## 2x Page Cache Size 8k Random Reads
//!
//! Another benchmark is to use twice the page-cache-sized working set (120 GiB on the `i4i.2xlarge`).
//! This will result in an about 50% page cache hit rate, i.e., 2x the IOPS than if we would always hit the disk.
//!
//! There is no meaningful difference between the fairness-tuned and the performance-tuned configurations.
//! Both yield ca 216k 8k random reads per second from 1200 concurrent tokio tasks.
//! The mean latency is 5.2ms, and p99.99 latency is 21ms.
//! The reason for these high latencies is that the disk is at its throughput limit; queuing is happening in software.
//! A real application would likely reject clients and try to scale out to keep latency at bay.
//!
//! Total CPU utilization was less than 20% busy in this configuration.
//!
//! For comparison, `tokio::fs` achieves about 180k IOPS.
//! CPU utilization is higher but acceptable.
//! We did not measure latency jitter.
//!

/// The operations that this crate supports. Use these as an entrypoint to learn the API.
pub mod ops;
use std::os::fd::OwnedFd;

use ops::read::ReadOp;
use ops::OpFut;

mod system;
pub use system::lifecycle::handle::SystemHandle;
pub use system::lifecycle::System;

pub(crate) mod util;

mod shared_system_handle;
pub use shared_system_handle::SharedSystemHandle;

mod thread_local_system_handle;
use system::submission::SubmitSide;
pub use thread_local_system_handle::ThreadLocalSubmitSideProvider;
pub use thread_local_system_handle::ThreadLocalSystemLauncher;
use tokio_uring::buf::IoBufMut;

impl SubmitSideProvider for SystemHandle {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(&self, f: F) -> R {
        f(self.state.guaranteed_live().submit_side.clone())
    }
}

pub trait SubmitSideProvider: Unpin + Sized {
    fn with_submit_side<F: FnOnce(SubmitSide) -> R, R>(&self, f: F) -> R;
    fn read<B: IoBufMut + Send>(self, file: OwnedFd, offset: u64, buf: B) -> OpFut<ReadOp<B>> {
        let op = ReadOp { file, offset, buf };
        self.with_submit_side(|submit_side| OpFut::new(op, submit_side))
    }
}


pub trait ResourcesOwnedByKernel {
    type Resources;
    type Success;
    type Error;
    fn on_failed_submission(self) -> Self::Resources;
    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>);
}
