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
