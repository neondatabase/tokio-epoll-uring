# Motivation

The motivation for this crate was to allow for efficient kernel-buffered reads from async Rust.
"Efficiency" here means **low CPU and latency overhead**.
"Kernel-buffered" means that we don't use O_DIRECT or similar, just regular `read` syscalls that
go through the page cache.

`tokio::fs` is not well-suited for this task because it is just a wrapper around `spawn_blocking`.
`spawn_blocking` is backed by a thread pool and hence comes with OS thread context switching overhead.
For example, a single `tokio::fs::File::read().await` will, at the very least, context switch from
executor thread to spawned thread, then back to executor thread once the `read()` call completes.
Context switches have different CPU overhead, depending on how NUMA-close the target thread is.
But even in the most optimal case, the two context switches will consume
a medium amount of single-digit microseconds of CPU time.

That overhead is justified if the `read()` operation will block because it hits misses the page cache and has to read from the disk.
For example, a page-cache-missing 8k random `read()` an `i4i.2xlarge` EC2 instance takes `~110us`.
In this case, `tokio::fs` is a better choice than blocking the executor thread through sync IO.

But, a page-cache-*hit* 8k random `read()` on the same instance takes `~1us`.
In this case, `tokio::fs` is clearly a bad choice than blocking the executor thread through sync IO.

The problem is that we don't know ahead of time if it will be a hit or miss.
So, we must use the same code path for both cases.
And, we can't use `tokio::fs` for both cases because it's a bad choice for the "hit" case.
But we can't use synchronous IO either because it would block the executor thread for too long if it's a `miss`.

Now, one could argue to just switch to [`tokio_uring`](https://docs.rs/tokio-uring/latest/tokio_uring/).
It's a new async Rust runtime that uses [`io_uring`](https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html)
instead of [`epoll`](https://manpages.debian.org/unstable/manpages/epoll.7.en.html) under the hood.
Pretty cool, if only ...
* ... it was as widely used as `tokio`, so one could be sure that it's production-ready and
* ... [Rust allowed us to be generic over async runtimes](https://github.com/rust-lang/wg-async/issues/45),
  so all our code that depends on vanilla `tokio` won't be broken at runtime or compile time.

So, the next best idea is to use `tokio` and `tokio_uring` within the same process like so:
- Use `tokio` by default.
- From within our future running on the `tokio` executor thread:
  - create `tokio_uring` futures,
  - spawn a task on the `tokio_uring` runtime to drive the future to completion, and
  - use a oneshot channel to connect the task with our `tokio` future.

The above works functionally but still has context-switching-induced performance problems.
Specifically, the thread(s) that processes the uring completions are part of the `tokio_uring` runtime
and hence not the same threads as the `tokio` runtime's executor threads where we're awaiting the
oneshot channel receiver.
So, delivery of the completion is again a context switch.
(It's also an outside-of-runtime `wake()` for `tokio`, which allegedly is more expensive than
 intra-runtime `wake()`s. Make sense, but I have yet to measure it in isolation).
