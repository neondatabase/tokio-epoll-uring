`tokio-epoll-uring` enables high-performance use of [Linux's `io_uring`](https://kernel.dk/io_uring.pdf) from vanilla [`tokio`](https://tokio.rs/).

> [!WARNING]
> This project is currently not intended for use outside of Neon.
> The plan is to prove the core architectural idea by deploying it to production in [Neon's Pageserver](https://github.com/neondatabase/neon/tree/main/pageserver).
> We will share the results with the tokio community to push forward `io_uring`-enabled `tokio`.

# Documentation

Use `cargo doc --no-deps --open`.

The Rust docs include sections on the motivation behind this project, the design of the crate, and benchmarks.
If you prefer to read them in Markdown in your browser, check out the files in [`tokio-epoll-uring/src/doc`](tokio-epoll-uring/src/doc).

# Examples

Check out [`./tokio-epoll-uring/examples`](./tokio-epoll-uring/examples).

# Bug Reports, Roadmap, Contributing

As noted at the top of this readme file, this project is not yet intended for use outside of Neon.

* Genuine bug reports are welcome.
* We will not be able to accept feature requests at this time.
* We are unlikely to accept big refactoring PRs.
* We are likely to accept PRs that add support for new io_uring opcodes.
  Duplicating some code per opcdoe added is preferred over doing a big DRY refactoring.
* Feedback on the architecture is best provided by opening an issue.

# Benchmarking

See [`./BENCHMARKING.md`](BENCHMARKING.md).
