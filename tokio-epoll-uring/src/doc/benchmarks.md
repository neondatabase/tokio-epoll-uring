# Benchmarks

We use the `benchmark` crate that's maintained in the same repository as this crate.
It's a micro-benchmark that evaluates different IO engines' performance in 8k random reads issued by a configurable number of tasks.
The evaluation is done on an `i4i.2xlarge` EC2 instance.

Highlights:

 * With 400 tasks, each operating on a 100MiB file, the workload fits completely into the page cache.
   We achieve 1.8 million 8k random read IOPS with a high degree of fairness.
   When willing to sacrifice fairness, a different configuration achieves 2.45 million 8k random read IOPS.
   Even the "1.8mio" are 5x of what `tokio::fs`/`tokio::spawn_blocking` achieve.
   It is 4x what a single `tokio_uring` runtime can achieve, because our design takes advantage of multiple cores
   through multiple tokio worker threads, whereas `tokio_uring` is limited to a executor thread / CPU core.

* With 1200 tasks (100MiB file per task), the workload is about twice the page cache size.
  Hence we're maxing out the IOPS supported by the `i4i.2xlarge`'s Instance Store NVMe (~130k IOPS).
  Our crate has performance equivalent to `tokio::fs`/`tokio::spawn_blocking` and outperforms `tokio_uring` by about 2x.
  Again, `tokio_uring` is bottlenecked by its limitation to a single executor thread / CPU core.

Fairness is measured as follows: each tokio task in the benchmark is given a fixed amount of 8k random reads to perform.
We measure per task the time from benchmark start (same for all tasks) to the time the task finished.
Assuming a fair kernel page cache, a fair system will result in all tasks finishing at about the same time.
An unfair system will result in some tasks finishing earlier than others.
By sorting & plotting task runtimes on a scatter plot (x axis: index of sorted result, y axis: task runtime)
we get a visualization of task fairness: a flat line is maximum fairness, a steep line means some tasks were heavily favored over others.
We can also compare `min` and `max` task runtime for a given configuration and calculate the spread factor `max/min`.
A lower spread factor means higher fairness.

Detailed results: <https://docs.google.com/spreadsheets/d/1bs_q7IyoTzF43SeEIBWCJk7z2mdZZVsLbqvVwE88YWA/edit?usp=sharing>
See `tokio-epoll-uring.git:{README.md,benchmark/scripts/runbench.py}` for details on how the benchmark is run.


