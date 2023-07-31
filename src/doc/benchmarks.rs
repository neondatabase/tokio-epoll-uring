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
