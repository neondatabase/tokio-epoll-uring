use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();
    tracing::info!("starting");

    tokio::spawn(async move {
        // print tokio runtime metrics every second
        loop {
            let metrics = tokio::runtime::Handle::current().metrics();
            tracing::info!(
                "num workers: {:?} idle_blocking: {:?} blocking: {:?}",
                metrics.num_workers(),
                metrics.num_idle_blocking_threads(),
                metrics.num_blocking_threads()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    for _ in 0..8 {
        tokio::spawn(async move {
            let mut file = tempfile::tempfile().unwrap();
            {
                use std::io::Write;
                file.write_all(&[23u8].repeat(1024)).unwrap();
                file.write_all(&[42u8].repeat(1024)).unwrap();
                file.write_all(&[67u8].repeat(1024)).unwrap();
            }

            let buf = vec![0; 2048];
            let (_file, buf, res) = tokio_epoll_uring::read(
                tokio_epoll_uring::ThreadLocalSystemHandle,
                file.into(),
                512,
                buf,
            )
            .await;
            let read = res.unwrap();
            assert_eq!(read, 2048, "not expecting short read");
            assert_eq!(&buf[0..512], &[23u8; 512]);
            assert_eq!(&buf[512..512 + 1024], &[42u8; 1024]);
            assert_eq!(&buf[512 + 1024..512 + 1024 + 512], &[67u8; 512]);
            loop {
                tokio::task::yield_now().await;
            }
        });
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    let stop = Arc::new(AtomicBool::new(false));
    for _ in 0..8 {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            tokio::task::block_in_place(|| {
                tracing::info!("block_in_place on id {:?}", std::thread::current().id());
                loop {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            });
        });
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    for _ in 0..8 {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut file = tempfile::tempfile().unwrap();
            {
                use std::io::Write;
                file.write_all(&[23u8].repeat(1024)).unwrap();
                file.write_all(&[42u8].repeat(1024)).unwrap();
                file.write_all(&[67u8].repeat(1024)).unwrap();
            }

            let buf = vec![0; 2048];
            let (_file, buf, res) = tokio_epoll_uring::read(
                tokio_epoll_uring::ThreadLocalSystem,
                file.into(),
                512,
                buf,
            )
            .await;
            let read = res.unwrap();
            assert_eq!(read, 2048, "not expecting short read");
            assert_eq!(&buf[0..512], &[23u8; 512]);
            assert_eq!(&buf[512..512 + 1024], &[42u8; 1024]);
            assert_eq!(&buf[512 + 1024..512 + 1024 + 512], &[67u8; 512]);
            loop {
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        });
    }

    tokio::time::sleep(Duration::from_secs(5)).await;

    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    tokio::time::sleep(Duration::from_secs(30)).await;
}
