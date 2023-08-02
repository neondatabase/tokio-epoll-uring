use tokio_epoll_uring::Ops;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();
    tracing::info!("starting");

    let mut jhs = Vec::new();
    for _ in 0..8 {
        jhs.push(tokio::spawn(async move {
            let mut file = tempfile::tempfile().unwrap();
            {
                use std::io::Write;
                file.write_all(&[23u8].repeat(1024)).unwrap();
                file.write_all(&[42u8].repeat(1024)).unwrap();
                file.write_all(&[67u8].repeat(1024)).unwrap();
            }

            let buf = vec![0; 2048];
            let ((_file, buf), res) = tokio_epoll_uring::with_thread_local_system(|system| {
                system.read(file.into(), 512, buf)
            })
            .await;
            let read = res.unwrap();
            assert_eq!(read, 2048, "not expecting short read");
            assert_eq!(&buf[0..512], &[23u8; 512]);
            assert_eq!(&buf[512..][..1024], &[42u8; 1024]);
            assert_eq!(&buf[512 + 1024..][..512], &[67u8; 512]);
        });
    }

    for jh in jhs {
        jh.await.unwrap();
    }
}
