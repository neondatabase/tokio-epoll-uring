use std::os::fd::OwnedFd;

use tokio_epoll_uring::Ops;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_file(true)
        .with_line_number(true)
        .init();
    tracing::info!("starting");

    let mut file = tempfile::tempfile().unwrap();
    {
        use std::io::Write;
        file.write_all(&[23u8].repeat(1024)).unwrap();
        file.write_all(&[42u8].repeat(1024)).unwrap();
        file.write_all(&[67u8].repeat(1024)).unwrap();
    }

    let file: OwnedFd = file.into();

    let system = tokio_epoll_uring::System::launch().await;

    let buf = vec![0; 1024];

    let ((file, buf), res) = system.read(file, 0, buf).await;
    let read = res.unwrap();
    assert_eq!(read, 1024, "not expecting short read");
    assert_eq!(&buf, &[23u8; 1024]);

    let ((file, buf), res) = system.read(file, 1024, buf).await;
    let read = res.unwrap();
    assert_eq!(read, 1024, "not expecting short read");
    assert_eq!(buf, &[42u8; 1024]);

    let ((_file, buf), res) = system.read(file, 2048, buf).await;
    let read = res.unwrap();
    assert_eq!(read, 1024, "not expecting short read");
    assert_eq!(buf, &[67u8; 1024]);
}
