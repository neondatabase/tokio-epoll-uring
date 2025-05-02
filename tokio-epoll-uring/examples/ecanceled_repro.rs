use std::{
    os::{
        fd::OwnedFd,
        unix::{fs::OpenOptionsExt, thread::JoinHandleExt},
    },
    sync::Arc,
    time::Duration,
};

use nix::libc::{O_DIRECT, SIGUSR1};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let path = tempfile::NamedTempFile::new_in(std::env::current_dir().unwrap())
        .unwrap()
        .into_temp_path();

    loop {
        let path = path.to_path_buf();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let barrier2 = barrier.clone();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async move {
                tokio::spawn(async move {
                    let mut sigusr1 = tokio::signal::unix::signal(
                        tokio::signal::unix::SignalKind::user_defined1(),
                    )
                    .expect("failed to install signal handler");
                    let mut sigterm =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                            .expect("failed to install signal handler");

                    barrier2.wait();

                    loop {
                        tokio::select! {
                            _ = sigterm.recv() => {
                            }
                            _ = sigusr1.recv() => {
                            }
                        }
                    }
                })
            });

            rt.block_on(async {
                let cancel = CancellationToken::new();
                let system = tokio_epoll_uring::System::launch().await.unwrap();
                let cancel = cancel.child_token();
                let fd: Arc<OwnedFd> = Arc::new(
                    std::fs::OpenOptions::new()
                        .write(true)
                        .custom_flags(O_DIRECT)
                        .open(&path)
                        .unwrap()
                        .into(),
                );
                let fd = Arc::clone(&fd);
                let buf = unsafe {
                    std::alloc::alloc(std::alloc::Layout::from_size_align(4096, 4096).unwrap())
                };
                if buf.is_null() {
                    panic!("failed to allocate buffer");
                }
                let mut buf = unsafe { Vec::from_raw_parts(buf, 4096, 4096) };
                buf.fill(1);

                let mut fd = fd;
                let mut buf = buf;
                for i in 0..3 {
                    println!("{}: {:?}", i, std::thread::current().id());
                    let res;
                    ((fd, buf), res) = system.write(fd, 512 * i, buf).await;
                    let _n = res.unwrap();
                }
                system.initiate_shutdown().await;
            });
        });
        barrier.wait();
        for i in 0..10000 {
            unsafe {
                nix::libc::pthread_kill(thread.as_pthread_t(), SIGUSR1);
            }
        }
    }
}
