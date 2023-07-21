pub mod ops;
pub use ops::read::read;

mod system;

pub use system::SubmitSideProvider;
pub use system::SystemHandle;

mod shared_system_handle;
pub use shared_system_handle::SharedSystemHandle;

pub fn launch_owned() -> SystemHandle {
    system::System::launch()
}

pub fn launch_shared() -> SharedSystemHandle {
    SharedSystemHandle::launch()
}

mod thread_local_system_handle;
pub use thread_local_system_handle::ThreadLocalSystemHandle;

pub fn thread_local_system() -> ThreadLocalSystemHandle {
    ThreadLocalSystemHandle
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use super::*;

    #[tokio::test]
    async fn panics_if_future_is_polled_after_shutdown() {
        let system = launch_shared();
        let (reader, _writer) = os_pipe::pipe().unwrap();
        let reader_owned =
            unsafe { OwnedFd::from_raw_fd(nix::unistd::dup(reader.as_raw_fd()).unwrap()) };
        drop(reader);
        let buf = vec![1u8; 4096];
        // spawn in task, will not complete because system shutdown
        let fut = read(system.clone(), reader_owned, 0, buf);
        let jh = tokio::spawn(async move {
            use futures::FutureExt;
            // https://stackoverflow.com/a/65764041
            let res = std::panic::AssertUnwindSafe(fut).catch_unwind().await;
            let whatever = res.unwrap_err();
            let msg: &String = whatever.downcast_ref().expect("expecting a &str panic");
            assert_eq!(
                msg,
                "SharedSystemHandle is shut down, cannot submit new operations"
            );
        });
        system.shutdown();
        jh.await.unwrap();
    }
}
