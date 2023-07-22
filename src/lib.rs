pub mod ops;
pub use ops::read::read;

mod system;

pub(crate) mod shutdown_request;

pub use system::SubmitSideProvider;
use system::SystemHandle;

mod shared_system_handle;
pub use shared_system_handle::SharedSystemHandle;

pub async fn launch_owned() -> SystemHandle {
    system::System::launch().await
}

pub async fn launch_shared() -> SharedSystemHandle {
    SharedSystemHandle::launch().await
}

mod thread_local_system_handle;
pub use thread_local_system_handle::ThreadLocalSystemHandle;

pub fn thread_local_system() -> ThreadLocalSystemHandle {
    todo!();
    ThreadLocalSystemHandle
}

#[cfg(test)]
mod tests {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use super::*;

}
