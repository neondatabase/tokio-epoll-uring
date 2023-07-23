pub mod ops;
pub use ops::read::read;

mod system;

pub(crate) mod shutdown_request;

pub use system::SubmitSideProvider;
pub use system::System;
pub use system::SystemHandle;

mod shared_system_handle;
pub use shared_system_handle::SharedSystemHandle;

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
