pub mod buf;
pub mod open_options;
#[cfg(target_os = "linux")]
pub mod open_options_io_uring_ext;

pub mod io_fd;

#[cfg(target_os = "linux")]
pub use io_uring;
#[cfg(target_os = "linux")]
pub use libc;
#[cfg(target_os = "linux")]
pub use linux_raw_sys;
