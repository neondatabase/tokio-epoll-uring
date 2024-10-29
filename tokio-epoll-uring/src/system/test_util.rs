use std::time::Duration;

pub(super) mod shared_system_handle;
pub(crate) mod timerfd;

pub(crate) const FOREVER: Duration = Duration::from_secs(365 * 24 * 60 * 60);
