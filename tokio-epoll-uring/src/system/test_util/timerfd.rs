use bytes::Buf;
use nix::sys::time::TimeSpec;
use nix::sys::timerfd;
use nix::sys::timerfd::ClockId;
use uring_common::io_fd::IoFd;

use nix::sys::timer::TimerSetTimeFlags;

use nix::sys::timerfd::Expiration;

use nix::sys::timerfd::TimerFlags;

use std::time::Duration;

use crate::SystemHandle;

/// Abstraction for creating a timerfd.
///
/// See [`oneshot`] for creating it, and [`read`] for using `tokio-epoll-uring` to read from it.
pub struct TimerFd {
    pub(crate) timerfd: timerfd::TimerFd,
}

pub fn oneshot(duration: Duration) -> TimerFd {
    // setup a timerfd that will block readers forever
    let timerfd = timerfd::TimerFd::new(ClockId::CLOCK_MONOTONIC, TimerFlags::empty())
        .expect("timerfd creation");
    timerfd
        .set(
            Expiration::OneShot(TimeSpec::from_duration(duration)),
            TimerSetTimeFlags::empty(),
        )
        .unwrap();
    TimerFd { timerfd }
}

impl TimerFd {
    pub fn set(&self, duration: Duration) {
        self.timerfd
            .set(
                Expiration::OneShot(TimeSpec::from_duration(duration)),
                TimerSetTimeFlags::empty(),
            )
            .unwrap()
    }
}

impl IoFd for TimerFd {
    // Safety: we own the timerfd, so, as per the trait definition, we're allowed to return the fd.
    unsafe fn as_fd(&self) -> i32 {
        use std::os::fd::AsRawFd;
        self.timerfd.as_raw_fd()
    }
}

pub async fn read<T>(fd: impl IoFd + Send, system: T)
where
    T: AsRef<SystemHandle>,
{
    let value = vec![0u8; 8];
    let ((_, value), res) = system.as_ref().read(fd, 0, value).await;
    let n: usize = res.unwrap();
    assert_eq!(n, 8);
    let mut value = bytes::Bytes::from(value);
    assert_ne!(value.get_u64_ne(), 0);
    assert!(value.is_empty());
}
