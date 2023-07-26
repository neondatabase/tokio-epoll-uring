//! Like [`tokio::sync::oneshot`], but the [`Receiver`] is clone-able for convenience.
//! The sent value need not be clonable; only one receiver will get it.

use std::sync::{Arc, Mutex};

struct State<T> {
    nreceivers: usize,
    inner: StateInner<T>,
}
enum StateInner<T> {
    Undefined,
    Waiting,
    Posted(T),
    Taken,
    SenderDropped,
}
struct Shared<T> {
    state: Mutex<State<T>>,
    waiters: tokio::sync::Notify,
}

/// Create a new sender-receiver pair.
pub fn channel<T>() -> (SendOnce<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            nreceivers: 1,
            inner: StateInner::Waiting,
        }),
        waiters: tokio::sync::Notify::new(),
    });
    (SendOnce(shared.clone()), Receiver(shared))
}

/// The sender half of the channel.
///
/// More [`SendOnce::send`].
pub struct SendOnce<T>(Arc<Shared<T>>);

/// The receiver half of a oneshot channel.
///
/// Clone-able for convenience. See [`Receiver::recv`].
pub struct Receiver<T>(Arc<Shared<T>>);

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.0.state.lock().unwrap().nreceivers += 1;
        Receiver(self.0.clone())
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().nreceivers -= 1;
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        assert_eq!(
            self.state
                .lock()
                .map(|g| g.nreceivers)
                .unwrap_or_else(|le| le.get_ref().nreceivers),
            0
        );
    }
}

impl<T> SendOnce<T> {
    /// Send the given value to the receiver(s).
    pub fn send(self, v: T) -> Result<(), T> {
        let mut guard = self.0.state.lock().unwrap();
        if guard.nreceivers == 0 {
            return Err(v);
        }
        let cur = std::mem::replace(&mut guard.inner, StateInner::Undefined);
        match cur {
            StateInner::Undefined => panic!("implementation error"),
            StateInner::Waiting => {
                guard.inner = StateInner::Posted(v);
            }
            StateInner::Posted(_) => unreachable!("we only set it in this function"),
            StateInner::Taken => unreachable!("we only set Taken after we set Posted"),
            StateInner::SenderDropped => unreachable!("we're executing on self"),
        }
        self.0.waiters.notify_waiters();
        Ok(())
    }
}

impl<T> Drop for SendOnce<T> {
    fn drop(&mut self) {
        let mut guard = self.0.state.lock().unwrap();
        let cur = std::mem::replace(&mut guard.inner, StateInner::Undefined);
        match cur {
            StateInner::Undefined => panic!("implementation error"),
            StateInner::Waiting => {
                guard.inner = StateInner::SenderDropped;
            }
            StateInner::SenderDropped => unreachable!("we only set it in this function"),
            x @ StateInner::Posted(_) | x @ StateInner::Taken => {
                guard.inner = x;
            }
        }
        drop(guard);
        self.0.waiters.notify_waiters();
    }
}

/// The result of [`Receiver::recv`].
pub enum RecvResult<T> {
    /// If there is and explicit [`Sender::send`] call, the first receiver wins the sent value.
    FirstRecv(T),
    /// If there is and explicit [`Sender::send`] call, not-first receivers get this result.
    NotFirstRecv,
    /// If the [`Sender`] is dropped, all receivers get this result.
    SenderDropped,
}

impl<T> Receiver<T> {
    /// See [`RecvResult`].
    pub async fn recv(&self) -> RecvResult<T> {
        let mut iter = 0;
        loop {
            let wait_notify = loop {
                let mut guard = self.0.state.lock().unwrap();
                let cur = std::mem::replace(&mut guard.inner, StateInner::Undefined);
                assert!(iter < 2, "implementation error");
                match cur {
                    StateInner::Undefined => panic!("implementation error"),
                    StateInner::Waiting => {
                        // FIXME: allocation on the hot path
                        let mut notified = Box::pin(self.0.waiters.notified());
                        notified.as_mut().enable();
                        guard.inner = StateInner::Waiting;
                        drop(guard);
                        break notified;
                    }
                    StateInner::Posted(v) => {
                        guard.inner = StateInner::Taken;
                        return RecvResult::FirstRecv(v);
                    }
                    StateInner::Taken => {
                        guard.inner = StateInner::Taken;
                        return RecvResult::NotFirstRecv;
                    }
                    StateInner::SenderDropped => {
                        guard.inner = StateInner::SenderDropped;
                        return RecvResult::SenderDropped;
                    }
                }
            };
            wait_notify.await;
            iter += 1;
            continue;
        }
    }
}
