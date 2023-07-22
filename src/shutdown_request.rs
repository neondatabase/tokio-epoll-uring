use std::sync::{Arc, Mutex};

enum State<T> {
    Undefined,
    Waiting,
    Posted(T),
    Taken,
    SenderDropped,
}
struct Shared<T> {
    shutdown_request: Mutex<State<T>>,
    posted_shutdown_request: tokio::sync::Notify,
}

pub fn new<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        shutdown_request: Mutex::new(State::Waiting),
        posted_shutdown_request: tokio::sync::Notify::new(),
    });
    (Sender(shared.clone()), Receiver(shared))
}

/// Explicitly not Clone so that `shutdown()` consuming `self` means what it says.
pub struct Sender<T>(Arc<Shared<T>>);

pub struct Receiver<T>(Arc<Shared<T>>);

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver(self.0.clone())
    }
}

impl<T> Sender<T> {
    pub fn shutdown(self, req: T) {
        let mut guard = self.0.shutdown_request.lock().unwrap();
        let cur = std::mem::replace(&mut *guard, State::Undefined);
        match cur {
            State::Undefined => panic!("implementation error"),
            State::Waiting => {
                *guard = State::Posted(req);
            }
            State::Posted(_) => unreachable!("we only set it in this function"),
            State::Taken => unreachable!("we only set Taken after we set Posted"),
            State::SenderDropped => unreachable!("we're executing on self"),
        }
        self.0.posted_shutdown_request.notify_waiters();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut guard = self.0.shutdown_request.lock().unwrap();
        let cur = std::mem::replace(&mut *guard, State::Undefined);
        match cur {
            State::Undefined => panic!("implementation error"),
            State::Waiting => {
                *guard = State::SenderDropped;
            }
            State::SenderDropped => unreachable!("we only set it in this function"),
            x @ State::Posted(_) | x @ State::Taken => {
                *guard = x;
            }
        }
        self.0.posted_shutdown_request.notify_waiters();
    }
}

pub enum WaitForShutdownResult<T> {
    ExplicitRequest(T),
    ExplicitRequestObservedEarlier,
    SenderDropped,
}

impl<T> Receiver<T> {
    pub async fn wait_for_shutdown_request(&self) -> WaitForShutdownResult<T> {
        self.0.posted_shutdown_request.notified().await;
        let mut guard = self.0.shutdown_request.lock().unwrap();
        let cur = std::mem::replace(&mut *guard, State::Undefined);
        match cur {
            State::Undefined => panic!("implementation error"),
            State::Waiting => unreachable!("we only notify after we set Posted"),
            State::Posted(req) => {
                *guard = State::Taken;
                return WaitForShutdownResult::ExplicitRequest(req);
            }
            State::Taken => {
                *guard = State::Taken;
                return WaitForShutdownResult::ExplicitRequestObservedEarlier;
            }
            State::SenderDropped => {
                *guard = State::SenderDropped;
                return WaitForShutdownResult::SenderDropped;
            }
        }
    }
}
