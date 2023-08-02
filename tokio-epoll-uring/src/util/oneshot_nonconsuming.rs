//! Like [`tokio::sync::oneshot`], but the [`Receiver`] is clone-able for convenience.
//! The sent value need not be clonable; only one receiver will get it.

use std::sync::{Arc, Mutex};

use tokio::sync::broadcast::{
    self,
    error::{RecvError, SendError},
};

enum State<T: Send> {
    NotSent,
    SentNotTaken(T),
    Taken,
}
struct Shared<T: Send>(Arc<Mutex<State<T>>>);

impl<T: Send> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Shared(Arc::clone(&self.0))
    }
}

/// Create a new sender-receiver pair.
pub fn channel<T: Send>() -> (SendOnce<T>, Receiver<T>) {
    let shared = Shared(Arc::new(Mutex::new(State::NotSent)));
    let (sender, receiver) = broadcast::channel(1);
    (
        SendOnce(shared.clone(), sender),
        Receiver(shared.clone(), receiver),
    )
}

/// The sender half of the channel.
///
/// More [`SendOnce::send`].
pub struct SendOnce<T: Send>(Shared<T>, broadcast::Sender<()>);

/// The receiver half of a oneshot channel.
///
/// Clone-able for convenience. See [`Receiver::recv`].
pub struct Receiver<T: Send>(Shared<T>, broadcast::Receiver<()>);

impl<T: Send> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver(self.0.clone(), self.1.resubscribe())
    }
}

impl<T: Send> SendOnce<T> {
    /// Send the given value to the receiver(s).
    pub fn send(self, v: T) -> Result<(), T> {
        let mut guard = self.0 .0.lock().unwrap();
        let prev = std::mem::replace(&mut *guard, State::SentNotTaken(v));
        match prev {
            State::NotSent => (),
            State::SentNotTaken(_) => unreachable!(),
            State::Taken => unreachable!(),
        }
        drop(guard);
        match self.1.send(()) {
            Ok(_) => Ok(()),
            Err(SendError(())) => {
                let mut guard = self.0 .0.lock().unwrap();
                let prev = std::mem::replace(&mut *guard, State::NotSent);
                match prev {
                    State::NotSent => unreachable!(),
                    State::SentNotTaken(v) => Err(v),
                    State::Taken => unreachable!(),
                }
            }
        }
    }
}

/// The result of [`Receiver::recv`].
pub enum RecvResult<T> {
    /// If there is and explicit [`SendOnce::send`] call, the first receiver wins the sent value.
    FirstRecv(T),
    /// If there is and explicit [`SendOnce::send`] call, not-first receivers get this result.
    NotFirstRecv,
    /// If the [`SendOnce`] is dropped, all receivers get this result.
    SenderDropped,
}

impl<T: Send> Receiver<T> {
    /// See [`RecvResult`].
    pub async fn recv(&mut self) -> RecvResult<T> {
        match self.1.recv().await {
            Ok(()) => {
                let mut guard = self.0 .0.lock().unwrap();
                let cur = std::mem::replace(&mut *guard, State::Taken);
                match cur {
                    State::NotSent => {
                        unreachable!("we only send after we've transitited out of this state")
                    }
                    State::SentNotTaken(v) => RecvResult::FirstRecv(v),
                    State::Taken => RecvResult::NotFirstRecv,
                }
            }
            Err(RecvError::Closed) => {
                let mut guard = self.0 .0.lock().unwrap();
                let cur = std::mem::replace(&mut *guard, State::Taken);
                match cur {
                    State::NotSent => RecvResult::SenderDropped,
                    State::SentNotTaken(v) => RecvResult::FirstRecv(v),
                    State::Taken => RecvResult::NotFirstRecv,
                }
            }
            Err(RecvError::Lagged(_)) => {
                unreachable!("we only send once, and the channel has capacity of 1")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[tokio::test]
    async fn sender_dropped_before_sending() {
        let (sender, mut receiver) = super::channel::<()>();
        drop(sender);
        assert!(matches!(
            receiver.recv().await,
            super::RecvResult::SenderDropped
        ));
    }
    #[tokio::test]
    async fn first_recv() {
        let (sender, mut receiver) = super::channel::<()>();
        sender.send(()).unwrap();
        assert!(matches!(
            receiver.recv().await,
            super::RecvResult::FirstRecv(())
        ));
    }
    #[tokio::test]
    async fn concurrent_recv() {
        let (sender, mut receiver1) = super::channel::<()>();
        let mut receiver2 = receiver1.clone();
        let r1 = tokio::task::spawn(async move { receiver1.recv().await });
        let r2 = tokio::task::spawn(async move { receiver2.recv().await });
        tokio::time::sleep(Duration::from_secs(1)).await; // TODO: don't rely on timing
        sender.send(()).unwrap();
        let results = [r1.await.unwrap(), r2.await.unwrap()];
        assert_eq!(
            1,
            results
                .iter()
                .filter(|r| matches!(r, super::RecvResult::FirstRecv(())))
                .count()
        );
        assert_eq!(
            1,
            results
                .iter()
                .filter(|r| matches!(r, super::RecvResult::NotFirstRecv))
                .count()
        );
    }
    #[tokio::test]
    async fn receive_on_receiver_cloned_after_send() {
        let (sender, mut orig_receiver) = super::channel::<()>();
        sender.send(()).unwrap();
        let mut cloned_receiver = orig_receiver.clone();
        let res = cloned_receiver.recv().await;
        assert!(matches!(res, super::RecvResult::FirstRecv(())));
        assert!(matches!(
            orig_receiver.recv().await,
            super::RecvResult::NotFirstRecv
        ));
    }
}
