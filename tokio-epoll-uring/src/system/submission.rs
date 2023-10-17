pub(crate) mod op_fut;

use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, Weak},
};

use io_uring::{SubmissionQueue, Submitter};

use super::{
    completion::CompletionSide,
    lifecycle::ShutdownRequest,
    slots::{self, Slots},
};

pub(crate) struct SubmitSideNewArgs {
    pub(crate) id: usize,
    pub(crate) submitter: Submitter<'static>,
    pub(crate) sq: SubmissionQueue<'static>,
    pub(crate) slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
    pub(crate) completion_side: Arc<Mutex<CompletionSide>>,
    pub(crate) shutdown_tx: crate::util::oneshot_nonconsuming::SendOnce<ShutdownRequest>,
}

pub(crate) struct SubmitSide {
    // This is the only long-lived strong reference to the `SubmitSideInner`.
    inner: Arc<tokio::sync::Mutex<SubmitSideInner>>,
    shutdown_tx: Option<crate::util::oneshot_nonconsuming::SendOnce<ShutdownRequest>>,
    shutdown_done_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl SubmitSide {
    pub(crate) fn new(args: SubmitSideNewArgs) -> SubmitSide {
        let SubmitSideNewArgs {
            id,
            submitter,
            sq,
            slots: ops,
            completion_side,
            shutdown_tx,
        } = args;
        SubmitSide {
            inner: Arc::new(tokio::sync::Mutex::new(SubmitSideInner::Open(
                SubmitSideOpen {
                    id,
                    submitter,
                    sq,
                    slots: ops,
                    completion_side: Arc::clone(&completion_side),
                },
            ))),
            shutdown_tx: Some(shutdown_tx),
            shutdown_done_tx: None,
        }
    }
}

impl SubmitSide {
    pub fn shutdown(mut self) -> impl std::future::Future<Output = ()> + Send {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let prev = self.shutdown_done_tx.replace(done_tx);
        assert!(prev.is_none());
        drop(self); // sends the shutdown request
        async {
            done_rx
                .await
                // TODO: return the error?
                .unwrap()
        }
    }
}

impl Drop for SubmitSide {
    fn drop(&mut self) {
        self.shutdown_tx
            .take()
            .unwrap()
            .send(ShutdownRequest {
                done_tx: self.shutdown_done_tx.take(),
                submit_side_inner: Arc::clone(&self.inner),
            })
            // TODO: can we just ignore the error?
            .ok()
            .unwrap();
    }
}

impl SubmitSideOpen {
    pub(crate) fn submit_raw(
        &mut self,
        sqe: io_uring::squeue::Entry,
    ) -> std::result::Result<(), SubmitError> {
        self.sq.sync();
        match unsafe { self.sq.push(&sqe) } {
            Ok(()) => {}
            Err(_queue_full) => {
                return Err(SubmitError::QueueFull);
            }
        }
        self.sq.sync();
        self.submitter.submit().unwrap();
        Ok(())
    }
}

#[derive(Clone)]
pub struct SubmitSideWeak(Weak<tokio::sync::Mutex<SubmitSideInner>>);

impl SubmitSideWeak {
    pub(crate) async fn upgrade_to_open(&self) -> Option<SubmitSideOpenGuard> {
        let inner: Arc<tokio::sync::Mutex<SubmitSideInner>> = match self.0.upgrade() {
            Some(inner) => inner,
            None => return None,
        };
        let mut inner_guard = inner.lock_owned().await;
        match &mut *inner_guard {
            SubmitSideInner::Open(_) => Some(SubmitSideOpenGuard(inner_guard)),
            SubmitSideInner::ShutDownInitiated => None,
        }
    }
}

pub(crate) struct SubmitSideOpenGuard(tokio::sync::OwnedMutexGuard<SubmitSideInner>);

impl Deref for SubmitSideOpenGuard {
    type Target = SubmitSideOpen;
    fn deref(&self) -> &Self::Target {
        match &*self.0 {
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::ShutDownInitiated => unreachable!(),
        }
    }
}

impl DerefMut for SubmitSideOpenGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut *self.0 {
            SubmitSideInner::Open(open) => open,
            SubmitSideInner::ShutDownInitiated => unreachable!(),
        }
    }
}

pub(crate) enum SubmitSideInner {
    Open(SubmitSideOpen),
    ShutDownInitiated,
}

pub(crate) struct SubmitSideOpen {
    #[allow(dead_code)]
    id: usize,
    submitter: Submitter<'static>,
    sq: SubmissionQueue<'static>,
    slots: Slots<{ slots::co_owner::SUBMIT_SIDE }>,
    completion_side: Arc<Mutex<CompletionSide>>,
}

impl SubmitSide {
    pub(crate) fn weak(&self) -> SubmitSideWeak {
        SubmitSideWeak(Arc::downgrade(&self.inner))
    }
}

impl SubmitSideOpen {
    pub(crate) fn deconstruct(self) -> (Submitter<'static>, SubmissionQueue<'static>) {
        let SubmitSideOpen { submitter, sq, .. } = self;
        (submitter, sq)
    }
}

unsafe impl Send for SubmitSideOpen {}
unsafe impl Sync for SubmitSideOpen {}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("queue full")]
    QueueFull,
}
