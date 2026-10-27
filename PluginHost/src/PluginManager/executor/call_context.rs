//! ABI-neutral metadata and cancellation primitives for one logical plugin operation.
//!
//! This type is intentionally safe to hand to different executor implementations. Its data model
//! exposes no ABI callback or process-local pointer; an inherited parent cancellation check may be
//! wrapped behind a local host closure, but that closure is not serialized. Executors must choose
//! their own execution-boundary adapter.

use std::sync::{
    Arc,
    Mutex,
    Weak,
    mpsc::{ self, Receiver, Sender },
    atomic::{ AtomicBool, Ordering },
};
use std::time::{ Duration, Instant };

type ParentCancellationCheck = Arc<dyn (Fn() -> bool) + Send + Sync + 'static>;

/// Shared cancellation source owned by the runtime. Clones refer to the same local cancellation
/// flag and subscribers are notified once when local cancellation is requested.
pub(crate) struct PluginCancellationLocalState {
    flag: AtomicBool,
    subscribers: Mutex<Vec<Sender<()>>>,
}

impl PluginCancellationLocalState {
    pub(crate) fn cancel(&self) {
        if self.flag.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut subscribers = self.subscribers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for sender in subscribers.drain(..) {
            let _ = sender.send(());
        }
    }

    fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub(crate) struct PluginCancellationSource {
    local: Arc<PluginCancellationLocalState>,
    parent_check: Option<ParentCancellationCheck>,
}

/// Read-only cancellation handle carried by a `PluginCallContext`.
#[derive(Clone)]
pub(crate) struct PluginCancellationToken {
    local: Arc<PluginCancellationLocalState>,
    parent_check: Option<ParentCancellationCheck>,
}

/// Metadata for one logical executor operation. The deadline is absolute within this process;
/// process-backed executors should transmit `remaining_budget()` rather than serializing `Instant`.
#[derive(Clone)]
pub(crate) struct PluginCallContext {
    operation_id: u64,
    deadline: Instant,
    cancellation: PluginCancellationToken,
}

impl PluginCancellationSource {
    pub(crate) fn new(
        parent_check: Option<Arc<dyn (Fn() -> bool) + Send + Sync + 'static>>
    ) -> Self {
        Self {
            local: Arc::new(PluginCancellationLocalState {
                flag: AtomicBool::new(false),
                subscribers: Mutex::new(Vec::new()),
            }),
            parent_check,
        }
    }

    pub(crate) fn token(&self) -> PluginCancellationToken {
        PluginCancellationToken {
            local: Arc::clone(&self.local),
            parent_check: self.parent_check.clone(),
        }
    }

    pub(crate) fn cancel(&self) {
        self.local.cancel();
    }

    pub(crate) fn local_state(&self) -> Arc<PluginCancellationLocalState> {
        Arc::clone(&self.local)
    }

    pub(crate) fn weak_local_state(&self) -> Weak<PluginCancellationLocalState> {
        Arc::downgrade(&self.local)
    }
}

impl PluginCancellationToken {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.local.is_cancelled() || self.parent_check.as_ref().is_some_and(|check| check())
    }

    /// Subscribe to local cancellation. If cancellation has already been requested (or the parent
    /// is already cancelled), the returned receiver is notified immediately. Parent cancellation
    /// that happens later remains observable through `is_cancelled()`.
    pub(crate) fn subscribe(&self) -> Receiver<()> {
        let (sender, receiver) = mpsc::channel();
        // Evaluate a possible inherited ABI cancellation before taking the subscriber lock. The
        // parent callback is supplied by the caller and must not run while internal state is locked.
        if self.is_cancelled() {
            let _ = sender.send(());
            return receiver;
        }

        let mut subscribers = self.local.subscribers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Pair this second local-flag check with `cancel()` holding the same mutex while draining,
        // so a local cancellation cannot be lost between checking and registering the receiver.
        if self.local.is_cancelled() {
            let _ = sender.send(());
        } else {
            subscribers.push(sender);
        }
        receiver
    }
}

impl PluginCallContext {
    pub(crate) fn new(
        operation_id: u64,
        deadline: Instant,
        cancellation: PluginCancellationToken
    ) -> Self {
        Self { operation_id, deadline, cancellation }
    }

    pub(crate) fn operation_id(&self) -> u64 {
        self.operation_id
    }

    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns the budget left at the instant of the call. It reaches zero even if an operation
    /// spent its entire budget waiting in the runtime's bounded worker queue.
    pub(crate) fn remaining_budget(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub(crate) fn is_deadline_expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    pub(crate) fn cancellation_token(&self) -> PluginCancellationToken {
        self.cancellation.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{ PluginCallContext, PluginCancellationSource };
    use std::time::{ Duration, Instant };

    #[test]
    fn context_exposes_operation_id_and_non_increasing_deadline_budget() {
        let context = PluginCallContext::new(
            42,
            Instant::now() + Duration::from_millis(250),
            PluginCancellationSource::new(None).token()
        );

        assert_eq!(context.operation_id(), 42);
        assert!(!context.is_deadline_expired());
        assert!(context.remaining_budget() <= Duration::from_millis(250));
    }

    #[test]
    fn expired_context_has_no_remaining_budget() {
        let context = PluginCallContext::new(
            7,
            Instant::now() - Duration::from_millis(1),
            PluginCancellationSource::new(None).token()
        );

        assert!(context.is_deadline_expired());
        assert_eq!(context.remaining_budget(), Duration::ZERO);
    }

    #[test]
    fn cancellation_subscription_observes_local_cancellation() {
        let source = PluginCancellationSource::new(None);
        let token = source.token();
        let notified = token.subscribe();

        assert!(!token.is_cancelled());
        source.cancel();
        assert!(token.is_cancelled());
        assert!(notified.try_recv().is_ok());
    }

    #[test]
    fn subscription_after_cancellation_is_not_lost() {
        let source = PluginCancellationSource::new(None);
        source.cancel();
        let notified = source.token().subscribe();
        assert!(notified.try_recv().is_ok());
    }
}
