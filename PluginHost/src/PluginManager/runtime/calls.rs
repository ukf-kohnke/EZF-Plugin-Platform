//! Admission, cancellation, and caller/worker call-lifecycle state.
use super::super::executor::{
    PluginCancellationLocalState,
    PluginCancellationSource,
    PluginCancellationToken,
};
use PluginAbi::PluginCancellationV2;
use std::sync::{ Arc, Condvar, Mutex, Weak };
use std::time::{ Duration, Instant };

pub(super) struct PluginCallGate {
    state: Mutex<usize>,
    active_cancellations: Mutex<Vec<Weak<PluginCancellationLocalState>>>,
    max_concurrent_calls: usize,
    idle: Condvar,
}

impl PluginCallGate {
    pub(super) fn new(max_concurrent_calls: usize) -> Self {
        Self {
            state: Mutex::new(0),
            active_cancellations: Mutex::new(Vec::new()),
            max_concurrent_calls: max_concurrent_calls.max(1),
            idle: Condvar::new(),
        }
    }

    /// Admission is deliberately non-blocking. A full plugin is considered overloaded instead
    /// of making a caller wait behind already admitted work. Request execution still has its own
    /// timeout after admission.
    pub(super) fn try_acquire(
        self: &Arc<Self>,
        cancellation: PluginCancellationSource
    ) -> Result<PluginCallPermit, ()> {
        let mut active = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *active >= self.max_concurrent_calls {
            return Err(());
        }
        *active += 1;
        self.active_cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(cancellation.weak_local_state());
        Ok(PluginCallPermit {
            gate: Arc::clone(self),
            cancellation,
        })
    }

    pub(super) fn active_calls(&self) -> usize {
        *self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Requests cooperative cancellation from every currently admitted plugin operation.
    /// No native thread is terminated and no memory is invalidated by this operation.
    pub(super) fn cancel_all(&self) {
        let mut cancellations = self.active_cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cancellations.retain(|weak| {
            let Some(local_state) = weak.upgrade() else {
                return false;
            };
            local_state.cancel();
            true
        });
    }

    pub(super) fn wait_until_idle(&self, deadline: Instant) -> bool {
        let mut active = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while *active != 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (guard, timeout) = self.idle
                .wait_timeout(active, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            active = guard;
            if timeout.timed_out() && *active != 0 {
                return false;
            }
        }
        true
    }
}

pub(super) struct PluginCallPermit {
    gate: Arc<PluginCallGate>,
    cancellation: PluginCancellationSource,
}

impl Drop for PluginCallPermit {
    fn drop(&mut self) {
        // Keep the same lock order as `try_acquire` (state -> active_cancellations) to avoid
        // an ABBA deadlock when admission and completion happen concurrently.
        let mut active = self.gate.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        *active = (*active).saturating_sub(1);
        if *active == 0 {
            self.gate.idle.notify_all();
        }
        drop(active);

        let local_state = self.cancellation.local_state();
        let mut cancellations = self.gate.active_cancellations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cancellations.retain(|weak| {
            weak.upgrade().is_some_and(|candidate| !Arc::ptr_eq(&candidate, &local_state))
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PluginCallLifecycleState {
    Pending,
    TimedOut,
    Completed,
}

pub(super) struct PluginCallTracker {
    state: Mutex<PluginCallLifecycleState>,
    cancellation: PluginCancellationSource,
    // The ABI cancellation descriptor borrows its context from the caller. A nested call may
    // outlive its caller after timeout, so gate callback access and invalidate it before return.
    parent_context: Option<Arc<Mutex<Option<PluginCancellationV2>>>>,
}

impl PluginCallTracker {
    pub(super) fn new(parent: Option<PluginCancellationV2>) -> Self {
        let parent_context = parent.map(|parent| Arc::new(Mutex::new(Some(parent))));
        let parent_check = parent_context.as_ref().map(|context| {
            let context = Arc::clone(context);
            Arc::new(move || {
                // Keep the mutex guard held while invoking the callback. Invalidating the context
                // must wait for any in-flight callback to return before the borrowed ABI context
                // can leave scope; copying the raw descriptor and unlocking first would leave a
                // narrow use-after-return race.
                let parent = context.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                match *parent {
                    Some(parent) => unsafe { parent.is_cancelled() }
                    // A nested operation that is still unwinding after its caller returned must
                    // treat that parent as cancelled instead of dereferencing a stale ABI pointer.
                    None => true,
                }
            }) as Arc<dyn (Fn() -> bool) + Send + Sync>
        });
        Self {
            state: Mutex::new(PluginCallLifecycleState::Pending),
            cancellation: PluginCancellationSource::new(parent_check),
            parent_context,
        }
    }

    pub(super) fn invalidate_parent_context(&self) {
        if let Some(context) = &self.parent_context {
            *context.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        }
    }

    pub(super) fn cancellation_source(&self) -> PluginCancellationSource {
        self.cancellation.clone()
    }

    pub(super) fn cancellation_token(&self) -> PluginCancellationToken {
        self.cancellation.token()
    }

    /// Marks the caller-visible timeout and requests cooperative cancellation atomically with the
    /// state transition. This closes the timeout/completion race deterministically.
    pub(super) fn mark_timed_out<F>(&self, on_marked: F) -> bool where F: FnOnce() {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *state != PluginCallLifecycleState::Pending {
            return false;
        }
        self.cancellation.cancel();
        *state = PluginCallLifecycleState::TimedOut;
        on_marked();
        true
    }

    /// Completes the underlying plugin operation. Returns whether the caller had already timed
    /// out, allowing the worker to close the corresponding `timed_out_active_calls` lifecycle.
    pub(super) fn complete(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let timed_out = *state == PluginCallLifecycleState::TimedOut;
        if *state != PluginCallLifecycleState::Completed {
            *state = PluginCallLifecycleState::Completed;
        }
        timed_out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PluginCallAccounting {
    /// A caller-visible plugin operation. These calls contribute to runtime health and call
    /// counters because they represent productive plugin traffic.
    Observed,
    /// Loader-internal plugin execution (currently `name` and `manifest`). These operations still
    /// use the full bounded/timeout/panic-isolated execution path, but must not pollute productive
    /// call counters because they happen before the plugin is registered.
    Internal,
}

#[derive(Debug)]
pub(super) enum PluginCallFailure {
    Busy,
    Timeout(Duration),
    Failed(String),
}

impl std::fmt::Display for PluginCallFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => write!(f, "plugin call admission or worker queue capacity reached"),
            Self::Timeout(timeout) => write!(f, "plugin call timed out after {:?}", timeout),
            Self::Failed(message) => write!(f, "{message}"),
        }
    }
}

#[cfg(test)]
mod parent_context_tests {
    use super::PluginCallTracker;
    use PluginAbi::PluginCancellationV2;
    use std::ffi::c_void;
    use std::sync::atomic::{ AtomicBool, Ordering };

    unsafe extern "C" fn parent_is_cancelled(context: *const c_void) -> bool {
        if context.is_null() {
            return true;
        }
        (unsafe { &*context.cast::<AtomicBool>() }).load(Ordering::Acquire)
    }

    #[test]
    fn invalidated_parent_context_is_treated_as_cancelled() {
        let external_state = AtomicBool::new(false);
        let parent = PluginCancellationV2 {
            context: (&external_state as *const AtomicBool).cast(),
            is_cancelled: parent_is_cancelled,
        };
        let tracker = PluginCallTracker::new(Some(parent));
        let token = tracker.cancellation_token();

        assert!(!token.is_cancelled());
        tracker.invalidate_parent_context();
        assert!(token.is_cancelled());
    }
}
