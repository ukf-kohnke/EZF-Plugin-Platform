//! Bounded plugin-call execution, timeout accounting, and completion tracking.

use super::*;
use PluginAbi::PluginCancellationV2;
use log::LevelFilter;
use std::sync::mpsc;
use std::time::Instant;

impl PluginRuntime {
    pub(super) fn call_with_timeout<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where
            R: Send + 'static,
            F: FnOnce(&PluginRuntime, PluginCallContext) -> Result<R, String> + Send + 'static
    {
        self.call_with_timeout_accounted_parent(
            operation,
            PluginCallAccounting::Observed,
            None,
            call
        )
    }

    pub(super) fn call_with_timeout_parent<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        parent_cancellation: PluginCancellationV2,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where
            R: Send + 'static,
            F: FnOnce(&PluginRuntime, PluginCallContext) -> Result<R, String> + Send + 'static
    {
        self.call_with_timeout_accounted_parent(
            operation,
            PluginCallAccounting::Observed,
            Some(parent_cancellation),
            call
        )
    }

    pub(super) fn call_with_timeout_internal<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where
            R: Send + 'static,
            F: FnOnce(&PluginRuntime, PluginCallContext) -> Result<R, String> + Send + 'static
    {
        self.call_with_timeout_accounted_parent(
            operation,
            PluginCallAccounting::Internal,
            None,
            call
        )
    }

    fn call_with_timeout_accounted_parent<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        accounting: PluginCallAccounting,
        parent_cancellation: Option<PluginCancellationV2>,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where
            R: Send + 'static,
            F: FnOnce(&PluginRuntime, PluginCallContext) -> Result<R, String> + Send + 'static
    {
        let tracker = Arc::new(PluginCallTracker::new(parent_cancellation));
        let permit = match self.gate.try_acquire(tracker.cancellation_source()) {
            Ok(permit) => permit,
            Err(_) => {
                self.rejected_calls.fetch_add(1, Ordering::Relaxed);
                crate::emit_log(
                    LevelFilter::Warn,
                    &format!(
                        "Plugin operation '{}' rejected: admitted-call limit {} reached (workers={}, queue_capacity={})",
                        operation,
                        self.config.max_concurrent_calls,
                        self.config.worker_threads,
                        self.config.queue_capacity
                    )
                );
                return Err(PluginCallFailure::Busy);
            }
        };

        if self.unloading.load(Ordering::Acquire) {
            drop(permit);
            return Err(PluginCallFailure::Failed("plugin runtime is unloading".to_owned()));
        }

        let call_id = self.next_call_id.fetch_add(1, Ordering::Relaxed) + 1;
        let deadline = Instant::now() + self.config.call_timeout;
        let call_context = PluginCallContext::new(call_id, deadline, tracker.cancellation_token());
        if accounting == PluginCallAccounting::Observed {
            self.total_calls.fetch_add(1, Ordering::Relaxed);
        }
        let runtime = Arc::clone(self);
        let worker_tracker = Arc::clone(&tracker);
        let worker_context = call_context;
        let (sender, receiver) = mpsc::sync_channel(1);
        let job: PluginJob = Box::new(move || {
            let result = std::panic::catch_unwind(
                std::panic::AssertUnwindSafe(|| call(&runtime, worker_context))
            );
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(format!("plugin operation '{operation}' panicked")),
            };
            let was_timed_out = worker_tracker.complete();

            if accounting == PluginCallAccounting::Observed {
                match &result {
                    Ok(_) => runtime.record_success(call_id, was_timed_out),
                    Err(error) => runtime.record_failure(call_id, error.clone(), was_timed_out),
                }
            }

            if was_timed_out {
                let outcome = if result.is_ok() { "success" } else { "failure" };
                crate::emit_log(
                    LevelFilter::Info,
                    &format!("Plugin operation '{}' completed after the caller timed out ({outcome})", operation)
                );
            }

            // Release admission before publishing completion. Otherwise the caller can observe
            // the completed result and immediately submit a new call while the previous permit
            // is still held, producing a spurious `Busy` rejection.
            drop(permit);
            let _ = sender.send(result);
        });

        match self.worker_pool.submit(job) {
            Ok(()) => {}
            Err(PluginWorkerSubmitError::QueueFull(job)) => {
                // Dropping the rejected closure releases its admission permit and any captured
                // request state. Queue saturation is overload, not an internal plugin failure.
                drop(job);
                self.rejected_calls.fetch_add(1, Ordering::Relaxed);
                crate::emit_log(
                    LevelFilter::Warn,
                    &format!(
                        "Plugin operation '{}' rejected: worker queue is full (workers={}, queue_capacity={}, max_admitted_calls={})",
                        operation,
                        self.config.worker_threads,
                        self.config.queue_capacity,
                        self.config.max_concurrent_calls
                    )
                );
                return Err(PluginCallFailure::Busy);
            }
            Err(PluginWorkerSubmitError::Closed(job)) => {
                drop(job);
                let message = "plugin worker pool is closed".to_owned();
                if accounting == PluginCallAccounting::Observed {
                    self.record_submission_failure(message.clone());
                }
                return Err(PluginCallFailure::Failed(message));
            }
        }

        match receiver.recv_timeout(self.config.call_timeout) {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(PluginCallFailure::Failed(error)),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let timed_out = if accounting == PluginCallAccounting::Observed {
                    tracker.mark_timed_out(|| {
                        self.record_request_timeout(
                            format!(
                                "plugin operation '{operation}' timed out after {:?}; cooperative cancellation requested; operation remains active until it returns",
                                self.config.call_timeout
                            )
                        );
                    })
                } else {
                    tracker.mark_timed_out(|| {})
                };
                if timed_out {
                    let message = format!(
                        "plugin operation '{operation}' timed out after {:?}; cooperative cancellation requested; operation remains active until it returns",
                        self.config.call_timeout
                    );
                    crate::emit_log(LevelFilter::Warn, message.as_str());
                    Err(PluginCallFailure::Timeout(self.config.call_timeout))
                } else {
                    // The worker won the timeout/completion race. The result is already on the
                    // completion channel, so consume it instead of manufacturing a false timeout.
                    match receiver.recv() {
                        Ok(Ok(result)) => Ok(result),
                        Ok(Err(error)) => Err(PluginCallFailure::Failed(error)),
                        Err(_) => {
                            let message = format!(
                                "plugin operation '{operation}' worker disconnected"
                            );
                            Err(PluginCallFailure::Failed(message))
                        }
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let message = format!("plugin operation '{operation}' worker disconnected");
                Err(PluginCallFailure::Failed(message))
            }
        }
    }
}
