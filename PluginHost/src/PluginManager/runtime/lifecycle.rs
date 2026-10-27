//! Quiescing, lifecycle callbacks, unload, and retry state transitions.

use super::*;
use std::{ sync::mpsc, time::Instant };

unsafe extern "C" fn plugin_is_cancelled(context: *const std::ffi::c_void) -> bool {
    if context.is_null() {
        return true;
    }
    let state = unsafe { &*(context as *const PluginCancellationState) };
    if state.local.load(Ordering::Acquire) {
        return true;
    }
    state.parent.is_some_and(|parent| unsafe { parent.is_cancelled() })
}

impl PluginRuntime {
    fn start_lifecycle_operation(
        &self,
        operation: PluginLifecycleOperation,
        executor: Arc<dyn PluginExecutor>
    ) -> Result<(), String> {
        let mut pending = self.pending_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.is_some() {
            return Err("a plugin lifecycle callback is already in progress".to_owned());
        }

        let cancellation = Arc::new(PluginCancellationState {
            local: AtomicBool::new(false),
            parent: None,
        });
        let callback_cancellation = PluginCancellationV2 {
            context: Arc::as_ptr(&cancellation).cast(),
            is_cancelled: plugin_is_cancelled,
        };
        // Keep the cancellation state alive for the full callback even if the caller times out.
        let cancellation_guard = Arc::clone(&cancellation);
        let receiver = self.lifecycle_executor.submit(
            Box::new(move || {
                let _cancellation_guard = cancellation_guard;
                match operation {
                    PluginLifecycleOperation::OnUnload => executor.on_unload(callback_cancellation),
                    PluginLifecycleOperation::Destroy => executor.destroy(callback_cancellation),
                }
            })
        )?;
        *pending = Some(PendingPluginLifecycle { operation, receiver, cancellation });
        Ok(())
    }

    fn start_destroy_operation(&self, executor: Arc<dyn PluginExecutor>) -> Result<(), String> {
        self.start_lifecycle_operation(PluginLifecycleOperation::Destroy, executor)
    }

    fn wait_for_pending_lifecycle(
        &self
    ) -> Result<Option<(PluginLifecycleOperation, Result<PluginLifecycleStatus, String>)>, bool> {
        let pending = {
            let mut pending = self.pending_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            pending.take()
        };
        let Some(pending) = pending else {
            return Ok(None);
        };

        match pending.receiver.recv_timeout(self.config.lifecycle_timeout) {
            Ok(result) => Ok(Some((pending.operation, result))),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                pending.cancellation.local.store(true, Ordering::Release);
                let mut slot = self.pending_lifecycle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *slot = Some(pending);
                Err(true)
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Ok(
                    Some((
                        pending.operation,
                        Err("plugin lifecycle worker disconnected".to_owned()),
                    ))
                )
            }
        }
    }

    fn validate_lifecycle_executor(&self) -> Result<Arc<dyn PluginExecutor>, String> {
        let state = self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.destroyed {
            return Err("plugin runtime is unloaded".to_owned());
        }
        let executor = state.executor
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| "plugin runtime is unloaded".to_owned())?;
        executor.validate_for_lifecycle()?;
        Ok(executor)
    }

    fn complete_destroy_success(&self) -> bool {
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.executor.is_none() || state.destroyed {
            return false;
        }
        // Mark the opaque instance as destroyed, but retain the executor/library image until the
        // final PluginRuntime reference is dropped, matching the previous unload behavior.
        state.destroyed = true;
        self.set_health(PluginHealthState::Unloaded);
        self.unloading.store(false, Ordering::Release);
        self.worker_pool.shutdown();
        self.lifecycle_executor.shutdown();
        true
    }

    /// Runs or retries the destroy phase after `on_unload` has completed successfully.
    /// Keeping the phases separate ensures a timeout or destroy failure never causes a
    /// completed `on_unload` callback to be invoked a second time.
    fn finish_destroy_lifecycle(&self, executor: Arc<dyn PluginExecutor>) -> bool {
        if let Err(message) = self.start_destroy_operation(executor) {
            self.unloading.store(false, Ordering::Release);
            self.set_health_error(PluginHealthState::Failed, message);
            return false;
        }

        match self.wait_for_pending_lifecycle() {
            Err(true) => {
                let message = format!(
                    "plugin destroy timed out after {:?}; lifecycle callback remains active and the runtime stays registered",
                    self.config.lifecycle_timeout
                );
                self.set_health_error(PluginHealthState::Stopping, message);
                false
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Success)))) =>
                self.complete_destroy_success(),
            Ok(
                Some(
                    (PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Failed(status))),
                ),
            ) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy returned status {status}")
                );
                false
            }
            Ok(Some((_, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy failed: {message}")
                );
                false
            }
            Ok(None) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin destroy lifecycle result was unexpectedly absent"
                );
                false
            }
            Ok(Some((PluginLifecycleOperation::OnUnload, Ok(PluginLifecycleStatus::Success)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "unexpected on_unload result while waiting for plugin destroy: 0"
                );
                false
            }
            Ok(
                Some(
                    (PluginLifecycleOperation::OnUnload, Ok(PluginLifecycleStatus::Failed(status))),
                ),
            ) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!(
                        "unexpected on_unload result while waiting for plugin destroy: {status}"
                    )
                );
                false
            }
            Err(false) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin destroy lifecycle wait failed"
                );
                false
            }
        }
    }

    /// Quiesces the plugin with a bounded wait. A timeout never forcefully unloads a library while
    /// plugin work is still executing or queued; the manager keeps the plugin registered so the
    /// operation can be retried safely after all admitted calls drain.
    pub(crate) fn unload(&self) -> bool {
        // A prior lifecycle callback may still be finishing after its caller-side timeout. In that
        // case the runtime remains in `Stopping`; a subsequent unload retries by first observing
        // the actual callback result. No lifecycle stage is invoked concurrently.
        if
            self.pending_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some()
        {
            match self.wait_for_pending_lifecycle() {
                Err(true) => {
                    self.set_health_error(
                        PluginHealthState::Stopping,
                        format!(
                            "plugin lifecycle callback is still running after {:?}",
                            self.config.lifecycle_timeout
                        )
                    );
                    return false;
                }
                Err(false) => {
                    self.unloading.store(false, Ordering::Release);
                    self.set_health_error(
                        PluginHealthState::Failed,
                        "plugin lifecycle callback failed".to_string()
                    );
                    return false;
                }
                Ok(None) => {}
                Ok(Some((operation, result))) => {
                    match (operation, result) {
                        (
                            PluginLifecycleOperation::OnUnload,
                            Ok(PluginLifecycleStatus::Success),
                        ) => {
                            // The timed-out callback has now completed successfully. Persist the
                            // phase before resuming so later destroy retries never invoke it again.
                            self.on_unload_completed.store(true, Ordering::Release);
                            let executor = match self.validate_lifecycle_executor() {
                                Ok(value) => value,
                                Err(message) => {
                                    self.unloading.store(false, Ordering::Release);
                                    self.set_health_error(PluginHealthState::Failed, message);
                                    return false;
                                }
                            };
                            return self.finish_destroy_lifecycle(executor);
                        }
                        (
                            PluginLifecycleOperation::OnUnload,
                            Ok(PluginLifecycleStatus::Failed(status)),
                        ) => {
                            self.unloading.store(false, Ordering::Release);
                            self.set_health_error(
                                PluginHealthState::Failed,
                                format!("plugin on_unload returned status {status}")
                            );
                            return false;
                        }
                        (PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Success)) => {
                            return self.complete_destroy_success();
                        }
                        (
                            PluginLifecycleOperation::Destroy,
                            Ok(PluginLifecycleStatus::Failed(status)),
                        ) => {
                            self.unloading.store(false, Ordering::Release);
                            self.set_health_error(
                                PluginHealthState::Failed,
                                format!("plugin destroy returned status {status}")
                            );
                            return false;
                        }
                        (operation, Err(message)) => {
                            self.unloading.store(false, Ordering::Release);
                            self.set_health_error(
                                PluginHealthState::Failed,
                                format!(
                                    "plugin lifecycle operation {operation:?} failed: {message}"
                                )
                            );
                            return false;
                        }
                    }
                }
            }
        } else if self.unloading.swap(true, Ordering::AcqRel) {
            return false;
        }

        self.set_health(PluginHealthState::Stopping);
        self.gate.cancel_all();

        let deadline = Instant::now() + self.config.unload_timeout;
        if !self.gate.wait_until_idle(deadline) {
            self.unloading.store(false, Ordering::Release);
            let message = format!(
                "plugin unload timed out after {:?} while waiting for in-flight calls to drain",
                self.config.unload_timeout
            );
            self.set_health_error(PluginHealthState::Degraded, message);
            return false;
        }

        let executor = match self.validate_lifecycle_executor() {
            Ok(value) => value,
            Err(message) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(PluginHealthState::Failed, message);
                return false;
            }
        };

        if self.on_unload_completed.load(Ordering::Acquire) {
            return self.finish_destroy_lifecycle(executor);
        }

        if
            let Err(message) = self.start_lifecycle_operation(
                PluginLifecycleOperation::OnUnload,
                Arc::clone(&executor)
            )
        {
            self.unloading.store(false, Ordering::Release);
            self.set_health_error(PluginHealthState::Failed, message);
            return false;
        }

        match self.wait_for_pending_lifecycle() {
            Err(true) => {
                let message = format!(
                    "plugin on_unload timed out after {:?}; lifecycle callback remains active and the runtime stays registered",
                    self.config.lifecycle_timeout
                );
                self.set_health_error(PluginHealthState::Stopping, message);
                false
            }
            Ok(Some((PluginLifecycleOperation::OnUnload, Ok(PluginLifecycleStatus::Success)))) => {
                self.on_unload_completed.store(true, Ordering::Release);
                self.finish_destroy_lifecycle(executor)
            }
            Ok(
                Some(
                    (PluginLifecycleOperation::OnUnload, Ok(PluginLifecycleStatus::Failed(status))),
                ),
            ) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin on_unload returned status {status}")
                );
                false
            }
            Ok(Some((PluginLifecycleOperation::OnUnload, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin on_unload failed: {message}")
                );
                false
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Success)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin destroy returned status 0 after on_unload completed"
                );
                false
            }
            Ok(
                Some(
                    (PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Failed(status))),
                ),
            ) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy returned status {status} after on_unload completed")
                );
                false
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy failed after on_unload completed: {message}")
                );
                false
            }
            Ok(None) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin on_unload lifecycle result was unexpectedly absent"
                );
                false
            }
            Err(false) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin on_unload lifecycle wait failed"
                );
                false
            }
        }
    }
}
