//! Quiescing, lifecycle callbacks, unload, and retry state transitions.

use super::*;
use std::{ sync::mpsc, time::Instant };

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

        let cancellation = PluginCancellationSource::new(None);
        let escalation_grace = executor.lifecycle_escalation_grace();
        let operation_id = self.next_call_id.fetch_add(1, Ordering::Relaxed) + 1;
        let deadline = Instant::now() + self.config.lifecycle_timeout;
        let call_context = PluginCallContext::new(operation_id, deadline, cancellation.token());
        // The call context and pending lifecycle state share the cancellation source, so a timeout
        // remains observable until the native callback actually returns.
        let receiver = self.lifecycle_executor.submit(
            Box::new(move || {
                match operation {
                    PluginLifecycleOperation::OnUnload => executor.on_unload(&call_context),
                    PluginLifecycleOperation::Destroy => executor.destroy(&call_context),
                }
            })
        )?;
        *pending = Some(PendingPluginLifecycle {
            operation,
            receiver,
            cancellation,
            escalation_grace,
        });
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
                pending.cancellation.cancel();

                // Process-backed callbacks need a bounded interval to send Cancel, consume the
                // configured cancellation grace, and kill/reap an uncooperative child. This is
                // intentionally zero for InProcessExecutor: a native thread cannot be killed
                // safely, so its callback remains pending and the plugin stays registered.
                if !pending.escalation_grace.is_zero() {
                    match pending.receiver.recv_timeout(pending.escalation_grace) {
                        Ok(result) => {
                            return Ok(Some((pending.operation, result)));
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            return Ok(
                                Some((
                                    pending.operation,
                                    Err(
                                        "plugin lifecycle worker disconnected during cancellation escalation".to_owned()
                                    ),
                                ))
                            );
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                }

                // The child may already be reaped even if the lifecycle executor thread has not
                // yet published its result. In that case the process boundary is complete and we
                // must not require a second public unload attempt merely to observe the channel.
                if self.process_exit_confirmed() {
                    return Ok(
                        Some((pending.operation, Ok(PluginLifecycleStatus::ForcedTermination)))
                    );
                }

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

    fn process_exit_confirmed(&self) -> bool {
        let state = self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.executor.as_ref().is_some_and(|executor| executor.process_exit_confirmed())
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

    fn complete_destroy_success(&self) -> PluginUnloadOutcome {
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.executor.is_none() || state.destroyed {
            return PluginUnloadOutcome::Failed;
        }
        // Mark the opaque instance as destroyed, but retain the executor/library image until the
        // final PluginRuntime reference is dropped, matching the previous unload behavior.
        state.destroyed = true;
        self.set_health(PluginHealthState::Unloaded);
        self.unloading.store(false, Ordering::Release);
        self.worker_pool.shutdown();
        self.lifecycle_executor.shutdown();
        PluginUnloadOutcome::Unloaded
    }

    /// Detaches a process-backed plugin after the executor confirms that its worker is dead and
    /// reaped, while preserving the fact that plugin cleanup did not finish gracefully. This also
    /// covers a worker killed during `on_unload` or `destroy`, not just a failed shutdown handshake.
    fn complete_forced_termination(&self) -> PluginUnloadOutcome {
        let mut state = match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.executor.is_none() || state.destroyed {
            return PluginUnloadOutcome::Failed;
        }
        state.destroyed = true;
        self.set_health_error(
            PluginHealthState::Failed,
            "plugin worker termination/reaping was confirmed before graceful lifecycle completion; plugin cleanup may be incomplete"
        );
        self.unloading.store(false, Ordering::Release);
        self.worker_pool.shutdown();
        self.lifecycle_executor.shutdown();
        PluginUnloadOutcome::ForcedTermination
    }

    /// Runs or retries the destroy phase after `on_unload` has completed successfully.
    /// Keeping the phases separate ensures a timeout or destroy failure never causes a
    /// completed `on_unload` callback to be invoked a second time.
    fn finish_destroy_lifecycle(&self, executor: Arc<dyn PluginExecutor>) -> PluginUnloadOutcome {
        if let Err(message) = self.start_destroy_operation(executor) {
            self.unloading.store(false, Ordering::Release);
            self.set_health_error(PluginHealthState::Failed, message);
            return PluginUnloadOutcome::Failed;
        }

        match self.wait_for_pending_lifecycle() {
            Err(true) => {
                let message = format!(
                    "plugin destroy timed out after {:?}; lifecycle callback remains active and the runtime stays registered",
                    self.config.lifecycle_timeout
                );
                self.set_health_error(PluginHealthState::Stopping, message);
                PluginUnloadOutcome::Failed
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Success)))) =>
                self.complete_destroy_success(),
            Ok(
                Some(
                    (
                        PluginLifecycleOperation::Destroy,
                        Ok(PluginLifecycleStatus::ForcedTermination),
                    ),
                ),
            ) => self.complete_forced_termination(),
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
                PluginUnloadOutcome::Failed
            }
            Ok(Some((_, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy failed: {message}")
                );
                PluginUnloadOutcome::Failed
            }
            Ok(None) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin destroy lifecycle result was unexpectedly absent"
                );
                PluginUnloadOutcome::Failed
            }
            Ok(Some((PluginLifecycleOperation::OnUnload, Ok(PluginLifecycleStatus::Success)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "unexpected on_unload result while waiting for plugin destroy: 0"
                );
                PluginUnloadOutcome::Failed
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
                PluginUnloadOutcome::Failed
            }
            Ok(
                Some(
                    (
                        PluginLifecycleOperation::OnUnload,
                        Ok(PluginLifecycleStatus::ForcedTermination),
                    ),
                ),
            ) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "unexpected forced termination result while waiting for plugin on_unload"
                );
                PluginUnloadOutcome::Failed
            }
            Err(false) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin destroy lifecycle wait failed"
                );
                PluginUnloadOutcome::Failed
            }
        }
    }

    /// Quiesces the plugin with a bounded wait. In-process code is never forcefully removed while
    /// its native callback is running. A process-backed executor may complete non-gracefully only
    /// after it confirms the child has exited and has been reaped.
    pub(crate) fn unload(&self) -> PluginUnloadOutcome {
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
                    return PluginUnloadOutcome::Failed;
                }
                Err(false) => {
                    self.unloading.store(false, Ordering::Release);
                    self.set_health_error(
                        PluginHealthState::Failed,
                        "plugin lifecycle callback failed".to_string()
                    );
                    return PluginUnloadOutcome::Failed;
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
                                    if self.process_exit_confirmed() {
                                        return self.complete_forced_termination();
                                    }
                                    self.unloading.store(false, Ordering::Release);
                                    self.set_health_error(PluginHealthState::Failed, message);
                                    return PluginUnloadOutcome::Failed;
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
                            return PluginUnloadOutcome::Failed;
                        }
                        (
                            PluginLifecycleOperation::OnUnload,
                            Ok(PluginLifecycleStatus::ForcedTermination),
                        ) => {
                            return self.complete_forced_termination();
                        }
                        (PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Success)) => {
                            return self.complete_destroy_success();
                        }
                        (
                            PluginLifecycleOperation::Destroy,
                            Ok(PluginLifecycleStatus::ForcedTermination),
                        ) => {
                            return self.complete_forced_termination();
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
                            return PluginUnloadOutcome::Failed;
                        }
                        (operation, Err(message)) => {
                            self.unloading.store(false, Ordering::Release);
                            self.set_health_error(
                                PluginHealthState::Failed,
                                format!(
                                    "plugin lifecycle operation {operation:?} failed: {message}"
                                )
                            );
                            return PluginUnloadOutcome::Failed;
                        }
                    }
                }
            }
        } else if self.unloading.swap(true, Ordering::AcqRel) {
            return PluginUnloadOutcome::Failed;
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
            return PluginUnloadOutcome::Failed;
        }

        let executor = match self.validate_lifecycle_executor() {
            Ok(value) => value,
            Err(message) => {
                if self.process_exit_confirmed() {
                    return self.complete_forced_termination();
                }
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(PluginHealthState::Failed, message);
                return PluginUnloadOutcome::Failed;
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
            return PluginUnloadOutcome::Failed;
        }

        match self.wait_for_pending_lifecycle() {
            Err(true) => {
                let message = format!(
                    "plugin on_unload timed out after {:?}; lifecycle callback remains active and the runtime stays registered",
                    self.config.lifecycle_timeout
                );
                self.set_health_error(PluginHealthState::Stopping, message);
                PluginUnloadOutcome::Failed
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
                PluginUnloadOutcome::Failed
            }
            Ok(
                Some(
                    (
                        PluginLifecycleOperation::OnUnload,
                        Ok(PluginLifecycleStatus::ForcedTermination),
                    ),
                ),
            ) => self.complete_forced_termination(),
            Ok(Some((PluginLifecycleOperation::OnUnload, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin on_unload failed: {message}")
                );
                PluginUnloadOutcome::Failed
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Ok(PluginLifecycleStatus::Success)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin destroy returned status 0 after on_unload completed"
                );
                PluginUnloadOutcome::Failed
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
                PluginUnloadOutcome::Failed
            }
            Ok(
                Some(
                    (
                        PluginLifecycleOperation::Destroy,
                        Ok(PluginLifecycleStatus::ForcedTermination),
                    ),
                ),
            ) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "unexpected forced termination result for destroy before on_unload completed"
                );
                PluginUnloadOutcome::Failed
            }
            Ok(Some((PluginLifecycleOperation::Destroy, Err(message)))) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    format!("plugin destroy failed after on_unload completed: {message}")
                );
                PluginUnloadOutcome::Failed
            }
            Ok(None) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin on_unload lifecycle result was unexpectedly absent"
                );
                PluginUnloadOutcome::Failed
            }
            Err(false) => {
                self.unloading.store(false, Ordering::Release);
                self.set_health_error(
                    PluginHealthState::Failed,
                    "plugin on_unload lifecycle wait failed"
                );
                PluginUnloadOutcome::Failed
            }
        }
    }
}
