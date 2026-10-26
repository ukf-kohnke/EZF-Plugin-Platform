//! Bounded plugin-call execution, timeout accounting, and ABI buffer helpers.

use super::*;
use PluginContracts::ApiContract::{ PluginEndpointError, PluginEndpointResponse };
use PluginAbi::{ AbiSlice, PluginEndpointFnV2, PluginEndpointRawFnV2, PluginVTableV2Ext };
use log::LevelFilter;
use serde::de::DeserializeOwned;
use std::sync::mpsc;

impl PluginRuntime {
    pub(super) fn call_with_timeout<R, F>(
        self: &Arc<Self>,
        operation: &'static str,
        call: F
    )
        -> Result<R, PluginCallFailure>
        where
            R: Send + 'static,
            F: FnOnce(&PluginRuntime, PluginCancellationV2) -> Result<R, String> + Send + 'static
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
            F: FnOnce(&PluginRuntime, PluginCancellationV2) -> Result<R, String> + Send + 'static
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
            F: FnOnce(&PluginRuntime, PluginCancellationV2) -> Result<R, String> + Send + 'static
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
            F: FnOnce(&PluginRuntime, PluginCancellationV2) -> Result<R, String> + Send + 'static
    {
        let tracker = Arc::new(PluginCallTracker::new(parent_cancellation));
        let permit = match self.gate.try_acquire(tracker.cancellation()) {
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
        if accounting == PluginCallAccounting::Observed {
            self.total_calls.fetch_add(1, Ordering::Relaxed);
        }
        let runtime = Arc::clone(self);
        let worker_tracker = Arc::clone(&tracker);
        let (sender, receiver) = mpsc::sync_channel(1);
        let job: PluginJob = Box::new(move || {
            let cancellation = worker_tracker.abi_cancellation();
            let result = std::panic::catch_unwind(
                std::panic::AssertUnwindSafe(|| call(&runtime, cancellation))
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

    /// Calls an ABI function and copies its response bytes into host-owned memory without
    /// decoding the JSON payload. The plugin allocator is always used to release the ABI buffer.
    /// This is used by the database capability to forward successful response envelopes unchanged.
    pub(super) fn call_abi_bytes_direct_from_bytes<F>(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2,
        select_call: F
    ) -> Result<Vec<u8>, String>
        where F: FnOnce(&PluginVTableV2) -> PluginEndpointFnV2
    {
        self.with_instance(|context, vtable| {
            let mut response = AbiBuffer::EMPTY;
            let status = unsafe {
                select_call(vtable)(
                    context,
                    AbiSlice::from_bytes(request_bytes),
                    cancellation,
                    &mut response
                )
            };
            if status != PLUGIN_STATUS_OK {
                if Self::is_valid_buffer(response) && !response.ptr.is_null() {
                    unsafe {
                        (vtable.free_buffer)(response);
                    }
                }
                return Err(format!("plugin ABI call failed with status {status}"));
            }
            if !Self::is_valid_buffer(response) {
                return Err("plugin returned an invalid response buffer".to_owned());
            }
            let bytes = unsafe { response.to_vec() };
            // Release through the plugin's allocator regardless of whether conversion succeeded.
            unsafe {
                (vtable.free_buffer)(response);
            }
            bytes.ok_or_else(|| "plugin returned an invalid response buffer".to_owned())
        })?
    }

    pub(super) fn call_json_direct_from_bytes<R: DeserializeOwned, F>(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2,
        select_call: F
    ) -> Result<R, String>
        where F: FnOnce(&PluginVTableV2) -> PluginEndpointFnV2
    {
        let bytes = self.call_abi_bytes_direct_from_bytes(
            request_bytes,
            cancellation,
            select_call
        )?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode plugin response: {error}"))
    }

    /// Calls a plugin endpoint using the optional split-metadata/body ABI fast path.
    ///
    /// New plugins return small JSON metadata plus an opaque body buffer. Older ABI V2 plugins
    /// continue to use the original single JSON envelope. In either case the host owns a copy of
    /// the response bytes before invoking the plugin's allocator-specific free callback.
    pub(super) fn call_endpoint_direct_from_bytes(
        &self,
        request_bytes: &[u8],
        cancellation: PluginCancellationV2
    ) -> Result<Result<PluginEndpointResponse, PluginEndpointError>, String> {
        self.with_instance(|context, vtable| {
            if let Some(raw_endpoint) = Self::raw_endpoint_callback(vtable) {
                let mut metadata = AbiBuffer::EMPTY;
                let mut body = AbiBuffer::EMPTY;
                let status = unsafe {
                    raw_endpoint(
                        context,
                        AbiSlice::from_bytes(request_bytes),
                        cancellation,
                        &mut metadata,
                        &mut body
                    )
                };

                if status != PLUGIN_STATUS_OK {
                    Self::free_if_valid(vtable, metadata);
                    Self::free_if_valid(vtable, body);
                    return Err(
                        format!("plugin ABI endpoint fast path failed with status {status}")
                    );
                }

                // Read both buffers before propagating either error so a valid companion buffer
                // is not leaked if the other output is malformed.
                let metadata_bytes = Self::copy_and_free_buffer(vtable, metadata);
                let body_bytes = Self::copy_and_free_buffer(vtable, body);
                let metadata_bytes = metadata_bytes?;
                let body_bytes = body_bytes?;

                let mut response: Result<PluginEndpointResponse, PluginEndpointError> = serde_json
                    ::from_slice(&metadata_bytes)
                    .map_err(|error|
                        format!("failed to decode plugin endpoint metadata: {error}")
                    )?;

                match &mut response {
                    Ok(success) => {
                        if !success.body.is_empty() {
                            return Err(
                                "plugin endpoint fast path returned a body in metadata and body buffers".to_owned()
                            );
                        }
                        success.body = body_bytes;
                    }
                    Err(_) => {
                        if !body_bytes.is_empty() {
                            return Err(
                                "plugin endpoint fast path returned a body alongside an error".to_owned()
                            );
                        }
                    }
                }

                Ok(response)
            } else {
                // Compatibility path for existing plugins compiled before the optional endpoint
                // callback was added to the append-only vtable extension.
                let mut response = AbiBuffer::EMPTY;
                let status = unsafe {
                    (vtable.handle_endpoint)(
                        context,
                        AbiSlice::from_bytes(request_bytes),
                        cancellation,
                        &mut response
                    )
                };

                if status != PLUGIN_STATUS_OK {
                    Self::free_if_valid(vtable, response);
                    return Err(format!("plugin ABI call failed with status {status}"));
                }

                let bytes = Self::copy_and_free_buffer(vtable, response)?;
                serde_json
                    ::from_slice(&bytes)
                    .map_err(|error| format!("failed to decode plugin response: {error}"))
            }
        })?
    }

    fn raw_endpoint_callback(vtable: &PluginVTableV2) -> Option<PluginEndpointRawFnV2> {
        if (vtable.struct_size as usize) < std::mem::size_of::<PluginVTableV2Ext>() {
            return None;
        }

        // SAFETY: The advertised table size was checked above. `PluginVTableV2Ext` begins with
        // the unchanged `PluginVTableV2` prefix, so the extension field is readable when present.
        let extended = unsafe { &*(vtable as *const PluginVTableV2 as *const PluginVTableV2Ext) };
        extended.handle_endpoint_raw
    }

    fn free_if_valid(vtable: &PluginVTableV2, buffer: AbiBuffer) {
        if Self::is_valid_buffer(buffer) && !buffer.ptr.is_null() {
            unsafe {
                (vtable.free_buffer)(buffer);
            }
        }
    }

    fn copy_and_free_buffer(vtable: &PluginVTableV2, buffer: AbiBuffer) -> Result<Vec<u8>, String> {
        if !Self::is_valid_buffer(buffer) {
            return Err("plugin returned an invalid response buffer".to_owned());
        }

        let bytes = unsafe { buffer.to_vec() };
        // SAFETY: `is_valid_buffer` verified the producer buffer metadata. Always reclaim valid
        // buffers with the allocator callback supplied by the producing plugin.
        unsafe {
            (vtable.free_buffer)(buffer);
        }
        bytes.ok_or_else(|| "plugin returned an invalid response buffer".to_owned())
    }
}
