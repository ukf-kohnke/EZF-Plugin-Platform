//! Public plugin operations executed through the bounded runtime call path.

use super::*;
use PluginAbi::PluginCancellationV2;
use PluginContracts::ApiContract::{
    PLUGIN_OVERLOADED_CODE,
    PLUGIN_OVERLOADED_USER_MESSAGE,
    PluginManifest,
};

impl PluginRuntime {
    pub(crate) fn name(self: &Arc<Self>) -> Result<String, String> {
        let bytes = self
            .call_with_timeout_internal("name", |runtime, context| {
                runtime.with_executor(|executor| executor.name_bytes(&context))
            })
            .map_err(|error| error.to_string())?;
        let name = serde_json
            ::from_slice::<String>(&bytes)
            .map_err(|error| format!("failed to decode plugin name: {error}"))?;
        self.set_name(name.clone());
        Ok(name)
    }

    pub(crate) fn manifest(self: &Arc<Self>) -> Result<PluginManifest, String> {
        let bytes = self
            .call_with_timeout_internal("manifest", |runtime, context| {
                runtime.with_executor(|executor| executor.manifest_bytes(&context))
            })
            .map_err(|error| error.to_string())?;
        serde_json
            ::from_slice(&bytes)
            .map_err(|error| format!("failed to decode plugin manifest: {error}"))
    }

    pub(crate) fn is_loaded(self: &Arc<Self>) -> Result<bool, String> {
        self.call_with_timeout("is_loaded", |runtime, context| {
            runtime.with_executor(|executor| executor.is_loaded(&context))
        }).map_err(|error| error.to_string())
    }

    pub(crate) fn handle_endpoint(
        self: &Arc<Self>,
        request: PluginContracts::ApiContract::PluginEndpointRequest
    ) -> Result<
        PluginContracts::ApiContract::PluginEndpointResponse,
        PluginContracts::ApiContract::PluginEndpointError
    > {
        self.handle_endpoint_with_parent(request, None)
    }

    pub(crate) fn handle_endpoint_with_parent(
        self: &Arc<Self>,
        request: PluginContracts::ApiContract::PluginEndpointRequest,
        parent_cancellation: Option<PluginCancellationV2>
    ) -> Result<
        PluginContracts::ApiContract::PluginEndpointResponse,
        PluginContracts::ApiContract::PluginEndpointError
    > {
        let call = move |runtime: &PluginRuntime, context: PluginCallContext| {
            let request_bytes = serde_json
                ::to_vec(&request)
                .map_err(|error| format!("failed to encode plugin request: {error}"))?;
            runtime.with_executor(|executor| executor.handle_endpoint(&request_bytes, &context))
        };

        let result = match parent_cancellation {
            Some(parent) => self.call_with_timeout_parent("handle_endpoint", parent, call),
            None => self.call_with_timeout("handle_endpoint", call),
        };

        result.map_err(|error| {
            match error {
                PluginCallFailure::Timeout(timeout) =>
                    PluginContracts::ApiContract::PluginEndpointError::new(
                        504,
                        "plugin_timeout",
                        "The plugin did not respond within the configured timeout",
                        Some(format!("plugin call timeout: {:?}", timeout))
                    ),
                PluginCallFailure::Busy =>
                    PluginContracts::ApiContract::PluginEndpointError::new(
                        503,
                        PLUGIN_OVERLOADED_CODE,
                        PLUGIN_OVERLOADED_USER_MESSAGE,
                        None
                    ),
                PluginCallFailure::Failed(message) =>
                    PluginContracts::ApiContract::PluginEndpointError::internal(
                        "plugin_abi_error",
                        "The plugin ABI call failed",
                        message
                    ),
            }
        })?
    }

    pub(crate) fn on_session_closed_with_parent(
        self: &Arc<Self>,
        context: &PluginContracts::ApiContract::PluginSessionContext,
        parent_cancellation: Option<PluginCancellationV2>
    ) -> Result<(), PluginContracts::ApiContract::PluginLifecycleError> {
        let session_context = context.clone();
        let call = move |runtime: &PluginRuntime, call_context: PluginCallContext| {
            let request_bytes = serde_json
                ::to_vec(&session_context)
                .map_err(|error| format!("failed to encode session lifecycle request: {error}"))?;
            runtime.with_executor(|executor|
                executor.on_session_closed(&request_bytes, &call_context)
            )
        };

        let result = match parent_cancellation {
            Some(parent) => self.call_with_timeout_parent("on_session_closed", parent, call),
            None => self.call_with_timeout("on_session_closed", call),
        };

        result.map_err(|error| {
            match error {
                PluginCallFailure::Timeout(_) =>
                    PluginContracts::ApiContract::PluginLifecycleError::new(
                        "plugin_timeout",
                        "Plugin session cleanup exceeded the configured timeout"
                    ),
                PluginCallFailure::Busy =>
                    PluginContracts::ApiContract::PluginLifecycleError::new(
                        "plugin_overloaded",
                        "Plugin session cleanup was rejected because the plugin is overloaded"
                    ),
                PluginCallFailure::Failed(message) =>
                    PluginContracts::ApiContract::PluginLifecycleError::new(
                        "plugin_abi_error",
                        message
                    ),
            }
        })?
    }

    /// Executes a database ABI call while preserving the datasource's serialized response.
    /// Request ownership and the response ABI buffer are handled entirely inside the host; no
    /// Rust `DatabaseResponse` graph is constructed for successful large query results.
    pub(crate) fn execute_database_bytes_with_parent(
        self: &Arc<Self>,
        request_bytes: Vec<u8>,
        parent_cancellation: PluginCancellationV2
    ) -> Result<Vec<u8>, DatabaseContracts::DatabaseError> {
        let call = move |runtime: &PluginRuntime, context: PluginCallContext| {
            runtime.with_executor(|executor|
                executor.execute_database_bytes(&request_bytes, &context)
            )
        };

        self.call_with_timeout_parent("execute_database", parent_cancellation, call).map_err(
            |error| {
                match error {
                    PluginCallFailure::Timeout(timeout) =>
                        DatabaseContracts::DatabaseError::new(
                            DatabaseContracts::DatabaseErrorKind::Query,
                            format!(
                                "plugin database call timed out after {:?}; cooperative cancellation requested",
                                timeout
                            )
                        ),
                    PluginCallFailure::Busy =>
                        DatabaseContracts::DatabaseError::new(
                            DatabaseContracts::DatabaseErrorKind::Query,
                            "plugin database worker or queue capacity reached"
                        ),
                    PluginCallFailure::Failed(message) =>
                        DatabaseContracts::DatabaseError::new(
                            DatabaseContracts::DatabaseErrorKind::Decode,
                            message
                        ),
                }
            }
        )
    }

    pub(crate) fn execute_database_with_parent(
        self: &Arc<Self>,
        request: DatabaseContracts::DatabaseRequest,
        parent_cancellation: Option<PluginCancellationV2>
    ) -> Result<DatabaseContracts::DatabaseResponse, DatabaseContracts::DatabaseError> {
        let call = move |runtime: &PluginRuntime, context: PluginCallContext| {
            let request_bytes = serde_json
                ::to_vec(&request)
                .map_err(|error| format!("failed to encode database request: {error}"))?;
            let response: Result<
                DatabaseContracts::DatabaseResponse,
                DatabaseContracts::DatabaseError
            > = runtime.with_executor(|executor|
                executor.execute_database(&request_bytes, &context)
            )?;
            response.map_err(|error| format!("plugin database operation failed: {error}"))
        };

        let result = match parent_cancellation {
            Some(parent) => self.call_with_timeout_parent("execute_database", parent, call),
            None => self.call_with_timeout("execute_database", call),
        };

        result.map_err(|error| {
            match error {
                PluginCallFailure::Timeout(timeout) =>
                    DatabaseContracts::DatabaseError::new(
                        DatabaseContracts::DatabaseErrorKind::Query,
                        format!(
                            "plugin database call timed out after {:?}; cooperative cancellation requested",
                            timeout
                        )
                    ),
                PluginCallFailure::Busy =>
                    DatabaseContracts::DatabaseError::new(
                        DatabaseContracts::DatabaseErrorKind::Query,
                        "plugin database worker or queue capacity reached"
                    ),
                PluginCallFailure::Failed(message) =>
                    DatabaseContracts::DatabaseError::new(
                        DatabaseContracts::DatabaseErrorKind::Decode,
                        message
                    ),
            }
        })
    }
}
