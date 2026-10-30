//! Internal Factory database capability operations.
use super::*;

impl PluginHostInstance {
    pub fn check_database_readiness(&self) -> Result<(), String> {
        let request = DatabaseContracts::DatabaseRequest::Query(DatabaseContracts::QueryRequest {
            statement: "SELECT 1".to_owned(),
            parameters: Vec::new(),
            columns: Vec::new(),
        });
        let result = self.with_database_plugin(|plugin|
            plugin.execute_database(request, &PluginCancellation::uncancelled())
        )?;
        let result = result.map_err(|error| error.to_string())?;
        match result {
            DatabaseContracts::DatabaseResponse::Query(rows) if rows.rows.len() == 1 => Ok(()),
            DatabaseContracts::DatabaseResponse::Query(rows) =>
                Err(
                    format!(
                        "database connectivity query returned {} rows instead of 1",
                        rows.rows.len()
                    )
                ),
            DatabaseContracts::DatabaseResponse::Execute(_) =>
                Err("database connectivity probe returned an execution result".to_owned()),
        }
    }

    pub fn has_database_plugin(&self) -> bool {
        let manager = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        !matches!(manager.database_plugin_selection(), DatabasePluginSelection::None)
    }

    pub fn with_database_plugin<R, F>(&self, func: F) -> Result<R, String>
        where F: FnOnce(&dyn IPlugin) -> R
    {
        let selection = self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .database_plugin_selection();
        match selection {
            DatabasePluginSelection::None =>
                Err("no database data-source plugin is loaded".to_owned()),
            DatabasePluginSelection::Unique { runtime } => Ok(func(&PluginProxy::new(runtime))),
            DatabasePluginSelection::Multiple { names } =>
                Err(
                    format!(
                        "multiple database data-source plugins are loaded: {}",
                        names.join(", ")
                    )
                ),
        }
    }

    pub fn execute_database_raw(
        &self,
        request_bytes: Vec<u8>,
        cancellation: PluginAbi::PluginCancellationV2
    ) -> Result<Vec<u8>, DatabaseContracts::DatabaseError> {
        self.database_dispatcher().execute_database_raw(request_bytes, cancellation)
    }

    pub fn load_database_plugins(&self) -> Result<usize, String> {
        self.load_database_plugins_with_config(self.config.runtime)
    }

    pub fn load_database_plugins_with_config(
        &self,
        config: PluginRuntimeConfig
    ) -> Result<usize, String> {
        let database_root = self.config.plugin_root.join("database");
        let entries = fs
            ::read_dir(&database_root)
            .map_err(|error|
                format!(
                    "failed to read database plugin root '{}': {error}",
                    database_root.display()
                )
            )?;
        let mut directories = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error|
                format!(
                    "failed to inspect database plugin root '{}': {error}",
                    database_root.display()
                )
            )?;
            let path = entry.path();
            if path.is_dir() {
                directories.push(path);
            }
        }
        directories.sort();
        let mut loaded_count = 0usize;
        for path in directories {
            let path_text = path.to_string_lossy().into_owned();
            let report = self
                .load_plugins_with_policy_and_config(
                    &path_text,
                    PluginLoadPolicy::optional_all(),
                    config
                )
                .map_err(|error|
                    format!("failed to load database plugins from '{}': {error}", path_text)
                )?;
            loaded_count += report.loaded_count();
        }
        Ok(loaded_count)
    }
}
