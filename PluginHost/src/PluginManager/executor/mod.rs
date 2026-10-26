//! Concrete plugin execution backends.
mod in_process;
pub(super) use self::in_process::load_plugin;
