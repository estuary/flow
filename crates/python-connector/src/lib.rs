//! Shared machinery of connectors which run user-authored Python projects:
//! capture-python, derive-python, and (in time) materialize-python.

mod config;
mod config_types;
mod generated;
mod project;
pub mod pydantic;
mod spec;

pub use config::{
    Files, SENTINEL, split_sentinel, strip_sentinel_at, text_files, without_sentinel,
};
pub use config_types::{ConfigTypes, Resources, config_types_py};
pub use generated::{ENTRY, TASK_PACKAGE, imports_py, load_task_py, module_files, module_parts};
pub use project::{GENERATED_PREFIX, Install, LOCK_FILE, PYPROJECT, Project, Use, relative_to};
pub use spec::Spec;

/// Protocol version of connector Spec responses.
pub const PROTOCOL: u32 = 3032023;
