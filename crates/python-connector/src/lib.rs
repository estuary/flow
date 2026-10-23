//! Shared machinery of connectors which run user-authored Python projects:
//! capture-python, derive-python, and (in time) materialize-python.

mod config_types;
mod generated;
mod project;
pub mod pydantic;

pub use config_types::{ConfigTypes, Resources, config_types_py};
pub use generated::{ENTRY, TASK_PACKAGE, imports_py, load_task_py, module_files, module_parts};
pub use project::{GENERATED_PREFIX, Install, Project, Use};
