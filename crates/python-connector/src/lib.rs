//! Shared machinery of connectors which run user-authored Python projects:
//! derive-python, and (in time) capture-python and materialize-python.

mod config_types;
mod generated;
mod project;
pub mod pydantic;

pub use config_types::config_types_py;
pub use generated::{ENTRY, TASK_PACKAGE, imports_py, load_task_py, module_files, module_parts};
pub use project::{GENERATED_PREFIX, Install, Project};
