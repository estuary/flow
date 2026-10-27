//! Shared machinery of connectors which run user-authored Python projects:
//! capture-python, derive-python, and materialize-python.

mod config;
mod config_types;
#[cfg(feature = "staged")]
mod connector;
mod generated;
pub mod pydantic;
#[cfg(feature = "staged")]
mod staged;

pub use config::{
    CDK_DEPENDENCY, LOCK, MANIFEST, ModuleConfig, Project, ProjectConfig, RESERVED, SENTINEL,
    entry, starter_manifest,
};
pub use config_types::{config_shape, endpoint_config_py, query_config};
#[cfg(feature = "staged")]
pub use connector::{DOCUMENTATION_URL, declared_spec, main, oauth2, project_urls, starter_files};
pub use generated::{
    ENTRY, GENERATED_PREFIX, imports_py, load_task_py, module_files, module_parts,
};
#[cfg(feature = "staged")]
pub use staged::{Exited, Install, Staged};

/// Protocol version of a connector's Spec responses.
pub const PROTOCOL: u32 = 3032023;
