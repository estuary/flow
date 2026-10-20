//! Shared machinery of connectors which run user-authored TypeScript projects
//! under Deno: derive-typescript, and (in time) capture-typescript and
//! materialize-typescript.

mod config_types;
mod project;
pub mod ts_types;

pub use config_types::config_types_ts;
pub use project::{DENO_MISSING, ENTRY, GENERATED_PREFIX, Project};

/// Path of the platform's protocol types within every project, beside its
/// generated types. A connector's own protocol messages import them.
pub const ESTUARY_PATH: &str = "flow_generated/typescript/estuary.ts";
pub const ESTUARY_TS: &str = include_str!("estuary.ts");
