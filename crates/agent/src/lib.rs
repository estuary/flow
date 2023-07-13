mod connector_tags;
mod directives;
mod discovers;
mod derivation_previews;
pub(crate) mod draft;
pub(crate) mod evolution;
mod handlers;
mod jobs;
pub mod logs;
pub(crate) mod publications;

pub use agent_sql::{CatalogType, Id};
pub use connector_tags::TagHandler;
pub use directives::DirectiveHandler;
pub use discovers::DiscoverHandler;
pub use derivation_previews::DerivationPreviewHandler;
pub use evolution::EvolutionHandler;
pub use handlers::{serve, Handler, HandlerStatus};
pub use publications::PublishHandler;

// Used during tests.
#[cfg(test)]
const FIXED_DATABASE_URL: &str = "postgresql://postgres:postgres@localhost:5432/postgres";
