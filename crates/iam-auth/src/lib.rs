pub mod config;
pub mod providers;
pub mod schema;
pub mod tokens;

// Re-export main types and functions for convenient access
pub use config::{AWSConfig, AzureConfig, GCPConfig, IAMAuthConfig};
pub use schema::extract_iam_auth_from_connector_config;
pub use tokens::{AWSTokens, AzureTokens, GCPTokens, IAMTokens};
