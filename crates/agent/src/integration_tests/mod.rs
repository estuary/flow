//! # Control plane integration tests
//!
//! These tests cover end-to-end scenarios involving the control plane. The data plane and
//! connectors are not exercised as part of these.
mod auto_discovers;
mod dependencies_and_activations;
mod evolutions;
pub mod harness;
mod locking_retries;
mod null_bytes;
mod periodic_publications;
mod quotas;
mod schema_evolution;
mod source_captures;
mod spec_updates;
mod unknown_connectors;
mod user_discovers;
mod user_publications;

fn spec_fixture() -> proto_flow::capture::response::Spec {
    proto_flow::capture::response::Spec {
        config_schema_json: r#"{"type": "object"}"#.to_string(),
        resource_config_schema_json: r#"{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}}}"#.to_string(),
        resource_path_pointers: vec!["/id".to_string()],
        ..Default::default()
    }
}
