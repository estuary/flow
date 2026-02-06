//! # Control plane integration tests
//!
//! These tests cover end-to-end scenarios involving the control plane. The data plane and
//! connectors are not exercised as part of these.
mod alerts;
mod auto_discovers;
mod collection_resets;
mod config_updates;
mod dependencies_and_activations;
mod graphql;
pub mod harness;
mod inferred_schemas;
mod locking_retries;
mod null_bytes;
mod periodic_publications;
mod quotas;
mod republish;
mod shard_failures;
mod source_captures;
mod tenant_alerts;
mod unknown_connectors;
mod user_discovers;
mod user_publications;

fn spec_fixture() -> proto_flow::capture::response::Spec {
    proto_flow::capture::response::Spec {
        config_schema_json: r#"{"type": "object"}"#.into(),
        resource_config_schema_json: r#"{"type": "object", "properties": {"id": {"type": "string", "x-collection-name": true}}}"#.into(),
        resource_path_pointers: vec!["/id".to_string()],
        ..Default::default()
    }
}
