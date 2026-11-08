//! Controller handling of python captures and materializations, which have
//! no connector tag and run as the capture-python and materialize-python images.

use super::config_updates::upsert_config_update;
use crate::integration_tests::harness::{TestHarness, draft_catalog};
use models::{CaptureEndpoint, CatalogType, MaterializationEndpoint, status::ShardRef};
use proto_flow::capture::response::{Discovered, discovered::Binding};
use serde_json::json;

const CAPTURE: &str = "pythons/source-acme";
const MATERIALIZATION: &str = "pythons/materialize-acme";

/// Files of a python task: its manifest and entry point.
fn python_files(name: &str) -> serde_json::Value {
    let mut files = serde_json::Map::new();
    files.insert(
        "pyproject.toml".to_string(),
        json!(format!("[project]\nname = \"{name}\"\n")),
    );
    files.insert(format!("{name}/__init__.py"), json!("pass\n"));
    files.into()
}

fn python_capture(config: serde_json::Value) -> serde_json::Value {
    json!({
        "endpoint": {"python": {"files": python_files("source-acme"), "config": config}},
        "bindings": [],
        // A `secrets` stanza requires the V2 runtime.
        "shards": {"flags": {"enable-runtime-v2": "true"}},
    })
}

fn python_materialization(config: serde_json::Value) -> serde_json::Value {
    json!({
        "endpoint": {"python": {"files": python_files("materialize-acme"), "config": config}},
        "bindings": [],
        // A python materialization requires the V2 runtime.
        "shards": {"flags": {"enable-runtime-v2": "true"}},
    })
}

/// Returns the `config` and `secrets` of a live python task.
fn python_config_and_secrets(
    state: &crate::controllers::ControllerState,
) -> (serde_json::Value, serde_json::Value) {
    match state.live_spec.as_ref().unwrap() {
        models::AnySpec::Capture(capture) => {
            let CaptureEndpoint::Python(python) = &capture.endpoint else {
                panic!("expected python endpoint, got: {:?}", capture.endpoint);
            };
            (
                python.config.to_value(),
                serde_json::to_value(&capture.secrets).unwrap(),
            )
        }
        models::AnySpec::Materialization(materialization) => {
            let MaterializationEndpoint::Python(python) = &materialization.endpoint else {
                panic!(
                    "expected python endpoint, got: {:?}",
                    materialization.endpoint
                );
            };
            (
                python.config.to_value(),
                serde_json::to_value(&materialization.secrets).unwrap(),
            )
        }
        spec => panic!("expected a capture or materialization, got: {spec:?}"),
    }
}

#[tokio::test]
async fn test_python_tasks_activation_and_auto_discover() {
    let mut harness = TestHarness::init("test_python_tasks_activation_and_auto_discover").await;
    let user_id = harness.setup_tenant("pythons").await;

    let mut capture = python_capture(json!({"region": "north"}));
    capture["autoDiscover"] = json!({"addNewBindings": true});

    let result = harness
        .user_publication(
            user_id,
            "initial publication",
            draft_catalog(json!({
                "captures": { CAPTURE: capture },
                "materializations": { MATERIALIZATION: python_materialization(json!({})) },
            })),
        )
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );

    harness.run_pending_controllers(None).await;
    // Python tasks run as image connectors, and have shards to activate.
    harness.control_plane().assert_activations(
        "initial publication",
        vec![
            (CAPTURE, Some(CatalogType::Capture)),
            (MATERIALIZATION, Some(CatalogType::Materialization)),
        ],
    );

    // Having no connector tag, the auto-discover interval is a platform constant.
    let state = harness.get_controller_state(CAPTURE).await;
    let status = state.current_status.unwrap_capture();
    let next_at = status.auto_discover.as_ref().unwrap().next_at.unwrap();
    assert_eq!(chrono::Duration::hours(2), next_at - state.created_at);

    let spec = proto_flow::capture::response::Spec {
        config_schema_json: json!({"type": "object"}).to_string().into(),
        resource_config_schema_json: json!({
            "type": "object",
            "properties": {"id": {"type": "string", "x-collection-name": true}},
            "required": ["id"],
        })
        .to_string()
        .into(),
        ..Default::default()
    };
    let discovered = Discovered {
        bindings: vec![Binding {
            recommended_name: "widgets".to_string(),
            resource_config_json: json!({"id": "widgets"}).to_string().into(),
            document_schema_json: json!({
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "required": ["id"],
            })
            .to_string()
            .into(),
            key: vec!["/id".to_string()],
            disable: false,
            resource_path: vec!["widgets".to_string()],
            is_fallback_key: false,
        }],
    };
    harness
        .connectors
        .mock_discover(CAPTURE, Ok((spec, discovered)));
    harness.set_auto_discover_due(CAPTURE).await;
    harness.run_pending_controller(CAPTURE).await;

    // The Discover dialed the capture-python image, with the capture's files
    // pushed down into its configuration.
    let request = harness.connectors.last_discover_request(CAPTURE).unwrap();
    assert_eq!(
        proto_flow::flow::capture_spec::ConnectorType::Image as i32,
        request.connector_type
    );
    let connector: models::ConnectorConfig = serde_json::from_slice(&request.config_json).unwrap();
    assert_eq!(
        format!("{}:stable", validation::CAPTURE_PYTHON_IMAGE),
        connector.image
    );
    let config = connector.config.to_value();
    assert_eq!(json!("north"), config["region"]);
    assert_eq!(
        python_files("source-acme"),
        config[python_connector::SENTINEL]["files"]
    );

    let state = harness.get_controller_state(CAPTURE).await;
    let capture = state.live_spec.as_ref().unwrap().as_capture().unwrap();
    assert_eq!(1, capture.bindings.len());
    assert_eq!("pythons/widgets", capture.bindings[0].target.as_str());
}

#[tokio::test]
async fn test_python_tasks_config_updates() {
    let mut harness = TestHarness::init("test_python_tasks_config_updates").await;
    let user_id = harness.setup_tenant("pythons").await;

    let initial = json!({"credentials": {"refresh_token": "initial"}});
    let result = harness
        .user_publication(
            user_id,
            "initial publication",
            draft_catalog(json!({
                "captures": { CAPTURE: python_capture(initial.clone()) },
                "materializations": { MATERIALIZATION: python_materialization(initial) },
            })),
        )
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    harness.run_pending_controllers(None).await;

    // An update of /task/update-config maps onto `endpoint.python.config`
    // and the `secrets` stanza.
    for name in [CAPTURE, MATERIALIZATION] {
        let state = harness.get_controller_state(name).await;
        let shard = ShardRef {
            name: name.to_string(),
            build: state.last_build_id,
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
        };
        upsert_config_update(
            &mut harness,
            &shard,
            json!({"credentials": {"refresh_token": "rotated"}}),
            Some(json!({"pythons/credentials": "/credentials"})),
        )
        .await;
        let state = harness.run_pending_controller(name).await;
        assert!(state.error.is_none(), "{name}: {:?}", state.error);

        assert_eq!(
            (
                json!({"credentials": {"refresh_token": "rotated"}}),
                json!({"pythons/credentials": "/credentials"}),
            ),
            python_config_and_secrets(&state),
            "{name}"
        );
    }
}
