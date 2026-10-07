//! Control-plane parity of python captures, which have no connector tag and
//! run as the built-in capture-python image connector.

use super::config_updates::upsert_config_update;
use crate::integration_tests::harness::{TestHarness, draft_catalog};
use models::{CaptureEndpoint, CatalogType, status::ShardRef};
use proto_flow::capture::response::{Discovered, discovered::Binding};
use serde_json::json;

const CAPTURE: &str = "pythons/source-acme";

fn python_files() -> serde_json::Value {
    json!({
        "pyproject.toml": "[project]\nname = \"source-acme\"\n",
        "source_acme/__init__.py": "",
        "source_acme/__main__.py": "print('hello')\n",
    })
}

/// Declared `spec` of the capture, which discovers and config updates keep.
fn python_spec() -> serde_json::Value {
    json!({
        "configSchema": {
            "type": "object",
            "properties": {"region": {"type": "string", "default": "north"}},
        },
        "resourceConfigSchema": {
            "type": "object",
            "properties": {"id": {"type": "string", "x-collection-name": true}},
            "required": ["id"],
        },
    })
}

fn python_capture(config: serde_json::Value) -> serde_json::Value {
    json!({
        "autoDiscover": {
            "addNewBindings": true,
            "evolveIncompatibleCollections": true,
        },
        "endpoint": {
            "python": {
                "files": python_files(),
                "config": config,
                "spec": python_spec(),
            }
        },
        "bindings": [],
    })
}

/// Spec response of the capture-python connector, which answers from the
/// declared `spec` and has no resource path pointers.
fn connector_spec() -> proto_flow::capture::response::Spec {
    let spec = python_spec();
    proto_flow::capture::response::Spec {
        config_schema_json: spec["configSchema"].to_string().into(),
        resource_config_schema_json: spec["resourceConfigSchema"].to_string().into(),
        resource_path_pointers: Vec::new(),
        ..Default::default()
    }
}

/// Discovered bindings, each of which carries its own resource path.
fn discovered(names: &[&str]) -> Discovered {
    Discovered {
        bindings: names
            .iter()
            .map(|name| Binding {
                recommended_name: name.to_string(),
                resource_config_json: json!({"id": name}).to_string().into(),
                document_schema_json: json!({
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                    "required": ["id"],
                })
                .to_string()
                .into(),
                key: vec!["/id".to_string()],
                disable: false,
                resource_path: vec![name.to_string()],
                is_fallback_key: false,
            })
            .collect(),
    }
}

fn python_endpoint(state: &crate::controllers::ControllerState) -> models::CapturePython {
    let capture = state.live_spec.as_ref().unwrap().as_capture().unwrap();
    let CaptureEndpoint::Python(python) = &capture.endpoint else {
        panic!("expected python endpoint, got: {:?}", capture.endpoint);
    };
    python.clone()
}

#[tokio::test]
async fn test_python_capture_activation_and_auto_discover() {
    let mut harness = TestHarness::init("test_python_capture_activation_and_auto_discover").await;
    let user_id = harness.setup_tenant("pythons").await;

    let result = harness
        .user_publication(
            user_id,
            "initial publication",
            draft_catalog(json!({
                "captures": { CAPTURE: python_capture(json!({"region": "north"})) }
            })),
        )
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );

    harness.run_pending_controllers(None).await;
    // A python capture runs as an image connector, and has shards to activate.
    harness
        .control_plane()
        .assert_activated("initial publication", CAPTURE, CatalogType::Capture);

    // Having no connector tag, the auto-discover interval is a platform constant.
    let state = harness.get_controller_state(CAPTURE).await;
    let status = state.current_status.unwrap_capture();
    let next_at = status.auto_discover.as_ref().unwrap().next_at.unwrap();
    assert_eq!(chrono::Duration::hours(2), next_at - state.created_at);

    harness
        .connectors
        .mock_discover(CAPTURE, Ok((connector_spec(), discovered(&["widgets"]))));
    harness.set_auto_discover_due(CAPTURE).await;
    let state = harness.run_pending_controller(CAPTURE).await;

    let status = state.current_status.unwrap_capture();
    let auto_discover = status.auto_discover.as_ref().unwrap();
    assert!(
        auto_discover.failure.is_none(),
        "unexpected failure: {:?}",
        auto_discover.failure
    );
    let success = auto_discover.last_success.as_ref().unwrap();
    assert_eq!(1, success.added.len());
    assert!(success.publish_result.as_ref().unwrap().is_success());

    // The Discover dialed the capture-python image, with the capture's code
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
    assert_eq!(python_files(), config[validation::PYTHON_SENTINEL]["files"]);

    // The discovered binding was published, and the endpoint is unchanged.
    let state = harness.get_controller_state(CAPTURE).await;
    let capture = state.live_spec.as_ref().unwrap().as_capture().unwrap();
    assert_eq!(1, capture.bindings.len());
    assert_eq!("pythons/widgets", capture.bindings[0].target.as_str());

    let python = python_endpoint(&state);
    assert_eq!(json!({"region": "north"}), python.config.to_value());
    assert_eq!(python_files(), serde_json::to_value(&python.files).unwrap());
    assert_eq!(python_spec(), serde_json::to_value(&python.spec).unwrap());
}

#[tokio::test]
async fn test_python_capture_user_discovers() {
    let mut harness = TestHarness::init("test_python_capture_user_discovers").await;
    let user_id = harness.setup_tenant("pythons").await;

    // A discover of a python capture carries its `config`, and takes its
    // `files` from the drafted capture.
    let draft_id = harness
        .create_draft(
            user_id,
            "python discover",
            draft_catalog(json!({
                "captures": { CAPTURE: python_capture(json!({"region": "north"})) }
            })),
        )
        .await;
    let discover_id = harness
        .queue_python_discover(
            CAPTURE,
            draft_id,
            r#"{"region": "south"}"#,
            false,
            Ok((connector_spec(), discovered(&["widgets", "gadgets"]))),
        )
        .await;
    let result = harness.run_queued_discover(discover_id).await;
    assert!(
        result.job_status.is_success(),
        "unexpected status: {:?}, errors: {:?}",
        result.job_status,
        result.errors
    );
    assert_eq!(2, result.draft.collections.len());

    let model = result
        .draft
        .captures
        .get_by_key(&models::Capture::new(CAPTURE))
        .unwrap()
        .model
        .as_ref()
        .unwrap();
    let CaptureEndpoint::Python(python) = &model.endpoint else {
        panic!("expected python endpoint, got: {:?}", model.endpoint);
    };
    assert_eq!(json!({"region": "south"}), python.config.to_value());
    assert_eq!(python_files(), serde_json::to_value(&python.files).unwrap());
    assert_eq!(python_spec(), serde_json::to_value(&python.spec).unwrap());
    assert_eq!(2, model.bindings.len());

    // A second discover against the same draft matches its bindings by the
    // `/_meta/path` which the first discover recorded, as the connector has
    // no resource path pointers, and adds nothing.
    let discover_id = harness
        .queue_python_discover(
            CAPTURE,
            draft_id,
            r#"{"region": "south"}"#,
            false,
            Ok((connector_spec(), discovered(&["widgets", "gadgets"]))),
        )
        .await;
    let again = harness.run_queued_discover(discover_id).await;
    assert!(
        again.job_status.is_success(),
        "unexpected status: {:?}, errors: {:?}",
        again.job_status,
        again.errors
    );
    let again_model = again
        .draft
        .captures
        .get_by_key(&models::Capture::new(CAPTURE))
        .unwrap()
        .model
        .as_ref()
        .unwrap();
    assert_eq!(model.bindings, again_model.bindings);
    assert!(
        model
            .bindings
            .iter()
            .all(|binding| binding.resource.get().contains(r#""_meta":{"path":"#)),
        "{:?}",
        model.bindings
    );

    let request = harness.connectors.last_discover_request(CAPTURE).unwrap();
    let connector: models::ConnectorConfig = serde_json::from_slice(&request.config_json).unwrap();
    assert!(
        connector
            .image
            .starts_with(validation::CAPTURE_PYTHON_IMAGE)
    );
    assert_eq!(json!("south"), connector.config.to_value()["region"]);

    let pub_result = harness
        .create_user_publication(user_id, draft_id, "publish discovered")
        .await;
    assert!(
        pub_result.status.is_success(),
        "publication failed: {:?}",
        pub_result.errors
    );

    // Once live, a python capture is re-discovered from an empty draft.
    let draft_id = harness
        .create_draft(user_id, "python re-discover", Default::default())
        .await;
    let discover_id = harness
        .queue_python_discover(
            CAPTURE,
            draft_id,
            r#"{"region": "east"}"#,
            true,
            Ok((connector_spec(), discovered(&["widgets", "gadgets"]))),
        )
        .await;
    let result = harness.run_queued_discover(discover_id).await;
    assert!(
        result.job_status.is_success(),
        "unexpected status: {:?}, errors: {:?}",
        result.job_status,
        result.errors
    );
    let model = result
        .draft
        .captures
        .get_by_key(&models::Capture::new(CAPTURE))
        .unwrap()
        .model
        .as_ref()
        .unwrap();
    let CaptureEndpoint::Python(python) = &model.endpoint else {
        panic!("expected python endpoint, got: {:?}", model.endpoint);
    };
    assert_eq!(json!({"region": "east"}), python.config.to_value());
    assert_eq!(python_files(), serde_json::to_value(&python.files).unwrap());

    // A discover without a connector tag, of a capture which is neither
    // drafted nor live, has no python code to run.
    let draft_id = harness
        .create_draft(user_id, "no python capture", Default::default())
        .await;
    let discover_id = harness
        .queue_python_discover(
            "pythons/source-missing",
            draft_id,
            r#"{}"#,
            false,
            Ok((connector_spec(), discovered(&["widgets"]))),
        )
        .await;
    let result = harness.run_queued_discover(discover_id).await;
    assert!(
        matches!(
            result.job_status,
            crate::discovers::JobStatus::DiscoverFailed
        ),
        "unexpected status: {:?}",
        result.job_status
    );
    insta::assert_debug_snapshot!(result.errors, @r#"
    [
        (
            "flow://capture/pythons/source-missing",
            "a discover without a connector tag must be of a python capture, which must be drafted or already published",
        ),
    ]
    "#);
}

#[tokio::test]
async fn test_python_capture_config_updates() {
    let mut harness = TestHarness::init("test_python_capture_config_updates").await;
    let user_id = harness.setup_tenant("pythons").await;

    let mut capture = python_capture(json!({"credentials": {"refresh_token": "initial"}}));
    capture["autoDiscover"] = serde_json::Value::Null;
    // A `secrets` stanza requires the V2 runtime.
    capture["shards"] = json!({"flags": {"enable-runtime-v2": "true"}});

    let result = harness
        .user_publication(
            user_id,
            "initial publication",
            draft_catalog(json!({ "captures": { CAPTURE: capture } })),
        )
        .await;
    assert!(
        result.status.is_success(),
        "publication failed: {:?}",
        result.errors
    );
    let states = harness.run_pending_controllers(None).await;
    let state = states.iter().find(|s| s.catalog_name == CAPTURE).unwrap();
    let shard = ShardRef {
        name: CAPTURE.to_string(),
        build: state.last_build_id,
        key_begin: "00000000".to_string(),
        r_clock_begin: "00000000".to_string(),
    };

    // An update of /task/update-config maps onto `endpoint.python.config`
    // and the `secrets` stanza, and an echoed `_python` sentinel is removed.
    upsert_config_update(
        &mut harness,
        &shard,
        json!({
            "credentials": {"refresh_token": "rotated"},
            "_python": {"files": {"injected.py": ""}},
        }),
        Some(json!({"pythons/credentials": "/credentials"})),
    )
    .await;
    let state = harness.run_pending_controller(CAPTURE).await;
    assert!(state.error.is_none(), "unexpected error: {:?}", state.error);

    let python = python_endpoint(&state);
    assert_eq!(
        json!({"credentials": {"refresh_token": "rotated"}}),
        python.config.to_value()
    );
    assert_eq!(python_files(), serde_json::to_value(&python.files).unwrap());
    let capture = state.live_spec.as_ref().unwrap().as_capture().unwrap();
    assert_eq!(
        json!({"pythons/credentials": "/credentials"}),
        serde_json::to_value(&capture.secrets).unwrap()
    );

    // A legacy `configUpdate` event, having no `secrets`, is rejected.
    let shard = ShardRef {
        build: state.last_build_id,
        ..shard
    };
    upsert_config_update(
        &mut harness,
        &shard,
        json!({"credentials": {"refresh_token": "legacy"}, "sops": {}}),
        None,
    )
    .await;
    let state = harness.run_pending_controller(CAPTURE).await;
    let error = state.error.as_deref().unwrap_or_default();
    assert!(
        error.contains("python tasks cannot apply a legacy `configUpdate` event"),
        "unexpected error: {error}"
    );
    assert_eq!(
        json!({"credentials": {"refresh_token": "rotated"}}),
        python_endpoint(&state).config.to_value()
    );
}
