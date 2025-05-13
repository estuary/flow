use data_plane_controller::{controller, stack};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

mod util;
use util::{initial_state, mock_emit_log_fn, mock_run_cmd_fn, TraceEntry};

#[tokio::test]
async fn test_validate_state() {
    let trace = Arc::new(Mutex::new(Vec::new()));

    let controller = data_plane_controller::Controller {
        dns_ttl: std::time::Duration::ZERO,
        dry_dock_remote: "git@github.com:estuary/est-dry-dock.git".to_string(),
        ops_remote: "git@github.com:estuary/ops.git".to_string(),
        secrets_provider: "testing".to_string(),
        state_backend: "file:///tmp/pulumi-test-state".parse().unwrap(),
        emit_log_fn: mock_emit_log_fn(trace.clone()),
        run_cmd_fn: mock_run_cmd_fn(trace.clone()),
    };

    let mut state: Option<stack::State> = None;
    let mut inbox: VecDeque<(models::Id, Option<controller::Message>)> = VecDeque::new();
    let mut checkouts: HashMap<String, tempfile::TempDir> = HashMap::new();
    let mut row_state = initial_state();
    row_state.stack.config.model.deployments[0].oci_image = "quay.io/coreos/etcd:v0.0".to_string();
    row_state.stack.config.model.deployments[1].oci_image =
        "ghcr.io/estuary/flow:v0.0.0".to_string();
    row_state.stack.config.model.deployments[2].oci_image =
        "ghcr.io/gazette/broker:v0.0.0".to_string();

    inbox.push_back((
        models::Id::zero(),
        Some(controller::Message::Start(row_state.data_plane_id)),
    ));
    inbox.push_back((models::Id::zero(), Some(controller::Message::Enable)));

    let outcome = controller
        .on_poll(
            models::Id::new([42; 8]), // Task ID.
            &mut state,
            &mut inbox,
            &mut checkouts,
            Vec::new(),
            row_state.clone(),
        )
        .await;

    insta::assert_json_snapshot!(trace.lock().unwrap().as_slice());

    if let Err(e) = outcome {
        assert_eq!(
            e.to_string(),
            r#"failed to validate data-plane state: 
"quay.io/coreos/etcd:v0.0" is not one of ["quay.io/coreos/etcd:v3.5","ghcr.io/estuary/flow:v2.3.4","ghcr.io/gazette/broker:v1.2.3","ghcr.io/gazette/broker:v5.6.7"] at /stack/config/est-dry-dock:model/deployments/0/oci_image
"ghcr.io/estuary/flow:v0.0.0" is not one of ["quay.io/coreos/etcd:v3.5","ghcr.io/estuary/flow:v2.3.4","ghcr.io/gazette/broker:v1.2.3","ghcr.io/gazette/broker:v5.6.7"] at /stack/config/est-dry-dock:model/deployments/1/oci_image
"ghcr.io/gazette/broker:v0.0.0" is not one of ["quay.io/coreos/etcd:v3.5","ghcr.io/estuary/flow:v2.3.4","ghcr.io/gazette/broker:v1.2.3","ghcr.io/gazette/broker:v5.6.7"] at /stack/config/est-dry-dock:model/deployments/2/oci_image"#
        );
    } else {
        panic!("expected validation error");
    }
}
