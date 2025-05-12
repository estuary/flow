use data_plane_controller::{controller, stack};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

mod util;
use util::{initial_state, mock_emit_log_fn, mock_run_cmd_fn, TraceEntry};

#[tokio::test]
async fn test() {
    let trace = Arc::new(Mutex::new(Vec::new()));

    let controller = data_plane_controller::Controller {
        dns_ttl: std::time::Duration::ZERO,
        infra_remote: "git@github.com:estuary/est-dry-dock.git".to_string(),
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

    inbox.push_back((
        models::Id::zero(),
        Some(controller::Message::Start(row_state.data_plane_id)),
    ));
    inbox.push_back((models::Id::zero(), Some(controller::Message::Enable)));

    // An immediate release that's applied to the initial row state fixture.
    let releases: Vec<stack::Release> = vec![stack::Release {
        prev_image: "ghcr.io/gazette/broker:v1.2.3".to_string(),
        next_image: "ghcr.io/gazette/broker:v5.6.7".to_string(),
        step: 3,
    }];

    for _ in 0..50 {
        let outcome = controller
            .on_poll(
                models::Id::new([42; 8]), // Task ID.
                &mut state,
                &mut inbox,
                &mut checkouts,
                releases.clone(),
                row_state.clone(),
            )
            .await
            .expect("on_poll failed");

        let mut trace = trace.lock().unwrap();

        if let Some(exports) = outcome.publish_exports {
            trace.push(TraceEntry::Exports(exports));
        }
        if let Some(stack) = outcome.publish_stack {
            trace.push(TraceEntry::Stack(stack.clone()));
            row_state.stack = stack;
        }
        trace.push(TraceEntry::Status(outcome.status));

        if matches!(outcome.status, stack::Status::Idle) && outcome.sleep.as_secs() > 0 {
            break;
        }
    }

    insta::assert_json_snapshot!(trace.lock().unwrap().as_slice());
}
