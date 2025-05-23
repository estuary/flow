use data_plane_controller::{controller, stack};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

mod util;
use util::{initial_state, mock_emit_log_fn, mock_run_cmd_fn, TraceEntry};

#[tokio::test]
async fn test_private_links() {
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
    row_state.stack.config.model.private_links =
        vec![stack::PrivateLink::AWS(stack::AWSPrivateLink {
            az_ids: vec!["a".to_string(), "b".to_string()],
            region: "us-west-2".to_string(),
            service_name: "service".to_string(),
        })];

    inbox.push_back((
        models::Id::zero(),
        Some(controller::Message::Start(row_state.data_plane_id)),
    ));
    inbox.push_back((models::Id::zero(), Some(controller::Message::Enable)));

    for _ in 0..50 {
        let outcome = controller
            .on_poll(
                models::Id::new([42; 8]), // Task ID.
                &mut state,
                &mut inbox,
                &mut checkouts,
                Vec::new(),
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

    row_state
        .stack
        .config
        .model
        .private_links
        .push(stack::PrivateLink::AWS(stack::AWSPrivateLink {
            az_ids: vec!["b".to_string(), "c".to_string()],
            region: "us-west-2".to_string(),
            service_name: "service-2".to_string(),
        }));

    inbox.push_back((models::Id::zero(), Some(controller::Message::Converge)));

    for _ in 0..50 {
        let outcome = controller
            .on_poll(
                models::Id::new([42; 8]), // Task ID.
                &mut state,
                &mut inbox,
                &mut checkouts,
                Vec::new(),
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
