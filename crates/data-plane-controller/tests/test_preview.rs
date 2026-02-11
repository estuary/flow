use data_plane_controller::job::executor::{Executor, Message, Preview};
use data_plane_controller::shared::{controller::ControllerConfig, stack};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

mod util;
use util::{TraceEntry, initial_state, mock_dispatch_fn};

#[tokio::test]
async fn test() {
    let trace = Arc::new(Mutex::new(Vec::new()));

    let controller_config = ControllerConfig {
        dns_ttl: std::time::Duration::ZERO,
        dry_dock_remote: "git@github.com:estuary/est-dry-dock.git".to_string(),
        ops_remote: "git@github.com:estuary/ops.git".to_string(),
        secrets_provider: "testing".to_string(),
        state_backend: "file:///tmp/pulumi-test-state".parse().unwrap(),
        dry_run: false,
    };

    let executor = Executor::new_with_dispatch(
        controller_config,
        mock_dispatch_fn(trace.clone()),
    );

    let mut state: Option<stack::State> = None;
    let mut inbox: VecDeque<(models::Id, Option<Message>)> = VecDeque::new();
    let mut row_state = initial_state();

    inbox.push_back((
        models::Id::zero(),
        Some(Message::Start(row_state.data_plane_id)),
    ));
    inbox.push_back((
        models::Id::zero(),
        Some(Message::Preview(Preview {
            branch: "preview-branch".to_string(),
        })),
    ));
    inbox.push_back((models::Id::zero(), Some(Message::Enable)));

    for _ in 0..50 {
        let outcome = executor
            .on_poll(
                models::Id::new([42; 8]), // Task ID.
                &mut state,
                &mut inbox,
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
