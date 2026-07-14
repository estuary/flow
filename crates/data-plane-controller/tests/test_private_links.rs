use data_plane_controller::job::executor::{Executor, Message};
use data_plane_controller::shared::{controller::ControllerConfig, stack};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

mod util;
use util::{TraceEntry, initial_state, mock_dispatch_fn};

fn test_executor(trace: Arc<Mutex<Vec<TraceEntry>>>) -> Executor {
    let controller_config = ControllerConfig {
        dns_ttl: std::time::Duration::ZERO,
        dry_dock_remote: "git@github.com:estuary/est-dry-dock.git".to_string(),
        ops_remote: "git@github.com:estuary/ops.git".to_string(),
        secrets_provider: "testing".to_string(),
        state_backend: "file:///tmp/pulumi-test-state".parse().unwrap(),
        dry_run: false,
    };
    Executor::new_with_dispatch(controller_config, mock_dispatch_fn(trace))
}

#[tokio::test]
async fn test_private_links() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let executor = test_executor(trace.clone());

    let mut state: Option<stack::State> = None;
    let mut inbox: VecDeque<(models::Id, Option<Message>)> = VecDeque::new();
    let mut row_state = initial_state();
    row_state.stack.config.model.private_links = vec![stack::PrivateLinkEntry {
        id: Some(models::Id::new([0, 0, 0, 0, 0, 0, 0xb, 0x1])),
        config: stack::PrivateLink::AWS(stack::AWSPrivateLink {
            az_ids: vec!["a".to_string(), "b".to_string()],
            region: "us-west-2".to_string(),
            service_name: "service".to_string(),
            service_region: None,
        }),
    }];

    inbox.push_back((
        models::Id::zero(),
        Some(Message::Start(row_state.data_plane_id)),
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

    row_state
        .stack
        .config
        .model
        .private_links
        .push(stack::PrivateLinkEntry {
            id: Some(models::Id::new([0, 0, 0, 0, 0, 0, 0xb, 0x2])),
            config: stack::PrivateLink::AWS(stack::AWSPrivateLink {
                az_ids: vec!["b".to_string(), "c".to_string()],
                region: "us-west-2".to_string(),
                service_name: "service-2".to_string(),
                service_region: None,
            }),
        });

    inbox.push_back((models::Id::zero(), Some(Message::Converge)));

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

#[tokio::test]
async fn generation_only_change_queues_converge() {
    let link_id = models::Id::new([0, 0, 0, 0, 0, 0, 0xb, 0x1]);
    let pinned = |generation| stack::PinnedLink {
        id: link_id,
        generation,
    };

    // While idle, generations are the change-detection baseline. The plane is
    // disabled so the queued converge remains observable instead of starting.
    let trace = Arc::new(Mutex::new(Vec::new()));
    let executor = test_executor(trace);
    let mut idle = initial_state();
    idle.disabled = true;
    idle.stack.encrypted_key = "encrypted".to_string();
    idle.last_pulumi_up = chrono::Utc::now();
    idle.last_refresh = chrono::Utc::now();
    idle.pinned_links = vec![pinned(1)];
    let mut row_state = idle.clone();
    row_state.pinned_links = vec![pinned(2)];

    let mut state = Some(idle);
    let mut inbox = VecDeque::new();
    let outcome = executor
        .on_poll(
            models::Id::new([42; 8]),
            &mut state,
            &mut inbox,
            Vec::new(),
            row_state,
        )
        .await
        .unwrap();
    let state = state.unwrap();
    assert!(matches!(outcome.status, stack::Status::Idle));
    assert!(state.pending_converge);
    assert_eq!(state.pinned_links, vec![pinned(2)]);

    // During a converge, the same change queues a follow-up but preserves the
    // PulumiUp1 pins needed to reject this converge's now-stale result.
    let trace = Arc::new(Mutex::new(Vec::new()));
    let executor = test_executor(trace);
    let mut active = initial_state();
    active.stack.encrypted_key = "encrypted".to_string();
    active.status = stack::Status::AwaitDNS1;
    active.last_pulumi_up = chrono::Utc::now();
    active.pinned_links = vec![pinned(1)];
    let mut row_state = active.clone();
    row_state.status = stack::Status::Idle;
    row_state.pinned_links = vec![pinned(2)];

    let mut state = Some(active);
    let mut inbox = VecDeque::new();
    let outcome = executor
        .on_poll(
            models::Id::new([42; 8]),
            &mut state,
            &mut inbox,
            Vec::new(),
            row_state,
        )
        .await
        .unwrap();
    let state = state.unwrap();
    assert!(matches!(outcome.status, stack::Status::Ansible));
    assert!(state.pending_converge);
    assert_eq!(state.pinned_links, vec![pinned(1)]);
}
