use data_plane_controller::{
    commands,
    controller::{EmitLogFn, RunCmdFn},
    stack,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use itertools::Itertools;
use sqlx::types::uuid;
use std::sync::{Arc, Mutex};

#[derive(serde::Serialize)]
pub enum TraceEntry {
    Log(&'static str, String),
    Cmd(&'static str, String),
    Status(stack::Status),
    Exports(stack::ControlExports),
    Stack(stack::PulumiStack),
}

pub fn mock_emit_log_fn(trace: Arc<Mutex<Vec<TraceEntry>>>) -> EmitLogFn {
    Box::new(
        move |_logs_token, stream, message| -> BoxFuture<'static, anyhow::Result<()>> {
            trace.lock().unwrap().push(TraceEntry::Log(stream, message));

            futures::future::ready(Ok(())).boxed()
        },
    )
}

pub fn mock_run_cmd_fn(trace: Arc<Mutex<Vec<TraceEntry>>>) -> RunCmdFn {
    Box::new(
        move |cmd, _capture, stream, _logs_token| -> BoxFuture<'static, anyhow::Result<Vec<u8>>> {
            trace.lock().unwrap().push(TraceEntry::Cmd(
                stream,
                commands::args(&cmd).map(|s| s.to_string_lossy()).join(" "),
            ));
            let mut output = Vec::new();

            // If we're initializing a stack, the controller expects the encrypted key
            // of the stack YAML to be updated by the Pulumi command.
            if commands::starts_with(&cmd, &["pulumi", "stack", "init"]) {
                commands::write_stack_init_fixture(&cmd, "test-fixture").unwrap();
            }

            // Return a fixture which models a typical Pulumi stack output.
            if commands::starts_with(&cmd, &["pulumi", "stack", "output"]) {
                output = include_bytes!("../src/dry_run_fixture.json").to_vec();
            }

            // Return a fixture which models a typical Pulumi stack history output with a change.
            if commands::starts_with(&cmd, &["pulumi", "stack", "history"]) {
                output = serde_json::to_vec(&[crate::stack::PulumiStackHistory {
                    resource_changes: crate::stack::PulumiStackResourceChanges {
                        create: 1,
                        delete: 0,
                        same: 0,
                        update: 0,
                    },
                }])
                .unwrap();
            }


            futures::future::ready(Ok(output)).boxed()
        },
    )
}

pub fn initial_state() -> stack::State {
    let model: stack::DataPlane = serde_json::from_value(serde_json::json!({
      "name": "test-plane",
      "fqdn": "test-plane.estuary.dev",
      "builds_root": "gs://estuary-control/builds/",
      "deployments": [
        {
          "role": "etcd",
          "current": 0,
          "desired": 3,
          "template": {
            "plan": "vc2-1c-2gb",
            "os_id": 2284,
            "region": "ord",
            "provider": "vultr"
          },
          "oci_image": "quay.io/coreos/etcd:v3.5"
        },
        {
          "role": "reactor",
          "current": 0,
          "desired": 1,
          "template": {
            "plan": "vc2-1c-2gb",
            "os_id": 2284,
            "region": "ord",
            "provider": "vultr"
          },
          "oci_image": "ghcr.io/estuary/flow:v2.3.4"
        },
        {
          "role": "gazette",
          "current": 0,
          "desired": 4,
          "template": {
            "plan": "vc2-1c-2gb",
            "os_id": 2284,
            "region": "ord",
            "provider": "vultr"
          },
          "oci_image": "ghcr.io/gazette/broker:v1.2.3"
        }
      ],
      "gcp_project": "some-gcp-project",
      "ssh_subnets": [
        "12.34.56.78/32",
        "2600:1234:5000:6000::/128"
      ],
      "data_buckets": [
        "gs://example-bucket"
      ],
      "builds_kms_keys": [
        "projects/example/key"
      ],
      "control_plane_api": "https://example.api/",
      "connector_limits": {
        "cpu": "200m",
        "memory": "1g"
      }
    }))
    .unwrap();

    stack::State {
        data_plane_id: models::Id::new([32; 8]),
        deploy_branch: "test-branch".to_string(),
        last_pulumi_up: chrono::DateTime::default(),
        last_refresh: chrono::DateTime::default(),
        logs_token: uuid::uuid!("12345678-1234-5678-1234-567812345678"),
        stack: stack::PulumiStack {
            config: stack::PulumiStackConfig { model },
            secrets_provider: "passphrase".to_string(), // Start with passphrase
            encrypted_key: String::new(),               // No key initially
        },
        stack_name: "test-stack".to_string(),
        status: stack::Status::Idle,
        disabled: false, // Start enabled for convergence
        pending_preview: false,
        preview_branch: String::new(),
        pending_refresh: false,
        pending_converge: false, // Start false, should be set by diffing
        publish_exports: None,
        publish_stack: None,
    }
}
