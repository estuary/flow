//! Observe the production command paths with an inert engine. A subprocess
//! supplies synthetic ambient secrets before any runtime threads exist.

const SECRETS: &[&str] = &[
    "CONSUMER_AUTH_KEYS",
    "BROKER_AUTH_KEYS",
    "SOPS_AGE_KEY",
    "FLOW_AUTH_TOKEN",
];
const CONFIGURATION: &[&str] = &[
    "HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_RUNTIME_DIR",
    "CONTAINER_HOST",
    "CONTAINER_CONNECTION",
    "CONTAINER_SSHKEY",
    "SSH_AUTH_SOCK",
    "REGISTRY_AUTH_FILE",
    "DOCKER_CONFIG",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "CONTAINERS_CONF",
    "CONTAINERS_REGISTRIES_CONF",
    "CONTAINERS_STORAGE_CONF",
    "STORAGE_DRIVER",
    "STORAGE_OPTS",
    "PODMAN_USERNS",
    "http_proxy",
    "HTTPS_PROXY",
    "no_proxy",
    "CONNECTOR_VMM_TESTS_HOST_PID",
    "CUSTOM_ENGINE_SETTING",
];

#[test]
fn engine_environment() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::Builder::new()
        .prefix("connector-engine-")
        .tempdir()
        .unwrap();
    let engine = dir.path().join("engine");
    let mut script = String::from("#!/bin/sh\nset -eu\n");
    script.push_str("secrets=absent\nconfiguration=preserved\n");
    for name in SECRETS {
        script.push_str(&format!(
            "if [ \"${{{name}+present}}\" = present ]; then secrets={name}; fi\n"
        ));
    }
    for name in CONFIGURATION {
        script.push_str(&format!(
            "if [ \"${{{name}-}}\" != synthetic-engine-setting ]; then configuration={name}; fi\n"
        ));
    }
    script.push_str(
        r#"
if [ "$PATH" != "$ENGINE_EXPECTED_PATH" ]; then configuration=PATH; fi
if [ "$TMPDIR" != "$ENGINE_TEST_DIR" ]; then configuration=TMPDIR; fi
if [ "$DOCKER_CLI" != "$ENGINE_TEST_DIR/engine" ]; then configuration=DOCKER_CLI; fi
step="$1 ${2:-}"
stdin=unfenced
if [ -f /proc/self/fd/0 ]; then
    stdin=locked-record
    flock -n -E 75 /proc/self/fd/0 true && exit 99 || status=$?
    [ "$status" = 75 ] || exit 98
fi
printf '%s | secrets=%s configuration=%s stdin=%s\n' "$step" "$secrets" "$configuration" "$stdin" >>"$ENGINE_TEST_DIR/calls"
case "$step" in
    "pull "*) ;;
    "image inspect")
        echo '[{"Id":"synthetic-image","Created":"2026-01-01T00:00:00Z","Config":{"Env":[],"Labels":{"FLOW_RUNTIME_PROTOCOL":"derive"}}}]'
        ;;
    "network create")
        for arg; do network=$arg; done
        printf '%s' "$network" >"$ENGINE_TEST_DIR/network"
        ;;
    "create --rm")
        for arg; do
            case "$arg" in
                --env=CONNECTOR_MOUNT=*) mount=${arg#--env=CONNECTOR_MOUNT=} ;;
            esac
        done
        [ -r "$mount/task-update.json" ] || exit 46
        echo 0000000000000000000000000000000000000000000000000000000000000001
        ;;
    "network ls") cat "$ENGINE_TEST_DIR/network" ;;
    "network rm") rm "$ENGINE_TEST_DIR/network" ;;
    "start --attach") exit 42 ;;
    "run --rm")
        for arg; do
            if [ "$arg" = boundary ]; then exit 0; fi
        done
        exit 43
        ;;
    "fail "*) echo synthetic-engine-failure >&2; exit 44 ;;
    "rm "* | "ps "*) ;;
    *) echo unexpected-engine-command >&2; exit 45 ;;
esac
"#,
    );
    std::fs::write(&engine, script).unwrap();
    std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
    let init = dir.path().join("flow-connector-init");
    std::fs::write(&init, "#!/bin/sh\nexit 1\n").unwrap();
    std::fs::set_permissions(&init, std::fs::Permissions::from_mode(0o555)).unwrap();

    let path = format!("{}:/usr/bin:/bin", dir.path().display());
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child
        .args([
            "--exact",
            "container::engine_tests::engine_environment_child",
            "--ignored",
            "--nocapture",
        ])
        .env("ENGINE_TEST_DIR", dir.path())
        .env("TMPDIR", dir.path())
        .env("DOCKER_CLI", &engine)
        .env("PATH", &path)
        .env("ENGINE_EXPECTED_PATH", &path);
    for name in SECRETS {
        child.env(name, "synthetic-platform-secret");
    }
    for name in CONFIGURATION {
        child.env(name, "synthetic-engine-setting");
    }
    let output = child.output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let mut calls: Vec<String> = std::fs::read_to_string(dir.path().join("calls"))
        .unwrap()
        .lines()
        .map(ToString::to_string)
        .collect();
    calls.sort();
    insta::assert_snapshot!(calls.join("\n"));
}

#[test]
#[ignore = "only run by engine_environment with a synthetic subprocess environment"]
fn engine_environment_child() {
    let dir = std::path::PathBuf::from(std::env::var_os("ENGINE_TEST_DIR").unwrap());
    let engine = dir.join("engine").to_str().unwrap().to_string();
    let state = dir.join("state");
    std::fs::create_dir(&state).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let image = "ghcr.io/estuary/derive-python:synthetic";
    let ctx = crate::protocol::StartContext {
        container_network: String::new(),
        execution: proto_flow::flow::ConnectorExecution {
            vmm: true,
            egress: None,
        },
        log_level: ops::LogLevel::Warn,
        log_sink: crate::LogSink::tracing(),
        plane: crate::Plane::Local,
        process: None,
        task_name: "acmeCo/engine-environment".to_string(),
        vmm: None,
        secret_resolver: std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        task_update: Some(crate::TaskUpdate::for_test()),
    };
    let vmm = crate::Vmm {
        image: "synthetic-vmm".to_string(),
        podman: engine.clone(),
        state_dir: state.to_str().unwrap().to_string(),
        disk_mib: 1,
        memory_limit: "512m".to_string(),
        cpu_limit: "1".to_string(),
        guest_memory_mib: 256,
        vcpus: 1,
        cgroup_parent: None,
    };
    let crate::vmm::Execution::Vmm {
        vmm,
        eligible,
        egress,
    } = crate::vmm::vmm_for(
        Some(&vmm),
        ops::TaskType::Derivation,
        Some(image),
        &ctx.execution,
        None,
    )
    .unwrap()
    else {
        panic!("the synthetic derivation must select VMM execution");
    };
    runtime.block_on(async {
        let error = super::engine_cmd(&engine, &["fail"]).await.unwrap_err();
        assert!(error.to_string().contains("synthetic-engine-failure"));
        let Err(error) = async {
            let inspection = super::pull_and_inspect(image, &ctx.log_sink).await?;
            let mount = crate::protocol::create_connector_mount()?;
            super::run(
                inspection,
                super::RunParams {
                    env: Default::default(),
                    host_gateway_names: Vec::new(),
                    labels: Default::default(),
                    log_sink: ctx.log_sink.clone(),
                    mount: mount.path().to_owned(),
                    network: String::new(),
                    publish_ports: true,
                },
                |log| log,
            )
            .await
        }
        .await
        else {
            panic!("the inert ordinary engine must exit before readiness");
        };
        assert!(
            error
                .to_string()
                .contains("exited before flow-connector-init")
        );
        let Err(error) = crate::vmm::launch::start(
            &ctx,
            vmm,
            eligible,
            egress,
            image,
            &crate::EMPTY_SECRETS,
            None,
        )
        .await
        else {
            panic!("the inert VMM engine must exit before readiness");
        };
        assert!(
            error
                .to_string()
                .contains("exited before flow-connector-init")
        );
    });
    assert_eq!(std::fs::read_dir(&state).unwrap().count(), 0);
    let mounts = dir.join(format!("connector-mounts-{}", unsafe { libc::geteuid() }));
    assert_eq!(std::fs::read_dir(mounts).unwrap().count(), 0);
}
