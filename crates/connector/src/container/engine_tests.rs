//! Observe the production command paths with an inert engine. A subprocess
//! sets the engine's environment before any runtime threads exist.

#[test]
fn engine_environment() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::Builder::new()
        .prefix("connector-engine-")
        .tempdir()
        .unwrap();
    let engine = dir.path().join("engine");
    let mut script = String::from("#!/bin/sh\nset -eu\n");
    script.push_str(
        r#"
step="$1 ${2:-}"
printf '%s\n' "${step% }" >>"$ENGINE_TEST_DIR/calls"
case "$step" in
    "pull "*) ;;
    "image inspect")
        if [ "${3:-}" = --format ]; then
            echo 00000000000000000000000000000000000000000000000000000000000000aa
        else
            echo '[{"Id":"synthetic-image","Created":"2026-01-01T00:00:00Z","Config":{"Env":[],"Labels":{"FLOW_RUNTIME_PROTOCOL":"derive"}}}]'
        fi
        ;;
    "network create")
        for arg; do network=$arg; done
        printf '%s' "$network" >"$ENGINE_TEST_DIR/network"
        ;;
    "create --rm")
        previous=
        for arg; do
            if [ "$previous" = --connector-mount ]; then mount=$arg; fi
            previous=$arg
            case "$arg" in
                --env=CONNECTOR_MOUNT=*) mount=${arg#--env=CONNECTOR_MOUNT=} ;;
            esac
        done
        [ -r "$mount/task-update.json" ] || exit 46
        echo 0000000000000000000000000000000000000000000000000000000000000001
        ;;
    "network ls") if [ -e "$ENGINE_TEST_DIR/network" ]; then cat "$ENGINE_TEST_DIR/network"; fi ;;
    "network rm") rm "$ENGINE_TEST_DIR/network" ;;
    "volume create")
        for arg; do volume=$arg; done
        printf '%s' "$volume" >"$ENGINE_TEST_DIR/volume"
        ;;
    "volume ls") if [ -e "$ENGINE_TEST_DIR/volume" ]; then cat "$ENGINE_TEST_DIR/volume"; fi ;;
    "volume rm") rm "$ENGINE_TEST_DIR/volume" ;;
    "start --attach") exit 42 ;;
    "run --rm")
        mount=
        for arg; do
            case "$arg" in
                boundary) exit 0 ;;
                --env=CONNECTOR_MOUNT=*) mount=${arg#--env=CONNECTOR_MOUNT=} ;;
            esac
        done
        if [ -n "$mount" ]; then
            printf '%s | holds %s\n' "$*" "$(echo $(ls "$mount"))" |
                sed -e "s|$mount|\$MOUNT|g" -e 's|--name=fc_[0-9a-f]*|--name=$NAME|' \
                >>"$ENGINE_TEST_DIR/ordinary"
        fi
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
        // Ordinary runs' snapshotted argv takes these limits' defaults.
        .env_remove("CONNECTOR_MEMORY_LIMIT")
        .env_remove("CONNECTOR_CPU_LIMIT")
        .env_remove("CONNECTOR_CGROUP_PARENT");
    let output = child.output().unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    #[cfg(target_os = "linux")]
    {
        let mut calls: Vec<String> = std::fs::read_to_string(dir.path().join("calls"))
            .unwrap()
            .lines()
            .map(ToString::to_string)
            .collect();
        calls.sort();
        insta::assert_snapshot!(calls.join("\n"));
    }

    let ordinary = std::fs::read_to_string(dir.path().join("ordinary")).unwrap();
    insta::assert_snapshot!(ordinary, @r"
    run --rm --name=$NAME --network=bridge --entrypoint=$MOUNT/flow-connector-init --log-driver=none --mount=type=bind,source=$MOUNT,target=$MOUNT,readonly --memory 1g --cpus 2 --platform=linux/amd64 --env=CONNECTOR_MOUNT=$MOUNT --label=image=ghcr.io/estuary/derive-python:synthetic --label=task-name=acmeCo/engine-environment --label=task-type=derivation ghcr.io/estuary/derive-python:synthetic --image-inspect-json-path=$MOUNT/image-inspect.json --port=49092 | holds flow-connector-init image-inspect.json task-update.json
    run --rm --name=$NAME --network=bridge --entrypoint=$MOUNT/flow-connector-init --log-driver=none --mount=type=bind,source=$MOUNT,target=$MOUNT,readonly --memory 1g --cpus 2 --platform=linux/amd64 --env=CONNECTOR_MOUNT=$MOUNT --label=image=ghcr.io/estuary/derive-typescript:synthetic --label=task-name=acmeCo/engine-environment --label=task-type=derivation ghcr.io/estuary/derive-typescript:synthetic --image-inspect-json-path=$MOUNT/image-inspect.json --port=49092 | holds flow-connector-init image-inspect.json task-update.json
    ");
}

#[test]
#[ignore = "only run by engine_environment with a synthetic subprocess environment"]
fn engine_environment_child() {
    let dir = std::path::PathBuf::from(std::env::var_os("ENGINE_TEST_DIR").unwrap());
    let engine = dir.join("engine").to_str().unwrap().to_string();
    let state = dir.join("state");
    std::fs::create_dir(&state).unwrap();
    std::fs::set_permissions(&state, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let image = "ghcr.io/estuary/derive-python:synthetic";
    let execution = proto_flow::flow::ConnectorExecution {
        vmm: true,
        egress: Some(Default::default()),
    };
    let ctx = crate::protocol::StartContext {
        container_network: String::new(),
        log_sink: crate::LogSink::tracing(),
        plane: crate::Plane::Local,
        process: None,
        start: crate::proto::request::Start {
            execution: Some(Box::new(execution.clone())),
            ..Default::default()
        },
        task_name: "acmeCo/engine-environment".to_string(),
        vmm: None,
        secret_resolver: std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        task_update: Some(crate::TaskUpdate::for_test()),
    };
    #[cfg(target_os = "linux")]
    let vmm = crate::Vmm {
        image: "synthetic-vmm".to_string(),
        podman: engine.clone(),
        state_dir: state.to_str().unwrap().to_string(),
        disk_mib: 1,
        memory: Ok(crate::vmm::Memory {
            bytes: 512 << 20,
            guest_mib: 256,
        }),
        cpu_limit: "1".to_string(),
        vcpus: Ok(1),
        cgroup_parent: None,
    };
    #[cfg(target_os = "linux")]
    let crate::vmm::Execution::Vmm {
        vmm,
        eligible,
        egress,
    } = crate::vmm::vmm_for(
        Some(&vmm),
        ctx.plane,
        ops::TaskType::Derivation,
        Some(image),
        &execution,
        None,
    )
    .unwrap()
    else {
        panic!("the synthetic derivation must select VMM execution");
    };
    let mounts = dir.join(format!(
        "connector-mounts-{}",
        rustix::process::geteuid().as_raw()
    ));
    let ordinary = |plane, image: &str| {
        let config: bytes::Bytes = serde_json::json!({"image": image, "config": {}})
            .to_string()
            .into();
        let Some((.., proto_flow::connector::request::Kind::Derive(open))) =
            crate::execution_fixture::every_first_request(
                crate::execution_fixture::image_connector_type,
                &config,
                None,
            )
            .into_iter()
            .find(|(label, ..)| *label == "derive Open")
        else {
            panic!("the fixture has a derive Open");
        };
        let ctx = crate::protocol::StartContext {
            container_network: String::new(),
            log_sink: crate::LogSink::tracing(),
            plane,
            process: None,
            start: Default::default(),
            task_name: ctx.task_name.clone(),
            vmm: None,
            secret_resolver: std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
            task_update: Some(crate::TaskUpdate::for_test()),
        };
        crate::protocol::start::<crate::derive::Derive>(ctx, None, open)
    };
    runtime.block_on(async {
        let error = super::engine_cmd(&engine, &["fail"]).await.unwrap_err();
        assert!(error.to_string().contains("synthetic-engine-failure"));

        let Err(error) = ordinary(crate::Plane::Public, image).await else {
            panic!("an ordinary public Python start must be refused");
        };
        assert_eq!(
            format!("{error:#}"),
            "Python derivations may only run in private data-planes, or within a VMM (`vmm: true`)"
        );
        assert!(!mounts.exists(), "a refused start made a mount");
        let calls = std::fs::read_to_string(dir.join("calls")).unwrap();
        assert!(!calls.contains("pull"), "{calls}");

        for (plane, image) in [
            (crate::Plane::Private, image),
            (
                crate::Plane::Public,
                "ghcr.io/estuary/derive-typescript:synthetic",
            ),
        ] {
            let Err(error) = ordinary(plane, image).await else {
                panic!("the inert ordinary engine must exit before readiness");
            };
            assert!(
                format!("{error:#}").contains("exited before flow-connector-init started"),
                "{error:#}"
            );
        }
        let Err(error) = async {
            let inspection = super::pull_and_inspect(image, &ctx.log_sink).await?;
            let mount = crate::protocol::create_connector_mount()?;
            super::run(
                inspection,
                super::RunParams {
                    env: Default::default(),
                    host_gateway_names: Vec::new(),
                    labels: Default::default(),
                    mount: mount.path().to_owned(),
                    network: String::new(),
                    publish_ports: true,
                    network_ports: Vec::new(),
                },
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
        #[cfg(target_os = "linux")]
        {
            let Err(error) = crate::vmm::launch::start(
                &ctx,
                vmm,
                eligible,
                egress,
                Vec::new(),
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
                    .contains("exited (exit status: 42) before flow-connector-init")
                    || error
                        .to_string()
                        .contains("closed stderr before flow-connector-init started"),
                "{error:#}"
            );
            assert_eq!(vmm.recover().await, 0);
        }
    });
    assert_eq!(std::fs::read_dir(&state).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(mounts).unwrap().count(), 0);
}
