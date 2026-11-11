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
    "CUSTOM_ENGINE_SETTING",
];

#[test]
fn engine_environment() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let engine = dir.path().join("engine");
    let mut script = String::from("#!/bin/sh\nset -eu\n");
    for name in SECRETS {
        script.push_str(&format!(
            "[ \"${{{name}+present}}\" != present ] || exit 71\n"
        ));
    }
    for name in CONFIGURATION {
        script.push_str(&format!(
            "[ \"${{{name}-}}\" = synthetic-engine-setting ] || exit 72\n"
        ));
    }
    script.push_str(r#"
[ "$PATH" = "$ENGINE_EXPECTED_PATH" ] || exit 73
[ "$TMPDIR" = "$ENGINE_TEST_DIR" ] || exit 74
[ "$DOCKER_CLI" = "$ENGINE_TEST_DIR/engine" ] || exit 75
case "$1" in cp) detail=init ;; *) detail=${2:-} ;; esac
printf '%s %s | secrets=absent configuration=preserved\n' "$1" "$detail" >>"$ENGINE_TEST_DIR/calls"
case "$1 ${2:-}" in
    "pull "*) ;;
    "image inspect") echo '[{"Id":"synthetic-image","Created":"2026-01-01T00:00:00Z","Config":{"Env":[],"Labels":{"FLOW_RUNTIME_PROTOCOL":"derive"}}}]' ;;
    "run --rm")
        if [ "$ENGINE_MODE" = early ]; then echo synthetic-early-failure >&2; exit 43; fi
        # exec keeps the pipe reader at the child PID for cancellation checks.
        echo "$$" >"$ENGINE_TEST_DIR/pid"
        if [ "$ENGINE_MODE" = marker ]; then printf ' ' >&2; fi
        exec cat "$ENGINE_TEST_DIR/gate"
        ;;
    "inspect --format") echo synthetic-network-failure >&2; exit 44 ;;
    "create "* | "rm "*) ;;
    "cp "*) printf '#!/bin/sh\nexit 1\n' >"$3" ;;
    "fail "*) echo synthetic-engine-failure >&2; exit 45 ;;
    *) exit 46 ;;
esac
"#);
    std::fs::write(&engine, script).unwrap();
    std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(dir.path().join("gate"))
            .status()
            .unwrap()
            .success()
    );
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
    for mode in ["early", "marker", "cancel"] {
        let output = child.env("ENGINE_MODE", mode).output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mode = std::env::var("ENGINE_MODE").unwrap();
    runtime.block_on(async {
        let sink = crate::LogSink::tracing();
        let inspection = super::pull_and_inspect("synthetic:latest", &sink)
            .await
            .unwrap();
        let mount = tempfile::tempdir_in(&dir).unwrap();
        let mut start = Box::pin(super::run(
            inspection,
            super::RunParams {
                env: Default::default(),
                host_gateway_names: Vec::new(),
                labels: Default::default(),
                log_sink: sink,
                mount: mount.path().to_owned(),
                network: String::new(),
                publish_ports: true,
                network_ports: Vec::new(),
            },
            |log| log,
        ));
        if mode == "cancel" {
            tokio::select! {
                result = &mut start => panic!("startup ended: {:?}", result.err()),
                () = async {
                    while !dir.join("pid").exists() { tokio::task::yield_now().await; }
                } => (),
            }
        } else {
            let Err(error) = (&mut start).await else {
                panic!("inert engine cannot start")
            };
            let expected = if mode == "early" {
                "container exited before flow-connector-init"
            } else {
                "synthetic-network-failure"
            };
            assert!(format!("{error:#}").contains(expected), "{error:#}");
        }
        drop(start);
        if let Ok(pid) = std::fs::read_to_string(dir.join("pid")) {
            let pid: i32 = pid.trim().parse().unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while unsafe { libc::kill(pid, 0) } == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled engine is killed and reaped");
            std::fs::remove_file(dir.join("pid")).unwrap();
        }
    });
    runtime.block_on(async {
        let error = super::docker_cmd(&["fail"]).await.unwrap_err();
        assert!(error.to_string().contains("synthetic-engine-failure"));
    });
    for name in SECRETS {
        assert_eq!(std::env::var(name).unwrap(), "synthetic-platform-secret");
    }
}
