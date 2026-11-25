//! Integration tests of the Docker provider, against the local Docker daemon.
//! They need the `flow-sandbox:local` image (`mise run local:sandbox-image`)
//! and a debug build of flowctl (`cargo build -p flowctl`), so they're ignored
//! by default. Run them with:
//!
//!   mise exec -- cargo nextest run -p control-plane-api --test sandbox_docker --run-ignored only

use control_plane_api::sandboxes::docker::{Docker, Mount, SCOPE_LABEL};
use control_plane_api::sandboxes::provider::{Error, Exec, Output, Provider, State, WRAPPER_ARGV};
use futures::{SinkExt, StreamExt};

const IMAGE: &str = "flow-sandbox:local";

/// Each test's containers carry a unique scope, which `Cleanup` removes
/// when dropped, including on a panic.
struct Cleanup {
    scope: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let output = std::process::Command::new("docker")
            .args(["ps", "--all", "--quiet", "--filter"])
            .arg(format!("label={SCOPE_LABEL}={}", self.scope))
            .output()
            .expect("docker ps");
        let ids = String::from_utf8(output.stdout).unwrap();
        let ids: Vec<&str> = ids.lines().collect();
        if ids.is_empty() {
            return;
        }
        let status = std::process::Command::new("docker")
            .args(["rm", "--force"])
            .args(&ids)
            .stdout(std::process::Stdio::null())
            .status()
            .expect("docker rm");
        // Not a panic: this may run while unwinding from one.
        if !status.success() {
            eprintln!("failed to remove test containers {ids:?}");
        }
    }
}

/// Returns a provider with a unique scope, its cleanup guard, and a unique
/// container handle within that scope.
fn setup(mounts: Vec<Mount>) -> (Docker, Cleanup, String) {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let scope = format!("test-{id}");

    // Test binaries are built into `<target>/debug/deps/`.
    let exe = std::env::current_exe().unwrap();
    let flowctl = exe.parent().unwrap().parent().unwrap().join("flowctl");
    assert!(
        flowctl.is_file(),
        "{} is missing: run `cargo build -p flowctl`",
        flowctl.display()
    );

    let docker = Docker {
        image: IMAGE.to_string(),
        flowctl,
        mounts,
        scope: scope.clone(),
    };
    (docker, Cleanup { scope }, format!("sbx-test-{id}"))
}

/// What a command produced, from the frames of the wrapper and the
/// output of the `docker exec` which ran it.
#[derive(Debug, Default)]
struct Outcome {
    stdout: String,
    stderr: String,
    /// The wrapper's last frame: `Exit` or `Error`.
    last: Option<sandbox_exec::Output>,
    /// Stderr of the wrapper process itself.
    wrapper_stderr: String,
    wrapper_exit: Option<i32>,
}

fn encode(frames: &[sandbox_exec::Input]) -> bytes::Bytes {
    let mut buf = bytes::BytesMut::new();
    for frame in frames {
        frame.encode(&mut buf).unwrap();
    }
    buf.freeze()
}

fn header(workspace: &str, command: &str) -> sandbox_exec::Input {
    sandbox_exec::Input::Header(sandbox_exec::Header {
        version: sandbox_exec::PROTOCOL_VERSION,
        workspace: workspace.to_string(),
        command: command.to_string(),
        env: [("HEADER_VAR".to_string(), "from-header".to_string())].into(),
    })
}

/// Read `output` to its end, decoding the wrapper's frames from its stdout.
async fn collect(
    mut output: futures::stream::BoxStream<'static, Result<Output, Error>>,
) -> Outcome {
    let mut outcome = Outcome::default();
    let mut buf = bytes::BytesMut::new();

    while let Some(item) = output.next().await {
        match item.expect("output stream item") {
            Output::Stdout(data) => buf.extend_from_slice(&data),
            Output::Stderr(data) => outcome
                .wrapper_stderr
                .push_str(&String::from_utf8_lossy(&data)),
            Output::Exit(code) => outcome.wrapper_exit = Some(code),
        }
        while let Some(frame) = sandbox_exec::Output::decode(&mut buf).unwrap() {
            match frame {
                sandbox_exec::Output::Stdout(data) => {
                    outcome.stdout.push_str(&String::from_utf8_lossy(&data))
                }
                sandbox_exec::Output::Stderr(data) => {
                    outcome.stderr.push_str(&String::from_utf8_lossy(&data))
                }
                last => outcome.last = Some(last),
            }
        }
    }
    assert!(buf.is_empty(), "trailing partial frame");
    outcome
}

/// Run `argv` directly (not through the wrapper), returning its stdout and exit code.
async fn run_direct(docker: &Docker, handle: &str, argv: &[&str]) -> (String, i32) {
    let Exec { stdin, mut output } = docker
        .exec(handle, &serde_json::json!({}), argv)
        .await
        .unwrap();
    drop(stdin);

    let (mut stdout, mut code) = (String::new(), None);
    while let Some(item) = output.next().await {
        match item.unwrap() {
            Output::Stdout(data) => stdout.push_str(&String::from_utf8_lossy(&data)),
            Output::Stderr(_) => {}
            Output::Exit(c) => code = Some(c),
        }
    }
    (stdout, code.expect("Exit is the last item"))
}

#[tokio::test]
#[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
async fn test_lifecycle() {
    let (docker, _cleanup, handle) = setup(Vec::new());
    let state = serde_json::json!({});

    assert_eq!(docker.kind(), "docker");
    assert_eq!(docker.provision(&handle).await.unwrap(), state);
    assert_eq!(docker.list().await.unwrap(), vec![handle.clone()]);

    // Another scope's provider doesn't see this one's container.
    let (other, _other_cleanup, _) = setup(Vec::new());
    assert!(other.list().await.unwrap().is_empty());

    assert_eq!(run_direct(&docker, &handle, &["true"]).await.1, 0);
    assert_eq!(run_direct(&docker, &handle, &["false"]).await.1, 1);

    assert!(docker.destroy(&handle, &state).await.unwrap());
    assert!(!docker.destroy(&handle, &state).await.unwrap());
    assert!(matches!(
        docker.exec(&handle, &state, &["true"]).await,
        Err(Error::Gone)
    ));
    assert!(docker.list().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
async fn test_stopped_container_is_gone() {
    let (docker, _cleanup, handle) = setup(Vec::new());
    let state = docker.provision(&handle).await.unwrap();

    let status = std::process::Command::new("docker")
        .args(["stop", "--time", "0", &handle])
        .stdout(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());

    assert!(matches!(
        docker.exec(&handle, &state, &["true"]).await,
        Err(Error::Gone)
    ));
    assert_eq!(docker.list().await.unwrap(), vec![handle.clone()]);
    assert!(docker.destroy(&handle, &state).await.unwrap());
}

#[tokio::test]
#[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
async fn test_wrapper_end_to_end() {
    let mounted = tempfile::tempdir().unwrap();
    std::fs::write(mounted.path().join("file"), "mounted content\n").unwrap();
    // The container's user must be able to read the mount.
    std::fs::set_permissions(
        mounted.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();

    // A workspace skeleton, owned by the host's user and mounted read-only.
    let skel = tempfile::tempdir().unwrap();
    std::fs::create_dir(skel.path().join(".config")).unwrap();
    std::fs::write(skel.path().join(".config/profile.json"), "{}\n").unwrap();
    for (path, mode) in [
        ("", 0o755),
        (".config", 0o755),
        (".config/profile.json", 0o644),
    ] {
        std::fs::set_permissions(
            skel.path().join(path),
            std::os::unix::fs::PermissionsExt::from_mode(mode),
        )
        .unwrap();
    }

    let (docker, _cleanup, handle) = setup(vec![
        Mount {
            host: mounted.path().to_path_buf(),
            container: "/opt/mounted".to_string(),
        },
        Mount {
            host: skel.path().to_path_buf(),
            container: "/opt/estuary/skel".to_string(),
        },
    ]);
    let state = docker.provision(&handle).await.unwrap();

    let command = r#"
        read line
        echo "stdin: $line"
        echo "rest: $(cat)"
        echo "env: $HEADER_VAR"
        echo "home: $HOME"
        echo "cwd: $(pwd)"
        echo "user: $(id -un)"
        echo "flowctl: $(command -v flowctl)"
        sudo -n true && echo "sudo: ok"
        cat /opt/mounted/file
        stat -c '%n %U %a' .config .config/profile.json
        echo more >> .config/profile.json && cat .config/profile.json
        echo "to stderr" >&2
        exit 3
    "#;

    let Exec { mut stdin, output } = docker.exec(&handle, &state, &WRAPPER_ARGV).await.unwrap();
    stdin
        .send(encode(&[
            header("ws-one", command),
            sandbox_exec::Input::Stdin(bytes::Bytes::from_static(b"first line\nsecond ")),
            sandbox_exec::Input::Stdin(bytes::Bytes::from_static(b"and third")),
            sandbox_exec::Input::StdinEof,
        ]))
        .await
        .unwrap();

    let outcome = collect(output).await;
    drop(stdin);

    insta::assert_debug_snapshot!(outcome, @r#"
    Outcome {
        stdout: "stdin: first line\nrest: second and third\nenv: from-header\nhome: /work/ws-one\ncwd: /work/ws-one\nuser: sandbox\nflowctl: /opt/estuary/bin/flowctl\nsudo: ok\nmounted content\n.config sandbox 755\n.config/profile.json sandbox 644\n{}\nmore\n",
        stderr: "to stderr\n",
        last: Some(
            Exit {
                code: Some(
                    3,
                ),
                signal: None,
            },
        ),
        wrapper_stderr: "",
        wrapper_exit: Some(
            0,
        ),
    }
    "#);
}

#[tokio::test]
#[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
async fn test_concurrent_execs() {
    let (docker, _cleanup, handle) = setup(Vec::new());
    let state = docker.provision(&handle).await.unwrap();

    // Each command writes a file into its workspace, waits so that the two
    // overlap, and then lists its workspace.
    let command = r#"read word; echo "$word" > mine; sleep 1; echo "$(pwd): $(ls) $(cat mine)""#;

    let run = |workspace: &'static str, word: &'static str| {
        let (docker, handle, state) = (&docker, &handle, &state);
        async move {
            let Exec { mut stdin, output } =
                docker.exec(handle, state, &WRAPPER_ARGV).await.unwrap();
            stdin
                .send(encode(&[
                    header(workspace, command),
                    sandbox_exec::Input::Stdin(format!("{word}\n").into()),
                    sandbox_exec::Input::StdinEof,
                ]))
                .await
                .unwrap();
            let outcome = collect(output).await;
            drop(stdin);
            outcome
        }
    };
    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(run("ws-a", "apple"), run("ws-b", "banana"));

    // Run serially, they'd take at least two seconds.
    assert!(started.elapsed() < std::time::Duration::from_millis(1900));
    insta::assert_debug_snapshot!((a.stdout, a.last, b.stdout, b.last), @r#"
    (
        "/work/ws-a: mine apple\n",
        Some(
            Exit {
                code: Some(
                    0,
                ),
                signal: None,
            },
        ),
        "/work/ws-b: mine banana\n",
        Some(
            Exit {
                code: Some(
                    0,
                ),
                signal: None,
            },
        ),
    )
    "#);
}

/// Start a long-running command through the wrapper, and return its Exec
/// and the PID of the command within the container.
async fn start_sleeper(docker: &Docker, handle: &str, state: &State) -> (Exec, String) {
    let Exec {
        mut stdin,
        mut output,
    } = docker.exec(handle, state, &WRAPPER_ARGV).await.unwrap();
    stdin
        .send(encode(&[header(
            "hang",
            "echo $$ > pid; echo started; exec sleep 600",
        )]))
        .await
        .unwrap();

    let mut buf = bytes::BytesMut::new();
    loop {
        match output.next().await.unwrap().unwrap() {
            Output::Stdout(data) => buf.extend_from_slice(&data),
            other => panic!("unexpected {other:?}"),
        }
        if let Some(frame) = sandbox_exec::Output::decode(&mut buf).unwrap() {
            assert_eq!(
                frame,
                sandbox_exec::Output::Stdout(bytes::Bytes::from_static(b"started\n"))
            );
            break;
        }
    }

    let (pid, code) = run_direct(docker, handle, &["cat", "/work/hang/pid"]).await;
    assert_eq!(code, 0);
    (Exec { stdin, output }, pid.trim().to_string())
}

/// Count running wrapper processes and processes having `pid`.
const PROCESSES: &str = r#"
    wrappers=0
    for cmdline in /proc/[0-9]*/cmdline; do
        case "$(tr '\0' ' ' < "$cmdline" 2>/dev/null)" in
            "/opt/estuary/bin/flowctl raw sandbox-exec "*) wrappers=$((wrappers + 1)) ;;
        esac
    done
    if [ -e "/proc/$1" ]; then command=1; else command=0; fi
    echo "$wrappers $command"
"#;

/// Wait until neither the wrapper nor the command at `pid` is running,
/// returning how long that took.
async fn wait_for_exit(docker: &Docker, handle: &str, pid: &str) -> std::time::Duration {
    let started = std::time::Instant::now();
    loop {
        let (counts, code) =
            run_direct(docker, handle, &["bash", "-c", PROCESSES, "processes", pid]).await;
        assert_eq!(code, 0);

        if counts.trim() == "0 0" {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "processes (wrappers, command) still running: {counts}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
async fn test_drop_exec_hangs_up() {
    let (docker, _cleanup, handle) = setup(Vec::new());
    let state = docker.provision(&handle).await.unwrap();

    let (exec, pid) = start_sleeper(&docker, &handle, &state).await;
    drop(exec);
    let elapsed = wait_for_exit(&docker, &handle, &pid).await;

    // The wrapper SIGTERMs `sleep`, which exits at once.
    eprintln!("command and wrapper exited {elapsed:?} after dropping Exec");
    assert!(elapsed < std::time::Duration::from_secs(5), "{elapsed:?}");
}

#[tokio::test]
#[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
async fn test_drop_stdin_hangs_up() {
    let (docker, _cleanup, handle) = setup(Vec::new());
    let state = docker.provision(&handle).await.unwrap();

    let (Exec { stdin, output }, pid) = start_sleeper(&docker, &handle, &state).await;
    let started = std::time::Instant::now();
    drop(stdin);

    // The wrapper treats raw EOF as a hang-up: it cancels the command and
    // fails without an Exit frame.
    let outcome = collect(output).await;
    eprintln!(
        "wrapper exited {:?} after dropping stdin",
        started.elapsed()
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));

    insta::assert_debug_snapshot!(outcome, @r#"
    Outcome {
        stdout: "",
        stderr: "",
        last: None,
        wrapper_stderr: "Error: input closed (client hung up)\n",
        wrapper_exit: Some(
            1,
        ),
    }
    "#);
    wait_for_exit(&docker, &handle, &pid).await;
}

#[tokio::test]
#[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
async fn test_drop_output_keeps_running() {
    let (docker, _cleanup, handle) = setup(Vec::new());
    let state = docker.provision(&handle).await.unwrap();

    // Sends of output are best effort, so dropping the output stream isn't a
    // teardown signal. Closing stdin still is.
    let (Exec { stdin, output }, pid) = start_sleeper(&docker, &handle, &state).await;
    drop(output);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let (counts, code) = run_direct(
        &docker,
        &handle,
        &["bash", "-c", PROCESSES, "processes", &pid],
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(
        counts.trim(),
        "1 1",
        "(wrappers, command) after dropping output"
    );

    drop(stdin);
    wait_for_exit(&docker, &handle, &pid).await;
}
