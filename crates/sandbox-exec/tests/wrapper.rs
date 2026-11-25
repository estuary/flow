use sandbox_exec::{Config, Header, Input, Output, PROTOCOL_VERSION};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn test_stdout_stderr_and_exit_code() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    session
        .send(header("echo out; echo err >&2; echo more out; exit 3"))
        .await;

    insta::assert_debug_snapshot!(session.finish().await, @r#"
    Outcome {
        stdout: "out\nmore out\n",
        stderr: "err\n",
        tail: [
            Exit {
                code: Some(
                    3,
                ),
                signal: None,
            },
        ],
        result: Ok(
            (),
        ),
    }
    "#);
}

#[tokio::test]
async fn test_stdin_relay() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    session.send(header("cat; echo ' (after eof)'")).await;
    session.send(stdin(b"hello ")).await;
    session.send(stdin(b"world")).await;
    session.send(Input::StdinEof).await;

    insta::assert_debug_snapshot!(session.finish().await, @r#"
    Outcome {
        stdout: "hello world (after eof)\n",
        stderr: "",
        tail: [
            Exit {
                code: Some(
                    0,
                ),
                signal: None,
            },
        ],
        result: Ok(
            (),
        ),
    }
    "#);
}

#[tokio::test]
async fn test_child_never_reads_stdin() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    session.send(header("echo done")).await;
    let input = session.take_input();

    // Far more than a pipe buffer. The command exits without reading any of
    // it, and further Stdin frames are discarded.
    let sender = tokio::spawn(async move {
        let mut input = input;
        let chunk = vec![b'x'; 64 << 10];
        for _ in 0..32 {
            send_frame(&mut input, stdin(&chunk)).await?;
        }
        send_frame(&mut input, Input::StdinEof).await?;
        // Hold the input open: closing it would be a hang-up.
        hold_until_closed(input).await
    });

    insta::assert_debug_snapshot!(session.finish().await, @r#"
    Outcome {
        stdout: "done\n",
        stderr: "",
        tail: [
            Exit {
                code: Some(
                    0,
                ),
                signal: None,
            },
        ],
        result: Ok(
            (),
        ),
    }
    "#);
    // The wrapper may return before or after the sender is done.
    _ = sender.await.unwrap();
}

#[tokio::test]
async fn test_stdin_backpressure() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    // The command doesn't read stdin for a while. Meanwhile the client is
    // stuck writing Stdin frames, because the wrapper stopped reading at 8 MiB
    // queued. All of it reaches the command.
    session.send(header("sleep 1; wc -c")).await;
    let mut input = session.take_input();

    let sender = tokio::spawn(async move {
        let chunk = vec![b'x'; 1 << 20];
        for _ in 0..12 {
            send_frame(&mut input, stdin(&chunk)).await?;
        }
        send_frame(&mut input, Input::StdinEof).await?;
        hold_until_closed(input).await
    });

    insta::assert_debug_snapshot!(session.finish().await, @r#"
    Outcome {
        stdout: "12582912\n",
        stderr: "",
        tail: [
            Exit {
                code: Some(
                    0,
                ),
                signal: None,
            },
        ],
        result: Ok(
            (),
        ),
    }
    "#);
    _ = sender.await.unwrap();
}

#[tokio::test]
async fn test_env_home_and_cwd() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    let mut frame = header(r#"printf '%s|%s|%s\n' "$GREETING" "$HOME" "$PWD""#);
    let Input::Header(h) = &mut frame else {
        unreachable!()
    };
    h.workspace = "my-ws_1".to_string();
    h.env = [("GREETING".to_string(), "hello there".to_string())].into();
    session.send(frame).await;

    let mut outcome = session.finish().await;
    outcome.stdout = outcome
        .stdout
        .replace(root.path().to_str().unwrap(), "<root>");

    insta::assert_debug_snapshot!(outcome, @r#"
    Outcome {
        stdout: "hello there|<root>/my-ws_1|<root>/my-ws_1\n",
        stderr: "",
        tail: [
            Exit {
                code: Some(
                    0,
                ),
                signal: None,
            },
        ],
        result: Ok(
            (),
        ),
    }
    "#);
    assert!(root.path().join("my-ws_1").is_dir());
}

#[tokio::test]
async fn test_header_errors() {
    let root = tempfile::tempdir().unwrap();

    let mut unknown_version = header("true");
    if let Input::Header(h) = &mut unknown_version {
        h.version = 2;
    }
    let mut invalid_workspace = header("true");
    if let Input::Header(h) = &mut invalid_workspace {
        h.workspace = "../escape".to_string();
    }
    let mut outcomes = Vec::new();

    for first in [unknown_version, invalid_workspace, Input::StdinEof] {
        let mut session = Session::start(config(root.path()));
        session.send(first).await;
        outcomes.push(session.finish().await);
    }

    // A Header frame whose payload isn't valid JSON.
    let mut session = Session::start(config(root.path()));
    let input = session.input.as_mut().unwrap();
    input.write_all(&[1, 0, 0, 0, 2, b'{', b'}']).await.unwrap();
    outcomes.push(session.finish().await);

    insta::assert_debug_snapshot!(outcomes, @r#"
    [
        Outcome {
            stdout: "",
            stderr: "",
            tail: [
                Error {
                    message: "unsupported protocol version 2 (expected 1)",
                },
            ],
            result: Err(
                "unsupported protocol version 2 (expected 1)",
            ),
        },
        Outcome {
            stdout: "",
            stderr: "",
            tail: [
                Error {
                    message: "invalid workspace name \"../escape\": must match ^[a-z0-9][a-z0-9_-]{0,62}$",
                },
            ],
            result: Err(
                "invalid workspace name \"../escape\": must match ^[a-z0-9][a-z0-9_-]{0,62}$",
            ),
        },
        Outcome {
            stdout: "",
            stderr: "",
            tail: [
                Error {
                    message: "first frame must be a Header",
                },
            ],
            result: Err(
                "first frame must be a Header",
            ),
        },
        Outcome {
            stdout: "",
            stderr: "",
            tail: [
                Error {
                    message: "failed to decode Header frame: missing field `version` at line 1 column 2",
                },
            ],
            result: Err(
                "failed to decode Header frame: missing field `version` at line 1 column 2",
            ),
        },
    ]
    "#);

    // No workspace directory was created.
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn test_workspace_cannot_be_created() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("not-a-dir");
    std::fs::write(&file, "").unwrap();

    let mut session = Session::start(config(&file));
    session.send(header("true")).await;

    let mut outcome = session.finish().await;
    for frame in &mut outcome.tail {
        if let Output::Error { message } = frame {
            *message = message.replace(root.path().to_str().unwrap(), "<root>");
        }
    }
    if let Err(err) = &mut outcome.result {
        *err = err.replace(root.path().to_str().unwrap(), "<root>");
    }

    insta::assert_debug_snapshot!(outcome, @r#"
    Outcome {
        stdout: "",
        stderr: "",
        tail: [
            Error {
                message: "failed to create workspace directory <root>/not-a-dir/ws: Not a directory (os error 20)",
            },
        ],
        result: Err(
            "failed to create workspace directory <root>/not-a-dir/ws: Not a directory (os error 20)",
        ),
    }
    "#);
}

#[tokio::test]
async fn test_skeleton_is_copied_into_new_workspace() {
    let root = tempfile::tempdir().unwrap();
    let skel = tempfile::tempdir().unwrap();
    write_skeleton(skel.path());

    let mut session = Session::start(Config {
        workspace_skeleton: skel.path().to_path_buf(),
        ..config(root.path())
    });
    // Copies are writable where their mode says so, and the read-only
    // directory's contents were copied before its mode was applied.
    session
        .send(header(
            "echo more >> .config/flowctl/local.json && cat link && cat locked/file && cat bin/tool.sh",
        ))
        .await;
    let outcome = session.finish().await;

    insta::assert_debug_snapshot!((outcome, tree(&root.path().join("ws"))), @r#"
    (
        Outcome {
            stdout: "{}\nmore\nlocked\n#!/bin/sh\n",
            stderr: "",
            tail: [
                Exit {
                    code: Some(
                        0,
                    ),
                    signal: None,
                },
            ],
            result: Ok(
                (),
            ),
        },
        [
            ".config 755",
            ".config/flowctl 750",
            ".config/flowctl/local.json 644",
            "bin 755",
            "bin/tool.sh 755",
            "dangling -> /does/not/exist",
            "link -> .config/flowctl/local.json",
            "locked 555",
            "locked/file 444",
            "private 700",
            "private/secret 600",
        ],
    )
    "#);
    // No temporary directory remains.
    assert_eq!(entries(root.path()), vec!["ws"]);
}

#[tokio::test]
async fn test_skeleton_does_not_modify_existing_workspace() {
    let root = tempfile::tempdir().unwrap();
    let skel = tempfile::tempdir().unwrap();
    write_skeleton(skel.path());
    std::fs::create_dir(root.path().join("ws")).unwrap();
    std::fs::write(root.path().join("ws/mine"), "mine").unwrap();

    let mut session = Session::start(Config {
        workspace_skeleton: skel.path().to_path_buf(),
        ..config(root.path())
    });
    session.send(header("cat mine")).await;
    let outcome = session.finish().await;

    assert_eq!(outcome.stdout, "mine");
    assert!(outcome.result.is_ok(), "{outcome:?}");
    assert_eq!(entries(&root.path().join("ws")), vec!["mine"]);
    assert_eq!(entries(root.path()), vec!["ws"]);
}

#[tokio::test]
async fn test_missing_skeleton_means_empty_workspace() {
    let root = tempfile::tempdir().unwrap();

    let mut session = Session::start(Config {
        workspace_skeleton: root.path().join(".missing"),
        ..config(root.path())
    });
    session.send(header("ls -A")).await;
    let outcome = session.finish().await;

    assert_eq!(outcome.stdout, "");
    assert!(outcome.result.is_ok(), "{outcome:?}");
    assert_eq!(entries(root.path()), vec!["ws"]);
}

#[tokio::test]
async fn test_skeleton_copy_failure() {
    let root = tempfile::tempdir().unwrap();
    let skel = tempfile::tempdir().unwrap();
    write_skeleton(skel.path());
    rustix::fs::mknodat(
        rustix::fs::CWD,
        &skel.path().join("private/fifo"),
        rustix::fs::FileType::Fifo,
        rustix::fs::Mode::from_raw_mode(0o644),
        0,
    )
    .unwrap();

    let mut session = Session::start(Config {
        workspace_skeleton: skel.path().to_path_buf(),
        ..config(root.path())
    });
    session.send(header("true")).await;

    let mut outcome = session.finish().await;
    for frame in &mut outcome.tail {
        if let Output::Error { message } = frame {
            *message = message
                .replace(root.path().to_str().unwrap(), "<root>")
                .replace(skel.path().to_str().unwrap(), "<skel>");
        }
    }
    outcome.result = Err("elided".to_string());

    insta::assert_debug_snapshot!(outcome, @r#"
    Outcome {
        stdout: "",
        stderr: "",
        tail: [
            Error {
                message: "failed to create workspace directory <root>/ws: failed to copy workspace skeleton <skel>: <skel>/private/fifo is not a directory, regular file, or symlink",
            },
        ],
        result: Err(
            "elided",
        ),
    }
    "#);
    // The partial copy was removed, including its read-only directory.
    assert_eq!(entries(root.path()), Vec::<String>::new());
}

/// Commands which start at once in the same new workspace each observe the
/// whole skeleton, all in the same directory, and no temporary directory
/// remains.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_first_commands() {
    let root = tempfile::tempdir().unwrap();
    let skel = tempfile::tempdir().unwrap();
    write_skeleton(skel.path());
    // Enough files that the copies of concurrent commands overlap.
    std::fs::create_dir(skel.path().join("many")).unwrap();
    for i in 0..1000 {
        std::fs::write(skel.path().join(format!("many/{i}")), "x").unwrap();
    }
    let expected = tree(skel.path());

    let mut sessions = Vec::new();
    for _ in 0..16 {
        sessions.push(Session::start(Config {
            workspace_skeleton: skel.path().to_path_buf(),
            ..config(root.path())
        }));
    }
    let command = r#"stat -c %i . && find . -mindepth 1 | wc -l"#;
    for session in &mut sessions {
        session.send(header(command)).await;
    }

    let mut stdouts = std::collections::BTreeSet::new();
    for session in sessions {
        let outcome = session.finish().await;
        assert!(outcome.result.is_ok(), "{outcome:?}");
        stdouts.insert(outcome.stdout);
    }

    let inode =
        std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(root.path().join("ws")).unwrap());
    let expected_stdout = format!("{inode}\n{}\n", expected.len());
    assert_eq!(stdouts, [expected_stdout].into());
    assert_eq!(tree(&root.path().join("ws")), expected);
    assert_eq!(entries(root.path()), vec!["ws"]);
}

#[tokio::test]
async fn test_hang_up_kills_command_which_ignores_sigterm() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    // SIG_IGN is inherited, so the `sleep` children ignore SIGTERM as well.
    session
        .send(header(
            "trap '' TERM; echo $$; while :; do sleep 0.05; done",
        ))
        .await;
    let pid = session.read_pid().await;

    let hung_up = std::time::Instant::now();
    drop(session.take_input()); // Raw EOF.
    let outcome = session.finish().await;

    assert!(hung_up.elapsed() >= Duration::from_millis(500)); // The kill grace.
    assert_gone(pid).await;
    insta::assert_debug_snapshot!(outcome, @r#"
    Outcome {
        stdout: "",
        stderr: "",
        tail: [],
        result: Err(
            "input closed (client hung up)",
        ),
    }
    "#);
}

#[tokio::test]
async fn test_output_writes_are_best_effort() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    // The client stops reading output, but holds its input open. The command
    // runs to completion, and only the Exit frame's write fails.
    session
        .send(header("echo started; sleep 0.3; echo more; touch done"))
        .await;
    assert!(matches!(session.next().await, Some(Output::Stdout(_))));
    let Session {
        input,
        output,
        task,
        ..
    } = session;
    drop(output);

    let result = task.await.unwrap().map_err(|err| format!("{err:#}"));
    drop(input);

    assert!(root.path().join("ws/done").exists());
    insta::assert_debug_snapshot!(result, @r#"
    Err(
        "failed to write Exit frame: broken pipe",
    )
    "#);
}

#[tokio::test]
async fn test_cancel_future_cancels() {
    let root = tempfile::tempdir().unwrap();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    let mut session = Session::start_with_cancel(config(root.path()), async move {
        _ = cancel_rx.await;
    });

    session.send(header("echo $$; sleep 30")).await;
    let pid = session.read_pid().await;
    drop(cancel_tx);

    let outcome = session.finish().await;
    assert_gone(pid).await;
    insta::assert_debug_snapshot!(outcome, @r#"
    Outcome {
        stdout: "",
        stderr: "",
        tail: [],
        result: Err(
            "cancelled",
        ),
    }
    "#);
}

#[tokio::test]
async fn test_background_child_is_killed_at_exit() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(Config {
        drain_timeout: Duration::from_secs(10),
        ..config(root.path())
    });

    // The background `sleep` holds stdout open. The process-group kill closes
    // it, so the drain completes well before its timeout.
    session.send(header("sleep 30 & echo $!")).await;

    let started = std::time::Instant::now();
    let outcome = session.finish().await;
    assert!(started.elapsed() < Duration::from_secs(5));

    let pid = outcome.stdout.trim().parse().unwrap();
    assert_gone(pid).await;
    insta::assert_debug_snapshot!(outcome.tail, @r#"
    [
        Exit {
            code: Some(
                0,
            ),
            signal: None,
        },
    ]
    "#);
}

#[tokio::test]
async fn test_detached_child_does_not_stall_exit() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(Config {
        drain_timeout: Duration::from_millis(300),
        ..config(root.path())
    });

    // `setsid` moves `sleep` into its own session, beyond the reach of the
    // process-group kill, and it holds stdout open. The command waits until
    // it's moved: otherwise the group kill can race and reach it first.
    session
        .send(header(
            r#"setsid sleep 30 & pid=$!
            until [ "$(ps -o sid= -p $pid | tr -d ' ')" = "$pid" ]; do sleep 0.01; done
            echo $pid"#,
        ))
        .await;

    let started = std::time::Instant::now();
    let outcome = session.finish().await;
    let _cleanup = KillOnDrop(outcome.stdout.trim().parse().unwrap());

    assert!(started.elapsed() >= Duration::from_millis(300));
    assert!(started.elapsed() < Duration::from_secs(5));
    insta::assert_debug_snapshot!(outcome.tail, @r#"
    [
        Exit {
            code: Some(
                0,
            ),
            signal: None,
        },
    ]
    "#);
}

#[tokio::test]
async fn test_death_by_signal() {
    let root = tempfile::tempdir().unwrap();
    let mut session = Session::start(config(root.path()));

    session.send(header("kill -KILL $$")).await;

    insta::assert_debug_snapshot!(session.finish().await, @r#"
    Outcome {
        stdout: "",
        stderr: "",
        tail: [
            Exit {
                code: None,
                signal: Some(
                    9,
                ),
            },
        ],
        result: Ok(
            (),
        ),
    }
    "#);
}

fn config(root: &std::path::Path) -> Config {
    Config {
        workspace_root: root.to_path_buf(),
        workspace_skeleton: root.join(".no-skeleton"),
        kill_grace: Duration::from_millis(500),
        drain_timeout: Duration::from_secs(1),
    }
}

/// Populate skeleton directory `dir` with a directory, regular file, and
/// symlink of each interesting kind.
fn write_skeleton(dir: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let mode = |path: &str, mode: u32| {
        std::fs::set_permissions(dir.join(path), std::fs::Permissions::from_mode(mode)).unwrap()
    };

    std::fs::create_dir_all(dir.join(".config/flowctl")).unwrap();
    std::fs::write(dir.join(".config/flowctl/local.json"), "{}\n").unwrap();
    std::fs::create_dir(dir.join("bin")).unwrap();
    std::fs::write(dir.join("bin/tool.sh"), "#!/bin/sh\n").unwrap();
    std::fs::create_dir(dir.join("private")).unwrap();
    std::fs::write(dir.join("private/secret"), "").unwrap();
    std::fs::create_dir(dir.join("locked")).unwrap();
    std::fs::write(dir.join("locked/file"), "locked\n").unwrap();
    std::os::unix::fs::symlink(".config/flowctl/local.json", dir.join("link")).unwrap();
    std::os::unix::fs::symlink("/does/not/exist", dir.join("dangling")).unwrap();

    mode(".config", 0o755);
    mode(".config/flowctl", 0o750);
    mode(".config/flowctl/local.json", 0o644);
    mode("bin", 0o755);
    mode("bin/tool.sh", 0o755);
    mode("private", 0o700);
    mode("private/secret", 0o600);
    mode("locked/file", 0o444);
    mode("locked", 0o555);
}

/// Each entry under `dir`, recursively and sorted, with its permission bits or
/// symlink target. Asserts that each is owned by us.
fn tree(dir: &std::path::Path) -> Vec<String> {
    use std::os::unix::fs::MetadataExt;

    fn walk(base: &std::path::Path, dir: &std::path::Path, out: &mut Vec<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = std::fs::symlink_metadata(&path).unwrap();
            assert_eq!(meta.uid(), rustix::process::getuid().as_raw());
            let name = path.strip_prefix(base).unwrap().display().to_string();

            if meta.file_type().is_symlink() {
                let target = std::fs::read_link(&path).unwrap();
                out.push(format!("{name} -> {}", target.display()));
            } else {
                out.push(format!("{name} {:o}", meta.mode() & 0o7777));
            }
            if meta.is_dir() {
                walk(base, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// Names of the entries of `dir`, sorted.
fn entries(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn header(command: &str) -> Input {
    Input::Header(Header {
        version: PROTOCOL_VERSION,
        workspace: "ws".to_string(),
        command: command.to_string(),
        env: Default::default(),
    })
}

fn stdin(data: &[u8]) -> Input {
    Input::Stdin(bytes::Bytes::copy_from_slice(data))
}

/// A wrapper `run` on its own task, and the client's ends of its input and
/// output streams.
struct Session {
    input: Option<tokio::io::DuplexStream>,
    output: tokio::io::DuplexStream,
    out_buf: bytes::BytesMut,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// Everything a Session produced. Stdout and stderr chunks are concatenated
/// per stream, because their chunking and interleaving are not deterministic.
#[derive(Debug)]
#[allow(dead_code)] // Fields are read through Debug, by snapshots.
struct Outcome {
    stdout: String,
    stderr: String,
    tail: Vec<Output>,
    result: Result<(), String>,
}

impl Session {
    fn start(config: Config) -> Self {
        Self::start_with_cancel(config, std::future::pending())
    }

    fn start_with_cancel(
        config: Config,
        cancel: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Self {
        let (input, wrapper_input) = tokio::io::duplex(64 << 10);
        let (wrapper_output, output) = tokio::io::duplex(64 << 10);

        let task = tokio::spawn(async move {
            sandbox_exec::run(&config, wrapper_input, wrapper_output, cancel).await
        });

        Self {
            input: Some(input),
            output,
            out_buf: bytes::BytesMut::new(),
            task,
        }
    }

    async fn send(&mut self, frame: Input) {
        send_frame(self.input.as_mut().unwrap(), frame)
            .await
            .unwrap();
    }

    fn take_input(&mut self) -> tokio::io::DuplexStream {
        self.input.take().unwrap()
    }

    async fn next(&mut self) -> Option<Output> {
        loop {
            if let Some(frame) = Output::decode(&mut self.out_buf).unwrap() {
                return Some(frame);
            }
            if self.output.read_buf(&mut self.out_buf).await.unwrap() == 0 {
                assert!(self.out_buf.is_empty(), "output ended within a frame");
                return None;
            }
        }
    }

    /// Read the first line of stdout, which the command uses to report a pid.
    async fn read_pid(&mut self) -> i32 {
        let mut line = String::new();
        while !line.ends_with('\n') {
            match self.next().await {
                Some(Output::Stdout(data)) => line.push_str(std::str::from_utf8(&data).unwrap()),
                other => panic!("expected Stdout, got {other:?}"),
            }
        }
        line.trim().parse().unwrap()
    }

    async fn finish(mut self) -> Outcome {
        let (mut stdout, mut stderr, mut tail) = (String::new(), String::new(), Vec::new());

        while let Some(frame) = self.next().await {
            match frame {
                Output::Stdout(data) => stdout.push_str(std::str::from_utf8(&data).unwrap()),
                Output::Stderr(data) => stderr.push_str(std::str::from_utf8(&data).unwrap()),
                frame => tail.push(frame),
            }
        }
        let result = self.task.await.unwrap().map_err(|err| format!("{err:#}"));

        Outcome {
            stdout,
            stderr,
            tail,
            result,
        }
    }
}

async fn send_frame(input: &mut tokio::io::DuplexStream, frame: Input) -> std::io::Result<()> {
    let mut buf = bytes::BytesMut::new();
    frame.encode(&mut buf).unwrap();
    input.write_all(&buf).await
}

/// Hold `input` open until the wrapper drops its end. The wrapper never writes
/// to it, so a read returns only at EOF.
async fn hold_until_closed(mut input: tokio::io::DuplexStream) -> std::io::Result<()> {
    let mut buf = [0; 1];
    input.read(&mut buf).await.map(|_| ())
}

/// Wait for `pid` to exit. A zombie counts: it's exited, and awaits reaping by
/// init.
async fn assert_gone(pid: i32) {
    for _ in 0..100 {
        let gone = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => true,
            // The state follows the parenthesized command name.
            Ok(stat) => stat.rsplit_once(") ").unwrap().1.starts_with('Z'),
        };
        if gone {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("process {pid} is still running");
}

struct KillOnDrop(i32);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let pid = rustix::process::Pid::from_raw(self.0).unwrap();
        _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL);
    }
}
