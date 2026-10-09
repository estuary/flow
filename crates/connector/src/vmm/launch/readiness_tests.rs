//! VMM readiness through the real launcher, without KVM or podman.
//!
//! A scripted engine and socket peers gate startup, exit and cancellation.
//! Every launch checks that teardown releases its resources.
//!
//! Readiness is connector-init's health check answering. Neither stderr, nor a
//! client handshake, nor a live HTTP/2 transport without that answer is enough.

use std::path::{Path, PathBuf};

const DIR: &str = "READINESS_TEST_DIR";
const IMAGE: &str = "ghcr.io/estuary/derive-python:synthetic";
/// What `readiness_engine.sh`'s `create` prints.
const CONTAINER: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

const EXITED: &str = "failed: the VMM exited before flow-connector-init started; \
                      the cause is in the preceding connector logs";
const ANSWERED: &str = "started, and answered a health check";
const PREMATURE: &str =
    "started before any response, though a test health check then reached the service";

#[test]
fn stderr_never_announces_readiness() {
    in_child("stderr_never_announces_readiness", |mut h| async move {
        let cases: &[(&str, &str)] = &[
            ("a lone space, connector-init's own marker", " "),
            (
                "an indented diagnostic",
                "  WARN devices::virtio::fs::server: request failed\n",
            ),
            (
                "a raw line and its indented continuation",
                "WARN libkrun: virtio-fs request failed\n    caused by: no such file\n",
            ),
            (
                "a panic's backtrace",
                "thread '<unnamed>' panicked at src/devices/src/virtio/fs/server.rs:120:9:\n\
                 unexpected request\n\
                 stack backtrace:\n   \
                 0: rust_begin_unwind\n   \
                 1: core::panicking::panic_fmt\n\
                 note: Some details are omitted, run with `RUST_BACKTRACE=full` for a verbose backtrace.\n",
            ),
        ];

        let (mut actual, mut expected) = (String::new(), String::new());
        for (name, stderr) in cases {
            let report = launch(
                &mut h,
                Launch {
                    stderr,
                    peer: Peer::Absent,
                    then: Then::Exit,
                },
            )
            .await;
            actual.push_str(&format!("# {name}\n{}", render(&report)));
            expected.push_str(&format!("# {name}\n{EXITED}\n"));
        }
        assert_eq!(actual, expected);
    });
}

#[test]
fn a_peer_which_only_reads_the_preface_is_not_ready() {
    in_child(
        "a_peer_which_only_reads_the_preface_is_not_ready",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: " ",
                    peer: Peer::Preface,
                    then: Then::Exit,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{EXITED}\n"));
        },
    );
}

#[test]
fn a_responding_service_is_ready_with_silent_stderr() {
    in_child(
        "a_responding_service_is_ready_with_silent_stderr",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "",
                    peer: Peer::Serving,
                    then: Then::Await,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{ANSWERED}\n"));
        },
    );
}

#[test]
fn a_held_response_is_not_ready_when_the_vmm_exits() {
    in_child(
        "a_held_response_is_not_ready_when_the_vmm_exits",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: " ",
                    peer: Peer::Held,
                    then: Then::Exit,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{EXITED}\n"));
        },
    );
}

#[test]
fn a_live_transport_without_a_response_is_not_ready() {
    in_child(
        "a_live_transport_without_a_response_is_not_ready",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: " ",
                    peer: Peer::Unanswered,
                    then: Then::Exit,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{EXITED}\n"));
        },
    );
}

#[test]
fn a_released_response_permits_startup() {
    in_child("a_released_response_permits_startup", |mut h| async move {
        let report = launch(
            &mut h,
            Launch {
                stderr: " ",
                peer: Peer::Held,
                then: Then::Release,
            },
        )
        .await;
        assert_eq!(render(&report), format!("{ANSWERED}\n"));
    });
}

#[test]
fn an_exit_while_waiting_ends_the_launch() {
    in_child(
        "an_exit_while_waiting_ends_the_launch",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "",
                    peer: Peer::Absent,
                    then: Then::Exit,
                },
            )
            .await;
            assert_eq!(
                (render(&report), report.ended.as_str()),
                (format!("{EXITED}\n"), "exit")
            );
        },
    );
}

#[test]
fn a_cancellation_while_waiting_releases_the_launch() {
    in_child(
        "a_cancellation_while_waiting_releases_the_launch",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "",
                    peer: Peer::Absent,
                    then: Then::Cancel,
                },
            )
            .await;
            // Ended by teardown's removal, not by its wait for the client.
            assert_eq!(
                (render(&report), report.ended.as_str()),
                ("cancelled\n".to_string(), "rm")
            );
        },
    );
}

// The cancelled task comes first: were its cancellation a failure, the panic
// would name it instead.
#[tokio::test]
#[should_panic(expected = "the peer failed")]
async fn stopping_peers_surfaces_a_failure_but_not_a_cancellation() {
    let failed = tokio::spawn(async { panic!("the peer failed") });
    while !failed.is_finished() {
        tokio::task::yield_now().await;
    }
    stop_peers(vec![tokio::spawn(std::future::pending()), failed]).await;
}

// On this thread, a skipped release or hold reports Answering in the same
// poll as Gated or HeldForGood, before this test resumes.
#[tokio::test(flavor = "current_thread")]
async fn the_unanswered_fixture_answers_a_received_check_only_once_released() {
    for send in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("init.sock");
        let socket = socket.to_str().unwrap();
        let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel();
        let (peer_tasks, release) = serve(Peer::Unanswered, socket, gate_tx);
        let channel = tonic::transport::Endpoint::from_shared(format!("unix:{socket}"))
            .unwrap()
            .connect()
            .await
            .unwrap();

        let mut rpc = Box::pin(check(channel));
        tokio::select! {
            received = gate_rx.recv() => {
                assert_eq!(received, Some(Checkpoint::Gated), "the fixture's gate channel closed");
            }
            answer = &mut rpc => {
                panic!("the check ended before the fixture received it: {answer}");
            }
        }
        assert_eq!(
            gate_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty),
            "the received check was answered before its release"
        );

        let release = release.unwrap();
        if send {
            release.send(()).unwrap();
            assert_eq!(gate_rx.recv().await, Some(Checkpoint::Answering));
            assert_eq!(rpc.await, ANSWERED);
        } else {
            std::mem::drop(release);
            assert_eq!(
                gate_rx.recv().await,
                Some(Checkpoint::HeldForGood),
                "a release dropped unsent answered the check"
            );
            assert_eq!(
                gate_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty),
                "the check was answered after its release was dropped"
            );
        }
        stop_peers(peer_tasks).await;
    }
}

// The paused clock jumps to the next timer only once nothing else can run.
// Dials of a socket with no listener fail at once, and are retried; a dial of
// a listener which never accepts is held. Both end at the one deadline. Only
// the outermost error is compared: hyper's keep-alive reads the real clock, so
// whether it fails a held dial in the deadline's last millisecond varies.
#[tokio::test(start_paused = true)]
async fn dials_end_at_the_ready_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let silent = dir.path().join("silent.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&silent).unwrap();

    let mut outcomes = Vec::new();
    for socket in [dir.path().join("absent.sock"), silent] {
        let started = tokio::time::Instant::now();
        let err = super::dial(socket.to_str().unwrap())
            .await
            .expect_err("nothing answers");
        outcomes.push((err.to_string(), started.elapsed()));
    }
    let timeout = "timeout waiting for the VMM to become ready".to_string();
    assert_eq!(
        outcomes,
        vec![
            (timeout.clone(), super::READY_TIMEOUT),
            (timeout, super::READY_TIMEOUT)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_health_which_is_not_serving_is_never_ready() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("init.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("", tonic_health::ServingStatus::NotServing)
        .await;
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let accepted = listener.accept().await.map(|(stream, _)| stream);
        Some((accepted, listener))
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let started = tokio::time::Instant::now();
    let err = super::dial(socket.to_str().unwrap())
        .await
        .expect_err("health is never SERVING");
    assert_eq!(
        (
            err.to_string(),
            err.root_cause().to_string(),
            started.elapsed()
        ),
        (
            "timeout waiting for the VMM to become ready".to_string(),
            "connector-init's health is NOT_SERVING".to_string(),
            super::READY_TIMEOUT
        )
    );
    stop_peers(vec![server]).await;
}

struct Launch {
    stderr: &'static str,
    peer: Peer,
    /// What the test does once stderr is written and the peer's gate passed.
    then: Then,
}

#[derive(Clone, Copy)]
enum Peer {
    /// Nothing is bound at `init.sock`.
    Absent,
    /// Accepts, reads the client's HTTP/2 preface, and never writes. Its gate
    /// passes once a preface is read.
    Preface,
    /// connector-init's health service, which writes nothing on any
    /// connection until it's released. Its gate passes once a connection is
    /// held.
    Held,
    Serving,
    /// connector-init's health service, handed every connection at once,
    /// which leaves each check unanswered until released. Its gate passes once
    /// a check is received, so HTTP/2 and gRPC both work.
    Unanswered,
}

#[derive(Clone, Copy)]
enum Then {
    Exit,
    Cancel,
    Release,
    Await,
}

#[derive(Debug, PartialEq)]
enum Checkpoint {
    Gated,
    /// An unanswered check's release was dropped unsent, and it stays held.
    HeldForGood,
    /// An unanswered check was released, and nothing else awaits its answer.
    Answering,
}

/// How a launch went, with its directory and VMM's id elided.
struct Report {
    outcome: String,
    /// What ended the engine's attached VMM: `exit` or `rm`.
    ended: String,
    left: Vec<String>,
}

fn render(Report { outcome, left, .. }: &Report) -> String {
    let mut rendered = format!("{outcome}\n");
    for left in left {
        rendered.push_str(&format!("left: {left}\n"));
    }
    rendered
}

type Events = tokio::io::Lines<tokio::io::BufReader<tokio::net::unix::pipe::Receiver>>;

struct Harness {
    dir: PathBuf,
    events: Events,
}

type Started = anyhow::Result<(
    proto_flow::runtime::Container,
    tonic::transport::Channel,
    super::Guard,
    connector_init::Codec,
)>;

/// Run `scenario` in a child of this test binary, as the test `test`. The
/// child's PATH, holding a stand-in flow-connector-init which the launch
/// copies and nothing runs, and TMPDIR are set before any thread exists.
fn in_child<F>(test: &str, scenario: impl FnOnce(Harness) -> F)
where
    F: std::future::Future<Output = ()>,
{
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::PermissionsExt;

    if let Some(dir) = std::env::var_os(DIR) {
        let dir = PathBuf::from(dir);
        std::fs::write(dir.join("ran"), test).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            // Read-write, so that the engine's writes never wait for a reader.
            let events = tokio::net::unix::pipe::OpenOptions::new()
                .read_write(true)
                .open_receiver(dir.join("events"))
                .unwrap();
            let events = tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(events));
            scenario(Harness { dir, events }).await
        });
        return;
    }

    let dir = tempfile::Builder::new()
        .prefix("connector-readiness-")
        .tempdir()
        .unwrap();
    for (name, content, mode) in [
        ("engine", include_str!("readiness_engine.sh"), 0o755),
        ("flow-connector-init", "#!/bin/sh\nexit 1\n", 0o555),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, content).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    }
    for fifo in ["events", "control"] {
        let path = std::ffi::CString::new(dir.path().join(fifo).as_os_str().as_bytes()).unwrap();
        // SAFETY: `path` is a NUL-terminated string which outlives the call.
        let made = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
        assert_eq!(made, 0, "{fifo}: {}", std::io::Error::last_os_error());
    }
    std::fs::create_dir(dir.path().join("state")).unwrap();

    let (_crate, module) = module_path!().split_once("::").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &format!("{module}::{test}"),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(DIR, dir.path())
        .env("TMPDIR", dir.path())
        .env("PATH", format!("{}:/usr/bin:/bin", dir.path().display()))
        .output()
        .unwrap();
    // A name which matched no test would run nothing, and succeed.
    let ran = std::fs::read_to_string(dir.path().join("ran")).unwrap_or_default();
    assert!(
        output.status.success() && ran == test,
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Launch a VMM whose stderr is `stderr`, with `peer` at its socket, and do
/// `then` once the gates before it pass. Returns once teardown has finished.
async fn launch(h: &mut Harness, Launch { stderr, peer, then }: Launch) -> Report {
    let dir = h.dir.clone();
    std::fs::write(dir.join("stderr"), stderr).unwrap();
    for stale in ["calls", "ended", "socket"] {
        match std::fs::remove_file(dir.join(stale)) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => panic!("removing {stale}: {e}"),
        }
    }

    let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(64);
    let (log_sink, read_through) = crate::LogSink::response(response_tx);
    // Drained, so that the pump never waits on a full channel.
    let logs = tokio::spawn(async move {
        while let Some(response) = response_rx.recv().await {
            response.unwrap();
        }
    });
    let ctx = crate::protocol::StartContext {
        container_network: String::new(),
        execution: proto_flow::flow::ConnectorExecution {
            vmm: true,
            egress: None,
        },
        log_level: ops::LogLevel::Info,
        log_sink,
        plane: crate::Plane::Local,
        process: None,
        task_name: "acmeCo/readiness".to_string(),
        vmm: None,
        secret_resolver: std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        task_update: None,
    };
    let config = crate::Vmm {
        image: "synthetic-vmm".to_string(),
        podman: dir.join("engine").to_str().unwrap().to_string(),
        state_dir: dir.join("state").to_str().unwrap().to_string(),
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
        Some(&config),
        ops::TaskType::Derivation,
        Some(IMAGE),
        &ctx.execution,
        None,
    )
    .unwrap()
    else {
        panic!("the synthetic derivation must select VMM execution");
    };

    let mut start = Box::pin(super::start(
        &ctx,
        vmm,
        eligible,
        egress,
        IMAGE,
        &crate::EMPTY_SECRETS,
        None,
    ));
    let mut outcome = None;

    if gate(
        start.as_mut(),
        &mut outcome,
        next_event(&mut h.events, "attached"),
    )
    .await
    .is_none()
    {
        panic!(
            "the launch ended before its VMM was attached: {}",
            match &outcome {
                None => "unresolved".to_string(),
                Some(Ok(_)) => "started".to_string(),
                Some(Err(err)) => format!("{err:#}"),
            }
        );
    }
    let socket = std::fs::read_to_string(dir.join("socket")).unwrap();
    let name = Path::new(&socket)
        .ancestors()
        .nth(2)
        .and_then(Path::file_name)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel();
    let (peer_tasks, mut release) = serve(peer, &socket, gate_tx);

    command(&dir, "stderr");
    // The engine reports it whatever the launch does meanwhile.
    if gate(
        start.as_mut(),
        &mut outcome,
        next_event(&mut h.events, "written"),
    )
    .await
    .is_none()
    {
        next_event(&mut h.events, "written").await;
    }
    if matches!(peer, Peer::Preface | Peer::Held | Peer::Unanswered) {
        let gated = gate(start.as_mut(), &mut outcome, gate_rx.recv()).await;
        assert_ne!(gated, Some(None), "the peer's gate channel closed");
    }
    // Record startup before a test check verifies the live transport, so later
    // exit or release cannot erase its premature outcome.
    let premature = if let (Peer::Unanswered, Some(Ok((_, channel, ..)))) = (peer, &outcome) {
        tokio::select! {
            received = gate_rx.recv() => {
                assert_eq!(received, Some(Checkpoint::Gated), "the peer's gate channel closed");
            }
            answer = check(channel.clone()) => {
                panic!("a test check ended before the service received it: {answer}");
            }
        }
        true
    } else {
        false
    };

    match then {
        Then::Exit => command(&dir, "exit 101"),
        Then::Release => {
            let release = release.take().expect("only a held service is released");
            release.send(()).unwrap();
        }
        Then::Cancel | Then::Await => (),
    }
    let outcome = match (outcome, then) {
        (Some(started), _) => Some(started),
        (None, Then::Cancel) => None,
        (None, _) => Some(start.as_mut().await),
    };
    std::mem::drop(start);

    let answers = matches!(
        (peer, then),
        (Peer::Serving, _) | (Peer::Held, Then::Release)
    );
    let outcome = match outcome {
        None => "cancelled".to_string(),
        Some(Err(err)) => format!("failed: {err:#}"),
        Some(Ok((_container, channel, guard, _codec))) => {
            // A peer which cannot answer would leave a check waiting forever.
            let outcome = if premature {
                PREMATURE.to_string()
            } else if answers {
                check(channel).await
            } else {
                "started".to_string()
            };
            std::mem::drop(guard);
            outcome
        }
    };

    // The launch's task and its pump hold the only other clones of the sink.
    std::mem::drop(ctx);
    _ = read_through.await;
    logs.await.unwrap();
    stop_peers(peer_tasks).await;
    std::mem::drop(release);

    let ended = match std::fs::read_to_string(dir.join("ended")) {
        Ok(ended) => ended.trim().to_string(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => panic!("reading ended: {e}"),
    };
    let elide = |text: &str| {
        text.replace(dir.to_str().unwrap(), "<dir>")
            .replace(&name, "fv_<id>")
    };
    Report {
        outcome: elide(&outcome),
        left: left(&dir, &name, &ended)
            .iter()
            .map(|left| elide(left))
            .collect(),
        ended,
    }
}

/// Await `gate`, unless the start resolves first, whose outcome is then kept
/// and never awaited again.
async fn gate<T>(
    start: std::pin::Pin<&mut impl std::future::Future<Output = Started>>,
    outcome: &mut Option<Started>,
    gate: impl std::future::Future<Output = T>,
) -> Option<T> {
    if outcome.is_some() {
        return None;
    }
    tokio::select! {
        value = gate => Some(value),
        started = start => {
            *outcome = Some(started);
            None
        }
    }
}

async fn next_event(events: &mut Events, want: &str) {
    let event = events
        .next_line()
        .await
        .unwrap()
        .expect("the events FIFO stays open: this process holds its write end too");
    assert_eq!(event, want, "the engine's next event");
}

/// Tell the engine's attached VMM `command`. The FIFO is opened read-write,
/// which never blocks, so that a command to a VMM which has gone is dropped.
fn command(dir: &Path, command: &str) {
    use std::io::Write;

    let mut control = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("control"))
        .unwrap();
    control
        .write_all(format!("{command}\n").as_bytes())
        .unwrap();
}

/// Bind `peer` at `socket`, sending its checkpoints to `gate_tx`. Returns its
/// tasks, and the sender which releases a held or unanswered service. A sender
/// dropped unsent holds it for good.
fn serve(
    peer: Peer,
    socket: &str,
    gate_tx: tokio::sync::mpsc::UnboundedSender<Checkpoint>,
) -> (
    Vec<tokio::task::JoinHandle<()>>,
    Option<tokio::sync::oneshot::Sender<()>>,
) {
    let (held, listener) = match peer {
        Peer::Absent => return (Vec::new(), None),
        Peer::Preface => {
            let listener = tokio::net::UnixListener::bind(socket).unwrap();
            let task = tokio::spawn(async move {
                let mut silent = Vec::new();
                loop {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut preface = [0; PREFACE.len()];
                    tokio::io::AsyncReadExt::read_exact(&mut stream, &mut preface)
                        .await
                        .unwrap();
                    assert_eq!(&preface, PREFACE);
                    silent.push(stream);
                    _ = gate_tx.send(Checkpoint::Gated);
                }
            });
            return (vec![task], None);
        }
        Peer::Held => (true, tokio::net::UnixListener::bind(socket).unwrap()),
        Peer::Serving | Peer::Unanswered => {
            (false, tokio::net::UnixListener::bind(socket).unwrap())
        }
    };

    let (incoming_tx, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (mut release_rx, stub) = match peer {
        Peer::Unanswered => {
            let release = futures::FutureExt::shared(release_rx);
            let unanswered = Some((gate_tx.clone(), release));
            (None, Stub { unanswered })
        }
        _ => (held.then_some(release_rx), Stub { unanswered: None }),
    };
    let accept = tokio::spawn(async move {
        let mut holding = held;
        let mut parked = Vec::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    if holding {
                        parked.push(stream);
                        _ = gate_tx.send(Checkpoint::Gated);
                    } else {
                        _ = incoming_tx.send(stream);
                    }
                }
                released = async { release_rx.as_mut().unwrap().await }, if release_rx.is_some() => {
                    release_rx = None;
                    if released.is_ok() {
                        holding = false;
                        for stream in parked.drain(..) {
                            _ = incoming_tx.send(stream);
                        }
                    }
                }
            }
        }
    });
    let server = tokio::spawn(async move {
        let incoming = futures::StreamExt::map(
            tokio_stream::wrappers::UnboundedReceiverStream::new(incoming_rx),
            Ok::<_, std::io::Error>,
        );
        tonic::transport::Server::builder()
            .add_service(tonic_health::pb::health_server::HealthServer::new(stub))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let released = matches!(peer, Peer::Held | Peer::Unanswered);
    (vec![accept, server], released.then_some(release_tx))
}

async fn stop_peers(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for task in tasks {
        task.abort();
        if let Err(err) = task.await {
            assert!(err.is_cancelled(), "a peer task failed: {err}");
        }
    }
}

struct Stub {
    unanswered: Option<(
        tokio::sync::mpsc::UnboundedSender<Checkpoint>,
        futures::future::Shared<tokio::sync::oneshot::Receiver<()>>,
    )>,
}

#[tonic::async_trait]
impl tonic_health::pb::health_server::Health for Stub {
    async fn check(
        &self,
        request: tonic::Request<tonic_health::pb::HealthCheckRequest>,
    ) -> Result<tonic::Response<tonic_health::pb::HealthCheckResponse>, tonic::Status> {
        let service = &request.get_ref().service;
        if !service.is_empty() {
            return Err(tonic::Status::not_found(format!(
                "expected the whole server, not {service:?}"
            )));
        }
        if let Some((checkpoints, release)) = &self.unanswered {
            _ = checkpoints.send(Checkpoint::Gated);
            if release.clone().await.is_err() {
                _ = checkpoints.send(Checkpoint::HeldForGood);
                std::future::pending::<()>().await;
            }
            _ = checkpoints.send(Checkpoint::Answering);
        }
        Ok(tonic::Response::new(
            tonic_health::pb::HealthCheckResponse {
                status: tonic_health::pb::health_check_response::ServingStatus::Serving.into(),
            },
        ))
    }

    type WatchStream =
        futures::stream::Empty<Result<tonic_health::pb::HealthCheckResponse, tonic::Status>>;

    async fn watch(
        &self,
        _request: tonic::Request<tonic_health::pb::HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("the launcher only checks"))
    }
}

async fn check(channel: tonic::transport::Channel) -> String {
    match super::serving(channel).await {
        Ok(()) => ANSWERED.to_string(),
        Err(err) => format!("started, and its health check failed: {err:#}"),
    }
}

/// Remaining resources or missing teardown actions.
fn left(dir: &Path, name: &str, ended: &str) -> Vec<String> {
    let mut left = Vec::new();

    // SAFETY: geteuid takes nothing and cannot fail.
    let mounts = dir.join(format!("connector-mounts-{}", unsafe { libc::geteuid() }));
    for owned in [dir.join("state"), mounts] {
        for entry in std::fs::read_dir(&owned).unwrap() {
            left.push(entry.unwrap().path().display().to_string());
        }
    }
    for resource in ["container", "network"] {
        if dir.join(resource).exists() {
            left.push(format!("the engine's {resource}"));
        }
    }
    let calls = std::fs::read_to_string(dir.join("calls")).unwrap();
    for removal in [
        format!("rm --force --time=0 --ignore {CONTAINER}"),
        format!("network rm {name}"),
    ] {
        if !calls.lines().any(|call| call == removal) {
            left.push(format!("no call of `{removal}`"));
        }
    }
    if !matches!(ended, "exit" | "rm") {
        left.push(format!("a VMM not ended by an exit or rm: {ended:?}"));
    }
    left
}
