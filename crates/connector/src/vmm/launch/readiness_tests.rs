//! VMM readiness through the real launcher, without KVM or podman.
//!
//! A scripted engine and socket peers gate startup, exit and cancellation.
//! Every launch checks that teardown releases its resources.
//!
//! Readiness is connector-init's health check answering. Neither stderr, nor a
//! client handshake, nor a live HTTP/2 transport without that answer is enough.
//!
//! A scripted peer also serves a derive connector, whose Spec is the session's
//! first RPC. Its guest can die once the launcher has its SERVING answer.

use std::path::{Path, PathBuf};

const DIR: &str = "READINESS_TEST_DIR";
const IMAGE: &str = "ghcr.io/estuary/derive-python:synthetic";
/// What `readiness_engine.sh`'s `create` prints.
const CONTAINER: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

const EXITED: &str = "failed: the VMM exited (exit status: 101) before flow-connector-init \
                      answered its health check; the cause is in the preceding connector logs";
const ANSWERED: &str = "started, and answered a health check";
const PREMATURE: &str =
    "started before any response, though a test health check then reached the service";
/// How long the session's first RPC may wait on a guest which died.
const SPEC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// How long a launch's teardown may take once its session ends.
const RELEASED: std::time::Duration = std::time::Duration::from_secs(60);
const DIED: &str = "the guest died, and a redial was refused";
const FAILED: &str = "Spec failed";

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

#[test]
fn a_spec_follows_a_health_which_became_serving() {
    in_child(
        "a_spec_follows_a_health_which_became_serving",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "",
                    peer: Peer::Scripted { not_serving: 1 },
                    then: Then::Spec,
                },
            )
            .await;
            assert_eq!(
                (render(&report), report.ended.as_str()),
                (answered_spec(2), "rm")
            );
        },
    );
}

// Each NOT_SERVING answer ends a dial, which the launcher retries on a new
// connection.
#[test]
fn a_flapping_health_is_redialed_until_serving() {
    in_child(
        "a_flapping_health_is_redialed_until_serving",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "",
                    peer: Peer::Scripted { not_serving: 5 },
                    then: Then::Spec,
                },
            )
            .await;
            assert_eq!(
                (render(&report), report.ended.as_str()),
                (answered_spec(6), "rm")
            );
        },
    );
}

// Readiness is the launcher's own decoded SERVING, after which the guest owes
// nothing: its death must fail the session's first RPC, whether before that
// Spec is sent or while it is held unanswered, and release the launch. A
// fresh launch then answers its Spec.
#[test]
fn a_guest_dead_once_serving_answers_no_spec() {
    in_child(
        "a_guest_dead_once_serving_answers_no_spec",
        |mut h| async move {
            let (mut actual, mut expected) = (String::new(), String::new());
            for (name, then, received) in [
                ("before its Spec is sent", Then::DieBeforeSpec, 0),
                ("while its Spec is held", Then::DieHoldingSpec, 1),
            ] {
                let died = launch(
                    &mut h,
                    Launch {
                        stderr: "",
                        peer: Peer::Scripted { not_serving: 1 },
                        then,
                    },
                )
                .await;
                let fresh = launch(
                    &mut h,
                    Launch {
                        stderr: "",
                        peer: Peer::Scripted { not_serving: 1 },
                        then: Then::Spec,
                    },
                )
                .await;
                actual.push_str(&format!(
                    "# {name}\n{}ended: {}\n# then a fresh launch\n{}ended: {}\n",
                    render(&died),
                    died.ended,
                    render(&fresh),
                    fresh.ended
                ));
                let held = if received == 1 {
                    "Spec received and held\n"
                } else {
                    ""
                };
                expected.push_str(&format!(
                    "# {name}\n{}{held}{DIED}\n{FAILED}\n\
                     {received} Specs received, 0 answered\nended: exit\n\
                     # then a fresh launch\n{}ended: rm\n",
                    started(2),
                    answered_spec(2),
                ));
            }
            assert_eq!(actual, expected);
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

#[tokio::test]
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

    let channel = tonic::transport::Endpoint::from_shared(format!("unix:{}", socket.display()))
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert_eq!(
        super::serving(channel).await.unwrap_err().to_string(),
        "connector-init's health is NOT_SERVING"
    );

    stop_peers(vec![server]).await;
}

// Use real time: a paused clock can advance past deadlines while hyper's
// socket IO is still pending.
#[tokio::test]
async fn a_dial_held_unanswered_is_abandoned_for_the_next() {
    const REAPER: std::time::Duration = std::time::Duration::from_secs(5);

    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("init.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let (_reporter, health) = tonic_health::server::health_reporter();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let incoming = futures::stream::unfold(
        (listener, Some(closed_tx)),
        |(listener, mut hold)| async move {
            loop {
                let accepted = listener.accept().await.map(|(stream, _)| stream);
                if let Some(closed_tx) = hold.take() {
                    let mut stream = accepted.unwrap();
                    tokio::spawn(async move {
                        // Read without answering, so EOF proves the client abandoned
                        // the connection before the simulated reaper closed it.
                        let closed = tokio::time::timeout(
                            REAPER,
                            tokio::io::copy(&mut stream, &mut tokio::io::sink()),
                        )
                        .await;
                        closed_tx.send(closed).unwrap();
                    });
                } else {
                    return Some((accepted, (listener, hold)));
                }
            }
        },
    );
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(health)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    let started = std::time::Instant::now();
    super::dial(socket.to_str().unwrap())
        .await
        .expect("the second dial is answered");
    let elapsed = started.elapsed();
    assert!(
        elapsed < REAPER,
        "the held dial was waited out: ready after {elapsed:?}"
    );
    closed_rx
        .await
        .expect("the held connection was observed")
        .expect("the client must close the abandoned connection before the reaper")
        .expect("reading the held connection through EOF");
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
    /// connector-init's health, NOT_SERVING for its first `not_serving` checks
    /// and SERVING after, beside a derive connector which holds each Spec
    /// until released. Its gate passes once a Spec is received.
    Scripted {
        not_serving: usize,
    },
}

#[derive(Clone, Copy)]
enum Then {
    Exit,
    Cancel,
    Release,
    Await,
    /// Once started, exchange a Spec with a scripted peer.
    Spec,
    /// Once started, kill a scripted peer's guest, then send a Spec.
    DieBeforeSpec,
    /// Once started, send a Spec, and kill the guest once its peer holds it.
    DieHoldingSpec,
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
    // The child's timings, for a run which shows passing tests' output.
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
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
        memory_bytes: 512 << 20,
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
    let (mut peer_tasks, mut release, scripted) = match peer {
        Peer::Scripted { not_serving } => {
            let (tasks, release, scripted) = serve_scripted(not_serving, &socket, gate_tx);
            (tasks, Some(release), Some(scripted))
        }
        peer => {
            let (tasks, release) = serve(peer, &socket, gate_tx);
            (tasks, release, None)
        }
    };
    let served = std::time::Instant::now();

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
        Then::Cancel | Then::Await | Then::Spec | Then::DieBeforeSpec | Then::DieHoldingSpec => (),
    }
    let outcome = match (outcome, then) {
        (Some(started), _) => Some(started),
        (None, Then::Cancel) => None,
        (None, _) => Some(start.as_mut().await),
    };
    let dialed = served.elapsed();
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
            let outcome = if let Some(scripted) = &scripted {
                exchange(
                    &dir,
                    &socket,
                    then,
                    scripted,
                    dialed,
                    channel,
                    &mut gate_rx,
                    &mut peer_tasks,
                    &mut release,
                )
                .await
            } else if premature {
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
    let begun = std::time::Instant::now();
    _ = tokio::time::timeout(RELEASED, read_through)
        .await
        .expect("the launch's teardown ends within RELEASED");
    eprintln!("readiness: teardown took {:?}", begun.elapsed());
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
        Peer::Scripted { .. } => unreachable!("served by serve_scripted"),
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

/// A scripted peer's counts, and its connections, which a dying guest ends.
#[derive(Clone)]
struct Scripted {
    not_serving: usize,
    checks: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    received: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    answered: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Duplicates of each accepted connection's descriptor.
    connections: std::sync::Arc<std::sync::Mutex<Vec<std::os::unix::net::UnixStream>>>,
    gate_tx: tokio::sync::mpsc::UnboundedSender<Checkpoint>,
    release: futures::future::Shared<tokio::sync::oneshot::Receiver<()>>,
}

/// Bind a scripted peer at `socket`. Returns its tasks, whose abort closes
/// its listener, and the sender which releases its held Specs.
fn serve_scripted(
    not_serving: usize,
    socket: &str,
    gate_tx: tokio::sync::mpsc::UnboundedSender<Checkpoint>,
) -> (
    Vec<tokio::task::JoinHandle<()>>,
    tokio::sync::oneshot::Sender<()>,
    Scripted,
) {
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let scripted = Scripted {
        not_serving,
        checks: Default::default(),
        received: Default::default(),
        answered: Default::default(),
        connections: Default::default(),
        gate_tx,
        release: futures::FutureExt::shared(release_rx),
    };

    let (incoming_tx, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
    let connections = scripted.connections.clone();
    let accept = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = stream.into_std().unwrap();
            connections
                .lock()
                .unwrap()
                .push(stream.try_clone().unwrap());
            _ = incoming_tx.send(tokio::net::UnixStream::from_std(stream).unwrap());
        }
    });
    let server = tokio::spawn({
        let scripted = scripted.clone();
        async move {
            let incoming = futures::StreamExt::map(
                tokio_stream::wrappers::UnboundedReceiverStream::new(incoming_rx),
                Ok::<_, std::io::Error>,
            );
            tonic::transport::Server::builder()
                .add_service(tonic_health::pb::health_server::HealthServer::new(
                    scripted.clone(),
                ))
                .add_service(proto_grpc::derive::connector_server::ConnectorServer::new(
                    scripted,
                ))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        }
    });
    (vec![accept, server], release_tx, scripted)
}

/// Exchange a Spec over a scripted peer's started launch as `then` directs.
/// Returns its milestones, in order, as lines of an outcome.
#[allow(clippy::too_many_arguments)]
async fn exchange(
    dir: &Path,
    socket: &str,
    then: Then,
    scripted: &Scripted,
    dialed: std::time::Duration,
    channel: tonic::transport::Channel,
    gate_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Checkpoint>,
    peer_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    release: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> String {
    use std::sync::atomic::Ordering::SeqCst;

    let checks = scripted.checks.load(SeqCst);
    let accepted = scripted.connections.lock().unwrap().len();
    // Each dial answered NOT_SERVING waits DIAL_INTERVAL before the next.
    let floor = super::DIAL_INTERVAL * scripted.not_serving as u32;
    eprintln!("readiness: started {dialed:?} after the peer was bound");
    assert!(
        dialed >= floor && dialed < super::READY_TIMEOUT,
        "started {dialed:?} after the peer was bound, outside [{floor:?}, {:?})",
        super::READY_TIMEOUT
    );
    assert_eq!(checks, accepted, "a health check per dial's connection");
    let mut milestones = started(checks);

    let mut spec = Box::pin(crate::protocol::spec_rpc::<crate::derive::Derive>(
        channel, 0,
    ));
    if matches!(then, Then::DieBeforeSpec) {
        milestones.push_str(&die(dir, socket, peer_tasks, scripted).await);
    } else {
        tokio::select! {
            received = gate_rx.recv() => {
                assert_eq!(received, Some(Checkpoint::Gated), "the peer's gate channel closed");
            }
            spec = &mut spec => panic!("the Spec ended before the peer received it: {spec:?}"),
            () = tokio::time::sleep(SPEC_TIMEOUT) => {
                panic!("the peer received no Spec within {SPEC_TIMEOUT:?}")
            }
        }
        milestones.push_str("Spec received and held\n");
        match then {
            Then::Spec => release
                .take()
                .expect("a scripted peer is released once")
                .send(())
                .unwrap(),
            Then::DieHoldingSpec => {
                milestones.push_str(&die(dir, socket, peer_tasks, scripted).await)
            }
            _ => unreachable!("only a Spec, or a death, follows a scripted start"),
        }
    }

    let begun = std::time::Instant::now();
    let spec = tokio::time::timeout(SPEC_TIMEOUT, spec).await;
    eprintln!(
        "readiness: the Spec ended after {:?}: {spec:?}",
        begun.elapsed()
    );
    milestones.push_str(&match spec {
        Err(_elapsed) => format!("Spec still pending after {SPEC_TIMEOUT:?}\n"),
        Ok(Ok(crate::proto::response::started::Spec::Derive(spec))) => {
            format!("Spec answered with protocol {}\n", spec.protocol)
        }
        Ok(Ok(spec)) => format!("Spec answered as another protocol: {spec:?}\n"),
        Ok(Err(_)) => format!("{FAILED}\n"),
    });
    milestones.push_str(&format!(
        "{} Specs received, {} answered\n",
        scripted.received.load(SeqCst),
        scripted.answered.load(SeqCst)
    ));
    milestones.trim_end().to_string()
}

/// Kill a scripted peer's guest: its listener closes, so a redial is refused,
/// its connections end, and its VMM exits.
async fn die(
    dir: &Path,
    socket: &str,
    peer_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    scripted: &Scripted,
) -> String {
    stop_peers(std::mem::take(peer_tasks)).await;
    for connection in scripted.connections.lock().unwrap().iter() {
        match connection.shutdown(std::net::Shutdown::Both) {
            Ok(()) => (),
            // The launcher already closed a NOT_SERVING dial's connection.
            Err(e) if e.kind() == std::io::ErrorKind::NotConnected => (),
            Err(e) => panic!("ending a connection: {e}"),
        }
    }
    command(dir, "exit 101");
    let redial = std::os::unix::net::UnixStream::connect(socket).map(|_| ());
    assert_eq!(
        redial.map_err(|e| e.kind()),
        Err(std::io::ErrorKind::ConnectionRefused),
        "a dead guest's socket must refuse a redial"
    );
    format!("{DIED}\n")
}

fn started(checks: usize) -> String {
    format!("started after {checks} health checks over {checks} connections\n")
}

fn answered_spec(checks: usize) -> String {
    format!(
        "{}Spec received and held\nSpec answered with protocol 3032023\n\
         1 Specs received, 1 answered\n",
        started(checks)
    )
}

#[tonic::async_trait]
impl tonic_health::pb::health_server::Health for Scripted {
    async fn check(
        &self,
        _request: tonic::Request<tonic_health::pb::HealthCheckRequest>,
    ) -> Result<tonic::Response<tonic_health::pb::HealthCheckResponse>, tonic::Status> {
        use tonic_health::pb::health_check_response::ServingStatus;

        let prior = self
            .checks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let status = if prior < self.not_serving {
            ServingStatus::NotServing
        } else {
            ServingStatus::Serving
        };
        Ok(tonic::Response::new(
            tonic_health::pb::HealthCheckResponse {
                status: status.into(),
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

#[tonic::async_trait]
impl proto_grpc::derive::connector_server::Connector for Scripted {
    type DeriveStream =
        futures::stream::BoxStream<'static, Result<proto_flow::derive::Response, tonic::Status>>;

    async fn derive(
        &self,
        request: tonic::Request<tonic::Streaming<proto_flow::derive::Request>>,
    ) -> Result<tonic::Response<Self::DeriveStream>, tonic::Status> {
        use proto_flow::derive::{request, response};

        let first = request.into_inner().message().await?;
        let Some(proto_flow::derive::Request {
            kind: Some(request::Kind::Spec(_)),
            ..
        }) = first
        else {
            return Err(tonic::Status::invalid_argument(format!(
                "expected a Spec, not {first:?}"
            )));
        };
        self.received
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        _ = self.gate_tx.send(Checkpoint::Gated);
        if self.release.clone().await.is_err() {
            std::future::pending::<()>().await;
        }
        self.answered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let response = proto_flow::derive::Response {
            kind: Some(response::Kind::Spec(Box::new(response::Spec {
                protocol: 3032023,
                config_schema_json: "{}".into(),
                resource_config_schema_json: "{}".into(),
                documentation_url: "https://example.com".to_string(),
                ..Default::default()
            }))),
            ..Default::default()
        };
        Ok(tonic::Response::new(Box::pin(futures::stream::once(
            async move { Ok(response) },
        ))))
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
    for resource in ["container", "network", "volume"] {
        if dir.join(resource).exists() {
            left.push(format!("the engine's {resource}"));
        }
    }
    let calls = std::fs::read_to_string(dir.join("calls")).unwrap();
    for removal in [
        format!("rm --force --time=0 --ignore {CONTAINER}"),
        format!("network rm {name}"),
        format!("volume rm {name}"),
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
