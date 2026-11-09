//! VMM readiness through the real launcher, without KVM or podman.
//!
//! A scripted engine and socket peers gate startup, exit and cancellation.
//! Every launch checks that teardown releases its resources.

use std::path::{Path, PathBuf};

const DIR: &str = "READINESS_TEST_DIR";
const IMAGE: &str = "ghcr.io/estuary/derive-python:synthetic";
/// What `readiness_engine.sh`'s `create` prints.
const CONTAINER: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

const EXITED: &str = "failed: the VMM exited (exit status: 101) before flow-connector-init \
                      was ready; the cause is in the preceding connector logs";
// Stderr can close before the attached client's exit status becomes available.
const CLOSED: &str = "failed: the VMM closed stderr before flow-connector-init started; \
                      the cause is in the preceding connector logs";
const STARTED: &str = "started";
/// How long the session's first RPC may wait on a guest which died.
const SPEC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// How long a launch's teardown may take once its session ends.
const RELEASED: std::time::Duration = std::time::Duration::from_secs(60);
const DIED: &str = "the guest died, and a redial was refused";
const FAILED: &str = "Spec failed";

#[test]
fn the_marker_permits_a_spec_on_one_connection() {
    in_child(
        "the_marker_permits_a_spec_on_one_connection",
        |mut h| async move {
            for stderr in [
                " ",
                "flow-connector-vmm: a diagnostic\n ",
                "flow-connector-vmm: a diagnostic\n {\"level\":\"info\",\"message\":\"init started\"}\n",
            ] {
                let report = launch(
                    &mut h,
                    Launch {
                        stderr,
                        peer: Peer::Serving,
                        then: Then::Spec,
                    },
                )
                .await;
                assert_eq!(
                    (render(&report), report.ended.as_str()),
                    (answered_spec(), "rm")
                );
            }
        },
    );
}

#[test]
fn a_listening_peer_without_the_marker_is_not_ready() {
    in_child(
        "a_listening_peer_without_the_marker_is_not_ready",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "",
                    peer: Peer::Serving,
                    then: Then::Exit,
                },
            )
            .await;
            assert!(
                [EXITED, CLOSED].contains(&report.outcome.as_str()),
                "{}",
                report.outcome
            );
            assert!(report.left.is_empty(), "{:?}", report.left);
        },
    );
}

#[test]
fn a_premature_marker_fails_the_dial_and_releases_the_launch() {
    in_child(
        "a_premature_marker_fails_the_dial_and_releases_the_launch",
        |mut h| async move {
            for stderr in [
                " ",
                "  an indented diagnostic\n",
                "flow-connector-vmm: failed\n    a continuation\n",
            ] {
                let report = launch(
                    &mut h,
                    Launch {
                        stderr,
                        peer: Peer::Absent,
                        then: Then::Await,
                    },
                )
                .await;
                assert!(
                    report
                        .outcome
                        .starts_with("failed: failed to connect to VMM connector-init at "),
                    "{}",
                    report.outcome
                );
                assert_eq!(report.ended, "rm");
                assert!(report.left.is_empty(), "{:?}", report.left);
            }
        },
    );
}

#[test]
fn a_marker_does_not_require_an_application_response() {
    in_child(
        "a_marker_does_not_require_an_application_response",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: " ",
                    peer: Peer::Preface,
                    then: Then::Await,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{STARTED}\n"));
        },
    );
}

#[test]
fn recovery_retries_failed_ingress_removal() {
    in_child("recovery_retries_failed_ingress_removal", |h| async move {
        let state = h.dir.join("state");
        let state = state.to_str().unwrap();
        let name = "fv_0123456789abcdef";
        let token = "00112233445566778899aabbccddeeff";
        let mount = h
            .dir
            .join("connector-mounts-1000")
            .join(format!("mount-{name}"));
        let line = crate::vmm::record::claim_line(
            name,
            token,
            Some(mount.to_str().unwrap()),
            Some("vmm:local"),
        );
        drop(crate::vmm::record::claim(state, name, &line).unwrap());
        std::fs::write(h.dir.join("ingress"), "").unwrap();
        std::fs::write(h.dir.join("refuse-revoke"), "").unwrap();
        let engine = h.dir.join("engine");
        let problems = crate::vmm::release::recover(engine.to_str().unwrap(), state).await;
        let subjects: Vec<&str> = problems.iter().map(|p| p.subject.as_str()).collect();
        assert_eq!(
            subjects,
            [format!(
                "{}: ingress {token}",
                crate::vmm::record::path(state, name)
            )]
        );
        assert!(h.dir.join("ingress").exists());
        assert!(Path::new(&crate::vmm::record::path(state, name)).exists());
        std::fs::remove_file(h.dir.join("refuse-revoke")).unwrap();
        let problems = crate::vmm::release::recover(engine.to_str().unwrap(), state).await;
        assert_eq!(problems, []);
        assert!(!h.dir.join("ingress").exists());
        assert!(!Path::new(&crate::vmm::record::path(state, name)).exists());
        let calls = std::fs::read_to_string(h.dir.join("calls")).unwrap();
        assert_eq!(
            calls
                .matches(&format!("boundary revoke --owner={token}"))
                .count(),
            2
        );
    });
}

/// What `boundary verify` of an image built from a commit which is never
/// this launcher's prints.
const OTHER_BUILD: &str =
    "listed\nflow-connector-vmm build 0000000000000000000000000000000000000000\n";

/// A VMM image whose build is not this launcher's is refused once verify
/// reports it, before any network, and the launch releases what it made.
/// Recovery, which revokes with a record's image, never verifies or pairs.
#[test]
fn an_unpaired_image_is_refused_before_its_network() {
    in_child(
        "an_unpaired_image_is_refused_before_its_network",
        |mut h| async move {
            h.allow_unpaired_build = false;
            std::fs::write(h.dir.join("verified"), OTHER_BUILD).unwrap();
            let (outcome, logs) = refused(&h).await;
            assert!(
            outcome.starts_with("failed (FailedPrecondition): VMM image synthetic-vmm is not paired with this launcher's build: "),
            "{outcome}"
        );
            assert!(
                !logs.iter().any(|log| log.contains("not paired")),
                "refused, not warned: {logs:?}"
            );
            let calls = std::fs::read_to_string(h.dir.join("calls")).unwrap();
            assert!(calls.contains(" boundary verify\n"), "{calls}");
            assert!(!calls.contains("network create"), "{calls}");

            let state = h.dir.join("state");
            let state = state.to_str().unwrap();
            let name = "fv_0123456789abcdef";
            let token = "00112233445566778899aabbccddeeff";
            let line = crate::vmm::record::claim_line(name, token, None, Some("unpaired-vmm:old"));
            drop(crate::vmm::record::claim(state, name, &line).unwrap());
            std::fs::write(h.dir.join("ingress"), "").unwrap();
            std::fs::remove_file(h.dir.join("calls")).unwrap();
            let engine = h.dir.join("engine");
            let problems = crate::vmm::release::recover(engine.to_str().unwrap(), state).await;
            assert_eq!(problems, []);
            assert!(!h.dir.join("ingress").exists());
            let calls = std::fs::read_to_string(h.dir.join("calls")).unwrap();
            assert!(
                calls.contains("unpaired-vmm:old boundary revoke --owner="),
                "{calls}"
            );
            assert!(!calls.contains("verify"), "{calls}");
        },
    );
}

/// The override skips pairing alone: an unpaired image launches, with a
/// warning to the task, and an image whose verify fails is still refused.
#[test]
fn the_override_skips_only_pairing() {
    in_child("the_override_skips_only_pairing", |mut h| async move {
        std::fs::write(h.dir.join("verified"), OTHER_BUILD).unwrap();
        std::fs::write(h.dir.join("verify-status"), "1").unwrap();
        let (outcome, _logs) = refused(&h).await;
        assert!(
            outcome.starts_with(
                "failed (FailedPrecondition): the host's VMM network boundary did not verify"
            ),
            "{outcome}"
        );

        std::fs::remove_file(h.dir.join("verify-status")).unwrap();
        let report = launch(
            &mut h,
            Launch {
                stderr: " ",
                peer: Peer::Serving,
                then: Then::Await,
            },
        )
        .await;
        assert_eq!(render(&report), format!("{STARTED}\n"));
        let warned = "VMM image synthetic-vmm is not paired with this launcher's build: ";
        assert!(
            report.logs.iter().any(|log| log.starts_with(warned)
                && log.ends_with("; launching it as CONNECTOR_VMM_ALLOW_UNPAIRED_BUILD permits")),
            "{:#?}",
            report.logs
        );
    });
}

/// A launch resolves its VMM image once, pulling it only if it's missing,
/// and verifies, grants and runs that image by ID, whatever its tag names
/// meanwhile.
#[test]
fn the_vmm_image_is_one_image_throughout() {
    in_child(
        "the_vmm_image_is_one_image_throughout",
        |mut h| async move {
            std::fs::write(h.dir.join("vmm-missing"), "").unwrap();
            // As `declared_ports_are_reported_and_permissions_released`, for a grant.
            std::fs::write(h.dir.join("inspection"), r#"[{"Id":"synthetic-image","Created":"2026-01-01T00:00:00Z","Config":{"Env":[],"Labels":{"FLOW_RUNTIME_PROTOCOL":"derive","dev.estuary.port-proto.9090":"http/1.1","dev.estuary.port-public.9090":"true"},"ExposedPorts":{"9090/tcp":{}}}}]"#).unwrap();
            h.ports = vec![8080];
            let report = launch(
                &mut h,
                Launch {
                    stderr: " ",
                    peer: Peer::Serving,
                    then: Then::Await,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{STARTED}\n"));

            let calls = std::fs::read_to_string(h.dir.join("calls")).unwrap();
            let id = "00000000000000000000000000000000000000000000000000000000000000aa";
            let vmm: Vec<String> = calls
                .lines()
                .filter(|call| call.contains("synthetic-vmm") || call.contains(id))
                .map(|call| {
                    let words: Vec<&str> = call.split(' ').collect();
                    match words.iter().position(|word| *word == id) {
                        Some(at) => {
                            words[..2].join(" ")
                                + " ... "
                                + &words[at..(at + 3).min(words.len())].join(" ")
                        }
                        None => call.to_string(),
                    }
                })
                .collect();
            insta::assert_snapshot!(vmm.join("\n"), @r"
        image inspect --format {{.Id}} synthetic-vmm
        pull synthetic-vmm --quiet --platform=linux/amd64
        image inspect --format {{.Id}} synthetic-vmm
        run --rm ... 00000000000000000000000000000000000000000000000000000000000000aa boundary verify
        run --network=host ... 00000000000000000000000000000000000000000000000000000000000000aa boundary grant
        create --rm ... 00000000000000000000000000000000000000000000000000000000000000aa run --policy
        run --rm ... 00000000000000000000000000000000000000000000000000000000000000aa boundary revoke
        ");
        },
    );
}

#[test]
fn a_public_vmm_start_has_only_its_launch_mount() {
    in_child(
        "a_public_vmm_start_has_only_its_launch_mount",
        |mut h| async move {
            std::fs::write(h.dir.join("stderr"), " ").unwrap();
            let Prepared {
                mut ctx,
                config,
                logs: _,
                read_through,
            } = prepare(&h);
            ctx.plane = crate::Plane::Public;
            ctx.vmm = Some(config);
            ctx.task_update = Some(crate::TaskUpdate::for_test());
            let execution = ctx.execution.clone();

            let endpoint: bytes::Bytes = serde_json::json!({"image": IMAGE, "config": {}})
                .to_string()
                .into();
            let Some((.., proto_flow::connector::request::Kind::Derive(open))) =
                crate::execution_fixture::every_first_request(
                    crate::execution_fixture::image_connector_type,
                    &endpoint,
                    Some(execution.clone()),
                )
                .into_iter()
                .find(|(label, ..)| *label == "derive Open")
            else {
                panic!("the fixture has a derive Open");
            };
            let mut start = Box::pin(crate::protocol::start::<crate::derive::Derive>(
                ctx, None, open,
            ));

            tokio::select! {
                () = next_event(&mut h.events, "attached") => (),
                started = &mut start => {
                    panic!("the start ended before its VMM was attached: {:?}", started.err())
                }
            }
            // SAFETY: geteuid takes nothing and cannot fail.
            let mounts = h
                .dir
                .join(format!("connector-mounts-{}", unsafe { libc::geteuid() }));
            let mounts: Vec<PathBuf> = std::fs::read_dir(mounts)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect();
            let [mount] = &mounts[..] else {
                panic!("the launch's mount must be the only one: {mounts:?}");
            };
            let name = mount.file_name().unwrap().to_str().unwrap();
            let name = name.strip_prefix("mount-").unwrap().to_string();
            assert!(name.starts_with("fv_"), "{name}");
            assert!(mount.join("task-update.json").exists());

            let socket = std::fs::read_to_string(h.dir.join("socket")).unwrap();
            let (gate_tx, _gate_rx) = tokio::sync::mpsc::unbounded_channel();
            let (peer_tasks, release, _scripted) = serve_scripted(&socket, gate_tx);
            release.send(()).unwrap();
            command(&h.dir, "stderr");

            let started = start.await.unwrap();
            let Some(proto_flow::connector::response::Kind::Started(response)) =
                &started.started.kind
            else {
                panic!("the start's response is Started");
            };
            assert_eq!(response.execution, Some(execution));
            assert!(response.container.is_some());
            std::mem::drop(started);

            _ = tokio::time::timeout(RELEASED, read_through)
                .await
                .expect("the launch's teardown ends within RELEASED");
            stop_peers(peer_tasks).await;
            let ended = std::fs::read_to_string(h.dir.join("ended")).unwrap_or_default();
            assert_eq!(left(&h.dir, &name, ended.trim()), Vec::<String>::new());
        },
    );
}

/// Start a launch refused before its VMM is attached, and wait for its
/// teardown. Returns its outcome, with its status code, and its task logs.
async fn refused(h: &Harness) -> (String, Vec<String>) {
    let Prepared {
        ctx,
        config,
        logs,
        read_through,
    } = prepare(h);
    let crate::vmm::Execution::Vmm {
        vmm,
        eligible,
        egress,
    } = crate::vmm::vmm_for(
        Some(&config),
        ctx.plane,
        ops::TaskType::Derivation,
        Some(IMAGE),
        &ctx.execution,
        None,
    )
    .unwrap()
    else {
        panic!("the synthetic derivation must select VMM execution");
    };
    let outcome = match super::start(
        &ctx,
        vmm,
        eligible,
        egress,
        Vec::new(),
        IMAGE,
        &crate::EMPTY_SECRETS,
        None,
    )
    .await
    {
        Ok(_) => panic!("the launch was not refused"),
        Err(err) => match err.downcast_ref::<proto_grpc::StatusError>() {
            Some(status) => format!("failed ({:?}): {}", status.0.code(), status.0.message()),
            None => format!("failed: {err:#}"),
        },
    };
    std::mem::drop(ctx);
    _ = tokio::time::timeout(RELEASED, read_through)
        .await
        .expect("the launch's teardown ends within RELEASED");
    let logs = logs.await.unwrap();

    let state = std::fs::read_dir(h.dir.join("state")).unwrap().count();
    assert_eq!(state, 0, "the launch released its state and record");
    // SAFETY: geteuid takes nothing and cannot fail.
    let mounts = h
        .dir
        .join(format!("connector-mounts-{}", unsafe { libc::geteuid() }));
    assert_eq!(
        std::fs::read_dir(mounts).unwrap().count(),
        0,
        "and its mount"
    );
    (outcome, logs)
}

/// The built spec's port is granted though the image exposes none of it, and
/// the port the image alone exposes is not.
#[test]
fn declared_ports_are_reported_and_permissions_released() {
    in_child(
        "declared_ports_are_reported_and_permissions_released",
        |mut h| async move {
            std::fs::write(h.dir.join("inspection"), r#"[{"Id":"synthetic-image","Created":"2026-01-01T00:00:00Z","Config":{"Env":[],"Labels":{"FLOW_RUNTIME_PROTOCOL":"derive","dev.estuary.port-proto.9090":"http/1.1","dev.estuary.port-public.9090":"true"},"ExposedPorts":{"9090/tcp":{}}}}]"#).unwrap();
            h.ports = vec![8080];
            let report = launch(
                &mut h,
                Launch {
                    stderr: " ",
                    peer: Peer::Serving,
                    then: Then::Await,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{STARTED}\n"));
            let calls = std::fs::read_to_string(h.dir.join("calls")).unwrap();
            assert!(calls.contains("boundary grant --owner="));
            assert!(calls.contains("--port=8080"));
            assert!(calls.contains("--ingress-port=8080"));
            assert!(!calls.contains("9090"));
            assert!(calls.contains("boundary revoke --owner="));
        },
    );
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
            assert!(
                [EXITED, CLOSED].contains(&report.outcome.as_str()),
                "{}",
                report.outcome
            );
            assert_eq!(report.ended, "exit");
            assert!(report.left.is_empty(), "{:?}", report.left);
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

// A guest can die after readiness. Its death must fail the first Spec and
// release the launch. A fresh launch then answers its Spec.
#[test]
fn a_guest_dead_after_readiness_answers_no_spec() {
    in_child(
        "a_guest_dead_after_readiness_answers_no_spec",
        |mut h| async move {
            let (mut actual, mut expected) = (String::new(), String::new());
            for (name, then, received) in [
                ("before its Spec is sent", Then::DieBeforeSpec, 0),
                ("while its Spec is held", Then::DieHoldingSpec, 1),
            ] {
                let died = launch(
                    &mut h,
                    Launch {
                        stderr: " ",
                        peer: Peer::Serving,
                        then,
                    },
                )
                .await;
                let fresh = launch(
                    &mut h,
                    Launch {
                        stderr: " ",
                        peer: Peer::Serving,
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
                    "# {name}\n{STARTED}\n{held}{DIED}\n{FAILED}\n\
                     {received} Specs received, 0 answered\nended: exit\n\
                     # then a fresh launch\n{}ended: rm\n",
                    answered_spec(),
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

#[test]
fn an_early_failure_preserves_a_partial_diagnostic() {
    in_child(
        "an_early_failure_preserves_a_partial_diagnostic",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "flow-guest-init: could not exec connector-init",
                    peer: Peer::Absent,
                    then: Then::Exit,
                },
            )
            .await;
            assert!(
                [EXITED, CLOSED].contains(&report.outcome.as_str()),
                "{}",
                report.outcome
            );
            assert!(report.left.is_empty(), "{:?}", report.left);
            assert!(
                report
                    .logs
                    .iter()
                    .any(|log| log == "flow-guest-init: could not exec connector-init"),
                "{:?}",
                report.logs
            );
        },
    );
}

#[test]
fn closed_stderr_ends_the_wait_and_releases_the_launch() {
    in_child(
        "closed_stderr_ends_the_wait_and_releases_the_launch",
        |mut h| async move {
            let report = launch(
                &mut h,
                Launch {
                    stderr: "",
                    peer: Peer::Absent,
                    then: Then::CloseStderr,
                },
            )
            .await;
            assert_eq!(render(&report), format!("{CLOSED}\n"));
            assert_eq!(report.ended, "rm");
        },
    );
}

#[test]
fn a_readiness_timeout_releases_the_launch() {
    in_child(
        "a_readiness_timeout_releases_the_launch",
        |mut h| async move {
            for stderr in ["", "flow-connector-vmm: a partial diagnostic "] {
                let report = launch(
                    &mut h,
                    Launch {
                        stderr,
                        peer: Peer::Serving,
                        then: Then::Timeout,
                    },
                )
                .await;
                assert!(
                    report
                        .outcome
                        .starts_with("failed: timeout waiting for the VMM to become ready"),
                    "{}",
                    report.outcome
                );
                assert_eq!(report.ended, "rm");
                assert!(report.left.is_empty(), "{:?}", report.left);
            }
        },
    );
}

#[tokio::test(start_paused = true)]
async fn a_missing_marker_ends_at_the_ready_deadline() {
    let (_ready_tx, ready_rx) = futures::channel::oneshot::channel();
    let started = tokio::time::Instant::now();
    let err = super::dial("unused.sock", ready_rx)
        .await
        .expect_err("no marker arrives");
    assert_eq!(
        err.to_string(),
        "timeout waiting for the VMM to become ready"
    );
    assert_eq!(started.elapsed(), super::READY_TIMEOUT);
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
    /// Accepts, reads the client's HTTP/2 preface, and never writes.
    Preface,
    /// A derive connector which holds its Spec until released.
    Serving,
}

#[derive(Clone, Copy)]
enum Then {
    Exit,
    Cancel,
    CloseStderr,
    Timeout,
    Await,
    Spec,
    DieBeforeSpec,
    DieHoldingSpec,
}

/// How a launch went, with its directory and VMM's id elided.
struct Report {
    outcome: String,
    /// What ended the engine's attached VMM: `exit` or `rm`.
    ended: String,
    left: Vec<String>,
    /// The messages of the task's logs.
    logs: Vec<String>,
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
    /// TCP ports of the launch's built spec.
    ports: Vec<u32>,
    /// The scripted verify reports no build unless a test scripts one.
    allow_unpaired_build: bool,
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
            scenario(Harness {
                dir,
                events,
                ports: Vec::new(),
                allow_unpaired_build: true,
            })
            .await
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
    std::fs::set_permissions(
        dir.path().join("state"),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .unwrap();

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

    let Prepared {
        ctx,
        config,
        logs,
        read_through,
    } = prepare(h);
    let crate::vmm::Execution::Vmm {
        vmm,
        eligible,
        egress,
    } = crate::vmm::vmm_for(
        Some(&config),
        ctx.plane,
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
        h.ports.clone(),
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
        Peer::Serving => {
            let (tasks, release, scripted) = serve_scripted(&socket, gate_tx);
            (tasks, Some(release), Some(scripted))
        }
        Peer::Absent => (Vec::new(), None, None),
        Peer::Preface => {
            let listener = tokio::net::UnixListener::bind(&socket).unwrap();
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut preface = [0; PREFACE.len()];
                tokio::io::AsyncReadExt::read_exact(&mut stream, &mut preface)
                    .await
                    .unwrap();
                assert_eq!(&preface, PREFACE);
                gate_tx.send(()).unwrap();
                std::future::pending::<()>().await;
                std::mem::drop(stream);
            });
            (vec![task], None, None)
        }
    };

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
    if matches!(peer, Peer::Preface)
        && gate(start.as_mut(), &mut outcome, gate_rx.recv())
            .await
            .is_none()
    {
        assert_eq!(gate_rx.recv().await, Some(()));
    }

    match then {
        Then::Exit => command(&dir, "exit 101"),
        Then::CloseStderr => command(&dir, "close-stderr"),
        Then::Timeout => {
            tokio::time::pause();
            tokio::time::advance(super::READY_TIMEOUT).await;
            tokio::time::resume();
        }
        Then::Cancel | Then::Await | Then::Spec | Then::DieBeforeSpec | Then::DieHoldingSpec => (),
    }
    let outcome = match (outcome, then) {
        (Some(started), _) => Some(started),
        (None, Then::Cancel) => None,
        (None, _) => Some(start.as_mut().await),
    };
    std::mem::drop(start);

    let outcome = match outcome {
        None => "cancelled".to_string(),
        Some(Err(err)) => format!("failed: {err:#}"),
        Some(Ok((container, channel, guard, _codec))) => {
            if !h.ports.is_empty() {
                assert_eq!(container.ip_addr, "10.89.0.2");
                // Reported as an ordinary container reports it, for validation.
                assert_eq!(
                    container.advertised_ports,
                    vec![proto_flow::flow::NetworkPort {
                        number: 9090,
                        protocol: "http/1.1".to_string(),
                        public: true,
                    }]
                );
                assert!(container.mapped_host_ports.is_empty());
                assert!(dir.join("ingress").exists());
            }
            let outcome = if matches!(
                then,
                Then::Spec | Then::DieBeforeSpec | Then::DieHoldingSpec
            ) {
                let scripted = scripted.as_ref().expect("a serving peer exchanges Specs");
                exchange(
                    &dir,
                    &socket,
                    then,
                    scripted,
                    channel,
                    &mut gate_rx,
                    &mut peer_tasks,
                    &mut release,
                )
                .await
            } else {
                STARTED.to_string()
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
    let logs = logs.await.unwrap();
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
        logs,
    }
}

/// A launch's start context and VMM configuration, the task draining its
/// logs, which returns their messages, and its read-through signal.
struct Prepared {
    ctx: crate::protocol::StartContext,
    config: crate::Vmm,
    logs: tokio::task::JoinHandle<Vec<String>>,
    read_through: tokio::sync::oneshot::Receiver<()>,
}

fn prepare(h: &Harness) -> Prepared {
    let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(64);
    let (log_sink, read_through) = crate::LogSink::response(response_tx);
    // Drained, so that the pump never waits on a full channel.
    let logs = tokio::spawn(async move {
        let mut messages = Vec::new();
        while let Some(response) = response_rx.recv().await {
            if let Some(proto_flow::connector::response::Kind::Log(log)) = response.unwrap().kind {
                messages.push(log.message);
            }
        }
        messages
    });
    let ctx = crate::protocol::StartContext {
        container_network: String::new(),
        execution: proto_flow::flow::ConnectorExecution {
            vmm: true,
            egress: Some(Default::default()),
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
        podman: h.dir.join("engine").to_str().unwrap().to_string(),
        state_dir: h.dir.join("state").to_str().unwrap().to_string(),
        disk_mib: 1,
        memory: Ok(crate::vmm::Memory {
            bytes: 512 << 20,
            guest_mib: 256,
        }),
        cpu_limit: "1".to_string(),
        vcpus: 1,
        cgroup_parent: None,
        allow_unpaired_build: h.allow_unpaired_build,
    };
    Prepared {
        ctx,
        config,
        logs,
        read_through,
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
    received: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    answered: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    /// Duplicates of each accepted connection's descriptor.
    connections: std::sync::Arc<std::sync::Mutex<Vec<std::os::unix::net::UnixStream>>>,
    gate_tx: tokio::sync::mpsc::UnboundedSender<()>,
    release: futures::future::Shared<tokio::sync::oneshot::Receiver<()>>,
}

/// Bind a scripted peer at `socket`. Returns its tasks, whose abort closes
/// its listener, and the sender which releases its held Specs.
fn serve_scripted(
    socket: &str,
    gate_tx: tokio::sync::mpsc::UnboundedSender<()>,
) -> (
    Vec<tokio::task::JoinHandle<()>>,
    tokio::sync::oneshot::Sender<()>,
    Scripted,
) {
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let scripted = Scripted {
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
    channel: tonic::transport::Channel,
    gate_rx: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
    peer_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    release: &mut Option<tokio::sync::oneshot::Sender<()>>,
) -> String {
    use std::sync::atomic::Ordering::SeqCst;

    let mut milestones = format!("{STARTED}\n");

    let mut spec = Box::pin(crate::protocol::spec_rpc::<crate::derive::Derive>(
        channel, 0,
    ));
    if matches!(then, Then::DieBeforeSpec) {
        milestones.push_str(&die(dir, socket, peer_tasks, scripted).await);
    } else {
        tokio::select! {
            received = gate_rx.recv() => {
                assert_eq!(received, Some(()), "the peer's gate channel closed");
            }
            spec = &mut spec => panic!("the Spec ended before the peer received it: {spec:?}"),
            () = tokio::time::sleep(SPEC_TIMEOUT) => {
                panic!("the peer received no Spec within {SPEC_TIMEOUT:?}")
            }
        }
        assert_eq!(
            scripted.connections.lock().unwrap().len(),
            1,
            "one connection for startup and Spec"
        );
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

fn answered_spec() -> String {
    format!(
        "{STARTED}\nSpec received and held\nSpec answered with protocol 3032023\n1 Specs received, 1 answered\n"
    )
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
        _ = self.gate_tx.send(());
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
    for resource in ["container", "network", "volume", "ingress"] {
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
