//! C14: the download of a Python derivation's dependency is cut mid-body,
//! inside a VMM, while derive-python's own installer (`uv run`, from the
//! generated `pyproject.toml`) installs it, and the same derivation is then
//! opened again once the endpoint serves whole.
//!
//! The dependency is `c14pkg`, a synthetic wheel (`fixtures/c14_wheel.py`)
//! which derive-python's image lacks, declared as a PEP 508 direct reference
//! to a controlled endpoint: a derivation's `UV_*` environment is reserved,
//! and a direct dependency URL lets this fixture choose its package source
//! without a `UV_*` override. Everything else (pydantic, pyright) comes from
//! PyPI through the VMM's gateway resolver, as a production VMM's would.
//!
//! The endpoint is this test's HTTP/1.1 server, on a controlled address in the
//! endpoint namespace, and records every request it answered and how many body
//! bytes it wrote. Cut, it declares the wheel's whole `Content-Length` and
//! closes after `CUT_AT` bytes, for every request, retries included.
//!
//! The guest's uv cache and derive-python's project live on the VMM's scratch
//! disk, which goes with the VMM, so a relaunch cannot reuse anything a cut
//! download left: what this shows is that a relaunch needs no repair, not
//! that a damaged cache is recovered. Each test writes `c14-<test>.evidence`
//! to the run directory as it goes, then fails naming every violated bound.

use super::c07::append;
use super::*;
use connector_vmm_tests::launcher;
use proto_flow::{connector as proto, derive, flow, ops};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

/// The launcher tests' bounds: a launch pulls its image; a release ends it.
const STARTED: Duration = Duration::from_secs(180);
const RELEASED: Duration = Duration::from_secs(60);
/// From `Started` to the derivation's `Opened`, or to the session's failure:
/// uv's whole resolution and install, PyPI's packages included. The handoff
/// caps the installer at 120 s; a healthy baseline must fit it as well.
const INSTALLED: Duration = Duration::from_secs(120);
/// Reads and a Flush of an opened derivation, to its `Flushed`.
const DERIVED: Duration = Duration::from_secs(30);
/// Each write of an endpoint connection, and the whole connection from its
/// accept: a whole wheel took 10 ms in B0.
const IO: Duration = Duration::from_secs(10);
const CONNECTION: Duration = Duration::from_secs(30);
/// Bytes of body a cut response writes before it closes, of about 3 MiB.
const CUT_AT: usize = 1 << 20;
const MAX_WHEEL: usize = 8 << 20;
const TASK: &str = "acmeCo/c14/rendered";
const SEEDS: [u64; 4] = [1, 2, 3, 1000];

const MODULE: &str = r#"from collections.abc import AsyncIterator
from importlib.metadata import version

import c14pkg

from acmeCo.c14.rendered import IDerivation, Document, Request


class Derivation(IDerivation):
    async def from_seeds(self, read: Request.ReadFromSeeds) -> AsyncIterator[Document]:
        n = read.doc.n
        yield Document(n=n, rendered=c14pkg.render(n), version=version("c14pkg"))
"#;

type Session = (
    tokio::sync::mpsc::Sender<proto::Request>,
    tokio::sync::mpsc::Receiver<tonic::Result<proto::Response>>,
    proto::response::Started,
);
type Logs = Mutex<Vec<String>>;

#[derive(serde::Deserialize)]
struct Wheel {
    path: String,
    bytes: usize,
    sha256: String,
    payload_sha256: String,
    version: String,
}

/// What the endpoint does with a request for the wheel, and the phase its
/// evidence is filed under.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Whole,
    Cut,
}

/// The test's package endpoint. Dropping it, as an unwinding test does too,
/// stops its server and joins it, so neither its listener nor a connection
/// outlives the test.
struct Endpoint {
    url: String,
    state: Arc<Mutex<(Mode, &'static str)>>,
    served: Arc<Mutex<Vec<serde_json::Value>>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    server: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

fn wheel(dir: &std::path::Path) -> (Wheel, Arc<Vec<u8>>) {
    std::fs::create_dir_all(dir).unwrap_or_else(|e| panic!("creating {}: {e}", dir.display()));
    let script = run::fixture("c14_wheel.py");
    let output = std::process::Command::new("python3")
        .args(["-I", script.to_str().expect("a UTF-8 path")])
        .arg(dir)
        .output()
        .expect("running c14_wheel.py");
    assert!(
        output.status.success(),
        "c14_wheel.py: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let wheel: Wheel = serde_json::from_slice(&output.stdout).expect("c14_wheel.py's summary");
    let body = std::fs::read(&wheel.path).unwrap_or_else(|e| panic!("reading {}: {e}", wheel.path));
    assert_eq!(body.len(), wheel.bytes);
    assert!(
        wheel.bytes <= MAX_WHEEL && wheel.bytes > 2 * CUT_AT,
        "the wheel is {} bytes",
        wheel.bytes
    );
    (wheel, Arc::new(body))
}

/// Serves `body` at `path` on `listener`, one connection at a time (the one
/// installer here makes few requests), and records each answered request, in
/// `evidence` and `served`, before it closes the connection.
fn serve(
    listener: std::net::TcpListener,
    path: String,
    body: Arc<Vec<u8>>,
    t0: Instant,
    evidence: std::path::PathBuf,
) -> Endpoint {
    let url = format!(
        "http://{}{path}",
        listener.local_addr().expect("a bound listener")
    );
    // Polled, so that a stop is seen without a connection to wake `accept`.
    listener
        .set_nonblocking(true)
        .expect("a nonblocking listener");
    let state = Arc::new(Mutex::new((Mode::Whole, "B0")));
    let served = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (shared_state, shared_served, shared_stop) = (state.clone(), served.clone(), stop.clone());

    let server = std::thread::spawn(move || {
        while !shared_stop.load(std::sync::atomic::Ordering::SeqCst) {
            let mut stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Err(e) => panic!("accepting on the package endpoint: {e}"),
            };
            // Copied, so the lock is not held across the connection's IO.
            let mode = *shared_state.lock().unwrap();
            let record = answer(&mut stream, &path, &body, mode, t0, &shared_stop);
            append(&evidence, record.clone());
            shared_served.lock().unwrap().push(record);
            // Only now, so a client which has read to the FIN finds its
            // request served. A cut is a FIN after the bytes written, not a
            // reset that could drop them.
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    });
    Endpoint {
        url,
        state,
        served,
        stop,
        server: Some(server),
    }
}

/// Answers one connection, within `CONNECTION` of its accept, leaving its
/// close to the caller: each read and write is bounded, and the body is
/// written in chunks between which a stop or the deadline ends it.
fn answer(
    stream: &mut std::net::TcpStream,
    path: &str,
    body: &[u8],
    (mode, phase): (Mode, &'static str),
    t0: Instant,
    stop: &std::sync::atomic::AtomicBool,
) -> serde_json::Value {
    let accepted = Instant::now();
    let accepted_ms = t0.elapsed().as_millis();
    let peer = stream
        .peer_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_default();
    // Reads wake each second to see a stop; writes are bounded by `IO`.
    let configured = stream
        .set_nonblocking(false)
        .and_then(|()| stream.set_read_timeout(Some(Duration::from_secs(1))))
        .and_then(|()| stream.set_write_timeout(Some(IO)));

    let mut head = Vec::new();
    let mut buffer = [0u8; 4096];
    while configured.is_ok()
        && !head.windows(4).any(|w| w == b"\r\n\r\n")
        && head.len() < 64 * 1024
        && accepted.elapsed() < CONNECTION
        && !stop.load(std::sync::atomic::Ordering::SeqCst)
    {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => head.extend_from_slice(&buffer[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => break,
        }
    }
    let complete = head.windows(4).any(|w| w == b"\r\n\r\n");
    let head = String::from_utf8_lossy(&head).into_owned();
    let request = head.split("\r\n").next().unwrap_or_default().to_string();
    let header = |name: &str| {
        head.split("\r\n")
            .skip(1)
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.trim()
                    .eq_ignore_ascii_case(name)
                    .then(|| value.trim().to_string())
            })
            .unwrap_or_default()
    };
    let (range, agent) = (header("range"), header("user-agent"));
    let target = request.split(' ').nth(1).unwrap_or_default();
    let is_get = request.starts_with("GET ");

    let mut written = 0usize;
    let outcome = if let Err(e) = configured {
        format!("configuring the connection: {e}")
    } else if !complete {
        "incomplete request".to_string()
    } else if target != path || !(is_get || request.starts_with("HEAD ")) {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        "not found".to_string()
    } else {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let send = match (is_get, mode) {
            (false, _) => 0,
            (true, Mode::Whole) => body.len(),
            (true, Mode::Cut) => CUT_AT,
        };
        let mut result = stream.write_all(headers.as_bytes());
        for chunk in body[..send].chunks(64 * 1024) {
            if result.is_err() {
                break;
            }
            if stop.load(std::sync::atomic::Ordering::SeqCst) || accepted.elapsed() > CONNECTION {
                result = Err(std::io::Error::other(
                    "stopped or past the connection's deadline",
                ));
                break;
            }
            result = stream.write_all(chunk);
            if result.is_ok() {
                written += chunk.len();
            }
        }
        match result.and_then(|()| stream.flush()) {
            Ok(()) if is_get && mode == Mode::Cut => "cut".to_string(),
            Ok(()) => "whole".to_string(),
            Err(e) => format!("write failed: {e}"),
        }
    };

    serde_json::json!({
        "served": phase, "mode": format!("{mode:?}"), "accepted_ms": accepted_ms,
        "ended_ms": t0.elapsed().as_millis(), "peer": peer, "request": request,
        "range": range, "user_agent": agent, "declared": body.len(), "written": written,
        "outcome": outcome,
    })
}

fn set(endpoint: &Endpoint, mode: Mode, phase: &'static str) {
    *endpoint.state.lock().unwrap() = (mode, phase);
}

fn served(endpoint: &Endpoint, phase: &str) -> Vec<serde_json::Value> {
    endpoint
        .served
        .lock()
        .unwrap()
        .iter()
        .filter(|record| record["served"] == phase)
        .cloned()
        .collect()
}

/// A launcher beneath `root`'s state directory, through its podman.
fn router(run: &run::Run, root: &launcher::Root) -> connector::ServiceRouter {
    let vmm = connector::Vmm {
        image: run.vmm_image.clone(),
        podman: root.podman.clone(),
        state_dir: root.state_dir.clone(),
        disk_mib: 2048,
        memory_bytes: 768 * MIB,
        cpu_limit: "2".to_string(),
        guest_memory_mib: 512,
        vcpus: 2,
        cgroup_parent: None,
    };
    let (_service, router) = connector::Service::new_local(
        String::new(),
        Some(vmm),
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    );
    router
}

/// The derivation's Open, as a reactor's runtime would send it: one transform
/// from a collection of `{n}`, and `c14pkg` as a direct reference to `url`.
fn open(image: &str, url: &str) -> proto::Request {
    let seeds = serde_json::json!({
        "name": "acmeCo/c14/seeds",
        "writeSchema": {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]},
        "key": ["/n"],
    });
    let request: derive::Request = serde_json::from_value(serde_json::json!({
        "open": {
            "collection": {
                "name": TASK,
                "writeSchema": {
                    "type": "object",
                    "properties": {
                        "n": {"type": "integer"},
                        "rendered": {"type": "string"},
                        "version": {"type": "string"},
                    },
                    "required": ["n", "rendered", "version"],
                },
                "key": ["/n"],
                "derivation": {
                    "connectorType": "IMAGE",
                    "config": {"image": image, "config": {
                        "module": MODULE,
                        "dependencies": {"c14pkg": format!(" @ {url}")},
                    }},
                    "transforms": [{
                        "name": "fromSeeds",
                        "collection": seeds,
                        "lambdaConfig": {"readOnly": false},
                    }],
                    "shardTemplate": {"labels": {"labels": [
                        {"name": "estuary.dev/build", "value": "0000000000000c14"},
                    ]}},
                    "execution": {"vmm": true},
                },
            },
            "version": "c14",
            "range": {"keyEnd": 4294967295u32, "rClockEnd": 4294967295u32},
            "state": {},
        }
    }))
    .expect("a derive Open");

    proto::Request {
        start: Some(proto::request::Start {
            execution: Some(flow::ConnectorExecution {
                vmm: true,
                egress: None,
            }),
            log_level: ops::log::Level::Debug as i32,
            ..Default::default()
        }),
        kind: Some(proto::request::Kind::Derive(request)),
    }
}

/// Starts a session with the Open, to its `Started`.
fn start(
    runtime: &tokio::runtime::Runtime,
    router: &connector::ServiceRouter,
    request: proto::Request,
    logs: &Logs,
) -> Session {
    let logger = |log: &ops::Log| logs.lock().unwrap().push(log_line(log));
    runtime
        .block_on(async {
            tokio::time::timeout(
                STARTED,
                proto_grpc::connector::start(router, &logger, TASK, request),
            )
            .await
        })
        .unwrap_or_else(|_| panic!("no Started within {STARTED:?}: {:#?}", logs.lock()))
        .unwrap_or_else(|err| panic!("starting: {err:#}\n{:#?}", logs.lock()))
}

fn log_line(log: &ops::Log) -> String {
    let fields: Vec<String> = log
        .fields_json_map
        .iter()
        .map(|(key, value)| format!("{key}={}", String::from_utf8_lossy(value)))
        .collect();
    format!(
        "{}: {} {}",
        log.level().as_str_name(),
        log.message,
        fields.join(" ")
    )
}

#[derive(Debug)]
enum Opening {
    Opened,
    Failed(String),
    Ended,
    TimedOut,
}

/// Awaits the derivation's `Opened` or the session's end, within `within`,
/// with every other response the session sent first.
fn opening(
    runtime: &tokio::runtime::Runtime,
    session: &mut Session,
    logs: &Logs,
    within: Duration,
) -> (Opening, Vec<String>) {
    let mut other = Vec::new();
    let opening = runtime.block_on(async {
        tokio::time::timeout(within, async {
            loop {
                match session.1.recv().await {
                    Some(Ok(proto::Response {
                        kind: Some(proto::response::Kind::Log(log)),
                    })) => logs.lock().unwrap().push(log_line(&log)),
                    Some(Ok(proto::Response {
                        kind:
                            Some(proto::response::Kind::Derive(derive::Response {
                                kind: Some(derive::response::Kind::Opened(_)),
                                ..
                            })),
                    })) => return Opening::Opened,
                    Some(Ok(response)) => other.push(format!("{response:?}")),
                    Some(Err(status)) => return Opening::Failed(format!("{status:?}")),
                    None => return Opening::Ended,
                }
            }
        })
        .await
        .unwrap_or(Opening::TimedOut)
    });
    (opening, other)
}

/// Reads each of `SEEDS`, then a Flush, and returns what was published
/// before `Flushed`.
fn derive(
    runtime: &tokio::runtime::Runtime,
    session: &mut Session,
    logs: &Logs,
) -> Result<Vec<serde_json::Value>, String> {
    let wrap = |kind| proto::Request {
        start: None,
        kind: Some(proto::request::Kind::Derive(derive::Request {
            kind: Some(kind),
            ..Default::default()
        })),
    };
    runtime.block_on(async {
        tokio::time::timeout(DERIVED, async {
            for n in SEEDS {
                let read = derive::request::Read {
                    transform: 0,
                    doc_json: serde_json::json!({"n": n}).to_string().into(),
                    ..Default::default()
                };
                session
                    .0
                    .send(wrap(derive::request::Kind::Read(read)))
                    .await
                    .map_err(|e| format!("sending a Read: {e}"))?;
            }
            session
                .0
                .send(wrap(derive::request::Kind::Flush(Default::default())))
                .await
                .map_err(|e| format!("sending a Flush: {e}"))?;

            let mut published = Vec::new();
            loop {
                match session.1.recv().await {
                    Some(Ok(proto::Response {
                        kind: Some(proto::response::Kind::Log(log)),
                    })) => logs.lock().unwrap().push(log_line(&log)),
                    Some(Ok(proto::Response {
                        kind:
                            Some(proto::response::Kind::Derive(derive::Response {
                                kind: Some(kind),
                                ..
                            })),
                    })) => match kind {
                        derive::response::Kind::Published(p) => published.push(
                            serde_json::from_slice(&p.doc_json)
                                .map_err(|e| format!("a published document: {e}"))?,
                        ),
                        derive::response::Kind::Flushed(_) => return Ok(published),
                        other => return Err(format!("unexpected {other:?}")),
                    },
                    other => return Err(format!("expected a document, got {other:?}")),
                }
            }
        })
        .await
        .unwrap_or_else(|_| Err(format!("no Flushed within {DERIVED:?}")))
    })
}

/// Ends a session as a client done with it, awaiting the end of its stream.
fn end(runtime: &tokio::runtime::Runtime, session: Session, logs: &Logs) -> bool {
    let (request_tx, mut response_rx, _started) = session;
    std::mem::drop(request_tx);
    runtime
        .block_on(async {
            tokio::time::timeout(RELEASED, async {
                while let Some(response) = response_rx.recv().await {
                    if let Ok(proto::Response {
                        kind: Some(proto::response::Kind::Log(log)),
                    }) = response
                    {
                        logs.lock().unwrap().push(log_line(&log));
                    }
                }
            })
            .await
        })
        .is_ok()
}

/// Every VMM `root`'s podman was asked to make, by its network, in order.
fn launches(root: &launcher::Root) -> Vec<String> {
    launcher::calls(root)
        .iter()
        .filter_map(|call| {
            call.strip_prefix("network create ")
                .and_then(|args| args.rsplit(' ').next())
                .map(ToString::to_string)
        })
        .collect()
}

fn expected(wheel: &Wheel) -> Vec<serde_json::Value> {
    SEEDS
        .iter()
        .map(|n| {
            serde_json::json!({
                "n": n,
                "rendered": format!(
                    "c14pkg {} n={n} square={} payload={}",
                    wheel.version,
                    n * n,
                    &wheel.payload_sha256[..16]
                ),
                "version": wheel.version,
            })
        })
        .collect()
}

/// The image's own Python cannot import `c14pkg` and has no uv cache: what a
/// launch installs, it fetched.
fn absent_from_image(image: &str) -> String {
    let name = format!("c14-absent-{}", run::random_hex());
    run::record("container", &name);
    run::podman(&[
        "run",
        "--rm",
        "--name",
        &name,
        "--network=none",
        "--entrypoint",
        "python3",
        image,
        "-I",
        "-c",
        "import importlib.util, os; print(importlib.util.find_spec('c14pkg'), os.path.exists('/tmp/uv-cache'))",
    ])
    .trim()
    .to_string()
}

struct Fixture {
    run: run::Run,
    root: launcher::Root,
    runtime: tokio::runtime::Runtime,
    router: connector::ServiceRouter,
    wheel: Wheel,
    endpoint: Endpoint,
    evidence: std::path::PathBuf,
    t0: Instant,
}

fn fixture(test: &str) -> Fixture {
    let run = run::load();
    let root = launcher::root();
    // SAFETY: no other thread exists yet to read the environment.
    unsafe { std::env::set_var("TMPDIR", &root.tmp_dir) };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    let router = router(&run, &root);
    let evidence = run.run_dir.join(format!("c14-{test}.evidence"));
    let t0 = Instant::now();

    let (wheel, body) = wheel(&run.run_dir.join(format!("c14-{test}-wheel")));
    // A path of its own each run, so no cache anywhere could answer for it.
    let path = format!(
        "/{}/c14pkg-{}-py3-none-any.whl",
        run::random_hex(),
        wheel.version
    );
    let address = *run.endpoints.last().expect("controlled endpoints");
    let listener = netns::tcp(
        netns::Namespace::Named(&run.endpoint_netns),
        std::net::SocketAddr::new(address.into(), 0),
    );
    let endpoint = serve(listener, path, body, t0, evidence.clone());
    let absent = absent_from_image(&run.derive_python_image);
    append(
        &evidence,
        serde_json::json!({
            "test": test, "url": endpoint.url, "wheel": wheel.path, "bytes": wheel.bytes,
            "sha256": wheel.sha256, "payload_sha256": wheel.payload_sha256, "cut_at": CUT_AT,
            "image": run.derive_python_image, "absent_from_image": absent,
            "bounds_ms": {"started": STARTED.as_millis(), "installed": INSTALLED.as_millis(),
                          "derived": DERIVED.as_millis(), "released": RELEASED.as_millis()},
        }),
    );
    assert_eq!(
        absent, "None False",
        "C14 inconclusive: the image already has c14pkg or a uv cache"
    );
    Fixture {
        run,
        root,
        runtime,
        router,
        wheel,
        endpoint,
        evidence,
        t0,
    }
}

/// One healthy launch of the derivation: Opened within `INSTALLED` of
/// `Started`, the expected documents, at least one whole download of the
/// wheel in `phase` and none cut, and a release within `RELEASED`.
fn healthy(f: &Fixture, phase: &'static str, violations: &mut Vec<String>) -> Duration {
    set(&f.endpoint, Mode::Whole, phase);
    let logs = Logs::default();
    let launched = Instant::now();
    let mut session = start(
        &f.runtime,
        &f.router,
        open(&f.run.derive_python_image, &f.endpoint.url),
        &logs,
    );
    let started = launched.elapsed();
    let name = launches(&f.root)
        .last()
        .cloned()
        .expect("the launch named its network");
    let (opening, other) = opening(&f.runtime, &mut session, &logs, INSTALLED);
    let installed = launched.elapsed() - started;
    let derived = match opening {
        Opening::Opened => derive(&f.runtime, &mut session, &logs),
        _ => Err(format!("not opened: {opening:?}")),
    };
    let ended = end(&f.runtime, session, &logs);
    let root = &f.root;
    let released = std::panic::catch_unwind(|| launcher::wait_released(root, &name, RELEASED));
    let requests = served(&f.endpoint, phase);

    append(
        &f.evidence,
        serde_json::json!({
            "phase": phase, "vmm": name, "started_ms": started.as_millis(),
            "installed_ms": installed.as_millis(), "opening": format!("{opening:?}"),
            "other": other, "derived": format!("{derived:?}"), "ended": ended,
            "released": released.is_ok(), "requests": requests.len(),
            "logs": *logs.lock().unwrap(),
        }),
    );

    if !matches!(opening, Opening::Opened) || installed > INSTALLED {
        violations.push(format!("{phase}: {opening:?} after {installed:?}"));
    }
    if !other.is_empty() {
        violations.push(format!("{phase}: before Opened: {other:?}"));
    }
    match derived {
        Ok(documents) if documents == expected(&f.wheel) => (),
        other => violations.push(format!(
            "{phase}: documents {other:?}, expected {:?}",
            expected(&f.wheel)
        )),
    }
    let whole = requests
        .iter()
        .filter(|r| r["outcome"] == "whole" && r["written"] == f.wheel.bytes);
    if whole.count() == 0 || requests.iter().any(|r| r["outcome"] == "cut") {
        violations.push(format!("{phase}: requests {requests:?}"));
    }
    if !ended || released.is_err() {
        violations.push(format!(
            "{phase}: ended {ended}, released {}",
            released.is_ok()
        ));
    }
    installed
}

/// A client's view of one GET of `path` from `addr`: the response's head and
/// every body byte before the server's FIN.
fn get(addr: std::net::SocketAddr, path: &str) -> (String, Vec<u8>) {
    let mut stream = std::net::TcpStream::connect(addr).expect("connecting to the endpoint");
    stream.set_read_timeout(Some(IO)).expect("a read timeout");
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: c14\r\n\r\n").as_bytes())
        .expect("sending a GET");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("reading to the server's FIN");
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a response head");
    (
        String::from_utf8_lossy(&response[..split]).into_owned(),
        response[split + 4..].to_vec(),
    )
}

/// The endpoint alone, without a VMM: a whole GET, a cut GET as a client
/// reads it (the whole length declared, `CUT_AT` bytes, then EOF), another
/// path refused, and a drop which, beside a connection that never sends a
/// request, stops and joins the server within seconds and closes its
/// listener.
#[test]
fn b0_endpoint_cuts_and_stops() {
    let run = run::load();
    let evidence = run.run_dir.join("c14-endpoint.evidence");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
    let addr = listener.local_addr().expect("a bound listener");
    let body: Arc<Vec<u8>> = Arc::new((0..3 * CUT_AT).map(|i| (i % 251) as u8).collect());
    let endpoint = serve(
        listener,
        "/c14/w.whl".to_string(),
        body.clone(),
        Instant::now(),
        evidence,
    );
    let declared = format!("Content-Length: {}", body.len());

    let (head, whole) = get(addr, "/c14/w.whl");
    assert!(
        head.starts_with("HTTP/1.1 200") && head.contains(&declared),
        "{head}"
    );
    assert!(whole == *body, "a whole GET read {} bytes", whole.len());

    set(&endpoint, Mode::Cut, "CUT");
    let (head, cut) = get(addr, "/c14/w.whl");
    assert!(
        head.starts_with("HTTP/1.1 200") && head.contains(&declared),
        "{head}"
    );
    assert!(cut == body[..CUT_AT], "a cut GET read {} bytes", cut.len());

    let (head, other) = get(addr, "/elsewhere");
    assert!(
        head.starts_with("HTTP/1.1 404") && other.is_empty(),
        "{head}"
    );

    let outcomes: Vec<(String, u64)> = endpoint
        .served
        .lock()
        .unwrap()
        .iter()
        .map(|r| {
            (
                r["outcome"].as_str().unwrap_or_default().to_string(),
                r["written"].as_u64().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        outcomes,
        [
            ("whole".to_string(), body.len() as u64),
            ("cut".to_string(), CUT_AT as u64),
            ("not found".to_string(), 0),
        ]
    );

    let mut silent = std::net::TcpStream::connect(addr).expect("connecting to the endpoint");
    std::thread::sleep(Duration::from_millis(200));
    let dropped = Instant::now();
    std::mem::drop(endpoint);
    let joined = dropped.elapsed();
    assert!(
        joined < Duration::from_secs(3),
        "the endpoint took {joined:?} to stop"
    );
    silent.set_read_timeout(Some(IO)).expect("a read timeout");
    let mut rest = Vec::new();
    assert!(
        matches!(silent.read_to_end(&mut rest), Ok(0)),
        "the silent connection was not closed"
    );
    assert!(
        std::net::TcpStream::connect(addr).is_err(),
        "the listener outlived its endpoint"
    );
}

/// C14-B0: derive-python's installer fetches `c14pkg` whole from the endpoint,
/// the derivation imports it and derives the expected documents, and its VMM
/// is released.
#[test]
fn b0_installs_from_the_endpoint() {
    let f = fixture("b0");
    let mut violations = Vec::new();
    let installed = healthy(&f, "B0", &mut violations);
    append(
        &f.evidence,
        serde_json::json!({"summary": "B0", "installed_ms": installed.as_millis(),
                           "total_ms": f.t0.elapsed().as_millis(), "violations": violations}),
    );
    assert!(violations.is_empty(), "C14-B0: {violations:#?}");
}

/// C14-CUT: every download of `c14pkg` is cut at `CUT_AT` of its declared
/// length. The session must fail within `INSTALLED` of `Started` and never
/// open or publish, and its VMM be released. Then, with the endpoint whole
/// again, the same derivation opened afresh through the same launcher must
/// install, derive the expected documents and be released.
#[test]
fn cut_mid_body_then_relaunch() {
    let f = fixture("cut");
    let mut violations = Vec::new();

    set(&f.endpoint, Mode::Cut, "CUT");
    let logs = Logs::default();
    let launched = Instant::now();
    let mut session = start(
        &f.runtime,
        &f.router,
        open(&f.run.derive_python_image, &f.endpoint.url),
        &logs,
    );
    let started = launched.elapsed();
    let name = launches(&f.root)
        .last()
        .cloned()
        .expect("the launch named its network");
    let (opening, other) = opening(&f.runtime, &mut session, &logs, INSTALLED);
    let failed = launched.elapsed() - started;
    // Nothing more may come of it once failed: no late Opened or document.
    let (late, late_other) = match opening {
        Opening::TimedOut => (Opening::TimedOut, Vec::new()),
        _ => self::opening(&f.runtime, &mut session, &logs, Duration::from_secs(2)),
    };
    let ended = end(&f.runtime, session, &logs);
    let root = &f.root;
    let released = std::panic::catch_unwind(|| launcher::wait_released(root, &name, RELEASED));
    let requests = served(&f.endpoint, "CUT");
    let logs = logs.into_inner().unwrap();
    // uv's own failure for this download, by line (the session's logs or its
    // failure status), and none of what it prints for a finished download or
    // install.
    let failure = regex::Regex::new(r"(?i)failed to (download|fetch|read|extract|prepare)")
        .expect("a valid regex");
    let installed = regex::Regex::new(r"Installed \d+ package").expect("a valid regex");
    let status = match &opening {
        Opening::Failed(status) => status.lines().map(str::to_string).collect(),
        _ => Vec::new(),
    };
    let attributed = logs
        .iter()
        .chain(&status)
        .any(|line| line.contains("c14pkg") && failure.is_match(line));
    let completed: Vec<&String> = logs
        .iter()
        .filter(|line| line.contains("Downloaded c14pkg") || installed.is_match(line))
        .collect();

    append(
        &f.evidence,
        serde_json::json!({
            "phase": "CUT", "vmm": name, "started_ms": started.as_millis(),
            "failed_ms": failed.as_millis(), "opening": format!("{opening:?}"), "other": other,
            "late": format!("{late:?}"), "late_other": late_other, "ended": ended,
            "released": released.is_ok(), "requests": requests.len(), "attributed": attributed,
            "completed": completed, "logs": logs,
        }),
    );

    let cut = requests
        .iter()
        .filter(|r| {
            r["outcome"] == "cut" && r["written"] == CUT_AT && r["declared"] == f.wheel.bytes
        })
        .count();
    if cut == 0 {
        // No proof the fault was met: the rest says nothing.
        panic!("C14-CUT inconclusive: no request was cut: {requests:?}");
    }
    if cut
        != requests
            .iter()
            .filter(|r| r["request"].as_str().is_some_and(|r| r.starts_with("GET ")))
            .count()
    {
        violations.push(format!("CUT: a GET was not cut: {requests:?}"));
    }
    if !matches!(opening, Opening::Failed(_) | Opening::Ended) || failed > INSTALLED {
        violations.push(format!("CUT: {opening:?} after {failed:?}"));
    }
    if !other.is_empty() || !late_other.is_empty() || matches!(late, Opening::Opened) {
        violations.push(format!(
            "CUT: other {other:?}, late {late:?} {late_other:?}"
        ));
    }
    if !attributed {
        violations.push("CUT: no line of uv's naming a failed c14pkg download".to_string());
    }
    if !completed.is_empty() {
        violations.push(format!("CUT: uv reported completing: {completed:?}"));
    }
    if !ended || released.is_err() {
        violations.push(format!("CUT: ended {ended}, released {}", released.is_ok()));
    }

    let installed = healthy(&f, "RECOVER", &mut violations);
    let all = launches(&f.root);
    if all.len() != 2 || all[0] == all[1] {
        violations.push(format!("expected two launches, got {all:?}"));
    }
    let left = launcher::leftovers(&f.root, None);
    if !left.is_empty() {
        violations.push(format!("left {left:?}"));
    }
    append(
        &f.evidence,
        serde_json::json!({"summary": "CUT", "cut": cut, "failed_ms": failed.as_millis(),
                           "recovered_installed_ms": installed.as_millis(),
                           "total_ms": f.t0.elapsed().as_millis(), "violations": violations}),
    );
    assert!(violations.is_empty(), "C14-CUT: {violations:#?}");
}
