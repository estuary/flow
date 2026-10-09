//! The connector service's VMM launches, through the actual launcher. Each
//! test drives an in-process `connector::Service` whose VMM configuration
//! names this run's images, and a state directory, TMPDIR and podman of the
//! test's own (`connector_vmm_tests::launcher`). Recovery tests also launch
//! in other processes, which they kill as a crash would: this same binary,
//! running only `owner_process` (see `Owner`). Like the rest of the suite,
//! these run only under `mise run ci:connector-vmm-kvm`.

#[path = "common/rpc.rs"]
mod rpc;

use connector_vmm_tests::{dns, endpoint, launcher, netns, run};
use proto_flow::{connector as proto, derive, flow, ops};
use std::io::BufRead;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Generous beside a launch's usual seconds: it pulls its connector image.
const STARTED: Duration = Duration::from_secs(180);
const RELEASED: Duration = Duration::from_secs(60);

/// Each test accounts for the connector mounts the launcher makes beneath
/// TMPDIR, so each has its own, set before the test's runtime exists.
fn setup() -> (run::Run, launcher::Root, tokio::runtime::Runtime) {
    let run = run::load();
    let root = launcher::root();
    // SAFETY: no other thread exists yet to read the environment.
    unsafe { std::env::set_var("TMPDIR", &root.tmp_dir) };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    (run, root, runtime)
}

fn vmm(root: &launcher::Root, image: &str) -> connector::Vmm {
    vmm_at(&root.podman, &root.state_dir, image)
}

fn vmm_at(podman: &str, state_dir: &str, image: &str) -> connector::Vmm {
    connector::Vmm {
        image: image.to_string(),
        podman: podman.to_string(),
        state_dir: state_dir.to_string(),
        disk_mib: 2048,
        memory_limit: "1280m".to_string(),
        cpu_limit: "2".to_string(),
        guest_memory_mib: 1024,
        vcpus: 2,
        cgroup_parent: None,
    }
}

fn router(vmm: connector::Vmm) -> connector::ServiceRouter {
    let (_service, router) = connector::Service::new_local(
        String::new(),
        Some(vmm),
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    );
    router
}

fn router_on(plane: connector::Plane, vmm: connector::Vmm) -> connector::ServiceRouter {
    // The router signs and the service verifies, both within this test.
    let key = [7u8; 32];
    let service = connector::Service::new(
        plane,
        String::new(),
        Some(vmm),
        proto_grpc::Authenticator::new(
            connector::LOCAL_ISSUER.to_string(),
            vec![tokens::jwt::DecodingKey::from_secret(&key)],
        ),
        None,
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
        None,
    );
    let signer = proto_grpc::Signer::new(
        connector::LOCAL_ISSUER.to_string(),
        tokens::jwt::EncodingKey::from_secret(&key),
    );
    connector::ServiceRouter::new(service, signer)
}

fn vmm_execution() -> flow::ConnectorExecution {
    flow::ConnectorExecution {
        vmm: true,
        egress: None,
    }
}

fn spec(image: &str) -> proto::Request {
    spec_with(image, vmm_execution())
}

fn spec_with(image: &str, execution: flow::ConnectorExecution) -> proto::Request {
    proto::Request {
        start: Some(proto::request::Start {
            execution: Some(execution),
            log_level: ops::log::Level::Info as i32,
            ..Default::default()
        }),
        kind: Some(proto::request::Kind::Derive(derive::Request {
            kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Image as i32,
                config_json: serde_json::json!({"image": image, "config": {}})
                    .to_string()
                    .into(),
            })),
            ..Default::default()
        })),
    }
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<ops::Log>>>);

impl Logs {
    fn push(&self, log: &ops::Log) {
        self.0.lock().unwrap().push(log.clone());
    }

    fn messages(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|log| format!("{}: {}", log.level().as_str_name(), log.message))
            .collect()
    }
}

fn unary(
    runtime: &tokio::runtime::Runtime,
    router: &connector::ServiceRouter,
    logs: &Logs,
    request: proto::Request,
) -> anyhow::Result<proto::response::Started> {
    let logger = |log: &ops::Log| logs.push(log);
    runtime
        .block_on(proto_grpc::connector::unary(
            router, &logger, request, STARTED, STARTED,
        ))
        .map(|(started, _response)| started)
}

/// A Spec through `router` which must succeed: a launch, whose start releases
/// what dead launches left beneath its state directory.
fn launch(runtime: &tokio::runtime::Runtime, router: &connector::ServiceRouter, image: &str) {
    let logs = Logs::default();
    let started = unary(runtime, router, &logs, spec(image))
        .unwrap_or_else(|err| panic!("launching: {err:#}\n{:#?}", logs.messages()));
    assert_started(&started);
}

/// Open a session without waiting on it, as a client which may walk away.
fn open(
    runtime: &tokio::runtime::Runtime,
    router: &connector::ServiceRouter,
    request: proto::Request,
) -> (
    tokio::sync::mpsc::Sender<proto::Request>,
    tokio::sync::mpsc::Receiver<tonic::Result<proto::Response>>,
) {
    use proto_grpc::connector::Router;

    let _entered = runtime.enter();
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
    request_tx.try_send(request).expect("the channel is empty");
    let response_rx = router.open(
        ops::TaskType::Derivation,
        proto_grpc::connector::SPEC_TASK_NAME,
        request_rx,
    );
    (request_tx, response_rx)
}

/// Drain startup logs while awaiting the hold, so backpressure cannot stall
/// startup and a failed launch ends the wait with its diagnostics.
fn held(
    root: &launcher::Root,
    what: &str,
    response_rx: &mut tokio::sync::mpsc::Receiver<tonic::Result<proto::Response>>,
) -> Result<(), String> {
    let path = format!("{}/held-{what}", root.dir);
    let mut logs = Vec::new();
    run::eventually(&format!("podman to hold {what}"), STARTED, || {
        loop {
            let ended = match response_rx.try_recv() {
                Ok(Ok(proto::Response {
                    kind: Some(proto::response::Kind::Log(log)),
                })) => {
                    logs.push(format!("{}: {}", log.level().as_str_name(), log.message));
                    continue;
                }
                Ok(Err(status)) => format!("failed: {status}"),
                Ok(Ok(response)) => format!("sent {response:?}"),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => "ended".to_string(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            };
            return Some(Err(format!(
                "the session {ended} before podman held {what}\n{logs:#?}"
            )));
        }
        launcher::path_exists(&path).then_some(Ok(()))
    })
}

fn assert_started(started: &proto::response::Started) {
    assert_eq!(
        started.execution,
        Some(vmm_execution()),
        "Started echoes VMM execution"
    );
    let Some(proto::response::started::Spec::Derive(spec)) = &started.spec else {
        panic!("expected a derivation's Spec, got {:?}", started.spec);
    };
    assert!(!spec.config_schema_json.is_empty(), "a config schema");

    let container = started.container.as_ref().expect("a container");
    assert_eq!(
        container.ip_addr, "",
        "no address: the VMM has only its socket"
    );
    assert!(container.network_ports.is_empty(), "no ports");
}

/// `proto_grpc` hands back an Unknown status as a plain error.
fn status(err: &anyhow::Error) -> (tonic::Code, String) {
    match err.downcast_ref::<proto_grpc::StatusError>() {
        Some(status) => (status.0.code(), status.0.message().to_string()),
        None => (tonic::Code::Unknown, format!("{err:#}")),
    }
}

fn normalized_calls(root: &launcher::Root, run: &run::Run, name: &str) -> String {
    let calls = launcher::calls(root).join("\n");
    let mount = calls
        .split(' ')
        .find_map(|arg| arg.strip_prefix("--env=CONNECTOR_MOUNT="))
        .map(ToString::to_string);
    let token = calls
        .split(' ')
        .find_map(|arg| arg.strip_prefix("--label=dev.estuary.vmm-owner="))
        .map(ToString::to_string);

    let mut calls = calls
        .replace(&run.fake_image, "<fake image>")
        .replace(&run.vmm_image, "<vmm image>");
    if let Some(mount) = mount {
        calls = calls.replace(&mount, "<mount>");
    }
    if let Some(token) = token {
        calls = calls.replace(&token, "<token>");
    }
    let calls = calls
        .replace(&root.dir, "<test>")
        .replace(&format!("fvm{}", &name[3..15]), "fvm<id>")
        .replace(name, "fv_<id>");

    let id = |word: &str| word.len() == 64 && word.bytes().all(|b| b.is_ascii_hexdigit());
    calls
        .lines()
        .map(|line| {
            line.split(' ')
                .map(|word| if id(word) { "<container id>" } else { word })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

const OWNER: &str = "CONNECTOR_VMM_TESTS_OWNER";

#[derive(serde::Deserialize, serde::Serialize)]
struct OwnerConfig {
    podman: String,
    state_dir: String,
    vmm_image: String,
    image: String,
}

/// Not a test of its own: the launch of an `Owner`, which runs this binary
/// with only this test selected and `OWNER` set. It reports on stdout, holds
/// its session until its stdin closes, then ends it and reports its release.
#[test]
#[ignore = "run only by Owner::spawn, as a process of its own"]
fn owner_process() {
    let Some(config) = std::env::var_os(OWNER) else {
        return;
    };
    let OwnerConfig {
        podman,
        state_dir,
        vmm_image,
        image,
    } = serde_json::from_str(config.to_str().expect("UTF-8")).expect("an owner's configuration");

    // The host's /proc names this process by its PID on the host, whatever
    // its own namespace calls it.
    let host_pid = std::fs::read_link("/proc/self").expect("reading /proc/self");
    println!("owner: pid {} {}", std::process::id(), host_pid.display());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    let router = router(vmm_at(&podman, &state_dir, &vmm_image));
    let logger = |log: &ops::Log| {
        eprintln!(
            "owner {}: {}: {}",
            host_pid.display(),
            log.level().as_str_name(),
            log.message
        )
    };

    let (request_tx, mut response_rx, started) =
        match runtime.block_on(proto_grpc::connector::start(
            &router,
            &logger,
            proto_grpc::connector::SPEC_TASK_NAME,
            spec(&image),
        )) {
            Ok(session) => session,
            Err(err) => {
                println!("owner: failed {err:#}");
                return;
            }
        };
    assert_started(&started);
    println!("owner: started");

    for _ in std::io::stdin().lines() {}
    std::mem::drop(request_tx);
    runtime.block_on(async { while response_rx.recv().await.is_some() {} });
    println!("owner: released");
}

/// A launch in another process: this binary, as `owner_process`.
struct Owner {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    reports: std::sync::mpsc::Receiver<String>,
    /// Its PID in its own namespace.
    pid: u32,
    /// Its PID on the host, by which it's killed.
    host_pid: i32,
}

impl Owner {
    /// Launch `image` in a VMM of `vmm_image`, with `root`'s podman and
    /// TMPDIR, beneath `state_dir`: in a new PID namespace of its own if
    /// `namespaced`, where the owner's PID is always 1 and its podman runs in
    /// the host's.
    fn spawn(
        run: &run::Run,
        root: &launcher::Root,
        state_dir: &str,
        vmm_image: &str,
        namespaced: bool,
    ) -> Self {
        let config = serde_json::to_string(&OwnerConfig {
            podman: root.podman.clone(),
            state_dir: state_dir.to_string(),
            vmm_image: vmm_image.to_string(),
            image: run.derive_python_image.clone(),
        })
        .unwrap();
        let exe = std::env::current_exe().expect("this test binary");
        let exe = exe.to_str().expect("a UTF-8 path");
        let test = [
            "owner_process",
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ];

        let mut command = if namespaced {
            // SAFETY: both take nothing and cannot fail.
            let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
            // The owner itself runs unprivileged, as the test does, and finds
            // flow-connector-init on the suite's PATH.
            let path = std::env::var("PATH").expect("PATH");
            let mut command = std::process::Command::new("sudo");
            command
                .args(["-n", "unshare", "--pid", "--fork", "setpriv"])
                .arg(format!("--reuid={uid}"))
                .arg(format!("--regid={gid}"))
                .args(["--init-groups", "env"])
                .arg(format!("{OWNER}={config}"))
                .arg(format!("TMPDIR={}", root.tmp_dir))
                .arg(format!("PATH={path}"))
                .arg("CONNECTOR_VMM_TESTS_HOST_PID=1")
                .arg(exe)
                .args(test);
            command
        } else {
            let mut command = std::process::Command::new(exe);
            command
                .args(test)
                .env(OWNER, &config)
                .env("TMPDIR", &root.tmp_dir);
            command
        };
        let mut child = command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("spawning an owner: {e}"));

        let stdout = child.stdout.take().expect("stdout is piped");
        let (reports_tx, reports) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
            {
                // libtest may have begun the line with the test's name.
                if let Some((_, report)) = line.split_once("owner: ")
                    && reports_tx.send(report.to_string()).is_err()
                {
                    return;
                }
            }
        });

        let mut owner = Self {
            stdin: child.stdin.take(),
            child,
            reports,
            pid: 0,
            host_pid: 0,
        };
        let pids = owner.report("pid", STARTED);
        let (pid, host_pid) = pids.split_once(' ').expect("two PIDs");
        owner.pid = pid.parse().expect("a PID");
        owner.host_pid = host_pid.parse().expect("a PID");
        owner
    }

    /// The owner's next report, which must be `what`, and what follows it.
    fn report(&self, what: &str, timeout: Duration) -> String {
        let report = self
            .reports
            .recv_timeout(timeout)
            .unwrap_or_else(|e| panic!("waiting for an owner's {what} report: {e}"));
        match report.strip_prefix(what) {
            Some(rest) => rest.trim().to_string(),
            None => panic!("expected an owner's {what} report, not {report:?}"),
        }
    }

    fn started(&self) {
        self.report("started", STARTED);
    }

    fn kill(mut self) {
        // SAFETY: kill takes plain integers.
        let killed = unsafe { libc::kill(self.host_pid, libc::SIGKILL) };
        assert_eq!(killed, 0, "killing owner {}", self.host_pid);
        self.child.wait().expect("waiting for a killed owner");
    }

    /// End the owner's session, as a client which is done with it would.
    fn end(mut self) {
        std::mem::drop(self.stdin.take());
        self.report("released", RELEASED);
        self.child.wait().expect("waiting for an owner");
    }
}

/// An RPC held open on the VMM of launch `name`, from this process.
fn hold_rpc(
    runtime: &tokio::runtime::Runtime,
    state_dir: &str,
    name: &str,
) -> tonic::Streaming<derive::Response> {
    rpc::hold(
        runtime,
        &format!("{state_dir}/{name}/sock/init.sock"),
        STARTED,
    )
}

fn removed(root: &launcher::Root, id: &str) -> bool {
    let removal = format!("rm --force --time=0 --ignore {id}");
    launcher::calls(root).contains(&removal)
}

/// The fake, verified with the real boundary, serving a Spec of the real
/// Python image. The session ends only once its VMM's teardown has.
#[test]
fn spec_through_the_fake() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.fake_image));
    let logs = Logs::default();

    let started = unary(&runtime, &router, &logs, spec(&run.derive_python_image))
        .unwrap_or_else(|err| panic!("Spec through the fake: {err:#}\n{:#?}", logs.messages()));
    assert_started(&started);

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new(),
        "teardown finished before the session did"
    );
    insta::assert_snapshot!(normalized_calls(&root, &run, &name));
}

/// The real VMM: derive-python runs as its image's user, which is not root,
/// and the unprivileged test process dialed it.
#[test]
fn spec_through_the_vmm() {
    for name in [
        "CONSUMER_AUTH_KEYS",
        "BROKER_AUTH_KEYS",
        "SOPS_AGE_KEY",
        "FLOW_AUTH_TOKEN",
    ] {
        // SAFETY: no other thread exists yet to read the environment.
        unsafe { std::env::set_var(name, "synthetic-platform-secret") };
    }
    let (run, root, runtime) = setup();
    std::fs::write(format!("{}/require-clean-environment", root.dir), "").unwrap();
    // SAFETY: geteuid takes nothing and cannot fail.
    assert_ne!(unsafe { libc::geteuid() }, 0, "the suite runs unprivileged");
    let user = run::podman(&[
        "image",
        "inspect",
        "--format",
        "{{.Config.User}}",
        &run.derive_python_image,
    ]);
    assert_eq!(user.trim(), "nobody", "the connector's image user");

    let router = router(vmm(&root, &run.vmm_image));
    let logs = Logs::default();

    let started = unary(&runtime, &router, &logs, spec(&run.derive_python_image))
        .unwrap_or_else(|err| panic!("Spec through the VMM: {err:#}\n{:#?}", logs.messages()));
    assert_started(&started);

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
    let commands: Vec<String> = launcher::calls(&root)
        .iter()
        .map(|call| call.split(' ').take(2).collect::<Vec<_>>().join(" "))
        .collect();
    assert_eq!(
        commands,
        [
            format!("pull {}", run.derive_python_image).as_str(),
            "image inspect",
            "run --rm",
            "network create",
            "create --rm",
            "start --attach",
            "rm --force",
            "ps --all",
            "network ls",
            "network rm",
        ]
    );
    assert_eq!(
        std::fs::read_to_string(format!("{}/clean-environment", root.dir)).unwrap(),
        std::fs::read_to_string(format!("{}/calls", root.dir)).unwrap(),
        "every engine call was checked before sudo"
    );
}

/// Run as guest root before connector-init starts: for each name and port,
/// resolve it through the guest's nameserver and connect, then write one log
/// line saying what happened. `unresolved` is the VMM's resolver refusing the
/// name, `unreachable` a connection which never completed.
const PROBE: &str = r#"
import json, socket, sys
for name, port in json.loads(CASES):
    try:
        address = socket.getaddrinfo(name, port, socket.AF_INET, socket.SOCK_STREAM)[0][4]
    except OSError:
        result = "unresolved"
    else:
        sock = socket.socket()
        sock.settimeout(2)
        try:
            sock.connect(address)
            result = "connected"
        except OSError:
            result = "unreachable"
        sock.close()
    line = {"level": "info", "message": "egress probe", "fields": {"name": name, "result": result}}
    sys.stderr.write(json.dumps(line) + "\n")
    sys.stderr.flush()
"#;

/// The controlled destinations of one egress test: three public endpoints and
/// the decoy, each listening, and a nameserver which answers for them.
struct Destinations {
    nameserver: dns::Nameserver,
    a: endpoint::Endpoint,
    b: endpoint::Endpoint,
    c: endpoint::Endpoint,
    decoy: endpoint::Endpoint,
}

/// Every name is answered, so that a refusal is the VMM's rather than the
/// nameserver's. `leak.acmeco.example` answers only with the decoy, an
/// address in an excluded range.
fn destinations(run: &run::Run) -> Destinations {
    let [a, b, c, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let decoy = run.decoys[0];
    let listen = |ip| {
        endpoint::listen(netns::tcp(
            netns::Namespace::Named(&run.endpoint_netns),
            (ip, 0).into(),
        ))
    };
    let nameserver = dns::serve(netns::udp(
        netns::Namespace::Named(&run.endpoint_netns),
        (a, 0).into(),
    ));
    for (name, ip) in [
        ("api.acmeco.example", a),
        ("below.api.acmeco.example", a),
        ("other.acmeco.example", a),
        ("svc.acmeco.example", b),
        ("a.b.svc.acmeco.example", b),
        ("leak.acmeco.example", decoy),
    ] {
        dns::set(&nameserver, name, vec![dns::Record::A(ip, 300)]);
    }

    Destinations {
        nameserver,
        a: listen(a),
        b: listen(b),
        c: listen(c),
        decoy: listen(decoy),
    }
}

struct Probed {
    /// The policy the launcher wrote for the VMM.
    policy: String,
    results: Vec<(String, String)>,
    /// Every session log, as `LEVEL: message`.
    messages: Vec<String>,
}

/// Launch derive-python's Spec on a `plane` service, as a task whose egress is
/// `egress`, with the guest probing `cases` before connector-init starts. The
/// launcher plans and writes the policy; this test's podman only appends the
/// VMM's test-only flags, for the nameserver and the probe.
fn probe_egress(
    (run, root, runtime): &(run::Run, launcher::Root, tokio::runtime::Runtime),
    plane: connector::Plane,
    egress: Option<&[&str]>,
    cases: &[(String, u16)],
    destinations: &Destinations,
) -> Probed {
    use base64::Engine;
    let router = router_on(plane, vmm(root, &run.vmm_image));

    let cases = serde_json::to_string(cases).expect("cases serialize");
    let script = PROBE.replace(
        "CASES",
        &serde_json::to_string(&cases).expect("a string serializes"),
    );
    let encoded = base64::engine::general_purpose::STANDARD.encode(script);
    launcher::vmm_flags(
        root,
        &[
            "--resolver-upstream".to_string(),
            destinations.nameserver.addr.to_string(),
            "--as-root-exec".to_string(),
            format!(
                "{} -c \"import base64; exec(base64.b64decode('{encoded}'))\"",
                run::PROBES[0]
            ),
        ],
    );
    let execution = flow::ConnectorExecution {
        vmm: true,
        egress: egress.map(|hosts| flow::connector_execution::Egress {
            hosts: hosts.iter().map(ToString::to_string).collect(),
        }),
    };

    // Held once the container exists, to read what the launcher wrote.
    let hold = launcher::hold(root, "start");
    let logs = Logs::default();
    let task = {
        let (router, logs) = (router.clone(), logs.clone());
        let request = spec_with(&run.derive_python_image, execution.clone());
        runtime.spawn(async move {
            let logger = move |log: &ops::Log| logs.push(log);
            proto_grpc::connector::unary(&router, &logger, request, STARTED, STARTED)
                .await
                .map(|(started, _response)| started)
        })
    };
    launcher::wait_held(root, "start", STARTED);
    let name = launcher::vmm_name(root).expect("the launch created a network");
    let policy_path = format!("{}/{name}/init/policy.json", root.state_dir);
    let policy = std::fs::read_to_string(&policy_path)
        .unwrap_or_else(|e| panic!("reading {policy_path}: {e}"));
    std::mem::drop(hold);

    let started = runtime
        .block_on(task)
        .expect("the launch task completes")
        .unwrap_or_else(|err| panic!("launching: {err:#}\n{:#?}", logs.messages()));
    assert_eq!(
        started.execution,
        Some(execution),
        "Started echoes the task's egress"
    );
    assert_eq!(launcher::leftovers(root, Some(&name)), Vec::<String>::new());

    let results = logs
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|log| log.message == "egress probe")
        .map(|log| {
            let field = |key: &str| -> String {
                let raw = log
                    .fields_json_map
                    .get(key)
                    .unwrap_or_else(|| panic!("a probe line has {key}"));
                serde_json::from_slice(raw).expect("a string field")
            };
            (field("name"), field("result"))
        })
        .collect();

    Probed {
        policy,
        results,
        messages: logs.messages(),
    }
}

fn case(name: impl ToString, endpoint: &endpoint::Endpoint) -> (String, u16) {
    (name.to_string(), endpoint.addr.port())
}

fn direct(endpoint: &endpoint::Endpoint) -> (String, u16) {
    (endpoint.addr.ip().to_string(), endpoint.addr.port())
}

fn assert_results(probed: &Probed, expected: &[(String, &str)]) {
    let expected: Vec<(String, String)> = expected
        .iter()
        .map(|(name, result)| (name.clone(), result.to_string()))
        .collect();
    assert_eq!(probed.results, expected, "{:#?}", probed.messages);
}

fn assert_logged(probed: &Probed, message: &str) {
    assert!(
        probed
            .messages
            .iter()
            .any(|logged| logged.ends_with(message)),
        "no log {message:?} in {:#?}",
        probed.messages
    );
}

/// A public data plane holds a task to the hosts it declares beyond the
/// connector's: an exact name and not its subdomains, every name beneath a
/// wildcard's base and not the base, and nothing which resolves to an
/// excluded address, whatever the task declares.
#[test]
fn task_egress_on_a_public_plane() {
    let launch = setup();
    let (run, ..) = &launch;
    let d = destinations(run);
    let probed = probe_egress(
        &launch,
        connector::Plane::Public,
        Some(&[
            "api.acmeco.example",
            "*.svc.acmeco.example",
            "leak.acmeco.example",
        ]),
        &[
            case("api.acmeco.example", &d.a),
            case("below.api.acmeco.example", &d.a),
            case("a.b.svc.acmeco.example", &d.b),
            case("svc.acmeco.example", &d.b),
            case("other.acmeco.example", &d.a),
            case("leak.acmeco.example", &d.decoy),
            direct(&d.decoy),
            direct(&d.c),
        ],
        &d,
    );

    assert_eq!(
        probed.policy,
        r#"{"allowedNames":["pypi.org","files.pythonhosted.org","api.acmeco.example","*.svc.acmeco.example","leak.acmeco.example"],"egress":"public"}"#
    );
    assert_results(
        &probed,
        &[
            ("api.acmeco.example".to_string(), "connected"),
            ("below.api.acmeco.example".to_string(), "unresolved"),
            ("a.b.svc.acmeco.example".to_string(), "connected"),
            ("svc.acmeco.example".to_string(), "unresolved"),
            ("other.acmeco.example".to_string(), "unresolved"),
            ("leak.acmeco.example".to_string(), "unresolved"),
            (direct(&d.decoy).0, "unreachable"),
            (direct(&d.c).0, "unreachable"),
        ],
    );
    assert_eq!(endpoint::accepted(&d.a), [run.endpoint_peer]);
    assert_eq!(endpoint::accepted(&d.b), [run.endpoint_peer]);
    assert!(
        endpoint::accepted(&d.c).is_empty(),
        "{:?}",
        endpoint::accepted(&d.c)
    );
    assert!(
        endpoint::accepted(&d.decoy).is_empty(),
        "{:?}",
        endpoint::accepted(&d.decoy)
    );

    assert_logged(
        &probed,
        "VMM egress permits only these host names: pypi.org, files.pythonhosted.org \
         (connector defaults); api.acmeco.example, *.svc.acmeco.example, \
         leak.acmeco.example (task egress.hosts)",
    );
    for name in [
        "below.api.acmeco.example",
        "svc.acmeco.example",
        "other.acmeco.example",
    ] {
        assert_logged(
            &probed,
            &format!(
                "refused DNS name {name}, which this connector's egress does not permit; \
                 a task may permit it in egress.hosts"
            ),
        );
    }
    assert_logged(
        &probed,
        "refused DNS name leak.acmeco.example, which resolved to an address that is not public",
    );
    let asked = dns::queries(&d.nameserver);
    for name in [
        "below.api.acmeco.example",
        "svc.acmeco.example",
        "other.acmeco.example",
    ] {
        assert!(
            !asked.iter().any(|asked| asked == name),
            "{name} went upstream: {asked:?}"
        );
    }
}

#[test]
fn undeclared_egress_on_a_public_plane() {
    let launch = setup();
    let (run, ..) = &launch;
    let d = destinations(run);
    let probed = probe_egress(
        &launch,
        connector::Plane::Public,
        None,
        &[
            case("api.acmeco.example", &d.a),
            case("a.b.svc.acmeco.example", &d.b),
            direct(&d.c),
        ],
        &d,
    );

    assert_eq!(
        probed.policy,
        r#"{"allowedNames":["pypi.org","files.pythonhosted.org"],"egress":"public"}"#
    );
    assert_results(
        &probed,
        &[
            ("api.acmeco.example".to_string(), "unresolved"),
            ("a.b.svc.acmeco.example".to_string(), "unresolved"),
            (direct(&d.c).0, "unreachable"),
        ],
    );
    for endpoint in [&d.a, &d.b, &d.c] {
        assert!(
            endpoint::accepted(endpoint).is_empty(),
            "{:?}",
            endpoint::accepted(endpoint)
        );
    }
    assert_logged(
        &probed,
        "VMM egress permits only these host names: pypi.org, files.pythonhosted.org \
         (connector defaults)",
    );
}

#[test]
fn undeclared_egress_on_a_private_plane() {
    let launch = setup();
    let (run, ..) = &launch;
    let d = destinations(run);
    let probed = probe_egress(
        &launch,
        connector::Plane::Private,
        None,
        &[
            case("other.acmeco.example", &d.a),
            case("leak.acmeco.example", &d.decoy),
            direct(&d.decoy),
            direct(&d.c),
        ],
        &d,
    );

    assert_eq!(probed.policy, r#"{"allowAll":true,"egress":"public"}"#);
    assert_results(
        &probed,
        &[
            ("other.acmeco.example".to_string(), "connected"),
            ("leak.acmeco.example".to_string(), "unresolved"),
            (direct(&d.decoy).0, "unreachable"),
            (direct(&d.c).0, "connected"),
        ],
    );
    assert_eq!(endpoint::accepted(&d.a), [run.endpoint_peer]);
    assert_eq!(endpoint::accepted(&d.c), [run.endpoint_peer]);
    assert!(
        endpoint::accepted(&d.decoy).is_empty(),
        "{:?}",
        endpoint::accepted(&d.decoy)
    );
    assert_logged(
        &probed,
        "VMM egress permits any public destination: the task declares no egress, \
         and this data plane does not require one",
    );
    assert_logged(
        &probed,
        "refused DNS name leak.acmeco.example, which resolved to an address that is not public",
    );
}

#[test]
fn empty_egress_on_a_private_plane() {
    let launch = setup();
    let (run, ..) = &launch;
    let d = destinations(run);
    let probed = probe_egress(
        &launch,
        connector::Plane::Private,
        Some(&[]),
        &[case("other.acmeco.example", &d.a), direct(&d.c)],
        &d,
    );

    assert_eq!(
        probed.policy,
        r#"{"allowedNames":["pypi.org","files.pythonhosted.org"],"egress":"public"}"#
    );
    assert_results(
        &probed,
        &[
            ("other.acmeco.example".to_string(), "unresolved"),
            (direct(&d.c).0, "unreachable"),
        ],
    );
    assert!(
        endpoint::accepted(&d.a).is_empty(),
        "{:?}",
        endpoint::accepted(&d.a)
    );
    assert!(
        endpoint::accepted(&d.c).is_empty(),
        "{:?}",
        endpoint::accepted(&d.c)
    );
    assert_logged(
        &probed,
        "refused DNS name other.acmeco.example, which this connector's egress does not \
         permit; a task may permit it in egress.hosts",
    );
}

#[test]
fn abandoned_before_the_vmm_runs() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.fake_image));

    let hold = launcher::hold(&root, "network-create");
    let (request_tx, mut response_rx) = open(&runtime, &router, spec(&run.derive_python_image));
    held(&root, "network-create", &mut response_rx).unwrap_or_else(|err| panic!("{err}"));
    std::mem::drop((request_tx, response_rx));
    std::mem::drop(hold);

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    launcher::wait_released(&root, &name, RELEASED);

    let calls = launcher::calls(&root);
    assert!(
        !calls.iter().any(|call| call.contains("--name=")),
        "the VMM was created: {calls:#?}"
    );
    assert_eq!(calls.last(), Some(&format!("network rm {name}")));
}

#[test]
fn abandoned_while_the_container_is_created() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.fake_image));

    let hold = launcher::hold(&root, "create");
    let (request_tx, mut response_rx) = open(&runtime, &router, spec(&run.derive_python_image));
    held(&root, "create", &mut response_rx).unwrap_or_else(|err| panic!("{err}"));
    std::mem::drop((request_tx, response_rx));
    std::mem::drop(hold);

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    launcher::wait_released(&root, &name, RELEASED);

    let calls = launcher::calls(&root);
    assert!(
        calls.iter().any(|call| call.starts_with("rm --force")),
        "the container was removed: {calls:#?}"
    );
    assert!(
        !calls.iter().any(|call| call.starts_with("start ")),
        "the container was started: {calls:#?}"
    );
}

#[test]
fn abandoned_before_readiness() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.vmm_image));

    launcher::gate_readiness(&root);
    let (request_tx, mut response_rx) = open(&runtime, &router, spec(&run.derive_python_image));
    held(&root, "readiness", &mut response_rx).unwrap_or_else(|err| panic!("{err}"));
    std::mem::drop((request_tx, response_rx));

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    launcher::wait_released(&root, &name, RELEASED);

    let calls = launcher::calls(&root);
    assert!(
        calls.iter().any(|call| call.starts_with("rm --force")),
        "the container was removed: {calls:#?}"
    );
}

/// A failed start must end the readiness wait after teardown completes.
#[test]
fn a_failed_start_ends_the_wait_for_readiness() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.vmm_image));

    launcher::gate_readiness(&root);
    let _fault = launcher::fail(&root, "start", "");
    let (request_tx, mut response_rx) = open(&runtime, &router, spec(&run.derive_python_image));
    let err =
        held(&root, "readiness", &mut response_rx).expect_err("a failed start is never ready");
    assert!(
        err.contains("the VMM exited before flow-connector-init started")
            && err.contains("podman.sh: failing start, as the test asks"),
        "{err}"
    );
    std::mem::drop((request_tx, response_rx));

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
}

#[test]
fn abandoned_after_readiness() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.vmm_image));
    let logs = Logs::default();
    let logger = |log: &ops::Log| logs.push(log);

    let (request_tx, response_rx, started) = runtime
        .block_on(proto_grpc::connector::start(
            &router,
            &logger,
            proto_grpc::connector::SPEC_TASK_NAME,
            spec(&run.derive_python_image),
        ))
        .unwrap_or_else(|err| panic!("starting: {err:#}\n{:#?}", logs.messages()));
    assert_started(&started);

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    std::mem::drop((request_tx, response_rx));

    launcher::wait_released(&root, &name, RELEASED);
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
}

/// A removal podman refuses (a sidecar on the VMM's network) is reported, and
/// the rest is removed. The record is kept as it was and let go, and the next
/// launch releases what remains once it can.
#[test]
fn a_failed_release_is_kept_and_retried() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.fake_image));
    let logs = Logs::default();
    let logger = |log: &ops::Log| logs.push(log);

    let (request_tx, mut response_rx, started) = runtime
        .block_on(proto_grpc::connector::start(
            &router,
            &logger,
            proto_grpc::connector::SPEC_TASK_NAME,
            spec(&run.derive_python_image),
        ))
        .unwrap_or_else(|err| panic!("starting: {err:#}\n{:#?}", logs.messages()));
    assert_started(&started);

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    let record = launcher::record(&root.state_dir, &name);
    let written = std::fs::read(&record).expect("the launch's record");
    let sidecar = run::sidecar(&run, &name);

    std::mem::drop(request_tx);
    let failures: Vec<ops::Log> = runtime.block_on(async {
        let mut failures = Vec::new();
        while let Some(response) = response_rx.recv().await {
            if let Ok(proto::Response {
                kind: Some(proto::response::Kind::Log(log)),
            }) = response
                && log.message == "failed to release a VMM resource"
            {
                failures.push(log);
            }
        }
        failures
    });

    let field = |log: &ops::Log, field: &str| {
        String::from_utf8_lossy(&log.fields_json_map[field]).into_owned()
    };
    let resources: Vec<String> = failures.iter().map(|log| field(log, "resource")).collect();
    assert_eq!(resources, [format!("\"network {name}\"")]);
    assert_eq!(failures[0].level(), ops::log::Level::Error);
    assert_eq!(field(&failures[0], "record"), format!("{record:?}"));
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        [format!("network {name}"), record.clone()],
        "everything else was removed"
    );
    assert_eq!(std::fs::read(&record).unwrap(), written, "kept as it was");
    assert!(!launcher::locked(&record), "and let go");

    std::mem::drop(sidecar);
    launch(&runtime, &router, &run.derive_python_image);
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
}

#[test]
fn an_unverified_boundary_refuses_the_launch() {
    let (run, root, runtime) = setup();

    let image = format!(
        "localhost/connector-vmm-kvm-unverifiable:{}",
        run::random_hex()
    );
    run::record("image", &image);
    let containerfile = format!("FROM {}\nENTRYPOINT [\"/bin/false\"]\n", run.fake_image);
    let built = run::sudo_output(
        &["podman", "build", "-q", "-t", &image, "-f", "-", &root.dir],
        Some(containerfile.as_bytes()),
    );
    assert!(
        built.status.success(),
        "building {image}: {}",
        String::from_utf8_lossy(&built.stderr)
    );

    let router = router(vmm(&root, &image));
    let logs = Logs::default();
    let err = unary(&runtime, &router, &logs, spec(&run.derive_python_image))
        .expect_err("a launch whose boundary does not verify");
    let (code, message) = status(&err);
    assert_eq!(code, tonic::Code::FailedPrecondition, "{message}");
    assert!(
        message.contains("the host's VMM network boundary did not verify"),
        "{message}"
    );

    let calls = launcher::calls(&root);
    assert!(
        !calls.iter().any(|call| call.starts_with("network create")),
        "a network was created: {calls:#?}"
    );
    assert_eq!(launcher::leftovers(&root, None), Vec::<String>::new());

    run::podman(&["image", "rm", &image]);
}

#[test]
fn a_vmm_which_fails_refuses_the_launch() {
    let (run, root, runtime) = setup();
    // An empty scratch disk, which mkfs.ext4 refuses before the VM starts.
    let router = router(connector::Vmm {
        disk_mib: 0,
        ..vmm(&root, &run.vmm_image)
    });
    let logs = Logs::default();

    let err = unary(&runtime, &router, &logs, spec(&run.derive_python_image))
        .expect_err("a VMM which cannot start");
    let (code, message) = status(&err);
    assert_eq!(code, tonic::Code::Unknown, "{message}");
    assert!(
        message.contains("the VMM exited before flow-connector-init started"),
        "{message}"
    );
    let messages = logs.messages();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("flow-connector-vmm:")),
        "the VMM's own failure is in the session's logs: {messages:#?}"
    );

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
}

/// An owner killed while its podman, held at `step`, is about to create its
/// `kind` (a network or container, as `exists` finds it): the command goes on
/// without its owner, and holds the record's lock until it's done. A launch
/// meanwhile passes the record over; the next, once the command is done,
/// releases what it made.
fn an_orphaned_command(step: &str, kind: &str, exists: fn(&str) -> bool) {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let successor = router(vmm_at(&other.podman, &root.state_dir, &run.fake_image));

    let hold = launcher::hold(&root, step);
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.fake_image, false);
    launcher::wait_held(&root, step, STARTED);
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    owner.kill();

    assert!(launcher::locked(&record), "the orphaned command holds it");
    launch(&runtime, &successor, &run.derive_python_image);
    assert!(launcher::locked(&record), "passed over");
    assert!(!exists(&name), "nothing was made yet");

    std::mem::drop(hold);
    run::eventually(
        &format!("the orphaned command to create the {kind} and end"),
        STARTED,
        || (exists(&name) && !launcher::locked(&record)).then_some(()),
    );

    launch(&runtime, &successor, &run.derive_python_image);
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, &name),
        Vec::<String>::new(),
        "released by the launch, before any cleanup of the test's"
    );
}

#[test]
fn a_kill_while_the_network_is_created() {
    an_orphaned_command("network-create", "network", launcher::network_exists);
}

#[test]
fn a_kill_while_the_container_is_created() {
    an_orphaned_command("create", "container", launcher::container_exists);
}

/// podman made the container, and its command ended, but its owner died
/// before learning the container's ID: it's found by its label.
#[test]
fn a_kill_before_the_container_id_is_read() {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let successor = router(vmm_at(&other.podman, &root.state_dir, &run.fake_image));

    let hold = launcher::hold(&root, "created");
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.fake_image, false);
    launcher::wait_held(&root, "created", STARTED);
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    owner.kill();

    assert!(
        launcher::container_exists(&name),
        "podman made the container"
    );
    assert!(!launcher::locked(&record), "its command let go as it ended");
    launch(&runtime, &successor, &run.derive_python_image);
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, &name),
        Vec::<String>::new()
    );
    std::mem::drop(hold);
}

#[test]
fn a_kill_before_readiness() {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let successor = router(vmm_at(&other.podman, &root.state_dir, &run.fake_image));

    launcher::gate_readiness(&root);
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    launcher::wait_held(&root, "readiness", STARTED);
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    // The pre-init gate keeps the guest alive without starting init's idle watchdog.
    owner.kill();

    launch(&runtime, &successor, &run.derive_python_image);
    assert!(removed(&other, &footprint.id), "the launch removed it");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
}

/// A VMM serving its session when its owner is killed is released by the
/// next launch, which leaves alone a live owner sharing its state directory
/// and one beneath another, as another stack's would be.
#[test]
fn a_kill_while_serving_spares_live_owners() {
    let (run, root, runtime) = setup();
    let (neighbour, stranger, other) = (launcher::root(), launcher::root(), launcher::root());
    let successor = router(vmm_at(&other.podman, &root.state_dir, &run.fake_image));

    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    owner.started();
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    let _rpc = hold_rpc(&runtime, &root.state_dir, &name);

    let alive: Vec<(Owner, launcher::Footprint, &launcher::Root, &str)> = [
        (&neighbour, root.state_dir.as_str()),
        (&stranger, stranger.state_dir.as_str()),
    ]
    .into_iter()
    .map(|(at, state_dir)| {
        let live = Owner::spawn(&run, at, state_dir, &run.fake_image, false);
        live.started();
        let name = launcher::vmm_name(at).expect("the launch named its network");
        let footprint = launcher::footprint(state_dir, &at.tmp_dir, &name);
        (live, footprint, at, state_dir)
    })
    .collect();

    owner.kill();
    launch(&runtime, &successor, &run.derive_python_image);
    assert!(removed(&other, &footprint.id), "the launch removed it");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());

    let replacement = launcher::vmm_name(&other).expect("the launch named its network");
    assert_ne!(
        replacement, name,
        "a replacement never takes a dead launch's id"
    );
    let socket = format!("{}/{name}/sock/init.sock", root.state_dir);
    assert!(!launcher::path_exists(&socket), "nor its socket, {socket}");

    for (live, footprint, at, state_dir) in alive {
        launcher::assert_whole(&footprint);
        live.end();
        assert_eq!(
            launcher::left(state_dir, &at.tmp_dir, &footprint.name),
            Vec::<String>::new()
        );
    }
}

/// Removal of the dead owner's container fails, unknowingly, by a fault in
/// its query, so nothing it may use is touched; once the fault clears, the
/// next launch releases it all.
#[test]
fn an_unknown_container_keeps_everything() {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let successor = router(vmm_at(&other.podman, &root.state_dir, &run.fake_image));

    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.fake_image, false);
    owner.started();
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    let _rpc = hold_rpc(&runtime, &root.state_dir, &name);
    owner.kill();

    let fault = launcher::fail(&other, "ps", &launcher::token(&record));
    launch(&runtime, &successor, &run.derive_python_image);
    launcher::assert_whole(&footprint);
    assert!(!launcher::locked(&record), "let go for a later launch");
    std::mem::drop(fault);

    launch(&runtime, &successor, &run.derive_python_image);
    assert!(removed(&other, &footprint.id), "the launch removed it");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
}

/// Owners in PID namespaces of their own, each of them PID 1: a live owner
/// is spared by a launch sharing its PID, and a dead one is released by
/// another.
#[test]
fn owners_alike_by_pid() {
    let (run, root, runtime) = setup();
    let (neighbour, successor) = (launcher::root(), launcher::root());

    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.fake_image, true);
    owner.started();
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);

    let live = Owner::spawn(&run, &neighbour, &root.state_dir, &run.fake_image, true);
    live.started();
    assert_eq!((owner.pid, live.pid), (1, 1));
    launcher::assert_whole(&footprint);
    let live_name = launcher::vmm_name(&neighbour).expect("the launch named its network");
    let live_footprint = launcher::footprint(&root.state_dir, &neighbour.tmp_dir, &live_name);
    let _rpc = hold_rpc(&runtime, &root.state_dir, &name);

    owner.kill();
    let next = Owner::spawn(&run, &successor, &root.state_dir, &run.fake_image, true);
    next.started();
    assert_eq!(next.pid, 1);
    assert!(removed(&successor, &footprint.id), "the launch removed it");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
    launcher::assert_whole(&live_footprint);

    live.end();
    next.end();
}

/// Records which prove nothing are never acted on: a well-formed record
/// whose name an unlabeled container and network share, and which marks no
/// directory made; a record of another version, one naming a foreign mount,
/// one claiming another id, and a torn one, each marking directories made; a
/// live owner's; and a state directory with no record at all, as an earlier
/// launcher left them.
#[test]
fn records_which_prove_nothing() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.fake_image));
    // SAFETY: geteuid takes nothing and cannot fail.
    let mounts = format!("{}/connector-mounts-{}", root.tmp_dir, unsafe {
        libc::geteuid()
    });
    let new_name = || format!("fv_{}", run::random_hex());
    let token = format!("{}{}", run::random_hex(), run::random_hex());
    let made = "{\"created\":\"mount\"}\n{\"created\":\"state\"}\n";

    let mut untouchable = Vec::new();
    let mut plant = |name: &str, record: Option<String>, mount: &str| {
        for dir in [mount.to_string(), format!("{}/{name}", root.state_dir)] {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(format!("{dir}/sentinel"), "").unwrap();
            untouchable.push(format!("{dir}/sentinel"));
        }
        if let Some(content) = record {
            let path = launcher::record(&root.state_dir, name);
            std::fs::write(&path, content).unwrap();
            path
        } else {
            String::new()
        }
    };
    let claim = |name: &str, version: u32, mount: &str| {
        format!(
            "{}\n",
            serde_json::json!({"version": version, "id": name, "token": token, "mount": mount})
        )
    };

    let named = new_name();
    let named_mount = format!("{mounts}/mount-{named}");
    let named_record = plant(&named, Some(claim(&named, 1, &named_mount)), &named_mount);
    run::record("network", &named);
    run::podman(&["network", "create", &named]);
    run::record("container", &named);
    run::podman(&[
        "create",
        &format!("--name={named}"),
        &format!("--network={named}"),
        "--entrypoint=sleep",
        &run.vmm_image,
        "infinity",
    ]);

    let mut kept = Vec::new();
    for (version, mount, claims) in [
        (2, None, None),
        (1, Some(format!("{}/foreign", root.dir)), None),
        (1, None, Some(new_name())),
    ] {
        let name = new_name();
        let mount = mount.map_or(format!("{mounts}/mount-{name}"), |dir| {
            format!("{dir}/mount-{name}")
        });
        let id = claims.unwrap_or(name.clone());
        kept.push(plant(
            &name,
            Some(claim(&id, version, &mount) + made),
            &mount,
        ));
    }
    let torn = new_name();
    let torn_mount = format!("{mounts}/mount-{torn}");
    let torn_claim = claim(&torn, 1, &torn_mount);
    kept.push(plant(
        &torn,
        Some(torn_claim.trim_end().to_string()),
        &torn_mount,
    ));

    let held = new_name();
    let held_mount = format!("{mounts}/mount-{held}");
    let held_record = plant(
        &held,
        Some(claim(&held, 1, &held_mount) + made),
        &held_mount,
    );
    let lock = std::fs::File::open(&held_record).unwrap();
    lock.lock().unwrap();
    kept.push(held_record);

    let unrecorded = new_name();
    plant(&unrecorded, None, &format!("{mounts}/mount-{unrecorded}"));

    launch(&runtime, &router, &run.derive_python_image);

    assert!(launcher::container_exists(&named) && launcher::network_exists(&named));
    assert!(
        !launcher::path_exists(&named_record),
        "the record owned nothing, so nothing is lost with it"
    );
    for path in kept.iter().chain(&untouchable) {
        assert!(launcher::path_exists(path), "{path} remains");
    }
    let launched = launcher::vmm_name(&root).expect("the launch named its network");
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, &launched),
        Vec::<String>::new()
    );

    run::podman(&["rm", "--force", "--time=0", &named]);
    run::podman(&["network", "rm", &named]);
}
