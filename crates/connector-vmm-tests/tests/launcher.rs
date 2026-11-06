//! The connector service's VMM launches, through the actual launcher. Each
//! test drives an in-process `connector::Service` whose VMM configuration
//! names this run's images, and a state directory, TMPDIR and podman of the
//! test's own (`connector_vmm_tests::launcher`). Recovery tests also launch,
//! or recover, in other processes, which they kill as a crash would: this
//! same binary, running only `owner_process` (see `Owner`). Like the rest of
//! the suite, these run only under `mise run ci:connector-vmm-kvm`.

mod replacement;
#[path = "common/rpc.rs"]
mod rpc;

use connector_vmm_tests::{dns, endpoint, launcher, netns, run};
use proto_flow::{capture, connector as proto, derive, flow, materialize, ops};
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
        memory: Ok(connector::VmmMemory {
            bytes: 1280 << 20,
            guest_mib: 1024,
        }),
        cpu_limit: "2".to_string(),
        vcpus: 2,
        cgroup_parent: None,
        allow_unpaired_build: !paired_build(),
    }
}

/// The task builds this binary and the run's VMM images from one tree, so
/// they pair unless that tree is modified: then every launch would be
/// refused, and only `build_pairing` enforces pairing.
fn paired_build() -> bool {
    // A commit alone: neither `-dirty`, nor empty.
    connector::BUILD_IDENTITY.is_some_and(|identity| identity.len() == 40)
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

/// Every VMM task declares egress. Unless a test says otherwise it declares
/// none at all, which permits no external destination.
fn vmm_execution() -> flow::ConnectorExecution {
    flow::ConnectorExecution {
        vmm: true,
        egress: Some(Default::default()),
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

/// One recovery pass beneath `vmm`'s state directory, as a reactor's
/// `connector::VmmRecovery` runs them. Returns how many problems it left.
fn recover(runtime: &tokio::runtime::Runtime, vmm: &connector::Vmm) -> u64 {
    runtime.block_on(vmm.recover())
}

/// Bounds a `connector::VmmRecovery`'s first pass, or one after its pause.
const PASSED: Duration = Duration::from_secs(90);

/// Open a session without waiting on it, as a client which may walk away.
fn open(
    runtime: &tokio::runtime::Runtime,
    router: &connector::ServiceRouter,
    request: proto::Request,
) -> (
    tokio::sync::mpsc::Sender<proto::Request>,
    tokio::sync::mpsc::Receiver<tonic::Result<proto::Response>>,
) {
    open_as(
        runtime,
        router,
        ops::TaskType::Derivation,
        proto_grpc::connector::SPEC_TASK_NAME,
        request,
    )
}

/// As `open`, as the task `task_name` of `task_type`.
fn open_as(
    runtime: &tokio::runtime::Runtime,
    router: &connector::ServiceRouter,
    task_type: ops::TaskType,
    task_name: &str,
    request: proto::Request,
) -> (
    tokio::sync::mpsc::Sender<proto::Request>,
    tokio::sync::mpsc::Receiver<tonic::Result<proto::Response>>,
) {
    use proto_grpc::connector::Router;

    let _entered = runtime.enter();
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
    request_tx.try_send(request).expect("the channel is empty");
    let response_rx = router.open(task_type, task_name, request_rx);
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
    assert!(container.advertised_ports.is_empty(), "no ports");
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

    // Launches run each VMM image by the ID they resolve its name to.
    let id = |image: &str| run::podman(&["image", "inspect", "--format", "{{.Id}}", image]);
    let mut calls = calls
        .replace(id(&run.fake_image).trim(), "<fake image id>")
        .replace(id(&run.vmm_image).trim(), "<vmm image id>")
        .replace(&run.fake_image, "<fake image>")
        .replace(&run.vmm_image, "<vmm image>");
    calls = regex::Regex::new(r"(--opt=image=)(?:sha256:)?[0-9a-f]{64}")
        .unwrap()
        .replace_all(&calls, "${1}<connector image id>")
        .into_owned();
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
    /// A path whose creation starts the launch, once the owner reports
    /// `waiting`; it then reports `go` with the time.
    #[serde(default)]
    barrier: Option<String>,
    /// The task's declared egress hosts, which `Started` must echo.
    #[serde(default)]
    egress: Option<Vec<String>>,
    /// Run one recovery pass, and report its problems, instead of a launch.
    #[serde(default)]
    recover: bool,
}

/// Not a test of its own: the launch of an `Owner`, which runs this binary
/// with only this test selected and `OWNER` set. It reports on stdout, holds
/// its session until its stdin closes, then ends it and reports its release.
/// A recovering owner instead reports its pass's problems, and exits.
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
        barrier,
        egress,
        recover: recovering,
    } = serde_json::from_str(config.to_str().expect("UTF-8")).expect("an owner's configuration");
    let execution = flow::ConnectorExecution {
        vmm: true,
        egress: Some(flow::connector_execution::Egress {
            hosts: egress.unwrap_or_default(),
            cidrs: Vec::new(),
        }),
    };

    // The host's /proc names this process by its PID on the host, whatever
    // its own namespace calls it.
    let host_pid = std::fs::read_link("/proc/self").expect("reading /proc/self");
    println!("owner: pid {} {}", std::process::id(), host_pid.display());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    let vmm = vmm_at(&podman, &state_dir, &vmm_image);
    let logger = |log: &ops::Log| {
        let fields: String = log
            .fields_json_map
            .iter()
            .map(|(key, value)| format!(" {key}={}", String::from_utf8_lossy(value)))
            .collect();
        eprintln!(
            "owner {}: {}: {}{fields}",
            host_pid.display(),
            log.level().as_str_name(),
            log.message
        )
    };

    if let Some(barrier) = barrier {
        println!("owner: waiting");
        run::eventually("the barrier to open", STARTED, || {
            launcher::path_exists(&barrier).then_some(())
        });
        println!("owner: go {}", epoch_ms());
    }
    if recovering {
        println!("owner: recovered {}", recover(&runtime, &vmm));
        return;
    }

    let router = router(vmm);
    let (request_tx, mut response_rx, started) =
        match runtime.block_on(proto_grpc::connector::start(
            &router,
            &logger,
            proto_grpc::connector::SPEC_TASK_NAME,
            spec_with(&image, execution.clone()),
        )) {
            Ok(session) => session,
            Err(err) => {
                println!("owner: failed {err:#}");
                return;
            }
        };
    assert_eq!(
        started.execution,
        Some(execution),
        "Started echoes the task's egress"
    );
    assert_started(&proto::response::Started {
        execution: Some(vmm_execution()),
        ..started
    });
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
        Self::spawn_at(run, root, state_dir, vmm_image, namespaced, None, None)
    }

    /// As `spawn`, launching only once `barrier` exists, if given, as a task
    /// declaring `egress` hosts, if given.
    fn spawn_at(
        run: &run::Run,
        root: &launcher::Root,
        state_dir: &str,
        vmm_image: &str,
        namespaced: bool,
        barrier: Option<&str>,
        egress: Option<&[&str]>,
    ) -> Self {
        let config = OwnerConfig {
            podman: root.podman.clone(),
            state_dir: state_dir.to_string(),
            vmm_image: vmm_image.to_string(),
            image: run.derive_python_image.clone(),
            barrier: barrier.map(ToString::to_string),
            egress: egress.map(|hosts| hosts.iter().map(ToString::to_string).collect()),
            recover: false,
        };
        Self::spawn_config(root, &config, namespaced)
    }

    /// One recovery pass beneath `state_dir`, with `root`'s podman and the
    /// run's VMM image, once `barrier` exists, if given.
    fn recovering(
        run: &run::Run,
        root: &launcher::Root,
        state_dir: &str,
        barrier: Option<&str>,
    ) -> Self {
        let config = OwnerConfig {
            podman: root.podman.clone(),
            state_dir: state_dir.to_string(),
            vmm_image: run.vmm_image.clone(),
            image: run.derive_python_image.clone(),
            barrier: barrier.map(ToString::to_string),
            egress: None,
            recover: true,
        };
        Self::spawn_config(root, &config, false)
    }

    fn spawn_config(root: &launcher::Root, config: &OwnerConfig, namespaced: bool) -> Self {
        let config = serde_json::to_string(config).unwrap();
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

    fn kill(mut self) -> std::process::ExitStatus {
        // SAFETY: kill takes plain integers.
        let killed = unsafe { libc::kill(self.host_pid, libc::SIGKILL) };
        assert_eq!(killed, 0, "killing owner {}", self.host_pid);
        self.child.wait().expect("waiting for a killed owner")
    }

    /// End the owner's session, as a client which is done with it would.
    fn end(mut self) {
        std::mem::drop(self.stdin.take());
        self.report("released", RELEASED);
        self.child.wait().expect("waiting for an owner");
    }
}

/// Release owners after test panics so they cannot retain records or stderr.
/// Kill owners that do not exit within `RELEASED`.
impl Drop for Owner {
    fn drop(&mut self) {
        // This may run while unwinding, where a panic would abort.
        std::mem::drop(self.stdin.take());
        let deadline = std::time::Instant::now() + RELEASED;
        while matches!(self.child.try_wait(), Ok(None)) {
            if std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
            // Unknown until the owner reports it, and kill(0) would signal
            // this process's whole group.
            if self.host_pid > 0 {
                // SAFETY: kill takes plain integers.
                unsafe { libc::kill(self.host_pid, libc::SIGKILL) };
            } else {
                _ = self.child.kill();
            }
            _ = self.child.wait();
            return;
        }
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

/// Retagging after inspection must not replace the rootfs whose metadata
/// the launcher handed to connector-init.
#[test]
fn mounts_the_inspected_image_after_a_tag_changes() {
    let (run, root, runtime) = setup();
    let image = format!("ghcr.io/estuary/derive-python:vmm-{}", run::random_hex());
    run::record("image", &image);
    run::podman(&["tag", &run.derive_python_image, &image]);
    std::fs::write(format!("{}/local-image", root.dir), &image).unwrap();
    let inspected_id = run::podman(&["image", "inspect", "--format", "{{.Id}}", &image])
        .trim()
        .to_string();

    let router = router(vmm(&root, &run.vmm_image));
    let hold = launcher::hold(&root, "volume-create");
    let logs = Logs::default();
    let task = {
        let logs = logs.clone();
        let request = spec(&image);
        runtime.spawn(async move {
            let logger = move |log: &ops::Log| logs.push(log);
            proto_grpc::connector::unary(&router, &logger, request, STARTED, STARTED)
                .await
                .map(|(started, _response)| started)
        })
    };
    launcher::wait_held(&root, "volume-create", STARTED);

    // The replacement is a capture image which cannot serve the inspected
    // Python derivation's entrypoint or protocol.
    run::podman(&["tag", &run.hello_world_image, &image]);
    let replaced_id = run::podman(&["image", "inspect", "--format", "{{.Id}}", &image]);
    assert_ne!(replaced_id.trim(), inspected_id);
    std::mem::drop(hold);

    let started = runtime
        .block_on(task)
        .expect("the launch task completes")
        .unwrap_or_else(|err| {
            panic!(
                "launching the inspected image: {err:#}\n{:#?}",
                logs.messages()
            )
        });
    assert_started(&started);
    let name = launcher::vmm_name(&root).expect("the launch created a network");
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
    let calls = launcher::calls(&root);
    let rootfs = calls
        .iter()
        .find(|call| call.starts_with("volume create "))
        .unwrap();
    assert!(rootfs.contains(&format!("--opt=image={inspected_id}")));
    let create = calls
        .iter()
        .find(|call| call.starts_with("create "))
        .unwrap();
    assert!(create.contains(&format!("--label=image={image}")));
    run::podman(&["image", "rm", &image]);
}

/// Podman 4.9 unmounts an image volume even when its container never
/// started. A sibling's cleanup must not invalidate a VMM's prepared rootfs
/// between spec generation and the OCI runtime mounting it.
#[test]
fn an_unstarted_sibling_cannot_unmount_a_launching_vmm_rootfs() {
    let (run, root, runtime) = setup();
    let hold = launcher::hold(&root, "precreate");
    let hooks = format!("{}/hooks", root.dir);
    std::fs::create_dir(&hooks).unwrap();
    let hook = serde_json::json!({
        "version": "1.0.0",
        "hook": {
            "path": "/usr/bin/python3",
            "args": ["python3", "-c", r#"
import pathlib, sys, time
spec = sys.stdin.read()
pathlib.Path(sys.argv[1]).touch()
while pathlib.Path(sys.argv[2]).exists():
    time.sleep(0.05)
sys.stdout.write(spec)
"#, format!("{}/held-precreate", root.dir), format!("{}/hold-precreate", root.dir)],
            "timeout": 60
        },
        "when": {"always": true},
        "stages": ["precreate"]
    });
    std::fs::write(
        format!("{hooks}/gate.json"),
        serde_json::to_vec(&hook).unwrap(),
    )
    .unwrap();

    let router = router(vmm(&root, &run.vmm_image));
    let logs = Logs::default();
    let task = {
        let logs = logs.clone();
        let request = spec(&run.derive_python_image);
        runtime.spawn(async move {
            let logger = move |log: &ops::Log| logs.push(log);
            proto_grpc::connector::unary(&router, &logger, request, STARTED, STARTED)
                .await
                .map(|(started, _)| started)
        })
    };
    run::eventually("the OCI precreate hook", STARTED, || {
        assert!(
            !task.is_finished(),
            "launch ended before its hook: {:#?}",
            logs.messages()
        );
        launcher::path_exists(&format!("{}/held-precreate", root.dir)).then_some(())
    });

    let sibling = format!("fv_{}", run::random_hex());
    run::record("container", &sibling);
    run::podman(&[
        "create",
        &format!("--name={sibling}"),
        "--network=none",
        &format!(
            "--mount=type=image,source={},destination=/rootfs,rw=false",
            run.derive_python_image
        ),
        &run.vmm_image,
    ]);
    run::podman(&["rm", &sibling]);
    std::mem::drop(hold);

    let started = runtime.block_on(task).unwrap().unwrap_or_else(|err| {
        panic!(
            "starting beside sibling cleanup: {err:#}\n{:#?}",
            logs.messages()
        )
    });
    assert_started(&started);
    let name = launcher::vmm_name(&root).unwrap();
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
}

/// The socket's parent admits only the launcher, even with traversable ancestors.
#[test]
fn rpc_socket_rejects_other_users() {
    let (run, root, runtime) = setup();
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    owner.started();
    let name = launcher::vmm_name(&root).expect("the launch created a network");
    let directory = format!("{}/{name}/sock", root.state_dir);
    // Expose the test's ancestor so this still measures the socket directory.
    run::sudo(&["chmod", "0711", &root.state_dir]);

    // Check the live socket from another UID, without inherited group access.
    // Stat the directory after dropping privileges so an inaccessible ancestor
    // cannot make this test pass without the socket directory protecting it.
    let result = run::sudo(&[
        "python3",
        "-c",
        r#"
import os, socket, stat, sys
path = sys.argv[1]
uid = os.stat(path).st_uid
assert stat.S_IMODE(os.stat(path + "/init.sock").st_mode) == 0o777
os.setgroups([])
os.setgid(65534)
os.setuid(65534 if uid != 65534 else 65533)
os.stat(path)
with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
    client.setblocking(False)
    try:
        client.connect(path + "/init.sock")
    except PermissionError:
        print("permission denied")
    else:
        raise AssertionError("another UID connected to connector-init")
"#,
        &directory,
    ]);
    assert_eq!(result.trim(), "permission denied");
    run::sudo(&["chmod", "0700", &root.state_dir]);
    eprintln!("{}", served(&runtime, &root.state_dir, &name));

    owner.end();
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
}

/// Exact source paths remain unreadable to another host UID, even when the
/// launch id and record location are known. The VMM still serves its connector.
#[test]
fn credential_mount_rejects_other_users() {
    let (run, root, runtime) = setup();
    // Keep ancestors traversable so denial must come from the production
    // mount parent and state directory, independent of the suite's umask.
    run::sudo(&["chmod", "0755", &root.tmp_dir]);
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    owner.started();
    let name = launcher::vmm_name(&root).expect("the launch created a network");
    let record_path = launcher::record(&root.state_dir, &name);
    let record = std::fs::read_to_string(&record_path).unwrap();
    let claim: serde_json::Value = serde_json::from_str(record.lines().next().unwrap()).unwrap();
    let mount = claim["mount"].as_str().unwrap();
    let credential = format!("{mount}/task-update.json");
    // A Spec has no task credential; use an inert marker to test its filesystem boundary.
    std::fs::write(&credential, "synthetic-task-update").unwrap();
    std::fs::set_permissions(
        &credential,
        std::os::unix::fs::PermissionsExt::from_mode(0o444),
    )
    .unwrap();

    let result = run::sudo(&[
        "python3",
        "-c",
        r#"
import os, stat, sys
credential, record, state = sys.argv[1:]
parent = os.path.dirname(os.path.dirname(credential))
uid = os.stat(parent).st_uid
assert stat.S_IMODE(os.stat(parent).st_mode) == 0o700
assert stat.S_IMODE(os.stat(record).st_mode) == 0o600
assert stat.S_IMODE(os.stat(state).st_mode) == 0o700
assert open(credential).read() == "synthetic-task-update"
os.setgroups([])
os.setgid(65534)
os.setuid(65534 if uid != 65534 else 65533)
for path in [credential, record]:
    try:
        open(path).read()
    except PermissionError:
        print("permission denied")
    else:
        raise AssertionError("another UID read " + path)
"#,
        &credential,
        &record_path,
        &root.state_dir,
    ]);
    assert_eq!(result.trim(), "permission denied\npermission denied");
    eprintln!("{}", served(&runtime, &root.state_dir, &name));
    owner.end();
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
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
            // The VMM image's ID, which the run's own image needs no pull for.
            "image inspect",
            "run --rm",
            "network create",
            "volume create",
            "create --rm",
            "start --attach",
            "rm --force",
            "ps --all",
            "network ls",
            "network rm",
            "volume ls",
            "volume rm",
        ]
    );
    assert_eq!(
        std::fs::read_to_string(format!("{}/clean-environment", root.dir)).unwrap(),
        std::fs::read_to_string(format!("{}/calls", root.dir)).unwrap(),
        "every engine call was checked before sudo"
    );
}

/// A Spec of `image` as the Python connector of `task_type`, whose project
/// is empty. A Python connector answers Spec from its task's declared spec,
/// without staging or running the project.
fn python_spec(task_type: ops::TaskType, image: &str) -> proto::Request {
    let config_json = serde_json::json!({"image": image, "config": {"_python": {}}})
        .to_string()
        .into();
    let kind = match task_type {
        ops::TaskType::Capture => proto::request::Kind::Capture(capture::Request {
            kind: Some(capture::request::Kind::Spec(capture::request::Spec {
                connector_type: flow::capture_spec::ConnectorType::Image as i32,
                config_json,
            })),
            ..Default::default()
        }),
        ops::TaskType::Derivation => proto::request::Kind::Derive(derive::Request {
            kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Image as i32,
                config_json,
            })),
            ..Default::default()
        }),
        ops::TaskType::Materialization => proto::request::Kind::Materialize(materialize::Request {
            kind: Some(materialize::request::Kind::Spec(
                materialize::request::Spec {
                    connector_type: flow::materialization_spec::ConnectorType::Image as i32,
                    config_json,
                },
            )),
            ..Default::default()
        }),
        task_type => panic!("no Python connector of {task_type:?}"),
    };
    proto::Request {
        start: Some(proto::request::Start {
            execution: Some(vmm_execution()),
            log_level: ops::log::Level::Info as i32,
            ..Default::default()
        }),
        kind: Some(kind),
    }
}

/// Each Python connector built from this checkout runs in a VMM, as its
/// image's unprivileged user, and answers Spec with its egress denied: Spec
/// runs no project code and installs nothing. Each launch is released.
#[test]
fn python_connectors_spec_through_the_vmm() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.vmm_image));

    for (task_type, image) in [
        (ops::TaskType::Capture, &run.capture_python_image),
        (ops::TaskType::Derivation, &run.local_derive_python_image),
        (
            ops::TaskType::Materialization,
            &run.materialize_python_image,
        ),
    ] {
        let user = run::podman(&["image", "inspect", "--format", "{{.Config.User}}", image]);
        assert_eq!(user.trim(), "nobody", "{image}'s user");

        let logs = Logs::default();
        let started = unary(&runtime, &router, &logs, python_spec(task_type, image))
            .unwrap_or_else(|err| panic!("Spec of {image}: {err:#}\n{:#?}", logs.messages()));
        assert_eq!(started.execution, Some(vmm_execution()), "{image}");
        let schema = match started.spec {
            Some(proto::response::started::Spec::Capture(spec)) => spec.config_schema_json,
            Some(proto::response::started::Spec::Derive(spec)) => spec.config_schema_json,
            Some(proto::response::started::Spec::Materialize(spec)) => spec.config_schema_json,
            None => panic!("{image} Started without a Spec"),
        };
        assert!(!schema.is_empty(), "{image} has a config schema");
        assert!(
            logs.messages().iter().any(|logged| logged.ends_with(
                "VMM egress permits no external destination: the task declares empty egress"
            )),
            "{image}: {:#?}",
            logs.messages()
        );
    }
    assert_eq!(launcher::leftovers(&root, None), Vec::<String>::new());
}

/// Listens on 8080 and 9090 in the guest, answering `ping` with `pong`.
const LISTENER: &str = r#"
import socket, threading
def serve(port):
    server = socket.create_server(("0.0.0.0", port))
    while True:
        conn, _ = server.accept()
        with conn, conn.makefile("rb") as reader:
            if reader.readline(16) == b"ping\n":
                conn.sendall(b"pong\n")
for port in (8080, 9090):
    threading.Thread(target=serve, args=(port,)).start()
"#;

/// What a host client reads from `ip:port` after sending `ping`.
fn ping(ip: std::net::Ipv4Addr, port: u16) -> std::io::Result<String> {
    use std::io::{BufRead, Write};

    let mut stream =
        std::net::TcpStream::connect_timeout(&(ip, port).into(), Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.write_all(b"ping\n")?;
    let mut line = String::new();
    std::io::BufReader::new(stream).read_line(&mut line)?;
    Ok(line)
}

/// Ingress is the built spec's ports, whatever the image exposes. A session
/// whose built spec declares 8080, of an image which exposes only 9090, is
/// reachable from the host at 8080 and not at 9090, though its guest listens
/// on both. Its grant goes with the session.
#[test]
fn built_spec_ports_are_the_only_ingress() {
    use base64::Engine;
    const TASK: &str = "acmeCo/vmm-ingress/derivation";

    let (run, root, runtime) = setup();
    // An eligible repository, under a tag of this test's own, which no
    // launch pulls.
    let image = format!("ghcr.io/estuary/derive-python:vmm-{}", run::random_hex());
    let context = format!("{}/exposing", root.dir);
    std::fs::create_dir(&context).unwrap();
    let containerfile = format!("{context}/Containerfile");
    std::fs::write(
        &containerfile,
        format!("FROM {}\nEXPOSE 9090\n", run.derive_python_image),
    )
    .unwrap();
    run::record("image", &image);
    run::podman(&["build", "-q", "-t", &image, "-f", &containerfile, &context]);
    std::fs::write(format!("{}/local-image", root.dir), &image).unwrap();

    let encoded = base64::engine::general_purpose::STANDARD.encode(LISTENER);
    launcher::gate_readiness_after(
        &root,
        &format!(
            "({} -c \"import base64; exec(base64.b64decode('{encoded}'))\" &)",
            run::PROBES[0]
        ),
    );
    let router = router(vmm(&root, &run.vmm_image));
    let request = proto::Request {
        start: Some(proto::request::Start {
            execution: Some(vmm_execution()),
            log_level: ops::log::Level::Info as i32,
            ..Default::default()
        }),
        kind: Some(proto::request::Kind::Derive(derive::Request {
            kind: Some(derive::request::Kind::Open(Box::new(
                derive::request::Open {
                    collection: Some(flow::CollectionSpec {
                        name: TASK.to_string(),
                        derivation: Some(Box::new(flow::collection_spec::Derivation {
                            connector_type: flow::collection_spec::derivation::ConnectorType::Image
                                as i32,
                            config_json: serde_json::json!({"image": image, "config": {}})
                                .to_string()
                                .into(),
                            network_ports: vec![flow::NetworkPort {
                                number: 8080,
                                protocol: String::new(),
                                public: false,
                            }],
                            execution: Some(vmm_execution()),
                            shard_template: serde_json::from_value(serde_json::json!({
                                "labels": {"labels": [
                                    {"name": "estuary.dev/build", "value": "1122334455667788"}
                                ]}
                            }))
                            .unwrap(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ))),
            ..Default::default()
        })),
    };
    let (request_tx, mut response_rx) =
        open_as(&runtime, &router, ops::TaskType::Derivation, TASK, request);
    held(&root, "readiness", &mut response_rx).unwrap_or_else(|err| panic!("{err}"));

    let name = launcher::vmm_name(&root).expect("the launch created a network");
    let ip: std::net::Ipv4Addr = run::podman(&[
        "inspect",
        "--format",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        &name,
    ])
    .trim()
    .parse()
    .expect("the VMM's container has an IPv4 address");

    // The guest's listener may still be starting. The launch's readiness
    // wait, which the hold outlasts, ends its VMM after 60 seconds.
    let pong = run::eventually(
        "the declared port to answer",
        Duration::from_secs(30),
        || ping(ip, 8080).ok().filter(|line| line == "pong\n"),
    );
    assert_eq!(pong, "pong\n");
    assert!(ping(ip, 9090).is_err(), "the image's own port admitted");

    let calls = launcher::calls(&root);
    let grant = calls
        .iter()
        .find(|call| call.contains("boundary grant"))
        .expect("a grant of the declared port");
    assert!(
        grant.contains("--port=8080") && !grant.contains("9090"),
        "{grant}"
    );
    let create = calls
        .iter()
        .find(|call| call.starts_with("create "))
        .expect("the VMM's container");
    assert!(
        create.contains("--ingress-port=8080") && !create.contains("--ingress-port=9090"),
        "{create}"
    );

    // Abandoned before readiness, as a client walking away.
    std::mem::drop((request_tx, response_rx));
    run::eventually("the session's release", RELEASED, || {
        launcher::leftovers(&root, Some(&name))
            .is_empty()
            .then_some(())
    });
    assert!(
        launcher::calls(&root)
            .iter()
            .any(|call| call.contains("boundary revoke --owner=")),
        "the grant was revoked"
    );
    assert!(ping(ip, 8080).is_err(), "the port outlived its session");
    run::podman(&["image", "rm", &image]);
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
        # Dropped SYNs must finish inside the launcher's readiness window.
        sock.settimeout(5)
        try:
            sock.connect(address)
            sock.settimeout(30)
            sock.sendall(b"ping\n")
            with sock.makefile("rb") as reply:
                result = "connected" if reply.readline(16) == b"pong\n" else "bad-reply"
        except OSError:
            result = "unreachable"
        sock.close()
    line = {"level": "info", "message": "egress probe", "fields": {"name": name, "result": result}}
    sys.stderr.write(json.dumps(line) + "\n")
    sys.stderr.flush()
"#;

/// The controlled destinations of one egress test: three public endpoints,
/// one standing in for the package index, and the decoy, each listening, and
/// a nameserver which answers for them.
struct Destinations {
    nameserver: dns::Nameserver,
    a: endpoint::Endpoint,
    b: endpoint::Endpoint,
    c: endpoint::Endpoint,
    pypi: endpoint::Endpoint,
    decoy: endpoint::Endpoint,
}

/// Every name is answered, so that a refusal is the VMM's rather than the
/// nameserver's. `leak.acmeco.example` answers only with the decoy, an
/// address in an excluded range.
fn destinations(run: &run::Run) -> Destinations {
    let [a, b, c, pypi, ..] = run.endpoints[..] else {
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
        // A connector default, which `{}` egress does not permit.
        ("pypi.org", pypi),
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
        pypi: listen(pypi),
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
/// VMM's test controls, for the nameserver and the probe.
fn probe_egress(
    (run, root, runtime): &(run::Run, launcher::Root, tokio::runtime::Runtime),
    plane: connector::Plane,
    egress: &[&str],
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
    let mut flags = vec![
        "--resolver-upstream".to_string(),
        destinations.nameserver.addr.to_string(),
    ];
    flags.extend(launcher::before_connector_init(&format!(
        "{} -c \"import base64; exec(base64.b64decode('{encoded}'))\"",
        run::PROBES[0]
    )));
    launcher::vmm_flags(root, &flags);
    let execution = flow::ConnectorExecution {
        vmm: true,
        egress: Some(flow::connector_execution::Egress {
            hosts: egress.iter().map(ToString::to_string).collect(),
            cidrs: Vec::new(),
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

/// A public data plane holds a task to the connector's hosts and those it
/// declares: an exact name and not its subdomains, every name beneath a
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
        &[
            "api.acmeco.example",
            "*.svc.acmeco.example",
            "leak.acmeco.example",
        ],
        &[
            case("pypi.org", &d.pypi),
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
            ("pypi.org".to_string(), "connected"),
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
    assert_eq!(endpoint::accepted(&d.pypi), [run.endpoint_peer]);
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

/// A VMM task which declares no egress is refused on every plane before
/// anything is pulled or launched.
#[test]
fn undeclared_egress_is_refused_before_a_launch() {
    let (run, root, runtime) = setup();
    for plane in [
        connector::Plane::Public,
        connector::Plane::Private,
        connector::Plane::Local,
    ] {
        let router = router_on(plane, vmm(&root, &run.vmm_image));
        let logs = Logs::default();
        let request = spec_with(
            &run.derive_python_image,
            flow::ConnectorExecution {
                vmm: true,
                egress: None,
            },
        );
        let err = unary(&runtime, &router, &logs, request).expect_err("refused");
        assert_eq!(
            status(&err),
            (
                tonic::Code::InvalidArgument,
                "the task requests VMM execution but declares no egress; a VMM task must \
                 declare `egress`, and `egress: {}` permits no external destination"
                    .to_string()
            ),
            "{plane:?}"
        );
    }
    assert_eq!(launcher::calls(&root), Vec::<String>::new());
    assert_eq!(launcher::leftovers(&root, None), Vec::<String>::new());
}

/// Egress declaring no host and no CIDR permits nothing on a private plane,
/// not even the connector's own package index: the resolver refuses every
/// name without asking upstream, and no address is reachable.
#[test]
fn empty_egress_on_a_private_plane() {
    let launch = setup();
    let (run, ..) = &launch;
    let d = destinations(run);
    let probed = probe_egress(
        &launch,
        connector::Plane::Private,
        &[],
        &[
            case("pypi.org", &d.pypi),
            case("other.acmeco.example", &d.a),
            direct(&d.c),
        ],
        &d,
    );

    assert_eq!(probed.policy, r#"{"allowedNames":[],"egress":"public"}"#);
    assert_results(
        &probed,
        &[
            ("pypi.org".to_string(), "unresolved"),
            ("other.acmeco.example".to_string(), "unresolved"),
            (direct(&d.c).0, "unreachable"),
        ],
    );
    for endpoint in [&d.pypi, &d.a, &d.c] {
        assert!(
            endpoint::accepted(endpoint).is_empty(),
            "{:?}",
            endpoint::accepted(endpoint)
        );
    }
    let asked = dns::queries(&d.nameserver);
    for name in ["pypi.org", "other.acmeco.example"] {
        assert!(
            !asked.iter().any(|asked| asked == name),
            "{name} went upstream: {asked:?}"
        );
    }
    assert_logged(
        &probed,
        "VMM egress permits no external destination: the task declares empty egress",
    );
    assert_logged(
        &probed,
        "refused DNS name pypi.org, which this connector's egress does not permit; a task \
         may permit it in egress.hosts",
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
        !calls.iter().any(|call| call.starts_with("create ")),
        "the VMM was created: {calls:#?}"
    );
    assert_eq!(calls[calls.len() - 2], format!("network rm {name}"));
    assert!(calls.last().unwrap().starts_with("volume ls "));
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
        err.contains(
            "the VMM exited (exit status: 125) before flow-connector-init answered its health check"
        ) && err.contains("podman.sh: failing start, as the test asks"),
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

/// A reactor stops a shard by destroying its runtime, and the launch's task
/// with it, before any teardown runs. No launch follows: the process's
/// recovery, started as a reactor's, releases it all.
#[test]
fn a_destroyed_runtime_is_recovered_without_a_launch() {
    let (run, root, runtime) = setup();
    let vmm = vmm(&root, &run.fake_image);
    let shard = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a shard's runtime");
    let router = router(vmm.clone());
    let logs = Logs::default();
    let logger = |log: &ops::Log| logs.push(log);

    let (request_tx, response_rx, started) = shard
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
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    let _rpc = hold_rpc(&runtime, &root.state_dir, &name);

    shard.shutdown_background();
    std::mem::drop((request_tx, response_rx));
    run::eventually("the launch's lock to go with its runtime", RELEASED, || {
        (!launcher::locked(&record)).then_some(())
    });
    launcher::assert_whole(&footprint);
    assert!(!removed(&root, &footprint.id), "no teardown ran");

    let recovery = vmm.spawn_recovery(runtime.handle());
    run::eventually("recovery to release it", PASSED, || {
        launcher::remaining(&footprint).is_empty().then_some(())
    });
    std::mem::drop(recovery);
    assert!(removed(&root, &footprint.id), "recovery removed it");
    assert_eq!(
        launcher::leftovers(&root, Some(&name)),
        Vec::<String>::new()
    );
    let pulls = launcher::calls(&root)
        .into_iter()
        .filter(|call| call.starts_with("pull "))
        .count();
    assert_eq!(pulls, 1, "only the destroyed launch's");
}

/// A removal podman refuses (a sidecar on the VMM's network) is reported, and
/// the rest is removed. The record is kept as it was and let go, and a
/// recovery pass releases what remains once it can.
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

    let recovering = vmm(&root, &run.fake_image);
    assert_eq!(recover(&runtime, &recovering), 1, "the network is in use");
    assert_eq!(std::fs::read(&record).unwrap(), written, "kept again");
    std::mem::drop(sidecar);
    assert_eq!(recover(&runtime, &recovering), 0);
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

    // The override skips pairing, never the verify.
    let router = router(connector::Vmm {
        allow_unpaired_build: true,
        ..vmm(&root, &image)
    });
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

/// A commit no checkout's HEAD has.
const OTHER_BUILD: &str = "0000000000000000000000000000000000000000";

/// The run's VMM image, but for its `boundary verify`'s report of its build:
/// `build` in place of its own, or nothing at all, as an image built before
/// pairing reports. Its verify, and everything else, are the run's VMM's.
fn reporting(run: &run::Run, root: &launcher::Root, build: Option<&str>) -> String {
    use std::os::unix::fs::PermissionsExt;

    let image = format!(
        "localhost/connector-vmm-kvm-reporting:{}",
        run::random_hex()
    );
    run::record("image", &image);
    let context = format!("{}/reporting-{}", root.dir, run::random_hex());
    std::fs::create_dir(&context).unwrap();
    let script = format!("{context}/report-build");
    std::fs::write(
        &script,
        r#"#!/bin/sh
if [ "$1 $2" != "boundary verify" ]; then exec /usr/local/bin/flow-connector-vmm "$@"; fi
verified=$(/usr/local/bin/flow-connector-vmm "$@")
status=$?
printf '%s\n' "$verified" | while IFS= read -r line; do
    case "$line" in
    "flow-connector-vmm build "*) [ -z "$REPORT" ] || printf '%s\n' "$REPORT" ;;
    *) printf '%s\n' "$line" ;;
    esac
done
exit $status
"#,
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let report = build.map_or(String::new(), |build| {
        format!("flow-connector-vmm build {build}")
    });
    let containerfile = format!(
        "FROM {}\nCOPY report-build /usr/local/bin/report-build\nENV REPORT=\"{report}\"\n\
         ENTRYPOINT [\"/usr/local/bin/report-build\"]\n",
        run.vmm_image
    );
    let built = run::sudo_output(
        &["podman", "build", "-q", "-t", &image, "-f", "-", &context],
        Some(containerfile.as_bytes()),
    );
    assert!(
        built.status.success(),
        "building {image}: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    image
}

/// Pairing, enforced: the run's own VMM image launches only if this binary
/// and it were built from an unmodified tree, and an image reporting another
/// commit's build, or none, is refused once its verify passes, before any
/// network, leaving nothing. The override launches the latter, with a
/// warning to the task.
#[test]
fn build_pairing() {
    let (run, root, runtime) = setup();
    let enforced = |image: &str| connector::Vmm {
        allow_unpaired_build: false,
        ..vmm(&root, image)
    };

    let logs = Logs::default();
    let outcome = unary(
        &runtime,
        &router(enforced(&run.vmm_image)),
        &logs,
        spec(&run.derive_python_image),
    );
    match (paired_build(), outcome) {
        (true, Ok(started)) => assert_started(&started),
        (false, Err(err)) => {
            let (code, message) = status(&err);
            assert_eq!(code, tonic::Code::FailedPrecondition, "{message}");
            assert!(
                message.contains("is not paired with this launcher's build: this launcher"),
                "{message}"
            );
        }
        (paired, outcome) => panic!(
            "this build {:?} is paired: {paired}, yet {:?}\n{:#?}",
            connector::BUILD_IDENTITY,
            outcome.map(|_| "started"),
            logs.messages()
        ),
    }
    let name = launcher::vmm_name(&root);
    assert_eq!(
        launcher::leftovers(&root, name.as_deref()),
        Vec::<String>::new()
    );

    let other = reporting(&run, &root, Some(OTHER_BUILD));
    let older = reporting(&run, &root, None);
    for image in [&other, &older] {
        let before = launcher::calls(&root).len();
        let logs = Logs::default();
        let err = unary(
            &runtime,
            &router(enforced(image)),
            &logs,
            spec(&run.derive_python_image),
        )
        .expect_err("an unpaired image");
        let (code, message) = status(&err);
        assert_eq!(code, tonic::Code::FailedPrecondition, "{message}");
        assert!(
            message.contains(&format!(
                "VMM image {image} is not paired with this launcher's build: "
            )),
            "{message}"
        );
        let calls = launcher::calls(&root)[before..].to_vec();
        assert!(
            calls.iter().any(|call| call.ends_with(" boundary verify")),
            "{calls:#?}"
        );
        assert!(
            !calls.iter().any(|call| call.starts_with("network create")),
            "a network was created: {calls:#?}"
        );
        assert_eq!(launcher::leftovers(&root, None), Vec::<String>::new());
    }

    let logs = Logs::default();
    let started = unary(
        &runtime,
        &router(connector::Vmm {
            allow_unpaired_build: true,
            ..vmm(&root, &other)
        }),
        &logs,
        spec(&run.derive_python_image),
    )
    .unwrap_or_else(|err| panic!("the override: {err:#}\n{:#?}", logs.messages()));
    assert_started(&started);
    let warned = format!("VMM image {other} is not paired with this launcher's build: ");
    assert!(
        logs.messages()
            .iter()
            .any(|message| message.contains(&warned)
                && message
                    .ends_with("; launching it as CONNECTOR_VMM_ALLOW_UNPAIRED_BUILD permits")),
        "{:#?}",
        logs.messages()
    );

    for image in [other, older] {
        run::podman(&["image", "rm", &image]);
    }
}

/// Recovery revokes a record's grant with the image the record names, though
/// pairing refuses a launch of it: recovery neither verifies nor pairs.
#[test]
fn recovery_revokes_with_an_unpaired_image() {
    let (run, root, runtime) = setup();
    let unpaired = reporting(&run, &root, Some(OTHER_BUILD));
    let name = format!("fv_{}", run::random_hex());
    let token = format!("{}{}", run::random_hex(), run::random_hex());

    // Granted from that image, as its launch's grant was.
    run::record("ingress", &format!("{unpaired} {token}"));
    run::podman(&[
        "run",
        "--rm",
        "--network=host",
        "--log-driver=none",
        "--read-only",
        "--cap-drop=all",
        "--cap-add=CAP_NET_ADMIN",
        &unpaired,
        "boundary",
        "grant",
        &format!("--owner={token}"),
        &format!("--bridge=fvm{}", &name[3..15]),
        "--port=8080",
    ]);
    let granted = || {
        let listed = run::sudo_output(
            &[
                "podman",
                "run",
                "--rm",
                "--network=host",
                "--cap-drop=all",
                "--cap-add=CAP_NET_ADMIN",
                "--entrypoint=nft",
                &run.vmm_image,
                "list",
                "table",
                "inet",
                &format!("flow_vmm_ingress_{token}"),
            ],
            None,
        );
        listed.status.success()
    };
    assert!(granted());
    let mounts = launcher::mounts(&root);
    std::fs::write(
        launcher::record(&root.state_dir, &name),
        format!(
            "{}\n",
            serde_json::json!({
                "version": 1,
                "id": name,
                "token": token,
                "mount": format!("{mounts}/mount-{name}"),
                "ingress_image": unpaired,
            })
        ),
    )
    .unwrap();

    let enforced = connector::Vmm {
        allow_unpaired_build: false,
        ..vmm(&root, &run.vmm_image)
    };
    assert_eq!(recover(&runtime, &enforced), 0);
    assert!(!granted(), "revoked");
    assert!(!launcher::path_exists(&launcher::record(
        &root.state_dir,
        &name
    )));
    let calls = launcher::calls(&root);
    assert!(
        calls
            .iter()
            .any(|call| call.ends_with(&format!("{unpaired} boundary revoke --owner={token}"))),
        "{calls:#?}"
    );
    assert!(
        !calls.iter().any(|call| call.contains("verify")),
        "{calls:#?}"
    );
    // The image stays for the task's release, whose recorded revoke runs it.
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
        message.contains(
            "the VMM exited (exit status: 2) before flow-connector-init answered its health check"
        ),
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

/// Hold a create past its owner's death, as a reactor's would be. A recovery
/// started afterwards, as a restarted reactor's with no shards, releases the
/// record at once. A later pass of its own, with no launch between, removes the
/// late network or volume by its state directory's label. A late container
/// create fails because recovery removed its network and volume.
fn an_orphaned_command(step: &str, kind: &str, exists: fn(&str) -> bool, makes: bool) {
    let (run, root, runtime) = setup();
    let other = launcher::root();

    let hold = launcher::hold(&root, step);
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.fake_image, false);
    launcher::wait_held(&root, step, STARTED);
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    owner.kill();

    let recovery =
        vmm_at(&other.podman, &root.state_dir, &run.fake_image).spawn_recovery(runtime.handle());
    let first = run::eventually("the first pass to end", PASSED, || {
        let metrics = connector::Vmm::recovery_metrics();
        (metrics.last_success != 0).then_some(metrics)
    });
    assert_eq!((first.failed_passes, first.pending), (0, 0));
    assert!(
        !launcher::path_exists(&record),
        "released by the first pass"
    );
    assert!(!exists(&name), "nothing was made yet");

    std::mem::drop(hold);
    run::eventually("the orphaned command to end", STARTED, || {
        wrappers(&root).is_empty().then_some(())
    });
    // The next pass starts 30 seconds after the first ended.
    assert_eq!(
        exists(&name),
        makes,
        "the {kind}, once the command ended after the first pass"
    );

    run::eventually("a later pass to remove what it made", PASSED, || {
        launcher::left(&root.state_dir, &root.tmp_dir, &name)
            .is_empty()
            .then_some(())
    });
    std::mem::drop(recovery);
    let calls = launcher::calls(&other);
    assert!(
        !calls.iter().any(|call| call.starts_with("pull ")),
        "with no launch: {calls:#?}"
    );
}

#[test]
fn a_kill_while_the_network_is_created() {
    an_orphaned_command("network-create", "network", launcher::network_exists, true);
}

#[test]
fn a_kill_while_the_container_is_created() {
    an_orphaned_command("create", "container", launcher::container_exists, false);
}

#[test]
fn a_kill_while_the_rootfs_volume_is_created() {
    an_orphaned_command(
        "volume-create",
        "rootfs volume",
        launcher::volume_exists,
        true,
    );
}

/// podman made the container, and its command ended, but its owner died
/// before learning the container's ID: it's found by its label.
#[test]
fn a_kill_before_the_container_id_is_read() {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let successor = vmm_at(&other.podman, &root.state_dir, &run.fake_image);

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
    assert_eq!(recover(&runtime, &successor), 0);
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
    let successor = vmm_at(&other.podman, &root.state_dir, &run.fake_image);

    launcher::gate_readiness(&root);
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    launcher::wait_held(&root, "readiness", STARTED);
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    // The pre-init gate keeps the guest alive without starting init's idle watchdog.
    owner.kill();

    assert_eq!(recover(&runtime, &successor), 0);
    assert!(removed(&other, &footprint.id), "recovery removed it");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
}

/// A VMM serving its session when its owner is killed is released by a
/// recovery pass, which leaves alone a live owner sharing its state directory
/// and one beneath another, as another stack's would be.
#[test]
fn a_kill_while_serving_spares_live_owners() {
    let (run, root, runtime) = setup();
    let (neighbour, stranger, other) = (launcher::root(), launcher::root(), launcher::root());
    let successor = vmm_at(&other.podman, &root.state_dir, &run.fake_image);

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
    assert_eq!(recover(&runtime, &successor), 0);
    assert!(removed(&other, &footprint.id), "recovery removed it");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());

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
/// its query, so nothing it may use is touched. A recovery retries it every
/// pass, each counted failed with the one problem pending, and once the
/// fault clears, a later pass of its own releases it all.
#[test]
fn an_unknown_container_keeps_everything() {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let successor = vmm_at(&other.podman, &root.state_dir, &run.fake_image);

    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.fake_image, false);
    owner.started();
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    let _rpc = hold_rpc(&runtime, &root.state_dir, &name);
    owner.kill();

    let fault = launcher::fail(&other, "ps", &launcher::token(&record));
    let recovery = successor.spawn_recovery(runtime.handle());
    let failing = run::eventually("two failed passes", PASSED * 2, || {
        let metrics = connector::Vmm::recovery_metrics();
        (metrics.failed_passes >= 2).then_some(metrics)
    });
    assert_eq!(
        (failing.started, failing.pending, failing.last_success),
        (true, 1, 0)
    );
    launcher::assert_whole(&footprint);
    // In the pause after the pass which just ended.
    assert!(!launcher::locked(&record), "let go for a later pass");
    std::mem::drop(fault);

    run::eventually("a later pass to release it", PASSED, || {
        launcher::remaining(&footprint).is_empty().then_some(())
    });
    let recovered = run::eventually("that pass to end", PASSED, || {
        let metrics = connector::Vmm::recovery_metrics();
        (metrics.last_success != 0).then_some(metrics)
    });
    std::mem::drop(recovery);
    assert_eq!(recovered.pending, 0);
    assert!(recovered.failed_passes >= 2);
    assert!(removed(&other, &footprint.id), "recovery removed it");
    assert!(
        !launcher::calls(&other)
            .iter()
            .any(|call| call.starts_with("pull ")),
        "with no launch"
    );
}

/// Owners in PID namespaces of their own, each of them PID 1: a live owner,
/// and another launched after a third died, are spared by a recovery pass
/// which releases the dead one.
#[test]
fn owners_alike_by_pid() {
    let (run, root, runtime) = setup();
    let (neighbour, successor, recoverer) = (launcher::root(), launcher::root(), launcher::root());

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
    let next_name = launcher::vmm_name(&successor).expect("the launch named its network");
    let next_footprint = launcher::footprint(&root.state_dir, &successor.tmp_dir, &next_name);

    let recovering = vmm_at(&recoverer.podman, &root.state_dir, &run.fake_image);
    assert_eq!(recover(&runtime, &recovering), 0);
    assert!(removed(&recoverer, &footprint.id), "recovery removed it");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
    launcher::assert_whole(&live_footprint);
    launcher::assert_whole(&next_footprint);

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
    let mounts = launcher::mounts(&root);
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

    assert_eq!(
        recover(&runtime, &vmm(&root, &run.fake_image)),
        4,
        "each malformed record is a problem; the held one is not"
    );

    assert!(launcher::container_exists(&named) && launcher::network_exists(&named));
    assert!(
        !launcher::path_exists(&named_record),
        "the record owned nothing, so nothing is lost with it"
    );
    for path in kept.iter().chain(&untouchable) {
        assert!(launcher::path_exists(path), "{path} remains");
    }

    run::podman(&["rm", "--force", "--time=0", &named]);
    run::podman(&["network", "rm", &named]);
}

#[test]
fn labeled_resources_without_a_record() {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let new_name = || format!("fv_{}", run::random_hex());
    let make = |name: &str, state_dir: &str| {
        let label = format!("--label=dev.estuary.vmm-state-dir={state_dir}");
        run::record("network", name);
        run::podman(&["network", "create", &label, name]);
        run::record("volume", name);
        run::podman(&["volume", "create", &label, name]);
    };
    let exists = |name: &str| {
        (
            launcher::network_exists(name),
            launcher::volume_exists(name),
        )
    };

    let unrecorded = new_name();
    make(&unrecorded, &root.state_dir);
    let held = new_name();
    make(&held, &root.state_dir);
    let record = launcher::record(&root.state_dir, &held);
    std::fs::write(&record, "").unwrap();
    let lock = std::fs::File::open(&record).unwrap();
    lock.lock().unwrap();
    let foreign = new_name();
    make(&foreign, &other.state_dir);

    assert_eq!(recover(&runtime, &vmm(&root, &run.fake_image)), 0);

    assert_eq!(exists(&unrecorded), (false, false), "its record is gone");
    assert_eq!(exists(&held), (true, true), "its record is held");
    assert_eq!(exists(&foreign), (true, true), "another state directory's");

    for name in [&held, &foreign] {
        run::podman(&["network", "rm", name]);
        run::podman(&["volume", "rm", name]);
    }
    std::mem::drop(lock);
    std::fs::remove_file(&record).unwrap();
}

/// Bounds reaching a gate, and must leave the held command well within the
/// release's 60-second command timeout once there.
const GATE: Duration = Duration::from_secs(60);
const ORPHAN: Duration = Duration::from_secs(30);
/// Bounds a Spec of a serving VMM, inside the gate's budget.
const SERVED: Duration = Duration::from_secs(20);

/// Where a process recovering a dead owner's VMM is killed.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Gate {
    /// Its removal of the container is held before podman runs it, and runs
    /// once the next pass has recovered the record.
    BeforeContainerRemoval,
    /// Its removal of the container has finished.
    AfterContainerRemoval,
    /// Its removal of the network is held before podman runs it, and runs
    /// once the next pass has recovered the record.
    BeforeNetworkRemoval,
    /// As before, but the orphaned removal runs before the next pass.
    BeforeNetworkRemovalRunFirst,
    /// Its removal of the network has finished. The mount, state and record
    /// remain: no engine call separates their removals, so no gate does.
    AfterNetworkRemoval,
}

/// Timestamped lines in `<run dir>/<name>.evidence`, which the task keeps,
/// written as observed so that a failure keeps what preceded it.
struct Evidence(std::path::PathBuf);

impl Evidence {
    fn note(&self, line: impl std::fmt::Display) {
        use std::io::Write;
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.0)
            .unwrap_or_else(|e| panic!("opening {}: {e}", self.0.display()));
        writeln!(file, "{}.{:03} {line}", at.as_secs(), at.subsec_millis())
            .unwrap_or_else(|e| panic!("writing {}: {e}", self.0.display()));
    }
}

/// What no record names, beside a recovery: a network of a VMM's shape
/// labeled with a token no record holds, an unlabeled container of the same
/// name on it, and a state directory and connector mount of an id without a
/// record.
#[derive(Debug)]
struct Sentinels {
    container: String,
    network: String,
    network_id: String,
    files: Vec<String>,
}

impl Sentinels {
    fn plant(run: &run::Run, root: &launcher::Root) -> Self {
        let name = format!("fv_{}", run::random_hex());
        let token = format!("{}{}", run::random_hex(), run::random_hex());
        run::record("network", &name);
        run::podman(&[
            "network",
            "create",
            &format!("--label=dev.estuary.vmm-owner={token}"),
            &name,
        ]);
        let network_id = network_id(&name);
        run::record("container", &name);
        let container = run::podman(&[
            "create",
            &format!("--name={name}"),
            &format!("--network={name}"),
            "--entrypoint=sleep",
            &run.vmm_image,
            "infinity",
        ])
        .trim()
        .to_string();

        let id = format!("fv_{}", run::random_hex());
        let mounts = launcher::mounts(root);
        let mut files = Vec::new();
        for dir in [
            format!("{}/{id}", root.state_dir),
            format!("{mounts}/mount-{id}"),
        ] {
            std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("creating {dir}: {e}"));
            let file = format!("{dir}/sentinel");
            std::fs::write(&file, "").unwrap_or_else(|e| panic!("creating {file}: {e}"));
            files.push(file);
        }
        Self {
            container,
            network: name,
            network_id,
            files,
        }
    }

    fn assert_intact(&self) {
        assert!(
            launcher::container_exists(&self.container),
            "sentinel container {} remains",
            self.container
        );
        assert!(
            launcher::network_exists(&self.network),
            "sentinel network {} remains",
            self.network
        );
        assert_eq!(network_id(&self.network), self.network_id);
        for file in &self.files {
            assert!(launcher::path_exists(file), "sentinel {file} remains");
        }
    }

    fn remove(self) {
        run::podman(&["rm", "--force", "--time=0", &self.container]);
        run::podman(&["network", "rm", &self.network]);
        for file in &self.files {
            let dir = std::path::Path::new(file).parent().expect("a directory");
            std::fs::remove_dir_all(dir)
                .unwrap_or_else(|e| panic!("removing {}: {e}", dir.display()));
            assert!(!launcher::path_exists(file));
        }
        assert!(!launcher::container_exists(&self.container));
        assert!(!launcher::network_exists(&self.network));
    }
}

fn network_id(name: &str) -> String {
    run::podman(&["network", "inspect", "--format", "{{.ID}}", name])
        .trim()
        .to_string()
}

/// A Spec of the derivation connector in launch `name`'s VMM, over its
/// init.sock from this process: what its connector-init and connector
/// answered, and how long that took.
fn served(runtime: &tokio::runtime::Runtime, state_dir: &str, name: &str) -> String {
    let socket = format!("{state_dir}/{name}/sock/init.sock");
    let began = std::time::Instant::now();
    let responses = runtime.block_on(async {
        tokio::time::timeout(SERVED, async {
            let mut client = proto_grpc::derive::connector_client::ConnectorClient::new(
                rpc::connect(socket.clone()).await,
            );
            let request = derive::Request {
                kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                    connector_type: flow::collection_spec::derivation::ConnectorType::Image as i32,
                    config_json: "{}".into(),
                })),
                ..Default::default()
            };
            let responses = client
                .derive(futures::stream::once(async { request }))
                .await
                .unwrap_or_else(|status| panic!("a Spec over {socket}: {status}"))
                .into_inner();
            futures::TryStreamExt::try_collect::<Vec<_>>(responses)
                .await
                .unwrap_or_else(|status| panic!("the Spec over {socket}: {status}"))
        })
        .await
        .unwrap_or_else(|_| panic!("no Spec over {socket} within {SERVED:?}"))
    });
    let Some(derive::response::Kind::Spec(spec)) =
        responses.first().and_then(|response| response.kind.clone())
    else {
        panic!("expected a Spec over {socket}, got {responses:?}");
    };
    assert_eq!(spec.protocol, 3032023, "the derive protocol");
    assert!(!spec.config_schema_json.is_empty(), "a config schema");
    format!(
        "{name} served Spec protocol {} ({} byte config schema) in {:?}",
        spec.protocol,
        spec.config_schema_json.len(),
        began.elapsed()
    )
}

/// A process's parent, by its `stat`.
fn parent(pid: u32) -> String {
    let path = format!("/proc/{pid}/stat");
    let stat = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("reading {path}: {e}"));
    stat.rsplit_once(')')
        .and_then(|(_, fields)| fields.split_whitespace().nth(1))
        .unwrap_or_else(|| panic!("parsing {path}: {stat:?}"))
        .to_string()
}

/// A process recovering a dead owner's serving VMM, as a reactor's recovery
/// would, is killed with SIGKILL at `gate`, beside a live owner sharing its
/// state directory and resources no record names. The next pass releases
/// everything the dead owner's record names, removing the record last, and
/// touches nothing else.
fn a_kill_of_recovery(gate: Gate) {
    use std::os::unix::process::ExitStatusExt;

    let (run, root, runtime) = setup();
    let (neighbour, recoverer, other) = (launcher::root(), launcher::root(), launcher::root());
    let successor = vmm_at(&other.podman, &root.state_dir, &run.fake_image);
    let evidence = Evidence(
        run.run_dir
            .join(format!("recovery-killed-{gate:?}.evidence")),
    );
    evidence.note(format_args!(
        "gate {gate:?}: dead owner {}, peer {}, recovering {}, successor {}",
        root.dir, neighbour.dir, recoverer.dir, other.dir
    ));

    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    owner.started();
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    let token = launcher::token(&record);
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    let _rpc = hold_rpc(&runtime, &root.state_dir, &name);

    let peer = Owner::spawn(&run, &neighbour, &root.state_dir, &run.vmm_image, false);
    peer.started();
    let peer_name = launcher::vmm_name(&neighbour).expect("the launch named its network");
    let peer_footprint = launcher::footprint(&root.state_dir, &neighbour.tmp_dir, &peer_name);
    // Beside its owner's session, so that no lull ends the VMM between checks.
    let _peer_rpc = hold_rpc(&runtime, &root.state_dir, &peer_name);
    evidence.note(format_args!(
        "peer before any kill: {}",
        served(&runtime, &root.state_dir, &peer_name)
    ));
    let sentinels = Sentinels::plant(&run, &root);
    evidence.note(format_args!("dead owner's footprint {footprint:?}"));
    evidence.note(format_args!("peer's footprint {peer_footprint:?}"));
    evidence.note(format_args!("sentinels {sentinels:?}"));

    let killed = owner.kill();
    assert_eq!(killed.signal(), Some(libc::SIGKILL));
    launcher::assert_whole(&footprint);
    assert!(!launcher::locked(&record), "the owner's lock went with it");
    evidence.note(format_args!(
        "owner {name} {killed}; its record unlocked: {:?}",
        std::fs::read_to_string(&record).expect("reading the record")
    ));

    let (hold, what) = match gate {
        Gate::BeforeContainerRemoval => (launcher::hold(&recoverer, "rm"), "rm"),
        Gate::AfterContainerRemoval => (launcher::hold_after(&recoverer, "rm", 1), "after-rm"),
        Gate::BeforeNetworkRemoval | Gate::BeforeNetworkRemovalRunFirst => {
            (launcher::hold(&recoverer, "network-rm"), "network-rm")
        }
        Gate::AfterNetworkRemoval => (
            launcher::hold_after(&recoverer, "network-rm", 1),
            "after-network-rm",
        ),
    };
    let recovering = Owner::recovering(&run, &recoverer, &root.state_dir, None);
    launcher::wait_held(&recoverer, what, GATE);
    let gated = std::time::Instant::now();
    let held = launcher::held_by(&recoverer, what);
    let wrapper = held[0];
    let wrapper_started = launcher::start_time(wrapper).expect("the held podman runs");
    let cmdline = std::fs::read(format!("/proc/{wrapper}/cmdline"))
        .map(|cmdline| String::from_utf8_lossy(&cmdline).replace('\0', " "))
        .expect("reading the held podman's cmdline");
    if what.starts_with("after-") {
        assert_eq!(held[1..], [0], "the held removal succeeded");
    }

    let removals = vec![
        format!(
            "ps --all --no-trunc --filter label=dev.estuary.vmm-owner={token} --format {{{{.ID}}}}"
        ),
        format!("rm --force --time=0 --ignore {}", footprint.id),
        format!("network ls --filter label=dev.estuary.vmm-owner={token} --format {{{{.Name}}}}"),
        format!("network rm {name}"),
    ];
    let reached = match gate {
        Gate::BeforeContainerRemoval | Gate::AfterContainerRemoval => 2,
        _ => 4,
    };
    assert_eq!(
        launcher::calls(&recoverer),
        removals[..reached],
        "the recovering process called nothing else, so far"
    );
    assert!(launcher::locked(&record), "the recovering process holds it");
    let at_gate = launcher::remaining(&footprint);
    if gate == Gate::BeforeContainerRemoval {
        launcher::assert_whole(&footprint);
    } else {
        assert!(
            !launcher::container_exists(&footprint.id),
            "the container is gone"
        );
    }
    assert_eq!(
        launcher::network_exists(&name),
        gate != Gate::AfterNetworkRemoval
    );
    for path in &footprint.paths {
        assert!(launcher::path_exists(path), "{path} remains at the gate");
    }
    launcher::assert_whole(&peer_footprint);
    evidence.note(format_args!(
        "peer at the gate: {}",
        served(&runtime, &root.state_dir, &peer_name)
    ));
    sentinels.assert_intact();
    evidence.note(format_args!(
        "held {what}: podman {wrapper} ({cmdline}) wrote {held:?}; recovering process {} \
         holds the record; remaining {at_gate:?}",
        recovering.host_pid
    ));

    let recovering_pid = recovering.host_pid;
    let killed = recovering.kill();
    assert_eq!(killed.signal(), Some(libc::SIGKILL));
    assert_eq!(
        launcher::start_time(u32::try_from(recovering_pid).expect("a PID")),
        None,
        "the recovering process is gone"
    );
    assert_eq!(
        launcher::start_time(wrapper),
        Some(wrapper_started.clone()),
        "the held podman outlives its caller"
    );
    let reparented = parent(wrapper);
    assert!(
        !launcher::locked(&record),
        "the record's lock went with the recovering process"
    );
    assert_eq!(
        launcher::remaining(&footprint),
        at_gate,
        "the kill changed nothing"
    );
    evidence.note(format_args!(
        "recovering process {recovering_pid} {killed} after {:?} held; podman {wrapper} lives \
         on beneath {reparented}; record unlocked",
        gated.elapsed()
    ));

    let orphan_ends = |hold: launcher::Hold| {
        std::mem::drop(hold);
        run::eventually("the orphaned podman to end", ORPHAN, || {
            (launcher::start_time(wrapper).as_ref() != Some(&wrapper_started)).then_some(())
        });
        evidence.note(format_args!(
            "released the orphaned podman {wrapper}, which ended; container {} exists: {}; \
             network {name} exists: {}",
            footprint.id,
            launcher::container_exists(&footprint.id),
            launcher::network_exists(&name)
        ));
    };
    if matches!(
        gate,
        Gate::BeforeContainerRemoval | Gate::BeforeNetworkRemoval
    ) {
        assert_eq!(recover(&runtime, &successor), 0);
        evidence.note(format_args!(
            "recovered beside the orphaned removal; remaining {:?}",
            launcher::remaining(&footprint)
        ));
        orphan_ends(hold);
    } else {
        orphan_ends(hold);
        if gate == Gate::BeforeNetworkRemovalRunFirst {
            assert!(
                !launcher::network_exists(&name),
                "the orphaned command removed the network"
            );
        }
        assert_eq!(recover(&runtime, &successor), 0);
    }

    let successor_calls: Vec<String> = launcher::calls(&other)
        .into_iter()
        .filter(|call| {
            [&token, &name, &footprint.id]
                .iter()
                .any(|of| call.contains(*of))
        })
        .collect();
    let mut looked = match gate {
        Gate::BeforeContainerRemoval => removals.clone(),
        Gate::AfterContainerRemoval | Gate::BeforeNetworkRemoval => {
            vec![
                removals[0].clone(),
                removals[2].clone(),
                removals[3].clone(),
            ]
        }
        Gate::BeforeNetworkRemovalRunFirst | Gate::AfterNetworkRemoval => {
            vec![removals[0].clone(), removals[2].clone()]
        }
    };
    looked.extend([
        format!("volume ls --filter label=dev.estuary.vmm-owner={token} --format {{{{.Name}}}}"),
        format!("volume rm {name}"),
    ]);
    assert_eq!(successor_calls, looked, "the next pass's release");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, &name),
        Vec::<String>::new()
    );
    launcher::assert_whole(&peer_footprint);
    evidence.note(format_args!(
        "peer after the next pass: {}",
        served(&runtime, &root.state_dir, &peer_name)
    ));
    sentinels.assert_intact();
    assert_eq!(
        launcher::leftovers(&recoverer, None),
        Vec::<String>::new(),
        "the killed process made nothing of its own"
    );
    evidence.note(format_args!(
        "next pass released {name} with {successor_calls:?}; remaining {:?}; peer whole; \
         sentinels intact",
        launcher::remaining(&footprint)
    ));

    peer.end();
    assert_eq!(
        launcher::left(&root.state_dir, &neighbour.tmp_dir, &peer_name),
        Vec::<String>::new()
    );
    assert_eq!(launcher::remaining(&peer_footprint), Vec::<String>::new());
    // With every owner gone, a last recovery still leaves what's unrecorded.
    assert_eq!(recover(&runtime, &successor), 0);
    sentinels.assert_intact();
    let state: std::collections::BTreeSet<String> = std::fs::read_dir(&root.state_dir)
        .expect("reading the state directory")
        .map(|entry| {
            entry
                .expect("reading the state directory")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect();
    let unrecorded = std::path::Path::new(&sentinels.files[0])
        .parent()
        .and_then(|dir| dir.file_name())
        .expect("the sentinel state directory")
        .to_string_lossy()
        .to_string();
    assert_eq!(
        state,
        [unrecorded].into(),
        "only the unrecorded state remains"
    );
    evidence.note(format_args!(
        "peer ended and released; last recovery left sentinels {sentinels:?}; state {state:?}"
    ));
    sentinels.remove();
    evidence.note("sentinels removed and listed gone");
}

#[test]
fn a_recovery_killed_before_container_removal() {
    a_kill_of_recovery(Gate::BeforeContainerRemoval);
}

#[test]
fn a_recovery_killed_after_container_removal() {
    a_kill_of_recovery(Gate::AfterContainerRemoval);
}

#[test]
fn a_recovery_killed_before_network_removal_then_recovered() {
    a_kill_of_recovery(Gate::BeforeNetworkRemoval);
}

#[test]
fn a_recovery_killed_before_network_removal_which_runs_first() {
    a_kill_of_recovery(Gate::BeforeNetworkRemovalRunFirst);
}

#[test]
fn a_recovery_killed_after_network_removal() {
    a_kill_of_recovery(Gate::AfterNetworkRemoval);
}

/// Bounds the eight passes and four launches, from the barrier's opening.
const GROUP: Duration = Duration::from_secs(90);

fn epoch_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis()
}

/// The launches with records in `state_dir`.
fn records(state_dir: &str) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(state_dir)
        .unwrap_or_else(|e| panic!("reading {state_dir}: {e}"))
        .filter_map(|entry| {
            let entry = entry.unwrap_or_else(|e| panic!("reading {state_dir}: {e}"));
            let file = entry.file_name().to_string_lossy().to_string();
            file.strip_suffix(".owner").map(ToString::to_string)
        })
        .collect()
}

/// PIDs of `root`'s podman wrappers still running, by their command lines.
fn wrappers(root: &launcher::Root) -> Vec<u32> {
    running_with(&root.podman)
}

/// PIDs of processes running with `arg` among their arguments.
fn running_with(arg: &str) -> Vec<u32> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir("/proc").expect("reading /proc") {
        let entry = entry.expect("reading /proc");
        let Some(pid) = entry.file_name().to_str().and_then(|pid| pid.parse().ok()) else {
            continue;
        };
        let cmdline = match std::fs::read(entry.path().join("cmdline")) {
            Ok(cmdline) => cmdline,
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.raw_os_error() == Some(libc::ESRCH) =>
            {
                continue;
            }
            Err(e) => panic!("reading {}'s cmdline: {e}", entry.path().display()),
        };
        if cmdline
            .split(|byte| *byte == 0)
            .any(|word| word == arg.as_bytes())
        {
            found.push(pid);
        }
    }
    found
}

/// A dead owner's launch, left at one stage of its lifecycle.
struct Orphan {
    stage: &'static str,
    root: launcher::Root,
    name: String,
    token: String,
    record: String,
    /// The calls the pass which takes its record makes to release it.
    release: Vec<String>,
    /// Kept by the test until the race, so that nothing recovers it first.
    lock: Option<std::fs::File>,
}

/// Leaves an owner killed while `hold` holds its podman at `what`, and
/// locks its record for the test.
fn orphan(
    run: &run::Run,
    state_dir: &str,
    stage: &'static str,
    hold: impl FnOnce(&launcher::Root) -> launcher::Hold,
    what: &str,
) -> Orphan {
    use std::os::unix::process::ExitStatusExt;

    let root = launcher::root();
    let before = records(state_dir);
    let hold = hold(&root);
    let owner = Owner::spawn(run, &root, state_dir, &run.fake_image, false);
    launcher::wait_held(&root, what, GATE);
    let killed = owner.kill();
    assert_eq!(killed.signal(), Some(libc::SIGKILL));
    std::mem::drop(hold);
    run::eventually("the orphaned podman to end", ORPHAN, || {
        wrappers(&root).is_empty().then_some(())
    });
    claimed(root, state_dir, stage, &before)
}

/// The record `root`'s launch claimed beneath `state_dir`, locked.
fn claimed(
    root: launcher::Root,
    state_dir: &str,
    stage: &'static str,
    before: &std::collections::BTreeSet<String>,
) -> Orphan {
    let made: Vec<String> = records(state_dir).difference(before).cloned().collect();
    let [name] = &made[..] else {
        panic!("{stage}: expected one new record, not {made:?}");
    };
    let record = launcher::record(state_dir, name);
    let lock = std::fs::File::open(&record).unwrap_or_else(|e| panic!("opening {record}: {e}"));
    lock.try_lock()
        .unwrap_or_else(|e| panic!("{stage}: {record} is still locked: {e}"));
    let token = launcher::token(&record);
    let label = format!("label=dev.estuary.vmm-owner={token}");
    let mut release = vec![format!(
        "ps --all --no-trunc --filter {label} --format {{{{.ID}}}}"
    )];
    if matches!(stage, "created" | "serving") {
        let id = run::podman(&["container", "inspect", "--format", "{{.Id}}", name]);
        release.push(format!("rm --force --time=0 --ignore {}", id.trim()));
    }
    release.push(format!(
        "network ls --filter {label} --format {{{{.Name}}}}"
    ));
    if stage != "claimed" {
        release.push(format!("network rm {name}"));
    }
    release.push(format!("volume ls --filter {label} --format {{{{.Name}}}}"));
    if matches!(stage, "created" | "serving") {
        release.push(format!("volume rm {name}"));
    }
    Orphan {
        stage,
        name: name.clone(),
        token,
        record,
        release,
        lock: Some(lock),
        root,
    }
}

/// Eight recovery passes start at once beneath one state directory, as the
/// recoveries of processes sharing it would, and four launches with them,
/// beside a serving VMM and orphans left at four stages: a claimed record
/// and its directories only, a network, a created container, and a serving
/// VMM. Each pass's first `ps` naming an orphan is held, so the four which
/// take an orphan's record wait holding its lock while the other four,
/// finding every orphan locked, pass them by. Each orphan is released by
/// exactly the pass which took it, every launch starts and is left whole, and
/// the peer serves throughout. The launches and the orphans other than the
/// serving VMM run the fake image; the peer and the serving orphan are real
/// VMMs.
#[test]
fn eight_recoveries_race_beside_launches() {
    use std::os::unix::process::ExitStatusExt;
    use std::sync::atomic::{AtomicBool, Ordering};

    let (run, shared, runtime) = setup();
    let state_dir = shared.state_dir.clone();
    let evidence = Evidence(run.run_dir.join("shared-orphans.evidence"));

    let peer_root = launcher::root();
    let peer = Owner::spawn(&run, &peer_root, &state_dir, &run.vmm_image, false);
    peer.started();
    let peer_name = launcher::vmm_name(&peer_root).expect("the launch named its network");
    let peer_footprint = launcher::footprint(&state_dir, &peer_root.tmp_dir, &peer_name);
    let _peer_rpc = hold_rpc(&runtime, &state_dir, &peer_name);
    evidence.note(format_args!(
        "peer {} at {}: {}",
        peer_name,
        peer_root.dir,
        served(&runtime, &state_dir, &peer_name)
    ));

    // The test holds each orphan's record locked until the race.
    let serving_root = launcher::root();
    let before = records(&state_dir);
    let owner = Owner::spawn(&run, &serving_root, &state_dir, &run.vmm_image, false);
    owner.started();
    let serving_name = launcher::vmm_name(&serving_root).expect("the launch named its network");
    let serving_footprint = launcher::footprint(&state_dir, &serving_root.tmp_dir, &serving_name);
    let _serving_rpc = hold_rpc(&runtime, &state_dir, &serving_name);
    assert_eq!(owner.kill().signal(), Some(libc::SIGKILL));
    let mut orphans = vec![claimed(serving_root, &state_dir, "serving", &before)];
    orphans.push(orphan(
        &run,
        &state_dir,
        "claimed",
        |root| launcher::hold(root, "verify"),
        "verify",
    ));
    orphans.push(orphan(
        &run,
        &state_dir,
        "network",
        |root| launcher::hold_after(root, "network-create", 1),
        "after-network-create",
    ));
    orphans.push(orphan(
        &run,
        &state_dir,
        "created",
        |root| launcher::hold(root, "created"),
        "created",
    ));
    for orphan in &orphans {
        let others: Vec<String> = launcher::calls(&orphan.root)
            .into_iter()
            .filter(|call| {
                orphans
                    .iter()
                    .any(|o| o.name != orphan.name && call.contains(&o.token))
            })
            .collect();
        assert_eq!(
            others,
            Vec::<String>::new(),
            "{}'s launch touched no other orphan",
            orphan.stage
        );
    }
    let sentinels = Sentinels::plant(&run, &shared);
    evidence.note(format_args!(
        "orphans {:?}; serving footprint {serving_footprint:?}; sentinels {sentinels:?}",
        orphans
            .iter()
            .map(|o| (o.stage, &o.name, &o.root.dir))
            .collect::<Vec<_>>()
    ));

    let barrier = format!("{}/barrier", shared.dir);
    let recoverers: Vec<launcher::Root> = (0..8).map(|_| launcher::root()).collect();
    let orphan_tokens: Vec<&str> = orphans.iter().map(|orphan| orphan.token.as_str()).collect();
    let holds: Vec<launcher::Hold> = recoverers
        .iter()
        .map(|root| launcher::hold_naming(root, "ps", &orphan_tokens))
        .collect();
    let recovering: Vec<Owner> = recoverers
        .iter()
        .map(|root| Owner::recovering(&run, root, &state_dir, Some(&barrier)))
        .collect();
    let launchers: Vec<launcher::Root> = (0..4).map(|_| launcher::root()).collect();
    let owners: Vec<Owner> = launchers
        .iter()
        .map(|root| {
            Owner::spawn_at(
                &run,
                root,
                &state_dir,
                &run.fake_image,
                false,
                Some(&barrier),
                None,
            )
        })
        .collect();
    for owner in recovering.iter().chain(&owners) {
        owner.report("waiting", STARTED);
    }

    // The starting state, with only the twelve about to start.
    for orphan in &mut orphans {
        std::mem::drop(orphan.lock.take());
        assert!(
            !launcher::locked(&orphan.record),
            "{} is free",
            orphan.stage
        );
        let (network, container) = match orphan.stage {
            "claimed" => (false, false),
            "network" => (true, false),
            _ => (true, true),
        };
        assert_eq!(
            (
                launcher::network_exists(&orphan.name),
                launcher::container_exists(&orphan.name)
            ),
            (network, container),
            "{}'s network and container",
            orphan.stage
        );
        for path in [
            format!("{state_dir}/{}", orphan.name),
            orphan.record.clone(),
        ] {
            assert!(launcher::path_exists(&path), "{path} exists");
        }
    }
    launcher::assert_whole(&serving_footprint);
    launcher::assert_whole(&peer_footprint);
    let mut expected: std::collections::BTreeSet<String> =
        orphans.iter().map(|o| o.name.clone()).collect();
    expected.insert(peer_name.clone());
    assert_eq!(records(&state_dir), expected, "the records before the race");
    evidence.note(format_args!(
        "starting state: records {expected:?}, orphans unlocked, peer whole; {}",
        served(&runtime, &state_dir, &peer_name)
    ));

    let stop = AtomicBool::new(false);
    let checks = std::thread::scope(|scope| {
        let checker = scope.spawn(|| {
            let mut checks = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                checks.push(format!(
                    "{} {}",
                    epoch_ms(),
                    served(&runtime, &state_dir, &peer_name)
                ));
                std::thread::sleep(Duration::from_millis(250));
            }
            checks
        });
        struct Stop<'a>(&'a AtomicBool);
        impl Drop for Stop<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }
        let _stop = Stop(&stop);

        let opened = epoch_ms();
        std::fs::write(&barrier, "").expect("opening the barrier");
        let gone: Vec<u128> = recovering
            .iter()
            .chain(&owners)
            .map(|owner| owner.report("go", GROUP).parse().expect("a time"))
            .collect();
        evidence.note(format_args!(
            "barrier opened at {opened}; the twelve went at {gone:?}, within {} ms",
            gone.iter().max().unwrap() - gone.iter().min().unwrap()
        ));

        let held = |root: &launcher::Root| launcher::path_exists(&format!("{}/held-ps", root.dir));
        run::eventually("four passes to hold an orphan each", GROUP, || {
            (recoverers.iter().filter(|root| held(root)).count() == orphans.len()).then_some(())
        });
        let mut taken = std::collections::BTreeMap::new();
        for (i, root) in recoverers.iter().enumerate() {
            if !held(root) {
                assert_eq!(recovering[i].report("recovered", GROUP), "0");
                evidence.note(format_args!("pass {i} ended, past every orphan"));
            }
        }
        for (i, owner) in owners.iter().enumerate() {
            owner.report("started", GROUP);
            evidence.note(format_args!("launch {i} started"));
        }
        // Everything below is observed while the four still hold.
        for (i, root) in recoverers.iter().enumerate() {
            let calls = launcher::calls(root);
            let named: Vec<&Orphan> = orphans
                .iter()
                .filter(|o| calls.iter().any(|call| call.contains(&o.token)))
                .collect();
            if held(root) {
                let [orphan] = &named[..] else {
                    panic!("pass {i} holds one orphan, not {calls:?}");
                };
                assert_eq!(
                    calls,
                    orphan.release[..1],
                    "pass {i} is held at its first call"
                );
                assert!(
                    launcher::locked(&orphan.record),
                    "pass {i} holds {}",
                    orphan.stage
                );
                assert!(
                    taken.insert(orphan.stage, i).is_none(),
                    "{} taken once",
                    orphan.stage
                );
            } else {
                assert!(named.is_empty(), "pass {i} touched no orphan: {calls:?}");
            }
        }
        assert_eq!(
            taken.len(),
            orphans.len(),
            "every orphan is held by one pass"
        );
        launcher::assert_whole(&serving_footprint);
        launcher::assert_whole(&peer_footprint);
        evidence.note(format_args!(
            "while held: orphan stage -> pass {taken:?}, each record locked, the serving \
             orphan whole; {}",
            served(&runtime, &state_dir, &peer_name)
        ));

        std::mem::drop(holds);
        for i in taken.values() {
            assert_eq!(recovering[*i].report("recovered", GROUP), "0");
            evidence.note(format_args!("pass {i} ended, having released its orphan"));
        }
        stop.store(true, Ordering::Relaxed);
        (checker.join().expect("the peer's service checks"), taken)
    });
    let (checks, taken) = checks;
    evidence.note(format_args!(
        "all eight passes ended and all four launches started; the peer answered all {} checks \
         through the race",
        checks.len()
    ));
    for check in &checks {
        evidence.note(format_args!("peer check at {check}"));
    }

    // Each orphan was released by the pass which took it, and only that.
    for orphan in &orphans {
        let i = taken[orphan.stage];
        let calls = launcher::calls(&recoverers[i]);
        let n = orphan.release.len();
        assert_eq!(
            calls[..n],
            orphan.release[..],
            "pass {i} released {}",
            orphan.stage
        );
        // It then lists what carries the state directory's label. A
        // concurrent release may leave it a resource to remove meanwhile.
        let labeled = format!("--filter label=dev.estuary.vmm-state-dir={state_dir} ");
        assert_eq!(
            calls[n..]
                .iter()
                .filter(|call| call.contains(&labeled))
                .count(),
            3,
            "pass {i} then listed by its state directory: {calls:?}"
        );
        for (j, root) in recoverers.iter().chain(&launchers).enumerate() {
            assert!(
                j == i
                    || !launcher::calls(root)
                        .iter()
                        .any(|call| call.contains(&orphan.token)),
                "only pass {i} released {}",
                orphan.stage
            );
        }
        assert_eq!(
            launcher::left(&state_dir, &orphan.root.tmp_dir, &orphan.name),
            Vec::<String>::new(),
            "{} is gone",
            orphan.stage
        );
    }
    assert_eq!(
        launcher::remaining(&serving_footprint),
        Vec::<String>::new()
    );
    let footprints: Vec<launcher::Footprint> = launchers
        .iter()
        .map(|root| {
            let name = launcher::vmm_name(root).expect("the launch named its network");
            launcher::footprint(&state_dir, &root.tmp_dir, &name)
        })
        .collect();
    launcher::assert_whole(&peer_footprint);
    sentinels.assert_intact();
    evidence.note(format_args!(
        "orphans released as expected; the four launches serve {:?}; peer whole; sentinels \
         intact; {}",
        footprints.iter().map(|f| &f.name).collect::<Vec<_>>(),
        served(&runtime, &state_dir, &peer_name)
    ));

    for owner in owners {
        owner.end();
    }
    for footprint in &footprints {
        assert_eq!(
            launcher::remaining(footprint),
            Vec::<String>::new(),
            "{} is gone",
            footprint.name
        );
    }
    peer.end();
    assert_eq!(launcher::remaining(&peer_footprint), Vec::<String>::new());
    let other = launcher::root();
    assert_eq!(
        recover(
            &runtime,
            &vmm_at(&other.podman, &state_dir, &run.fake_image)
        ),
        0
    );
    sentinels.assert_intact();
    let unrecorded = std::path::Path::new(&sentinels.files[0])
        .parent()
        .and_then(|dir| dir.file_name())
        .expect("the sentinel state directory")
        .to_string_lossy()
        .to_string();
    let state: std::collections::BTreeSet<String> = std::fs::read_dir(&state_dir)
        .expect("reading the state directory")
        .map(|entry| {
            entry
                .expect("reading the state directory")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect();
    assert_eq!(
        state,
        [unrecorded].into(),
        "only the unrecorded state remains"
    );
    let roots = recoverers
        .iter()
        .chain(&launchers)
        .chain(orphans.iter().map(|orphan| &orphan.root))
        .chain([&peer_root, &other]);
    for root in roots {
        assert_eq!(
            wrappers(root),
            Vec::<u32>::new(),
            "{}'s podman has ended",
            root.dir
        );
    }
    evidence.note(format_args!(
        "the launches and the peer ended and were measured gone; a last recovery left state \
         {state:?}; no podman of the test's runs"
    ));
    sentinels.remove();
    evidence.note("sentinels removed and listed gone");
}

/// A process's state and start time, by its `stat`, or None if it has gone.
fn stat(pid: u32) -> Option<(String, String)> {
    let path = format!("/proc/{pid}/stat");
    let stat = match std::fs::read_to_string(&path) {
        Ok(stat) => stat,
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.raw_os_error() == Some(libc::ESRCH) =>
        {
            return None;
        }
        Err(e) => panic!("reading {path}: {e}"),
    };
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, fields)| fields.split_whitespace().collect())
        .unwrap_or_default();
    match (fields.first(), fields.get(19)) {
        (Some(state), Some(started)) => Some((state.to_string(), started.to_string())),
        _ => panic!("parsing {path}: {stat:?}"),
    }
}

/// Whether process `pid`, started at `started`, still runs. A zombie, ended
/// but not yet reaped, does not.
fn runs(pid: u32, started: &str) -> bool {
    stat(pid).is_some_and(|(state, start)| start == started && state != "Z")
}

/// A running process's children, each with its start time.
fn children(pid: u32) -> Vec<(u32, String)> {
    let path = format!("/proc/{pid}/task/{pid}/children");
    let children = match std::fs::read_to_string(&path) {
        Ok(children) => children,
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.raw_os_error() == Some(libc::ESRCH) =>
        {
            return Vec::new();
        }
        Err(e) => panic!("reading {path}: {e}"),
    };
    children
        .split_whitespace()
        .filter_map(|child| {
            let child: u32 = child.parse().unwrap_or_else(|e| panic!("{path}: {e}"));
            stat(child).map(|(_, started)| (child, started))
        })
        .collect()
}

/// A real engine timeout must terminate its command, preserve resources whose
/// container status is unknown, and allow a later recovery to finish. Release
/// state-machine tests cover failures at the other query and removal steps.
#[test]
fn a_stalled_container_query_keeps_everything() {
    let (run, root, runtime) = setup();
    let (neighbour, recoverer) = (launcher::root(), launcher::root());
    let evidence = Evidence(run.run_dir.join("stalled-recovery.evidence"));
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    owner.started();
    let name = launcher::vmm_name(&root).unwrap();
    let record = launcher::record(&root.state_dir, &name);
    let token = launcher::token(&record);
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    let _rpc = hold_rpc(&runtime, &root.state_dir, &name);
    let peer = Owner::spawn(&run, &neighbour, &root.state_dir, &run.vmm_image, false);
    peer.started();
    let peer_name = launcher::vmm_name(&neighbour).unwrap();
    let peer_footprint = launcher::footprint(&root.state_dir, &neighbour.tmp_dir, &peer_name);
    let _peer_rpc = hold_rpc(&runtime, &root.state_dir, &peer_name);
    let sentinels = Sentinels::plant(&run, &root);
    owner.kill();
    let written = std::fs::read(&record).unwrap();
    let control = launcher::stall(&recoverer, "ps", &[&token]);
    let recovering = vmm_at(&recoverer.podman, &root.state_dir, &run.vmm_image);
    let task = {
        let recovering = recovering.clone();
        runtime.spawn(async move {
            tokio::time::timeout(STARTED, recovering.recover())
                .await
                .expect("the pass to finish")
        })
    };
    let stalled = run::eventually("the recovery's stalled query", STARTED, || {
        launcher::stalled(&recoverer, "ps").into_iter().next()
    });
    let started = stat(stalled.pid).expect("the stalled command runs").1;
    let descendants = children(stalled.pid);
    assert!(launcher::locked(&record));
    launcher::assert_whole(&footprint);
    evidence.note(format_args!(
        "stalled {stalled:?}; peer: {}",
        served(&runtime, &root.state_dir, &peer_name)
    ));

    assert_eq!(runtime.block_on(task).unwrap(), 1, "the timed-out query");
    // The stall stays installed: only the pass's timeout can end the command.
    run::eventually(
        "the timed-out command and its observed children to end",
        RELEASED,
        || {
            (!runs(stalled.pid, &started)
                && descendants
                    .iter()
                    .all(|(pid, started)| !runs(*pid, started)))
            .then_some(())
        },
    );
    assert_eq!(launcher::stalled(&recoverer, "ps"), [stalled]);
    assert_eq!(std::fs::read(&record).unwrap(), written);
    assert!(!launcher::locked(&record));
    launcher::assert_whole(&footprint);
    launcher::assert_whole(&peer_footprint);
    sentinels.assert_intact();
    evidence.note(format_args!(
        "timeout kept the orphan; peer: {}",
        served(&runtime, &root.state_dir, &peer_name)
    ));

    drop(control);
    assert_eq!(recover(&runtime, &recovering), 0);
    assert!(removed(&recoverer, &footprint.id));
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
    launcher::assert_whole(&peer_footprint);
    sentinels.assert_intact();
    evidence.note(format_args!(
        "retry released the orphan; peer: {}",
        served(&runtime, &root.state_dir, &peer_name)
    ));
    peer.end();
    assert_eq!(launcher::remaining(&peer_footprint), Vec::<String>::new());
    assert_eq!(records(&root.state_dir), Default::default());
    assert_eq!(wrappers(&recoverer), Vec::<u32>::new());
    sentinels.remove();
}

/// A launch's ingress grant which podman's API service creates, or starts,
/// only after recovery has revoked its token and removed its record. Each
/// grant is a remote `podman run`'s requests, sent apart: its create, then
/// (perhaps) its start. Recovery reaches the same service, as a production
/// launcher reaches the host's.
struct LateGrant {
    /// The service's URL.
    url: String,
    /// The VMM image, whose `nft` lists the host's tables.
    vmm_image: String,
    /// The launch's name, `fv_<id>`.
    name: String,
    /// The grant's container: `fv_<id>-ingress`, and its ID.
    container: String,
    id: String,
    token: String,
    /// The ID of the image it was created from.
    image_id: String,
    /// When its create was sent, in seconds since the epoch.
    since: String,
}

impl LateGrant {
    /// The create through `api` of a grant of a new launch beneath
    /// `state_dir`, whose record is gone.
    fn create(run: &run::Run, api: &launcher::Api, state_dir: &str) -> Self {
        let name = format!("fv_{}", run::random_hex());
        let token = format!("{}{}", run::random_hex(), run::random_hex());
        Self::create_as(run, api, state_dir, &name, &token, &run.vmm_image)
    }

    /// The create of launch `name`'s grant under `token`, from `image`, as
    /// the plan's line creates it.
    fn create_as(
        run: &run::Run,
        api: &launcher::Api,
        state_dir: &str,
        name: &str,
        token: &str,
        image: &str,
    ) -> Self {
        let container = format!("{name}-ingress");
        // Released newest first: the container, then its grant.
        run::record("ingress", &format!("{} {token}", run.vmm_image));
        run::record("container", &container);

        let since = epoch_ms().div_euclid(1000).to_string();
        let created = api.podman(&[
            "create",
            "--network=host",
            "--log-driver=none",
            "--read-only",
            "--cap-drop=all",
            "--cap-add=CAP_NET_ADMIN",
            &format!("--name={container}"),
            &format!("--label=dev.estuary.vmm-owner={token}"),
            &format!("--label=dev.estuary.vmm-state-dir={state_dir}"),
            image,
            "boundary",
            "grant",
            &format!("--owner={token}"),
            &format!("--bridge=fvm{}", &name[3..15]),
            "--port=8080",
        ]);
        assert!(
            created.status.success(),
            "creating {container}: {}",
            String::from_utf8_lossy(&created.stderr)
        );
        let id = String::from_utf8_lossy(&created.stdout).trim().to_string();
        let image_id = run::podman(&["container", "inspect", "--format", "{{.Image}}", &id]);
        Self {
            url: api.url.clone(),
            vmm_image: run.vmm_image.clone(),
            image_id: image_id.trim().to_string(),
            id,
            name: name.to_string(),
            container,
            token: token.to_string(),
            since,
        }
    }

    /// `podman --remote` of its service, as root.
    fn api(&self, args: &[&str]) -> std::process::Output {
        let mut argv = vec!["podman", "--remote", "--url", &self.url];
        argv.extend(args);
        run::sudo_output(&argv, None)
    }

    /// Its start, which must grant, and its container then exit and remain.
    fn start(&self) {
        let started = self.api(&["start", &self.id]);
        assert!(
            started.status.success(),
            "starting {}: {}",
            self.container,
            String::from_utf8_lossy(&started.stderr)
        );
        let waited = self.api(&["wait", &self.id]);
        assert_eq!(
            String::from_utf8_lossy(&waited.stdout).trim(),
            "0",
            "{} granted",
            self.container
        );
        assert!(self.granted() && self.exists(), "granted, and kept");
    }

    fn exists(&self) -> bool {
        launcher::container_exists(&self.id)
    }

    /// Whether the host holds the grant's nftables table.
    fn granted(&self) -> bool {
        let table = format!("flow_vmm_ingress_{}", self.token);
        let listed = run::sudo_output(
            &[
                "podman",
                "run",
                "--rm",
                "--network=host",
                "--cap-drop=all",
                "--cap-add=CAP_NET_ADMIN",
                "--entrypoint=nft",
                &self.vmm_image,
                "list",
                "table",
                "inet",
                &table,
            ],
            None,
        );
        match listed.status.code() {
            Some(0) => true,
            Some(1) => false,
            _ => panic!(
                "listing {table}: {}",
                String::from_utf8_lossy(&listed.stderr)
            ),
        }
    }

    /// What the service did with the container, in its order.
    fn events(&self) -> Vec<String> {
        let events = self.api(&[
            "events",
            "--stream=false",
            "--since",
            &self.since,
            "--filter",
            &format!("container={}", self.id),
            "--format",
            "{{.Status}}",
        ]);
        assert!(
            events.status.success(),
            "{}",
            String::from_utf8_lossy(&events.stderr)
        );
        String::from_utf8_lossy(&events.stdout)
            .lines()
            .filter(|status| *status != "sync")
            .map(ToString::to_string)
            .collect()
    }

    /// The claim a pass must record of it, in its fields' order: its token,
    /// and the ID of the image it was created from.
    fn claim(&self) -> String {
        format!(
            "{{\"version\":1,\"id\":\"{}\",\"token\":\"{}\",\"ingress_image\":\"{}\"}}\n",
            self.name, self.token, self.image_id
        )
    }

    fn assert_gone(&self, state_dir: &str) {
        assert!(!self.exists(), "{} is removed", self.container);
        assert!(!self.granted(), "{}'s grant is revoked", self.container);
        assert!(
            !launcher::path_exists(&launcher::record(state_dir, &self.name)),
            "its record is removed"
        );
    }
}

/// The recovery of `state_dir` by a podman of its own, `root`'s, which
/// reaches `api`.
fn remote_recovery(
    root: &launcher::Root,
    api: &launcher::Api,
    state_dir: &str,
    run: &run::Run,
) -> connector::Vmm {
    launcher::remote(root, &api.url);
    vmm_at(&root.podman, state_dir, &run.vmm_image)
}

/// The race of a grant whose start arrives after recovery. A dead launch's
/// record is released, revoking its token. Then its grant's container is
/// created, after that release, and the next pass finds it with no record.
/// That pass records it first, locked, and only then removes it. The start
/// podman's service is yet to serve then fails, so nothing is granted.
#[test]
fn a_grant_started_after_recovery_cannot_grant() {
    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root, false);
    let recoverer = launcher::root();
    let recovery = remote_recovery(&recoverer, &api, &root.state_dir, &run);
    let evidence = Evidence(run.run_dir.join("late-grant-start.evidence"));

    // What a launch's own claim held: its token, and the image to revoke with.
    let name = format!("fv_{}", run::random_hex());
    let token = format!("{}{}", run::random_hex(), run::random_hex());
    let mounts = launcher::mounts(&root);
    std::fs::write(
        launcher::record(&root.state_dir, &name),
        format!(
            "{}\n",
            serde_json::json!({
                "version": 1,
                "id": name,
                "token": token,
                "mount": format!("{mounts}/mount-{name}"),
                "ingress_image": run.vmm_image,
            })
        ),
    )
    .unwrap();
    assert_eq!(recover(&runtime, &recovery), 0);
    let calls = launcher::calls(&recoverer);
    assert!(
        calls
            .iter()
            .any(|call| call.ends_with(&format!("boundary revoke --owner={token}"))),
        "the record's release revoked {token}: {calls:#?}"
    );
    assert!(!launcher::path_exists(&launcher::record(
        &root.state_dir,
        &name
    )));

    // Its grant's create arrives only now.
    let late = LateGrant::create_as(&run, &api, &root.state_dir, &name, &token, &run.vmm_image);
    assert!(late.exists() && !late.granted());
    evidence.note(format_args!(
        "{} created after its record's release, not started",
        late.id
    ));

    let hold = launcher::hold(&recoverer, "rm");
    let pass = {
        let recovery = recovery.clone();
        runtime.spawn(async move { recovery.recover().await })
    };
    launcher::wait_held(&recoverer, "rm", GATE);
    let record = launcher::record(&root.state_dir, &late.name);
    assert_eq!(
        std::fs::read_to_string(&record).unwrap(),
        late.claim(),
        "recorded before its removal"
    );
    assert!(launcher::locked(&record), "and locked");
    assert!(late.exists(), "not yet removed");
    evidence.note(format_args!(
        "held its removal: record {:?}, locked",
        std::fs::read_to_string(&record).unwrap()
    ));
    std::mem::drop(hold);
    assert_eq!(runtime.block_on(pass).unwrap(), 0);
    late.assert_gone(&root.state_dir);

    let started = late.api(&["start", &late.id]);
    assert!(!started.status.success(), "a start after its removal fails");
    assert!(
        String::from_utf8_lossy(&started.stderr).contains("no such container"),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    assert!(!late.granted(), "and grants nothing");
    assert_eq!(late.events(), ["create", "remove"]);
    evidence.note(format_args!(
        "the late start failed: {}; the service's events {:?}",
        String::from_utf8_lossy(&started.stderr).trim(),
        late.events()
    ));
}

/// A late grant which podman created and started, so that it granted, is
/// revoked by the next pass, which records it, removes it, then revokes. The
/// name it was run by has since moved to another image: the record holds the
/// ID of the one which granted, and the revoke runs that.
#[test]
fn a_late_grant_which_ran_is_revoked() {
    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root, false);
    let recoverer = launcher::root();
    let recovery = remote_recovery(&recoverer, &api, &root.state_dir, &run);

    let tag = format!("localhost/connector-vmm-tests-grant:{}", run::random_hex());
    run::record("image", &tag);
    run::podman(&["tag", &run.vmm_image, &tag]);
    let name = format!("fv_{}", run::random_hex());
    let token = format!("{}{}", run::random_hex(), run::random_hex());
    let late = LateGrant::create_as(&run, &api, &root.state_dir, &name, &token, &tag);
    late.start();
    run::podman(&["tag", &run.fake_image, &tag]);
    let moved = run::podman(&["image", "inspect", "--format", "{{.Id}}", &tag]);
    assert_ne!(
        moved.trim(),
        late.image_id,
        "the name now names another image"
    );

    let hold = launcher::hold(&recoverer, "rm");
    let pass = {
        let recovery = recovery.clone();
        runtime.spawn(async move { recovery.recover().await })
    };
    launcher::wait_held(&recoverer, "rm", GATE);
    let record = launcher::record(&root.state_dir, &late.name);
    assert_eq!(std::fs::read_to_string(&record).unwrap(), late.claim());
    std::mem::drop(hold);
    assert_eq!(runtime.block_on(pass).unwrap(), 0);
    late.assert_gone(&root.state_dir);

    let calls: Vec<String> = launcher::calls(&recoverer)
        .into_iter()
        .filter(|call| {
            [&late.token, &late.name, &late.id]
                .iter()
                .any(|of| call.contains(*of))
        })
        .map(|call| {
            call.replace(&late.token, "<token>")
                .replace(&late.name, "fv_<id>")
                .replace(&late.id, "<id>")
                .replace(&late.image_id, "<granting image id>")
                .replace(&root.state_dir, "<state>")
        })
        .collect();
    insta::assert_snapshot!(calls.join("\n"), @r"
    container inspect fv_<id>-ingress
    ps --all --no-trunc --filter label=dev.estuary.vmm-owner=<token> --format {{.ID}}
    rm --force --time=0 --ignore <id>
    run --rm --network=host --log-driver=none --read-only --cap-drop=all --cap-add=CAP_NET_ADMIN --name=fv_<id>-revoke --label=dev.estuary.vmm-owner=<token> --label=dev.estuary.vmm-state-dir=<state> <granting image id> boundary revoke --owner=<token>
    network ls --filter label=dev.estuary.vmm-owner=<token> --format {{.Name}}
    volume ls --filter label=dev.estuary.vmm-owner=<token> --format {{.Name}}
    ");
    let events = late.events();
    let at = |status: &str| events.iter().position(|event| event == status);
    assert!(
        at("create") < at("start") && at("start") < at("died") && at("died") < at("remove"),
        "{events:?}"
    );
    run::podman(&["untag", &tag, &tag]);
}

/// What a pass records of a late grant outlasts a failed removal of its
/// container, and a failed revoke, either of which keeps the record for a
/// later pass to finish from.
#[test]
fn a_late_grant_is_kept_recorded_across_failures() {
    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root, false);
    let recoverer = launcher::root();
    let recovery = remote_recovery(&recoverer, &api, &root.state_dir, &run);

    let late = LateGrant::create(&run, &api, &root.state_dir);
    late.start();
    let record = launcher::record(&root.state_dir, &late.name);
    let fault = launcher::fail(&recoverer, "rm", &late.id);
    assert_eq!(recover(&runtime, &recovery), 1, "its removal failed");
    assert_eq!(std::fs::read_to_string(&record).unwrap(), late.claim());
    assert!(!launcher::locked(&record), "let go for a later pass");
    assert!(late.exists() && late.granted(), "untouched");
    std::mem::drop(fault);

    let fault = launcher::fail(&recoverer, "revoke", &late.token);
    assert_eq!(recover(&runtime, &recovery), 1, "its revoke failed");
    assert_eq!(std::fs::read_to_string(&record).unwrap(), late.claim());
    assert!(!late.exists() && late.granted(), "removed, not yet revoked");
    std::mem::drop(fault);

    assert_eq!(recover(&runtime, &recovery), 0);
    late.assert_gone(&root.state_dir);
}

/// A process recovering a late grant dies after recording it: before its
/// removal of the container, or after it and before the revoke. The record it
/// left, unlocked, has the next pass finish the job.
#[test]
fn a_late_grant_survives_its_recovery_dying() {
    use std::os::unix::process::ExitStatusExt;

    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root, false);
    let next = launcher::root();
    let recovery = remote_recovery(&next, &api, &root.state_dir, &run);

    for (what, removed) in [("rm", false), ("after-rm", true)] {
        let late = LateGrant::create(&run, &api, &root.state_dir);
        late.start();
        let record = launcher::record(&root.state_dir, &late.name);
        let dying = launcher::root();
        launcher::remote(&dying, &api.url);
        let hold = match what {
            "rm" => launcher::hold(&dying, "rm"),
            _ => launcher::hold_after(&dying, "rm", 1),
        };
        let owner = Owner::recovering(&run, &dying, &root.state_dir, None);
        launcher::wait_held(&dying, what, GATE);
        assert_eq!(std::fs::read_to_string(&record).unwrap(), late.claim());
        assert!(launcher::locked(&record), "held by the recovering process");
        assert_eq!(owner.kill().signal(), Some(libc::SIGKILL));
        assert!(!launcher::locked(&record), "let go as it died");
        assert_eq!(std::fs::read_to_string(&record).unwrap(), late.claim());
        assert_eq!((late.exists(), late.granted()), (!removed, true), "{what}");

        assert_eq!(recover(&runtime, &recovery), 0, "{what}");
        late.assert_gone(&root.state_dir);
        std::mem::drop(hold);
        run::eventually("the orphaned podman to end", ORPHAN, || {
            wrappers(&dying).is_empty().then_some(())
        });
        assert!(
            !late.granted(),
            "{what}: the orphaned removal grants nothing"
        );
    }
}

/// Two passes find one late grant with no record. Only the one whose claim
/// links its record first releases it; the other's claim finds the record
/// there, and it leaves the grant to the first. A third pass, meanwhile,
/// finds the record held and lets it be.
#[test]
fn a_late_grant_is_recorded_once() {
    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root, false);
    let (first, second, third) = (launcher::root(), launcher::root(), launcher::root());
    let passes =
        [&first, &second, &third].map(|at| remote_recovery(at, &api, &root.state_dir, &run));

    let late = LateGrant::create(&run, &api, &root.state_dir);
    late.start();
    let record = launcher::record(&root.state_dir, &late.name);
    let holds = [
        launcher::hold(&first, "inspect"),
        launcher::hold(&second, "inspect"),
    ];
    let spawn = |vmm: &connector::Vmm| {
        let vmm = vmm.clone();
        runtime.spawn(async move { vmm.recover().await })
    };
    let (first_pass, second_pass) = (spawn(&passes[0]), spawn(&passes[1]));
    launcher::wait_held(&first, "inspect", GATE);
    launcher::wait_held(&second, "inspect", GATE);
    assert!(!launcher::path_exists(&record), "both found it unrecorded");

    let removal = launcher::hold(&first, "rm");
    let [first_inspect, second_inspect] = holds;
    std::mem::drop(first_inspect);
    launcher::wait_held(&first, "rm", GATE);
    assert!(launcher::locked(&record), "the first recorded it");
    std::mem::drop(second_inspect);
    assert_eq!(runtime.block_on(second_pass).unwrap(), 0);
    assert_eq!(recover(&runtime, &passes[2]), 0);
    for (at, root) in [("second", &second), ("third", &third)] {
        let named: Vec<String> = launcher::calls(root)
            .into_iter()
            .filter(|call| {
                call.starts_with("rm ") || call.contains(&late.token) || call.contains(&late.id)
            })
            .collect();
        assert_eq!(named, Vec::<String>::new(), "the {at} removed nothing");
    }
    assert!(late.exists() && late.granted(), "left to the first");

    std::mem::drop(removal);
    assert_eq!(runtime.block_on(first_pass).unwrap(), 0);
    late.assert_gone(&root.state_dir);
}

/// A pass inspects a late grant, then another pass records, removes and
/// revokes it, and removes its record, before the first claims one. The
/// first's record then names a grant already gone: its release finds no
/// container, revokes again, and removes the record.
#[test]
fn a_late_grant_released_meanwhile_is_released_again() {
    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root, false);
    let (first, second) = (launcher::root(), launcher::root());
    let passes = [&first, &second].map(|at| remote_recovery(at, &api, &root.state_dir, &run));

    let late = LateGrant::create(&run, &api, &root.state_dir);
    late.start();
    let inspected = launcher::hold_after(&first, "inspect", 1);
    let first_pass = {
        let vmm = passes[0].clone();
        runtime.spawn(async move { vmm.recover().await })
    };
    launcher::wait_held(&first, "after-inspect", GATE);
    assert_eq!(recover(&runtime, &passes[1]), 0);
    late.assert_gone(&root.state_dir);

    std::mem::drop(inspected);
    assert_eq!(runtime.block_on(first_pass).unwrap(), 0);
    late.assert_gone(&root.state_dir);
    let calls: Vec<String> = launcher::calls(&first)
        .into_iter()
        .filter(|call| call.contains(&late.token) || call.contains(&late.name))
        .map(|call| {
            call.replace(&late.token, "<token>")
                .replace(&late.name, "fv_<id>")
                .replace(&late.image_id, "<granting image id>")
                .replace(&root.state_dir, "<state>")
        })
        .collect();
    insta::assert_snapshot!(calls.join("\n"), @r"
    container inspect fv_<id>-ingress
    ps --all --no-trunc --filter label=dev.estuary.vmm-owner=<token> --format {{.ID}}
    run --rm --network=host --log-driver=none --read-only --cap-drop=all --cap-add=CAP_NET_ADMIN --name=fv_<id>-revoke --label=dev.estuary.vmm-owner=<token> --label=dev.estuary.vmm-state-dir=<state> <granting image id> boundary revoke --owner=<token>
    network ls --filter label=dev.estuary.vmm-owner=<token> --format {{.Name}}
    volume ls --filter label=dev.estuary.vmm-owner=<token> --format {{.Name}}
    ");
}

/// Root's POSIX lock on `path` until dropped: the kind container storage
/// takes, so podman waits on it, as it would not on a BSD `flock`.
struct StoreGate {
    holder: std::process::Child,
    inode: String,
}

impl StoreGate {
    fn hold(path: &str) -> Self {
        use std::io::BufRead;

        const LOCK: &str = "import fcntl, sys
f = open(sys.argv[1], 'r+')
fcntl.lockf(f, fcntl.LOCK_EX)
print('locked', flush=True)
sys.stdin.buffer.read()
";
        let mut holder = std::process::Command::new("sudo")
            .args(["-n", "python3", "-c", LOCK, path])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("locking {path}: {e}"));
        let mut locked = String::new();
        std::io::BufReader::new(holder.stdout.as_mut().expect("stdout is piped"))
            .read_line(&mut locked)
            .unwrap_or_else(|e| panic!("locking {path}: {e}"));
        assert_eq!(locked.trim(), "locked", "locking {path}");
        Self {
            holder,
            inode: run::sudo(&["stat", "--format=%i", path]).trim().to_string(),
        }
    }

    /// The process waiting on this lock in `/proc/locks`, if its command
    /// line names `url`: an API service, waiting while it serves a request.
    fn waiter(&self, url: &str) -> Option<u32> {
        let locks = std::fs::read_to_string("/proc/locks").expect("reading /proc/locks");
        locks.lines().find_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [_, "->", "POSIX", _, _, pid, file, ..] = fields[..] else {
                return None;
            };
            if file.rsplit(':').next() != Some(self.inode.as_str()) {
                return None;
            }
            let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
            String::from_utf8_lossy(&cmdline)
                .split('\0')
                .any(|arg| arg == url)
                .then(|| pid.parse().expect("a PID"))
        })
    }
}

impl Drop for StoreGate {
    fn drop(&mut self) {
        std::mem::drop(self.holder.stdin.take());
        _ = self.holder.wait();
    }
}

/// A create which podman's API service accepted, and then waited to carry
/// out, outlives its client, and the release of its launch's record by a
/// pass which found nothing of it. The service, of a store of its own so that
/// its gate holds up no other test, waits on a lock of its containers'
/// storage, which its listings don't take. Once the create lands, a later
/// pass of the same recovery, with no launch between, removes it.
#[test]
fn a_create_the_service_accepted_lands_after_its_release() {
    an_accepted_create("", &[]);
}

/// As a launch's container, its boundary verify's, which `run --rm` removes
/// only once it has started. It grants nothing, so it's removed as it is.
#[test]
fn a_verify_the_service_accepted_lands_after_its_release() {
    an_accepted_create("-verify", &["--rm"]);
}

/// The create of launch `fv_<id>`'s container `fv_<id><suffix>`, with
/// `options`, held by its service until after the release of its record.
fn an_accepted_create(suffix: &str, options: &[&str]) {
    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root, true);
    let evidence = Evidence(
        run.run_dir
            .join(format!("accepted-create{suffix}.evidence")),
    );
    let image = "localhost/connector-vmm-tests-empty:latest";
    let empty = format!("{}/empty.tar", api.dir);
    // An archive of nothing: two zeroed blocks, padded to a record.
    std::fs::write(&empty, vec![0u8; 10240]).unwrap();
    let imported = api.podman(&["import", &empty, image]);
    assert!(
        imported.status.success(),
        "importing {image}: {}",
        String::from_utf8_lossy(&imported.stderr)
    );
    let recoverer = launcher::root();
    launcher::remote(&recoverer, &api.url);
    let recovering = vmm_at(&recoverer.podman, &root.state_dir, &run.vmm_image);
    let listed = |name: &str| {
        let listed = api.podman(&["ps", "--all", "--format", "{{.Names}}"]);
        assert!(listed.status.success(), "listing through {}", api.url);
        String::from_utf8_lossy(&listed.stdout)
            .lines()
            .any(|line| line == name)
    };

    // The launch's record, held as its live owner holds it.
    let name = format!("fv_{}", run::random_hex());
    let container = format!("{name}{suffix}");
    let token = format!("{}{}", run::random_hex(), run::random_hex());
    let mounts = launcher::mounts(&root);
    let record = launcher::record(&root.state_dir, &name);
    std::fs::write(
        &record,
        format!(
            "{}\n",
            serde_json::json!({
                "version": 1,
                "id": name,
                "token": token,
                "mount": format!("{mounts}/mount-{name}"),
            })
        ),
    )
    .unwrap();
    let owner = std::fs::File::open(&record).unwrap();
    owner.lock().unwrap();

    let lock = format!("{}/store/vfs-containers/containers.lock", api.dir);
    assert!(launcher::path_exists_as_root(&lock), "{lock}");
    let gate = StoreGate::hold(&lock);
    // Its create, by a client which writes its own PID first.
    let client_pid = format!("{}/client.pid", root.dir);
    let mut client = std::process::Command::new("sudo")
        .args([
            "-n",
            "sh",
            "-c",
            r#"echo $$ >"$1"; shift; exec "$@""#,
            "client",
        ])
        .args([
            &client_pid,
            "podman",
            "--remote",
            "--url",
            &api.url,
            "create",
        ])
        .args(options)
        .args(["--network=none", &format!("--name={container}")])
        .arg(format!("--label=dev.estuary.vmm-owner={token}"))
        .arg(format!(
            "--label=dev.estuary.vmm-state-dir={}",
            root.state_dir
        ))
        .args([image, "true"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("sending the create");
    let waiter = run::eventually("the service to wait on the gate", GATE, || {
        gate.waiter(&api.url)
    });
    assert!(!listed(&container), "the create hasn't landed");

    let client_pid = run::eventually("the client's PID", GATE, || {
        std::fs::read_to_string(&client_pid)
            .ok()
            .and_then(|pid| pid.trim().parse::<u32>().ok())
    });
    run::sudo(&["kill", "-KILL", &client_pid.to_string()]);
    client.wait().expect("waiting for the client's sudo");
    assert_eq!(launcher::start_time(client_pid), None, "the client is gone");
    std::mem::drop(owner);
    evidence.note(format_args!(
        "service process {waiter} waits on {lock} (inode {}) for {container}'s create; its \
         client {client_pid} was killed and the record let go",
        gate.inode
    ));

    let recovery = recovering.spawn_recovery(runtime.handle());
    let first = run::eventually("the first pass to end", PASSED, || {
        let metrics = connector::Vmm::recovery_metrics();
        (metrics.last_success != 0).then_some(metrics)
    });
    assert_eq!((first.failed_passes, first.pending), (0, 0));
    assert!(
        !launcher::path_exists(&record),
        "released by the first pass"
    );
    assert_eq!(
        gate.waiter(&api.url),
        Some(waiter),
        "the create still waits, once the pass has ended"
    );
    assert!(!listed(&container), "and hasn't landed");
    evidence.note(format_args!(
        "the first pass released {record} and ended at {}; the service still waits",
        first.last_success
    ));

    std::mem::drop(gate);
    run::eventually("the accepted create to land", GATE, || {
        listed(&container).then_some(())
    });
    // The next pass starts 30 seconds after the first ended.
    assert_eq!(connector::Vmm::recovery_metrics(), first, "no pass since");
    evidence.note(format_args!("{container} landed once the gate was let go"));

    run::eventually("a later pass to remove it", PASSED, || {
        (!listed(&container)).then_some(())
    });
    std::mem::drop(recovery);
    let calls = launcher::calls(&recoverer);
    assert!(
        calls.contains(&format!("rm --force --time=0 --ignore {container}")),
        "{calls:#?}"
    );
    assert!(
        !calls
            .iter()
            .any(|call| call.starts_with("container inspect")),
        "it was not taken for a grant: {calls:#?}"
    );
    assert!(!launcher::path_exists(&record), "nor recorded");
    assert!(
        !calls
            .iter()
            .any(|call| call.starts_with("pull ") || call.starts_with("create ")),
        "with no launch: {calls:#?}"
    );
    evidence.note(format_args!(
        "a later pass removed {container}, with no launch"
    ));
}
