//! The connector service's VMM launches, through the actual launcher. Each
//! test drives an in-process `connector::Service` whose VMM configuration
//! names this run's images, and a state directory, TMPDIR and podman of the
//! test's own (`connector_vmm_tests::launcher`). Recovery tests also launch
//! in other processes, which they kill as a crash would: this same binary,
//! running only `owner_process` (see `Owner`). Like the rest of the suite,
//! these run only under `mise run ci:connector-vmm-kvm`.

mod replacement;
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
        memory_bytes: 1280 << 20,
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
        barrier,
        egress,
    } = serde_json::from_str(config.to_str().expect("UTF-8")).expect("an owner's configuration");
    let execution = flow::ConnectorExecution {
        vmm: true,
        egress: egress.map(|hosts| flow::connector_execution::Egress { hosts }),
    };

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
        let config = serde_json::to_string(&OwnerConfig {
            podman: root.podman.clone(),
            state_dir: state_dir.to_string(),
            vmm_image: vmm_image.to_string(),
            image: run.derive_python_image.clone(),
            barrier: barrier.map(ToString::to_string),
            egress: egress.map(|hosts| hosts.iter().map(ToString::to_string).collect()),
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
            "ps --all",
            "network ls",
            "volume ls",
            format!("pull {}", run.derive_python_image).as_str(),
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

/// An owner killed while its podman, held at `step`, is about to create its
/// `kind`, as `exists` finds it. The command goes on without its owner, whose
/// record is then free. A launch meanwhile takes the record, finds nothing
/// made yet, and releases it. Let go, the command makes a network or volume
/// that no record names, and the next launch removes it by its state
/// directory's label. A container's create fails instead, because the
/// release removed its network and volume.
fn an_orphaned_command(step: &str, kind: &str, exists: fn(&str) -> bool, makes: bool) {
    let (run, root, runtime) = setup();
    let other = launcher::root();
    let successor = router(vmm_at(&other.podman, &root.state_dir, &run.fake_image));

    let hold = launcher::hold(&root, step);
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.fake_image, false);
    launcher::wait_held(&root, step, STARTED);
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    owner.kill();

    launch(&runtime, &successor, &run.derive_python_image);
    assert!(!launcher::path_exists(&record), "released meanwhile");
    assert!(!exists(&name), "nothing was made yet");

    std::mem::drop(hold);
    run::eventually("the orphaned command to end", STARTED, || {
        wrappers(&root).is_empty().then_some(())
    });
    assert_eq!(exists(&name), makes, "the {kind}, once the command ended");

    launch(&runtime, &successor, &run.derive_python_image);
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, &name),
        Vec::<String>::new(),
        "removed by the launch, before any cleanup of the test's"
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

/// A podman service may create a launch's network or volume after its
/// release removed the record. A launch removes what carries its state
/// directory's label with no record there. A held record keeps its pair,
/// whatever it holds, and another state directory's pair is not looked at.
#[test]
fn labeled_resources_without_a_record() {
    let (run, root, runtime) = setup();
    let router = router(vmm(&root, &run.fake_image));
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

    launch(&runtime, &router, &run.derive_python_image);

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

/// Where a launch recovering a dead owner's VMM is killed.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Gate {
    /// Its removal of the container is held before podman runs it, and runs
    /// once the next launch has recovered the record.
    BeforeContainerRemoval,
    /// Its removal of the container has finished.
    AfterContainerRemoval,
    /// Its removal of the network is held before podman runs it, and runs
    /// once the next launch has recovered the record.
    BeforeNetworkRemoval,
    /// As before, but the orphaned removal runs before the next launch.
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

/// A launch recovering a dead owner's serving VMM is killed with SIGKILL at
/// `gate`, beside a live owner sharing its state directory and resources no
/// record names. The next launch releases everything the dead owner's record
/// names, removing the record last, and touches nothing else.
fn a_kill_of_recovery(gate: Gate) {
    use std::os::unix::process::ExitStatusExt;

    let (run, root, runtime) = setup();
    let (neighbour, recoverer, other) = (launcher::root(), launcher::root(), launcher::root());
    let successor = router(vmm_at(&other.podman, &root.state_dir, &run.fake_image));
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
    let recovering = Owner::spawn(&run, &recoverer, &root.state_dir, &run.fake_image, false);
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
        "the recovering launch called nothing else, so far"
    );
    assert!(launcher::locked(&record), "the recovering launch holds it");
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
        "held {what}: podman {wrapper} ({cmdline}) wrote {held:?}; recovering launch {} \
         holds the record; remaining {at_gate:?}",
        recovering.host_pid
    ));

    let recovering_pid = recovering.host_pid;
    let killed = recovering.kill();
    assert_eq!(killed.signal(), Some(libc::SIGKILL));
    assert_eq!(
        launcher::start_time(u32::try_from(recovering_pid).expect("a PID")),
        None,
        "the recovering launch is gone"
    );
    assert_eq!(
        launcher::start_time(wrapper),
        Some(wrapper_started.clone()),
        "the held podman outlives its launcher"
    );
    let reparented = parent(wrapper);
    assert!(
        !launcher::locked(&record),
        "the record's lock went with the recovering launch"
    );
    assert_eq!(
        launcher::remaining(&footprint),
        at_gate,
        "the kill changed nothing"
    );
    evidence.note(format_args!(
        "recovering launch {recovering_pid} {killed} after {:?} held; podman {wrapper} lives \
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
        launch(&runtime, &successor, &run.derive_python_image);
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
        launch(&runtime, &successor, &run.derive_python_image);
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
    assert_eq!(successor_calls, looked, "the next launch's release");
    assert_eq!(launcher::remaining(&footprint), Vec::<String>::new());
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, &name),
        Vec::<String>::new()
    );
    launcher::assert_whole(&peer_footprint);
    evidence.note(format_args!(
        "peer after the next launch's recovery: {}",
        served(&runtime, &root.state_dir, &peer_name)
    ));
    sentinels.assert_intact();
    assert_eq!(
        launcher::leftovers(&recoverer, None),
        Vec::<String>::new(),
        "the killed launch made nothing of its own"
    );
    let replacement = launcher::vmm_name(&other).expect("the launch named its network");
    assert_eq!(
        launcher::left(&root.state_dir, &other.tmp_dir, &replacement),
        Vec::<String>::new()
    );
    evidence.note(format_args!(
        "next launch released {name} with {successor_calls:?}; remaining {:?}; peer whole; \
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
    launch(&runtime, &successor, &run.derive_python_image);
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

/// Bounds the eight launches, from the barrier's opening.
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
    /// The calls the launch which takes its record makes to release it.
    release: Vec<String>,
    /// Kept by the test until the race, so that no launch made meanwhile
    /// recovers it first.
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

/// Eight launchers start at once beneath one state directory, beside a
/// serving VMM and orphans left at four stages: a claimed record and its
/// directories only, a network, a created container, and a serving VMM.
/// Each launcher's first `ps` is held, so the four which take an orphan's
/// record wait holding its lock while the other four, finding every orphan
/// locked, must launch past them. Each orphan is released by exactly the
/// launch which took it, every launch starts, and the peer serves
/// throughout. The eight and the orphans other than the serving VMM run the
/// fake image; the peer and the serving orphan are real VMMs.
#[test]
fn eight_launches_recover_shared_orphans() {
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

    // The serving orphan first: each later orphan's launch passes over the
    // earlier ones, which the test holds locked.
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
            "{}'s launch recovered nothing",
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
    let launchers: Vec<launcher::Root> = (0..8).map(|_| launcher::root()).collect();
    let orphan_tokens: Vec<&str> = orphans.iter().map(|orphan| orphan.token.as_str()).collect();
    let holds: Vec<launcher::Hold> = launchers
        .iter()
        .map(|root| launcher::hold_naming(root, "ps", &orphan_tokens))
        .collect();
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
    for owner in &owners {
        owner.report("waiting", STARTED);
    }

    // The starting state, with only the eight about to launch.
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
        let gone: Vec<u128> = owners
            .iter()
            .map(|owner| owner.report("go", GROUP).parse().expect("a time"))
            .collect();
        evidence.note(format_args!(
            "barrier opened at {opened}; the eight went at {gone:?}, within {} ms",
            gone.iter().max().unwrap() - gone.iter().min().unwrap()
        ));

        let held = |root: &launcher::Root| launcher::path_exists(&format!("{}/held-ps", root.dir));
        run::eventually("four launches to hold an orphan each", GROUP, || {
            (launchers.iter().filter(|root| held(root)).count() == orphans.len()).then_some(())
        });
        let mut taken = std::collections::BTreeMap::new();
        for (i, root) in launchers.iter().enumerate() {
            if !held(root) {
                owners[i].report("started", GROUP);
                evidence.note(format_args!("launch {i} started, past every orphan"));
            }
        }
        // Everything below is observed while the four still hold.
        for (i, root) in launchers.iter().enumerate() {
            let calls = launcher::calls(root);
            let named: Vec<&Orphan> = orphans
                .iter()
                .filter(|o| calls.iter().any(|call| call.contains(&o.token)))
                .collect();
            if held(root) {
                let [orphan] = &named[..] else {
                    panic!("launch {i} holds one orphan, not {calls:?}");
                };
                assert_eq!(
                    calls,
                    orphan.release[..1],
                    "launch {i} is held at its first call"
                );
                assert!(
                    launcher::locked(&orphan.record),
                    "launch {i} holds {}",
                    orphan.stage
                );
                assert!(
                    taken.insert(orphan.stage, i).is_none(),
                    "{} taken once",
                    orphan.stage
                );
            } else {
                assert!(named.is_empty(), "launch {i} touched no orphan: {calls:?}");
            }
        }
        assert_eq!(
            taken.len(),
            orphans.len(),
            "every orphan is held by one launch"
        );
        launcher::assert_whole(&serving_footprint);
        launcher::assert_whole(&peer_footprint);
        evidence.note(format_args!(
            "while held: orphan stage -> launch {taken:?}, each record locked, the serving \
             orphan whole; {}",
            served(&runtime, &state_dir, &peer_name)
        ));

        std::mem::drop(holds);
        for i in taken.values() {
            owners[*i].report("started", GROUP);
            evidence.note(format_args!(
                "launch {i} started, having released its orphan"
            ));
        }
        stop.store(true, Ordering::Relaxed);
        (checker.join().expect("the peer's service checks"), taken)
    });
    let (checks, taken) = checks;
    evidence.note(format_args!(
        "all eight started; the peer answered all {} checks through the race",
        checks.len()
    ));
    for check in &checks {
        evidence.note(format_args!("peer check at {check}"));
    }

    // Each orphan was released by the launch which took it, and only that.
    for orphan in &orphans {
        let i = taken[orphan.stage];
        let calls = launcher::calls(&launchers[i]);
        let n = orphan.release.len();
        assert_eq!(
            calls[..n],
            orphan.release[..],
            "launch {i} released {}",
            orphan.stage
        );
        // Its recovery then lists what carries the state directory's label.
        // A concurrent release may leave it a resource to remove meanwhile.
        let pull = calls
            .iter()
            .position(|call| call.starts_with("pull "))
            .unwrap_or_else(|| panic!("launch {i} then launched: {calls:?}"));
        let labeled = format!("--filter label=dev.estuary.vmm-state-dir={state_dir} ");
        assert_eq!(
            calls[n..pull]
                .iter()
                .filter(|call| call.contains(&labeled))
                .count(),
            3,
            "launch {i} listed by its state directory, then launched: {calls:?}"
        );
        for (j, root) in launchers.iter().enumerate() {
            assert!(
                j == i
                    || !launcher::calls(root)
                        .iter()
                        .any(|call| call.contains(&orphan.token)),
                "only launch {i} released {}",
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
        "orphans released as expected; the eight serve {:?}; peer whole; sentinels intact; {}",
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
    let successor = router(vmm_at(&other.podman, &state_dir, &run.fake_image));
    launch(&runtime, &successor, &run.derive_python_image);
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
    let roots = launchers
        .iter()
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
        "the eight and the peer ended and were measured gone; a last recovery left state \
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
    let recovering = router(vmm_at(&recoverer.podman, &root.state_dir, &run.vmm_image));
    let task = {
        let router = recovering.clone();
        let request = spec(&run.derive_python_image);
        runtime.spawn(async move {
            let logs = Logs::default();
            let logger = |log: &ops::Log| logs.push(log);
            tokio::time::timeout(
                STARTED,
                proto_grpc::connector::start(
                    &router,
                    &logger,
                    proto_grpc::connector::SPEC_TASK_NAME,
                    request,
                ),
            )
            .await
            .expect("the recovering launch to finish")
            .unwrap_or_else(|err| panic!("launching: {err:#}\n{:#?}", logs.messages()))
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

    let (request_tx, mut response_rx, started_response) = runtime.block_on(task).unwrap();
    assert_started(&started_response);
    // The stall stays installed: only the launcher's timeout can end the command.
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
    drop(request_tx);
    runtime.block_on(async {
        tokio::time::timeout(RELEASED, async {
            while response_rx.recv().await.is_some() {}
        })
        .await
        .expect("the session to end");
    });

    drop(control);
    launch(&runtime, &recovering, &run.derive_python_image);
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
    for name in launcher::calls(&recoverer).iter().filter_map(|call| {
        call.strip_prefix("network create ")
            .and_then(|args| args.rsplit(' ').next())
    }) {
        assert_eq!(
            launcher::left(&root.state_dir, &recoverer.tmp_dir, name),
            Vec::<String>::new()
        );
    }
    assert_eq!(wrappers(&recoverer), Vec::<u32>::new());
    sentinels.remove();
}
