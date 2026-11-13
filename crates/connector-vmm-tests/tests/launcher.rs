//! The connector service's VMM launches, through the actual launcher. Each
//! test drives an in-process `connector::Service` whose VMM configuration
//! names this run's images, and a state directory, TMPDIR and podman of the
//! test's own (`connector_vmm_tests::launcher`). Recovery tests also launch,
//! or recover, in other processes, which they kill as a crash would: this
//! same binary, running only `owner_process` (see `Owner`). Like the rest of
//! the suite, these run only under `mise run ci:connector-vmm-kvm`.

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
            let (uid, gid) = (
                rustix::process::getuid().as_raw(),
                rustix::process::getgid().as_raw(),
            );
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
        let pid =
            rustix::process::Pid::from_raw(self.host_pid).expect("the owner reported its PID");
        rustix::process::kill_process(pid, rustix::process::Signal::KILL)
            .unwrap_or_else(|e| panic!("killing owner {}: {e}", self.host_pid));
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
            match rustix::process::Pid::from_raw(self.host_pid) {
                Some(pid) => _ = rustix::process::kill_process(pid, rustix::process::Signal::KILL),
                None => _ = self.child.kill(),
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
    assert_ne!(
        rustix::process::geteuid().as_raw(),
        0,
        "the suite runs unprivileged"
    );
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

/// Exercise stderr readiness through the API attachment used by reactors.
#[test]
fn repeated_specs_through_the_podman_api() {
    let (run, root, runtime) = setup();
    let api = launcher::Api::start(&root);
    launcher::remote(&root, &api.url);
    let router = router(vmm(&root, &run.vmm_image));

    for attempt in 0..4 {
        let logs = Logs::default();
        let started = unary(&runtime, &router, &logs, spec(&run.derive_python_image))
            .unwrap_or_else(|err| panic!("API launch {attempt}: {err:#}\n{:#?}", logs.messages()));
        assert_started(&started);
        for call in launcher::calls(&root) {
            if let Some(name) = call
                .strip_prefix("network create ")
                .and_then(|args| args.rsplit(' ').next())
            {
                assert_eq!(launcher::leftovers(&root, Some(name)), Vec::<String>::new());
            }
        }
    }
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
    /// The egress flags the launcher passed to the VMM.
    policy: Vec<String>,
    results: Vec<(String, String)>,
    /// Every session log, as `LEVEL: message`.
    messages: Vec<String>,
}

/// Launch derive-python's Spec on a `plane` service, as a task whose egress is
/// `egress`, with the guest probing `cases` before connector-init starts. The
/// launcher plans and passes the policy; this test's podman only appends the
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
    let policy = launcher::egress_flags(root, &name);
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
        [
            "--egress-host=pypi.org",
            "--egress-host=files.pythonhosted.org",
            "--egress-host=api.acmeco.example",
            "--egress-host=*.svc.acmeco.example",
            "--egress-host=leak.acmeco.example",
        ]
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
    let vmm = vmm(&root, &run.vmm_image);
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
    let router = router(vmm(&root, &run.vmm_image));
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

    let recovering = vmm(&root, &run.vmm_image);
    assert_eq!(recover(&runtime, &recovering), 1, "the network is in use");
    assert_eq!(std::fs::read(&record).unwrap(), written, "kept again");
    std::mem::drop(sidecar);
    assert_eq!(recover(&runtime, &recovering), 0);
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
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
    launcher::wait_held(&root, step, STARTED);
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let record = launcher::record(&root.state_dir, &name);
    owner.kill();

    let recovery =
        vmm_at(&other.podman, &root.state_dir, &run.vmm_image).spawn_recovery(runtime.handle());
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
    let successor = vmm_at(&other.podman, &root.state_dir, &run.vmm_image);

    let hold = launcher::hold(&root, "created");
    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
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
    let successor = vmm_at(&other.podman, &root.state_dir, &run.vmm_image);

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
    let successor = vmm_at(&other.podman, &root.state_dir, &run.vmm_image);

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
        let live = Owner::spawn(&run, at, state_dir, &run.vmm_image, false);
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
    let successor = vmm_at(&other.podman, &root.state_dir, &run.vmm_image);

    let owner = Owner::spawn(&run, &root, &root.state_dir, &run.vmm_image, false);
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
        recover(&runtime, &vmm(&root, &run.vmm_image)),
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

    assert_eq!(recover(&runtime, &vmm(&root, &run.vmm_image)), 0);

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
    let successor = vmm_at(&other.podman, &root.state_dir, &run.vmm_image);
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

fn epoch_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis()
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
    let api = launcher::Api::start(&root);
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
    let api = launcher::Api::start(&root);
    let recoverer = launcher::root();
    let recovery = remote_recovery(&recoverer, &api, &root.state_dir, &run);

    let tag = format!("localhost/connector-vmm-tests-grant:{}", run::random_hex());
    run::record("image", &tag);
    run::podman(&["tag", &run.vmm_image, &tag]);
    let name = format!("fv_{}", run::random_hex());
    let token = format!("{}{}", run::random_hex(), run::random_hex());
    let late = LateGrant::create_as(&run, &api, &root.state_dir, &name, &token, &tag);
    late.start();
    run::podman(&["tag", &run.production_vmm_image, &tag]);
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
