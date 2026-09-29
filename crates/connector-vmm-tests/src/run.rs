//! The run the task prepared, and the VMM containers a test launches in it.
//!
//! Every podman call and every touch of the state directory goes through
//! `sudo -n`; the test process itself stays unprivileged. Anything a test is
//! about to create is appended to the task's resources list first, so that a
//! test killed before its own cleanup runs still leaves nothing behind.

use std::io::{Read, Write};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Created by the task, which holds its lock and owns `run.json` and
/// `resources` inside it. Short, because `state/fv_<16 hex>/sock/init.sock`
/// beneath it has to fit a Unix socket address.
pub const ROOT: &str = "/var/tmp/connector-vmm-kvm";

/// Where the test guest image keeps the probe server.
pub const PROBES: &[&str] = &["/usr/local/bin/python", "/probes.py"];

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Run {
    pub run_dir: PathBuf,
    pub vmm_image: String,
    pub guest_image: String,
    pub hello_world_image: String,
    pub connector_init: PathBuf,
    pub endpoints: Vec<Ipv4Addr>,
}

/// What a test launches. `Spec::probes` is the common case.
pub struct Spec<'a> {
    pub policy: &'a str,
    pub connector_image: String,
    pub memory_mib: u32,
    pub disk_mib: u64,
    /// The persistent disk's guest path; its host directory is created empty
    /// and writable by any guest user.
    pub persistent_disk: Option<&'a str>,
    pub task_update: Option<&'a str>,
    pub rm: bool,
    pub flags: Vec<String>,
    pub exec: Option<Vec<String>>,
    /// Changes to the reference line itself, for the tests of its guards.
    pub edit: fn(&mut Vec<String>),
}

impl<'a> Spec<'a> {
    pub fn probes(run: &Run, policy: &'a str) -> Self {
        let mut exec: Vec<String> = PROBES.iter().map(ToString::to_string).collect();
        exec.push("serve".to_string());

        Spec {
            policy,
            connector_image: run.guest_image.clone(),
            memory_mib: 512,
            disk_mib: 256,
            persistent_disk: None,
            task_update: None,
            rm: true,
            flags: Vec::new(),
            exec: Some(exec),
            edit: |_| {},
        }
    }
}

#[derive(Default)]
pub struct Captured {
    pub bytes: Vec<u8>,
    /// When a line beginning with a space first appeared: the readiness
    /// marker, even before its line is complete.
    pub ready: Option<Instant>,
}

/// One VMM container, removed with everything staged for it on drop. A test
/// that panics also gets the container's output printed on the way out.
pub struct Vmm {
    pub name: String,
    pub state_dir: String,
    pub connector_mount: String,
    pub persistent_dir: Option<String>,
    pub argv: Vec<String>,
    pub started: Instant,
    pub stderr: Arc<Mutex<Captured>>,
    pub stdout: Arc<Mutex<Captured>>,
    child: std::process::Child,
}

pub fn load() -> Run {
    if std::env::var_os("CONNECTOR_VMM_KVM").is_none_or(|value| value.is_empty()) {
        panic!(
            "the KVM suite runs only under `mise run ci:connector-vmm-kvm`, \
             which sets CONNECTOR_VMM_KVM"
        );
    }
    let path = Path::new(ROOT).join("run.json");
    let content = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "reading {}: {e}; the suite runs under `mise run ci:connector-vmm-kvm`",
            path.display()
        )
    });
    serde_json::from_slice(&content).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

/// Append to the task's resources list. Called before the resource exists.
pub fn record(kind: &str, value: &str) {
    let path = Path::new(ROOT).join("resources");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("opening {}: {e}", path.display()));
    // One write per line, so concurrent tests' lines never interleave.
    file.write_all(format!("{kind} {value}\n").as_bytes())
        .unwrap_or_else(|e| panic!("appending to {}: {e}", path.display()));
}

pub fn sudo(args: &[&str]) -> String {
    let output = sudo_output(args, None);
    if !output.status.success() {
        panic!(
            "sudo {}: {}\n{}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout).expect("command output is UTF-8")
}

pub fn sudo_output(args: &[&str], stdin: Option<&[u8]>) -> std::process::Output {
    let mut child = std::process::Command::new("sudo")
        .arg("-n")
        .args(args)
        .stdin(match stdin {
            Some(_) => std::process::Stdio::piped(),
            None => std::process::Stdio::null(),
        })
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawning sudo {}: {e}", args.join(" ")));

    if let Some(content) = stdin {
        let mut pipe = child.stdin.take().expect("stdin is piped");
        pipe.write_all(content).expect("writing to sudo's stdin");
    }
    child
        .wait_with_output()
        .unwrap_or_else(|e| panic!("waiting for sudo {}: {e}", args.join(" ")))
}

pub fn sudo_write(path: &str, content: &[u8], mode: &str) {
    let output = sudo_output(&["tee", path], Some(content));
    assert!(
        output.status.success(),
        "writing {path}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    sudo(&["chmod", mode, path]);
}

pub fn podman(args: &[&str]) -> String {
    let mut argv = vec!["podman"];
    argv.extend_from_slice(args);
    sudo(&argv)
}

pub fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(relative)
}

/// Stage the per-connector directory and connector mount as a launcher
/// would, then run the reference line with `spec`'s additions.
pub fn start(run: &Run, spec: Spec) -> Vmm {
    let name = format!("fv_{}", random_hex());
    let state_dir = format!("{ROOT}/state/{name}");
    let connector_mount = format!("{ROOT}/connector-mounts-0/mount-{}", &name[3..]);

    record("dir", &state_dir);
    sudo(&["install", "-d", "-m", "0711", &state_dir]);
    for dir in ["init", "scratch"] {
        sudo(&["install", "-d", "-m", "0700", &format!("{state_dir}/{dir}")]);
    }
    // Traversable by the unprivileged suite, which dials init.sock in here.
    sudo(&["install", "-d", "-m", "0711", &format!("{state_dir}/sock")]);
    let policy = std::fs::read(fixture(&format!("policies/{}", spec.policy)))
        .unwrap_or_else(|e| panic!("reading policy {}: {e}", spec.policy));
    sudo_write(&format!("{state_dir}/init/policy.json"), &policy, "0444");

    record("dir", &connector_mount);
    sudo(&[
        "install",
        "-d",
        "-m",
        "0711",
        &format!("{ROOT}/connector-mounts-0"),
    ]);
    sudo(&["install", "-d", "-m", "0711", &connector_mount]);
    sudo(&[
        "install",
        "-m",
        "0555",
        &run.connector_init.to_string_lossy(),
        &format!("{connector_mount}/flow-connector-init"),
    ]);
    let inspect = podman(&["image", "inspect", &spec.connector_image]);
    sudo_write(
        &format!("{connector_mount}/image-inspect.json"),
        inspect.as_bytes(),
        "0444",
    );
    if let Some(generation) = spec.task_update {
        replace_task_update(&connector_mount, generation);
    }

    let persistent_dir = spec.persistent_disk.map(|_| {
        let dir = format!("{state_dir}/persistent");
        // Writable by the image's user without a chown, which the disk's
        // owner would read as a change to the task's data.
        sudo(&["install", "-d", "-m", "0777", &dir]);
        dir
    });

    let mut argv = crate::launch::reference(&crate::launch::Launch {
        name: &name,
        state_dir: &state_dir,
        connector_mount: &connector_mount,
        connector_image: &spec.connector_image,
        vmm_image: &run.vmm_image,
        memory_mib: spec.memory_mib,
        vcpus: 2,
        disk_mib: spec.disk_mib,
        log_level: "warn",
        persistent_disk: spec.persistent_disk.map(|guest_path| {
            (
                persistent_dir.as_deref().expect("created above"),
                guest_path,
            )
        }),
    });
    if !spec.rm {
        argv.retain(|argument| argument != "--rm");
    }
    (spec.edit)(&mut argv);
    argv.extend(spec.flags);
    if let Some(exec) = spec.exec {
        argv.push("--exec".to_string());
        argv.extend(exec);
    }

    record("container", &name);
    let mut child = std::process::Command::new("sudo")
        .arg("-n")
        .arg("podman")
        .args(&argv)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawning sudo podman run");
    let started = Instant::now();

    let stdout = capture(
        child.stdout.take().expect("stdout is piped"),
        run.run_dir.join(format!("{name}.stdout")),
    );
    let stderr = capture(
        child.stderr.take().expect("stderr is piped"),
        run.run_dir.join(format!("{name}.stderr")),
    );

    Vmm {
        name,
        state_dir,
        connector_mount,
        persistent_dir,
        argv,
        started,
        stderr,
        stdout,
        child,
    }
}

/// The runtime's protocol for a credential refresh: stage beside it, then
/// rename over it, so every generation is a new inode.
pub fn replace_task_update(connector_mount: &str, content: &str) {
    let staged = format!("{connector_mount}/.task-update.json.tmp");
    sudo_write(&staged, content.as_bytes(), "0444");
    sudo(&[
        "mv",
        "-f",
        &staged,
        &format!("{connector_mount}/task-update.json"),
    ]);
}

pub fn socket_path(vmm: &Vmm) -> PathBuf {
    PathBuf::from(format!("{}/sock/init.sock", vmm.state_dir))
}

pub fn wait_ready(vmm: &Vmm, timeout: Duration) -> Duration {
    eventually(&format!("readiness from {}", vmm.name), timeout, || {
        let ready = vmm.stderr.lock().unwrap().ready?;
        Some(ready - vmm.started)
    })
}

pub fn wait_exit(vmm: &mut Vmm, timeout: Duration) -> std::process::ExitStatus {
    let name = vmm.name.clone();
    let child = &mut vmm.child;
    eventually(&format!("{name} to exit"), timeout, || {
        child.try_wait().expect("polling podman run")
    })
}

/// The VMM's pid on the host, once podman reports one.
pub fn pid(vmm: &Vmm) -> u32 {
    eventually(
        &format!("a pid for {}", vmm.name),
        Duration::from_secs(30),
        || {
            let output = sudo_output(
                &["podman", "inspect", "--format", "{{.State.Pid}}", &vmm.name],
                None,
            );
            let pid: u32 = String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .ok()?;
            (pid > 0).then_some(pid)
        },
    )
}

pub fn inspect(name: &str, format: &str) -> String {
    podman(&["inspect", "--format", format, name])
        .trim()
        .to_string()
}

pub fn nsenter(pid: u32, args: &[&str]) -> String {
    let pid = pid.to_string();
    let mut argv = vec!["nsenter", "-t", &pid, "-n"];
    argv.extend_from_slice(args);
    sudo(&argv)
}

pub fn stderr_text(vmm: &Vmm) -> String {
    String::from_utf8_lossy(&vmm.stderr.lock().unwrap().bytes).into_owned()
}

pub fn stdout_text(vmm: &Vmm) -> String {
    String::from_utf8_lossy(&vmm.stdout.lock().unwrap().bytes).into_owned()
}

/// The launcher reads a line beginning with a space as connector readiness,
/// so the only such line may be the workload's own marker. libkrun's WARN
/// records and a connector's logs may follow it and need not be JSON.
pub fn assert_framing(vmm: &Vmm, readiness: bool) {
    let stderr = vmm.stderr.lock().unwrap();
    let marked = stderr
        .bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| line.first() == Some(&b' '))
        .count();

    assert_eq!(
        marked,
        usize::from(readiness),
        "{}: lines beginning with a space on stderr, where only the workload's \
         readiness marker may begin with one",
        vmm.name
    );
}

/// Killed outright: the VMM is the container's PID 1 and ignores SIGTERM, so
/// podman's default stop would only wait ten seconds to send SIGKILL anyway.
pub fn remove_container(name: &str) {
    let _ = sudo_output(
        &["podman", "rm", "-f", "--time", "0", "--ignore", name],
        None,
    );
}

/// Poll `probe` until it returns something or `timeout` passes.
pub fn eventually<T>(what: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        if Instant::now() > deadline {
            panic!("timed out after {timeout:?} waiting for {what}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

impl Drop for Vmm {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("{}", diagnostics(self));
        }
        remove_container(&self.name);
        // The podman client exits once its container is gone. Bounded, so a
        // wedged client cannot hang the test; sudo is root's, so no kill.
        let deadline = Instant::now() + Duration::from_secs(10);
        while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = sudo_output(
            &[
                "rm",
                "-rf",
                "--one-file-system",
                &self.state_dir,
                &self.connector_mount,
            ],
            None,
        );
    }
}

fn diagnostics(vmm: &Vmm) -> String {
    let stdout = stdout_text(vmm);
    let tail: Vec<&str> = stdout.lines().rev().take(40).collect();
    let tail: Vec<&str> = tail.into_iter().rev().collect();

    format!(
        "--- {} after {:?}\n--- argv: sudo -n podman {}\n--- stderr:\n{}\n--- stdout (last 40 lines):\n{}\n---",
        vmm.name,
        vmm.started.elapsed(),
        vmm.argv.join(" "),
        stderr_text(vmm),
        tail.join("\n"),
    )
}

fn capture(mut stream: impl Read + Send + 'static, path: PathBuf) -> Arc<Mutex<Captured>> {
    let captured = Arc::new(Mutex::new(Captured::default()));
    let shared = captured.clone();
    let mut file =
        std::fs::File::create(&path).unwrap_or_else(|e| panic!("creating {}: {e}", path.display()));

    std::thread::spawn(move || {
        let mut chunk = [0u8; 8192];
        loop {
            let read = match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(read) => read,
            };
            let _ = file.write_all(&chunk[..read]);
            let mut captured = shared.lock().unwrap();
            let before = captured.bytes.len();
            captured.bytes.extend_from_slice(&chunk[..read]);

            if captured.ready.is_none() && begins_marked_line(&captured.bytes, before) {
                captured.ready = Some(Instant::now());
            }
        }
    });
    captured
}

/// Whether a line that starts at or after `from` begins with a space.
fn begins_marked_line(bytes: &[u8], from: usize) -> bool {
    (from..bytes.len()).any(|i| bytes[i] == b' ' && (i == 0 || bytes[i - 1] == b'\n'))
}

fn random_hex() -> String {
    let mut bytes = [0u8; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .expect("reading /dev/urandom");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
