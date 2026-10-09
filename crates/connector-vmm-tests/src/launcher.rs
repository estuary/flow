//! Per-test state and podman wrapper for launcher tests.

use crate::run;
use std::path::Path;
use std::time::Duration;

pub struct Root {
    pub dir: String,
    pub state_dir: String,
    pub tmp_dir: String,
    pub podman: String,
}

/// Recorded before it exists, created by root so that it can sit in the
/// run's root-owned directory, and handed to the unprivileged test.
pub fn root() -> Root {
    let dir = format!("{}/launcher-{}", run::ROOT, run::random_hex());
    run::record("dir", &dir);
    // SAFETY: both take nothing and cannot fail.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    run::sudo(&[
        "install",
        "-d",
        "-o",
        &uid.to_string(),
        "-g",
        &gid.to_string(),
        "-m",
        "0755",
        &dir,
    ]);

    let state_dir = format!("{dir}/state");
    let tmp_dir = format!("{dir}/tmp");
    for dir in [&state_dir, &tmp_dir] {
        std::fs::create_dir(dir).unwrap_or_else(|e| panic!("creating {dir}: {e}"));
    }
    let podman = format!("{dir}/podman");
    for (fixture, path) in [
        ("podman.sh", podman.clone()),
        ("gate.py", format!("{dir}/gate.py")),
    ] {
        std::fs::copy(run::fixture(fixture), &path)
            .unwrap_or_else(|e| panic!("copying {fixture} to {path}: {e}"));
    }
    std::fs::set_permissions(&podman, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .unwrap_or_else(|e| panic!("making {podman} executable: {e}"));

    Root {
        dir,
        state_dir,
        tmp_dir,
        podman,
    }
}

pub fn calls(root: &Root) -> Vec<String> {
    match std::fs::read_to_string(format!("{}/calls", root.dir)) {
        Ok(calls) => calls.lines().map(ToString::to_string).collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("reading the calls of {}: {e}", root.dir),
    }
}

pub fn vmm_name(root: &Root) -> Option<String> {
    calls(root).iter().find_map(|call| {
        call.strip_prefix("network create ")
            .and_then(|args| args.rsplit(' ').next())
            .map(ToString::to_string)
    })
}

/// Holds the first call of `step` of the test's podman until dropped. Other
/// calls of the step pass, so that a hold catches only the launch awaited.
pub struct Hold {
    dir: String,
    path: String,
    /// What each call of the step claims its ordinal by: this and a number.
    taken: String,
}

pub fn hold(root: &Root, step: &str) -> Hold {
    hold_as(root, "hold", step, 1)
}

/// Holds the `nth` call of `step`, as `hold` does the first.
pub fn hold_nth(root: &Root, step: &str, nth: u32) -> Hold {
    hold_as(root, "hold", step, nth)
}

/// A hold whose waiting runs as root, beneath sudo: it, and the fence it
/// keeps as its stdin, outlive the kill of everything its launcher's user
/// can signal, as a rootful podman command does.
pub fn root_hold(root: &Root, step: &str) -> Hold {
    hold_as(root, "root-hold", step, 1)
}

fn hold_as(root: &Root, control: &str, step: &str, nth: u32) -> Hold {
    let path = format!("{}/{control}-{step}", root.dir);
    std::fs::write(&path, nth.to_string()).unwrap_or_else(|e| panic!("creating {path}: {e}"));
    Hold {
        dir: root.dir.clone(),
        path,
        taken: format!("taken-{control}-{step}-"),
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&self.taken) {
                let _ = std::fs::remove_dir(entry.path());
            }
        }
    }
}

/// While held, the test's podman creates VMM containers without `/dev/kvm`.
pub fn no_kvm(root: &Root) -> Control {
    control(root, "no-kvm", "")
}

/// While held, the test's podman runs the launcher's boundary verification
/// in network namespace `netns`, in place of the host's.
pub fn verify_in(root: &Root, netns: &str) -> Control {
    control(root, "verify-netns", netns)
}

/// A control file of the test's podman, removed when dropped.
pub struct Control {
    path: String,
}

fn control(root: &Root, name: &str, content: &str) -> Control {
    let path = format!("{}/{name}", root.dir);
    std::fs::write(&path, content).unwrap_or_else(|e| panic!("creating {path}: {e}"));
    Control { path }
}

impl Drop for Control {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Fails calls of `step` of the test's podman until dropped: every call, or
/// with `naming`, only those whose arguments contain it.
pub struct Fault {
    path: String,
}

pub fn fail(root: &Root, step: &str, naming: &str) -> Fault {
    let path = format!("{}/fail-{step}", root.dir);
    std::fs::write(&path, naming).unwrap_or_else(|e| panic!("creating {path}: {e}"));
    Fault { path }
}

impl Drop for Fault {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Appends `flags`, test-only flags of the VMM's `run`, to every `podman
/// create` of the test's podman. The calls it records are the launcher's own.
pub fn vmm_flags(root: &Root, flags: &[String]) {
    let path = format!("{}/vmm-flags", root.dir);
    let mut content = String::new();
    for flag in flags {
        assert!(!flag.contains('\n'), "a flag is one line: {flag:?}");
        content.push_str(flag);
        content.push('\n');
    }
    std::fs::write(&path, content).unwrap_or_else(|e| panic!("creating {path}: {e}"));
}

const GATE: &str = "connector-vmm-tests: held before connector-init";

/// Guest-init waits for this shell to exit; SIGSTOP holds it indefinitely
/// before connector-init and its idle watchdog can start.
fn gate_command() -> String {
    format!("printf '%s\\n' '{GATE}' >&2 && kill -STOP $$")
}

/// Holds the test's VMMs before connector-init starts, through `vmm_flags`
/// (replacing any others). `gate.py` keeps the guest's `GATE` line from the
/// launcher, and marks the hold `readiness` with it.
pub fn gate_readiness(root: &Root) {
    vmm_flags(root, &["--as-root-exec".to_string(), gate_command()]);
    let path = format!("{}/gate-readiness", root.dir);
    std::fs::write(&path, GATE).unwrap_or_else(|e| panic!("creating {path}: {e}"));
}

pub fn wait_held(root: &Root, what: &str, timeout: Duration) {
    let path = format!("{}/held-{what}", root.dir);
    run::eventually(&format!("podman to hold {what}"), timeout, || {
        path_exists(&path).then_some(())
    });
}

/// Everything the test's one launch left behind: the container and network
/// named `name`, if it got so far as naming them, anything in the test's
/// state directory, and any connector mount in its TMPDIR.
pub fn leftovers(root: &Root, name: Option<&str>) -> Vec<String> {
    let mut left = Vec::new();

    if let Some(name) = name {
        if container_exists(name) {
            left.push(format!("container {name}"));
        }
        if network_exists(name) {
            left.push(format!("network {name}"));
        }
    }
    for entry in std::fs::read_dir(&root.state_dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", root.state_dir))
    {
        let entry = entry.unwrap_or_else(|e| panic!("reading {}: {e}", root.state_dir));
        left.push(entry.path().display().to_string());
    }
    for path in walk(Path::new(&root.tmp_dir)) {
        if path
            .file_name()
            .is_some_and(|file| file.to_string_lossy().starts_with("mount-"))
        {
            left.push(path.display().to_string());
        }
    }
    left
}

pub fn wait_released(root: &Root, name: &str, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let left = leftovers(root, Some(name));
        if left.is_empty() {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("{name} left {left:?} after {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Everything beneath `dir`, without following links. What vanishes while
/// it's read, as a teardown removes it, is gone; failing to read anything
/// else fails the test, rather than hiding what's in it.
fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let vanished = |e: &std::io::Error| e.kind() == std::io::ErrorKind::NotFound;
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if vanished(&e) => return Vec::new(),
        Err(e) => panic!("reading {}: {e}", dir.display()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()));
        let path = entry.path();
        match entry.file_type() {
            Ok(file_type) if file_type.is_dir() => paths.extend(walk(&path)),
            Ok(_) => (),
            Err(e) if vanished(&e) => continue,
            Err(e) => panic!("reading {}: {e}", path.display()),
        }
        paths.push(path);
    }
    paths
}

/// Whether `path` exists, as this process sees it. Only a missing path, or
/// one beneath something which is not a directory, is absent: anything which
/// keeps this from looking fails the test.
pub fn path_exists(path: &str) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            false
        }
        Err(e) => panic!("checking whether {path} exists: {e}"),
    }
}

/// `path_exists`, as root sees it.
pub fn path_exists_as_root(path: &str) -> bool {
    const PROBE: &str = "import os, sys
try:
    os.lstat(sys.argv[1])
except (FileNotFoundError, NotADirectoryError):
    print('absent')
else:
    print('present')
";
    let output = run::sudo_output(&["python3", "-c", PROBE, path], None);
    match (output.status.success(), output.stdout.as_slice()) {
        (true, b"present\n") => true,
        (true, b"absent\n") => false,
        _ => panic!(
            "checking as root whether {path} exists: {}\n{}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

/// Whether podman has a container whose full ID or name is `container`. A
/// listing which fails fails the test.
pub fn container_exists(container: &str) -> bool {
    run::podman(&[
        "ps",
        "--all",
        "--no-trunc",
        "--format",
        "{{.ID}} {{.Names}}",
    ])
    .lines()
    .any(|line| line.split(' ').any(|word| word == container))
}

/// Whether podman has a network named `name`. A listing which fails fails
/// the test.
pub fn network_exists(name: &str) -> bool {
    run::podman(&["network", "ls", "--format", "{{.Name}}"])
        .lines()
        .any(|line| line == name)
}

/// What remains of launch `name` alone, among others which share its state
/// directory and TMPDIR: its container, network, state, record and mount.
pub fn left(state_dir: &str, tmp_dir: &str, name: &str) -> Vec<String> {
    let mut left = Vec::new();

    if container_exists(name) {
        left.push(format!("container {name}"));
    }
    if network_exists(name) {
        left.push(format!("network {name}"));
    }
    for path in [format!("{state_dir}/{name}"), record(state_dir, name)] {
        if path_exists(&path) {
            left.push(path);
        }
    }
    let mount = format!("mount-{name}");
    for path in walk(Path::new(tmp_dir)) {
        if path.file_name().is_some_and(|file| file == mount.as_str()) {
            left.push(path.display().to_string());
        }
    }
    left
}

pub fn record(state_dir: &str, name: &str) -> String {
    format!("{state_dir}/{name}.owner")
}

/// Whether anyone holds the lock of the record at `path`, as its owner, a
/// command it fenced, or a releaser does.
pub fn locked(path: &str) -> bool {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("opening {path}: {e}"));
    match file.try_lock() {
        Ok(()) => false,
        Err(std::fs::TryLockError::WouldBlock) => true,
        Err(std::fs::TryLockError::Error(e)) => panic!("locking {path}: {e}"),
    }
}

pub fn token(path: &str) -> String {
    let content = std::fs::read(path).unwrap_or_else(|e| panic!("reading {path}: {e}"));
    let claim = content.split(|b| *b == b'\n').next().unwrap_or_default();
    let claim: serde_json::Value =
        serde_json::from_slice(claim).unwrap_or_else(|e| panic!("parsing {path}'s claim: {e}"));
    claim["token"]
        .as_str()
        .unwrap_or_else(|| panic!("{path}'s claim has no token"))
        .to_string()
}

/// Everything the host holds for a running launch, beyond what `left` sees:
/// the namespace, processes, cgroup, storage layer and links of its container.
/// The VMM's scratch disk is an unnamed file it holds open, so it goes with
/// the VMM's process; the guest's tap is in the container's namespace.
#[derive(Debug)]
pub struct Footprint {
    pub name: String,
    pub id: String,
    pub netns: String,
    /// The container's first process (the VMM) and its conmon, each with its
    /// start time, so that a reused PID isn't taken for the process.
    pub processes: Vec<(String, u32, String)>,
    pub cgroup: String,
    pub layer: String,
    pub bridge: String,
    /// Host ends of the bridge's veths, by name and index.
    pub veths: Vec<(String, String)>,
    pub paths: Vec<String>,
}

pub fn footprint(state_dir: &str, tmp_dir: &str, name: &str) -> Footprint {
    let inspected = run::podman(&[
        "container",
        "inspect",
        "--format",
        "{{.Id}} {{.NetworkSettings.SandboxKey}} {{.State.Pid}} {{.State.ConmonPid}} {{.GraphDriver.Data.UpperDir}}",
        name,
    ]);
    let [id, netns, pid, conmon, layer] = inspected.split_whitespace().collect::<Vec<_>>()[..]
    else {
        panic!("inspecting {name}: {inspected:?}");
    };
    let pid: u32 = pid
        .parse()
        .unwrap_or_else(|e| panic!("{name}'s pid {pid:?}: {e}"));
    let conmon: u32 = conmon
        .parse()
        .unwrap_or_else(|e| panic!("{name}'s conmon pid {conmon:?}: {e}"));

    let cgroup = run::sudo(&["cat", &format!("/proc/{pid}/cgroup")]);
    let cgroup = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap_or_else(|| panic!("{name}'s VMM is in no unified cgroup: {cgroup:?}"));

    let bridge = format!("fvm{}", &name[3..15]);
    let veths = links(&["-o", "link", "show", "master", &bridge])
        .into_iter()
        .filter(|(link, _)| link.starts_with("veth"))
        .collect();

    let mut paths = vec![format!("{state_dir}/{name}"), record(state_dir, name)];
    paths.extend(
        left(state_dir, tmp_dir, name)
            .into_iter()
            .filter(|path| path.ends_with(&format!("/mount-{name}"))),
    );

    let footprint = Footprint {
        name: name.to_string(),
        id: id.to_string(),
        netns: netns.to_string(),
        processes: vec![
            (
                "VMM".to_string(),
                pid,
                start_time(pid).expect("the VMM runs"),
            ),
            (
                "conmon".to_string(),
                conmon,
                start_time(conmon).expect("conmon runs"),
            ),
        ],
        cgroup: format!("/sys/fs/cgroup{cgroup}"),
        layer: layer.to_string(),
        bridge,
        veths,
        paths,
    };
    assert!(
        !footprint.veths.is_empty() && footprint.paths.len() == 3,
        "a running launch has a veth, state, record and mount: {footprint:#?}"
    );
    assert_whole(&footprint);
    footprint
}

pub fn assert_whole(footprint: &Footprint) {
    let present = remaining(footprint);
    assert_eq!(
        present.len(),
        8 + footprint.veths.len() + footprint.paths.len(),
        "all of {footprint:#?} remains, not just {present:#?}"
    );
}

pub fn remaining(footprint: &Footprint) -> Vec<String> {
    let Footprint {
        name,
        id,
        netns,
        processes,
        cgroup,
        layer,
        bridge,
        veths,
        paths,
    } = footprint;
    let mut remaining = Vec::new();

    if container_exists(id) {
        remaining.push(format!("container {id}"));
    }
    if network_exists(name) {
        remaining.push(format!("network {name}"));
    }
    for (what, path) in [("netns", netns), ("cgroup", cgroup), ("layer", layer)] {
        if path_exists_as_root(path) {
            remaining.push(format!("{what} {path}"));
        }
    }
    for (what, pid, started) in processes {
        if start_time(*pid).as_ref() == Some(started) {
            remaining.push(format!("{what} process {pid}"));
        }
    }
    let present = links(&["-o", "link", "show"]);
    if present.iter().any(|(link, _)| link == bridge) {
        remaining.push(format!("bridge {bridge}"));
    }
    for veth in veths {
        if present.contains(veth) {
            remaining.push(format!("veth {} (index {})", veth.0, veth.1));
        }
    }
    for path in paths {
        if path_exists(path) {
            remaining.push(path.clone());
        }
    }
    remaining
}

/// A process's start time in clock ticks, field 22 of its `stat`, or None
/// if it has gone. Any other failure to read it fails the test.
pub fn start_time(pid: u32) -> Option<String> {
    let path = format!("/proc/{pid}/stat");
    let stat = match std::fs::read_to_string(&path) {
        Ok(stat) => stat,
        // Gone, or exiting as it was read.
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.raw_os_error() == Some(libc::ESRCH) =>
        {
            return None;
        }
        Err(e) => panic!("reading {path}: {e}"),
    };
    // The command name may hold spaces; the fields after it don't.
    let started = stat
        .rsplit_once(')')
        .and_then(|(_, fields)| fields.split_whitespace().nth(19));
    match started {
        Some(started) => Some(started.to_string()),
        None => panic!("parsing {path}: {stat:?}"),
    }
}

/// `ip`'s one-line links, as name and index. An `ip` which fails, or a line
/// it prints which isn't a link, fails the test.
fn links(args: &[&str]) -> Vec<(String, String)> {
    let output = std::process::Command::new("ip")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("running ip {}: {e}", args.join(" ")));
    if !output.status.success() {
        panic!(
            "ip {}: {}\n{}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| {
            let mut fields = line.split(": ");
            let index = fields.next().map(str::trim).filter(|i| !i.is_empty());
            let link = fields.next().and_then(|link| link.split('@').next());
            match (index, link) {
                (Some(index), Some(link)) => (link.to_string(), index.to_string()),
                _ => panic!("ip {}: unexpected line {line:?}", args.join(" ")),
            }
        })
        .collect()
}

#[cfg(test)]
mod test {
    /// A path is absent only if it was seen to be.
    #[test]
    fn paths_are_present_absent_or_unknown() {
        use std::os::unix::fs::PermissionsExt;

        let dir =
            std::env::temp_dir().join(format!("connector-vmm-tests-{}", crate::run::random_hex()));
        let file = dir.join("file");
        let sealed = dir.join("sealed");
        std::fs::create_dir_all(&sealed).unwrap();
        std::fs::write(&file, "").unwrap();
        let path = |path: std::path::PathBuf| path.to_str().unwrap().to_string();

        assert!(super::path_exists(&path(file.clone())));
        assert!(!super::path_exists(&path(dir.join("missing"))));
        assert!(!super::path_exists(&path(file.join("beneath"))));

        // Root looks inside a sealed directory regardless.
        // SAFETY: geteuid takes nothing and cannot fail.
        if unsafe { libc::geteuid() } != 0 {
            let inside = path(sealed.join("inside"));
            std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o000)).unwrap();
            let looked = std::panic::catch_unwind(|| super::path_exists(&inside));
            std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o700)).unwrap();
            assert!(looked.is_err(), "{inside} is neither present nor absent");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn processes_run_or_are_gone() {
        assert!(super::start_time(std::process::id()).is_some());
        // Above any pid_max.
        assert_eq!(super::start_time(u32::MAX), None);
    }

    #[test]
    fn the_gate_announces_then_stops() {
        use std::io::Read;

        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", &super::gate_command()])
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id() as libc::pid_t;
        let mut status = 0;
        // SAFETY: `status` is the int waitpid writes. WUNTRACED returns once
        // the child stops, as well as if it exits.
        let waited = unsafe { libc::waitpid(pid, &mut status, libc::WUNTRACED) };
        let stopped = waited == pid && libc::WIFSTOPPED(status);

        child.kill().unwrap();
        child.wait().unwrap();
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        assert_eq!((stopped, stderr), (true, format!("{}\n", super::GATE)));
    }
}
