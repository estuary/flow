//! `fixtures/podman.sh`, the launchers' podman in privileged tests, run by
//! bash against stand-ins for sudo and podman which log what they're given.
//! Its controls must change only the calls they name, a hold must catch only
//! the first call of its step, and the calls it records must be the
//! launcher's own. Needs no KVM, podman or sudo.

use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Stands in for sudo, dropping its `-n`: a command it runs keeps this
/// process's stdin, as sudo's does.
const SUDO: &str = r#"#!/usr/bin/env bash
set -euo pipefail
stubs=$(dirname "$(readlink -f "$0")")
if [[ "${2:-} ${3:-}" == "bash -c" ]]; then
    printf 'sudo %s %s <script> %s\n' "$1" "$2 $3" "${*:5}" >>"${stubs}/log"
else
    printf 'sudo %s\n' "$*" >>"${stubs}/log"
fi
shift
exec "$@"
"#;

const PODMAN: &str = r#"#!/usr/bin/env bash
set -euo pipefail
stubs=$(dirname "$(readlink -f "$0")")
printf 'podman %s (stdin %s)\n' "$*" "$(readlink /proc/self/fd/0)" >>"${stubs}/log"
if [[ "$1" == create ]]; then
    echo 0000000000000000000000000000000000000000000000000000000000000001
fi
if [[ "$1" == start && -e "${stubs}/stderr" ]]; then
    cat "${stubs}/stderr" >&2
fi
"#;

struct Setup {
    dir: PathBuf,
    test: PathBuf,
    stubs: PathBuf,
}

fn setup() -> Setup {
    let dir = std::env::temp_dir().join(format!(
        "connector-vmm-kvm-wrapper-{}",
        connector_vmm_tests::run::random_hex()
    ));
    let test = dir.join("launcher-0");
    let stubs = dir.join("stubs");
    for path in [&test, &stubs] {
        std::fs::create_dir_all(path).unwrap();
    }
    std::fs::write(dir.join("resources"), "").unwrap();
    std::fs::write(dir.join("fence"), "").unwrap();

    let executable = |path: PathBuf, content: &[u8]| {
        std::fs::write(&path, content).unwrap();
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
    };
    executable(
        test.join("podman"),
        &std::fs::read(connector_vmm_tests::run::fixture("podman.sh")).unwrap(),
    );
    executable(stubs.join("sudo"), SUDO.as_bytes());
    executable(stubs.join("podman"), PODMAN.as_bytes());

    Setup { dir, test, stubs }
}

/// Removing the directory, its controls included, lets any call still held
/// go, so that a failed test leaves no loop behind.
impl Drop for Setup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Run the wrapper with `args`, its stdin the `fence` file, as a launcher's
/// fenced command has its record.
fn spawn(setup: &Setup, args: &[&str]) -> std::process::Child {
    std::process::Command::new(setup.test.join("podman"))
        .args(args)
        .env(
            "PATH",
            format!(
                "{}:{}",
                setup.stubs.display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .stdin(std::fs::File::open(setup.dir.join("fence")).unwrap())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("running podman.sh")
}

fn call(setup: &Setup, args: &[&str]) {
    let output = spawn(setup, args).wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "podman.sh {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn control(setup: &Setup, name: &str, content: &str) {
    std::fs::write(setup.test.join(name), content).unwrap();
}

fn wait_for(path: PathBuf) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// What the wrapper recorded and what it ran, its paths made stable.
fn report(setup: &Setup) -> String {
    let read = |path: PathBuf| std::fs::read_to_string(path).unwrap_or_default();
    format!(
        "--- calls\n{}--- resources\n{}--- ran\n{}",
        read(setup.test.join("calls")),
        read(setup.dir.join("resources")),
        read(setup.stubs.join("log")),
    )
    .replace(&setup.dir.display().to_string(), "<tmp>")
}

const NETWORK: &[&str] = &[
    "network",
    "create",
    "--driver=bridge",
    "--interface-name=fvm000000000001",
    "--label=dev.estuary.vmm-owner=00000000000000000000000000000001",
    "fv_0000000000000001",
];
const VERIFY: &[&str] = &[
    "run",
    "--rm",
    "--network=host",
    "--log-driver=none",
    "--read-only",
    "--cap-drop=all",
    "--cap-add=CAP_NET_ADMIN",
    "sha256:0000",
    "boundary",
    "verify",
];
const CREATE: &[&str] = &[
    "create",
    "--rm",
    "--name=fv_0000000000000001",
    "--network=fv_0000000000000001",
    "--device=/dev/kvm",
    "--device=/dev/net/tun",
    "sha256:0000",
    "run",
    "--policy",
    "/init/policy.json",
];
const START: &[&str] = &[
    "start",
    "--attach",
    "0000000000000000000000000000000000000000000000000000000000000001",
];

#[test]
fn calls_pass_through() {
    let setup = setup();
    for args in [NETWORK, VERIFY, CREATE, START] {
        call(&setup, args);
    }
    insta::assert_snapshot!(report(&setup));
}

#[test]
fn controls_change_only_their_own_calls() {
    let setup = setup();
    control(&setup, "no-kvm", "");
    control(&setup, "verify-netns", "connector-vmm-kvm-0");
    control(&setup, "vmm-flags", "--test-only\n");
    let install: Vec<&str> = VERIFY[..VERIFY.len() - 1]
        .iter()
        .copied()
        .chain(["install"])
        .collect();
    for args in [NETWORK, VERIFY, &install, CREATE, START] {
        call(&setup, args);
    }
    insta::assert_snapshot!(report(&setup));
}

#[test]
fn a_hold_catches_only_the_first_call() {
    let setup = setup();
    control(&setup, "hold-create", "");
    let first = spawn(&setup, CREATE);
    wait_for(setup.test.join("held-create"));

    let second: Vec<&str> = CREATE
        .iter()
        .map(|arg| match *arg {
            "--name=fv_0000000000000001" => "--name=fv_0000000000000002",
            arg => arg,
        })
        .collect();
    call(&setup, &second);
    let ran = report(&setup);
    assert!(ran.contains("--name=fv_0000000000000002"), "{ran}");
    assert!(
        !ran.contains("podman create --rm --name=fv_0000000000000001"),
        "{ran}"
    );

    std::fs::remove_file(setup.test.join("hold-create")).unwrap();
    let output = first.wait_with_output().unwrap();
    assert!(output.status.success());
    insta::assert_snapshot!(report(&setup));
}

#[test]
fn a_hold_can_catch_a_later_call() {
    let setup = setup();
    control(&setup, "hold-start", "2");
    call(&setup, START);
    let second = spawn(&setup, START);
    wait_for(setup.test.join("held-start"));
    call(&setup, START);
    let ran = std::fs::read_to_string(setup.stubs.join("log")).unwrap();
    assert_eq!(
        ran.lines()
            .filter(|line| line.starts_with("podman "))
            .count(),
        2,
        "{ran}"
    );

    std::fs::remove_file(setup.test.join("hold-start")).unwrap();
    assert!(second.wait_with_output().unwrap().status.success());
    let ran = std::fs::read_to_string(setup.stubs.join("log")).unwrap();
    assert_eq!(
        ran.lines()
            .filter(|line| line.starts_with("podman "))
            .count(),
        3,
        "{ran}"
    );
}

#[test]
fn the_readiness_gate_keeps_only_its_checkpoint() {
    const CHECKPOINT: &str = "connector-vmm-tests: a checkpoint";
    let resembling = format!(
        "WARN libkrun: a diagnostic\n {CHECKPOINT}\n{CHECKPOINT} and more\n \
         {{\"message\":\"a log\"}}\n"
    );

    let mut outcomes = Vec::new();
    for stderr in [
        format!("{resembling}partial"),
        format!("{resembling}{CHECKPOINT}\nafter\npartial"),
    ] {
        let setup = setup();
        std::fs::copy(
            connector_vmm_tests::run::fixture("gate.py"),
            setup.test.join("gate.py"),
        )
        .unwrap();
        control(&setup, "gate-readiness", CHECKPOINT);
        std::fs::write(setup.stubs.join("stderr"), stderr).unwrap();

        // Read to its end, which the gate holds open until it has finished.
        let output = spawn(&setup, START).wait_with_output().unwrap();
        assert!(output.status.success(), "{output:?}");
        outcomes.push((
            String::from_utf8(output.stderr).unwrap(),
            setup.test.join("held-readiness").exists(),
        ));
    }
    assert_eq!(
        outcomes,
        vec![
            (format!("{resembling}partial"), false),
            (format!("{resembling}after\npartial"), true),
        ]
    );
}

/// A root hold waits beneath sudo, holding its stdin, then runs podman with
/// it, so that the fence goes on past whatever kills its launcher's user.
#[test]
fn a_root_hold_waits_beneath_sudo() {
    let setup = setup();
    control(&setup, "root-hold-network-create", "");
    let held = spawn(&setup, NETWORK);
    wait_for(setup.test.join("held-network-create"));
    let ran = std::fs::read_to_string(setup.stubs.join("log")).unwrap();
    assert!(
        ran.starts_with("sudo -n bash -c <script> root-hold ")
            && !ran.lines().any(|line| line.starts_with("podman ")),
        "held beneath sudo, before podman runs: {ran}"
    );

    let second: Vec<&str> = NETWORK
        .iter()
        .map(|arg| match *arg {
            "fv_0000000000000001" => "fv_0000000000000002",
            arg => arg,
        })
        .collect();
    call(&setup, &second);

    std::fs::remove_file(setup.test.join("root-hold-network-create")).unwrap();
    let output = held.wait_with_output().unwrap();
    assert!(output.status.success());
    insta::assert_snapshot!(report(&setup));
}
