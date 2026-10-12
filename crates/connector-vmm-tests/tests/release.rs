//! `release` of ci:connector-vmm-kvm's resources list, run by bash against
//! stand-ins for sudo, mise, systemctl, docker and ip, which log their calls
//! and fail as each test asks. A stack which can't be shown stopped, or a
//! launch still owned, must keep everything the list holds, the host boundary
//! included. A removal which fails, or whose resource can't be shown gone,
//! must keep its entry and what it may still use, for a retry to release.
//! Needs no KVM, podman or sudo.

use std::path::PathBuf;

const LIB: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mise/tasks/ci/connector-vmm-kvm-lib.sh"
);

/// Stands in for whichever command it's named as. A control file beside it,
/// named for the call, sets that call's status, and `units` is what
/// `systemctl --user list-units` prints. Each line of `faults`, a status and
/// the start of a call, fails the calls it matches. `networks`, `builders`,
/// `netns`, `links` and `routes` are what the listings print.
const STUB: &str = r#"#!/usr/bin/env bash
set -euo pipefail
stubs=$(dirname "$(readlink -f "$0")")
name=$(basename "$0")
printf '%s %s\n' "${name}" "$*" >>"${stubs}/calls"
status() {
    if [[ -e "${stubs}/$1" ]]; then
        exit "$(<"${stubs}/$1")"
    fi
    exit 0
}
show() {
    if [[ -e "${stubs}/$1" ]]; then
        cat "${stubs}/$1"
    fi
}
if [[ -e "${stubs}/faults" ]]; then
    while read -r code call; do
        if [[ "${name} $*" == "${call}"* ]]; then
            echo "${name} $*: failing, as the test asks" >&2
            exit "${code}"
        fi
    done <"${stubs}/faults"
fi
case "${name} $*" in
"sudo -n podman network ls"*) show networks ;;
"docker buildx ls"*) show builders ;;
"ip netns list") show netns ;;
"ip -o link show") show links ;;
"ip route show"*) show routes ;;
"mise run local:stop")
    printf '  in %s\n' "${PWD}" >>"${stubs}/calls"
    status mise-status
    ;;
"systemctl --user list-units"*)
    if [[ -e "${stubs}/units" ]]; then
        cat "${stubs}/units"
    fi
    status systemctl-status
    ;;
esac
"#;

struct Setup {
    dir: PathBuf,
    root: PathBuf,
    stubs: PathBuf,
    record: PathBuf,
    launcher_record: PathBuf,
}

/// A run's directory, home, checkout and stubs, and a list naming a stack, a
/// platform directory and a launcher directory each holding one launch's
/// record, and what the stack's launches and the run made. The launcher's
/// network is already gone, as its test removed it.
fn setup() -> Setup {
    let dir = std::env::temp_dir().join(format!(
        "connector-vmm-kvm-release-{}",
        connector_vmm_tests::run::random_hex()
    ));
    let root = dir.join("root");
    let stubs = dir.join("stubs");
    let home = dir.join("home");
    let checkout = dir.join("checkout");
    let dropin = home.join(".config/systemd/user/flow-reactor@local-acme-cluster-10299.service.d");
    let state = root.join("platform-0/state");
    let launcher_state = root.join("launcher-0/state");
    for path in [&stubs, &checkout, &dropin, &state, &launcher_state] {
        std::fs::create_dir_all(path).unwrap();
    }
    std::fs::write(dropin.join("connector-vmm.conf"), "").unwrap();
    let record = state.join("fv_0000000000000001.owner");
    std::fs::write(&record, "").unwrap();
    let launcher_record = launcher_state.join("fv_0000000000000002.owner");
    std::fs::write(&launcher_record, "").unwrap();

    let stub = stubs.join("stub");
    std::fs::write(&stub, STUB).unwrap();
    std::fs::set_permissions(&stub, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    for name in ["sudo", "mise", "systemctl", "docker", "ip"] {
        std::os::unix::fs::symlink(&stub, stubs.join(name)).unwrap();
    }
    for (listing, content) in [
        ("networks", "podman\nfv_0000000000000001\n"),
        (
            "builders",
            "default\ndefault\nconnector-vmm-kvm-0\nconnector-vmm-kvm-00\n",
        ),
        ("netns", "connector-vmm-kvm (id: 0)\n"),
        (
            "links",
            "1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536\n7: vmm-kvm0@if2: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500\n",
        ),
        // ip ends each route with a space.
        ("routes", "blackhole 192.31.196.240/28 metric 4000 \n"),
    ] {
        std::fs::write(stubs.join(listing), content).unwrap();
    }

    let resources = [
        "builder connector-vmm-kvm-0".to_string(),
        "image sha256:0000".to_string(),
        "boundary sha256:0000".to_string(),
        "route blackhole 192.31.196.240/28 metric 4000".to_string(),
        "netns connector-vmm-kvm".to_string(),
        "link vmm-kvm0".to_string(),
        format!("dir {}", root.join("platform-0").display()),
        format!("dropin {}", dropin.join("connector-vmm.conf").display()),
        format!("stack acme local-acme-cluster {}", checkout.display()),
        "network fv_0000000000000001".to_string(),
        "container fv_0000000000000001".to_string(),
        format!("dir {}", root.join("launcher-0").display()),
        "network fv_0000000000000002".to_string(),
        "container fv_0000000000000002".to_string(),
    ];
    std::fs::write(root.join("resources"), resources.join("\n") + "\n").unwrap();

    Setup {
        dir,
        root,
        stubs,
        record,
        launcher_record,
    }
}

/// Run `release`, and describe what it did: its status, what it said, the
/// calls it made, and what of the list it kept.
fn release(setup: &Setup) -> String {
    let resources = setup.root.join("resources");
    let before = std::fs::read_to_string(&resources).unwrap();

    let output = std::process::Command::new("bash")
        .arg("-c")
        .arg(r#"set -euo pipefail; ROOT=$2; TABLE=flow_vmm_boundary; source "$1"; release"#)
        .arg("release")
        .arg(LIB)
        .arg(&setup.root)
        .env(
            "PATH",
            format!(
                "{}:{}",
                setup.stubs.display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("HOME", setup.dir.join("home"))
        // So that what find says doesn't depend on the host's locale.
        .env("LC_ALL", "C")
        .output()
        .expect("running bash");

    let after = std::fs::read_to_string(&resources).unwrap();
    let kept = match after.as_str() {
        "" => "emptied\n".to_string(),
        after if after == before => "kept whole\n".to_string(),
        after => format!(
            "kept\n{}",
            after
                .lines()
                .map(|line| format!("  {line}\n"))
                .collect::<String>()
        ),
    };
    let calls_path = setup.stubs.join("calls");
    let calls = std::fs::read_to_string(&calls_path).unwrap_or_default();
    // So that a retry's report holds only its own calls.
    let _ = std::fs::remove_file(&calls_path);
    let report = format!(
        "status: {}\nlist: {kept}--- stderr\n{}--- calls\n{calls}",
        output.status.code().unwrap(),
        String::from_utf8_lossy(&output.stderr),
    );
    report.replace(&setup.dir.display().to_string(), "<tmp>")
}

/// `release` while the stand-ins fail as `faults` says, then again once they
/// no longer do.
fn release_and_retry(setup: &Setup, faults: &str) -> String {
    control(setup, "faults", faults);
    let failed = release(setup);
    std::fs::remove_file(setup.stubs.join("faults")).unwrap();
    format!("{failed}=== retried\n{}", release(setup))
}

fn control(setup: &Setup, file: &str, content: &str) {
    std::fs::write(setup.stubs.join(file), content).unwrap();
}

fn cleanup(setup: &Setup) {
    std::fs::remove_dir_all(&setup.dir).unwrap();
}

#[test]
fn a_stopped_stack_is_released() {
    let setup = setup();
    insta::assert_snapshot!(release(&setup));
    cleanup(&setup);
}

#[test]
fn a_failed_stop_keeps_everything() {
    let setup = setup();
    control(&setup, "mise-status", "1");
    insta::assert_snapshot!(release(&setup));
    cleanup(&setup);
}

#[test]
fn a_stack_still_running_keeps_everything() {
    let setup = setup();
    control(
        &setup,
        "units",
        "flow-reactor@local-acme-cluster-10299.service loaded deactivating stop-sigterm Flow Reactor\n",
    );
    insta::assert_snapshot!(release(&setup));
    cleanup(&setup);
}

#[test]
fn a_stack_which_cannot_be_listed_keeps_everything() {
    let setup = setup();
    control(&setup, "systemctl-status", "1");
    insta::assert_snapshot!(release(&setup));
    cleanup(&setup);
}

#[test]
fn an_owned_launch_keeps_everything() {
    let setup = setup();
    let record = std::fs::File::open(&setup.record).unwrap();
    record.lock().unwrap();
    insta::assert_snapshot!(release(&setup));
    drop(record);
    cleanup(&setup);
}

#[test]
fn an_owned_launch_of_a_test_keeps_everything() {
    let setup = setup();
    let record = std::fs::File::open(&setup.launcher_record).unwrap();
    record.lock().unwrap();
    insta::assert_snapshot!(release(&setup));
    drop(record);
    cleanup(&setup);
}

#[test]
fn records_which_cannot_be_looked_for_keep_everything() {
    // Root looks inside a sealed directory regardless.
    // SAFETY: geteuid takes nothing and cannot fail.
    if unsafe { libc::geteuid() } == 0 {
        return;
    }
    let setup = setup();
    let state = setup.root.join("platform-0/state");
    std::fs::set_permissions(&state, std::os::unix::fs::PermissionsExt::from_mode(0o000)).unwrap();
    let report = release(&setup);
    std::fs::set_permissions(&state, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    insta::assert_snapshot!(report);
    cleanup(&setup);
}

#[test]
fn a_failing_engine_keeps_what_it_holds_until_a_retry() {
    let setup = setup();
    insta::assert_snapshot!(release_and_retry(
        &setup,
        "125 sudo -n podman\n125 docker\n"
    ));
    cleanup(&setup);
}

#[test]
fn networks_which_cannot_be_listed_are_kept_until_a_retry() {
    let setup = setup();
    insta::assert_snapshot!(release_and_retry(&setup, "125 sudo -n podman network ls\n"));
    cleanup(&setup);
}

#[test]
fn a_network_still_in_use_keeps_what_it_may_use_until_a_retry() {
    let setup = setup();
    // podman's status for a network which a container still uses.
    insta::assert_snapshot!(release_and_retry(
        &setup,
        "2 sudo -n podman network rm fv_0000000000000001\n"
    ));
    cleanup(&setup);
}

#[test]
fn a_refused_boundary_keeps_its_image_until_a_retry() {
    let setup = setup();
    insta::assert_snapshot!(release_and_retry(&setup, "1 sudo -n podman run\n"));
    cleanup(&setup);
}

#[test]
fn failures_outside_the_engine_keep_only_their_own_entries_until_a_retry() {
    let setup = setup();
    insta::assert_snapshot!(release_and_retry(
        &setup,
        "1 ip netns list\n2 sudo -n ip route del\n1 sudo -n rm -rf\n"
    ));
    cleanup(&setup);
}

#[test]
fn images_and_builders_left_behind_are_only_reported() {
    let setup = setup();
    control(
        &setup,
        "faults",
        "125 sudo -n podman image rm\n1 docker buildx ls\n",
    );
    insta::assert_snapshot!(release(&setup));
    cleanup(&setup);
}
