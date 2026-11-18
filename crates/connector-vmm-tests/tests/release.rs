//! ci:connector-vmm-kvm's `claim` of its root and `release` of its resources
//! list, run by bash against stand-ins for sudo, mise, systemctl, docker and
//! ip, which log their calls and fail as each test asks. Every fixture is an
//! inert file in a temporary directory, so no entry, hostile or not, reaches
//! a host service.
//!
//! Only the explicitly ignored ownership test requires sudo.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const LIB: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mise/tasks/ci/connector-vmm-kvm-lib.sh"
);

/// The run's loaded VMM image, and the image its boundary runs.
const IMAGE: &str = concat!(
    "sha256:",
    "0000000000000000",
    "0000000000000000",
    "0000000000000000",
    "0000000000000000"
);
/// The suite's VMM image, whose grants `ingress` entries name.
const TEST_IMAGE: &str = "localhost/connector-vmm-test-controls:kvm-00000000";
/// The owner of the list's one grant.
const OWNER: &str = concat!("0000000000000000", "0000000000000002");
/// Beneath HOME, the drop-in directory of the fixture's reactor.
const DROPIN_DIR: &str = ".config/systemd/user/flow-reactor@local-acme-cluster-10299.service.d";

/// Stands in for whichever command it's named as. A control file beside it,
/// named for the call, sets that call's status, and `units` is what
/// `systemctl --user list-units` prints. Each line of `faults`, a status and
/// the start of a call, fails the calls it matches. `networks`, `volumes`,
/// `builders`, `netns`, `links` and `routes` are what the listings print.
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
"sudo -n podman volume ls"*) show volumes ;;
"docker buildx ls"*) show builders ;;
"ip netns list") show netns ;;
"ip -o link show") show links ;;
"ip route show"*) show routes ;;
"mise run local:stop")
    printf '  in %s\n' "${PWD}" >>"${stubs}/calls"
    # A launcher of the stack lists what `on-stop` holds as it stops.
    if [[ -e "${stubs}/on-stop" ]]; then
        cat "${stubs}/on-stop" >>"${ROOT}/members"
        cat "${stubs}/on-stop" >>"${ROOT}/resources"
    fi
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
    /// The platform launch's record.
    record: PathBuf,
    /// What the list and members hold as set up.
    list: String,
    members: String,
}

/// A run's root, home, checkout and stubs, and a list naming a stack, a
/// platform directory and a launcher directory each holding one launch's
/// record, a grant, and what the stack's launches and the run made. The
/// launcher's network is already gone, as its test removed it. Members
/// vouch for every entry, and for the run's two VMM images.
fn setup() -> Setup {
    let dir = std::env::temp_dir().join(format!(
        "connector-vmm-kvm-release-{}",
        connector_vmm_tests::run::random_hex()
    ));
    let root = dir.join("root");
    let stubs = dir.join("stubs");
    let checkout = dir.join("checkout");
    let home = dir.join("home");
    let dropin = home.join(DROPIN_DIR);
    let platform = root.join("platform-00000000");
    let launcher = root.join("launcher-0000000000000000");
    // Explicit modes, whatever the umask: claim refuses a root, and release a
    // directory on the way to what it removes, which others can write.
    for (path, mode) in [
        (dir.clone(), 0o755),
        (stubs.clone(), 0o755),
        (checkout.clone(), 0o755),
        (dir.join("elsewhere"), 0o755),
        (home.clone(), 0o755),
        (home.join(".config"), 0o755),
        (home.join(".config/systemd"), 0o755),
        (home.join(".config/systemd/user"), 0o755),
        (dropin.clone(), 0o755),
        (root.clone(), 0o700),
        (platform.clone(), 0o755),
        (platform.join("state"), 0o700),
        (launcher.clone(), 0o755),
        (launcher.join("state"), 0o700),
    ] {
        std::fs::create_dir(&path).unwrap();
        set_mode(&path, mode);
    }
    std::fs::write(dropin.join("connector-vmm.conf"), "").unwrap();
    let record = platform.join("state/fv_0000000000000001.owner");
    write_private(&record, "");
    write_private(&launcher.join("state/fv_0000000000000002.owner"), "");

    let stub = stubs.join("stub");
    std::fs::write(&stub, STUB).unwrap();
    set_mode(&stub, 0o755);
    for name in ["sudo", "mise", "systemctl", "docker", "ip"] {
        std::os::unix::fs::symlink(&stub, stubs.join(name)).unwrap();
    }
    for (listing, content) in [
        ("networks", "podman\nfv_0000000000000001\n"),
        (
            "volumes",
            "unrelated\nfv_0000000000000001\nfv_0000000000000002\n",
        ),
        (
            "builders",
            "default\ndefault\nconnector-vmm-kvm-00000000\nconnector-vmm-kvm-000000000\n",
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

    let entries = [
        "builder connector-vmm-kvm-00000000".to_string(),
        format!("image {IMAGE}"),
        format!("boundary {IMAGE}"),
        format!("image {TEST_IMAGE}"),
        "route blackhole 192.31.196.240/28 metric 4000".to_string(),
        "netns connector-vmm-kvm".to_string(),
        "link vmm-kvm0".to_string(),
        format!("dir {}", platform.display()),
        format!("dropin {}", dropin.join("connector-vmm.conf").display()),
        format!("stack acme local-acme-cluster {}", checkout.display()),
        "network fv_0000000000000001".to_string(),
        "volume fv_0000000000000001".to_string(),
        "container fv_0000000000000001".to_string(),
        format!("dir {}", launcher.display()),
        "network fv_0000000000000002".to_string(),
        "volume fv_0000000000000002".to_string(),
        "container fv_0000000000000002".to_string(),
        format!("ingress {TEST_IMAGE} {OWNER}"),
    ];
    let list: String = entries.iter().map(|entry| format!("{entry}\n")).collect();
    let members = format!("vmm-image {IMAGE}\ntest-vmm-image {TEST_IMAGE}\n{list}");
    write_private(&root.join("resources"), &list);
    write_private(&root.join("members"), &members);

    Setup {
        dir,
        root,
        stubs,
        record,
        list,
        members,
    }
}

fn set_mode(path: &Path, mode: u32) {
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(mode)).unwrap();
}

/// Writes `path` as the task writes its files: the user's own, and
/// writable by no one else.
fn write_private(path: &Path, content: &str) {
    std::fs::write(path, content).unwrap();
    set_mode(path, 0o600);
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
}

/// Runs `script` with the library sourced and the stand-ins first on PATH,
/// as a run of this fixture's checkout, whose stack is `acme`, of data plane
/// `local-acme-cluster` and plane base 10200.
fn bash(setup: &Setup, script: &str) -> std::process::Output {
    std::process::Command::new("bash")
        .arg("-c")
        .arg(format!(r#"set -euo pipefail; source "$1"; {script}"#))
        .arg("release")
        .arg(LIB)
        .env(
            "PATH",
            format!(
                "{}:{}",
                setup.stubs.display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env("HOME", setup.dir.join("home"))
        .env("ROOT", &setup.root)
        .env("TABLE", "flow_vmm_boundary")
        .env("CHECKOUT", setup.dir.join("checkout"))
        .env("NETNS", "connector-vmm-kvm")
        .env("LINK", "vmm-kvm0")
        .env("FENCE", "192.31.196.240/28")
        .env("DECOY_FENCE", "198.18.255.240/28")
        .env("FLOW_STACK_NAME", "acme")
        .env("FLOW_CLUSTER", "local-acme-cluster")
        .env("FLOW_PLANE_BASE", "10200")
        // So that what find says doesn't depend on the host's locale.
        .env("LC_ALL", "C")
        .output()
        .expect("running bash")
}

/// What a script did: its status, what it said, and the stand-ins' calls.
struct Ran {
    status: i32,
    stderr: String,
    calls: Vec<String>,
}

fn ran(setup: &Setup, script: &str) -> Ran {
    let output = bash(setup, script);
    let calls_path = setup.stubs.join("calls");
    let calls = std::fs::read_to_string(&calls_path).unwrap_or_default();
    // So that a retry's calls are its own.
    let _ = std::fs::remove_file(&calls_path);
    Ran {
        status: output.status.code().unwrap(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        calls: calls.lines().map(ToString::to_string).collect(),
    }
}

/// Run `release` once `claim` has taken the root, as the task runs them, and
/// describe what it did: its status, what it said, the calls it made, and
/// what of the list it kept.
fn release(setup: &Setup) -> String {
    let resources = setup.root.join("resources");
    let before = read(&resources);
    let ran = ran(setup, "claim; release");
    let kept = match read(&resources) {
        after if after.is_empty() => "emptied\n".to_string(),
        after if after == before => "kept whole\n".to_string(),
        after => format!(
            "kept\n{}",
            after
                .lines()
                .map(|line| format!("  {line}\n"))
                .collect::<String>()
        ),
    };
    let calls: String = ran.calls.iter().map(|call| format!("{call}\n")).collect();
    format!(
        "status: {}\nlist: {kept}--- stderr\n{}--- calls\n{calls}",
        ran.status, ran.stderr
    )
    .replace(&setup.dir.display().to_string(), "<tmp>")
}

/// `release` while the stand-ins fail as `faults` says, then again once they
/// no longer do.
fn release_and_retry(setup: &Setup, faults: &str) -> String {
    control(setup, "faults", faults);
    let failed = release(setup);
    std::fs::remove_file(setup.stubs.join("faults")).unwrap();
    format!("{failed}=== retried\n{}", release(setup))
}

/// Runs `claim` and `release`, and asserts they failed saying `said`, called
/// no stand-in, and left the list and members as they were.
fn assert_changes_nothing(setup: &Setup, said: &str) {
    let (list, members) = (setup.root.join("resources"), setup.root.join("members"));
    let before = (read(&list), read(&members));
    let ran = ran(setup, "claim; release");
    assert_eq!(ran.status, 1, "{}", ran.stderr);
    assert!(
        ran.stderr.contains(said),
        "expected {said:?} in:\n{}",
        ran.stderr
    );
    assert_eq!(
        ran.calls,
        Vec::<String>::new(),
        "no stand-in may be called:\n{}",
        ran.stderr
    );
    assert_eq!((read(&list), read(&members)), before);
}

/// Asserts that `ran` failed having called stand-ins only to stop the
/// list's stack and show it stopped: no resource was tried or removed.
fn assert_only_stopped(setup: &Setup, ran: &Ran) {
    assert_eq!(ran.status, 1, "{}", ran.stderr);
    let stopped = [
        "mise run local:stop".to_string(),
        format!("  in {}", setup.dir.join("checkout").display()),
    ];
    for call in &ran.calls {
        assert!(
            stopped.contains(call) || call.starts_with("systemctl --user list-units "),
            "only the stack may be stopped, not {call:?}:\n{}",
            ran.stderr
        );
    }
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
fn an_owned_launch_keeps_everything() {
    let setup = setup();
    let record = std::fs::File::open(&setup.record).unwrap();
    record.lock().unwrap();
    insta::assert_snapshot!(release(&setup));
    drop(record);
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

/// Each entry is added to the run's own list alone. Those marked vouched
/// for are in members too, so that only what the run knows apart from both
/// refuses them. Some name what the host holds, beside the run's own.
#[test]
fn an_entry_which_is_not_the_runs_changes_nothing() {
    let setup = setup();
    let tmp = setup.dir.display().to_string();
    let unrelated = "fv_9999999999999999";
    for listing in ["networks", "volumes"] {
        let held = read(&setup.stubs.join(listing));
        control(&setup, listing, &format!("{held}{unrelated}\n"));
    }
    // Root removes what a directory entry names, so a link on the way would
    // lead it elsewhere.
    std::os::unix::fs::symlink(setup.dir.join("elsewhere"), setup.root.join("state")).unwrap();

    for (entry, vouched) in [
        (format!("container {unrelated}"), false),
        (format!("network {unrelated}"), false),
        (format!("volume {unrelated}"), false),
        ("builder connector-vmm-kvm-99999999".to_string(), false),
        (
            "image localhost/connector-vmm-kvm-environment:9999999999999999".to_string(),
            false,
        ),
        (format!("dir {tmp}/root/launcher-9999999999999999"), false),
        (format!("dir {tmp}/elsewhere"), true),
        (
            format!("dir {tmp}/root/launcher-0000000000000000/../../elsewhere"),
            true,
        ),
        (format!("dir {tmp}/root/state/fv_0000000000000003"), true),
        (
            format!("stack acme local-acme-cluster {tmp}/elsewhere"),
            true,
        ),
        (
            format!(
                "dropin {tmp}/home/.config/systemd/user/flow-reactor@local-other-cluster-10299.service.d/connector-vmm.conf"
            ),
            true,
        ),
        ("route blackhole 0.0.0.0/0 metric 4000".to_string(), true),
        ("image docker.io/library/postgres:16".to_string(), true),
        (
            "image localhost/connector-vmm-kvm-acme:9999999999999999".to_string(),
            true,
        ),
        ("network podman".to_string(), true),
        ("mount /".to_string(), true),
        ("link  vmm-kvm0".to_string(), true),
    ] {
        write_private(
            &setup.root.join("resources"),
            &format!("{}{entry}\n", setup.list),
        );
        let members = match vouched {
            true => format!("{}{entry}\n", setup.members),
            false => setup.members.clone(),
        };
        write_private(&setup.root.join("members"), &members);
        assert_changes_nothing(&setup, &format!("refusing '{entry}'"));
    }

    // tac or read would join an unterminated last line to the line before.
    write_private(
        &setup.root.join("resources"),
        &format!("{}link vmm-kvm0", setup.list),
    );
    assert_changes_nothing(&setup, "its last line is unterminated");

    // Bash would drop the NUL, and read an entry the run vouched for.
    write_private(
        &setup.root.join("resources"),
        &format!("{}container fv_0000000000000001\0\n", setup.list),
    );
    assert_changes_nothing(&setup, "it holds a NUL byte");
    cleanup(&setup);
}

/// A launcher of the stack may list a resource as local:stop stops it,
/// after release read the list. Release then keeps the list as it now is,
/// removing nothing, and a retry releases the new entry too.
#[test]
fn an_entry_listed_as_the_stack_stops_is_kept_for_a_retry() {
    let setup = setup();
    let late = "container fv_0000000000000003\n";
    control(&setup, "on-stop", late);
    let interrupted = ran(&setup, "claim; release");
    assert!(
        interrupted.stderr.contains("changed as the stack stopped"),
        "{}",
        interrupted.stderr
    );
    assert_only_stopped(&setup, &interrupted);
    assert_eq!(
        read(&setup.root.join("resources")),
        format!("{}{late}", setup.list)
    );
    assert_eq!(
        read(&setup.root.join("members")),
        format!("{}{late}", setup.members)
    );

    std::fs::remove_file(setup.stubs.join("on-stop")).unwrap();
    let retried = ran(&setup, "claim; release");
    assert_eq!(retried.status, 0, "{}", retried.stderr);
    let removal = "sudo -n podman rm -f --time 0 --ignore fv_0000000000000003".to_string();
    assert!(retried.calls.contains(&removal), "{:?}", retried.calls);
    assert_eq!(read(&setup.root.join("resources")), "");
    cleanup(&setup);
}

/// An image ID and its boundary, both vouched for as entries, are refused
/// unless members vouch for that ID as the run's loaded VMM image; so are a
/// test-controls tag and its grant, unless they vouch for that tag as the
/// suite's VMM image.
#[test]
fn a_forged_vmm_image_changes_nothing() {
    let setup = setup();
    let forged = format!("sha256:{}", "1".repeat(64));
    let tag = "localhost/connector-vmm-test-controls:kvm-11111111";
    for (entries, said) in [
        (
            format!("image {forged}\nboundary {forged}\n"),
            format!("refusing 'boundary {forged}'"),
        ),
        (
            format!("image {tag}\ningress {tag} {OWNER}\n"),
            format!("refusing 'ingress {tag} {OWNER}'"),
        ),
    ] {
        write_private(
            &setup.root.join("resources"),
            &format!("{}{entries}", setup.list),
        );
        write_private(
            &setup.root.join("members"),
            &format!("{}{entries}", setup.members),
        );
        assert_changes_nothing(&setup, &said);
        let first = entries.lines().next().unwrap();
        assert_changes_nothing(&setup, &format!("refusing '{first}'"));
    }
    cleanup(&setup);
}

/// A file of the root, or a launch's record, with another name is refused:
/// what writes that name would write it too.
#[test]
fn a_file_with_another_name_is_refused() {
    for name in ["lock", "resources", "members", "run.json"] {
        let setup = setup();
        let path = setup.root.join(name);
        if !path.exists() {
            write_private(&path, "");
        }
        std::fs::hard_link(&path, setup.dir.join("elsewhere").join(name)).unwrap();
        assert_changes_nothing(&setup, &format!("refusing {} (f ", path.display()));
        cleanup(&setup);
    }

    let setup = setup();
    std::fs::hard_link(&setup.record, setup.dir.join("elsewhere/record")).unwrap();
    let list = read(&setup.root.join("resources"));
    let ran = ran(&setup, "claim; release");
    assert!(
        ran.stderr
            .contains(&format!("refusing {}", setup.record.display())),
        "{}",
        ran.stderr
    );
    assert_only_stopped(&setup, &ran);
    assert_eq!(read(&setup.root.join("resources")), list);
    cleanup(&setup);
}

/// A list or members which can't be read, or a launcher directory which
/// can't be searched for records, keeps the list whole, and nothing is
/// removed.
#[test]
fn what_cannot_be_read_keeps_the_list_whole() {
    // Root reads whatever the mode.
    if rustix::process::geteuid().as_raw() == 0 {
        return;
    }
    for name in ["resources", "members"] {
        let setup = setup();
        let path = setup.root.join(name);
        set_mode(&path, 0o000);
        let ran = ran(&setup, "claim; release");
        set_mode(&path, 0o600);
        assert_eq!(ran.status, 1, "{}", ran.stderr);
        assert!(
            ran.stderr
                .contains(&format!("could not read {}", path.display())),
            "{}",
            ran.stderr
        );
        assert_eq!(ran.calls, Vec::<String>::new());
        assert_eq!(read(&setup.root.join("resources")), setup.list);
        assert_eq!(read(&setup.root.join("members")), setup.members);
        cleanup(&setup);
    }

    let setup = setup();
    let launcher = setup.root.join("launcher-0000000000000000");
    set_mode(&launcher, 0o000);
    let ran = ran(&setup, "claim; release");
    set_mode(&launcher, 0o755);
    assert!(
        ran.stderr.contains("could not look for records beneath"),
        "{}",
        ran.stderr
    );
    assert_only_stopped(&setup, &ran);
    assert_eq!(read(&setup.root.join("resources")), setup.list);
    cleanup(&setup);
}

/// While another run holds the lock, a run touches nothing: no stack, launch
/// or resource, and not the list. Once that run ends, a retry releases all.
#[test]
fn another_run_holding_the_lock_changes_nothing_until_it_ends() {
    let setup = setup();
    let lock = setup.root.join("lock");
    write_private(&lock, "");
    let held = std::fs::File::open(&lock).unwrap();
    held.lock().unwrap();
    assert_changes_nothing(&setup, "another run holds");
    drop(held);

    let ran = ran(&setup, "claim; release");
    assert_eq!(ran.status, 0, "{}", ran.stderr);
    assert_eq!(read(&setup.root.join("resources")), "");
    assert_eq!(read(&setup.root.join("members")), "");
    cleanup(&setup);
}

/// A list whose rewrite fails is kept whole, though what it listed is gone.
/// The retry finds each resource gone or removes it again, then empties it
/// and members.
#[test]
fn a_failed_rewrite_keeps_the_whole_list_for_a_retry() {
    let setup = setup();
    std::os::unix::fs::symlink(setup.stubs.join("stub"), setup.stubs.join("mv")).unwrap();
    control(&setup, "faults", "1 mv\n");
    let failed = ran(&setup, "claim; release");
    assert_eq!(failed.status, 1, "{}", failed.stderr);
    assert!(
        failed.stderr.contains("could not replace"),
        "{}",
        failed.stderr
    );
    assert!(
        failed.calls.last().unwrap().starts_with("mv -f -- "),
        "{:?}",
        failed.calls
    );
    assert_eq!(read(&setup.root.join("resources")), setup.list);
    assert_eq!(read(&setup.root.join("members")), setup.members);
    let staged: Vec<_> = std::fs::read_dir(&setup.root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().starts_with('.'))
        .collect();
    assert_eq!(staged, Vec::<std::ffi::OsString>::new());

    std::fs::remove_file(setup.stubs.join("mv")).unwrap();
    std::fs::remove_file(setup.stubs.join("faults")).unwrap();
    let retried = ran(&setup, "claim; release");
    assert_eq!(retried.status, 0, "{}", retried.stderr);
    assert_eq!(read(&setup.root.join("resources")), "");
    assert_eq!(read(&setup.root.join("members")), "");
    cleanup(&setup);
}

/// A grant whose revoke fails keeps the image the revoke runs, and what else
/// it may still use. Members still vouch for that image, so a retry revokes
/// the grant.
#[test]
fn a_failing_revoke_keeps_its_image_until_a_retry() {
    let setup = setup();
    let revoke = format!(
        "sudo -n podman run --rm --pull=never --network=host --log-driver=none --read-only \
         --cap-drop=all --cap-add=CAP_NET_ADMIN {TEST_IMAGE} boundary revoke --owner={OWNER}"
    );
    control(&setup, "faults", &format!("125 {revoke}\n"));
    let failed = ran(&setup, "claim; release");
    assert_eq!(failed.status, 1, "{}", failed.stderr);
    let tmp = setup.dir.display();
    assert_eq!(
        read(&setup.root.join("resources")),
        format!(
            "image {IMAGE}\nboundary {IMAGE}\nimage {TEST_IMAGE}\n\
             dir {tmp}/root/platform-00000000\ndir {tmp}/root/launcher-0000000000000000\n\
             ingress {TEST_IMAGE} {OWNER}\n"
        )
    );
    assert_eq!(read(&setup.root.join("members")), setup.members);

    std::fs::remove_file(setup.stubs.join("faults")).unwrap();
    let retried = ran(&setup, "claim; release");
    assert_eq!(retried.status, 0, "{}", retried.stderr);
    assert!(retried.calls.contains(&revoke), "{:?}", retried.calls);
    assert_eq!(read(&setup.root.join("resources")), "");
    cleanup(&setup);
}

/// An image or builder which can't be removed is kept, and fails the
/// release, until a retry removes it.
#[test]
fn a_failing_image_or_builder_removal_is_kept_until_a_retry() {
    let setup = setup();
    control(
        &setup,
        "faults",
        "1 sudo -n podman image rm\n1 docker buildx rm\n",
    );
    let failed = ran(&setup, "claim; release");
    assert_eq!(failed.status, 1, "{}", failed.stderr);
    assert!(
        failed
            .stderr
            .contains(&format!("could not remove image {TEST_IMAGE}")),
        "{}",
        failed.stderr
    );
    assert_eq!(
        read(&setup.root.join("resources")),
        format!("builder connector-vmm-kvm-00000000\nimage {IMAGE}\nimage {TEST_IMAGE}\n")
    );

    std::fs::remove_file(setup.stubs.join("faults")).unwrap();
    let retried = ran(&setup, "claim; release");
    assert_eq!(retried.status, 0, "{}", retried.stderr);
    for removal in [
        format!("sudo -n podman image rm --ignore --no-prune {IMAGE}"),
        "docker buildx rm connector-vmm-kvm-00000000".to_string(),
    ] {
        assert!(retried.calls.contains(&removal), "{:?}", retried.calls);
    }
    assert_eq!(read(&setup.root.join("resources")), "");
    cleanup(&setup);
}

/// A root, a directory above it or a file in it which another user could
/// have written is refused before the list is read or a command run, and so
/// is a link where `claim` expects a directory or file: nothing is made
/// through one.
#[test]
fn a_root_which_is_not_private_is_refused_untouched() {
    let setup = setup();
    let root = setup.root.display().to_string();
    let lock = setup.root.join("lock");

    set_mode(&setup.dir, 0o777);
    assert_changes_nothing(&setup, &format!("refusing {} (d ", setup.dir.display()));
    set_mode(&setup.dir, 0o755);

    for mode in [0o711, 0o750, 0o770] {
        set_mode(&setup.root, mode);
        assert_changes_nothing(&setup, &format!("refusing {root} (d "));
    }
    set_mode(&setup.root, 0o700);

    let moved = setup.dir.join("moved");
    std::fs::rename(&setup.root, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &setup.root).unwrap();
    assert_changes_nothing(&setup, &format!("refusing {root} (l "));
    std::fs::remove_file(&setup.root).unwrap();
    std::fs::rename(&moved, &setup.root).unwrap();
    assert!(!lock.exists(), "no lock is made before the root is taken");

    for name in ["lock", "run.json"] {
        let path = setup.root.join(name);
        std::os::unix::fs::symlink(setup.dir.join("elsewhere").join(name), &path).unwrap();
        assert_changes_nothing(&setup, &format!("refusing {root}/{name} (l "));
        std::fs::remove_file(&path).unwrap();
    }

    set_mode(&setup.root.join("resources"), 0o646);
    assert_changes_nothing(&setup, &format!("refusing {root}/resources (f "));
    set_mode(&setup.root.join("resources"), 0o600);

    std::fs::create_dir(setup.root.join("platform.json")).unwrap();
    assert_changes_nothing(&setup, &format!("refusing {root}/platform.json (d "));

    assert_eq!(
        std::fs::read_dir(setup.dir.join("elsewhere"))
            .unwrap()
            .count(),
        0,
        "nothing is made through a link"
    );
    cleanup(&setup);
}

/// An absent root is made the invoking user's, with mode 0700, holding its
/// lock, an empty list and empty members, each of mode 0600 with one name.
#[test]
fn an_absent_root_is_made_private() {
    let setup = setup();
    std::fs::remove_dir_all(&setup.root).unwrap();
    let ran = ran(&setup, "claim");
    assert_eq!((ran.status, ran.stderr.as_str()), (0, ""));

    let uid = rustix::process::geteuid().as_raw();
    let root = std::fs::symlink_metadata(&setup.root).unwrap();
    assert!(root.is_dir() && root.uid() == uid);
    assert_eq!(root.mode() & 0o7777, 0o700);
    for name in ["lock", "resources", "members"] {
        let file = std::fs::symlink_metadata(setup.root.join(name)).unwrap();
        assert!(file.is_file() && file.uid() == uid, "{name}");
        assert_eq!(
            (file.mode() & 0o7777, file.nlink(), file.len()),
            (0o600, 1, 0),
            "{name}"
        );
    }
    cleanup(&setup);
}

/// A root of another user's, or of root's as an earlier version of the task
/// made it, beneath a parent which is otherwise valid, is refused before
/// anything is made or run. Only root can give it to another user, so this
/// chowns its own temporary root, and only that, with passwordless sudo.
#[test]
#[ignore = "chowns only its own temporary root, with passwordless sudo; run it explicitly"]
fn a_root_another_user_owns_is_refused() {
    let setup = setup();
    let root = setup.root.display().to_string();
    let ours = format!(
        "{}:{}",
        rustix::process::getuid().as_raw(),
        rustix::process::getgid().as_raw()
    );
    for (owner, said) in [
        ("65534:65534", format!("refusing {root} (d 65534 700 ")),
        ("0:0", format!("refusing {root}: it is root's")),
    ] {
        connector_vmm_tests::run::sudo(&["chown", owner, &root]);
        let ran = ran(&setup, "claim; release");
        connector_vmm_tests::run::sudo(&["chown", &ours, &root]);
        assert_eq!(ran.status, 1, "{}", ran.stderr);
        assert!(ran.stderr.contains(&said), "{said:?} in:\n{}", ran.stderr);
        assert_eq!(ran.calls, Vec::<String>::new());
        assert!(!setup.root.join("lock").exists());
        assert_eq!(read(&setup.root.join("resources")), setup.list);
        assert_eq!(read(&setup.root.join("members")), setup.members);
    }
    cleanup(&setup);
}
