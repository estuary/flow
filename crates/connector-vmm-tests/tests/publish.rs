//! `ci:docker-images`, `ci:connector-vmm-gate` and `ci:package`, run by bash
//! against stand-ins for docker and for the binaries a package holds, which
//! log their calls and answer as each test asks. A VMM image is pushed only
//! once it is the image the bake built, has passed its gate, and reports the
//! expected commit, and only by the ID that passed; the other images publish
//! from the bake, before the gate, as they always have. A gate removes its
//! container however it ends. Needs no docker, KVM or sudo.

use std::path::{Path, PathBuf};

const DOCKER_IMAGES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mise/tasks/ci/docker-images"
);
const GATE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mise/tasks/ci/connector-vmm-gate"
);
const PACKAGE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../mise/tasks/ci/package");
const REPO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
const OTHER_COMMIT: &str = "89abcdef0123456789abcdef0123456789abcdef";
const VMM_ID: &str = "sha256:aaaa";
const GATE_NAME: &str = "connector-vmm-gate-test";

/// Stands in for docker. Each line of `faults`, a status and the start of a
/// call, fails the calls it matches; those of `faults-after-run` do once a
/// named container has run. A bake logs its targets and writes `metadata` as
/// its metadata file; `image inspect` answers from `ids`, lines of a reference
/// and its ID; `boundary verify` prints `build`. A named container is a file
/// in `containers` while it exists: `run --rm` removes it on exit unless
/// `linger` exists, and runs until killed if `hang` does.
const DOCKER: &str = r#"#!/usr/bin/env bash
set -euo pipefail
stubs=$(dirname "$(readlink -f "$0")")
name=$(basename "$0")
# A gate's script is long, and the same each time.
logged=() prev= container=
for arg in "$@"; do
    if [[ "${prev}" == -c ]]; then logged+=("<script>"); else logged+=("${arg}"); fi
    if [[ "${prev}" == --name ]]; then container=${arg}; fi
    prev=${arg}
done
call="${name} ${logged[*]}"
printf '%s\n' "${call}" >>"${stubs}/calls"
faults=("${stubs}/faults")
if [[ -e "${stubs}/ran" ]]; then
    faults+=("${stubs}/faults-after-run")
fi
for file in "${faults[@]}"; do
    if [[ -e "${file}" ]]; then
        while read -r code start; do
            if [[ "${call}" == "${start}"* ]]; then
                echo "${call}: failing, as the test asks" >&2
                exit "${code}"
            fi
        done <"${file}"
    fi
done
case "${call}" in
"docker buildx bake "*)
    while (($#)); do
        case "$1" in
        --metadata-file) metadata=$2 && shift 2 ;;
        -f) bake=$2 && shift 2 ;;
        *) shift ;;
        esac
    done
    jq -r '.target | to_entries[] | "  \(.key) \(.value.platforms | join(",")) \(.value.output | join(","))"' \
        "${bake}" >>"${stubs}/calls"
    if [[ -e "${stubs}/metadata" ]]; then
        cp "${stubs}/metadata" "${metadata}"
    fi
    ;;
"docker image inspect --format {{.Id}} "*)
    awk -v ref="${!#}" '$1 == ref { print $2; found = 1 } END { exit !found }' "${stubs}/ids"
    ;;
*" boundary verify")
    cat "${stubs}/build"
    ;;
"docker run "*)
    touch "${stubs}/containers/${container}" "${stubs}/ran"
    if [[ -e "${stubs}/hang" ]]; then
        echo "$$" >"${stubs}/client"
        exec sleep 60
    fi
    if [[ ! -e "${stubs}/linger" ]]; then
        rm "${stubs}/containers/${container}"
    fi
    ;;
"docker ps "*)
    ls "${stubs}/containers"
    ;;
"docker rm -f "*)
    rm -f "${stubs}/containers/${!#}"
    ;;
esac
"#;

struct Setup {
    dir: PathBuf,
    stubs: PathBuf,
    package: PathBuf,
}

/// A package of both architectures recorded from `COMMIT`, and a docker whose
/// bake builds `VMM_ID`, tagged `v1.2.3`, which reports `COMMIT`.
fn setup() -> Setup {
    let dir = std::env::temp_dir().join(format!(
        "connector-vmm-publish-{}",
        connector_vmm_tests::run::random_hex()
    ));
    let stubs = dir.join("stubs");
    let package = dir.join("package");
    for path in [&stubs.join("containers"), &dir.join("tmp")] {
        std::fs::create_dir_all(path).unwrap();
    }
    for arch in ["amd64", "arm64"] {
        std::fs::create_dir_all(package.join(arch)).unwrap();
        for file in ["flow-connector-vmm", "flow-guest-init", "flowctl"] {
            std::fs::write(package.join(arch).join(file), format!("{arch} {file}\n")).unwrap();
        }
        std::fs::write(
            package.join(format!("{arch}.build")),
            format!("source {COMMIT}\nflowctl {COMMIT}\nflowctl-go {COMMIT}\n"),
        )
        .unwrap();
        let status = std::process::Command::new("bash")
            .args(["-c", r#"sha256sum "$1"/* >"$1.sha256""#, "sums", arch])
            .current_dir(&package)
            .status()
            .unwrap();
        assert!(status.success());
    }
    std::fs::write(package.join("tags"), "v1.2.3\n").unwrap();

    let docker = stubs.join("docker");
    std::fs::write(&docker, DOCKER).unwrap();
    std::fs::set_permissions(&docker, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let setup = Setup {
        dir,
        stubs,
        package,
    };
    control(
        &setup,
        "metadata",
        &format!(
            r#"{{"connector-vmm": {{"containerimage.config.digest": "{VMM_ID}", "containerimage.digest": "sha256:bbbb"}}}}"#
        ),
    );
    control(
        &setup,
        "ids",
        &format!("ghcr.io/estuary/connector-vmm:v1.2.3 {VMM_ID}\n"),
    );
    control(
        &setup,
        "build",
        &format!("flow-connector-vmm build {COMMIT}\n"),
    );
    setup
}

fn control(setup: &Setup, file: &str, content: &str) {
    std::fs::write(setup.stubs.join(file), content).unwrap();
}

fn cleanup(setup: &Setup) {
    std::fs::remove_dir_all(&setup.dir).unwrap();
}

/// What a run of a script did: its status, what it said on stderr, and the
/// calls it made of docker.
struct Run {
    status: i32,
    stderr: String,
    calls: String,
}

/// Run `script` with `args` against the stand-in docker, its package and its
/// gate container's name, and `envs`.
fn run(setup: &Setup, script: &str, args: &[&str], envs: &[(&str, &str)]) -> Run {
    let output = command(setup, script, args, envs)
        .output()
        .expect("running bash");
    finish(setup, output.status, &output.stderr)
}

fn command(
    setup: &Setup,
    script: &str,
    args: &[&str],
    envs: &[(&str, &str)],
) -> std::process::Command {
    let mut command = std::process::Command::new("bash");
    command
        .arg(script)
        .args(args)
        .current_dir(REPO)
        .env("PACKAGE_DIR", &setup.package)
        .env("TMPDIR", setup.dir.join("tmp"))
        .env("CONNECTOR_VMM_GATE_NAME", GATE_NAME)
        // So that what sort and comm compare doesn't depend on the host's locale.
        .env("LC_ALL", "C")
        .env(
            "PATH",
            format!(
                "{}:{}",
                setup.stubs.display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env_remove("PUSH")
        .env_remove("EXPECTED_COMMIT")
        .env_remove("CONNECTOR_VMM_GATE_ENGINE")
        .env_remove("CONNECTOR_VMM_GATE_TIMEOUT")
        .envs(envs.iter().copied());
    command
}

fn finish(setup: &Setup, status: std::process::ExitStatus, stderr: &[u8]) -> Run {
    let calls_path = setup.stubs.join("calls");
    let calls = std::fs::read_to_string(&calls_path).unwrap_or_default();
    let _ = std::fs::remove_file(&calls_path);
    let normalize = |text: &str| {
        let text = text.replace(&setup.dir.display().to_string(), "<tmp>");
        // mktemp's names.
        regex::Regex::new(r"tmp\.[A-Za-z0-9]{10}")
            .unwrap()
            .replace_all(&text, "tmp.<random>")
            .into_owned()
    };
    Run {
        status: status.code().unwrap(),
        stderr: normalize(&String::from_utf8_lossy(stderr)),
        calls: normalize(&calls),
    }
}

fn report(run: &Run) -> String {
    format!(
        "exit {}\n--- stderr\n{}--- calls\n{}",
        run.status, run.stderr, run.calls
    )
}

/// One line of a table: a run's status, the steps it took of docker in their
/// order, and the last thing it said.
fn row(name: &str, run: &Run) -> String {
    let steps: Vec<String> = run
        .calls
        .lines()
        .filter_map(|call| {
            let reference = call.rsplit(':').next().unwrap();
            let step = if call.starts_with("docker buildx bake ") {
                "bake".to_string()
            } else if call.starts_with("docker image inspect ") {
                format!("id of {reference}")
            } else if call.ends_with(" boundary verify") {
                "verify".to_string()
            } else if call.starts_with("docker run ") {
                "gate".to_string()
            } else if call.starts_with("docker ps ") {
                "list".to_string()
            } else if call.starts_with("docker rm ") {
                "remove".to_string()
            } else if call.starts_with("docker push ") {
                format!("push {reference}")
            } else {
                // Builder selection, and the bake's targets.
                return None;
            };
            Some(step)
        })
        .collect();
    let steps = if steps.is_empty() {
        "no calls of docker".to_string()
    } else {
        steps.join(", ")
    };
    let said = run.stderr.lines().last().unwrap_or("nothing on stderr");
    format!("{name}: exit {}; {steps}; {said}\n", run.status)
}

fn publish(setup: &Setup, expected: &str, args: &[&str]) -> Run {
    run(
        setup,
        DOCKER_IMAGES,
        args,
        &[("PUSH", "true"), ("EXPECTED_COMMIT", expected)],
    )
}

#[test]
fn the_gated_vmm_image_is_pushed_by_its_id_after_the_others() {
    let setup = setup();
    insta::assert_snapshot!(report(&publish(&setup, COMMIT, &[])), @r"
    exit 0
    --- stderr
    --- calls
    docker buildx inspect flow-multiarch
    docker buildx use flow-multiarch
    docker buildx bake --allow=fs.read=<tmp>/package --metadata-file <tmp>/tmp/tmp.<random> -f <tmp>/tmp/tmp.<random>
      capture-python linux/amd64,linux/arm64 type=registry
      config-encryption linux/amd64,linux/arm64 type=registry
      connector-vmm linux/amd64 type=docker
      control-plane-agent linux/amd64,linux/arm64 type=registry
      data-plane-controller linux/amd64,linux/arm64 type=registry
      dekaf linux/amd64,linux/arm64 type=registry
      derive-python linux/amd64,linux/arm64 type=registry
      derive-typescript linux/amd64,linux/arm64 type=registry
      gazette-broker linux/amd64,linux/arm64 type=registry
      materialize-python linux/amd64,linux/arm64 type=registry
      oidc-discovery-server linux/amd64,linux/arm64 type=registry
      reactor linux/amd64,linux/arm64 type=registry
    docker image inspect --format {{.Id}} ghcr.io/estuary/connector-vmm:v1.2.3
    docker ps -a --filter name=connector-vmm-gate-test --format {{.Names}}
    docker run --rm --name connector-vmm-gate-test --network=none --log-driver=none --read-only --cap-drop=all --cap-add=CAP_NET_ADMIN --entrypoint=/bin/bash sha256:aaaa -c <script>
    docker ps -a --filter name=connector-vmm-gate-test --format {{.Names}}
    docker run --rm --network=none --cap-drop=all --cap-add=CAP_NET_ADMIN sha256:aaaa boundary verify
    docker image inspect --format {{.Id}} ghcr.io/estuary/connector-vmm:v1.2.3
    docker image inspect --format {{.Id}} ghcr.io/estuary/connector-vmm:v1.2.3
    docker push --quiet ghcr.io/estuary/connector-vmm:v1.2.3
    ");
    cleanup(&setup);
}

#[test]
fn nothing_reaches_docker_unless_the_publication_is_exactly_the_package() {
    let mut table = String::new();
    let mut refused = |name: &str, setup: Setup, expected: &str, args: &[&str]| {
        table.push_str(&row(name, &publish(&setup, expected, args)));
        cleanup(&setup);
    };

    refused(
        "bake arguments",
        setup(),
        COMMIT,
        &["--set", "*.output=type=registry"],
    );
    refused("no expected commit", setup(), "", &[]);

    let changed = setup();
    std::fs::write(changed.package.join("amd64/flowctl"), "another flowctl\n").unwrap();
    refused("a changed file", changed, COMMIT, &[]);

    let unlisted = setup();
    std::fs::write(unlisted.package.join("arm64/flow-connector-vmm-test"), "\n").unwrap();
    refused("an unlisted file", unlisted, COMMIT, &[]);

    let missing = setup();
    std::fs::remove_dir_all(missing.package.join("arm64")).unwrap();
    for record in ["arm64.sha256", "arm64.build"] {
        std::fs::remove_file(missing.package.join(record)).unwrap();
    }
    refused("a missing architecture", missing, COMMIT, &[]);

    refused("another commit's package", setup(), OTHER_COMMIT, &[]);

    let dirty = setup();
    std::fs::write(
        dirty.package.join("amd64.build"),
        format!("source {COMMIT}-dirty\nflowctl {COMMIT}-dirty\nflowctl-go {COMMIT}-dirty\n"),
    )
    .unwrap();
    refused("a modified checkout's package", dirty, COMMIT, &[]);

    let empty = setup();
    std::fs::write(empty.package.join("amd64.build"), "").unwrap();
    refused("an empty record", empty, COMMIT, &[]);

    for (name, record) in [
        ("a record of the source alone", format!("source {COMMIT}\n")),
        (
            "a record missing a launcher",
            format!("source {COMMIT}\nflowctl {COMMIT}\n"),
        ),
        (
            "a record with a line twice",
            format!("source {COMMIT}\nflowctl {COMMIT}\nflowctl {COMMIT}\nflowctl-go {COMMIT}\n"),
        ),
        (
            "a malformed record",
            format!("source {COMMIT}\nflowctl {COMMIT} extra\nflowctl-go {COMMIT}\n"),
        ),
    ] {
        let partial = setup();
        std::fs::write(partial.package.join("amd64.build"), record).unwrap();
        refused(name, partial, COMMIT, &[]);
    }

    // Everything else agrees, as it would for a publication of a modified
    // checkout which expects that checkout's own identity.
    let modified = setup();
    let dirty = format!("{COMMIT}-dirty");
    for arch in ["amd64", "arm64"] {
        std::fs::write(
            modified.package.join(format!("{arch}.build")),
            format!("source {dirty}\nflowctl {dirty}\nflowctl-go {dirty}\n"),
        )
        .unwrap();
    }
    control(
        &modified,
        "build",
        &format!("flow-connector-vmm build {dirty}\n"),
    );
    refused(
        "a modified checkout's identity, expected",
        modified,
        &dirty,
        &[],
    );
    refused(
        "an abbreviated commit, expected",
        setup(),
        &COMMIT[..12],
        &[],
    );

    insta::assert_snapshot!(table, @r"
    bake arguments: exit 1; no calls of docker; Error: refusing custom bake arguments when pushing: --set *.output=type=registry
    no expected commit: exit 1; no calls of docker; Error: pushing requires EXPECTED_COMMIT, the commit the package was built from
    a changed file: exit 1; no calls of docker; Error: the amd64 package differs from <tmp>/package/amd64.sha256
    an unlisted file: exit 1; no calls of docker; Error: the arm64 package holds files <tmp>/package/arm64.sha256 does not list: arm64/flow-connector-vmm-test
    a missing architecture: exit 1; no calls of docker; Error: the arm64 package has no <tmp>/package/arm64.sha256 and <tmp>/package/arm64.build; run ci:package
    another commit's package: exit 1; no calls of docker;   flowctl-go 0123456789abcdef0123456789abcdef01234567
    a modified checkout's package: exit 1; no calls of docker;   flowctl-go 0123456789abcdef0123456789abcdef01234567-dirty
    an empty record: exit 1; no calls of docker; Error: the amd64 package was not built from 0123456789abcdef0123456789abcdef01234567:
    a record of the source alone: exit 1; no calls of docker;   source 0123456789abcdef0123456789abcdef01234567
    a record missing a launcher: exit 1; no calls of docker;   flowctl 0123456789abcdef0123456789abcdef01234567
    a record with a line twice: exit 1; no calls of docker;   flowctl-go 0123456789abcdef0123456789abcdef01234567
    a malformed record: exit 1; no calls of docker;   flowctl-go 0123456789abcdef0123456789abcdef01234567
    a modified checkout's identity, expected: exit 1; no calls of docker; Error: EXPECTED_COMMIT '0123456789abcdef0123456789abcdef01234567-dirty' is not a full commit ID
    an abbreviated commit, expected: exit 1; no calls of docker; Error: EXPECTED_COMMIT '0123456789ab' is not a full commit ID
    ");
}

#[test]
fn no_vmm_image_is_pushed_unless_the_bakes_image_passed_and_reports_the_commit() {
    let mut table = String::new();
    let mut unpushed = |name: &str, setup: Setup| {
        let run = publish(&setup, COMMIT, &[]);
        // The other images were pushed by the bake regardless.
        assert!(
            run.calls
                .contains("\n  reactor linux/amd64,linux/arm64 type=registry\n"),
            "{}",
            report(&run)
        );
        table.push_str(&row(name, &run));
        cleanup(&setup);
    };

    let failed = setup();
    control(&failed, "faults", "1 docker run --rm --name\n");
    unpushed("a failed gate", failed);

    let unbuilt = setup();
    control(&unbuilt, "metadata", "{}");
    unpushed("no image in the bake's metadata", unbuilt);

    let other = setup();
    control(
        &other,
        "ids",
        "ghcr.io/estuary/connector-vmm:v1.2.3 sha256:cccc\n",
    );
    unpushed("a tag naming another image", other);

    let moved = setup();
    std::fs::write(moved.package.join("tags"), "v1.2.3,latest-dev\n").unwrap();
    control(
        &moved,
        "ids",
        &format!(
            "ghcr.io/estuary/connector-vmm:v1.2.3 {VMM_ID}\nghcr.io/estuary/connector-vmm:latest-dev sha256:cccc\n"
        ),
    );
    unpushed("a second tag moved", moved);

    for (name, build) in [
        ("no build reported", String::new()),
        (
            "a modified checkout's build",
            format!("flow-connector-vmm build {COMMIT}-dirty\n"),
        ),
        (
            "another commit's build",
            format!("flow-connector-vmm build {OTHER_COMMIT}\n"),
        ),
    ] {
        let reported = setup();
        control(&reported, "build", &build);
        unpushed(name, reported);
    }

    insta::assert_snapshot!(table, @r#"
    a failed gate: exit 1; bake, id of v1.2.3, list, gate, list; ci:connector-vmm-gate: sha256:aaaa failed its gate
    no image in the bake's metadata: exit 1; bake; Error: the bake's metadata names no connector-vmm image, so none can be gated
    a tag naming another image: exit 1; bake, id of v1.2.3; Error: ghcr.io/estuary/connector-vmm:v1.2.3 is not the image the bake built
    a second tag moved: exit 1; bake, id of v1.2.3, list, gate, list, verify, id of v1.2.3, id of latest-dev; Error: ghcr.io/estuary/connector-vmm:latest-dev is not the gated image sha256:aaaa
    no build reported: exit 1; bake, id of v1.2.3, list, gate, list, verify; Error: sha256:aaaa reports '', not the expected commit 0123456789abcdef0123456789abcdef01234567
    a modified checkout's build: exit 1; bake, id of v1.2.3, list, gate, list, verify; Error: sha256:aaaa reports 'flow-connector-vmm build 0123456789abcdef0123456789abcdef01234567-dirty', not the expected commit 0123456789abcdef0123456789abcdef01234567
    another commit's build: exit 1; bake, id of v1.2.3, list, gate, list, verify; Error: sha256:aaaa reports 'flow-connector-vmm build 89abcdef0123456789abcdef0123456789abcdef', not the expected commit 0123456789abcdef0123456789abcdef01234567
    "#);
}

#[test]
fn a_local_build_gates_the_vmm_image_unless_its_arguments_select_others() {
    // A local build is of this host's architecture, and only amd64's has a VMM image.
    if std::env::consts::ARCH != "x86_64" {
        return;
    }
    let mut table = String::new();
    let mut local = |name: &str, metadata: &str, args: &[&str]| {
        let setup = setup();
        control(&setup, "metadata", metadata);
        table.push_str(&row(name, &run(&setup, DOCKER_IMAGES, args, &[])));
        cleanup(&setup);
    };
    let vmm = format!(r#"{{"connector-vmm": {{"containerimage.config.digest": "{VMM_ID}"}}}}"#);
    let reactor = r#"{"reactor": {"containerimage.config.digest": "sha256:dddd"}}"#;

    local("every target", &vmm, &[]);
    local("every target, but no VMM image", reactor, &[]);
    local("the VMM image, selected", &vmm, &["connector-vmm"]);
    local("another image, selected", reactor, &["reactor"]);
    // --print builds nothing, and writes no metadata.
    local("the bake's configuration, printed", "", &["--print"]);

    insta::assert_snapshot!(table, @r#"
    every target: exit 0; bake, id of v1.2.3, list, gate, list, verify, id of v1.2.3; nothing on stderr
    every target, but no VMM image: exit 1; bake; Error: the bake's metadata names no connector-vmm image, so none can be gated
    the VMM image, selected: exit 0; bake, id of v1.2.3, list, gate, list, verify, id of v1.2.3; nothing on stderr
    another image, selected: exit 0; bake; nothing on stderr
    the bake's configuration, printed: exit 0; bake; nothing on stderr
    "#);
}

#[test]
fn the_gate_removes_its_container_however_it_ends() {
    let mut table = String::new();
    let mut gate = |name: &str, setup: Setup, envs: &[(&str, &str)]| {
        let run = run(&setup, GATE, &[VMM_ID], envs);
        let left: Vec<_> = std::fs::read_dir(setup.stubs.join("containers"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        table.push_str(&row(name, &run));
        table.push_str(&format!("  containers left: {left:?}\n"));
        cleanup(&setup);
    };

    gate("a gate which passes", setup(), &[]);

    let failed = setup();
    control(&failed, "faults", "1 docker run\n");
    gate("an engine which fails to run it", failed, &[]);

    let lingering = setup();
    control(&lingering, "linger", "");
    gate("a container left behind", lingering, &[]);

    let hung = setup();
    control(&hung, "hang", "");
    gate(
        "a gate which hangs",
        hung,
        &[("CONNECTOR_VMM_GATE_TIMEOUT", "1")],
    );

    let unremovable = setup();
    control(&unremovable, "linger", "");
    control(&unremovable, "faults", "1 docker rm\n");
    gate("a container which cannot be removed", unremovable, &[]);

    let unlisted = setup();
    control(&unlisted, "faults-after-run", "125 docker ps\n");
    gate("containers which cannot be listed", unlisted, &[]);

    let taken = setup();
    std::fs::write(taken.stubs.join("containers").join(GATE_NAME), "").unwrap();
    gate("a name another container has", taken, &[]);

    insta::assert_snapshot!(table, @r#"
    a gate which passes: exit 0; list, gate, list; nothing on stderr
      containers left: []
    an engine which fails to run it: exit 1; list, gate, list; ci:connector-vmm-gate: sha256:aaaa failed its gate
      containers left: []
    a container left behind: exit 0; list, gate, list, remove, list; nothing on stderr
      containers left: []
    a gate which hangs: exit 1; list, gate, list, remove, list; ci:connector-vmm-gate: sha256:aaaa's gate did not finish within 1s
      containers left: []
    a container which cannot be removed: exit 1; list, gate, list, remove, list, remove, list, remove, list; ci:connector-vmm-gate: could not remove container connector-vmm-gate-test
      containers left: ["connector-vmm-gate-test"]
    containers which cannot be listed: exit 1; list, gate, list; ci:connector-vmm-gate: could not list containers, to remove connector-vmm-gate-test
      containers left: []
    a name another container has: exit 1; list; ci:connector-vmm-gate: a container named connector-vmm-gate-test exists, and is not this gate's
      containers left: ["connector-vmm-gate-test"]
    "#);
}

#[test]
fn a_cancelled_gate_stops_its_client_and_removes_its_container() {
    let setup = setup();
    control(&setup, "hang", "");
    let child = command(&setup, GATE, &[VMM_ID], &[])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let client = setup.stubs.join("client");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !client.exists() {
        assert!(std::time::Instant::now() < deadline, "the gate never ran");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let client: i32 = std::fs::read_to_string(&client)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let client = rustix::process::Pid::from_raw(client).expect("a client PID");
    rustix::process::kill_process(
        rustix::process::Pid::from_child(&child),
        rustix::process::Signal::TERM,
    )
    .expect("signalling the publish");
    let output = child.wait_with_output().unwrap();
    let run = finish(&setup, output.status, &output.stderr);

    let client_alive = rustix::process::test_kill_process(client).is_ok();
    let left = std::fs::read_dir(setup.stubs.join("containers"))
        .unwrap()
        .count();
    insta::assert_snapshot!(
        format!("{}client alive: {client_alive}\ncontainers left: {left}\n", row("cancelled", &run)),
        @r#"
        cancelled: exit 143; list, gate, list, remove, list; nothing on stderr
        client alive: false
        containers left: 0
        "#
    );
    cleanup(&setup);
}

/// Stands in for every file a package holds. `vmm-help`, `flowctl` and
/// `flowctl-go` beside the stand-in docker say what the VMM's `run --help`
/// and the launchers' `build-identity` print.
const BINARY: &str = r#"#!/usr/bin/env bash
case "$(basename "$0") $*" in
"flow-connector-vmm run --help") cat "${STUBS}/vmm-help" ;;
"flowctl build-identity") cat "${STUBS}/flowctl" ;;
"flowctl-go build-identity") cat "${STUBS}/flowctl-go" ;;
*) exit 1 ;;
esac
"#;

/// What ci:package links from a platform build, and its checkout: a git
/// repository at its one commit, tagged v1.2.3, which is returned.
fn build(setup: &Setup) -> String {
    let dir = &setup.dir;
    let musl = format!(
        "target/{}-unknown-linux-musl/release",
        std::env::consts::ARCH
    );
    let binaries = [
        ("bin", &["sops", "etcd"][..]),
        (
            &musl,
            &[
                "capture-python",
                "derive-python",
                "derive-typescript",
                "materialize-python",
                "flow-connector-init",
                "flow-guest-init",
                "flow-parser",
            ],
        ),
        (
            "target/release",
            &[
                "agent",
                "flow-config-encryption",
                "flow-connector-vmm",
                "data-plane-controller",
                "dekaf",
                "flowctl",
                "oidc-discovery-server",
                "runtime-sidecar",
            ],
        ),
        ("gobin", &["gazette", "gazctl", "flowctl-go"]),
    ];
    for (parent, names) in binaries {
        std::fs::create_dir_all(dir.join(parent)).unwrap();
        for name in names {
            let path = dir.join(parent).join(name);
            std::fs::write(&path, BINARY).unwrap();
            std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
                .unwrap();
        }
    }
    let checkout = dir.join("checkout");
    for path in [
        "crates/flow-web/pkg/estuary-flow-web-0.0.0.tgz",
        "crates/data-plane-controller/entrypoint.sh",
        "crates/oidc-discovery-server/entrypoint.sh",
        "crates/agent/api-entrypoint.sh",
    ] {
        let path = checkout.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "\n").unwrap();
    }
    git(&checkout, &["init", "--quiet"]);
    git(&checkout, &["add", "."]);
    git(&checkout, &["commit", "--quiet", "-m", "release"]);
    git(&checkout, &["tag", "v1.2.3"]);
    let head = git(&checkout, &["rev-parse", "HEAD"]);

    control(
        setup,
        "vmm-help",
        "Usage: flow-connector-vmm run [OPTIONS]\n      --egress-host <NAME>\n",
    );
    control(setup, "flowctl", &format!("{head}\n"));
    control(setup, "flowctl-go", &format!("{head}\n"));
    head
}

/// git in `dir`, isolated from the user's and system's configuration.
fn git(dir: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .envs(GIT_ENV)
        .output()
        .expect("running git");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

const GIT_ENV: [(&str, &str); 6] = [
    ("GIT_CONFIG_NOSYSTEM", "1"),
    ("GIT_CONFIG_GLOBAL", "/dev/null"),
    ("GIT_AUTHOR_NAME", "acmeCo"),
    ("GIT_AUTHOR_EMAIL", "dev@acmeco.example"),
    ("GIT_COMMITTER_NAME", "acmeCo"),
    ("GIT_COMMITTER_EMAIL", "dev@acmeco.example"),
];

/// The architecture ci:package names this host's package for.
fn package_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        _ => "arm64",
    }
}

/// Run `ci:package` of the build, with `expected` as EXPECTED_COMMIT: its
/// status, what it said, and the build record it wrote, if any.
fn package(setup: &Setup, head: &str, expected: &str) -> (i32, String, Option<String>) {
    let dir = &setup.dir;
    let output = std::process::Command::new("bash")
        .arg(PACKAGE)
        .current_dir(dir.join("checkout"))
        .env("HOME", dir.join("home"))
        .env("CARGO_TARGET_DIR", dir.join("target"))
        .env("GOBIN", dir.join("gobin"))
        .env("STUBS", &setup.stubs)
        .env("EXPECTED_COMMIT", expected)
        .env("LC_ALL", "C")
        .env(
            "PATH",
            format!(
                "{}:{}",
                dir.join("bin").display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .envs(GIT_ENV)
        .output()
        .expect("running bash");
    let record = std::fs::read_to_string(
        dir.join("home/flow-package")
            .join(format!("{}.build", package_arch())),
    )
    .ok();
    let said = String::from_utf8_lossy(&output.stderr).replace(head, "<commit>");
    (
        output.status.code().unwrap(),
        said,
        record.map(|record| record.replace(head, "<commit>")),
    )
}

#[test]
fn a_package_records_its_launchers_builds_for_its_publication() {
    let setup = setup();
    let head = build(&setup);
    let (status, said, record) = package(&setup, &head, &head);
    assert_eq!((status, said.as_str()), (0, ""));
    insta::assert_snapshot!(record.unwrap(), @r"
    source <commit>
    flowctl <commit>
    flowctl-go <commit>
    ");

    // What ci:docker-images then checks the package against: a local build
    // of it, for the commit, passes; one of a changed file of it does not.
    let package = setup.dir.join("home/flow-package");
    control(
        &setup,
        "build",
        &format!("flow-connector-vmm build {head}\n"),
    );
    let publish = |setup: &Setup| {
        let mut command = command(setup, DOCKER_IMAGES, &[], &[("EXPECTED_COMMIT", &head)]);
        let output = command.env("PACKAGE_DIR", &package).output().unwrap();
        finish(setup, output.status, &output.stderr)
    };
    let run = publish(&setup);
    assert_eq!(run.status, 0, "{}", report(&run));
    std::fs::write(
        package.join(package_arch()).join("flowctl"),
        "another flowctl\n",
    )
    .unwrap();
    let run = publish(&setup);
    assert_eq!(run.status, 1, "{}", report(&run));
    assert!(run.calls.is_empty(), "{}", report(&run));
    cleanup(&setup);
}

#[test]
fn a_package_is_refused_unless_built_as_the_expected_commit() {
    let mut table = String::new();
    let mut refused = |name: &str, setup: Setup, head: &str, expected: &str| {
        let (status, said, record) = package(&setup, head, expected);
        table.push_str(&format!(
            "{name}: exit {status}, recorded: {}\n{said}",
            record.is_some()
        ));
        cleanup(&setup);
    };

    let test_controls = setup();
    let head = build(&test_controls);
    control(
        &test_controls,
        "vmm-help",
        "Usage: flow-connector-vmm run [OPTIONS]\n      --exec <PROGRAM>\n",
    );
    refused("a test-controls VMM", test_controls, &head, &head);

    let dirty = setup();
    let head = build(&dirty);
    control(&dirty, "flowctl", &format!("{head}-dirty\n"));
    refused("a modified checkout's flowctl", dirty, &head, &head);

    let other = setup();
    let head = build(&other);
    control(&other, "flowctl-go", &format!("{OTHER_COMMIT}\n"));
    refused("another commit's flowctl-go", other, &head, &head);

    let changed = setup();
    let head = build(&changed);
    std::fs::write(changed.dir.join("checkout/generated.h"), "\n").unwrap();
    std::fs::write(
        changed.dir.join("checkout/crates/agent/api-entrypoint.sh"),
        "#!/bin/sh\n",
    )
    .unwrap();
    refused("a checkout the builds changed", changed, &head, &head);

    let elsewhere = setup();
    let head = build(&elsewhere);
    refused("another commit's checkout", elsewhere, &head, OTHER_COMMIT);

    let modified = setup();
    let head = build(&modified);
    std::fs::write(modified.dir.join("checkout/generated.h"), "\n").unwrap();
    control(&modified, "flowctl", &format!("{head}-dirty\n"));
    control(&modified, "flowctl-go", &format!("{head}-dirty\n"));
    let dirty = format!("{head}-dirty");
    refused(
        "a modified checkout, expecting its own identity",
        modified,
        &head,
        &dirty,
    );

    insta::assert_snapshot!(table, @r"
    a test-controls VMM: exit 1, recorded: false
    flow-connector-vmm accepts --exec: it was built with the test-controls feature
    a modified checkout's flowctl: exit 1, recorded: false
    flowctl was built as <commit>-dirty, not the expected commit <commit>
    another commit's flowctl-go: exit 1, recorded: false
    flowctl-go was built as 89abcdef0123456789abcdef0123456789abcdef, not the expected commit <commit>
    a checkout the builds changed: exit 1, recorded: false
    checkout . is <commit>-dirty, not the expected commit <commit>
       M crates/agent/api-entrypoint.sh
      ?? generated.h
    another commit's checkout: exit 1, recorded: false
    checkout . is <commit>, not the expected commit 89abcdef0123456789abcdef0123456789abcdef
    a modified checkout, expecting its own identity: exit 1, recorded: false
    the expected commit '<commit>-dirty' is not a full commit ID
    ");
}
