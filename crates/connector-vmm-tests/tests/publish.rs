//! `ci:docker-images` and `ci:connector-vmm-gate`, run by bash against a
//! stand-in for docker which logs its calls and answers as each test asks. A
//! VMM image is pushed only once it is the image the bake built and has
//! passed its gate, and only by the ID that passed; the other images publish
//! from the bake, before the gate. Needs no docker, KVM or sudo.

use std::path::PathBuf;

const DOCKER_IMAGES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../mise/tasks/ci/docker-images"
);
const REPO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

const VMM_ID: &str = "sha256:aaaa";
const GATE_NAME: &str = "connector-vmm-gate-test";

/// Stands in for docker. Each line of `faults`, a status and the start of a
/// call, fails the calls it matches. A bake logs its targets and writes
/// `metadata` as its metadata file; `image inspect` answers from `ids`, lines
/// of a reference and its ID. A named container is a file in `containers`
/// while it runs.
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
if [[ -e "${stubs}/faults" ]]; then
    while read -r code start; do
        if [[ "${call}" == "${start}"* ]]; then
            echo "${call}: failing, as the test asks" >&2
            exit "${code}"
        fi
    done <"${stubs}/faults"
fi
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
"docker run "*)
    touch "${stubs}/containers/${container}"
    rm "${stubs}/containers/${container}"
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

/// A package of both architectures, and a docker whose bake builds `VMM_ID`,
/// tagged `v1.2.3`.
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

/// Publish with `ci:docker-images` against the stand-in docker, its package
/// and its gate container's name.
fn publish(setup: &Setup) -> Run {
    let output = std::process::Command::new("bash")
        .arg(DOCKER_IMAGES)
        .current_dir(REPO)
        .env("PACKAGE_DIR", &setup.package)
        .env("TMPDIR", setup.dir.join("tmp"))
        .env("CONNECTOR_VMM_GATE_NAME", GATE_NAME)
        .env("PUSH", "true")
        .env(
            "PATH",
            format!(
                "{}:{}",
                setup.stubs.display(),
                std::env::var("PATH").unwrap()
            ),
        )
        .env_remove("CONNECTOR_VMM_GATE_ENGINE")
        .env_remove("CONNECTOR_VMM_GATE_TIMEOUT")
        .output()
        .expect("running bash");
    finish(setup, output.status, &output.stderr)
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

#[test]
fn no_vmm_image_is_pushed_unless_the_bakes_image_passed_its_gate() {
    let mut table = String::new();
    let mut unpushed = |name: &str, setup: Setup| {
        let run = publish(&setup);
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

    insta::assert_snapshot!(table, @r#"
    a failed gate: exit 1; bake, id of v1.2.3, list, gate, list; ci:connector-vmm-gate: sha256:aaaa failed its gate
    no image in the bake's metadata: exit 1; bake; Error: the bake's metadata names no connector-vmm image, so none can be gated
    a tag naming another image: exit 1; bake, id of v1.2.3; Error: ghcr.io/estuary/connector-vmm:v1.2.3 is not the image the bake built
    a second tag moved: exit 1; bake, id of v1.2.3, list, gate, list, id of v1.2.3, id of latest-dev; Error: ghcr.io/estuary/connector-vmm:latest-dev is not the gated image sha256:aaaa
    "#);
}
