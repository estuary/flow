//! Python tasks of every type previewed and published through flowctl and the
//! reactor, and their files generated from nested catalogs. Run only under
//! `mise run ci:connector-vmm-kvm --platform`, which configures the local stack
//! and writes `platform.json`, one test at a time against that one stack.
//!
//! The legacy derivation runs the published `derive-python:stable`. Projects
//! run this checkout's images, which the task builds as `:local`.

use connector_vmm_tests::{launcher, run};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const DERIVATION: &str = "acmeCo/vmm-python/numbers";

/// The first greetings source-hello-world emits, which the published
/// derivation must have numbered.
const NUMBERED: std::ops::Range<i64> = 0..5;

const EGRESS: &str = "VMM egress permits only these host names: \
    pypi.org, files.pythonhosted.org (connector defaults); example.org (task egress.hosts)";
const REFUSED: &str = "refused DNS name example.com, which this connector's egress does not permit";

/// A command's whole run, VMM boots and dependency installs included.
const COMMAND: Duration = Duration::from_secs(600);
/// From publication to derived documents: shard assignment, a VMM boot and
/// install, and hello-world's rate.
const DERIVED: Duration = Duration::from_secs(600);
const RELEASED: Duration = Duration::from_secs(180);

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Platform {
    run_dir: PathBuf,
    /// Holds the podman (and its `calls`), state directory and TMPDIR which
    /// the reactor and flowctl share.
    dir: String,
    vmm_image: String,
    /// Beside a static `flow-connector-init`, so flowctl's launches find it.
    flowctl: String,
    /// The static build every VMM must be handed in its connector mount.
    connector_init: String,
    derive_python_image: String,
    reactor_unit: String,
    /// This checkout's `examples/`, whose materialization API the tests serve.
    examples: PathBuf,
}

fn load() -> Platform {
    if std::env::var_os("CONNECTOR_VMM_KVM").is_none_or(|value| value.is_empty()) {
        panic!("this runs only under `mise run ci:connector-vmm-kvm --platform`");
    }
    let path = Path::new(run::ROOT).join("platform.json");
    let content = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "reading {}: {e}; this runs under `mise run ci:connector-vmm-kvm --platform`",
            path.display()
        )
    });
    serde_json::from_slice(&content).unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()))
}

fn root(platform: &Platform) -> launcher::Root {
    launcher::Root {
        dir: platform.dir.clone(),
        state_dir: format!("{}/state", platform.dir),
        tmp_dir: format!("{}/tmp", platform.dir),
        podman: format!("{}/podman", platform.dir),
    }
}

struct Output {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}

fn flowctl(platform: &Platform, name: &str, args: &[&str], env: &[(&str, &str)]) -> Output {
    flowctl_in(platform, &root(platform), name, args, env)
}

fn flowctl_status(platform: &Platform, name: &str, args: &[&str], env: &[(&str, &str)]) -> Output {
    flowctl_status_in(platform, &root(platform), name, args, env)
}

/// flowctl, launching its VMMs with the podman, state directory and TMPDIR
/// of `at`. It must succeed.
fn flowctl_in(
    platform: &Platform,
    at: &launcher::Root,
    name: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> Output {
    let output = flowctl_status_in(platform, at, name, args, env);
    assert!(
        output.status.success(),
        "flowctl {} exited {}; see {}.stdout and .stderr in {}\n--- stderr (tail):\n{}",
        args.join(" "),
        output.status,
        name,
        platform.run_dir.display(),
        tail(&output.stderr, 60),
    );
    output
}

/// Run flowctl, keeping what it printed as `<name>.stdout` and `<name>.stderr`.
/// The stack's profile and the tenant's credentials come from the task's
/// environment; the VMM settings from `at`.
fn flowctl_status_in(
    platform: &Platform,
    at: &launcher::Root,
    name: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> Output {
    let stdout = platform.run_dir.join(format!("{name}.stdout"));
    let stderr = platform.run_dir.join(format!("{name}.stderr"));
    let mut child = flowctl_command(platform, at, args, env)
        .stdin(std::process::Stdio::null())
        .stdout(create(&stdout))
        .stderr(create(&stderr))
        .spawn()
        .unwrap_or_else(|e| panic!("spawning flowctl {}: {e}", args.join(" ")));

    let deadline = Instant::now() + COMMAND;
    let status = loop {
        if let Some(status) = child.try_wait().expect("polling flowctl") {
            break status;
        }
        if Instant::now() > deadline {
            // Not left running past a failed test, launching VMMs.
            _ = child.kill();
            _ = child.wait();
            panic!("flowctl {} ran longer than {COMMAND:?}", args.join(" "));
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let read = |path: &Path| {
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
    };
    Output {
        status,
        stdout: read(&stdout),
        stderr: read(&stderr),
    }
}

fn flowctl_command(
    platform: &Platform,
    at: &launcher::Root,
    args: &[&str],
    env: &[(&str, &str)],
) -> std::process::Command {
    let mut command = std::process::Command::new(&platform.flowctl);
    command
        .args(args)
        .env("CONNECTOR_VMM_IMAGE", &platform.vmm_image)
        .env("CONNECTOR_VMM_PODMAN", &at.podman)
        .env("CONNECTOR_VMM_STATE_DIR", &at.state_dir)
        .env("TMPDIR", &at.tmp_dir)
        .envs(env.iter().copied());
    command
}

/// What the stack's reactor serves at `/debug/metrics`. Its unit is
/// `flow-reactor@<data plane>-<port>.service`, and its host
/// `reactor-<data plane>.flow.localhost`.
fn reactor_metrics(platform: &Platform) -> String {
    let instance = platform
        .reactor_unit
        .strip_prefix("flow-reactor@")
        .and_then(|unit| unit.strip_suffix(".service"))
        .unwrap_or_else(|| panic!("a reactor unit, not {}", platform.reactor_unit));
    let (data_plane, port) = instance.rsplit_once('-').expect("<data plane>-<port>");
    let url = format!("https://reactor-{data_plane}.flow.localhost:{port}/debug/metrics");
    let ca = std::env::var("SSL_CERT_FILE").expect("the task sets SSL_CERT_FILE");
    let output = std::process::Command::new("curl")
        .args(["-fsS", "--max-time", "30", "--cacert", &ca, &url])
        .output()
        .unwrap_or_else(|e| panic!("running curl {url}: {e}"));
    assert!(
        output.status.success(),
        "curl {url}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("UTF-8 metrics")
}

fn create(path: &Path) -> std::fs::File {
    std::fs::File::create(path).unwrap_or_else(|e| panic!("creating {}: {e}", path.display()))
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

fn started(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter(|line| line.contains("started connector container"))
        .flat_map(vmm_names)
        .collect()
}

/// Launch names, `fv_<16 hex>`, as they appear anywhere in `text`.
fn vmm_names(text: &str) -> BTreeSet<String> {
    regex::Regex::new(r"fv_[0-9a-f]{16}")
        .unwrap()
        .find_iter(text)
        .map(|found| found.as_str().to_string())
        .collect()
}

/// Each launch creates one network; preserve launch order.
fn launched(calls: &[String]) -> Vec<String> {
    let mut names = Vec::new();
    for name in calls
        .iter()
        .filter_map(|call| call.strip_prefix("network create "))
        .flat_map(vmm_names)
    {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// `calls`, with what differs between runs replaced: each launch's id, token
/// and container ID by its ordinal, and this run's directory and VMM image.
fn normalize_calls(platform: &Platform, calls: &[String]) -> String {
    // Launches resolve the VMM image to its ID, and run it by that.
    let vmm_image_id = run::podman(&[
        "image",
        "inspect",
        "--format",
        "{{.Id}}",
        &platform.vmm_image,
    ]);
    let mut text = calls
        .join("\n")
        .replace(&platform.vmm_image, "<vmm image>")
        .replace(vmm_image_id.trim(), "<vmm image id>")
        .replace(&platform.dir, "<platform>");
    text = regex::Regex::new(r"(--opt=image=)(?:sha256:)?[0-9a-f]{64}")
        .unwrap()
        .replace_all(&text, "${1}<connector image id>")
        .into_owned();
    for (pattern, label) in [
        (r"fv_[0-9a-f]{16}", "fv_"),
        (r"fvm[0-9a-f]{12}", "fvm"),
        (r"vmm-owner=[0-9a-f]{32}", "vmm-owner=token"),
        (r"\b[0-9a-f]{64}\b", "container"),
        (r"connector-mounts-[0-9]+", "connector-mounts-"),
    ] {
        let pattern = regex::Regex::new(pattern).unwrap();
        let mut ordinals: BTreeMap<String, usize> = BTreeMap::new();
        for found in pattern.find_iter(&text.clone()) {
            let next = ordinals.len() + 1;
            ordinals.entry(found.as_str().to_string()).or_insert(next);
        }
        for (found, ordinal) in ordinals {
            text = text.replace(&found, &format!("{label}<{ordinal}>"));
        }
    }
    text
}

/// JSON documents, one per line, as flowctl prints them, without `_meta`,
/// whose UUIDs differ between runs.
fn documents(stdout: &str) -> Vec<serde_json::Value> {
    stdout
        .lines()
        .map(|line| {
            let mut doc: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("parsing {line:?}: {e}"));
            let fields = match &mut doc {
                serde_json::Value::Array(pair) => &mut pair[1],
                doc => doc,
            };
            fields
                .as_object_mut()
                .expect("a document is an object")
                .remove("_meta");
            doc
        })
        .collect()
}

fn files(dir: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in
            std::fs::read_dir(&next).unwrap_or_else(|e| panic!("reading {}: {e}", next.display()))
        {
            let path = entry.expect("reading a directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path.strip_prefix(dir).expect("a path underneath dir");
                found.push(relative.to_string_lossy().into_owned());
            }
        }
    }
    found.sort();
    found
}

fn assert_vmm_logs(what: &str, logs: &str) {
    for expected in [EGRESS, "started connector container", REFUSED] {
        assert!(logs.contains(expected), "{what} lacks {expected:?}");
    }
}

#[test]
fn a_python_derivation_through_the_platform() {
    let platform = load();
    let root = root(&platform);
    std::fs::write(
        format!("{}/expected-connector-init", platform.dir),
        &platform.connector_init,
    )
    .expect("configuring the wrapper's mount check");
    let mut evidence = Vec::new();
    // Earlier tests share the wrapper's records, which this test's checks of
    // every launch skip.
    let first = launcher::calls(&root).len();
    let first_mount = std::fs::read_to_string(format!("{}/checked-mounts", platform.dir))
        .map(|checked| checked.lines().count())
        .unwrap_or_default();

    let before = launcher::calls(&root).len();
    let preview = flowctl(
        &platform,
        "preview",
        &[
            "raw",
            "preview-next",
            "--source",
            &run::fixture("platform/numbers.flow.yaml").to_string_lossy(),
            "--task",
            DERIVATION,
            "--fixture",
            &run::fixture("platform/greetings.ndjson").to_string_lossy(),
            "--log-json",
        ],
        &[],
    );
    let calls = launcher::calls(&root)[before..].to_vec();
    let previewed = launched(&calls);
    let [validated, session] = &previewed[..] else {
        panic!("preview launches a Validate and a session: {calls:#?}");
    };
    assert_eq!(
        started(&preview.stderr),
        previewed.iter().cloned().collect()
    );
    assert_eq!(
        preview.stderr.matches(EGRESS).count(),
        2,
        "each launch logs its egress"
    );
    assert_vmm_logs("preview", &preview.stderr);
    insta::assert_json_snapshot!("preview", documents(&preview.stdout));

    // Validate awaits teardown. Preview's session ends with the command, and
    // flowctl awaits its teardown before it exits.
    for name in [validated, session] {
        assert_eq!(
            launcher::left(&root.state_dir, &root.tmp_dir, name),
            Vec::<String>::new()
        );
    }
    assert!(
        !preview
            .stderr
            .contains("VMM teardowns did not finish before exit"),
        "preview's teardowns finished"
    );
    // Through the session's start, less recovery's listings, which the
    // reactor's and flowctl's passes interleave.
    let session_start = calls
        .iter()
        .rposition(|call| call.starts_with("start --attach "))
        .expect("the session started");
    let launches: Vec<String> = calls[..=session_start]
        .iter()
        .filter(|call| !call.contains("--filter label=dev.estuary.vmm-state-dir="))
        .cloned()
        .collect();
    insta::assert_snapshot!("preview_calls", normalize_calls(&platform, &launches));
    evidence.push(format!(
        "preview: Validate {validated} and session {session} released before flowctl exited"
    ));

    // Publication: flowctl validates the derivation locally (its stderr), then
    // the agent has the stack's reactor validate it (the publication's logs,
    // on its stdout). The reactor then runs the task.
    let before = launcher::calls(&root).len();
    let publish = flowctl(
        &platform,
        "publish",
        &[
            "catalog",
            "publish",
            "--source",
            &run::fixture("platform/flow.yaml").to_string_lossy(),
            "--auto-approve",
        ],
        // flowctl's own Validate logs its launch at info.
        &[("RUST_LOG", "info")],
    );
    let local = started(&publish.stderr);
    let agent = started(&publish.stdout);
    assert_eq!(local.len(), 1, "flowctl validated once, in a VMM");
    assert_eq!(agent.len(), 1, "the reactor validated once, in a VMM");
    assert!(local.is_disjoint(&agent));
    assert_eq!(publish.stderr.matches(EGRESS).count(), 1);
    assert_eq!(publish.stdout.matches(EGRESS).count(), 1);
    evidence.push(format!("publication: flowctl {local:?}, reactor {agent:?}"));

    let numbered = run::eventually("the derivation's documents", DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        // Fails until the shard has made the collection's journal.
        let read = flowctl_status(
            &platform,
            "numbers",
            &["collections", "read", "--collection", DERIVATION],
            &[],
        );
        if !read.status.success() {
            return None;
        }
        let docs: BTreeMap<i64, serde_json::Value> = documents(&read.stdout)
            .into_iter()
            .map(|doc| (doc["n"].as_i64().expect("n is an integer"), doc))
            .collect();
        NUMBERED.clone().all(|n| docs.contains_key(&n)).then(|| {
            NUMBERED
                .clone()
                .map(|n| docs[&n].clone())
                .collect::<Vec<_>>()
        })
    });
    insta::assert_json_snapshot!("published", numbered);

    // The task's logs reach their journal on a schedule of their own.
    let logs = run::eventually("the task's logs of its VMM", DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        let logs = flowctl_status(&platform, "task-logs", &["logs", "--task", DERIVATION], &[]);
        [EGRESS, "started connector container", REFUSED, "Installed"]
            .iter()
            .all(|line| logs.status.success() && logs.stdout.contains(line))
            .then_some(logs)
    });
    let opened = started(&logs.stdout);
    assert!(!opened.is_empty(), "the shard's sessions name their VMMs");
    assert!(opened.is_disjoint(&agent) && opened.is_disjoint(&local));
    assert_vmm_logs("the task's logs", &logs.stdout);
    // uv names only its larger downloads; the documents carry humanize's
    // version.
    assert!(
        logs.stdout.contains("Installed"),
        "the task's logs lack uv's installation"
    );
    evidence.push(format!("reactor sessions: {opened:?}"));

    let calls = launcher::calls(&root)[before..].to_vec();
    let published: BTreeSet<String> = launched(&calls).into_iter().collect();
    let expected: BTreeSet<String> = local.iter().chain(&agent).chain(&opened).cloned().collect();
    assert_eq!(
        published, expected,
        "every VMM since publication was one of these launches"
    );

    let identity = run::podman(&[
        "image",
        "inspect",
        "--format",
        "{{.Id}} {{.Digest}}",
        &platform.derive_python_image,
    ]);
    evidence.push(format!(
        "{}: {}; humanize {}",
        platform.derive_python_image,
        identity.trim(),
        numbered[0]["humanize"]
    ));

    // Shard deletion drops the launch's runtime before teardown. No launch
    // follows, and this flowctl has no VMM capability, so recovers nothing:
    // the reactor's own recovery releases what its sessions left.
    let before = launcher::calls(&root).len();
    flowctl(
        &platform,
        "delete",
        &[
            "catalog",
            "delete",
            "--prefix",
            "acmeCo/vmm-python/",
            "--dangerous-auto-approve",
        ],
        &[("CONNECTOR_VMM_STATE_DIR", "")],
    );
    let deadline = Instant::now() + RELEASED;
    for name in &opened {
        run::eventually(
            &format!("the reactor's recovery to release {name}"),
            deadline.saturating_duration_since(Instant::now()),
            || {
                launcher::left(&root.state_dir, &root.tmp_dir, name)
                    .is_empty()
                    .then_some(())
            },
        );
    }
    let since = launcher::calls(&root)[before..].to_vec();
    assert_eq!(launched(&since), Vec::<String>::new(), "with no launch");
    // Every other launch's owner awaited its teardown.
    let launches = launched(&launcher::calls(&root)[first..]);
    for name in &launches {
        assert_eq!(
            launcher::left(&root.state_dir, &root.tmp_dir, name),
            Vec::<String>::new()
        );
    }
    evidence.push(format!(
        "after deletion the reactor's recovery released its sessions {opened:?}, with no launch"
    ));
    let metrics = reactor_metrics(&platform);
    let value = |name: &str| {
        metrics
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name} ")))
            .unwrap_or_else(|| panic!("the reactor exports {name}:\n{metrics}"))
            .parse::<f64>()
            .unwrap_or_else(|e| panic!("{name}: {e}"))
    };
    assert!(value("flow_vmm_recovery_last_success_timestamp_seconds") > 0.0);
    evidence.push(format!(
        "the reactor's recovery metrics: {}",
        metrics
            .lines()
            .filter(|line| line.starts_with("flow_vmm_recovery_"))
            .collect::<Vec<_>>()
            .join("; ")
    ));

    // A Spec beneath a state directory of its own, holding a dead launch's
    // record which owns nothing. No reactor recovers there: the invocation's
    // own recovery releases it, and its launch is released before it exits.
    let at = launcher::root();
    let mounts = launcher::mounts(&at);
    let dead = "fv_0000000000000001";
    let token = "00000000000000000000000000000001";
    std::fs::write(
        launcher::record(&at.state_dir, dead),
        format!(
            "{}\n",
            serde_json::json!({
                "version": 1,
                "id": dead,
                "token": token,
                "mount": format!("{mounts}/mount-{dead}"),
            })
        ),
    )
    .unwrap();
    flowctl_in(
        &platform,
        &at,
        "spec",
        &[
            "raw",
            "spec",
            "--source",
            &run::fixture("platform/numbers.flow.yaml").to_string_lossy(),
            "--task",
            DERIVATION,
        ],
        &[],
    );
    let calls = launcher::calls(&at);
    let respecified = launched(&calls);
    let [respecified] = &respecified[..] else {
        panic!("one launch for the Spec: {respecified:?}");
    };
    assert!(
        calls
            .iter()
            .any(|call| call.contains(&format!("vmm-owner={token}"))),
        "flowctl's recovery released {dead}: {calls:#?}"
    );
    for name in [dead, respecified.as_str()] {
        assert_eq!(
            launcher::left(&at.state_dir, &at.tmp_dir, name),
            Vec::<String>::new()
        );
    }
    evidence.push(format!(
        "flowctl's own recovery released {dead}; its Spec's launch {respecified} was released \
         before it exited"
    ));

    // A catalog which imports the derivation from a subdirectory. The
    // derivation's project root is its specification's directory, so its
    // generated files belong there and not beside the importing catalog.
    let project = platform.run_dir.join("generate");
    let nested = project.join("vmm-python");
    std::fs::create_dir_all(&nested)
        .unwrap_or_else(|e| panic!("creating {}: {e}", nested.display()));
    for file in ["numbers.flow.yaml", "numbers.py", "greetings.flow.yaml"] {
        let from = run::fixture(&format!("platform/{file}"));
        std::fs::copy(&from, nested.join(file))
            .unwrap_or_else(|e| panic!("copying {}: {e}", from.display()));
    }
    std::fs::write(
        project.join("flow.yaml"),
        "import:\n  - vmm-python/numbers.flow.yaml\n",
    )
    .expect("writing the importing catalog");

    let before = launcher::calls(&root).len();
    flowctl(
        &platform,
        "generate",
        &[
            "generate",
            "--source",
            &project.join("flow.yaml").to_string_lossy(),
        ],
        &[],
    );
    let generated = launched(&launcher::calls(&root)[before..]);
    let [generated] = &generated[..] else {
        panic!("one launch for the Validate: {generated:?}");
    };
    insta::assert_snapshot!("generated", files(&project).join("\n"));
    evidence.push(format!(
        "generate: Validate {generated} placed its files beside the nested specification"
    ));

    let launches = launched(&launcher::calls(&root)[first..]);
    for name in &launches {
        assert_eq!(
            launcher::left(&root.state_dir, &root.tmp_dir, name),
            Vec::<String>::new()
        );
    }
    let state: Vec<_> = std::fs::read_dir(&root.state_dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", root.state_dir))
        .map(|entry| entry.expect("reading the state directory").path())
        .collect();
    assert!(state.is_empty(), "the state directory holds {state:?}");
    let all = launcher::calls(&root)[first..].to_vec();

    let observed = std::fs::read_to_string(format!("{}/checked-mounts", platform.dir))
        .expect("reading the wrapper's mount checks");
    assert_eq!(
        observed
            .lines()
            .skip(first_mount)
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        launches.iter().cloned().collect(),
        "every launch received the static connector-init"
    );
    // Each launch runs the VMM image by the ID it resolved the name to.
    let vmm_image_id = run::podman(&[
        "image",
        "inspect",
        "--format",
        "{{.Id}}",
        &platform.vmm_image,
    ]);
    for call in all.iter().filter(|call| call.starts_with("create ")) {
        assert!(
            call.contains(&format!(" {} run ", vmm_image_id.trim())),
            "{call}"
        );
        assert!(call.contains("--label=dev.estuary.vmm-owner="), "{call}");
        assert!(
            call.contains(&format!("--label=task-name={DERIVATION}"))
                || call.contains("--label=task-name=<spec>"),
            "{call}"
        );
    }
    for flag in ["--debug", "--resolver-upstream", "--exec"] {
        assert!(
            !all.iter().any(|call| call.contains(flag)),
            "no launch is given {flag}"
        );
    }
    evidence.push(format!(
        "{} launches, each handed {} and released; reactor unit {}",
        launches.len(),
        platform.connector_init,
        platform.reactor_unit
    ));

    let summary = evidence.join("\n");
    std::fs::write(platform.run_dir.join("platform.evidence"), &summary)
        .expect("writing the evidence summary");
    eprintln!("{summary}");
}

const CAPTURE: &str = "acmeCo/vmm-python/source-hello";
const HOWDIES: &str = "acmeCo/vmm-python/howdies";
const GREETINGS: &str = "acmeCo/vmm-python/greetings";
const MATERIALIZATION: &str = "acmeCo/vmm-python/materialize-api";
/// Any image with Python 3.14 serves the API: this one is already built.
const API_IMAGE: &str = "ghcr.io/estuary/capture-python:local";

/// Every launch's connector mount must hold the static connector-init.
fn expect_connector_init(platform: &Platform) {
    std::fs::write(
        format!("{}/expected-connector-init", platform.dir),
        &platform.connector_init,
    )
    .expect("configuring the wrapper's mount check");
}

/// Copy fixture directory `from` to `to`, without what a developer's own
/// runs may have left in it.
fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap_or_else(|e| panic!("creating {}: {e}", to.display()));
    for entry in
        std::fs::read_dir(from).unwrap_or_else(|e| panic!("reading {}: {e}", from.display()))
    {
        let entry = entry.expect("reading a directory entry");
        let name = entry.file_name();
        if ["flow_generated", ".venv", "__pycache__"].contains(&name.to_str().unwrap_or_default()) {
            continue;
        }
        let (from, to) = (entry.path(), to.join(&name));
        if from.is_dir() {
            copy_tree(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap_or_else(|e| panic!("copying {}: {e}", from.display()));
        }
    }
}

fn render(path: &Path, from: &str, to: &str) {
    let content =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
    std::fs::write(path, content.replace(from, to))
        .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

/// The documents of `collection`, once its journal exists.
fn read(platform: &Platform, name: &str, collection: &str) -> Option<Vec<serde_json::Value>> {
    let read = flowctl_status(
        platform,
        name,
        &["collections", "read", "--collection", collection],
        &[],
    );
    read.status.success().then(|| documents(&read.stdout))
}

/// The running VMM containers of `task`, by name.
fn running(task: &str) -> BTreeSet<String> {
    run::podman(&[
        "ps",
        "--filter",
        &format!("label=task-name={task}"),
        "--format",
        "{{.Names}}",
    ])
    .lines()
    .map(str::trim)
    .filter(|name| !name.is_empty())
    .map(ToString::to_string)
    .collect()
}

/// The task's logs, once they hold every line of `expected`.
fn task_logs(platform: &Platform, name: &str, task: &str, expected: &[&str]) -> Output {
    run::eventually(&format!("the logs of {task}"), DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        let logs = flowctl_status(platform, name, &["logs", "--task", task], &[]);
        (logs.status.success() && expected.iter().all(|line| logs.stdout.contains(line)))
            .then_some(logs)
    })
}

/// Delete every task of the tests' prefix, with a flowctl which has no VMM
/// capability and so recovers nothing. The reactor's own recovery releases
/// what its sessions left, and every launch so far is then gone.
fn delete_and_release(platform: &Platform, name: &str) {
    let root = root(platform);
    let before = launcher::calls(&root).len();
    flowctl(
        platform,
        name,
        &[
            "catalog",
            "delete",
            "--prefix",
            "acmeCo/vmm-python/",
            "--dangerous-auto-approve",
        ],
        &[("CONNECTOR_VMM_STATE_DIR", "")],
    );
    let deadline = Instant::now() + RELEASED;
    for vmm in launched(&launcher::calls(&root)) {
        run::eventually(
            &format!("the release of {vmm}"),
            deadline.saturating_duration_since(Instant::now()),
            || {
                launcher::left(&root.state_dir, &root.tmp_dir, &vmm)
                    .is_empty()
                    .then_some(())
            },
        );
    }
    assert_eq!(
        launched(&launcher::calls(&root)[before..]),
        Vec::<String>::new(),
        "deletion launches nothing"
    );
}

/// examples/materialize-python's API of keyed records, served in a container
/// on a network of its own. A VMM reaches it by its address, as a CIDR the
/// task declares.
struct Api {
    name: String,
    network: String,
    address: String,
}

impl Api {
    fn start(platform: &Platform) -> Self {
        let name = format!("vmm-api-{}", run::random_hex());
        let network = name.clone();
        run::record("network", &network);
        run::podman(&["network", "create", &network]);
        run::record("container", &name);
        run::podman(&[
            "run",
            "--detach",
            "--rm",
            &format!("--name={name}"),
            &format!("--network={network}"),
            &format!(
                "--mount=type=bind,source={},target=/api.py,readonly",
                platform
                    .examples
                    .join("materialize-python/api.py")
                    .display()
            ),
            "--entrypoint=python3",
            API_IMAGE,
            "/api.py",
        ]);
        let address = run::podman(&[
            "inspect",
            "--format",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            &name,
        ])
        .trim()
        .to_string();
        Api {
            name,
            network,
            address,
        }
    }

    /// Every table's records, ordered by table and key.
    fn dump(&self) -> String {
        run::podman(&["exec", &self.name, "python3", "/api.py", "dump"])
    }

    /// The keys and messages of `table`'s greetings.
    fn greetings(&self, table: &str) -> BTreeMap<String, String> {
        self.dump()
            .lines()
            .filter_map(|line| {
                let row: serde_json::Value = serde_json::from_str(line).expect("a dumped row");
                (row[0] == table).then(|| {
                    (
                        row[1][0].as_str().expect("a timestamp key").to_string(),
                        row[2]["message"].as_str().expect("a message").to_string(),
                    )
                })
            })
            .collect()
    }
}

impl Drop for Api {
    fn drop(&mut self) {
        _ = run::sudo_output(&["podman", "rm", "--force", "--time=0", &self.name], None);
        _ = run::sudo_output(&["podman", "network", "rm", &self.network], None);
    }
}

/// A capture project, discovered locally and through the control plane,
/// previewed, then published. A session whose VMM is killed is restarted, and
/// resumes from the cursor its predecessor checkpointed, as does the session
/// of a later publication: each captures only what follows the cursor.
#[test]
fn a_python_capture_through_the_platform() {
    let platform = load();
    let root = root(&platform);
    expect_connector_init(&platform);
    let project = platform.run_dir.join("capture");
    copy_tree(&run::fixture("platform/capture"), &project);
    let source = project.join("flow.yaml");
    let source = source.to_string_lossy();

    let discover = flowctl(
        &platform,
        "capture-raw-discover",
        &["raw", "discover", "--source", &source, "--emit-raw"],
        &[("RUST_LOG", "info")],
    );
    assert_eq!(started(&discover.stderr).len(), 1, "discovered in one VMM");
    assert!(discover.stderr.contains(EGRESS));
    assert!(
        discover.stdout.contains("greetings"),
        "{}",
        tail(&discover.stdout, 40)
    );

    // Discovery by the control plane, through the reactor, rewrites the
    // catalog it's given, so it gets a copy of its own. Its collection's
    // schema opts in to being rewritten.
    let discovered = platform.run_dir.join("capture-discovered");
    copy_tree(&run::fixture("platform/capture"), &discovered);
    render(
        &discovered.join("greetings.schema.yaml"),
        "$defs:\n",
        "$defs:\n  \"flow://connector-schema\": {}\n",
    );
    let before = launcher::calls(&root).len();
    flowctl(
        &platform,
        "capture-discover",
        &[
            "discover",
            "--source",
            &discovered.join("flow.yaml").to_string_lossy(),
            "--capture",
            CAPTURE,
        ],
        &[],
    );
    let calls = launcher::calls(&root)[before..].to_vec();
    assert!(
        calls.iter().any(|call| call.starts_with("create ")
            && call.contains(&format!("--label=task-name={CAPTURE}"))),
        "the reactor discovered in a VMM: {calls:#?}"
    );

    let preview = flowctl(
        &platform,
        "capture-preview",
        &[
            "preview",
            "--source",
            &source,
            "--sessions",
            "2",
            "--timeout",
            "600s",
        ],
        &[],
    );
    let expected = std::fs::read_to_string(project.join("expected.jsonl")).unwrap();
    assert_eq!(preview.stdout, expected);

    flowctl(
        &platform,
        "capture-publish",
        &["catalog", "publish", "--source", &source, "--auto-approve"],
        &[],
    );
    let howdies = |ids: std::ops::RangeInclusive<i64>| {
        run::eventually("the captured documents", DERIVED, || {
            std::thread::sleep(Duration::from_secs(2));
            let docs = read(&platform, "howdies", HOWDIES)?;
            ids.clone()
                .all(|id| docs.iter().any(|doc| doc["id"] == id))
                .then_some(docs)
        })
    };
    howdies(1..=3);

    let killed = running(CAPTURE);
    assert_eq!(killed.len(), 1, "one session runs: {killed:?}");
    for vmm in &killed {
        run::podman(&["rm", "--force", vmm]);
    }
    run::eventually("the session's restart", DERIVED, || {
        std::thread::sleep(Duration::from_secs(1));
        let now = running(CAPTURE);
        (!now.is_empty() && now.is_disjoint(&killed)).then_some(())
    });
    // The restarted session polls its binding as it opens. A session which
    // lost the cursor would capture its greetings again, as duplicates.
    std::thread::sleep(Duration::from_secs(30));

    // A changed configuration captures only what follows the cursor.
    render(
        &project.join("flow.yaml"),
        "greeting: Howdy",
        "greeting: Hiya",
    );
    render(&project.join("flow.yaml"), "count: 3", "count: 5");
    flowctl(
        &platform,
        "capture-republish",
        &["catalog", "publish", "--source", &source, "--auto-approve"],
        &[],
    );
    let docs = howdies(1..=5);
    let mut captured: Vec<(i64, &str)> = docs
        .iter()
        .map(|doc| {
            (
                doc["id"].as_i64().unwrap(),
                doc["message"].as_str().unwrap(),
            )
        })
        .collect();
    captured.sort();
    assert_eq!(
        captured,
        [
            (1, "Howdy #1!"),
            (2, "Howdy #2!"),
            (3, "Howdy #3!"),
            (4, "Hiya #4!"),
            (5, "Hiya #5!"),
        ],
        "each greeting was captured once, after the cursor"
    );

    task_logs(
        &platform,
        "capture-logs",
        CAPTURE,
        &[EGRESS, "started connector container"],
    );
    delete_and_release(&platform, "capture-delete");
}

/// A materialization project into an API. Its preview loads, in later
/// sessions, what earlier ones stored, with standard and delta updates. Its
/// publication materializes live greetings, and a session whose VMM is
/// killed is restarted and replays into the idempotent API, so that every
/// greeting arrives in both tables.
#[test]
fn a_python_materialization_through_the_platform() {
    let platform = load();
    let root = root(&platform);
    expect_connector_init(&platform);

    let api = Api::start(&platform);
    let project = platform.run_dir.join("materialize-preview");
    copy_tree(&run::fixture("platform/materialize"), &project);
    render(&project.join("flow.yaml"), "API_ADDRESS", &api.address);
    for file in ["greetings.flow.yaml", "hello-world.flow.yaml"] {
        std::fs::copy(
            run::fixture(&format!("platform/{file}")),
            platform.run_dir.join(file),
        )
        .unwrap();
    }

    let before = launcher::calls(&root).len();
    let preview = flowctl(
        &platform,
        "materialize-preview",
        &[
            "preview",
            "--source",
            &project.join("flow.yaml").to_string_lossy(),
            "--task",
            MATERIALIZATION,
            "--fixture",
            &project.join("fixture.jsonl").to_string_lossy(),
            "--sessions",
            "2,1",
            "--timeout",
            "900s",
            "--log-json",
        ],
        &[],
    );
    let egress = format!(
        "VMM egress permits only these host names: pypi.org, files.pythonhosted.org \
         (connector defaults); and any address within {}/32 (task egress.cidrs)",
        api.address
    );
    assert!(
        preview.stderr.contains(&egress),
        "{}",
        tail(&preview.stderr, 40)
    );
    let previewed = launched(&launcher::calls(&root)[before..]);
    assert!(
        previewed.len() >= 3,
        "Validate, Apply and sessions: {previewed:?}"
    );
    let expected = std::fs::read_to_string(project.join("expected.jsonl")).unwrap();
    assert_eq!(api.dump(), expected);
    std::mem::drop(api);

    let api = Api::start(&platform);
    let project = platform.run_dir.join("materialize-publish");
    copy_tree(&run::fixture("platform/materialize"), &project);
    render(&project.join("flow.yaml"), "API_ADDRESS", &api.address);
    flowctl(
        &platform,
        "materialize-publish",
        &[
            "catalog",
            "publish",
            "--source",
            &project.join("flow.yaml").to_string_lossy(),
            "--auto-approve",
        ],
        &[],
    );
    let both = |at_least: usize| {
        run::eventually("greetings in both tables", DERIVED, || {
            std::thread::sleep(Duration::from_secs(2));
            let (standard, delta) = (api.greetings("greetings"), api.greetings("greetings_log"));
            (standard.len() >= at_least && delta.len() >= at_least).then_some(standard.len())
        })
    };
    let first = both(4);

    let killed = running(MATERIALIZATION);
    assert_eq!(killed.len(), 1, "one session runs: {killed:?}");
    for vmm in &killed {
        run::podman(&["rm", "--force", vmm]);
    }
    run::eventually("the session's restart", DERIVED, || {
        std::thread::sleep(Duration::from_secs(1));
        let now = running(MATERIALIZATION);
        (!now.is_empty() && now.is_disjoint(&killed)).then_some(())
    });
    both(first + 4);

    // Every greeting the collection held is eventually in both tables, as
    // the collection has it.
    let docs = read(&platform, "greetings", GREETINGS).expect("the collection exists");
    let held: BTreeMap<String, String> = docs
        .iter()
        .map(|doc| {
            (
                doc["ts"].as_str().unwrap().to_string(),
                doc["message"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    run::eventually("every greeting in both tables", DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        [api.greetings("greetings"), api.greetings("greetings_log")]
            .iter()
            .all(|table| {
                held.iter()
                    .all(|(ts, message)| table.get(ts) == Some(message))
            })
            .then_some(())
    });

    task_logs(
        &platform,
        "materialize-logs",
        MATERIALIZATION,
        &[&egress, "started connector container"],
    );
    delete_and_release(&platform, "materialize-delete");
    std::mem::drop(api);
}
