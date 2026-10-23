//! A Python derivation previewed and published through flowctl and the reactor, and its files
//! generated from a nested catalog. Run only under
//! `mise run ci:connector-vmm-kvm --platform`, which configures the local stack
//! and writes `platform.json`, one test at a time against that one stack.

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
    let mut text = calls
        .join("\n")
        .replace(&platform.vmm_image, "<vmm image>")
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

    let before = launcher::calls(&root).len();
    let preview = flowctl(
        &platform,
        "preview",
        &[
            "raw",
            "preview-next",
            "--source",
            &run::fixture("platform/numbers.flow.yaml").to_string_lossy(),
            "--name",
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

    // Validate awaits teardown. Preview exits without awaiting the session's
    // connector, so its leftovers may need recovery by the next launch.
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, validated),
        Vec::<String>::new()
    );
    let session_left = launcher::left(&root.state_dir, &root.tmp_dir, session);
    let session_record = launcher::record(&root.state_dir, session);
    let session_token = launcher::path_exists(&session_record).then(|| {
        assert!(!launcher::locked(&session_record), "preview has exited");
        launcher::token(&session_record)
    });
    // Through the session's start: whatever of its teardown follows depends on
    // how far it got before flowctl exited.
    let session_start = calls
        .iter()
        .rposition(|call| call.starts_with("start --attach "))
        .expect("the session started");
    insta::assert_snapshot!(
        "preview_calls",
        normalize_calls(&platform, &calls[..=session_start])
    );
    evidence.push(format!(
        "preview: Validate {validated} released; session {session} left {session_left:?} at exit"
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
        // flowctl's own Validate logs its launch, and recovery, at info.
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

    // flowctl's Validate was the first launch since, and released what the
    // preview's session left before its own pull.
    let calls = launcher::calls(&root)[before..].to_vec();
    if let Some(token) = &session_token {
        let pull = calls
            .iter()
            .position(|call| call.starts_with("pull "))
            .expect("the publication's launches pull");
        let recovered = format!("--filter label=dev.estuary.vmm-owner={token}");
        assert!(
            calls[..pull].iter().any(|call| call.contains(&recovered)),
            "the next launch recovers the preview's session: {calls:#?}"
        );
        assert!(
            publish.stderr.lines().any(|line| line
                .contains("released the resources of a dead VMM launch")
                && line.contains(session.as_str())),
            "flowctl's Validate reports recovering {session}"
        );
        evidence.push(format!(
            "preview session {session} recovered by the next launch"
        ));
    }
    assert_eq!(
        launcher::left(&root.state_dir, &root.tmp_dir, session),
        Vec::<String>::new()
    );

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

    // Shard deletion drops the launch's runtime before teardown. Wait for its
    // owner lock to go before testing recovery by a new launch.
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
        &[],
    );
    let deadline = Instant::now() + RELEASED;
    for name in &opened {
        let record = launcher::record(&root.state_dir, name);
        run::eventually(
            &format!("{name}'s owner to be gone"),
            deadline.saturating_duration_since(Instant::now()),
            || (!launcher::path_exists(&record) || !launcher::locked(&record)).then_some(()),
        );
    }
    // Every other launch's owner awaited its teardown, or was recovered above.
    let launches = launched(&launcher::calls(&root));
    for name in launches.iter().filter(|name| !opened.contains(*name)) {
        assert_eq!(
            launcher::left(&root.state_dir, &root.tmp_dir, name),
            Vec::<String>::new()
        );
    }
    let dead: Vec<&String> = opened
        .iter()
        .filter(|name| !launcher::left(&root.state_dir, &root.tmp_dir, name).is_empty())
        .collect();

    // One more launch, of the derivation's Spec, releases what they left
    // before its own pull, and is itself released before flowctl exits.
    let before = launcher::calls(&root).len();
    let spec = flowctl(
        &platform,
        "spec",
        &[
            "raw",
            "spec",
            "--source",
            &run::fixture("platform/numbers.flow.yaml").to_string_lossy(),
            "--name",
            DERIVATION,
        ],
        &[("RUST_LOG", "info")],
    );
    let respecified = launched(&launcher::calls(&root)[before..]);
    let [respecified] = &respecified[..] else {
        panic!("one launch for the Spec: {respecified:?}");
    };
    for name in &dead {
        assert!(
            spec.stderr.lines().any(|line| line
                .contains("released the resources of a dead VMM launch")
                && line.contains(name.as_str())),
            "the Spec's launch reports recovering {name}"
        );
    }
    evidence.push(format!(
        "after deletion the reactor's sessions {dead:?} were left to recovery; \
         the Spec's launch {respecified} released them"
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

    let launches = launched(&launcher::calls(&root));
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
    let all = launcher::calls(&root);

    let observed = std::fs::read_to_string(format!("{}/checked-mounts", platform.dir))
        .expect("reading the wrapper's mount checks");
    assert_eq!(
        observed
            .lines()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        launches.iter().cloned().collect(),
        "every launch received the static connector-init"
    );
    for call in all.iter().filter(|call| call.starts_with("create ")) {
        assert!(
            call.contains(&format!(" {} run ", platform.vmm_image)),
            "{call}"
        );
        assert!(call.contains("--label=dev.estuary.vmm-owner="), "{call}");
        assert!(
            call.contains(&format!("--label=task-name={DERIVATION}"))
                || call.contains("--label=task-name=<spec>"),
            "{call}"
        );
    }
    for flag in ["--resolver-upstream", "--as-root-exec"] {
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
