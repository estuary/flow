//! Python derivations, crash recovery, mount updates, and refused launches
//! through flowctl and the reactor, beside ordinary tasks. Run only under
//! `mise run ci:connector-vmm-kvm --platform`, which configures the local stack
//! and writes `platform.json`, one test at a time against that one stack.

#[path = "common/rpc.rs"]
mod rpc;

use connector_vmm_tests::{launch, launcher, run};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

/// Every connector mount which appears beneath the shared TMPDIR, and whether
/// its `flow-connector-init` was ever seen to be the static build's bytes. A
/// mount is watched from creation until it's gone, so a file read while the
/// launcher was still writing it is read again.
struct Mounts {
    seen: Arc<Mutex<BTreeMap<String, Result<(), String>>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

fn watch_mounts(platform: &Platform) -> Mounts {
    // SAFETY: geteuid takes nothing and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let dir = PathBuf::from(format!("{}/tmp/connector-mounts-{euid}", platform.dir));
    let expected = std::fs::read(&platform.connector_init)
        .unwrap_or_else(|e| panic!("reading {}: {e}", platform.connector_init));

    let seen = Arc::new(Mutex::new(BTreeMap::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let thread = {
        let (seen, stop) = (seen.clone(), stop.clone());
        std::thread::spawn(move || {
            let gone = |e: &std::io::Error| e.kind() == std::io::ErrorKind::NotFound;
            while !stop.load(Ordering::Relaxed) {
                let entries = match std::fs::read_dir(&dir) {
                    Ok(entries) => entries,
                    Err(e) if gone(&e) => {
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                    Err(e) => panic!("reading {}: {e}", dir.display()),
                };
                for entry in entries {
                    let entry = entry.unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()));
                    let file = entry.file_name().to_string_lossy().to_string();
                    let Some(name) = file.strip_prefix("mount-") else {
                        continue;
                    };
                    let mut seen = seen.lock().unwrap();
                    if matches!(seen.get(name), Some(Ok(()))) {
                        continue;
                    }
                    let path = entry.path().join("flow-connector-init");
                    let outcome = match std::fs::read(&path) {
                        Ok(bytes) if bytes == expected => Ok(()),
                        Ok(bytes) => Err(format!("{} bytes unlike the static build", bytes.len())),
                        Err(e) if gone(&e) => Err("not yet written".to_string()),
                        Err(e) => panic!("reading {}: {e}", path.display()),
                    };
                    seen.insert(name.to_string(), outcome);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        })
    };
    Mounts {
        seen,
        stop,
        thread: Some(thread),
    }
}

impl Mounts {
    fn finish(mut self) -> BTreeMap<String, Result<(), String>> {
        self.stop.store(true, Ordering::Relaxed);
        if let Err(panic) = self.thread.take().unwrap().join() {
            std::panic::resume_unwind(panic);
        }
        self.seen.lock().unwrap().clone()
    }
}

/// `calls`, with what differs between runs replaced: each launch's id, token
/// and container ID by its ordinal, and this run's directory and VMM image.
fn normalize_calls(platform: &Platform, calls: &[String]) -> String {
    let mut text = calls
        .join("\n")
        .replace(&platform.vmm_image, "<vmm image>")
        .replace(&platform.dir, "<platform>");
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

fn assert_vmm_logs(what: &str, logs: &str) {
    for expected in [EGRESS, "started connector container", REFUSED] {
        assert!(logs.contains(expected), "{what} lacks {expected:?}");
    }
}

#[test]
fn a_python_derivation_through_the_platform() {
    let platform = load();
    let root = root(&platform);
    let mounts = watch_mounts(&platform);
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

    let observed = mounts.finish();
    assert_eq!(
        observed.keys().cloned().collect::<BTreeSet<_>>(),
        launches.iter().cloned().collect(),
        "a connector mount was seen for each launch"
    );
    for (name, outcome) in &observed {
        assert_eq!(outcome, &Ok(()), "{name}'s flow-connector-init");
    }
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

/// A launch, its image's pull included.
const STARTED: Duration = Duration::from_secs(180);
const RESTARTED: Duration = Duration::from_secs(240);
/// A preview session's first document waits on its VMM's boot and install.
const FIRST_DOCUMENT: Duration = Duration::from_secs(300);
const DOCUMENT: Duration = Duration::from_secs(60);
/// Allow for delayed guest visibility of host file replacements.
const VISIBLE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(250);
/// The image user of derive-python, `nobody`.
const NOBODY: i64 = 65534;

const RECOVERED: &str = "released the resources of a dead VMM launch";
const UNVERIFIED: &str = "the host's VMM network boundary did not verify, \
    so this data plane cannot run VMM connectors";

/// What a test establishes, kept beside flowctl's output as it's learned, so
/// that a failed test leaves what it had shown.
struct Evidence {
    path: PathBuf,
    lines: Vec<String>,
}

impl Evidence {
    fn new(platform: &Platform, test: &str) -> Self {
        Self {
            path: platform.run_dir.join(format!("{test}.evidence")),
            lines: Vec::new(),
        }
    }

    fn push(&mut self, line: impl Into<String>) {
        let line = line.into();
        eprintln!("{line}");
        self.lines.push(line);
        std::fs::write(&self.path, self.lines.join("\n") + "\n")
            .unwrap_or_else(|e| panic!("writing {}: {e}", self.path.display()));
    }
}

/// `dir`'s podman and TMPDIR, beneath `state_dir`: a launcher of the test's
/// own beneath the stack's state directory, or another's.
fn at(dir: &launcher::Root, state_dir: &str) -> launcher::Root {
    launcher::Root {
        dir: dir.dir.clone(),
        state_dir: state_dir.to_string(),
        tmp_dir: dir.tmp_dir.clone(),
        podman: dir.podman.clone(),
    }
}

/// `fixtures/platform/`, copied beneath the run's directory with its names
/// moved from `acmeCo/vmm-python/` to `acmeCo/<prefix>/`, so that a test's
/// tasks and collections, and so their journals and tables, are its own.
struct Staged {
    dir: PathBuf,
    prefix: String,
}

impl Staged {
    fn name(&self, leaf: &str) -> String {
        format!("acmeCo/{}/{leaf}", self.prefix)
    }

    fn source(&self, file: &str) -> String {
        self.dir.join(file).to_string_lossy().into_owned()
    }

    /// The prefix as a Python package, or a PostgreSQL schema.
    fn underscored(&self) -> String {
        self.prefix.replace('-', "_")
    }
}

fn stage(platform: &Platform, prefix: &str) -> Staged {
    let dir = platform.run_dir.join(prefix);
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("removing {}: {e}", dir.display()),
    }
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("creating {}: {e}", dir.display()));

    let staged = Staged {
        dir,
        prefix: prefix.to_string(),
    };
    let fixtures = run::fixture("platform");
    for entry in std::fs::read_dir(&fixtures)
        .unwrap_or_else(|e| panic!("reading {}: {e}", fixtures.display()))
    {
        let entry = entry.unwrap_or_else(|e| panic!("reading {}: {e}", fixtures.display()));
        let content = std::fs::read_to_string(entry.path())
            .unwrap_or_else(|e| panic!("reading {}: {e}", entry.path().display()))
            .replace("vmm-python", prefix)
            .replace("vmm_python", &staged.underscored());
        let path = staged.dir.join(entry.file_name());
        std::fs::write(&path, content)
            .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
    }
    variant(&staged, "ordinary-numbers", false);
    staged
}

/// Write `<leaf>.flow.yaml` and its module: the staged numbers derivation,
/// renamed, with its VMM selection and egress or without them.
fn variant(staged: &Staged, leaf: &str, vmm: bool) {
    let numbers = std::fs::read_to_string(staged.dir.join("numbers.flow.yaml"))
        .expect("reading the staged numbers.flow.yaml");
    let numbers: serde_json::Value =
        serde_yaml::from_str(&numbers).expect("parsing the staged numbers.flow.yaml");
    let mut collection = numbers["collections"][staged.name("numbers")].clone();
    let package = leaf.replace('-', "_");

    let derive = collection["derive"]
        .as_object_mut()
        .expect("numbers is a derivation");
    derive["using"]["python"]["module"] = format!("{package}.py").into();
    if !vmm {
        derive.remove("vmm");
        derive.remove("egress");
    }
    let catalog = serde_json::json!({
        "import": ["greetings.flow.yaml"],
        "collections": { staged.name(leaf): collection },
    });
    let path = staged.dir.join(format!("{leaf}.flow.yaml"));
    std::fs::write(&path, catalog.to_string())
        .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));

    let module = std::fs::read_to_string(staged.dir.join("numbers.py"))
        .expect("reading the staged numbers.py");
    let import = format!("acmeCo.{}.numbers import", staged.underscored());
    assert!(module.contains(&import), "numbers.py imports {import}");
    let module = module.replace(
        &import,
        &format!("acmeCo.{}.{package} import", staged.underscored()),
    );
    let path = staged.dir.join(format!("{package}.py"));
    std::fs::write(&path, module).unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

/// A preview of a staged derivation fed greetings one transaction at a time
/// through its stdin, whose documents are read as flowctl prints them: one
/// session, which lives until its fixture ends or flowctl is killed.
struct Preview {
    label: String,
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    docs: std::sync::mpsc::Receiver<serde_json::Value>,
    stderr: PathBuf,
    greetings: String,
}

impl Preview {
    fn spawn(
        platform: &Platform,
        at: &launcher::Root,
        staged: &Staged,
        leaf: &str,
        label: &str,
    ) -> Self {
        let stderr = platform.run_dir.join(format!("{label}.stderr"));
        let args = [
            "raw",
            "preview-next",
            "--source",
            &staged.source(&format!("{leaf}.flow.yaml")),
            "--name",
            &staged.name(leaf),
            "--fixture",
            "-",
            "--log-json",
        ];
        let mut child = flowctl_command(platform, at, &args, &[])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(create(&stderr))
            .spawn()
            .unwrap_or_else(|e| panic!("spawning flowctl {}: {e}", args.join(" ")));

        let stdout = child.stdout.take().expect("stdout is piped");
        let mut kept = create(&platform.run_dir.join(format!("{label}.stdout")));
        let (docs_tx, docs) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                _ = writeln!(kept, "{line}");
                for doc in documents(&line) {
                    let doc = match doc {
                        serde_json::Value::Array(mut pair) => pair.swap_remove(1),
                        doc => doc,
                    };
                    if docs_tx.send(doc).is_err() {
                        return;
                    }
                }
            }
        });

        Self {
            label: label.to_string(),
            stdin: child.stdin.take(),
            child,
            docs,
            stderr,
            greetings: staged.name("greetings"),
        }
    }

    /// Feed the greeting of `n` as a transaction of its own, and await the
    /// session's document of it.
    fn send(&mut self, n: i64, timeout: Duration) -> serde_json::Value {
        let greeting = serde_json::json!([
            self.greetings,
            {"ts": format!("2026-01-01T00:00:00.{n:09}Z"), "message": format!("Hello {n}!")},
        ]);
        let stdin = self.stdin.as_mut().expect("the preview's fixture is open");
        writeln!(stdin, "{greeting}\n{}", serde_json::json!({"commit": true}))
            .and_then(|()| stdin.flush())
            .unwrap_or_else(|e| panic!("feeding {}: {e}", self.label));

        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let doc = self.docs.recv_timeout(remaining).unwrap_or_else(|e| {
                panic!(
                    "{}'s document of {n}: {e}; its stderr ends:\n{}",
                    self.label,
                    tail(
                        &std::fs::read_to_string(&self.stderr).unwrap_or_default(),
                        40
                    )
                )
            });
            if doc["n"] == n {
                return doc;
            }
        }
    }

    /// The launch of its session: the last of its launches to start.
    fn vmm(&self) -> String {
        let stderr = std::fs::read_to_string(&self.stderr)
            .unwrap_or_else(|e| panic!("reading {}: {e}", self.stderr.display()));
        stderr
            .lines()
            .filter(|line| line.contains("started connector container"))
            .flat_map(vmm_names)
            .last()
            .unwrap_or_else(|| panic!("{} started no VMM", self.label))
    }

    /// Killed outright, as a crash would: its children are not.
    fn kill(mut self) {
        self.child.kill().expect("killing flowctl");
        self.child.wait().expect("waiting for a killed flowctl");
    }

    fn end(mut self) {
        std::mem::drop(self.stdin.take());
        let deadline = Instant::now() + COMMAND;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("polling flowctl") {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "{} ran on past its fixture",
                self.label
            );
            std::thread::sleep(Duration::from_millis(100));
        };
        assert!(status.success(), "{} exited {status}", self.label);
    }
}

impl Drop for Preview {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            _ = self.child.kill();
            _ = self.child.wait();
        }
    }
}

/// One VMM launch through `at`: the staged numbers derivation's Spec, which
/// first releases whatever dead launches left beneath `at`'s state
/// directory, and whose own resources are released before flowctl exits.
fn spec_launch(platform: &Platform, at: &launcher::Root, staged: &Staged, label: &str) -> Output {
    flowctl_in(
        platform,
        at,
        label,
        &[
            "raw",
            "spec",
            "--source",
            &staged.source("numbers.flow.yaml"),
            "--name",
            &staged.name("numbers"),
        ],
        // The release of a dead launch's resources is logged at info.
        &[("RUST_LOG", "info")],
    )
}

fn reports_recovering(output: &Output, name: &str) -> bool {
    output
        .stderr
        .lines()
        .any(|line| line.contains(RECOVERED) && line.contains(name))
}

fn publish(
    platform: &Platform,
    at: &launcher::Root,
    staged: &Staged,
    file: &str,
    label: &str,
) -> Output {
    flowctl_status_in(
        platform,
        at,
        label,
        &[
            "catalog",
            "publish",
            "--source",
            &staged.source(file),
            "--auto-approve",
        ],
        &[],
    )
}

/// Deletes the staged prefix's tasks and collections, waits for the owners
/// of its shards' launches to go, then has a launch beneath the stack's
/// state directory release what they left.
fn delete(platform: &Platform, staged: &Staged, label: &str) -> Output {
    flowctl(
        platform,
        &format!("{label}-delete"),
        &[
            "catalog",
            "delete",
            "--prefix",
            &format!("acmeCo/{}/", staged.prefix),
            "--dangerous-auto-approve",
        ],
        &[],
    );
    let state_dir = root(platform).state_dir;
    run::eventually("the owners of the shards' launches to go", RELEASED, || {
        owned(&state_dir).is_empty().then_some(())
    });
    spec_launch(
        platform,
        &root(platform),
        staged,
        &format!("{label}-recovery"),
    )
}

fn owned(state_dir: &str) -> Vec<String> {
    std::fs::read_dir(state_dir)
        .unwrap_or_else(|e| panic!("reading {state_dir}: {e}"))
        .map(|entry| {
            entry
                .unwrap_or_else(|e| panic!("reading {state_dir}: {e}"))
                .path()
        })
        .filter(|path| path.extension().is_some_and(|ext| ext == "owner"))
        .map(|path| path.to_string_lossy().into_owned())
        .filter(|path| launcher::path_exists(path) && launcher::locked(path))
        .collect()
}

/// That nothing of the launches `names` remains, wherever `roots` put their
/// state and connector mounts, and those state directories are empty.
fn assert_released(names: &BTreeSet<String>, roots: &[&launcher::Root]) {
    for name in names {
        for at in roots {
            assert_eq!(
                launcher::left(&at.state_dir, &at.tmp_dir, name),
                Vec::<String>::new()
            );
        }
    }
    for at in roots {
        let state: Vec<_> = std::fs::read_dir(&at.state_dir)
            .unwrap_or_else(|e| panic!("reading {}: {e}", at.state_dir))
            .map(|entry| entry.expect("reading a state directory").path())
            .collect();
        assert!(state.is_empty(), "{} holds {state:?}", at.state_dir);
    }
}

/// The launches a podman's `calls` made, by the name of their network or,
/// for one refused before it had one, of their container.
fn named(calls: &[String]) -> BTreeSet<String> {
    calls
        .iter()
        .filter(|call| call.starts_with("network create ") || call.starts_with("create "))
        .flat_map(|call| vmm_names(call))
        .collect()
}

/// The VMMs `task`'s shards started, in order, as its logs report them.
fn shard_vmms(platform: &Platform, task: &str, label: &str) -> Vec<String> {
    let logs = flowctl_status(platform, label, &["logs", "--task", task], &[]);
    if !logs.status.success() {
        return Vec::new();
    }
    let mut names = Vec::new();
    for name in logs
        .stdout
        .lines()
        .filter(|line| line.contains("started connector container"))
        .flat_map(vmm_names)
    {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

fn task_logs_holding(platform: &Platform, task: &str, label: &str, lines: &[&str]) -> String {
    run::eventually(&format!("{task}'s logs to hold {lines:?}"), DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        let logs = flowctl_status(platform, label, &["logs", "--task", task], &[]);
        (logs.status.success() && lines.iter().all(|line| logs.stdout.contains(line)))
            .then_some(logs.stdout)
    })
}

/// The documents of a derived collection, by `n`.
fn derived(platform: &Platform, collection: &str, label: &str) -> BTreeMap<i64, serde_json::Value> {
    let read = flowctl_status(
        platform,
        label,
        &["collections", "read", "--collection", collection],
        &[],
    );
    if !read.status.success() {
        return BTreeMap::new();
    }
    documents(&read.stdout)
        .into_iter()
        .map(|doc| (doc["n"].as_i64().expect("n is an integer"), doc))
        .collect()
}

/// The derived documents of `collection` once it holds every `n` of `want`.
fn derived_holding(
    platform: &Platform,
    collection: &str,
    label: &str,
    want: impl Fn(&BTreeMap<i64, serde_json::Value>) -> bool,
) -> BTreeMap<i64, serde_json::Value> {
    run::eventually(&format!("{collection}'s documents"), DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        let docs = derived(platform, collection, label);
        want(&docs).then_some(docs)
    })
}

/// humanize's `ordinal`, in English.
fn ordinal(n: i64) -> String {
    let suffix = match (n % 100, n % 10) {
        (11..=13, _) => "th",
        (_, 1) => "st",
        (_, 2) => "nd",
        (_, 3) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}

/// humanize's `intcomma`.
fn intcomma(n: i64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// What numbers.py derives of greeting `n`: in a VMM held to its egress, or
/// ordinarily, where every name resolves.
fn assert_numbered(doc: &serde_json::Value, n: i64, vmm: bool) {
    let (resolved, unresolved): (&[&str], &[&str]) = if vmm {
        (&["pypi.org", "example.org"], &["example.com"])
    } else {
        (&["pypi.org", "example.org", "example.com"], &[])
    };
    assert_eq!(
        doc,
        &serde_json::json!({
            "n": n,
            "ordinal": ordinal(n),
            "comma": intcomma(n * 1000),
            "humanize": "4.12.3",
            "resolved": resolved,
            "unresolved": unresolved,
        })
    );
}

/// A query of this stack's Supabase database, which must succeed.
fn sql(query: &str) -> String {
    let url = std::env::var("FLOW_PG_URL").expect("FLOW_PG_URL, which the task's mise sets");
    let output = std::process::Command::new("psql")
        .arg(url)
        .args(["-tAqc", query])
        .output()
        .expect("running psql");
    assert!(
        output.status.success(),
        "psql {query}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("psql prints UTF-8")
        .trim()
        .to_string()
}

/// Rows of `schema.table` materialized from documents published after
/// `since`, or None while the materialization hasn't made the table.
fn rows_since(schema: &str, table: &str, since: &str) -> Option<i64> {
    if sql(&format!(
        "SELECT to_regclass('{schema}.{table}') IS NOT NULL"
    )) != "t"
    {
        return None;
    }
    let count = sql(&format!(
        "SELECT count(*) FROM {schema}.{table} WHERE flow_published_at > '{since}'"
    ));
    Some(
        count
            .parse()
            .unwrap_or_else(|e| panic!("counting {schema}.{table}: {count:?}: {e}")),
    )
}

fn assert_materializing(schema: &str, tables: &[&str], since: &str) -> Vec<(String, i64)> {
    tables
        .iter()
        .map(|table| {
            let rows = run::eventually(
                &format!("{schema}.{table} to gain rows published after {since}"),
                DERIVED,
                || {
                    std::thread::sleep(Duration::from_secs(1));
                    rows_since(schema, table, since).filter(|rows| *rows > 0)
                },
            );
            (table.to_string(), rows)
        })
        .collect()
}

/// The ordinary connector containers of `tasks` Docker runs now, by task.
fn ordinary_containers(tasks: &[String]) -> BTreeMap<String, Vec<String>> {
    tasks
        .iter()
        .map(|task| {
            let output = std::process::Command::new("docker")
                .args([
                    "ps",
                    "--filter",
                    &format!("label=task-name={task}"),
                    "--format",
                    "{{.Names}} {{.Image}}",
                ])
                .output()
                .expect("running docker ps");
            assert!(
                output.status.success(),
                "docker ps: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let containers = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(ToString::to_string)
                .collect();
            (task.clone(), containers)
        })
        .collect()
}

/// Docker's container creations, by task name, from a subscription the test
/// holds open: a query of the past reads a buffer Docker bounds, which can't
/// tell an eviction from no creation at all.
struct Creations {
    child: std::process::Child,
    seen: Arc<Mutex<Vec<String>>>,
}

fn watch_creations() -> Creations {
    let mut child = std::process::Command::new("docker")
        .args([
            "events",
            "--filter",
            "type=container",
            "--filter",
            "event=create",
            "--format",
            "{{index .Actor.Attributes \"task-name\"}}",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("running docker events");
    let stdout = child.stdout.take().expect("stdout is piped");
    let seen = Arc::new(Mutex::new(Vec::new()));
    {
        let seen = seen.clone();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                seen.lock().unwrap().push(line);
            }
        });
    }
    Creations { child, seen }
}

impl Creations {
    /// The task of each container created so far, while the subscription
    /// still runs.
    fn seen(&mut self) -> Vec<String> {
        assert!(
            matches!(self.child.try_wait(), Ok(None)),
            "docker events stopped watching"
        );
        self.seen.lock().unwrap().clone()
    }
}

impl Drop for Creations {
    fn drop(&mut self) {
        _ = self.child.kill();
        _ = self.child.wait();
    }
}

#[derive(Debug)]
struct Unit {
    active: String,
    sub: String,
    main_pid: u32,
    /// Every process in its control group, by PID, real UID and command.
    processes: Vec<(u32, u32, String)>,
}

fn unit(platform: &Platform) -> Unit {
    let output = std::process::Command::new("systemctl")
        .args([
            "--user",
            "show",
            "-p",
            "ActiveState",
            "-p",
            "SubState",
            "-p",
            "MainPID",
            "-p",
            "ControlGroup",
            &platform.reactor_unit,
        ])
        .output()
        .expect("running systemctl");
    assert!(
        output.status.success(),
        "systemctl show {}: {}",
        platform.reactor_unit,
        String::from_utf8_lossy(&output.stderr)
    );
    let shown: BTreeMap<String, String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    let field = |key: &str| {
        shown
            .get(key)
            .unwrap_or_else(|| panic!("systemctl shows no {key}: {shown:?}"))
            .clone()
    };

    let cgroup = field("ControlGroup");
    let processes = if cgroup.is_empty() {
        Vec::new()
    } else {
        let procs = format!("/sys/fs/cgroup{cgroup}/cgroup.procs");
        match std::fs::read_to_string(&procs) {
            Ok(pids) => pids
                .lines()
                .map(|pid| {
                    pid.parse()
                        .unwrap_or_else(|e| panic!("{procs}: {pid:?}: {e}"))
                })
                .filter_map(process)
                .collect(),
            // The group goes once the unit has stopped.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => panic!("reading {procs}: {e}"),
        }
    };
    Unit {
        active: field("ActiveState"),
        sub: field("SubState"),
        main_pid: field("MainPID")
            .parse()
            .unwrap_or_else(|e| panic!("MainPID: {e}")),
        processes,
    }
}

/// A process's real UID and command, or None once it has gone.
fn process(pid: u32) -> Option<(u32, u32, String)> {
    let read = |file: &str| match std::fs::read(format!("/proc/{pid}/{file}")) {
        Ok(content) => Some(content),
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                || e.raw_os_error() == Some(libc::ESRCH) =>
        {
            None
        }
        Err(e) => panic!("reading /proc/{pid}/{file}: {e}"),
    };
    let status = String::from_utf8(read("status")?).expect("a status is UTF-8");
    let uid = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|uids| uids.split_whitespace().next())
        .and_then(|uid| uid.parse().ok())
        .unwrap_or_else(|| panic!("/proc/{pid}/status has no Uid: {status:?}"));
    let command = String::from_utf8_lossy(&read("cmdline")?)
        .replace('\0', " ")
        .trim()
        .to_string();
    Some((pid, uid, command))
}

/// The reactor's main process, by PID and start time, once it runs: a test
/// before this one may have left it restarting.
fn reactor_main(platform: &Platform) -> (u32, String) {
    run::eventually("the reactor to run", RESTARTED, || {
        let unit = unit(platform);
        if unit.active != "active" || unit.main_pid == 0 {
            return None;
        }
        Some((unit.main_pid, launcher::start_time(unit.main_pid)?))
    })
}

/// Kill every process of the reactor's unit which its user may signal, as a
/// crash of the reactor would end it and its clients, and see its main
/// process gone. Root's processes, podman's among them, are left running.
fn kill_reactor(platform: &Platform) -> (u32, String) {
    let main = reactor_main(platform);
    let output = std::process::Command::new("systemctl")
        .args(["--user", "kill", "--signal=SIGKILL", &platform.reactor_unit])
        .output()
        .expect("running systemctl kill");
    // Denied for root's processes, which go on; what's killed is checked.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() || stderr.contains("Access denied"),
        "systemctl kill: {stderr}"
    );
    run::eventually("the reactor's main process to die", RELEASED, || {
        (launcher::start_time(main.0).as_ref() != Some(&main.1)).then_some(())
    });
    main
}

/// The reactor's main process, once systemd has restarted it after `dead`.
fn reactor_restarted(platform: &Platform, dead: &(u32, String)) -> (u32, String) {
    run::eventually("systemd to restart the reactor", RESTARTED, || {
        let unit = unit(platform);
        if unit.active != "active" || unit.main_pid == 0 {
            return None;
        }
        let started = launcher::start_time(unit.main_pid)?;
        ((unit.main_pid, started.clone()) != *dead).then_some((unit.main_pid, started))
    })
}

/// Every process whose command holds `pattern`, by PID, real UID, command
/// and control group.
fn running(pattern: &str) -> Vec<(u32, u32, String, String)> {
    std::fs::read_dir("/proc")
        .expect("reading /proc")
        .map(|entry| entry.expect("reading /proc").file_name())
        .filter_map(|name| name.to_str().and_then(|pid| pid.parse().ok()))
        .filter_map(process)
        .filter(|(pid, _, command)| *pid != std::process::id() && command.contains(pattern))
        .filter_map(|(pid, uid, command)| {
            let cgroup = match std::fs::read_to_string(format!("/proc/{pid}/cgroup")) {
                Ok(cgroup) => cgroup.trim().to_string(),
                Err(e)
                    if e.kind() == std::io::ErrorKind::NotFound
                        || e.raw_os_error() == Some(libc::ESRCH) =>
                {
                    return None;
                }
                Err(e) => panic!("reading /proc/{pid}/cgroup: {e}"),
            };
            Some((pid, uid, command, cgroup))
        })
        .collect()
}

/// The dead reactor's processes which remain: none of them may be its user's.
fn assert_rootful(unit: &Unit) {
    for (pid, uid, command) in &unit.processes {
        assert_eq!(
            *uid, 0,
            "process {pid} of the dead reactor survived: {command}"
        );
    }
}

/// The orphaned create keeps the record locked until it finishes; recovery
/// must skip it while locked, then release the container it leaves.
#[test]
fn a_preview_killed_while_its_container_is_created() {
    let platform = load();
    let mut evidence = Evidence::new(&platform, "preview-killed-creating");
    let staged = stage(&platform, "vmm-creating");
    let shared = root(&platform);
    // The preview's own podman, so that its hold catches only its launch,
    // beneath the stack's state directory.
    let own = launcher::root();
    let recoverer = launcher::root();
    let recoverer_at = at(&recoverer, &shared.state_dir);

    let hold = launcher::hold(&own, "create");
    let preview = Preview::spawn(
        &platform,
        &at(&own, &shared.state_dir),
        &staged,
        "numbers",
        "creating-preview",
    );
    launcher::wait_held(&own, "create", STARTED);
    let name = launcher::vmm_name(&own).expect("the launch made its network");
    let record = launcher::record(&shared.state_dir, &name);
    let token = launcher::token(&record);
    preview.kill();
    assert!(
        launcher::locked(&record),
        "the orphaned create holds {name}'s record"
    );
    assert!(launcher::network_exists(&name) && !launcher::container_exists(&name));
    evidence.push(format!(
        "flowctl preview killed while podman held the create of {name}; its record stays locked"
    ));

    let passed = spec_launch(&platform, &recoverer_at, &staged, "creating-passed-over");
    assert!(!reports_recovering(&passed, &name));
    assert!(launcher::locked(&record), "passed over");
    assert!(!launcher::container_exists(&name), "nothing was made yet");
    assert!(
        !launcher::calls(&recoverer)
            .iter()
            .any(|call| call.contains(&token)),
        "a locked record's resources aren't even looked for"
    );
    evidence.push(format!(
        "a launch meanwhile passed it over: {:?}",
        named(&launcher::calls(&recoverer))
    ));

    std::mem::drop(hold);
    run::eventually(
        "the orphaned create to make the container and end",
        STARTED,
        || (launcher::container_exists(&name) && !launcher::locked(&record)).then_some(()),
    );
    let inspected = run::podman(&[
        "container",
        "inspect",
        "--format",
        "{{.Id}} {{.State.Status}} {{.GraphDriver.Data.UpperDir}}",
        &name,
    ]);
    let [id, status, layer] = inspected.split_whitespace().collect::<Vec<_>>()[..] else {
        panic!("inspecting {name}: {inspected:?}");
    };
    assert_eq!(status, "created", "made by the orphan, never started");
    assert!(launcher::path_exists_as_root(layer));
    let before = launcher::left(&shared.state_dir, &own.tmp_dir, &name);
    assert_eq!(
        before.len(),
        5,
        "container, network, state, record and mount: {before:?}"
    );
    evidence.push(format!(
        "the orphan made container {id} ({status}, layer {layer}) and let the record go; left {before:?}"
    ));

    let made = launcher::calls(&recoverer).len();
    let recovering = spec_launch(&platform, &recoverer_at, &staged, "creating-recovery");
    assert!(
        reports_recovering(&recovering, &name),
        "the next launch reports releasing {name}"
    );
    let calls = launcher::calls(&recoverer)[made..].to_vec();
    assert!(
        calls.contains(&format!("rm --force --time=0 --ignore {id}")),
        "{calls:#?}"
    );
    assert_eq!(
        launcher::left(&shared.state_dir, &own.tmp_dir, &name),
        Vec::<String>::new()
    );
    assert!(
        !launcher::path_exists_as_root(layer),
        "the container's layer"
    );
    evidence.push(format!(
        "the next launch released {name}: container, its layer, network, state, record and mount"
    ));

    let mut names = named(&launcher::calls(&own));
    names.extend(named(&launcher::calls(&recoverer)));
    assert_released(
        &names,
        &[&shared, &at(&own, &shared.state_dir), &recoverer_at],
    );
    evidence.push(format!("every launch released: {names:?}"));
}

fn of_task(call: &str, task: &str) -> bool {
    let label = format!("--label=task-name={task}");
    call.split(' ').any(|arg| arg == label)
}

/// Recovery must release the dead shard's VMM while preserving live previews
/// in both shared and independent state directories, and resume derivation.
#[test]
fn a_reactor_killed_while_serving() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "reactor-killed-serving");
    let staged = stage(&platform, "vmm-serving");
    let shared = root(&platform);
    let independent = launcher::root();
    let recoverer = launcher::root();
    let recoverer_at = at(&recoverer, &shared.state_dir);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    let numbers = staged.name("numbers");
    let schema = staged.underscored();
    let ordinary_tasks = [
        staged.name("hello-world"),
        staged.name("ordinary-numbers"),
        staged.name("postgres"),
    ];
    let before = launcher::calls(&shared).len();

    let published = publish(
        &platform,
        &shared,
        &staged,
        "ordinary.flow.yaml",
        "serving-publish",
    );
    assert!(
        published.status.success(),
        "publishing: {}",
        tail(&published.stderr, 60)
    );
    let docs = derived_holding(&platform, &numbers, "serving-numbers", |docs| {
        NUMBERED.clone().all(|n| docs.contains_key(&n))
    });
    for n in NUMBERED {
        assert_numbered(&docs[&n], n, true);
    }
    let docs = derived_holding(
        &platform,
        &staged.name("ordinary-numbers"),
        "serving-ordinary-numbers",
        |docs| NUMBERED.clone().all(|n| docs.contains_key(&n)),
    );
    for n in NUMBERED {
        assert_numbered(&docs[&n], n, false);
    }
    let rows = assert_materializing(
        &schema,
        &["greetings", "numbers", "ordinary_numbers"],
        "-infinity",
    );
    let containers = ordinary_containers(&ordinary_tasks);
    for (task, running) in &containers {
        assert!(!running.is_empty(), "{task} runs in an ordinary container");
    }
    for call in &launcher::calls(&shared)[before..] {
        for task in &ordinary_tasks {
            assert!(
                !of_task(call, task),
                "{task} reached the VMM podman: {call}"
            );
        }
    }
    evidence.push(format!(
        "ordinary tasks run as Docker containers {containers:?}, none through the VMM podman; \
         materialized {rows:?}; ordinary-numbers resolves every name"
    ));

    let known = shard_vmms(&platform, &numbers, "serving-task-logs");
    let dead = known.last().expect("the shard started a VMM").clone();
    let dead_fp = launcher::footprint(&shared.state_dir, &shared.tmp_dir, &dead);
    let record = launcher::record(&shared.state_dir, &dead);

    let mut same = Preview::spawn(&platform, &shared, &staged, "numbers", "serving-same");
    let mut other = Preview::spawn(
        &platform,
        &independent,
        &staged,
        "numbers",
        "serving-independent",
    );
    assert_numbered(&same.send(7, FIRST_DOCUMENT), 7, true);
    assert_numbered(&other.send(7, FIRST_DOCUMENT), 7, true);
    let same_fp = launcher::footprint(&shared.state_dir, &shared.tmp_dir, &same.vmm());
    let other_fp = launcher::footprint(&independent.state_dir, &independent.tmp_dir, &other.vmm());
    evidence.push(format!(
        "serving: the shard's {dead}, preview {} beneath the shared state directory, \
         preview {} beneath its own",
        same_fp.name, other_fp.name
    ));

    let token = launcher::token(&record);
    let rpc = rpc::hold(
        &runtime,
        &format!("{}/{dead}/sock/init.sock", shared.state_dir),
        STARTED,
    );
    let numbered = *derived(&platform, &numbers, "serving-numbers-before")
        .keys()
        .max()
        .expect("numbered greetings");
    // Only the reactor launches through the shared podman from here.
    let watched = launcher::calls(&shared).len();
    let main = kill_reactor(&platform);
    let killed = Instant::now();
    let killed_at = sql("SELECT now()::text");

    // Root's attach client of the dead VMM may outlive its sudo, but not
    // necessarily for long; the VMM is the container's, and runs on.
    let client = format!("start --attach {}", dead_fp.id);
    let dead_unit = unit(&platform);
    assert_rootful(&dead_unit);
    launcher::assert_whole(&dead_fp);
    assert!(!launcher::locked(&record), "its owner is dead");
    evidence.push(format!(
        "reactor {main:?} killed; its unit is {} ({}), holding {:?}; the dead VMM's attach \
         client: {:?}; {dead} serves on, its record unlocked",
        dead_unit.active,
        dead_unit.sub,
        dead_unit.processes,
        running(&client)
    ));

    let restarted = reactor_restarted(&platform, &main);
    let restarted_after = killed.elapsed();
    let vmm = &dead_fp.processes[0];
    assert!(
        launcher::container_exists(&dead_fp.id)
            && launcher::start_time(vmm.1).as_ref() == Some(&vmm.2),
        "restarted while the dead reactor's VMM runs"
    );
    evidence.push(format!(
        "systemd restarted the reactor as {restarted:?} {restarted_after:?} after the kill, \
         {dead} still running; its attach client then: {:?}",
        running(&client)
    ));

    let relaunched = run::eventually("the shard's new VMM", DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        shard_vmms(&platform, &numbers, "serving-task-logs-after")
            .into_iter()
            .find(|name| !known.contains(name))
    });
    assert_ne!(
        relaunched, dead,
        "a fresh id, and so a fresh state directory and socket"
    );
    let calls = launcher::calls(&shared)[watched..].to_vec();
    let pull = calls
        .iter()
        .position(|call| call.starts_with("pull "))
        .expect("the relaunch pulled its image");
    let owner = format!("label=dev.estuary.vmm-owner={token}");
    assert!(
        calls[..pull]
            .iter()
            .any(|call| call.starts_with("ps ") && call.contains(&owner))
            && calls[..pull].contains(&format!("rm --force --time=0 --ignore {}", dead_fp.id)),
        "the relaunch released {dead} before its own pull: {calls:#?}"
    );
    assert!(named(&calls).contains(&relaunched), "{calls:#?}");
    assert_eq!(launcher::remaining(&dead_fp), Vec::<String>::new());
    launcher::assert_whole(&same_fp);
    launcher::assert_whole(&other_fp);
    std::mem::drop(rpc);
    run::eventually("the dead VMM's attach client to exit", RELEASED, || {
        running(&client).is_empty().then_some(())
    });
    evidence.push(format!(
        "the restarted reactor's relaunch {relaunched} released {dead} before its own pull, \
         all of {dead_fp:?}, and left both neighbours whole; its attach client then exited"
    ));

    let docs = derived_holding(&platform, &numbers, "serving-numbers-after", |docs| {
        docs.keys().any(|n| *n > numbered + 2)
    });
    for (n, doc) in docs.range(numbered + 1..) {
        assert_numbered(doc, *n, true);
    }
    let rows = assert_materializing(
        &schema,
        &["greetings", "numbers", "ordinary_numbers"],
        &killed_at,
    );
    launcher::assert_whole(&same_fp);
    launcher::assert_whole(&other_fp);
    assert_numbered(&same.send(8, DOCUMENT), 8, true);
    assert_numbered(&other.send(8, DOCUMENT), 8, true);
    evidence.push(format!(
        "the shard resumed in {relaunched}, numbering greetings past {numbered}; rows \
         materialized since the kill {rows:?}; both neighbours whole and deriving"
    ));

    same.kill();
    run::eventually("the killed preview's VMM to end", RELEASED, || {
        let gone = |what: &str| {
            same_fp
                .processes
                .iter()
                .find(|(name, ..)| name == what)
                .is_some_and(|(_, pid, started)| {
                    launcher::start_time(*pid).as_ref() != Some(started)
                })
        };
        (gone("VMM") && gone("conmon") && !launcher::container_exists(&same_fp.id)).then_some(())
    });
    let left = launcher::remaining(&same_fp);
    let mut expected = vec![format!("network {}", same_fp.name)];
    expected.extend(same_fp.paths.iter().cloned());
    assert_eq!(left, expected, "what the killed preview left for recovery");
    let recovering = spec_launch(&platform, &recoverer_at, &staged, "serving-killed-recovery");
    assert!(reports_recovering(&recovering, &same_fp.name));
    assert_eq!(launcher::remaining(&same_fp), Vec::<String>::new());
    launcher::assert_whole(&other_fp);
    evidence.push(format!(
        "preview {} killed while serving: its VMM, conmon and container ended by themselves, \
         leaving {left:?}, which the next launch released",
        same_fp.name
    ));

    other.end();
    let independent_at = at(&recoverer, &independent.state_dir);
    spec_launch(
        &platform,
        &independent_at,
        &staged,
        "serving-independent-recovery",
    );
    delete(&platform, &staged, "serving");

    let mut names = named(&launcher::calls(&shared)[before..]);
    names.extend(named(&launcher::calls(&independent)));
    names.extend(named(&launcher::calls(&recoverer)));
    assert_released(
        &names,
        &[&shared, &independent, &recoverer_at, &independent_at],
    );
    evidence.push(format!("every launch released: {names:?}"));
}

/// A rootful network create survives the reactor's death with its fence.
/// Both launchers must skip its locked record, then recover after it finishes.
#[test]
fn a_reactor_killed_while_its_network_is_created() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "reactor-killed-creating");
    let staged = stage(&platform, "vmm-restarted");
    variant(&staged, "second", true);
    let shared = root(&platform);
    // flowctl validates through its own podman and state directory, so that
    // the hold catches the reactor's launch and only the reactor recovers it.
    let publisher = launcher::root();
    let recoverer = launcher::root();
    let recoverer_at = at(&recoverer, &shared.state_dir);
    let before = launcher::calls(&shared).len();

    let hold = launcher::root_hold(&shared, "network-create");
    let label = "restart-publish";
    let args = [
        "catalog",
        "publish",
        "--source",
        &staged.source("flow.yaml"),
        "--auto-approve",
    ];
    let mut first = flowctl_command(&platform, &publisher, &args, &[])
        .stdin(std::process::Stdio::null())
        .stdout(create(&platform.run_dir.join(format!("{label}.stdout"))))
        .stderr(create(&platform.run_dir.join(format!("{label}.stderr"))))
        .spawn()
        .expect("spawning flowctl catalog publish");
    launcher::wait_held(&shared, "network-create", STARTED);
    let name = launched(&launcher::calls(&shared)[before..])
        .pop()
        .expect("the reactor's launch named its network");
    let record = launcher::record(&shared.state_dir, &name);
    let token = launcher::token(&record);
    let held_command = |unit: &Unit| {
        unit.processes
            .iter()
            .any(|(_, _, command)| command.contains("root-hold") && command.contains(&name))
    };

    let main = kill_reactor(&platform);
    let killed = Instant::now();
    let held = unit(&platform);
    assert_rootful(&held);
    assert!(held_command(&held), "the held command remains: {held:?}");
    assert!(
        launcher::locked(&record),
        "the held network create keeps the dead reactor's fence"
    );
    assert!(!launcher::network_exists(&name));
    evidence.push(format!(
        "reactor {main:?} killed while podman, as root, held the network create of {name}; \
         its unit is {} ({}), holding {:?}; the record stays locked",
        held.active, held.sub, held.processes
    ));

    let restarted = reactor_restarted(&platform, &main);
    let restarted_after = killed.elapsed();
    let at_restart = launcher::calls(&shared).len();
    let beside = unit(&platform);
    assert!(
        held_command(&beside) && launcher::locked(&record),
        "restarted beside the held command: {beside:?}"
    );
    evidence.push(format!(
        "systemd restarted the reactor as {restarted:?} {restarted_after:?} after the kill, \
         beside the held command"
    ));

    // flowctl was waiting on the agent's validation, which the kill ended.
    let deadline = Instant::now() + COMMAND;
    let first = loop {
        if let Some(status) = first.try_wait().expect("polling flowctl") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the first publication never ended"
        );
        std::thread::sleep(Duration::from_millis(500));
    };
    evidence.push(format!(
        "the publication whose validation the reactor died in exited {first}"
    ));

    let passed = spec_launch(&platform, &recoverer_at, &staged, "restart-passed-over");
    assert!(!reports_recovering(&passed, &name));
    assert!(
        !launcher::calls(&recoverer)
            .iter()
            .any(|call| call.contains(&token)),
        "a locked record's resources aren't even looked for"
    );
    if !first.success() {
        let again = publish(
            &platform,
            &publisher,
            &staged,
            "flow.yaml",
            "restart-republish",
        );
        assert!(
            again.status.success(),
            "republishing: {}",
            tail(&again.stderr, 60)
        );
    }
    let docs = derived_holding(
        &platform,
        &staged.name("numbers"),
        "restart-numbers",
        |docs| NUMBERED.clone().all(|n| docs.contains_key(&n)),
    );
    for n in NUMBERED {
        assert_numbered(&docs[&n], n, true);
    }
    let calls = launcher::calls(&shared)[at_restart..].to_vec();
    let launches = named(&calls);
    assert!(!launches.is_empty(), "the restarted reactor launched");
    assert!(
        !calls.iter().any(|call| call.contains(&token)),
        "the restarted reactor passed the record over: {calls:#?}"
    );
    assert!(launcher::locked(&record), "passed over");
    assert!(!launcher::network_exists(&name), "nothing was made yet");
    evidence.push(format!(
        "a flowctl launch, and the restarted reactor's launches {launches:?} validating and \
         running the publication, passed the record over while the command was held"
    ));

    std::mem::drop(hold);
    run::eventually(
        "the orphaned network create to make it and end",
        STARTED,
        || (launcher::network_exists(&name) && !launcher::locked(&record)).then_some(()),
    );
    evidence.push(format!(
        "released, the command made network {name} and ended, letting the record go"
    ));

    let released = launcher::calls(&shared).len();
    let second = publish(
        &platform,
        &publisher,
        &staged,
        "second.flow.yaml",
        "restart-second",
    );
    assert!(
        second.status.success(),
        "publishing: {}",
        tail(&second.stderr, 60)
    );
    let calls = launcher::calls(&shared)[released..].to_vec();
    let pull = calls
        .iter()
        .position(|call| call.starts_with("pull "))
        .expect("the reactor launched");
    let owner = format!("label=dev.estuary.vmm-owner={token}");
    assert!(
        calls[..pull].iter().any(|call| call.contains(&owner))
            && calls[..pull].contains(&format!("network rm {name}")),
        "the reactor's next launch released {name}: {calls:#?}"
    );
    assert_eq!(
        launcher::left(&shared.state_dir, &shared.tmp_dir, &name),
        Vec::<String>::new()
    );
    evidence.push(format!(
        "the reactor's next launch released {name}'s network, state, record and mount \
         before its own pull"
    ));

    delete(&platform, &staged, "restart");
    let mut names = named(&launcher::calls(&shared)[before..]);
    names.extend(named(&launcher::calls(&publisher)));
    names.extend(named(&launcher::calls(&recoverer)));
    assert_released(&names, &[&shared, &publisher, &recoverer_at]);
    evidence.push(format!("every launch released: {names:?}"));
}

/// Replace `task-update.json` in connector mount `mount` as the runtime's
/// producer does: staged beside it, read-only, then renamed over it, so that
/// each generation is a new file. When it was renamed.
fn produce(mount: &str, content: &str) -> Instant {
    use std::os::unix::fs::PermissionsExt;

    let staged = format!("{mount}/.task-update.json.tmp");
    match std::fs::remove_file(&staged) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("removing {staged}: {e}"),
    }
    std::fs::write(&staged, content).unwrap_or_else(|e| panic!("writing {staged}: {e}"));
    std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o444))
        .unwrap_or_else(|e| panic!("setting the mode of {staged}: {e}"));
    std::fs::rename(&staged, format!("{mount}/task-update.json"))
        .unwrap_or_else(|e| panic!("renaming {staged}: {e}"));
    Instant::now()
}

/// A synthetic `task-update.json`, as the runtime's producer would write it.
fn generation(g: u32) -> serde_json::Value {
    serde_json::Value::String(
        serde_json::json!({
            "token": format!("acmeCo-synthetic-token-{g}"),
            "control_plane_url": "https://control.acmeco.example",
            "config_encryption_url": "https://encryption.acmeco.example",
        })
        .to_string(),
    )
}

/// Feed `preview` a greeting every `POLL` until its fresh read of the file
/// renamed at `renamed` sees `next`. Until then, every fresh read must see
/// `previous` whole, and the file held since the path was first found reads
/// `held` once the path has been found, and `held_before` before. How long
/// after the rename the guest saw it, and in how many documents.
fn await_generation(
    preview: &mut Preview,
    n: &mut i64,
    renamed: Instant,
    previous: &serde_json::Value,
    next: &serde_json::Value,
    held_before: &serde_json::Value,
    held: &serde_json::Value,
) -> (Duration, i64) {
    let first = *n;
    loop {
        *n += 1;
        let remaining = VISIBLE
            .checked_sub(renamed.elapsed())
            .expect("the fresh generation must arrive within the visibility deadline");
        let doc = preview.send(*n, remaining);
        let elapsed = renamed.elapsed();
        assert!(
            elapsed < VISIBLE,
            "{next} never reached a fresh read within {VISIBLE:?}"
        );
        if doc["fresh"] == *next {
            assert_eq!(doc["held"], *held, "{doc}");
            return (elapsed, *n - first);
        }
        assert_eq!(doc["fresh"], *previous, "never torn nor missing: {doc}");
        assert_eq!(doc["held"], *held_before, "{doc}");
        std::thread::sleep(POLL);
    }
}

/// Fresh opens must see atomic replacements through the read-only mount;
/// a held descriptor must keep its original generation. Cover both a file
/// present at session start and one created after the first read.
#[test]
fn task_update_through_the_mount() {
    let platform = load();
    let mut evidence = Evidence::new(&platform, "task-update");
    let staged = stage(&platform, "vmm-updates");
    let own = launcher::root();
    // SAFETY: geteuid takes nothing and cannot fail.
    let euid = unsafe { libc::geteuid() };
    let mount_of = |name: &str| format!("{}/connector-mounts-{euid}/mount-{name}", own.tmp_dir);
    let absent = serde_json::Value::Null;

    // Present before the session starts: written while podman holds the
    // session's start, the preview's second launch after its Validate.
    let hold = launcher::hold_nth(&own, "start", 2);
    let mut preview = Preview::spawn(&platform, &own, &staged, "updates", "updates-present");
    launcher::wait_held(&own, "start", STARTED);
    let launches = launched(&launcher::calls(&own));
    let [_, session] = &launches[..] else {
        panic!("the preview's Validate, then its session: {launches:?}");
    };
    let mount = mount_of(session);
    produce(&mount, generation(1).as_str().unwrap());
    std::mem::drop(hold);

    let mut n = 1;
    let doc = preview.send(n, FIRST_DOCUMENT);
    assert_eq!(doc["fresh"], generation(1), "{doc}");
    assert_eq!(doc["held"], generation(1), "{doc}");
    assert_eq!(doc["uid"], NOBODY, "the image's user reads it: {doc}");
    assert_eq!(doc["mount"], mount.as_str(), "at CONNECTOR_MOUNT: {doc}");
    let options = doc["options"].as_str().expect("the mount's options");
    assert!(
        options.split(',').any(|o| o == "ro"),
        "read-only: {options}"
    );
    evidence.push(format!(
        "{session}: present at start, read as uid {} at {mount} ({options})",
        doc["uid"]
    ));
    for g in [2, 3] {
        let renamed = produce(&mount, generation(g).as_str().unwrap());
        let (delay, documents) = await_generation(
            &mut preview,
            &mut n,
            renamed,
            &generation(g - 1),
            &generation(g),
            &generation(1),
            &generation(1),
        );
        evidence.push(format!(
            "{session}: generation {g} reached a fresh read {delay:?} after its rename \
             ({documents} documents at {POLL:?}); the held file still read generation 1"
        ));
    }
    preview.end();

    let mut preview = Preview::spawn(&platform, &own, &staged, "updates", "updates-absent");
    let mut n = 1;
    let doc = preview.send(n, FIRST_DOCUMENT);
    assert_eq!((&doc["fresh"], &doc["held"]), (&absent, &absent), "{doc}");
    let session = preview.vmm();
    let mount = mount_of(&session);
    let renamed = produce(&mount, generation(1).as_str().unwrap());
    let (delay, documents) = await_generation(
        &mut preview,
        &mut n,
        renamed,
        &absent,
        &generation(1),
        &absent,
        &generation(1),
    );
    evidence.push(format!(
        "{session}: absent at its first read; generation 1 reached a fresh read {delay:?} \
         after its rename ({documents} documents)"
    ));
    let renamed = produce(&mount, generation(2).as_str().unwrap());
    let (delay, documents) = await_generation(
        &mut preview,
        &mut n,
        renamed,
        &generation(1),
        &generation(2),
        &generation(1),
        &generation(1),
    );
    evidence.push(format!(
        "{session}: generation 2 reached a fresh read {delay:?} after its rename \
         ({documents} documents); the held file still read generation 1"
    ));
    preview.end();

    spec_launch(&platform, &own, &staged, "updates-recovery");
    let names = named(&launcher::calls(&own));
    assert_released(&names, &[&own]);
    evidence.push(format!("every launch released: {names:?}"));
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// The VMM's container is created without `/dev/kvm`.
    NoKvm,
    /// The launcher verifies an empty network namespace, as a host which no
    /// one has given the boundary.
    MissingBoundary,
    /// The launcher verifies a network namespace whose boundary lacks one of
    /// its baseline exclusions.
    MismatchedBoundary,
}

/// `flow-connector-vmm boundary ACTION` of the run's VMM image, in network
/// namespace `netns`, which the test owns.
fn boundary_in(platform: &Platform, netns: &str, action: &str) {
    let mut argv = vec!["podman".to_string()];
    argv.extend(launch::boundary(
        &platform.vmm_image,
        &format!("ns:/run/netns/{netns}"),
        action,
    ));
    run::sudo(&argv.iter().map(String::as_str).collect::<Vec<_>>());
}

fn nft_in(platform: &Platform, netns: &str, command: &str) {
    let mut argv = vec![
        "podman".to_string(),
        "run".to_string(),
        "--rm".to_string(),
        format!("--network=ns:/run/netns/{netns}"),
        "--cap-drop=all".to_string(),
        "--cap-add=CAP_NET_ADMIN".to_string(),
        "--entrypoint=nft".to_string(),
        platform.vmm_image.clone(),
    ];
    argv.extend(command.split_whitespace().map(str::to_string));
    run::sudo(&argv.iter().map(String::as_str).collect::<Vec<_>>());
}

/// Preview, publication validation, and shard relaunch must refuse unsafe
/// VMM launches without falling back to ordinary containers or stopping
/// ordinary tasks.
#[test]
fn refused_launches_beside_ordinary_tasks() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "refused-launches");
    let staged = stage(&platform, "vmm-refused");
    let shared = root(&platform);
    // flowctl's own validation of a publication goes through a podman of its
    // own, unrefused, so that the reactor's is the validation refused.
    let publisher = launcher::root();
    let netns = run::netns();
    let schema = staged.underscored();
    let faults = [
        ("kvm", Fault::NoKvm),
        ("boundary", Fault::MissingBoundary),
        ("mismatch", Fault::MismatchedBoundary),
    ];
    let mut imports = vec!["ordinary.flow.yaml".to_string()];
    for (leaf, _) in faults {
        variant(&staged, &format!("refused-{leaf}"), true);
        variant(&staged, &format!("probe-{leaf}"), true);
        imports.push(format!("refused-{leaf}.flow.yaml"));
    }
    let catalog = staged.dir.join("refused.flow.yaml");
    std::fs::write(
        &catalog,
        serde_json::json!({ "import": imports }).to_string(),
    )
    .unwrap_or_else(|e| panic!("writing {}: {e}", catalog.display()));
    let ordinary_tasks = [
        staged.name("hello-world"),
        staged.name("ordinary-numbers"),
        staged.name("postgres"),
    ];
    let mut creations = watch_creations();
    let before = launcher::calls(&shared).len();

    let published = publish(
        &platform,
        &shared,
        &staged,
        "refused.flow.yaml",
        "refused-publish",
    );
    assert!(
        published.status.success(),
        "publishing: {}",
        tail(&published.stderr, 60)
    );
    let mut running = BTreeMap::new();
    for (leaf, _) in faults {
        let task = staged.name(&format!("refused-{leaf}"));
        let docs = derived_holding(
            &platform,
            &task,
            &format!("refused-{leaf}-numbers"),
            |docs| NUMBERED.clone().all(|n| docs.contains_key(&n)),
        );
        for n in NUMBERED {
            assert_numbered(&docs[&n], n, true);
        }
        let vmms = shard_vmms(&platform, &task, &format!("refused-{leaf}-task-logs"));
        let [vmm] = &vmms[..] else {
            panic!("{task} started one VMM: {vmms:?}");
        };
        running.insert(leaf, vmm.clone());
    }
    let rows = assert_materializing(&schema, &["greetings", "ordinary_numbers"], "-infinity");
    let containers = ordinary_containers(&ordinary_tasks);
    for (task, running) in &containers {
        assert!(!running.is_empty(), "{task} runs in an ordinary container");
    }
    let created = creations.seen();
    for task in &ordinary_tasks {
        assert!(created.contains(task), "Docker's creations: {created:?}");
    }
    evidence.push(format!(
        "published and running: {running:?} in VMMs, ordinary {containers:?}, \
         materialized {rows:?}"
    ));

    for (leaf, fault) in faults {
        let task = staged.name(&format!("refused-{leaf}"));
        let probe = staged.name(&format!("probe-{leaf}"));
        let since = sql("SELECT now()::text");
        let made = launcher::calls(&shared).len();
        let (control, expected): (launcher::Control, &[&str]) = match fault {
            Fault::NoKvm => (
                launcher::no_kvm(&shared),
                &[
                    "the VMM exited before flow-connector-init started",
                    "KVM is unavailable to this VMM: opening /dev/kvm",
                ],
            ),
            Fault::MissingBoundary => (
                launcher::verify_in(&shared, &netns.name),
                &[UNVERIFIED, "table is absent"],
            ),
            Fault::MismatchedBoundary => {
                boundary_in(&platform, &netns.name, "install");
                nft_in(
                    &platform,
                    &netns.name,
                    "delete element inet flow_vmm_boundary baseline { 169.254.0.0/16 }",
                );
                (launcher::verify_in(&shared, &netns.name), &[UNVERIFIED])
            }
        };

        let previewed = flowctl_status(
            &platform,
            &format!("refused-{leaf}-preview"),
            &[
                "raw",
                "preview-next",
                "--source",
                &staged.source("numbers.flow.yaml"),
                "--name",
                &staged.name("numbers"),
                "--fixture",
                &staged.source("greetings.ndjson"),
                "--log-json",
            ],
            &[],
        );
        assert!(!previewed.status.success(), "the preview is refused");
        for line in expected {
            assert!(previewed.stderr.contains(line), "the preview says {line:?}");
        }
        assert!(!previewed.stderr.contains("started connector container"));
        // libkrun never ran to panic, nor did its backtrace pass for readiness.
        for line in [
            "Error creating the Kvm object",
            "failed to connect to the VMM",
        ] {
            assert!(
                !previewed.stderr.contains(line),
                "the preview says {line:?}"
            );
        }

        let validated = publish(
            &platform,
            &publisher,
            &staged,
            &format!("probe-{leaf}.flow.yaml"),
            &format!("refused-{leaf}-publish"),
        );
        assert!(!validated.status.success(), "the publication is refused");
        let said = format!("{}{}", validated.stdout, validated.stderr);
        for line in expected {
            assert!(said.contains(line), "the publication says {line:?}");
        }

        let vmm = &running[leaf];
        run::podman(&["rm", "--force", "--time=0", vmm]);
        let logs = task_logs_holding(
            &platform,
            &task,
            &format!("refused-{leaf}-refusal-logs"),
            expected,
        );
        let started: Vec<String> = logs
            .lines()
            .filter(|line| line.contains("started connector container"))
            .flat_map(vmm_names)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        assert_eq!(
            &started,
            std::slice::from_ref(vmm),
            "no VMM of {task} started since"
        );

        let created = creations.seen();
        for of in [&task, &probe] {
            assert!(
                !created.contains(of),
                "Docker made {of} a container: {created:?}"
            );
        }
        let calls = launcher::calls(&shared)[made..].to_vec();
        match fault {
            Fault::NoKvm => {
                for call in calls.iter().filter(|call| call.starts_with("create ")) {
                    assert!(
                        call.contains(&format!(" {} run ", platform.vmm_image))
                            && call.contains("--device=/dev/kvm"),
                        "each container the launcher asked for was a VMM's, with KVM: {call}"
                    );
                }
            }
            Fault::MissingBoundary | Fault::MismatchedBoundary => {
                let made: Vec<&String> = calls
                    .iter()
                    .filter(|call| {
                        call.starts_with("network create ") || call.starts_with("create ")
                    })
                    .collect();
                assert_eq!(
                    made,
                    Vec::<&String>::new(),
                    "nothing past a failed verification"
                );
                assert!(calls.iter().any(|call| call.ends_with(" boundary verify")));
            }
        }

        let rows = assert_materializing(&schema, &["greetings", "ordinary_numbers"], &since);
        std::mem::drop(control);
        let refusal = logs
            .lines()
            .find(|line| line.contains(expected[0]))
            .unwrap_or_default()
            .to_string();
        evidence.push(format!(
            "{fault:?}: preview, publication and {task}'s relaunch refused; task logs: {refusal}; \
             ordinary rows since the fault {rows:?}"
        ));
    }

    delete(&platform, &staged, "refused");
    let mut names = named(&launcher::calls(&shared)[before..]);
    names.extend(named(&launcher::calls(&publisher)));
    assert_released(&names, &[&shared, &publisher]);
    evidence.push(format!("every launch released: {names:?}"));
}
