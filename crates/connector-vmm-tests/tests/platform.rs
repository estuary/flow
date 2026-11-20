//! Run under `mise run ci:connector-vmm-kvm --platform`. The tests share a
//! local stack and use its reactor's podman log to account for VMM launches
//! during discovery, publication and task execution.
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
    pypi.org, files.pythonhosted.org, example.org (task egress.hosts)";
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
    /// Holds the reactor's podman (and its `calls`), state directory and
    /// TMPDIR.
    dir: String,
    vmm_image: String,
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
    serde_json::from_slice(&run::read_private("platform.json"))
        .unwrap_or_else(|e| panic!("parsing {}/platform.json: {e}", run::ROOT))
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

fn flowctl(platform: &Platform, name: &str, args: &[&str]) -> Output {
    let output = flowctl_status(platform, name, args);
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
/// environment.
fn flowctl_status(platform: &Platform, name: &str, args: &[&str]) -> Output {
    let stdout = platform.run_dir.join(format!("{name}.stdout"));
    let stderr = platform.run_dir.join(format!("{name}.stderr"));
    let mut child = std::process::Command::new(&platform.flowctl)
        .args(args)
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

/// Publication streams remote job logs to stdout and local logs to stderr.
fn assert_no_local_launch(what: &str, output: &Output) {
    assert!(
        started(&output.stderr).is_empty(),
        "flowctl {what} started a connector of its own:\n{}",
        tail(&output.stderr, 40),
    );
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

/// Wait for teardown to release each launch.
fn assert_released<'n>(root: &launcher::Root, names: impl IntoIterator<Item = &'n String>) {
    let deadline = Instant::now() + RELEASED;
    for name in names {
        run::eventually(
            &format!("the release of {name}"),
            deadline.saturating_duration_since(Instant::now()),
            || {
                launcher::left(&root.state_dir, &root.tmp_dir, name)
                    .is_empty()
                    .then_some(())
            },
        );
    }
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

fn checked_mounts(platform: &Platform) -> Vec<String> {
    match std::fs::read_to_string(format!("{}/checked-mounts", platform.dir)) {
        Ok(checked) => checked.lines().map(ToString::to_string).collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("reading the wrapper's mount checks: {e}"),
    }
}

#[test]
fn a_python_derivation_through_the_platform() {
    let platform = load();
    let root = root(&platform);
    expect_connector_init(&platform);
    let source = run::fixture("platform/flow.yaml");
    let source = source.to_string_lossy();
    let mut evidence = Vec::new();
    // Earlier tests share the wrapper's records, which this test's checks of
    // every launch skip.
    let first = launcher::calls(&root).len();
    let first_mount = checked_mounts(&platform).len();

    // A dry run: the agent has the stack's reactor validate the derivation,
    // whose logs flowctl prints on stdout, and activates nothing.
    let before = launcher::calls(&root).len();
    let tested = flowctl(&platform, "test", &["catalog", "test", "--source", &source]);
    assert_no_local_launch("catalog test", &tested);
    let dry_run = started(&tested.stdout);
    assert_eq!(dry_run.len(), 1, "the reactor validated once, in a VMM");
    assert_eq!(tested.stdout.matches(EGRESS).count(), 1);
    assert_eq!(
        launched(&launcher::calls(&root)[before..])
            .into_iter()
            .collect::<BTreeSet<_>>(),
        dry_run,
        "the dry run's Validate was its only launch"
    );
    assert_released(&root, &dry_run);
    evidence.push(format!(
        "dry run: the reactor validated in {dry_run:?}, and released it"
    ));

    let before = launcher::calls(&root).len();
    let publish = flowctl(
        &platform,
        "publish",
        &["catalog", "publish", "--source", &source, "--auto-approve"],
    );
    assert_no_local_launch("catalog publish", &publish);
    let validated = started(&publish.stdout);
    assert_eq!(validated.len(), 1, "the reactor validated once, in a VMM");
    assert!(validated.is_disjoint(&dry_run));
    assert_eq!(publish.stdout.matches(EGRESS).count(), 1);
    evidence.push(format!(
        "publication: the reactor validated in {validated:?}"
    ));

    let numbered = run::eventually("the derivation's documents", DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        // Fails until the shard has made the collection's journal.
        let docs: BTreeMap<i64, serde_json::Value> = read(&platform, "numbers", DERIVATION)?
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
    let logs = task_logs(
        &platform,
        "task-logs",
        DERIVATION,
        &[EGRESS, "started connector container", REFUSED, "Installed"],
    );
    let opened = started(&logs.stdout);
    assert!(!opened.is_empty(), "the shard's sessions name their VMMs");
    assert!(opened.is_disjoint(&validated) && opened.is_disjoint(&dry_run));
    assert_vmm_logs("the task's logs", &logs.stdout);
    evidence.push(format!("reactor sessions: {opened:?}"));

    let published: BTreeSet<String> = launched(&launcher::calls(&root)[before..])
        .into_iter()
        .collect();
    let expected: BTreeSet<String> = validated.iter().chain(&opened).cloned().collect();
    assert_eq!(
        published, expected,
        "every VMM since publication was the reactor's Validate or a session"
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

    delete_and_release(&platform, "delete");
    evidence.push(format!(
        "after deletion prune released the sessions {opened:?}"
    ));

    let all = launcher::calls(&root)[first..].to_vec();
    let launches = launched(&all);
    let state: Vec<_> = std::fs::read_dir(&root.state_dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", root.state_dir))
        .map(|entry| entry.expect("reading the state directory").path())
        .collect();
    assert!(state.is_empty(), "the state directory holds {state:?}");

    assert_eq!(
        checked_mounts(&platform)
            .into_iter()
            .skip(first_mount)
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
            call.contains(&format!("--label=task-name={DERIVATION}")),
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
        "{} launches, each the reactor's, handed {} and released; reactor unit {}",
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

/// Kill the one running VMM of `task`, and wait for the reactor to restart its
/// session in another.
fn kill_and_restart(task: &str) {
    let killed = running(task);
    assert_eq!(killed.len(), 1, "one session runs: {killed:?}");
    for vmm in &killed {
        run::podman(&["rm", "--force", vmm]);
    }
    run::eventually("the session's restart", DERIVED, || {
        std::thread::sleep(Duration::from_secs(1));
        let now = running(task);
        (!now.is_empty() && now.is_disjoint(&killed)).then_some(())
    });
}

/// The task's logs, once they hold every line of `expected`.
fn task_logs(platform: &Platform, name: &str, task: &str, expected: &[&str]) -> Output {
    run::eventually(&format!("the logs of {task}"), DERIVED, || {
        std::thread::sleep(Duration::from_secs(2));
        let logs = flowctl_status(platform, name, &["logs", "--task", task]);
        (logs.status.success() && expected.iter().all(|line| logs.stdout.contains(line)))
            .then_some(logs)
    })
}

/// Deletion is asynchronous. Prune can claim a record once its shard stops.
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
    );
    let launches = launched(&launcher::calls(&root));
    run::eventually("prune to release the deleted tasks", RELEASED, || {
        let output = launcher::prune(&root.podman, &root.state_dir)
            .output()
            .expect("running prune");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        launches
            .iter()
            .all(|name| launcher::left(&root.state_dir, &root.tmp_dir, name).is_empty())
            .then_some(())
    });
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

/// A capture project, discovered through the control plane, then published.
/// A session whose VMM is killed is restarted, and resumes from the cursor
/// its predecessor checkpointed, as does the session of a later publication:
/// each captures only what follows the cursor.
#[test]
fn a_python_capture_through_the_platform() {
    let platform = load();
    let root = root(&platform);
    expect_connector_init(&platform);
    let project = platform.run_dir.join("capture");
    copy_tree(&run::fixture("platform/capture"), &project);
    let source = project.join("flow.yaml");
    let source = source.to_string_lossy();

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
    let discover = flowctl(
        &platform,
        "capture-discover",
        &[
            "discover",
            "--source",
            &discovered.join("flow.yaml").to_string_lossy(),
            "--capture",
            CAPTURE,
        ],
    );
    assert_no_local_launch("discover", &discover);
    let calls = launcher::calls(&root)[before..].to_vec();
    assert_eq!(
        launched(&calls).len(),
        1,
        "the reactor discovered in one VMM: {calls:#?}"
    );
    assert!(
        calls.iter().any(|call| call.starts_with("create ")
            && call.contains(&format!("--label=task-name={CAPTURE}"))),
        "the reactor's discovery launch names its capture: {calls:#?}"
    );

    let publish = flowctl(
        &platform,
        "capture-publish",
        &["catalog", "publish", "--source", &source, "--auto-approve"],
    );
    assert_no_local_launch("catalog publish", &publish);
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

    kill_and_restart(CAPTURE);
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

/// A materialization project into an API. Its publication materializes live
/// greetings with standard and delta updates, and a session whose VMM is
/// killed is restarted and replays into the idempotent API, so that every
/// greeting arrives in both tables.
#[test]
fn a_python_materialization_through_the_platform() {
    let platform = load();
    expect_connector_init(&platform);

    // The project imports the live greetings from its parent directory.
    for file in ["greetings.flow.yaml", "hello-world.flow.yaml"] {
        std::fs::copy(
            run::fixture(&format!("platform/{file}")),
            platform.run_dir.join(file),
        )
        .unwrap();
    }
    let api = Api::start(&platform);
    let project = platform.run_dir.join("materialize");
    copy_tree(&run::fixture("platform/materialize"), &project);
    render(&project.join("flow.yaml"), "API_ADDRESS", &api.address);

    let publish = flowctl(
        &platform,
        "materialize-publish",
        &[
            "catalog",
            "publish",
            "--source",
            &project.join("flow.yaml").to_string_lossy(),
            "--auto-approve",
        ],
    );
    assert_no_local_launch("catalog publish", &publish);
    let egress = format!(
        "VMM egress permits only these host names: pypi.org, files.pythonhosted.org \
         (task egress.hosts); and any address within {}/32 (task egress.cidrs)",
        api.address
    );
    assert!(
        publish.stdout.contains(&egress),
        "{}",
        tail(&publish.stdout, 40)
    );

    let both = |at_least: usize| {
        run::eventually("greetings in both tables", DERIVED, || {
            std::thread::sleep(Duration::from_secs(2));
            let (standard, delta) = (api.greetings("greetings"), api.greetings("greetings_log"));
            (standard.len() >= at_least && delta.len() >= at_least).then_some(standard.len())
        })
    };
    let first = both(4);
    kill_and_restart(MATERIALIZATION);
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
