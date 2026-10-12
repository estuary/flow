//! C06: complete final data across VMM and reactor crashes. A finite input of
//! two hello-world batches, each greetings `0..BATCH`, numbered `m` (batch B
//! offset by `BATCH`), is totalled per key by a VMM Python derivation and
//! materialized into the stack's Postgres. Every variant compares the
//! committed derived documents and the materialized rows to an oracle computed
//! from the exported input, not to another run.

use super::*;

const BATCH: i64 = 100;
const KEYS_PER_BATCH: i64 = 10;
/// The `m` whose read the gated variants hold, before its transaction flushes.
const GATE: i64 = 60;
/// A gate whose release file never arrives lets go after this long.
const HOLD_SECS: u64 = 150;
const BASELINE: Duration = Duration::from_secs(480);
const RECOVERY: Duration = Duration::from_secs(300);

const MODULE: &str = r#""""C06: per-key totals of numbered greetings, optionally holding one read."""

import asyncio
import os
import sys
import time
from collections.abc import AsyncIterator

from acmeCo.PACKAGE.totals import IDerivation, Document, Request

BATCH = 100
KEYS_PER_BATCH = 10
B_OFFSET = B_KEYS
GATE = GATE_M
HOLD = HOLD_SECS.0
RELEASE = os.path.join(os.environ.get("CONNECTOR_MOUNT", "/nonexistent"), "c06-release")


class Derivation(IDerivation):
    async def from_greetings(
        self, read: Request.ReadFromGreetings
    ) -> AsyncIterator[Document]:
        batch, _, rest = read.doc.message.partition(" ")
        n = int(rest.removesuffix("!"))
        if n < BATCH:
            yield await self.total(n if batch == "A" else BATCH + n)

    async def total(self, m: int) -> Document:
        if m == GATE:
            print(f"C06-GATE holding m={m}", file=sys.stderr, flush=True)
            deadline = time.monotonic() + HOLD
            while time.monotonic() < deadline and not os.path.exists(RELEASE):
                await asyncio.sleep(0.5)
            how = "file" if os.path.exists(RELEASE) else "timeout"
            print(f"C06-GATE released m={m} by {how}", file=sys.stderr, flush=True)
        key = m % KEYS_PER_BATCH + B_OFFSET * (m // BATCH)
        return Document(key=key, count=1, total=m, last=m, value=f"v{m}")
"#;

/// One key's expected or observed row: count, total, last and value.
type Row = (i64, i64, i64, String);

/// `probe`'s first value within `timeout`, or None: the caller then asserts
/// what it last saw, so that a miss reports the difference.
fn eventually_or_none<T>(timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(value) = probe() {
            return Some(value);
        }
    }
    None
}

fn write(dir: &Path, file: &str, content: impl AsRef<str>) {
    let path = dir.join(file);
    std::fs::write(&path, content.as_ref())
        .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
}

/// The variant's catalog beneath `acmeCo/<prefix>/`, with the derivation
/// holding the read of `gate` (or nothing), and keying batch B's `m` from
/// `b_offset`: `KEYS_PER_BATCH` beside batch A's keys, or 0 onto them.
fn stage_c06(platform: &Platform, prefix: &str, gate: Option<i64>, b_offset: i64) -> Staged {
    let dir = platform.run_dir.join(prefix);
    _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("creating {}: {e}", dir.display()));
    let staged = Staged {
        dir,
        prefix: prefix.to_string(),
    };
    let greetings = serde_json::json!({
        "type": "object",
        "properties": {
            "ts": {"type": "string", "format": "date-time"},
            "message": {"type": "string"},
        },
        "required": ["ts", "message"],
    });
    write(
        &staged.dir,
        "greetings.flow.yaml",
        serde_json::json!({"collections": {
            staged.name("greetings"): {"schema": greetings, "key": ["/ts"]},
        }})
        .to_string(),
    );
    // `delete`'s recovering launch is the stock numbers derivation's Spec.
    for file in ["numbers.flow.yaml", "numbers.py"] {
        let fixture = run::fixture("platform").join(file);
        let content = std::fs::read_to_string(&fixture)
            .unwrap_or_else(|e| panic!("reading {}: {e}", fixture.display()))
            .replace("vmm-python", prefix)
            .replace("vmm_python", &staged.underscored());
        write(&staged.dir, file, content);
    }
    for batch in ["a", "b"] {
        for disable in [false, true] {
            let mut capture = serde_json::json!({
                "endpoint": {"connector": {
                    "image": "ghcr.io/estuary/source-hello-world:dev",
                    "config": {"rate": 50},
                }},
                "bindings": [{
                    "resource": {"name": "greetings", "prefix": format!("{} {{}}!", batch.to_uppercase())},
                    "target": staged.name("greetings"),
                }],
            });
            if disable {
                capture["shards"] = serde_json::json!({"disable": true});
            }
            write(
                &staged.dir,
                &format!(
                    "capture-{batch}{}.flow.yaml",
                    if disable { "-off" } else { "" }
                ),
                serde_json::json!({
                    "import": ["greetings.flow.yaml"],
                    "captures": {staged.name(&format!("hello-{batch}")): capture},
                })
                .to_string(),
            );
        }
    }
    let reduced = |strategy: &str, kind: &str| serde_json::json!({"type": kind, "reduce": {"strategy": strategy}});
    write(
        &staged.dir,
        "totals.flow.yaml",
        serde_json::json!({
            "import": ["greetings.flow.yaml"],
            "collections": {staged.name("totals"): {
                "schema": {
                    "type": "object",
                    "reduce": {"strategy": "merge"},
                    "properties": {
                        "key": {"type": "integer"},
                        "count": reduced("sum", "integer"),
                        "total": reduced("sum", "integer"),
                        "last": reduced("maximize", "integer"),
                        "value": reduced("lastWriteWins", "string"),
                    },
                    "required": ["key", "count", "total", "last", "value"],
                },
                "key": ["/key"],
                "derive": {
                    "using": {"python": {"module": "totals.py"}},
                    "vmm": true,
                    "egress": {"hosts": []},
                    "shards": {"flags": {"enable-runtime-v2": "true"}},
                    // One transform: derive-python's generated main.py fails its own
                    // strict type check with two (C06-B0 run1).
                    "transforms": [
                        {"name": "fromGreetings", "source": {"name": staged.name("greetings")}, "shuffle": "any"},
                    ],
                },
            }},
        })
        .to_string(),
    );
    write(
        &staged.dir,
        "totals.py",
        MODULE
            .replace("PACKAGE", &staged.underscored())
            .replace("B_KEYS", &b_offset.to_string())
            .replace("GATE_M", &gate.unwrap_or(-1).to_string())
            .replace("HOLD_SECS", &HOLD_SECS.to_string()),
    );
    write(
        &staged.dir,
        "postgres.flow.yaml",
        serde_json::json!({
            "import": ["totals.flow.yaml"],
            "materializations": {staged.name("postgres"): {
                "endpoint": {"connector": {
                    "image": "ghcr.io/estuary/materialize-postgres:dev",
                    "config": {
                        "address": "db:5432",
                        "database": "postgres",
                        "user": "postgres",
                        "schema": staged.underscored(),
                        "credentials": {"auth_type": "UserPassword", "password": "postgres"},
                    },
                }},
                "shards": {"flags": {"enable-runtime-v2": "true"}},
                "bindings": [{"source": staged.name("totals"), "resource": {"table": "totals"}}],
            }},
        })
        .to_string(),
    );
    for (file, imports) in [
        (
            "all-ab.flow.yaml",
            &[
                "capture-a.flow.yaml",
                "capture-b.flow.yaml",
                "postgres.flow.yaml",
            ][..],
        ),
        (
            "all-a.flow.yaml",
            &["capture-a.flow.yaml", "postgres.flow.yaml"][..],
        ),
    ] {
        write(
            &staged.dir,
            file,
            serde_json::json!({"import": imports}).to_string(),
        );
    }
    staged
}

fn must_publish(platform: &Platform, staged: &Staged, file: &str, label: &str) {
    let published = publish(platform, &root(platform), staged, file, label);
    assert!(
        published.status.success(),
        "publishing {file}: {}",
        tail(&published.stderr, 60)
    );
}

/// The committed documents of `collection`, as a read-committed reader sees
/// them, kept as `<label>.stdout`.
fn committed(platform: &Platform, collection: &str, label: &str) -> Option<Vec<serde_json::Value>> {
    let read = flowctl_status(
        platform,
        label,
        &["collections", "read", "--collection", collection],
        &[],
    );
    read.status.success().then(|| documents(&read.stdout))
}

/// The greetings `n < BATCH` of a batch ("a" captures "A {n}!"), by `n`,
/// with how many committed documents carry each.
fn batch_input(docs: &[serde_json::Value], batch: &str) -> BTreeMap<i64, usize> {
    let prefix = format!("{} ", batch.to_uppercase());
    let mut input = BTreeMap::new();
    for doc in docs {
        let message = doc["message"].as_str().expect("a greeting has a message");
        if !message.starts_with(&prefix) {
            continue;
        }
        let n: i64 = message
            .strip_prefix(&prefix)
            .and_then(|m| m.strip_suffix('!'))
            .and_then(|m| m.parse().ok())
            .unwrap_or_else(|| panic!("an unnumbered greeting {doc}"));
        if n < BATCH {
            *input.entry(n).or_default() += 1;
        }
    }
    input
}

/// Await every greeting `0..BATCH` of a batch, then disable its capture, so
/// that no later crash restarts it, and export its final committed input.
fn complete_batch(
    platform: &Platform,
    staged: &Staged,
    batch: &str,
    evidence: &mut Evidence,
) -> BTreeMap<i64, usize> {
    let collection = staged.name("greetings");
    let label = format!("{}-input-{batch}", staged.prefix);
    run::eventually(
        &format!("{collection} to hold 0..{BATCH}"),
        BASELINE,
        || {
            std::thread::sleep(Duration::from_secs(2));
            let docs = committed(platform, &collection, &label)?;
            (batch_input(&docs, batch).len() as i64 == BATCH).then_some(())
        },
    );
    must_publish(
        platform,
        staged,
        &format!("capture-{batch}-off.flow.yaml"),
        &format!("{}-disable-{batch}", staged.prefix),
    );
    // A disabled capture may still commit what it had read; settle until two
    // reads a while apart agree on this batch's greetings. The other batch's
    // capture may still be appending to the same collection.
    let prefix = format!("{} ", batch.to_uppercase());
    let mut last: Option<usize> = None;
    let docs = run::eventually(
        &format!("{collection}'s batch {batch} to stop growing"),
        RECOVERY,
        || {
            std::thread::sleep(Duration::from_secs(5));
            let docs = committed(platform, &collection, &label)?;
            let ours = docs
                .iter()
                .filter(|doc| {
                    doc["message"]
                        .as_str()
                        .is_some_and(|m| m.starts_with(&prefix))
                })
                .count();
            let settled = last == Some(ours);
            last = Some(ours);
            settled.then_some(docs)
        },
    );
    let input = batch_input(&docs, batch);
    let exact = input.len() as i64 == BATCH && input.values().all(|c| *c == 1);
    let path = platform.run_dir.join(format!("{label}.final.json"));
    std::fs::write(&path, serde_json::to_string(&docs).unwrap())
        .unwrap_or_else(|e| panic!("writing {}: {e}", path.display()));
    evidence.push(format!(
        "input {batch}: {} committed greetings, {} distinct n < {BATCH}, each exactly once: {exact}; exported {}",
        docs.len(),
        input.len(),
        path.display()
    ));
    let off: Vec<_> = (0..BATCH)
        .filter(|n| input.get(n) != Some(&1))
        .map(|n| (n, input.get(&n).copied().unwrap_or(0)))
        .collect();
    assert!(
        exact,
        "batch {batch}: each n < {BATCH} must be committed exactly once; (n, copies) otherwise: {off:?}"
    );
    input
}

/// The oracle, from exported input alone: each `m` counted as often as its
/// greeting was committed, keyed as the module keys it with `b_offset`.
fn oracle(a: &BTreeMap<i64, usize>, b: &BTreeMap<i64, usize>, b_offset: i64) -> BTreeMap<i64, Row> {
    let mut rows: BTreeMap<i64, Row> = BTreeMap::new();
    let all = a
        .iter()
        .map(|(n, c)| (*n, *c))
        .chain(b.iter().map(|(n, c)| (BATCH + n, *c)));
    for (m, copies) in all {
        let key = m % KEYS_PER_BATCH + b_offset * (m / BATCH);
        let row = rows.entry(key).or_insert((0, 0, i64::MIN, String::new()));
        row.0 += copies as i64;
        row.1 += m * copies as i64;
        if m > row.2 {
            row.2 = m;
            row.3 = format!("v{m}");
        }
    }
    rows
}

/// The committed derived documents, reduced as their reduction annotations
/// say: summed counts and totals, the greatest last, the last value.
fn reduce(docs: &[serde_json::Value]) -> BTreeMap<i64, Row> {
    let mut rows: BTreeMap<i64, Row> = BTreeMap::new();
    for doc in docs {
        let int = |field: &str| {
            doc[field]
                .as_i64()
                .unwrap_or_else(|| panic!("{field} of {doc}"))
        };
        let row = rows
            .entry(int("key"))
            .or_insert((0, 0, i64::MIN, String::new()));
        row.0 += int("count");
        row.1 += int("total");
        row.2 = row.2.max(int("last"));
        row.3 = doc["value"].as_str().expect("a value").to_string();
    }
    rows
}

fn table(schema: &str) -> Option<BTreeMap<i64, Row>> {
    if sql(&format!(
        "SELECT to_regclass('{schema}.totals') IS NOT NULL"
    )) != "t"
    {
        return None;
    }
    let out = sql(&format!(
        "SELECT \"key\", \"count\", \"total\", \"last\", \"value\" FROM {schema}.totals ORDER BY \"key\""
    ));
    Some(
        out.lines()
            .map(|line| {
                let f: Vec<&str> = line.split('|').collect();
                let int = |i: usize| {
                    f[i].parse::<i64>()
                        .unwrap_or_else(|e| panic!("{line}: {e}"))
                };
                (int(0), (int(1), int(2), int(3), f[4].to_string()))
            })
            .collect(),
    )
}

/// The derived collection's committed documents, then the materialized table
/// after a forced sync, must equal `expected`, and still do a while later.
fn assert_final(
    platform: &Platform,
    staged: &Staged,
    expected: &BTreeMap<i64, Row>,
    evidence: &mut Evidence,
) {
    let totals = staged.name("totals");
    let label = format!("{}-totals", staged.prefix);
    let mut seen = BTreeMap::new();
    let reached = eventually_or_none(RECOVERY, || {
        std::thread::sleep(Duration::from_secs(2));
        seen = reduce(&committed(platform, &totals, &label)?);
        (seen.values().map(|r| r.0).sum::<i64>() >= expected.values().map(|r| r.0).sum::<i64>())
            .then_some(())
    });
    evidence.push(format!(
        "derived collection committed (reduced) reached the oracle's count: {}; equal to oracle: {}",
        reached.is_some(),
        seen == *expected
    ));
    assert_eq!(
        seen, *expected,
        "the derived collection's committed documents"
    );

    let schema = staged.underscored();
    flowctl(
        platform,
        &format!("{}-sync-now", staged.prefix),
        &["raw", "sync-now", "--task", &staged.name("postgres")],
        &[],
    );
    let rows = eventually_or_none(RECOVERY, || {
        std::thread::sleep(Duration::from_secs(2));
        table(&schema).filter(|rows| rows == expected)
    });
    let rows = rows.or_else(|| table(&schema)).unwrap_or_default();
    write(
        &platform.run_dir,
        &format!("{}-table.json", staged.prefix),
        serde_json::to_string(&rows).unwrap(),
    );
    assert_eq!(rows, *expected, "the materialized rows");

    // Quiescent: no later commit changes either.
    std::thread::sleep(Duration::from_secs(10));
    flowctl(
        platform,
        &format!("{}-sync-now-2", staged.prefix),
        &["raw", "sync-now", "--task", &staged.name("postgres")],
        &[],
    );
    let again = reduce(
        &committed(platform, &totals, &format!("{label}-again")).expect("re-reading totals"),
    );
    let rows_again = table(&schema).expect("the table");
    write(
        &platform.run_dir,
        &format!("{}-table-again.json", staged.prefix),
        serde_json::to_string(&rows_again).unwrap(),
    );
    evidence.push(format!(
        "final: derived committed == oracle: {}; table == oracle: {}; 10s and a second sync-now later both unchanged: {}; {} keys, {} events",
        seen == *expected,
        rows == *expected,
        again == *expected && rows_again == *expected,
        expected.len(),
        expected.values().map(|r| r.0).sum::<i64>()
    ));
    assert_eq!(again, *expected);
    assert_eq!(rows_again, *expected);
    write(
        &platform.run_dir,
        &format!("{}-oracle.json", staged.prefix),
        serde_json::to_string(expected).unwrap(),
    );
}

/// Deletes the variant's tasks, collections and table, and releases its
/// launches.
fn cleanup(platform: &Platform, staged: &Staged, before: usize, evidence: &mut Evidence) {
    let shared = root(platform);
    delete(platform, staged, &staged.prefix);
    sql(&format!(
        "DROP SCHEMA IF EXISTS {} CASCADE",
        staged.underscored()
    ));
    let names = named(&launcher::calls(&shared)[before..]);
    assert_released(&names, &[&shared]);
    evidence.push(format!(
        "cleanup: deleted acmeCo/{}/, dropped its schema, every launch released: {names:?}",
        staged.prefix
    ));
}

/// Waits for the gate's log line in the shard's logs, then the committed
/// state then: m = GATE's effect must not be committed.
fn await_gate(
    platform: &Platform,
    staged: &Staged,
    label: &str,
    evidence: &mut Evidence,
) -> (String, Vec<String>, String) {
    let totals = staged.name("totals");
    let logs = task_logs_holding(
        platform,
        &totals,
        label,
        &[&format!("C06-GATE holding m={GATE}")],
    );
    let holding = gate_ts(&logs, "C06-GATE holding");
    let known = shard_vmms(platform, &totals, &format!("{label}-vmms"));
    let vmm = known.last().expect("the shard started a VMM").clone();
    let docs = committed(platform, &totals, &format!("{label}-committed")).expect("reading totals");
    let reduced = reduce(&docs);
    let gate_key = GATE % KEYS_PER_BATCH;
    let committed_count: i64 = reduced.values().map(|r| r.0).sum();
    let gate_committed = reduced.get(&gate_key).is_some_and(|r| r.2 >= GATE);
    evidence.push(format!(
        "gate: {vmm} holds the read of m={GATE}; committed then: {committed_count} events over {} docs, \
         key {gate_key}'s committed last {:?}, so m={GATE} committed: {gate_committed}",
        docs.len(),
        reduced.get(&gate_key).map(|r| r.2)
    ));
    assert!(
        !gate_committed,
        "m={GATE} is committed while its read is held"
    );
    (vmm, known, holding)
}

/// The `ts` of the first task log line holding `marker`.
fn gate_ts(logs: &str, marker: &str) -> String {
    let line = logs
        .lines()
        .find(|line| line.contains(marker))
        .unwrap_or_else(|| panic!("no log line holds {marker}"));
    let log: serde_json::Value =
        serde_json::from_str(line).unwrap_or_else(|e| panic!("parsing {line:?}: {e}"));
    log["ts"]
        .as_str()
        .unwrap_or_else(|| panic!("no ts in {line}"))
        .to_string()
}

/// Immediately before a kill: the gate still holds. No released line has been
/// logged, and the hold began well within its timeout; only the test's file
/// in a successor's mount releases it otherwise.
fn assert_gate_held(
    platform: &Platform,
    staged: &Staged,
    holding: &str,
    label: &str,
    evidence: &mut Evidence,
) {
    let logs = flowctl_status(
        platform,
        label,
        &["logs", "--task", &staged.name("totals")],
        &[],
    );
    assert!(logs.status.success(), "reading the task's logs");
    let released = logs
        .stdout
        .lines()
        .filter(|line| line.contains("C06-GATE released"))
        .count();
    let age: f64 = sql(&format!(
        "SELECT extract(epoch FROM now() - '{holding}'::timestamptz)"
    ))
    .parse()
    .expect("an age in seconds");
    evidence.push(format!(
        "gate held at kill: holding logged at {holding}, {age:.1}s ago (timeout {HOLD_SECS}s, margin required 30s); \
         released lines logged: {released}"
    ));
    assert_eq!(released, 0, "the gate was released before the kill");
    assert!(
        age < (HOLD_SECS - 30) as f64,
        "the gate may have timed out: {age}s"
    );
}

/// The successor VMM, and its release: a file in its connector mount.
fn release_successor(
    platform: &Platform,
    staged: &Staged,
    dead: &str,
    known: &[String],
    evidence: &mut Evidence,
) -> String {
    let totals = staged.name("totals");
    let shared = root(platform);
    let started = Instant::now();
    let successor = run::eventually("the shard's successor VMM", RECOVERY, || {
        std::thread::sleep(Duration::from_secs(2));
        shard_vmms(platform, &totals, &format!("{}-vmms-after", staged.prefix))
            .into_iter()
            .find(|name| !known.contains(name))
    });
    let fp = launcher::footprint(&shared.state_dir, &shared.tmp_dir, &successor);
    let mount = fp.paths[2].clone();
    std::fs::write(format!("{mount}/c06-release"), "release\n")
        .unwrap_or_else(|e| panic!("writing {mount}/c06-release: {e}"));
    evidence.push(format!(
        "successor {successor} of {dead} (container {}, processes {:?}) seen {:?} after the kill; released through {mount}/c06-release",
        fp.id,
        fp.processes,
        started.elapsed()
    ));
    successor
}

#[test]
fn c06_1_b0_healthy_baseline() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "c06-b0");
    let staged = stage_c06(&platform, "c06-b0", None, KEYS_PER_BATCH);
    let before = launcher::calls(&root(&platform)).len();
    let started = Instant::now();
    must_publish(&platform, &staged, "all-ab.flow.yaml", "c06-b0-publish");
    let a = complete_batch(&platform, &staged, "a", &mut evidence);
    let b = complete_batch(&platform, &staged, "b", &mut evidence);
    let expected = oracle(&a, &b, KEYS_PER_BATCH);
    assert_final(&platform, &staged, &expected, &mut evidence);
    evidence.push(format!(
        "baseline complete {:?} after publication",
        started.elapsed()
    ));
    cleanup(&platform, &staged, before, &mut evidence);
}

#[test]
fn c06_fault_2_v1_vmm_killed_uncommitted() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "c06-v1");
    let staged = stage_c06(&platform, "c06-v1", Some(GATE), KEYS_PER_BATCH);
    let shared = root(&platform);
    let before = launcher::calls(&shared).len();
    must_publish(&platform, &staged, "all-ab.flow.yaml", "c06-v1-publish");
    let a = complete_batch(&platform, &staged, "a", &mut evidence);
    let b = complete_batch(&platform, &staged, "b", &mut evidence);
    let expected = oracle(&a, &b, KEYS_PER_BATCH);

    let (dead, known, holding) = await_gate(&platform, &staged, "c06-v1-gate", &mut evidence);
    let fp = launcher::footprint(&shared.state_dir, &shared.tmp_dir, &dead);
    let (what, pid, start) = fp.processes[0].clone();
    assert_eq!(what, "VMM");
    assert_eq!(
        launcher::start_time(pid).as_ref(),
        Some(&start),
        "the VMM is still the one inspected"
    );
    assert_gate_held(&platform, &staged, &holding, "c06-v1-held", &mut evidence);
    let killed = std::process::Command::new("sudo")
        .args(["-n", "kill", "-KILL", &pid.to_string()])
        .status()
        .expect("running sudo kill");
    assert!(killed.success(), "sudo kill -KILL {pid}");
    let at = Instant::now();
    let killed_at = sql("SELECT now()::text");
    run::eventually("the killed VMM to die", RELEASED, || {
        (launcher::start_time(pid).as_ref() != Some(&start)).then_some(())
    });
    evidence.push(format!(
        "SIGKILLed {dead}'s VMM pid {pid} (start {start}), container {} at {killed_at}; its process gone {:?} later",
        fp.id,
        at.elapsed()
    ));
    let successor = release_successor(&platform, &staged, &dead, &known, &mut evidence);
    assert_ne!(successor, dead);
    assert_final(&platform, &staged, &expected, &mut evidence);
    cleanup(&platform, &staged, before, &mut evidence);
}

#[test]
fn c06_fault_3_r1_reactor_killed_in_flight() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "c06-r1");
    let staged = stage_c06(&platform, "c06-r1", Some(GATE), KEYS_PER_BATCH);
    let shared = root(&platform);
    let before = launcher::calls(&shared).len();
    must_publish(&platform, &staged, "all-ab.flow.yaml", "c06-r1-publish");
    let a = complete_batch(&platform, &staged, "a", &mut evidence);
    let b = complete_batch(&platform, &staged, "b", &mut evidence);
    let expected = oracle(&a, &b, KEYS_PER_BATCH);

    let (dead, known, holding) = await_gate(&platform, &staged, "c06-r1-gate", &mut evidence);
    assert_gate_held(&platform, &staged, &holding, "c06-r1-held", &mut evidence);
    let main = kill_reactor(&platform);
    let killed_at = sql("SELECT now()::text");
    let restarted = reactor_restarted(&platform, &main);
    assert_ne!(restarted, main);
    evidence.push(format!(
        "reactor {main:?} killed (dead seen by {killed_at}) while {dead} held m={GATE}; restarted as {restarted:?}"
    ));
    release_successor(&platform, &staged, &dead, &known, &mut evidence);
    assert_final(&platform, &staged, &expected, &mut evidence);
    cleanup(&platform, &staged, before, &mut evidence);
}

#[test]
fn c06_fault_4_r2_reactor_killed_between_batches() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "c06-r2");
    let staged = stage_c06(&platform, "c06-r2", None, KEYS_PER_BATCH);
    let before = launcher::calls(&root(&platform)).len();
    must_publish(&platform, &staged, "all-a.flow.yaml", "c06-r2-publish");
    let a = complete_batch(&platform, &staged, "a", &mut evidence);
    let only_a = oracle(&a, &BTreeMap::new(), KEYS_PER_BATCH);

    // Batch A's outputs, visible to a read-committed reader, are acknowledged,
    // which follows the commit of the checkpoint that read them.
    let totals = staged.name("totals");
    let checkpoint = run::eventually("batch A's committed totals", RECOVERY, || {
        std::thread::sleep(Duration::from_secs(2));
        let reduced = reduce(&committed(&platform, &totals, "c06-r2-checkpoint")?);
        (reduced == only_a).then_some(reduced)
    });
    let shards = flowctl_status(
        &platform,
        "c06-r2-shards",
        &["raw", "list-shards", "--task", &totals],
        &[],
    );
    evidence.push(format!(
        "checkpoint: batch A's {} events committed in the derived collection, equal to its oracle: {}; shards listed in c06-r2-shards.stdout (exit {})",
        checkpoint.values().map(|r| r.0).sum::<i64>(),
        checkpoint == only_a,
        shards.status
    ));
    let main = kill_reactor(&platform);
    let killed_at = sql("SELECT now()::text");
    let restarted = reactor_restarted(&platform, &main);
    assert_ne!(restarted, main);
    evidence.push(format!(
        "reactor {main:?} killed (dead seen by {killed_at}) after batch A's commit, before batch B; restarted as {restarted:?}"
    ));

    must_publish(
        &platform,
        &staged,
        "capture-b.flow.yaml",
        "c06-r2-publish-b",
    );
    let b = complete_batch(&platform, &staged, "b", &mut evidence);
    let expected = oracle(&a, &b, KEYS_PER_BATCH);
    assert_final(&platform, &staged, &expected, &mut evidence);
    cleanup(&platform, &staged, before, &mut evidence);
}

/// C06U: batch B's `m` key onto batch A's keys, so that B updates A's rows.
const SAME_KEYS: i64 = 0;

/// The same-key oracle in closed form, after batch A alone or after both:
/// key `k` gets `m = k, 10 + k, ..` of each batch.
fn same_keys_closed(with_b: bool) -> BTreeMap<i64, Row> {
    (0..KEYS_PER_BATCH)
        .map(|k| match with_b {
            false => (k, (10, 450 + 10 * k, 90 + k, format!("v{}", 90 + k))),
            true => (k, (20, 1900 + 20 * k, 190 + k, format!("v{}", 190 + k))),
        })
        .collect()
}

/// The materialized table's OID and file node, which recreating or truncating
/// it would change, and each row's xmin, which updating it changes.
fn table_identity(schema: &str) -> (String, String) {
    let class = sql(&format!(
        "SELECT c.oid || '/' || c.relfilenode FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = '{schema}' AND c.relname = 'totals'"
    ));
    let xmins = sql(&format!(
        "SELECT string_agg(\"key\" || ':' || xmin::text, ' ' ORDER BY \"key\") FROM {schema}.totals"
    ));
    (class, xmins)
}

/// Batch A's gate: its committed derived documents, then its materialized
/// rows after a forced sync, each equal to `only_a`, kept as
/// `<label>-derived.json` and `<label>-table.json`. Returns the database's
/// time once both were seen, and the table's identity then.
fn assert_a_materialized(
    platform: &Platform,
    staged: &Staged,
    only_a: &BTreeMap<i64, Row>,
    label: &str,
    evidence: &mut Evidence,
) -> (String, (String, String)) {
    let totals = staged.name("totals");
    let mut seen = BTreeMap::new();
    eventually_or_none(RECOVERY, || {
        std::thread::sleep(Duration::from_secs(2));
        seen = reduce(&committed(platform, &totals, &format!("{label}-derived"))?);
        (seen == *only_a).then_some(())
    });
    write(
        &platform.run_dir,
        &format!("{label}-derived.json"),
        serde_json::to_string(&seen).unwrap(),
    );
    assert_eq!(seen, *only_a, "batch A's committed derived documents");
    let committed_at = sql("SELECT now()::text");

    let schema = staged.underscored();
    flowctl(
        platform,
        &format!("{label}-sync-now"),
        &["raw", "sync-now", "--task", &staged.name("postgres")],
        &[],
    );
    let rows = eventually_or_none(RECOVERY, || {
        std::thread::sleep(Duration::from_secs(2));
        table(&schema).filter(|rows| rows == only_a)
    });
    let rows = rows.or_else(|| table(&schema)).unwrap_or_default();
    write(
        &platform.run_dir,
        &format!("{label}-table.json"),
        serde_json::to_string(&rows).unwrap(),
    );
    assert_eq!(rows, *only_a, "batch A's materialized rows");
    let at = sql("SELECT now()::text");
    let identity = table_identity(&schema);
    evidence.push(format!(
        "{label}: batch A's derived committed == oracle(A) by {committed_at}; {schema}.totals == oracle(A) \
         after sync-now by {at}; table oid/relfilenode {}; row xmins {}",
        identity.0, identity.1
    ));
    (at, identity)
}

/// The shard's VMMs as its logs report them, and the latest's container and
/// VMM process, which must be a real VMM's. Returns the VMMs and that
/// process's PID and start time.
fn vmm_identity(
    platform: &Platform,
    staged: &Staged,
    label: &str,
    evidence: &mut Evidence,
) -> (Vec<String>, u32, String) {
    let shared = root(platform);
    let vmms = shard_vmms(platform, &staged.name("totals"), label);
    let vmm = vmms.last().expect("the shard started a VMM");
    let fp = launcher::footprint(&shared.state_dir, &shared.tmp_dir, vmm);
    evidence.push(format!(
        "{label}: shard VMMs {vmms:?}; {vmm} is container {} with processes {:?}",
        fp.id, fp.processes
    ));
    let (what, pid, start) = fp.processes[0].clone();
    assert_eq!(what, "VMM");
    (vmms, pid, start)
}

/// A batch's exported input: its committed greetings, those retained
/// (`n < BATCH`; the capture overshoots and the derivation drops the rest),
/// and the earliest `ts` of any, which must follow `after`.
fn assert_input_after(
    platform: &Platform,
    staged: &Staged,
    batch: &str,
    after: &str,
    evidence: &mut Evidence,
) {
    let path = platform
        .run_dir
        .join(format!("{}-input-{batch}.final.json", staged.prefix));
    let docs: Vec<serde_json::Value> = serde_json::from_slice(
        &std::fs::read(&path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display())),
    )
    .unwrap_or_else(|e| panic!("parsing {}: {e}", path.display()));
    let prefix = format!("{} ", batch.to_uppercase());
    let ts: Vec<&str> = docs
        .iter()
        .filter(|doc| {
            doc["message"]
                .as_str()
                .is_some_and(|m| m.starts_with(&prefix))
        })
        .map(|doc| doc["ts"].as_str().expect("a greeting has a ts"))
        .collect();
    let retained: usize = batch_input(&docs, batch).values().sum();
    let earliest = sql(&format!(
        "SELECT min(t)::text FROM unnest('{{{}}}'::timestamptz[]) t",
        ts.join(",")
    ));
    let follows = sql(&format!(
        "SELECT '{earliest}'::timestamptz > '{after}'::timestamptz"
    )) == "t";
    evidence.push(format!(
        "input {batch}: {} committed greetings of the batch, {retained} retained (n < {BATCH}), {} overshooting \
         n >= {BATCH} dropped by the derivation; earliest ts {earliest}, after {after}: {follows}",
        ts.len(),
        ts.len() - retained
    ));
    assert!(
        follows,
        "batch {batch}'s input began at {earliest}, not after {after}"
    );
}

#[test]
fn c06_updates_1_b0_same_keys_baseline() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "c06u-b0");
    let staged = stage_c06(&platform, "c06u-b0", None, SAME_KEYS);
    let before = launcher::calls(&root(&platform)).len();
    let started = Instant::now();
    evidence.push(format!(
        "images: VMM {}, derivation {}",
        platform.vmm_image, platform.derive_python_image
    ));
    must_publish(&platform, &staged, "all-a.flow.yaml", "c06u-b0-publish");
    let a = complete_batch(&platform, &staged, "a", &mut evidence);
    let only_a = oracle(&a, &BTreeMap::new(), SAME_KEYS);
    assert_eq!(only_a, same_keys_closed(false), "batch A's oracle");
    let (a_at, identity) =
        assert_a_materialized(&platform, &staged, &only_a, "c06u-b0-a", &mut evidence);
    vmm_identity(&platform, &staged, "c06u-b0-vmms-a", &mut evidence);

    must_publish(
        &platform,
        &staged,
        "capture-b.flow.yaml",
        "c06u-b0-publish-b",
    );
    let b = complete_batch(&platform, &staged, "b", &mut evidence);
    assert_input_after(&platform, &staged, "b", &a_at, &mut evidence);
    let expected = oracle(&a, &b, SAME_KEYS);
    assert_eq!(expected, same_keys_closed(true), "batch A and B's oracle");
    assert_final(&platform, &staged, &expected, &mut evidence);

    let after = table_identity(&staged.underscored());
    evidence.push(format!(
        "table continuity: oid/relfilenode {} after batch A, {} after batch B; row xmins after batch B {}",
        identity.0, after.0, after.1
    ));
    assert_eq!(
        after.0, identity.0,
        "batch B updated the table batch A made"
    );
    vmm_identity(&platform, &staged, "c06u-b0-vmms-final", &mut evidence);
    evidence.push(format!(
        "baseline complete {:?} after publication",
        started.elapsed()
    ));
    cleanup(&platform, &staged, before, &mut evidence);
}

/// Checkpoint evidence of both tasks: the derivation's and materialization's
/// shards, and the rows of the materialization's checkpoints table, kept as
/// `<label>-shards-*.stdout` and `<label>-checkpoints.txt`.
fn checkpoints(platform: &Platform, staged: &Staged, label: &str) {
    for task in ["totals", "postgres"] {
        flowctl_status(
            platform,
            &format!("{label}-shards-{task}"),
            &["raw", "list-shards", "--task", &staged.name(task)],
            &[],
        );
    }
    let schema = staged.underscored();
    let rows = if sql(&format!(
        "SELECT to_regclass('{schema}.flow_checkpoints_v1') IS NOT NULL"
    )) == "t"
    {
        sql(&format!(
            "SELECT row_to_json(c)::text FROM {schema}.flow_checkpoints_v1 c"
        ))
    } else {
        format!("{schema}.flow_checkpoints_v1 is absent")
    };
    write(&platform.run_dir, &format!("{label}-checkpoints.txt"), rows);
}

#[test]
fn c06_updates_2_r1_reactor_killed_between_same_key_batches() {
    let platform = load();
    reactor_main(&platform);
    let mut evidence = Evidence::new(&platform, "c06u-r1");
    let staged = stage_c06(&platform, "c06u-r1", None, SAME_KEYS);
    let before = launcher::calls(&root(&platform)).len();
    let started = Instant::now();
    evidence.push(format!(
        "images: VMM {}, derivation {}",
        platform.vmm_image, platform.derive_python_image
    ));
    must_publish(&platform, &staged, "all-a.flow.yaml", "c06u-r1-publish");
    let a = complete_batch(&platform, &staged, "a", &mut evidence);
    let only_a = oracle(&a, &BTreeMap::new(), SAME_KEYS);
    assert_eq!(only_a, same_keys_closed(false), "batch A's oracle");
    let (a_at, identity) =
        assert_a_materialized(&platform, &staged, &only_a, "c06u-r1-a", &mut evidence);
    checkpoints(&platform, &staged, "c06u-r1-a");
    let (known, vmm_pid, vmm_start) =
        vmm_identity(&platform, &staged, "c06u-r1-vmms-a", &mut evidence);

    // `kill_reactor` signals the whole unit; each of its user's processes
    // seen before must be gone after.
    let processes = unit(&platform).processes;
    let issued_at = sql("SELECT now()::text");
    let at = Instant::now();
    let main = kill_reactor(&platform);
    let dead_at = sql("SELECT now()::text");
    let survivors: Vec<_> = processes
        .iter()
        .filter(|(pid, uid, _)| *uid != 0 && launcher::start_time(*pid).is_some())
        .collect();
    evidence.push(format!(
        "kill: SIGKILL issued to unit {} at {issued_at} after batch A's gate at {a_at}; main {main:?} dead by {dead_at}; \
         the unit's processes before {processes:?}; its user's still running after: {survivors:?}",
        platform.reactor_unit
    ));
    assert!(
        survivors.is_empty(),
        "the reactor's processes survived: {survivors:?}"
    );
    let restarted = reactor_restarted(&platform, &main);
    let restarted_at = sql("SELECT now()::text");
    evidence.push(format!(
        "restart: reactor main {restarted:?} by {restarted_at}, distinct from {main:?}: {}",
        restarted != main
    ));
    assert_ne!(restarted, main);

    // Before batch B: the shard relaunched its VMM, and batch A's committed
    // and materialized state is as it was, in the same table.
    let totals = staged.name("totals");
    let successor = run::eventually("the shard's successor VMM", RECOVERY, || {
        std::thread::sleep(Duration::from_secs(2));
        shard_vmms(&platform, &totals, "c06u-r1-vmms-after")
            .into_iter()
            .find(|name| !known.contains(name))
    });
    let (after, pid, start) =
        vmm_identity(&platform, &staged, "c06u-r1-vmms-restarted", &mut evidence);
    evidence.push(format!(
        "successor: {successor} not among {known:?}; the latest VMM {:?} pid {pid} (start {start}); \
         the pre-kill VMM pid {vmm_pid} (start {vmm_start}) still running: {}",
        after.last(),
        launcher::start_time(vmm_pid).as_ref() == Some(&vmm_start)
    ));
    assert_eq!(after.last(), Some(&successor));
    assert_ne!((pid, &start), (vmm_pid, &vmm_start));
    // sync-now fails outright, without retrying, on a shard not yet assigned.
    run::eventually(
        "the materialization to sync after the restart",
        RECOVERY,
        || {
            std::thread::sleep(Duration::from_secs(2));
            let synced = flowctl_status(
                &platform,
                "c06u-r1-sync-restarted",
                &["raw", "sync-now", "--task", &staged.name("postgres")],
                &[],
            );
            synced.status.success().then_some(())
        },
    );
    let (a_again_at, identity_again) = assert_a_materialized(
        &platform,
        &staged,
        &only_a,
        "c06u-r1-a-restarted",
        &mut evidence,
    );
    checkpoints(&platform, &staged, "c06u-r1-a-restarted");
    let recovery = at.elapsed();
    evidence.push(format!(
        "recovery: batch A unchanged after the restart by {a_again_at}, {recovery:?} after the kill; \
         table oid/relfilenode {} before the kill, {} after",
        identity.0, identity_again.0
    ));
    assert_eq!(identity_again.0, identity.0, "the table batch A made");
    assert!(recovery < RECOVERY, "recovery took {recovery:?}");

    must_publish(
        &platform,
        &staged,
        "capture-b.flow.yaml",
        "c06u-r1-publish-b",
    );
    let b = complete_batch(&platform, &staged, "b", &mut evidence);
    assert_input_after(&platform, &staged, "b", &a_again_at, &mut evidence);
    let expected = oracle(&a, &b, SAME_KEYS);
    assert_eq!(
        expected,
        same_keys_closed(true),
        "batch A and B's oracle, as the healthy baseline's"
    );
    assert_final(&platform, &staged, &expected, &mut evidence);

    let last = table_identity(&staged.underscored());
    evidence.push(format!(
        "table continuity: oid/relfilenode {} after batch A, {} after batch B; row xmins after batch A {}, after batch B {}",
        identity.0, last.0, identity_again.1, last.1
    ));
    assert_eq!(last.0, identity.0, "batch B updated the table batch A made");
    vmm_identity(&platform, &staged, "c06u-r1-vmms-final", &mut evidence);
    let elapsed = started.elapsed();
    evidence.push(format!("complete {elapsed:?} after publication"));
    assert!(elapsed < BASELINE, "the test took {elapsed:?}");
    cleanup(&platform, &staged, before, &mut evidence);
}
