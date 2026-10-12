//! C13: synthetic `task-update.json` generations renamed into a running
//! guest's connector mount, by the runtime's stage-then-rename protocol, while
//! the guest opens the file afresh and keeps one descriptor from its first
//! open. Every fresh read must be one complete published generation, byte for
//! byte; the held descriptor must keep its generation; fresh reads must reach
//! the newest within the existing 30s bound. Then the same rotation is cut by
//! an orderly teardown or by killing the VMM, and the rotator must end, its
//! mount must stay gone, and a fresh guest must read the newest generation.
//!
//! The rotator is the test's own stand-in for the runtime's refresh task
//! (`connector::protocol::refresh_task_update`), which this does not run.
//! Each test appends to `c13.evidence` in the run directory, then fails
//! naming every violated bound.

use super::c07::append;
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const SEED: u64 = 0xC13;
const GENERATIONS: u32 = 100;
/// The fault tests' rotation, long enough to outlast virtiofs revalidation:
/// the healthy runs' fresh opens kept their first generation for about 5s,
/// longer than 100 generations take, so a fault there could not land while
/// the guest's reads were following the rotation.
const FAULT_GENERATIONS: u32 = 400;
/// The existing bound on a rename reaching a fresh open, and on every loop.
const BOUND: Duration = Duration::from_secs(30);
/// How long a rotator may take to end once stopped or once its mount is gone.
const JOINED: Duration = Duration::from_secs(5);
const MAX_FILE: usize = 64 * 1024;
/// What must be under way before a fault: generations published, and the
/// guest's fresh reads and the newest index they have seen.
const FAULT_PUBLISHED: usize = 20;
const FAULT_READS: u64 = 50;
const FAULT_INDEX: u64 = 10;

fn path() -> String {
    format!("{}/task-update.json", run::GUEST_CONNECTOR_MOUNT)
}

fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

/// zlib's crc32, which the guest checks with Python's stdlib.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
        }
    }
    !crc
}

/// Generation `n`: its index, a synthetic token, and a deterministic payload
/// of 512 B to 16 KiB carrying its crc32, so lengths straddle pages and a torn
/// read fails to parse or check even before the host matches its bytes.
fn generation(n: u32) -> String {
    let mut state = SEED ^ (u64::from(n) << 16);
    let len = 512 + (lcg(&mut state) % (16 * 1024 - 512)) as usize;
    let payload: String = format!("acmeCo-c13-{n}-")
        .chars()
        .cycle()
        .take(len)
        .collect();
    let doc = serde_json::json!({
        "index": n,
        "token": format!("acmeCo-c13-{SEED:x}-generation-{n}"),
        "payload": payload,
        "checksum": crc32(payload.as_bytes()),
        "control_plane_url": "https://control.acmeco.example",
        "config_encryption_url": "https://encryption.acmeco.example",
    })
    .to_string();
    assert!(
        doc.len() <= MAX_FILE,
        "generation {n} is {} bytes",
        doc.len()
    );
    doc
}

/// Each whole generation's length and crc32, by index, for the guest to
/// check its reads against when it cannot hand them back.
fn expected(last: u32) -> serde_json::Value {
    (0..=last)
        .map(|n| {
            let content = generation(n);
            (
                n.to_string(),
                serde_json::json!([content.len(), crc32(content.as_bytes())]),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into()
}

/// Stage and rename in one root shell, as `run::replace_task_update` does,
/// but failing rather than panicking, never creating a directory (a
/// torn-down mount refuses the write), and within 4s, so a stopped rotator
/// is joined within `JOINED`.
fn publish(mount: &str, content: &str) -> Result<(), String> {
    let output = run::sudo_output(
        &[
            "timeout",
            "-k",
            "1",
            "3",
            "sh",
            "-c",
            "cat > \"$1/.task-update.json.tmp\" && chmod 0444 \"$1/.task-update.json.tmp\" \
             && mv -f \"$1/.task-update.json.tmp\" \"$1/task-update.json\"",
            "sh",
            mount,
        ],
        Some(content.as_bytes()),
    );
    if output.status.success() {
        return Ok(());
    }
    Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
}

fn exists(path: &str) -> bool {
    run::sudo_output(&["test", "-e", path], None)
        .status
        .success()
}

/// The guest's `rotation` milestones, from the VMM's captured stderr.
fn milestones(stderr: &Mutex<run::Captured>) -> Vec<serde_json::Value> {
    let bytes = stderr.lock().unwrap().bytes.clone();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| serde_json::from_str(&line[line.find("{\"c13\":")?..]).ok())
        .collect()
}

/// The test's own rotator: generations in order, 20 to 50 ms apart, once the
/// guest reports `gate`, until stopped, done, `BOUND` passes or a write fails.
/// Dropping it stops and joins it, so a failing test leaves no writer behind.
struct Rotator {
    stop: Arc<AtomicBool>,
    published: Arc<Mutex<Vec<(u32, Instant)>>>,
    handle: Option<std::thread::JoinHandle<Option<String>>>,
}

impl Rotator {
    fn spawn(vmm: &run::Vmm, generations: std::ops::RangeInclusive<u32>, gate: &str) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let published = Arc::new(Mutex::new(Vec::new()));
        let (mount, stderr, gate) = (
            vmm.connector_mount.clone(),
            vmm.stderr.clone(),
            gate.to_string(),
        );
        let (stop_, published_) = (stop.clone(), published.clone());
        let handle = std::thread::spawn(move || {
            let started = Instant::now();
            while !milestones(&stderr)
                .iter()
                .any(|m| m["c13"] == gate.as_str())
            {
                if stop_.load(Ordering::SeqCst) || started.elapsed() > BOUND {
                    return Some(format!("the guest never reported {gate}"));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let mut state = SEED;
            for n in generations {
                if stop_.load(Ordering::SeqCst) {
                    return None;
                }
                if started.elapsed() > BOUND {
                    return Some(format!("{BOUND:?} passed before generation {n}"));
                }
                if let Err(error) = publish(&mount, &generation(n)) {
                    return Some(format!("generation {n}: {error}"));
                }
                published_.lock().unwrap().push((n, Instant::now()));
                std::thread::sleep(Duration::from_millis(20 + lcg(&mut state) % 31));
            }
            None
        });
        Rotator {
            stop,
            published,
            handle: Some(handle),
        }
    }

    fn published(&self) -> Vec<(u32, Instant)> {
        self.published.lock().unwrap().clone()
    }

    /// Whether it ended within `timeout` without being stopped.
    fn ended_within(&self, timeout: Duration) -> Option<Duration> {
        let started = Instant::now();
        let handle = self.handle.as_ref().expect("not yet joined");
        loop {
            if handle.is_finished() {
                return Some(started.elapsed());
            }
            if started.elapsed() >= timeout {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Stop and join within `JOINED`: what ended it, and how long that took.
    /// One that overruns stays owned, for `Drop` to stop and wait on again.
    fn finish(&mut self) -> (Option<String>, Duration) {
        let started = Instant::now();
        self.stop.store(true, Ordering::SeqCst);
        let finished = || self.handle.as_ref().expect("joined once").is_finished();
        while !finished() && started.elapsed() < JOINED {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(finished(), "the rotator outlived {JOINED:?} once stopped");
        let handle = self.handle.take().expect("joined once");
        let ended = handle.join().expect("the rotator does not panic");
        (ended, started.elapsed())
    }
}

impl Drop for Rotator {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        self.stop.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + JOINED;
        while !handle.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if handle.is_finished() {
            let _ = handle.join();
        } else {
            eprintln!("c13: a rotator outlived {JOINED:?} once stopped");
        }
    }
}

struct Evidence {
    path: std::path::PathBuf,
    test: &'static str,
    violations: Vec<String>,
}

impl Evidence {
    fn new(run: &run::Run, test: &'static str) -> Self {
        Evidence {
            path: run.run_dir.join("c13.evidence"),
            test,
            violations: Vec::new(),
        }
    }

    fn record(&self, what: &str, value: serde_json::Value) {
        append(
            &self.path,
            serde_json::json!({"test": self.test, "what": what, "value": value}),
        );
    }

    fn check(&mut self, ok: bool, violation: String) {
        if !ok {
            self.record("violation", violation.as_str().into());
            self.violations.push(violation);
        }
    }

    fn conclude(self) {
        assert!(
            self.violations.is_empty(),
            "{}: {:#?}",
            self.test,
            self.violations
        );
    }
}

/// Every fresh read one published generation, byte for byte; the held
/// descriptor its first generation throughout; `until` reached.
fn check_reads(
    evidence: &mut Evidence,
    result: &serde_json::Value,
    initial: Option<u32>,
    published: &[(u32, Instant)],
    until: u32,
) {
    let known: std::collections::BTreeMap<String, u32> = initial
        .into_iter()
        .chain(published.iter().map(|(n, _)| *n))
        .map(|n| (generation(n), n))
        .collect();
    let empty = serde_json::Map::new();
    let fresh = result["fresh"].as_object().unwrap_or(&empty);
    let mut seen = Vec::new();
    for (content, entry) in fresh {
        match known.get(content) {
            Some(n) => seen.push((*n, entry["count"].as_u64().unwrap_or(0))),
            None => evidence.check(
                false,
                format!(
                    "a fresh read matched no published generation: {} bytes, {:?}...",
                    content.len(),
                    content.chars().take(80).collect::<String>()
                ),
            ),
        }
    }
    seen.sort();
    let held_initial = result["held_initial"].as_str().unwrap_or("");
    let held = result["held"].as_object().unwrap_or(&empty);
    let held_generation = known.get(held_initial).copied();
    evidence.record(
        "reads",
        serde_json::json!({
            "reads": result["reads"],
            "distinct": seen.len(),
            "generations_seen": seen.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            "bad": result["bad"],
            "unknown": result["unknown"],
            "held_mismatch": result["held_mismatch"],
            "errors": result["errors"],
            "late_missing": result["late_missing"],
            "regressions": result["regressions"],
            "final_ms": result["final_ms"],
            "ms": result["ms"],
            "held_generation": held_generation,
            "held_reads": held.values().filter_map(|v| v.as_u64()).sum::<u64>(),
            "held_distinct": held.len(),
        }),
    );

    evidence.check(
        result["bad"] == 0,
        format!("{} fresh reads failed to parse or check", result["bad"]),
    );
    evidence.check(
        result["unknown"] == 0,
        format!(
            "{} fresh reads were not a whole generation",
            result["unknown"]
        ),
    );
    evidence.check(
        result["held_mismatch"] == 0,
        format!(
            "{} held reads differed from the held descriptor's first",
            result["held_mismatch"]
        ),
    );
    evidence.check(
        result["late_missing"] == 0,
        format!(
            "{} fresh opens failed after one succeeded",
            result["late_missing"]
        ),
    );
    let errors = result["errors"].as_object().unwrap_or(&empty);
    evidence.check(
        if initial.is_some() {
            errors.is_empty()
        } else {
            errors.keys().all(|k| k == "ENOENT")
        },
        format!("fresh opens failed: {errors:?}"),
    );
    evidence.check(
        !result["final_ms"].is_null() && seen.iter().any(|(n, _)| *n == until),
        format!("no fresh read saw generation {until} within {BOUND:?}"),
    );
    evidence.check(
        held_generation.is_some()
            && held.len() == 1
            && held.contains_key(held_initial),
        format!(
            "the held descriptor opened generation {held_generation:?} and then read {} distinct contents",
            held.len()
        ),
    );
    if let Some(initial) = initial {
        evidence.check(
            held_generation == Some(initial),
            format!("the held descriptor opened {held_generation:?}, not generation {initial}"),
        );
    }
}

/// Wait until rotation and the guest's reads are both well under way.
fn under_way(vmm: &run::Vmm, rotator: &Rotator) -> (usize, serde_json::Value) {
    run::eventually("rotation and guest reads under way", BOUND, || {
        let published = rotator.published.lock().unwrap().len();
        let last = milestones(&vmm.stderr)
            .into_iter()
            .rfind(|m| m["c13"] == "progress")?;
        (published >= FAULT_PUBLISHED
            && last["reads"].as_u64()? >= FAULT_READS
            && last["max_index"].as_u64()? >= FAULT_INDEX)
            .then_some((published, last))
    })
}

/// After teardown: the mount is gone and a late write cannot bring it back.
fn check_mount_gone(evidence: &mut Evidence, mount: &str) {
    let gone = !exists(mount);
    let late = publish(mount, &generation(GENERATIONS + 1));
    evidence.record(
        "teardown",
        serde_json::json!({"mount_gone": gone, "late_write": format!("{late:?}"), "after_late_write": exists(mount)}),
    );
    evidence.check(gone, format!("{mount} outlived its teardown"));
    evidence.check(
        late.is_err(),
        format!("a late write into {mount} succeeded"),
    );
    evidence.check(!exists(mount), format!("a late write recreated {mount}"));
}

/// A fresh guest, handed the newest generation, reads exactly that.
fn check_recovery(evidence: &mut Evidence, run: &run::Run, old_mount: &str, newest: u32) {
    let content = generation(newest);
    let spec = run::Spec {
        task_update: Some(&content),
        ..run::Spec::probes(run, "none.json")
    };
    let (vmm, mut guest) = start(run, spec);
    let read = probe(
        &mut guest,
        "c13-recovered",
        "read",
        serde_json::json!({"path": path()}),
    );
    evidence.record(
        "recovery",
        serde_json::json!({"vmm": vmm.name, "newest": newest, "matches": read["content"] == content.as_str()}),
    );
    evidence.check(
        read["content"] == content.as_str(),
        format!("a fresh guest did not read generation {newest}"),
    );
    evidence.check(
        vmm.connector_mount != old_mount,
        "the fresh guest reused the old mount".to_string(),
    );
}

fn start_reader(run: &run::Run, first: &str) -> (run::Vmm, guest::Guest) {
    let spec = run::Spec {
        task_update: Some(first),
        ..run::Spec::probes(run, "none.json")
    };
    let (vmm, mut guest) = start(run, spec);
    let child = probe(
        &mut guest,
        "c13-reader",
        "rotation",
        serde_json::json!({
            "path": path(),
            "until": FAULT_GENERATIONS,
            "seconds": BOUND.as_secs(),
            "expected": expected(FAULT_GENERATIONS),
            "background": true,
        }),
    );
    assert!(child.is_u64(), "the background reader: {child}");
    (vmm, guest)
}

/// A background reader's cumulative counts: every fresh read a whole
/// published generation, by length and crc32 against `expected`, and every
/// held read the generation-0 content the descriptor first read.
fn check_intact(evidence: &mut Evidence, when: &str, progress: &serde_json::Value) {
    evidence.check(
        progress["bad"] == 0
            && progress["unknown"] == 0
            && progress["late_missing"] == 0
            && progress["held_index"] == 0
            && progress["held_mismatch"] == 0,
        format!("the guest's reads {when}: {progress}"),
    );
}

fn record_fault(evidence: &mut Evidence, published: usize, progress: &serde_json::Value) {
    evidence.record(
        "fault",
        serde_json::json!({"published": published, "progress": progress}),
    );
    evidence.check(
        published < FAULT_GENERATIONS as usize,
        format!("rotation had finished ({published} published) before the fault"),
    );
    check_intact(evidence, "before the fault", progress);
}

/// The reader's last report, which a fault cut off.
fn last_report(evidence: &mut Evidence, stderr: &Mutex<run::Captured>) {
    let last = milestones(stderr)
        .into_iter()
        .rfind(|m| m["c13"] == "progress")
        .unwrap_or_default();
    evidence.record("last_report", last.clone());
    check_intact(evidence, "up to the fault", &last);
}

#[test]
fn b0_initial_and_one_replacement() {
    let run = run::load();
    let mut evidence = Evidence::new(&run, "b0");
    let first = generation(0);
    let spec = run::Spec {
        task_update: Some(&first),
        ..run::Spec::probes(&run, "none.json")
    };
    let (vmm, mut guest) = start(&run, spec);
    let mut rotator = Rotator::spawn(&vmm, 1..=1, "held");
    let result = probe(
        &mut guest,
        "c13-b0",
        "rotation",
        serde_json::json!({"path": path(), "until": 1, "seconds": BOUND.as_secs(), "expected": expected(1)}),
    );
    let (ended, joined) = rotator.finish();
    let published = rotator.published();
    evidence.record(
        "rotator",
        serde_json::json!({"published": published.len(), "ended": ended, "joined_ms": joined.as_millis()}),
    );
    evidence.check(
        ended.is_none() && published.len() == 1,
        format!(
            "the rotator published {} and ended with {ended:?}",
            published.len()
        ),
    );
    check_reads(&mut evidence, &result, Some(0), &published, 1);
    std::mem::drop((guest, vmm));
    evidence.conclude();
}

#[test]
fn rotate_hundred_generations() {
    let run = run::load();
    let mut evidence = Evidence::new(&run, "rotate");
    // Missing at start: the first generation arrives once the guest is reading.
    let (vmm, mut guest) = start(&run, run::Spec::probes(&run, "none.json"));
    let mut rotator = Rotator::spawn(&vmm, 1..=GENERATIONS, "started");
    let result = probe(
        &mut guest,
        "c13-rotate",
        "rotation",
        serde_json::json!({
            "path": path(),
            "until": GENERATIONS,
            "seconds": BOUND.as_secs(),
            "expected": expected(GENERATIONS),
        }),
    );
    let (ended, joined) = rotator.finish();
    let published = rotator.published();
    let gaps: Vec<u128> = published
        .windows(2)
        .map(|w| (w[1].1 - w[0].1).as_millis())
        .collect();
    evidence.record(
        "rotator",
        serde_json::json!({
            "published": published.len(),
            "ended": ended,
            "joined_ms": joined.as_millis(),
            "span_ms": published.last().zip(published.first()).map(|(l, f)| (l.1 - f.1).as_millis()),
            "gap_min_ms": gaps.iter().min(),
            "gap_max_ms": gaps.iter().max(),
        }),
    );
    evidence.check(
        ended.is_none() && published.len() == GENERATIONS as usize,
        format!(
            "the rotator published {} and ended with {ended:?}",
            published.len()
        ),
    );
    check_reads(&mut evidence, &result, None, &published, GENERATIONS);
    std::mem::drop((guest, vmm));
    evidence.conclude();
}

#[test]
fn cancel_mid_rotation() {
    let run = run::load();
    let mut evidence = Evidence::new(&run, "cancel");
    let (vmm, guest) = start_reader(&run, &generation(0));
    let mut rotator = Rotator::spawn(&vmm, 1..=FAULT_GENERATIONS, "held");
    let (published, progress) = under_way(&vmm, &rotator);
    record_fault(&mut evidence, published, &progress);
    let stderr = vmm.stderr.clone();

    // The teardown a cancelled session's owner performs, with the rotator
    // still writing: container, network, then state and mount.
    let mount = vmm.connector_mount.clone();
    let faulted = Instant::now();
    std::mem::drop((guest, vmm));
    let torn_down = faulted.elapsed();
    let ended = rotator.ended_within(JOINED);
    let (error, _) = rotator.finish();
    let after = rotator.published();
    evidence.record(
        "rotator",
        serde_json::json!({
            "teardown_ms": torn_down.as_millis(),
            "ended_after_teardown_ms": ended.map(|d| d.as_millis()),
            "error": error,
            "published": after.len(),
            "published_during_teardown": after.iter().filter(|(_, at)| *at > faulted).count(),
        }),
    );
    evidence.check(
        ended.is_some() && error.is_some(),
        format!("the rotator did not end on its own write failure within {JOINED:?}: {error:?}"),
    );
    last_report(&mut evidence, &stderr);
    check_mount_gone(&mut evidence, &mount);
    let newest = after.last().map_or(0, |(n, _)| *n);
    check_recovery(&mut evidence, &run, &mount, newest);
    evidence.conclude();
}

#[test]
fn crash_mid_rotation() {
    let run = run::load();
    let mut evidence = Evidence::new(&run, "crash");
    let (mut vmm, guest) = start_reader(&run, &generation(0));
    let pid = run::pid(&vmm);
    let mut rotator = Rotator::spawn(&vmm, 1..=FAULT_GENERATIONS, "held");
    let (published, progress) = under_way(&vmm, &rotator);
    record_fault(&mut evidence, published, &progress);

    let faulted = Instant::now();
    run::podman(&["kill", "-s", "KILL", &vmm.name]);
    let status = run::wait_exit(&mut vmm, BOUND);
    let exited = faulted.elapsed();
    let reports = milestones(&vmm.stderr).len();
    std::thread::sleep(Duration::from_millis(500));
    let reports_later = milestones(&vmm.stderr).len();
    // The owner is still alive, so its mount is too: the rotator writes on
    // into it until the owner stops it, ahead of the mount's teardown.
    let running = rotator.ended_within(Duration::ZERO).is_none();
    let (error, joined) = rotator.finish();
    let after = rotator.published();
    evidence.record(
        "fault",
        serde_json::json!({
            "pid": pid,
            "pid_gone": !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "status": format!("{status:?}"),
            "exited_ms": exited.as_millis(),
            "milestones_at_exit": reports,
            "milestones_500ms_later": reports_later,
        }),
    );
    evidence.record(
        "rotator",
        serde_json::json!({
            "running_after_crash": running,
            "error": error,
            "joined_ms": joined.as_millis(),
            "published": after.len(),
            "published_after_crash": after.iter().filter(|(_, at)| *at > faulted).count(),
            "published_at_fault": published,
        }),
    );
    evidence.check(
        reports == reports_later,
        "the guest reported after its VMM exited".to_string(),
    );
    let published_after_crash = after.iter().filter(|(_, at)| *at > faulted).count();
    evidence.check(
        running && published_after_crash > 0,
        format!(
            "the rotator was not still writing across the crash \
             (running {running}, {published_after_crash} published after it)"
        ),
    );
    evidence.check(
        error.is_none(),
        format!("the rotator failed rather than being stopped: {error:?}"),
    );
    last_report(&mut evidence, &vmm.stderr);
    let mount = vmm.connector_mount.clone();
    evidence.check(
        exists(&mount),
        format!("{mount} vanished before its teardown"),
    );
    std::mem::drop((guest, vmm));
    check_mount_gone(&mut evidence, &mount);
    let newest = after.last().map_or(0, |(n, _)| *n);
    check_recovery(&mut evidence, &run, &mount, newest);
    evidence.conclude();
}
