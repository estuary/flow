//! C18: short-TTL refresh while the VMM is throttled.
//!
//! One VMM under `ttl.json` (floor 3s, cap 10s), in four phases:
//!
//! - `b0`: an allowed name answering a one-second TTL, promised three, is
//!   refreshed every `REFRESH_EVERY` while fresh connections run between the
//!   queries. It sets the connect-latency bound, and must hold for the fault
//!   to run.
//! - `pressure`: the same, after this VMM's own container is held to
//!   `QUOTA` of a CPU and one guest worker spins. `cpu.stat` must show
//!   throttling over the window.
//! - `expire`: refreshes stop with the pressure still on. Fresh connections
//!   sent after the last promise's latest expiry must fail, the address must
//!   leave the kernel's `@resolved`, and the held stream is only recorded:
//!   it is not fresh-connect evidence. Then the quota is restored and a new
//!   query and connection must succeed.
//! - `order`: a second name answers ten seconds, then one (clamped to
//!   three), then an answer mixed with the suite's decoy, which the baseline
//!   excludes. The shorter answer must not shorten the outstanding promise,
//!   the refused one must authorize nothing, and the decoy is never in
//!   `@resolved` nor reached.
//!
//! The resolver serves one query at a time and drops replies carrying another
//! transaction id, so answers to one VMM cannot be reordered, and the scripted
//! nameserver cannot delay an answer: `order` covers later shorter and
//! forbidden answers, not reordered in-flight ones.
//!
//! Every connection is new and has its own probe name. A promise is judged
//! from both ends: the kernel's timer started after the query was sent and
//! before the guest saw the answer, so an attempt finished before the
//! earliest possible expiry must connect, and one sent after the latest must
//! not. Attempts in between are recorded, never judged. The test writes
//! `c18-ttl_refresh_pressure.evidence` to the run directory as it goes, then
//! fails naming every violated bound; a baseline that does not hold fails at
//! once, as inconclusive.

use super::c07::{append, cgroup_keyed, host_load};
use super::c15::{
    Listener, Sample, accepted, accepts_json, baseline, host_exchange, listen_logged,
    parse_resolved, sample_every, sample_json,
};
use super::*;

const POLICY: &str = "ttl.json";
const SHORT: &str = "a.acmeco.example";
const LONG: &str = "b.acmeco.example";
const BAD: &str = "d.acmeco.example";
/// Clamped up to `FLOOR` by `ttlFloorSecs`.
const SCRIPTED_SHORT: u32 = 1;
const FLOOR: u32 = 3;
/// `ttlCapSecs`: unclamped.
const LONG_TTL: u32 = 10;
/// Half the promise, so every refresh lands well inside the one before it.
const REFRESH_EVERY: Duration = Duration::from_millis(1_500);
/// Between guest requests: at most ten a second.
const PACE: Duration = Duration::from_millis(100);
const B0: Duration = Duration::from_secs(12);
const PRESSURE: Duration = Duration::from_secs(25);
/// A quarter of one CPU, for this VMM's container only.
const QUOTA: &str = "25000 100000";
/// Past the pressure and expiry phases' worst case; killed at recovery.
const SPIN_SECONDS: u64 = 90;
/// A fresh connect's own timeout, and the hard bound on its latency.
const FRESH: f64 = 5.0;
const HARD_LATENCY: Duration = Duration::from_secs(5);
const MIN_LATENCY_BOUND: Duration = Duration::from_secs(2);
const QUERY_TIMEOUT: f64 = 2.0;
/// nft timeouts are kept in jiffies.
const EXPIRY_SLACK: Duration = Duration::from_millis(100);
const RETAIN_MARGIN: Duration = Duration::from_millis(50);
/// From the latest expiry to the element leaving the set's listing.
const COLLECTED: Duration = Duration::from_secs(2);
/// An accept dated this long after the latest expiry was not a late accept of
/// an earlier, admitted connection.
const ACCEPT_LAG: Duration = Duration::from_millis(500);
/// From the latest expiry, for two fresh attempts to fail.
const EXPIRE_CAP: Duration = Duration::from_secs(20);
/// Fully covered attempts a refreshed phase needs, and each of its refresh
/// windows, so that it cannot pass vacuously.
const MIN_COVERED: usize = 20;
const MIN_COVERED_PER_WINDOW: usize = 2;
/// The longest the sampler may go unread while expiry is being watched.
const SAMPLE_GAP: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Target {
    A,
    B,
    D,
}

/// One answer the guest asked for, and the authorization it can have given.
struct Answer {
    phase: &'static str,
    addr: Ipv4Addr,
    sent: Duration,
    done: Duration,
    /// What the kernel was asked to hold. `None` for a refusal.
    timeout: Option<Duration>,
    /// The guest saw the answer, so the kernel acknowledged it.
    acked: bool,
}

struct Attempt {
    phase: &'static str,
    probe: String,
    target: Target,
    sent: Duration,
    done: Duration,
    guest: Duration,
    /// The connect-latency budget declared for this attempt's phase.
    budget: Duration,
    result: String,
}

struct S {
    t0: Instant,
    last: Option<Instant>,
    evidence: std::path::PathBuf,
    guest: guest::Guest,
    pid: u32,
    addrs: [(Target, Ipv4Addr, u16); 3],
    answers: Vec<Answer>,
    attempts: Vec<Attempt>,
    violations: Vec<String>,
    inconclusive: Vec<String>,
    seq: usize,
    budget: Duration,
}

impl S {
    fn target(&self, target: Target) -> (Ipv4Addr, u16) {
        let (_, addr, port) = self
            .addrs
            .iter()
            .find(|(held, ..)| *held == target)
            .expect("every target has an address");
        (*addr, *port)
    }

    /// Paced, dated on the host, and written to the evidence.
    fn call(
        &mut self,
        phase: &str,
        name: &str,
        op: &str,
        arguments: serde_json::Value,
    ) -> (Duration, Duration, Duration, serde_json::Value) {
        if let Some(last) = self.last {
            std::thread::sleep((last + PACE).saturating_duration_since(Instant::now()));
        }
        let sent = Instant::now();
        self.last = Some(sent);
        let (result, took) = guest::call_timed(&mut self.guest, name, op, arguments.clone());
        let (sent, done) = (sent - self.t0, self.t0.elapsed());
        append(
            &self.evidence,
            serde_json::json!({
                "kind": "call", "phase": phase, "probe": name, "op": op,
                "arguments": arguments, "sent_ms": sent.as_millis() as u64,
                "done_ms": done.as_millis() as u64, "guest_ms": took.as_millis() as u64,
                "result": result,
            }),
        );
        (sent, done, took, result)
    }

    fn fresh(&mut self, phase: &'static str, target: Target, timeout: f64) -> String {
        let (addr, port) = self.target(target);
        self.seq += 1;
        let probe = format!("{phase}-fresh-{target:?}-{}", self.seq);
        let (sent, done, guest, result) = self.call(
            phase,
            &probe,
            "tcp",
            serde_json::json!({"addr": addr.to_string(), "port": port, "timeout": timeout}),
        );
        let result = result.as_str().unwrap_or("non-string").to_string();
        self.attempts.push(Attempt {
            phase,
            probe,
            target,
            sent,
            done,
            guest,
            budget: self.budget,
            result: result.clone(),
        });
        result
    }

    /// A query whose answer is expected to be `want` (records at the clamped
    /// TTL) or, with `None`, REFUSED. Unexpected answers are violations; any
    /// answer that is neither a clean success nor a refusal is recorded as
    /// one that may have authorized `addrs` for up to the cap.
    fn query(
        &mut self,
        phase: &'static str,
        qname: &str,
        addrs: &[Ipv4Addr],
        want: Option<u32>,
    ) -> (Duration, Duration, serde_json::Value) {
        self.seq += 1;
        let probe = format!("{phase}-query-{}", self.seq);
        let (sent, done, _, result) = self.call(
            phase,
            &probe,
            "query",
            serde_json::json!({"nameserver": VMM_TAP.to_string(), "name": qname, "qtype": 1, "timeout": QUERY_TIMEOUT}),
        );
        let status = result["status"].as_str().unwrap_or_default().to_string();
        let refused = status == "rcode:5";
        let ok = status == "ok";
        match want {
            Some(ttl) => {
                let expected: Vec<serde_json::Value> = addrs
                    .iter()
                    .map(|addr| serde_json::json!(["A", addr.to_string(), ttl]))
                    .collect();
                if !ok || result["records"] != serde_json::json!(expected) {
                    self.violations.push(format!(
                        "{probe}: {qname} answered {result}, expected {expected:?}"
                    ));
                }
            }
            None if !refused => self.violations.push(format!(
                "{probe}: {qname} answered {result}, expected REFUSED"
            )),
            None => {}
        }
        for addr in addrs {
            let timeout = match (ok, refused) {
                (true, _) => result["records"].as_array().and_then(|records| {
                    records
                        .iter()
                        .find(|record| record[1] == addr.to_string())
                        .and_then(|record| record[2].as_u64())
                        .map(Duration::from_secs)
                }),
                (false, true) => None,
                (false, false) => Some(Duration::from_secs(u64::from(LONG_TTL))),
            };
            self.answers.push(Answer {
                phase,
                addr: *addr,
                sent,
                // An answer the guest never saw may have been authorized as
                // late as the guest's wait.
                done: if ok || refused {
                    done
                } else {
                    sent + Duration::from_secs_f64(QUERY_TIMEOUT)
                },
                timeout,
                acked: ok,
            });
        }
        (sent, done, result)
    }

    fn exchange(&mut self, phase: &str) -> String {
        let (sent, _, _, reply) = self.call(
            phase,
            &format!("{phase}-held-{}", self.attempts.len()),
            "exchange",
            serde_json::json!({"id": "held", "timeout": 2}),
        );
        let reply = reply.as_str().unwrap_or("non-string").to_string();
        append(
            &self.evidence,
            serde_json::json!({"kind": "held", "phase": phase, "sent_ms": sent.as_millis() as u64, "reply": reply}),
        );
        reply
    }

    fn point_read(&mut self, phase: &str) -> Sample {
        let sample = read_set(self.pid, self.t0)
            .unwrap_or_else(|| panic!("{phase}: reading @resolved failed"));
        append(
            &self.evidence,
            serde_json::json!({"kind": "point-read", "phase": phase, "sample": sample_json(&sample)}),
        );
        sample
    }

    /// Run fresh connections to `target` until `until`, refreshing `qname`
    /// every `REFRESH_EVERY` if given.
    fn drive(
        &mut self,
        phase: &'static str,
        target: Target,
        until: Duration,
        refresh: Option<&str>,
    ) {
        let (addr, _) = self.target(target);
        // On the cadence of the last query for `addr`, so a phase neither
        // starts with a back-to-back refresh nor a gap.
        let mut next_refresh = self
            .answers
            .iter()
            .rev()
            .find(|answer| answer.addr == addr)
            .map_or(self.t0.elapsed(), |answer| answer.sent + REFRESH_EVERY);
        while self.t0.elapsed() < until {
            if let Some(qname) = refresh {
                if self.t0.elapsed() >= next_refresh {
                    next_refresh = next_refresh.max(self.t0.elapsed()) + REFRESH_EVERY;
                    self.query(phase, qname, &[addr], Some(FLOOR));
                    continue;
                }
            }
            self.fresh(phase, target, FRESH);
        }
    }
}

/// `@resolved`, read from the VMM's network namespace by a process in the
/// host's cgroup, so a throttled container does not delay the reading.
fn read_set(pid: u32, t0: Instant) -> Option<Sample> {
    let start = Instant::now();
    let pid = pid.to_string();
    let output = run::sudo_output(
        &[
            "nsenter",
            "-t",
            &pid,
            "-n",
            "nft",
            "list",
            "set",
            "inet",
            "flow_egress",
            "resolved",
        ],
        None,
    );
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&output.stdout).to_string();
    Some(Sample {
        at: start - t0,
        took: start.elapsed(),
        elements: parse_resolved(&raw),
        raw,
    })
}

/// The earliest a promise to `addr` can lapse, from answers the guest had seen
/// before `before`: the kernel never shortens an outstanding timeout.
fn earliest(answers: &[Answer], addr: Ipv4Addr, before: Duration) -> Option<Duration> {
    answers
        .iter()
        .filter(|answer| answer.addr == addr && answer.acked && answer.done <= before)
        .filter_map(|answer| Some(answer.sent + answer.timeout?))
        .max()
}

/// The latest anything sent before `before` can have kept `addr` authorized.
fn latest(answers: &[Answer], addr: Ipv4Addr, before: Duration) -> Option<Duration> {
    answers
        .iter()
        .filter(|answer| answer.addr == addr && answer.sent < before)
        .filter_map(|answer| Some(answer.done + answer.timeout? + EXPIRY_SLACK))
        .max()
}

/// Sent early enough that, connecting within its budget, it would finish
/// inside a promise already acknowledged: it must connect within the budget,
/// however late a failure returns.
fn covered(answers: &[Answer], attempt: &Attempt, addr: Ipv4Addr) -> bool {
    earliest(answers, addr, attempt.sent)
        .is_some_and(|by| attempt.sent + attempt.budget + RETAIN_MARGIN <= by)
}

#[derive(Default, serde::Serialize)]
struct Counts {
    /// Covered by the budget: must connect within it.
    covered: usize,
    /// Not covered, but finished inside the promise: must have connected.
    finished_inside: usize,
    /// Sent after every promise could have lapsed: must not connect.
    expired: usize,
    unjudged: usize,
}

/// Every attempt judged against the promises in force when it was made.
fn judge(s: &mut S) -> std::collections::BTreeMap<String, Counts> {
    let mut counts = std::collections::BTreeMap::<String, Counts>::new();
    let mut violations = Vec::new();
    for attempt in &s.attempts {
        let (addr, _) = s.target(attempt.target);
        let entry = counts.entry(attempt.phase.to_string()).or_default();
        let inside = earliest(&s.answers, addr, attempt.sent)
            .is_some_and(|by| attempt.done + RETAIN_MARGIN < by);
        let must_not = latest(&s.answers, addr, attempt.sent).is_none_or(|by| attempt.sent > by);
        if attempt.result == "connected" && attempt.guest > attempt.budget {
            violations.push(format!(
                "bound: {} connected in {:?}, over its budget {:?}",
                attempt.probe, attempt.guest, attempt.budget
            ));
        }
        if covered(&s.answers, attempt, addr) {
            entry.covered += 1;
            if attempt.result != "connected" {
                violations.push(failure(s, attempt, addr));
            }
        } else if inside {
            entry.finished_inside += 1;
            if attempt.result != "connected" {
                violations.push(failure(s, attempt, addr));
            }
        } else if must_not {
            entry.expired += 1;
            if attempt.result == "connected" {
                violations.push(format!(
                    "{}: connected at {:?} after every promise to {addr} could have lapsed",
                    attempt.probe, attempt.sent
                ));
            }
        } else {
            entry.unjudged += 1;
        }
    }
    s.violations.extend(violations);
    counts
}

/// Covered attempts between each acknowledged answer for `addr` in `phase`
/// and the next query, so no refresh window passes vacuously.
fn windows(s: &S, phase: &str, addr: Ipv4Addr) -> Vec<(u64, usize)> {
    let answers: Vec<&Answer> = s
        .answers
        .iter()
        .filter(|answer| answer.addr == addr && answer.acked && answer.phase == phase)
        .collect();
    answers
        .windows(2)
        .map(|pair| {
            let count = s
                .attempts
                .iter()
                .filter(|attempt| attempt.sent >= pair[0].done && attempt.sent < pair[1].sent)
                .filter(|attempt| s.target(attempt.target).0 == addr)
                .filter(|attempt| covered(&s.answers, attempt, addr))
                .count();
            (pair[0].done.as_millis() as u64, count)
        })
        .collect()
}

/// Each reading of `@resolved` against the promises to `addr`: present, with
/// a kernel expiry no earlier than the earliest promise acknowledged before
/// the reading and no later than the latest answer sent before it ended; and
/// never absent while a promise was certainly outstanding.
fn kernel_timeline(s: &mut S, samples: &[Sample], addr: Ipv4Addr, label: &str) -> usize {
    let mut judged = 0;
    for sample in samples {
        let end = sample.at + sample.took;
        let Some(owed) = earliest(&s.answers, addr, sample.at) else {
            continue;
        };
        let element = sample.elements.iter().find(|e| e.addr == addr);
        match element.and_then(|e| e.expires_ms) {
            None if owed > end + EXPIRY_SLACK => s.violations.push(format!(
                "kernel: {label} missing from @resolved at {:?}, promised until {owed:?}: {}",
                sample.at, sample.raw
            )),
            None => {}
            Some(ms) => {
                judged += 1;
                // The reading happened somewhere in [at, end].
                let remaining = Duration::from_millis(ms);
                let expiry = (sample.at + remaining, end + remaining);
                if expiry.1 + EXPIRY_SLACK < owed {
                    s.violations.push(format!(
                        "kernel: {label} expires by {:?} at {:?}, promised until {owed:?}: {}",
                        expiry.1, sample.at, sample.raw
                    ));
                }
                let ceiling = latest(&s.answers, addr, end).unwrap_or_default();
                if expiry.0 > ceiling + EXPIRY_SLACK {
                    s.violations.push(format!(
                        "kernel: {label} expires at {:?} at {:?}, past any answer's {ceiling:?}: {}",
                        expiry.0, sample.at, sample.raw
                    ));
                }
            }
        }
    }
    judged
}

/// A failed must-connect attempt, against the most recent delivered TTL.
fn failure(s: &S, attempt: &Attempt, addr: Ipv4Addr) -> String {
    let answer = s
        .answers
        .iter()
        .filter(|answer| answer.addr == addr && answer.acked && answer.done <= attempt.sent)
        .last();
    format!(
        "{}: answered {:?} in {:?} at {:?}..{:?}, inside the promise of the answer sent at {:?}, \
         acknowledged at {:?}, timeout {:?}",
        attempt.probe,
        attempt.result,
        attempt.guest,
        attempt.sent,
        attempt.done,
        answer.map(|answer| answer.sent),
        answer.map(|answer| answer.done),
        answer.and_then(|answer| answer.timeout),
    )
}

/// Restores this VMM's `cpu.max` on every path, unless its cgroup is already
/// gone with it.
struct Quota {
    path: String,
    original: String,
}

impl Quota {
    fn set(cgroup: &str, value: &str) -> Quota {
        let path = format!("{cgroup}/cpu.max");
        let original = run::sudo(&["cat", &path]).trim().to_string();
        let quota = Quota { path, original };
        assert!(quota.write(value).success(), "setting {}", quota.path);
        quota
    }

    /// `tee` rather than `run::sudo_write`, which would also chmod the
    /// cgroup's own file.
    fn write(&self, value: &str) -> std::process::ExitStatus {
        run::sudo_output(&["tee", &self.path], Some(value.as_bytes())).status
    }

    fn restore(&self) {
        if std::path::Path::new(&self.path).exists() {
            // Not asserted: this also runs while unwinding.
            let _ = self.write(&self.original);
        }
    }
}

impl Drop for Quota {
    fn drop(&mut self) {
        self.restore();
    }
}

fn cpu_stat(cgroup: &str) -> std::collections::BTreeMap<String, u64> {
    cgroup_keyed(cgroup, "cpu.stat")
}

/// Whether every busy worker `spin` started is still running in the guest. A
/// finished one stays a zombie, which `kill -0` would still find.
fn spinning(s: &mut S, phase: &str, spin: &serde_json::Value) -> bool {
    let checks: Vec<String> = spin
        .as_array()
        .into_iter()
        .flatten()
        .map(|pid| format!("grep -q '^State:[[:space:]]*R' /proc/{pid}/status"))
        .collect();
    if checks.is_empty() {
        return false;
    }
    let (_, _, _, result) = s.call(
        phase,
        &format!("{phase}-spinning"),
        "exec",
        serde_json::json!({"argv": ["/bin/sh", "-c", checks.join(" && ")]}),
    );
    result == "exit:0"
}

fn rate(attempts: &[Attempt], phase: &str, window: (Duration, Duration)) -> f64 {
    let count = attempts.iter().filter(|a| a.phase == phase).count();
    count as f64 / (window.1 - window.0).as_secs_f64()
}

fn accepts_after(listener: &Listener, from: Duration, to: Duration) -> Vec<serde_json::Value> {
    accepts_json(listener)
        .as_array()
        .expect("an array")
        .iter()
        .filter(|accept| {
            let at = Duration::from_millis(accept["at_ms"].as_u64().unwrap_or_default());
            at > from && at < to
        })
        .cloned()
        .collect()
}

#[test]
fn ttl_refresh_pressure() {
    let run = run::load();
    let [ns_ip, a_ip, b_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let d_ip = run.decoys[0];
    let t0 = Instant::now();
    let evidence = run.run_dir.join("c18-ttl_refresh_pressure.evidence");
    let _ = std::fs::remove_file(&evidence);

    let nameserver = scripted(&run, ns_ip);
    let la = listen_logged(&run, a_ip, t0);
    let lb = listen_logged(&run, b_ip, t0);
    let ld = listen_logged(&run, d_ip, t0);
    dns::set(
        &nameserver,
        SHORT,
        vec![dns::Record::A(a_ip, SCRIPTED_SHORT)],
    );
    dns::set(&nameserver, LONG, vec![dns::Record::A(b_ip, LONG_TTL)]);
    dns::set(&nameserver, BAD, vec![dns::Record::A(d_ip, LONG_TTL)]);

    let reply = host_exchange(ld.addr);
    baseline(
        reply == "pong",
        format!("the host's control connection to D answered {reply:?}"),
    );
    run::eventually("the control's bytes", Duration::from_secs(2), || {
        (accepted(&ld) == (1, 5)).then_some(())
    });

    let mut spec = run::Spec::probes(&run, POLICY);
    spec.flags = upstream(&nameserver);
    let (vmm, guest) = start(&run, spec);
    let pid = run::pid(&vmm);
    let cgroup = format!(
        "/sys/fs/cgroup{}",
        run::inspect(&vmm.name, "{{.State.CgroupPath}}")
    );
    append(
        &evidence,
        serde_json::json!({
            "kind": "setup", "vmm": vmm.name, "pid": pid, "cgroup": cgroup,
            "nameserver": nameserver.addr.to_string(), "A": la.addr.to_string(),
            "B": lb.addr.to_string(), "D": ld.addr.to_string(), "policy": POLICY,
            "cpu.max": run::sudo(&["cat", &format!("{cgroup}/cpu.max")]).trim(),
            "host": host_load(),
        }),
    );
    let sampler = sample_every(move || read_set(pid, t0));

    let mut s = S {
        t0,
        last: None,
        evidence: evidence.clone(),
        guest,
        pid,
        addrs: [
            (Target::A, a_ip, la.addr.port()),
            (Target::B, b_ip, lb.addr.port()),
            (Target::D, d_ip, ld.addr.port()),
        ],
        answers: Vec::new(),
        attempts: Vec::new(),
        violations: Vec::new(),
        inconclusive: Vec::new(),
        seq: 0,
        budget: MIN_LATENCY_BOUND,
    };

    // C18-B0.
    let b0_start = t0.elapsed();
    let (_, _, first) = s.query("b0", SHORT, &[a_ip], Some(FLOOR));
    baseline(first["status"] == "ok", format!("{SHORT} answered {first}"));
    let read = s.point_read("b0");
    baseline(
        read.elements
            .iter()
            .any(|e| e.addr == a_ip && e.timeout_ms == Some(u64::from(FLOOR) * 1000)),
        format!("@resolved after the first answer: {}", read.raw),
    );
    let (_, _, _, held) = s.call(
        "b0",
        "b0-hold",
        "hold",
        serde_json::json!({"id": "held", "addr": a_ip.to_string(), "port": la.addr.port(), "timeout": 2}),
    );
    baseline(held == "connected", format!("the held stream: {held}"));
    s.drive("b0", Target::A, b0_start + B0, Some(SHORT));
    let b0_end = t0.elapsed();
    let b0_counts = judge(&mut s);
    let b0_latency = s
        .attempts
        .iter()
        .filter(|a| a.phase == "b0" && a.result == "connected")
        .map(|a| a.guest)
        .max()
        .unwrap_or_default();
    let bound = (b0_latency * 10).max(MIN_LATENCY_BOUND).min(HARD_LATENCY);
    let b0_refreshes = s
        .answers
        .iter()
        .filter(|a| a.acked && a.sent >= b0_start)
        .count();
    let b0_fresh = s.attempts.iter().filter(|a| a.phase == "b0").count();
    let b0_windows = windows(&s, "b0", a_ip);
    let b0_covered = b0_counts.get("b0").map_or(0, |c| c.covered);
    append(
        &evidence,
        serde_json::json!({
            "kind": "b0", "window_ms": [b0_start.as_millis(), b0_end.as_millis()],
            "counts": b0_counts, "max_connect_ms": b0_latency.as_millis(),
            "latency_bound_ms": bound.as_millis(), "acked_answers": b0_refreshes,
            "fresh_attempts": b0_fresh, "windows": b0_windows, "violations": s.violations,
        }),
    );
    baseline(
        s.violations.is_empty()
            && b0_end - b0_start >= Duration::from_secs(10)
            && b0_fresh >= 50
            && b0_refreshes >= 5
            && b0_covered >= MIN_COVERED
            && b0_windows.iter().all(|(_, n)| *n >= MIN_COVERED_PER_WINDOW),
        format!(
            "C18-B0: {:?} over {:?}, {b0_fresh} fresh, {b0_covered} covered, {b0_refreshes} acknowledged, windows {b0_windows:?}",
            s.violations,
            b0_end - b0_start
        ),
    );

    // C18-PRESSURE.
    let quota = Quota::set(&cgroup, QUOTA);
    let set = run::sudo(&["cat", &format!("{cgroup}/cpu.max")]);
    baseline(
        set.trim() == QUOTA,
        format!("{cgroup}/cpu.max is {set:?} after setting {QUOTA}"),
    );
    s.budget = bound;
    let (_, _, _, spin) = s.call(
        "pressure",
        "pressure-spin",
        "spin",
        serde_json::json!({"workers": 1, "seconds": SPIN_SECONDS}),
    );
    let p_start = t0.elapsed();
    let before = cpu_stat(&cgroup);
    append(
        &evidence,
        serde_json::json!({"kind": "pressure-on", "at_ms": p_start.as_millis(),
                           "cpu.max": run::sudo(&["cat", &format!("{cgroup}/cpu.max")]).trim(),
                           "spin": spin, "cpu.stat": before, "host": host_load()}),
    );
    s.drive("pressure", Target::A, p_start + PRESSURE, Some(SHORT));
    let p_end = t0.elapsed();
    let after = cpu_stat(&cgroup);
    let spinning_pressure = spinning(&mut s, "pressure", &spin);
    let delta = |key: &str| after[key].saturating_sub(before[key]);
    let used = delta("usage_usec") as f64 / (p_end - p_start).as_micros() as f64;
    let p_rate = rate(&s.attempts, "pressure", (p_start, p_end));
    let p_refreshes = s
        .answers
        .iter()
        .filter(|a| a.acked && a.sent >= p_start && a.sent < p_end)
        .count();
    append(
        &evidence,
        serde_json::json!({"kind": "pressure-window", "window_ms": [p_start.as_millis(), p_end.as_millis()],
                           "cpu.stat.before": before, "cpu.stat.after": after, "cpus_used": used,
                           "fresh_per_sec": p_rate, "acked_answers": p_refreshes,
                           "spinning": spinning_pressure, "host": host_load()}),
    );
    if delta("nr_throttled") == 0 || delta("throttled_usec") == 0 {
        s.inconclusive
            .push(format!("pressure: no throttling {before:?} -> {after:?}"));
    }
    if p_refreshes < 5 {
        s.inconclusive.push(format!(
            "pressure: only {p_refreshes} acknowledged refreshes"
        ));
    }
    if !(5.0..=10.5).contains(&p_rate) {
        s.inconclusive
            .push(format!("pressure: {p_rate:.2} fresh connections a second"));
    }
    let p_window = p_end - p_start;
    if p_window < Duration::from_secs(20) || p_window > Duration::from_secs(60) {
        s.inconclusive
            .push(format!("pressure: held for {p_window:?}"));
    }
    if !spinning_pressure {
        s.inconclusive
            .push("pressure: the busy worker was not running at its end".into());
    }

    // C18-EXPIRE: no more refreshes, pressure still on.
    let stopped = t0.elapsed();
    let lapse = latest(&s.answers, a_ip, stopped).expect("A was answered");
    let mut failed_after = 0;
    let mut held_replies = Vec::new();
    while failed_after < 2 && t0.elapsed() < lapse + EXPIRE_CAP {
        let result = s.fresh("expire", Target::A, FRESH);
        let sent = s.attempts.last().expect("just attempted").sent;
        if sent > lapse && result != "connected" {
            failed_after += 1;
        }
        if s.attempts.len() % 5 == 0 || failed_after >= 2 {
            held_replies.push(s.exchange("expire"));
        }
    }
    if failed_after < 2 {
        s.inconclusive.push(format!(
            "expire: fewer than two failed attempts after {lapse:?}"
        ));
    }
    s.point_read("expire");
    let expire_end = t0.elapsed();
    let expire_stat = cpu_stat(&cgroup);
    let spinning_expire = spinning(&mut s, "expire", &spin);
    let expire_delta = |key: &str| expire_stat[key].saturating_sub(after[key]);
    if expire_delta("nr_throttled") == 0 || expire_delta("throttled_usec") == 0 {
        s.inconclusive.push(format!(
            "expire: no throttling {after:?} -> {expire_stat:?}"
        ));
    }
    if !spinning_expire {
        s.inconclusive
            .push("expire: the busy worker was not running at its end".into());
    }

    // Recovery: the quota back, the spinner gone, a new answer admits again.
    quota.restore();
    let restored = run::sudo(&["cat", &format!("{cgroup}/cpu.max")]);
    if restored.trim() != quota.original {
        s.violations.push(format!(
            "recover: cpu.max is {restored:?}, not the original {:?}",
            quota.original
        ));
    }
    let pids: Vec<String> = spin
        .as_array()
        .map(|pids| pids.iter().map(|pid| pid.to_string()).collect())
        .unwrap_or_default();
    let (_, _, _, killed) = s.call(
        "recover",
        "recover-kill",
        "exec",
        serde_json::json!({"argv": ["/bin/sh", "-c", format!("kill -9 {}", pids.join(" "))]}),
    );
    s.budget = MIN_LATENCY_BOUND;
    let killed_spin = !spinning(&mut s, "recover", &spin);
    let recovered_at = t0.elapsed();
    append(
        &evidence,
        serde_json::json!({"kind": "pressure-off", "at_ms": recovered_at.as_millis(), "expire_end_ms": expire_end.as_millis(),
                           "cpu.max": run::sudo(&["cat", &format!("{cgroup}/cpu.max")]).trim(),
                           "kill": killed, "spinner_gone": killed_spin, "cpu.stat": expire_stat,
                           "spinning_expire": spinning_expire,
                           "lapse_ms": lapse.as_millis(), "held": held_replies}),
    );
    let silent = accepts_after(&la, lapse + ACCEPT_LAG, recovered_at);
    if !silent.is_empty() {
        s.violations.push(format!(
            "expire: A accepted {silent:?} after its promise lapsed at {lapse:?}"
        ));
    }
    if !killed_spin {
        s.violations
            .push("recover: the busy worker survived its kill".into());
    }
    let (requeried, _, _) = s.query("recover", SHORT, &[a_ip], Some(FLOOR));
    if s.fresh("recover", Target::A, BLOCKED) != "connected" {
        s.violations
            .push("recover: a fresh connect after a new answer failed".into());
    }

    // C18-ORDER: a long promise, then a shorter answer, then forbidden ones.
    let (o1_sent, o1_done, _) = s.query("order", LONG, &[b_ip], Some(LONG_TTL));
    let read = s.point_read("order-long");
    let first_remaining = read
        .elements
        .iter()
        .find(|e| e.addr == b_ip)
        .and_then(|e| e.expires_ms);
    let (_, o2_done, _) = {
        dns::set(
            &nameserver,
            LONG,
            vec![dns::Record::A(b_ip, SCRIPTED_SHORT)],
        );
        s.query("order", LONG, &[b_ip], Some(FLOOR))
    };
    let read = s.point_read("order-shorter");
    // What the first answer still had outstanding, at the latest this
    // reading could have been taken.
    let owed = (o1_sent + Duration::from_secs(u64::from(LONG_TTL)))
        .saturating_sub(read.at + read.took + EXPIRY_SLACK);
    match read.elements.iter().find(|e| e.addr == b_ip) {
        Some(element)
            if element
                .expires_ms
                .is_some_and(|ms| Duration::from_millis(ms) >= owed) => {}
        element => s.violations.push(format!(
            "order: the shorter answer left {b_ip} as {:?}, owed {owed:?}: {}",
            element.map(|e| (e.timeout_ms, e.expires_ms)),
            read.raw
        )),
    }
    dns::set(
        &nameserver,
        LONG,
        vec![
            dns::Record::A(b_ip, LONG_TTL),
            dns::Record::A(d_ip, LONG_TTL),
        ],
    );
    s.query("order", LONG, &[b_ip, d_ip], None);
    s.query("order", BAD, &[d_ip], None);
    let read = s.point_read("order-refused");
    if let Some(element) = read.elements.iter().find(|e| e.addr == b_ip) {
        let ceiling = (o1_done + Duration::from_secs(u64::from(LONG_TTL)) + EXPIRY_SLACK)
            .saturating_sub(read.at);
        if element
            .expires_ms
            .is_some_and(|ms| Duration::from_millis(ms) > ceiling)
        {
            s.violations.push(format!(
                "order: a refused answer extended {b_ip} past {ceiling:?}: {}",
                read.raw
            ));
        }
    }
    if s.fresh("order", Target::D, BLOCKED) == "connected" {
        s.violations.push("order: the decoy connected".into());
    }
    // Past anything the shorter answer alone promised, inside the first.
    let shorter_lapse = o2_done + Duration::from_secs(u64::from(FLOOR)) + EXPIRY_SLACK;
    std::thread::sleep(shorter_lapse.saturating_sub(t0.elapsed()) + Duration::from_millis(200));
    let first_earliest = o1_sent + Duration::from_secs(u64::from(LONG_TTL));
    let mut retained = 0;
    while t0.elapsed() + Duration::from_millis(600) < first_earliest {
        s.fresh("order", Target::B, BLOCKED);
        let attempt = s.attempts.last().expect("just attempted");
        if attempt.sent > shorter_lapse && attempt.done + RETAIN_MARGIN < first_earliest {
            retained += 1;
        }
    }
    if retained < 2 {
        s.inconclusive.push(format!(
            "order: only {retained} fresh attempts at B between {shorter_lapse:?} and {first_earliest:?}"
        ));
    }
    let b_lapse = latest(&s.answers, b_ip, t0.elapsed()).expect("B was answered");
    std::thread::sleep(b_lapse.saturating_sub(t0.elapsed()) + Duration::from_millis(200));
    let mut b_failed = 0;
    for _ in 0..4 {
        if s.fresh("order", Target::B, BLOCKED) != "connected" {
            b_failed += 1;
        }
    }
    if b_failed < 2 {
        s.violations.push(format!(
            "order: only {b_failed} of 4 attempts at B failed after {b_lapse:?}"
        ));
    }
    let order_end = t0.elapsed();
    let b_silent = accepts_after(
        &lb,
        b_lapse + ACCEPT_LAG,
        order_end + Duration::from_secs(1),
    );
    if !b_silent.is_empty() {
        s.violations
            .push(format!("order: B accepted {b_silent:?} after {b_lapse:?}"));
    }
    let final_held = s.exchange("order");

    let counts = judge(&mut s);
    std::thread::sleep(Duration::from_millis(500));
    let samples = sampler.finish();

    // C18-EXPIRE's deadline, from the continuous readings rather than a late
    // point read: A's first absence after refreshes stopped, and no return
    // before the recovery query.
    let deadline = lapse + COLLECTED;
    let has_a = |x: &Sample| x.elements.iter().any(|e| e.addr == a_ip);
    let watched: Vec<&Sample> = samples
        .iter()
        .filter(|x| x.at >= stopped && x.at < requeried)
        .collect();
    let first_absent = watched.iter().find(|x| !has_a(x)).map(|x| x.at);
    let last_present = watched.iter().filter(|x| has_a(x)).last().map(|x| {
        (
            x.at,
            x.elements
                .iter()
                .find(|e| e.addr == a_ip)
                .and_then(|e| e.expires_ms),
        )
    });
    if let Some(late) = watched.iter().find(|x| x.at > deadline && has_a(x)) {
        s.violations.push(format!(
            "expire: A still in @resolved at {:?}, past {deadline:?}: {}",
            late.at, late.raw
        ));
    } else if first_absent.is_none_or(|at| at > deadline) {
        s.inconclusive.push(format!(
            "expire: no reading of A's absence by {deadline:?} (first {first_absent:?})"
        ));
    }
    if let Some(gone) = first_absent {
        if let Some(back) = watched.iter().find(|x| x.at > gone && has_a(x)) {
            s.violations.push(format!(
                "expire: A back in @resolved at {:?} with no new answer: {}",
                back.at, back.raw
            ));
        }
    }
    let gap = watched
        .windows(2)
        .filter(|pair| pair[0].at <= deadline)
        .map(|pair| pair[1].at - pair[0].at)
        .max()
        .unwrap_or(Duration::MAX);
    if gap > SAMPLE_GAP {
        s.inconclusive
            .push(format!("expire: @resolved went unread for {gap:?}"));
    }
    let judged_a = kernel_timeline(&mut s, &samples, a_ip, "A");
    let judged_b = kernel_timeline(&mut s, &samples, b_ip, "B");
    if judged_a < 40 || judged_b < 5 {
        s.inconclusive.push(format!(
            "kernel: only {judged_a} readings of A and {judged_b} of B judged"
        ));
    }
    let p_windows = windows(&s, "pressure", a_ip);
    let p_covered = counts.get("pressure").map_or(0, |c| c.covered);
    if p_covered < MIN_COVERED || p_windows.iter().any(|(_, n)| *n < MIN_COVERED_PER_WINDOW) {
        s.inconclusive.push(format!(
            "pressure: {p_covered} covered attempts, windows {p_windows:?}"
        ));
    }
    for sample in &samples {
        if sample.elements.iter().any(|e| e.addr == d_ip) {
            s.violations.push(format!(
                "the decoy {d_ip} was in @resolved at {:?}: {}",
                sample.at, sample.raw
            ));
        }
    }
    if accepted(&ld).0 != 1 {
        s.violations.push(format!(
            "the decoy's listener accepted {}",
            accepts_json(&ld)
        ));
    }
    // Every connection the guest made arrived, and nothing else did.
    for (target, listener) in [(Target::A, &la), (Target::B, &lb)] {
        let connected = s
            .attempts
            .iter()
            .filter(|a| a.target == target && a.result == "connected")
            .count()
            + usize::from(target == Target::A);
        if accepted(listener).0 != connected {
            s.violations.push(format!(
                "{target:?}: {} accepts for {connected} connections",
                accepted(listener).0
            ));
        }
    }
    let a_present: Vec<_> = samples
        .iter()
        .filter(|sample| sample.at >= p_start && sample.at <= p_end)
        .map(|sample| sample.elements.iter().any(|e| e.addr == a_ip))
        .collect();
    append(
        &evidence,
        serde_json::json!({
            "kind": "summary", "counts": counts, "latency_bound_ms": bound.as_millis(),
            "windows": {"b0": b0_windows, "pressure": p_windows},
            "expiry": {"stopped_ms": stopped.as_millis(), "lapse_ms": lapse.as_millis(),
                       "deadline_ms": deadline.as_millis(),
                       "first_absent_ms": first_absent.map(|at| at.as_millis() as u64),
                       "last_present": last_present.map(|(at, ms)| (at.as_millis() as u64, ms)),
                       "max_gap_ms": gap.as_millis() as u64},
            "kernel_judged": {"A": judged_a, "B": judged_b},
            "pressure_samples": a_present.len(),
            "pressure_samples_with_A": a_present.iter().filter(|p| **p).count(),
            "first_remaining_ms": first_remaining, "retained": retained,
            "final_held": final_held, "expire_failed_after": failed_after,
            "samples": samples.iter().map(sample_json).collect::<Vec<_>>(),
            "accepts": {"A": accepts_json(&la), "B": accepts_json(&lb), "D": accepts_json(&ld)},
            "answers": s.answers.iter().map(|a| serde_json::json!({
                "addr": a.addr.to_string(), "sent_ms": a.sent.as_millis() as u64,
                "done_ms": a.done.as_millis() as u64,
                "timeout_ms": a.timeout.map(|t| t.as_millis() as u64), "acked": a.acked,
            })).collect::<Vec<_>>(),
            "violations": s.violations, "inconclusive": s.inconclusive,
            "cpu.max.final": run::sudo(&["cat", &format!("{cgroup}/cpu.max")]).trim(),
        }),
    );
    drop(quota);
    run::assert_framing(&vmm, true);
    assert!(
        s.violations.is_empty(),
        "FAIL:\n{}\nINCONCLUSIVE:\n{}",
        s.violations.join("\n"),
        s.inconclusive.join("\n")
    );
    assert!(
        s.inconclusive.is_empty(),
        "INCONCLUSIVE:\n{}",
        s.inconclusive.join("\n")
    );
}
