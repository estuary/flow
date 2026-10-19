//! An allowed name's answer changes, while a held stream and fresh
//! connections keep running, from a public address to a forbidden one, to a
//! mixed answer of a second public address and the forbidden one, to a CNAME
//! chain ending in a public address, and to one ending in the forbidden one.
//!
//! The forbidden address is the suite's decoy, which the baseline excludes, so
//! the resolver must refuse every answer containing it, whole: nothing in a
//! condemned answer is authorized, extended or remembered. An address already
//! authorized keeps only what its last acknowledged answer promised. Every
//! scripted TTL is one second, which `ttl.json`'s `ttlFloorSecs` clamps to the
//! three seconds the guest is promised.
//!
//! Each destination has a listener of this test's, which records when it
//! accepted and how many bytes it read, so a guest-side timeout is never the
//! only evidence nothing arrived. The host connects to the listeners that must
//! stay silent before the guest starts, which shows they would have seen a
//! connection. The kernel's `@resolved` is sampled throughout. The test writes
//! `dns-answers.evidence` to the run directory as it goes, then
//! fails naming every violated bound; a baseline that does not hold fails at
//! once, as inconclusive.

use super::*;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const NAME: &str = "name.acmeco.example";
/// CNAME targets, which `ttl.json` does not admit by name.
const EDGE: &str = "edge.cdn.example";
const BAD: &str = "bad.cdn.example";
/// Every scripted TTL, clamped up to `PROMISED` by `ttlFloorSecs`.
const SCRIPTED_TTL: u32 = 1;
const PROMISED: Duration = Duration::from_secs(3);
/// A connect across the tap takes milliseconds; a dropped SYN is a timeout.
const FRESH: f64 = 1.0;
const CONNECT_LATENCY: Duration = Duration::from_millis(500);
/// At most ten guest requests a second, queries and connections together.
const PACE: Duration = Duration::from_millis(100);
const ROUND: Duration = Duration::from_millis(250);
const MAX_ROUNDS: usize = 12;
const SEQUENCE: Duration = Duration::from_secs(90);
/// An attempt sent this long after a promise's latest possible expiry must not
/// connect: nft timeouts are kept in jiffies.
const EXPIRY_SLACK: Duration = Duration::from_millis(100);
/// An attempt finished this long before a promise's earliest possible expiry
/// must connect.
const RETAIN_MARGIN: Duration = Duration::from_millis(50);
/// From expiry to the element leaving the set's listing.
const COLLECTED: Duration = Duration::from_secs(2);
const SAMPLE_EVERY: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    A,
    B,
    C,
    D,
}

impl Target {
    fn label(self) -> &'static str {
        match self {
            Target::A => "A",
            Target::B => "B",
            Target::C => "C",
            Target::D => "D",
        }
    }
}

struct Accept {
    at: Duration,
    peer: Ipv4Addr,
    bytes: u64,
}

/// Answers every read with `pong` and never hangs up first, as
/// `endpoint::listen` does, but dates each accept and counts its bytes.
pub(super) struct Listener {
    pub(super) addr: SocketAddr,
    accepts: Arc<Mutex<Vec<Accept>>>,
}

pub(super) fn listen_logged(run: &run::Run, ip: Ipv4Addr, t0: Instant) -> Listener {
    let listener = netns::tcp(netns::Namespace::Named(&run.endpoint_netns), (ip, 0).into());
    let addr = listener
        .local_addr()
        .expect("a bound listener has an address");
    let accepts = Arc::new(Mutex::new(Vec::new()));
    let shared = accepts.clone();

    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let peer = match stream.peer_addr() {
                Ok(SocketAddr::V4(peer)) => *peer.ip(),
                _ => Ipv4Addr::UNSPECIFIED,
            };
            let index = {
                let mut accepts = shared.lock().unwrap();
                accepts.push(Accept {
                    at: t0.elapsed(),
                    peer,
                    bytes: 0,
                });
                accepts.len() - 1
            };
            let shared = shared.clone();
            std::thread::spawn(move || {
                let mut buffer = [0u8; 256];
                while let Ok(read) = stream.read(&mut buffer) {
                    if read == 0 {
                        return;
                    }
                    shared.lock().unwrap()[index].bytes += read as u64;
                    if stream.write_all(b"pong\n").is_err() {
                        return;
                    }
                }
            });
        }
    });
    Listener { addr, accepts }
}

pub(super) fn accepts_json(listener: &Listener) -> serde_json::Value {
    listener
        .accepts
        .lock()
        .unwrap()
        .iter()
        .map(|accept| {
            serde_json::json!({
                "at_ms": accept.at.as_millis() as u64,
                "peer": accept.peer.to_string(),
                "bytes": accept.bytes,
            })
        })
        .collect()
}

pub(super) fn accepted(listener: &Listener) -> (usize, u64) {
    let accepts = listener.accepts.lock().unwrap();
    (
        accepts.len(),
        accepts.iter().map(|accept| accept.bytes).sum(),
    )
}

/// From the host, over the suite's routed hop rather than any VMM: proof the
/// listener would see a connection that reached it.
pub(super) fn host_exchange(addr: SocketAddr) -> String {
    let mut stream = match std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
        Ok(stream) => stream,
        Err(error) => return format!("connect: {error}"),
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("setting a read timeout");
    if let Err(error) = stream.write_all(b"ping\n") {
        return format!("write: {error}");
    }
    let mut reply = [0u8; 16];
    match stream.read(&mut reply) {
        Ok(read) => String::from_utf8_lossy(&reply[..read]).trim().to_string(),
        Err(error) => format!("read: {error}"),
    }
}

#[derive(Clone)]
pub(super) struct Element {
    pub(super) addr: Ipv4Addr,
    pub(super) timeout_ms: Option<u64>,
    pub(super) expires_ms: Option<u64>,
}

/// One reading of the kernel's `@resolved`, in nft's text form, which unlike
/// its JSON keeps expiries to the millisecond.
#[derive(Clone)]
pub(super) struct Sample {
    pub(super) at: Duration,
    pub(super) took: Duration,
    pub(super) raw: String,
    pub(super) elements: Vec<Element>,
}

fn read_set(name: &str, t0: Instant) -> Option<Sample> {
    let start = Instant::now();
    let output = run::sudo_output(
        &[
            "podman",
            "exec",
            name,
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

/// A violation for each address of `forbidden` that `sample` holds.
fn excluded(sample: &Sample, phase: &str, forbidden: &[Ipv4Addr]) -> Vec<String> {
    sample
        .elements
        .iter()
        .filter(|element| forbidden.contains(&element.addr))
        .map(|element| {
            format!(
                "{phase}: {} was in @resolved at {:?}: {}",
                element.addr, sample.at, sample.raw
            )
        })
        .collect()
}

/// Reads `@resolved` every `SAMPLE_EVERY` until finished or dropped. Dropping
/// stops and joins it too, so a test that panics (a baseline that does not
/// hold, say) unwinds without leaving it reading a container being removed.
pub(super) struct Sampler {
    stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<Sample>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

pub(super) fn sample_every(read: impl Fn() -> Option<Sample> + Send + 'static) -> Sampler {
    let stop = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(Mutex::new(Vec::new()));
    let thread = {
        let (stop, samples) = (stop.clone(), samples.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let next = Instant::now() + SAMPLE_EVERY;
                if let Some(sample) = read() {
                    samples.lock().unwrap().push(sample);
                }
                std::thread::sleep(next.saturating_duration_since(Instant::now()));
            }
        })
    };
    Sampler {
        stop,
        samples,
        thread: Some(thread),
    }
}

impl Sampler {
    pub(super) fn finish(mut self) -> Vec<Sample> {
        self.join().expect("the sampler never panics");
        self.samples.lock().unwrap().clone()
    }

    fn join(&mut self) -> std::thread::Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        match self.thread.take() {
            Some(thread) => thread.join(),
            None => Ok(()),
        }
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        // Already unwinding, or already finished: nothing to report.
        let _ = self.join();
    }
}

pub(super) fn sample_json(sample: &Sample) -> serde_json::Value {
    serde_json::json!({
        "at_ms": sample.at.as_millis() as u64,
        "took_ms": sample.took.as_millis() as u64,
        "elements": sample.elements.iter().map(|element| serde_json::json!({
            "addr": element.addr.to_string(),
            "timeout_ms": element.timeout_ms,
            "expires_ms": element.expires_ms,
        })).collect::<Vec<_>>(),
        "raw": sample.raw,
    })
}

pub(super) fn parse_resolved(text: &str) -> Vec<Element> {
    let Some(start) = text.find("elements = {") else {
        return Vec::new();
    };
    let rest = &text[start + "elements = {".len()..];
    let end = rest.find('}').unwrap_or(rest.len());
    rest[..end]
        .split(',')
        .filter_map(|item| {
            let tokens: Vec<&str> = item.split_whitespace().collect();
            let addr = tokens.first()?.parse().ok()?;
            let after = |key: &str| {
                tokens
                    .iter()
                    .position(|token| *token == key)
                    .and_then(|index| tokens.get(index + 1))
                    .and_then(|value| millis(value))
            };
            Some(Element {
                addr,
                timeout_ms: after("timeout"),
                expires_ms: after("expires"),
            })
        })
        .collect()
}

/// nft's durations: `3s`, `2s345ms`, `1m2s`, `345ms`.
fn millis(text: &str) -> Option<u64> {
    let mut total = 0u64;
    let mut number = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            number.push(c);
            continue;
        }
        let value: u64 = number.parse().ok()?;
        number.clear();
        let unit = match c {
            'd' => 86_400_000,
            'h' => 3_600_000,
            'm' if chars.peek() == Some(&'s') => {
                chars.next();
                1
            }
            'm' => 60_000,
            's' => 1_000,
            _ => return None,
        };
        total += value * unit;
    }
    number.is_empty().then_some(total)
}

/// Every guest request goes through here: paced, dated on the host, and
/// written to the evidence.
struct Clock {
    t0: Instant,
    last: Option<Instant>,
    evidence: std::path::PathBuf,
}

struct Call {
    sent: Duration,
    done: Duration,
    guest: Duration,
    result: serde_json::Value,
}

fn call(
    clock: &mut Clock,
    guest: &mut guest::Guest,
    phase: &str,
    name: &str,
    op: &str,
    arguments: serde_json::Value,
) -> Call {
    if let Some(last) = clock.last {
        std::thread::sleep((last + PACE).saturating_duration_since(Instant::now()));
    }
    let sent = Instant::now();
    clock.last = Some(sent);
    let (result, took) = guest::call_timed(guest, name, op, arguments.clone());
    let call = Call {
        sent: sent - clock.t0,
        done: clock.t0.elapsed(),
        guest: took,
        result,
    };
    append(
        &clock.evidence,
        serde_json::json!({
            "kind": "call",
            "phase": phase,
            "probe": name,
            "op": op,
            "arguments": arguments,
            "sent_ms": call.sent.as_millis() as u64,
            "done_ms": call.done.as_millis() as u64,
            "guest_ms": call.guest.as_millis() as u64,
            "result": call.result,
        }),
    );
    call
}

struct Attempt {
    phase: &'static str,
    target: Target,
    sent: Duration,
    done: Duration,
    guest: Duration,
    result: String,
}

/// The rcode and answer count of a response the guest returned as hex.
fn raw_header(hex: &str) -> Option<(u16, u16)> {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(hex.get(index..index + 2)?, 16).ok())
        .collect::<Option<_>>()?;
    let flags = u16::from_be_bytes([*bytes.get(2)?, *bytes.get(3)?]);
    let answers = u16::from_be_bytes([*bytes.get(6)?, *bytes.get(7)?]);
    Some((flags & 0xF, answers))
}

struct Sequence {
    clock: Clock,
    guest: guest::Guest,
    vmm_name: String,
    addrs: [(Target, Ipv4Addr, u16); 4],
    attempts: Vec<Attempt>,
    exchanges: Vec<(Duration, String)>,
    violations: Vec<String>,
    inconclusive: Vec<String>,
    queries: usize,
}

impl Sequence {
    fn target(&self, target: Target) -> (Ipv4Addr, u16) {
        let (_, addr, port) = self
            .addrs
            .iter()
            .find(|(held, ..)| *held == target)
            .expect("every target has an address");
        (*addr, *port)
    }

    fn fresh(&mut self, phase: &'static str, target: Target) -> String {
        let (addr, port) = self.target(target);
        let name = format!("{phase}-fresh-{}", target.label());
        let call = call(
            &mut self.clock,
            &mut self.guest,
            phase,
            &name,
            "tcp",
            serde_json::json!({"addr": addr.to_string(), "port": port, "timeout": FRESH}),
        );
        let result = call.result.as_str().unwrap_or("non-string").to_string();
        if result == "connected" && call.guest > CONNECT_LATENCY {
            self.violations.push(format!(
                "bound: {name} connected in {:?}, over {CONNECT_LATENCY:?}",
                call.guest
            ));
        }
        self.attempts.push(Attempt {
            phase,
            target,
            sent: call.sent,
            done: call.done,
            guest: call.guest,
            result: result.clone(),
        });
        result
    }

    fn exchange(&mut self, phase: &'static str) {
        let call = call(
            &mut self.clock,
            &mut self.guest,
            phase,
            &format!("{phase}-held"),
            "exchange",
            serde_json::json!({"id": "held", "timeout": 2}),
        );
        let reply = call.result.as_str().unwrap_or("non-string").to_string();
        if reply != "pong" {
            self.violations.push(format!(
                "{phase}: the held stream answered {reply:?} at {:?}",
                call.sent
            ));
        }
        self.exchanges.push((call.sent, reply));
    }

    /// A raw query for `qname` and the status the guest decoded, checked
    /// against the response's own header.
    fn query(&mut self, phase: &'static str, qname: &str) -> Call {
        self.queries += 1;
        let name = format!("{phase}-query-{}", self.queries);
        let call = call(
            &mut self.clock,
            &mut self.guest,
            phase,
            &name,
            "query",
            serde_json::json!({"nameserver": VMM_TAP.to_string(), "name": qname, "qtype": 1, "timeout": BLOCKED, "raw": true}),
        );
        let status = call.result["status"].as_str().unwrap_or_default();
        let header = call.result["raw"].as_str().and_then(raw_header);
        let agrees = match (status, header) {
            ("ok", Some((0, _))) => true,
            (status, Some((rcode, _))) => status == format!("rcode:{rcode}"),
            (_, None) => false,
        };
        if !agrees {
            self.violations.push(format!(
                "harness: {name} decoded {status:?} but its raw header is {header:?}"
            ));
        }
        call
    }

    fn expect_records(&mut self, call: &Call, what: &str, want: serde_json::Value) -> bool {
        let got = &call.result;
        if got["status"] == "ok" && got["records"] == want {
            return true;
        }
        self.violations
            .push(format!("{what}: answered {got}, expected records {want}"));
        false
    }

    fn expect_refused(&mut self, call: &Call, what: &str) {
        if call.result["status"] != "rcode:5" {
            self.violations.push(format!(
                "{what}: answered {}, expected REFUSED (rcode:5)",
                call.result
            ));
        }
    }

    fn point_read(&mut self, phase: &str) -> Sample {
        let sample = read_set(&self.vmm_name, self.clock.t0)
            .unwrap_or_else(|| panic!("{phase}: reading @resolved failed"));
        append(
            &self.clock.evidence,
            serde_json::json!({"kind": "point-read", "phase": phase, "sample": sample_json(&sample)}),
        );
        let forbidden = [self.target(Target::B).0, self.target(Target::D).0];
        self.violations.extend(excluded(&sample, phase, &forbidden));
        sample
    }

    /// Fresh attempts at `target`, each round with an exchange on the held
    /// stream, until two attempts sent after `after` failed.
    fn until_expired(&mut self, phase: &'static str, target: Target, after: Duration) {
        let mut failed = 0;
        for _ in 0..MAX_ROUNDS {
            let round = Instant::now();
            let result = self.fresh(phase, target);
            let sent = self.attempts.last().expect("just attempted").sent;
            self.exchange(phase);
            if sent > after && result != "connected" {
                failed += 1;
                if failed == 2 {
                    return;
                }
            }
            std::thread::sleep((round + ROUND).saturating_duration_since(Instant::now()));
        }
        self.inconclusive.push(format!(
            "{phase}: {} never failed twice after {after:?} within {MAX_ROUNDS} rounds",
            target.label()
        ));
    }

    /// Wait for `addr` to leave the set, which it must by `by`.
    fn collected(&mut self, phase: &str, addr: Ipv4Addr, by: Duration) {
        loop {
            let sample = self.point_read(phase);
            if !sample.elements.iter().any(|element| element.addr == addr) {
                return;
            }
            if sample.at > by {
                self.violations.push(format!(
                    "{phase}: {addr} still in @resolved at {:?}, past {by:?}",
                    sample.at
                ));
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// O5 and O6 for one address: nothing connects once its latest promise
    /// can have lapsed, and everything connects while its earliest possible
    /// expiry is still ahead, from `from` (the condemning answer) on.
    fn retention(
        &mut self,
        target: Target,
        from: Duration,
        queried: Duration,
        acknowledged: Duration,
    ) -> serde_json::Value {
        let label = target.label();
        let latest = acknowledged + PROMISED + EXPIRY_SLACK;
        let earliest = (queried + PROMISED).saturating_sub(RETAIN_MARGIN);
        let mut before = 0;
        let mut after = 0;
        for attempt in self
            .attempts
            .iter()
            .filter(|attempt| attempt.target == target)
        {
            if attempt.sent > latest {
                after += 1;
                if attempt.result == "connected" {
                    self.violations.push(format!(
                        "{label}: a fresh connect sent at {:?} ({}) connected, after its promise lapsed by {latest:?}",
                        attempt.sent, attempt.phase
                    ));
                }
            }
            if attempt.sent >= from && attempt.done < earliest {
                before += 1;
                if attempt.result != "connected" {
                    self.violations.push(format!(
                        "{label}: a fresh connect sent at {:?} ({}) answered {:?} before its promise could lapse at {earliest:?}",
                        attempt.sent, attempt.phase, attempt.result
                    ));
                }
            }
        }
        if before < 2 {
            self.inconclusive.push(format!(
                "{label}: only {before} fresh attempts between the condemning answer at {from:?} and {earliest:?}"
            ));
        }
        if after < 2 {
            self.inconclusive.push(format!(
                "{label}: only {after} fresh attempts after {latest:?}"
            ));
        }
        serde_json::json!({
            "target": label,
            "condemned_at_ms": from.as_millis() as u64,
            "queried_ms": queried.as_millis() as u64,
            "acknowledged_ms": acknowledged.as_millis() as u64,
            "earliest_expiry_ms": earliest.as_millis() as u64,
            "latest_expiry_ms": latest.as_millis() as u64,
            "attempts_retained": before,
            "attempts_expired": after,
        })
    }
}

pub(super) fn baseline(condition: bool, what: impl std::fmt::Display) {
    assert!(
        condition,
        "INCONCLUSIVE: the baseline does not hold: {what}"
    );
}

#[test]
fn dns_answer_changes() {
    let run = run::load();
    let [ns_ip, a_ip, b_ip, c_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let d_ip = run.decoys[0];
    let t0 = Instant::now();
    let evidence = run.run_dir.join("dns-answers.evidence");
    let _ = std::fs::remove_file(&evidence);

    let nameserver = scripted(&run, ns_ip);
    let la = listen_logged(&run, a_ip, t0);
    let lb = listen_logged(&run, b_ip, t0);
    let lc = listen_logged(&run, c_ip, t0);
    let ld = listen_logged(&run, d_ip, t0);
    let a = serde_json::json!([["A", a_ip.to_string(), PROMISED.as_secs()]]);
    let c = serde_json::json!([["A", c_ip.to_string(), PROMISED.as_secs()]]);
    dns::set(&nameserver, NAME, vec![dns::Record::A(a_ip, SCRIPTED_TTL)]);
    // Scripted up front, so that a refusal of either is the gate's.
    dns::set(&nameserver, EDGE, vec![dns::Record::A(c_ip, SCRIPTED_TTL)]);
    dns::set(&nameserver, BAD, vec![dns::Record::A(d_ip, SCRIPTED_TTL)]);
    append(
        &evidence,
        serde_json::json!({
            "kind": "setup",
            "nameserver": nameserver.addr.to_string(),
            "A": la.addr.to_string(), "B": lb.addr.to_string(),
            "C": lc.addr.to_string(), "D": ld.addr.to_string(),
            "policy": "ttl.json", "scripted_ttl": SCRIPTED_TTL,
        }),
    );

    for (label, listener) in [("B", &lb), ("D", &ld)] {
        let reply = host_exchange(listener.addr);
        baseline(
            reply == "pong",
            format!("the host's control connection to {label} answered {reply:?}"),
        );
    }
    let controls = run::eventually("the controls' bytes", Duration::from_secs(2), || {
        let counts = (accepted(&lb), accepted(&ld));
        (counts == ((1, 5), (1, 5))).then_some(counts)
    });
    append(
        &evidence,
        serde_json::json!({"kind": "controls", "B": accepts_json(&lb), "D": accepts_json(&ld)}),
    );

    let mut spec = run::Spec::probes(&run, "ttl.json");
    spec.flags = upstream(&nameserver);
    let (vmm, guest) = start(&run, spec);
    let pid = run::pid(&vmm);
    let counters_before = host::counters(&vmm.name);

    let sampler = {
        let name = vmm.name.clone();
        sample_every(move || read_set(&name, t0))
    };

    let mut s = Sequence {
        clock: Clock {
            t0,
            last: None,
            evidence: evidence.clone(),
        },
        guest,
        vmm_name: vmm.name.clone(),
        addrs: [
            (Target::A, a_ip, la.addr.port()),
            (Target::B, b_ip, lb.addr.port()),
            (Target::C, c_ip, lc.addr.port()),
            (Target::D, d_ip, ld.addr.port()),
        ],
        attempts: Vec::new(),
        exchanges: Vec::new(),
        violations: Vec::new(),
        inconclusive: Vec::new(),
        queries: 0,
    };

    // The allowed name answers a public address, which is authorized for
    // its clamped TTL and reachable, and listener A sees it.
    let first = s.query("initial", NAME);
    baseline(
        first.result["status"] == "ok" && first.result["records"] == a,
        format!("{NAME} answered {}", first.result),
    );
    let read = s.point_read("initial");
    let element = read.elements.iter().find(|element| element.addr == a_ip);
    baseline(
        element.is_some_and(|element| element.timeout_ms == Some(PROMISED.as_millis() as u64)),
        format!("@resolved after the first answer: {}", read.raw),
    );
    let held = call(
        &mut s.clock,
        &mut s.guest,
        "initial",
        "initial-hold",
        "hold",
        serde_json::json!({"id": "held", "addr": a_ip.to_string(), "port": la.addr.port(), "timeout": 2}),
    );
    baseline(
        held.result == "connected",
        format!("holding A: {}", held.result),
    );
    s.exchange("initial");
    for _ in 0..2 {
        let result = s.fresh("initial", Target::A);
        baseline(
            result == "connected",
            format!("a fresh connect to A: {result}"),
        );
    }
    baseline(
        accepted(&la).0 == 3,
        format!("listener A accepted {:?}, not 3", accepted(&la)),
    );
    baseline(s.violations.is_empty(), format!("{:?}", s.violations));
    for target in [Target::B, Target::D] {
        let result = s.fresh("initial", target);
        if result == "connected" {
            s.violations.push(format!(
                "initial: {} connected before any answer named it",
                target.label()
            ));
        }
    }

    // Refresh A. This answer's promise is the one the rest must keep.
    let sequence = Instant::now();
    let refresh = s.query("refresh", NAME);
    s.expect_records(&refresh, "refresh refresh", a.clone());
    let (a_queried, a_acknowledged) = (refresh.sent, refresh.done);
    s.fresh("refresh", Target::A);

    // Forbidden, then mixed with A itself. A stays only as promised.
    dns::set(&nameserver, NAME, vec![dns::Record::A(d_ip, SCRIPTED_TTL)]);
    let forbidden = s.query("forbidden_answer", NAME);
    s.expect_refused(&forbidden, "forbidden_answer forbidden");
    let a_condemned = forbidden.done;
    for _ in 0..3 {
        s.fresh("forbidden_answer", Target::A);
    }
    s.exchange("forbidden_answer");
    dns::set(
        &nameserver,
        NAME,
        vec![
            dns::Record::A(a_ip, SCRIPTED_TTL),
            dns::Record::A(d_ip, SCRIPTED_TTL),
        ],
    );
    let mixed_a = s.query("forbidden_answer", NAME);
    s.expect_refused(&mixed_a, "forbidden_answer mixed A+D");
    let read = s.point_read("forbidden_answer-after-mixed");
    if let Some(element) = read.elements.iter().find(|element| element.addr == a_ip) {
        let implied = read.at + Duration::from_millis(element.expires_ms.unwrap_or(u64::MAX / 2));
        if implied > a_acknowledged + PROMISED + EXPIRY_SLACK {
            s.violations.push(format!(
                "forbidden_answer: after a condemned answer naming A, A expires at {implied:?}, past its promise {:?}",
                a_acknowledged + PROMISED
            ));
        }
    }
    s.until_expired(
        "forbidden_answer",
        Target::A,
        a_acknowledged + PROMISED + EXPIRY_SLACK,
    );
    s.collected(
        "forbidden_answer",
        a_ip,
        a_acknowledged + PROMISED + COLLECTED,
    );
    for _ in 0..2 {
        s.fresh("forbidden_answer", Target::D);
    }

    // A second public address beside the forbidden one, in both orders.
    for records in [
        vec![
            dns::Record::A(b_ip, SCRIPTED_TTL),
            dns::Record::A(d_ip, SCRIPTED_TTL),
        ],
        vec![
            dns::Record::A(d_ip, SCRIPTED_TTL),
            dns::Record::A(b_ip, SCRIPTED_TTL),
        ],
    ] {
        dns::set(&nameserver, NAME, records);
        let mixed = s.query("mixed_answer", NAME);
        s.expect_refused(&mixed, "mixed_answer mixed B+D");
    }
    s.point_read("mixed_answer");
    for target in [Target::B, Target::B, Target::D, Target::A] {
        s.fresh("mixed_answer", target);
    }
    s.exchange("mixed_answer");

    // A CNAME chain to a public address, then its remembered target asked
    // directly, which refreshes the same address.
    dns::set(
        &nameserver,
        NAME,
        vec![dns::Record::Cname(EDGE.to_string(), SCRIPTED_TTL)],
    );
    let chain = s.query("public_cname", NAME);
    let mut want = vec![serde_json::json!(["CNAME", EDGE, PROMISED.as_secs()])];
    want.extend(c.as_array().unwrap().iter().cloned());
    s.expect_records(&chain, "public_cname chain", serde_json::Value::Array(want));
    let edge = s.query("public_cname", EDGE);
    let (c_queried, c_acknowledged) =
        if s.expect_records(&edge, "public_cname remembered target", c) {
            (edge.sent, edge.done)
        } else {
            (chain.sent, chain.done)
        };
    for _ in 0..2 {
        s.fresh("public_cname", Target::C);
    }
    s.exchange("public_cname");

    // A CNAME chain to the forbidden address. C stays only as promised.
    dns::set(
        &nameserver,
        NAME,
        vec![dns::Record::Cname(BAD.to_string(), SCRIPTED_TTL)],
    );
    let bad_chain = s.query("forbidden_cname", NAME);
    s.expect_refused(&bad_chain, "forbidden_cname chain to forbidden");
    let c_condemned = bad_chain.done;
    let bad = s.query("forbidden_cname", BAD);
    s.expect_refused(&bad, "forbidden_cname condemned target asked directly");
    if bad.guest >= Duration::from_secs(1) {
        s.violations.push(format!(
            "forbidden_cname: refusing {BAD} took {:?}",
            bad.guest
        ));
    }
    for _ in 0..2 {
        s.fresh("forbidden_cname", Target::C);
    }
    s.until_expired(
        "forbidden_cname",
        Target::C,
        c_acknowledged + PROMISED + EXPIRY_SLACK,
    );
    s.collected(
        "forbidden_cname",
        c_ip,
        c_acknowledged + PROMISED + COLLECTED,
    );
    for target in [Target::A, Target::D, Target::D] {
        s.fresh("forbidden_cname", target);
    }
    s.exchange("forbidden_cname");
    s.point_read("end");
    let elapsed = sequence.elapsed();
    if elapsed > SEQUENCE {
        s.violations.push(format!(
            "bound: the sequence took {elapsed:?}, over {SEQUENCE:?}"
        ));
    }

    let samples = sampler.finish();

    let retained_a = s.retention(Target::A, a_condemned, a_queried, a_acknowledged);
    let retained_c = s.retention(Target::C, c_condemned, c_queried, c_acknowledged);

    for sample in &samples {
        s.violations
            .extend(excluded(sample, "sampler", &[b_ip, d_ip]));
    }
    let seen_a = samples
        .iter()
        .any(|sample| sample.elements.iter().any(|element| element.addr == a_ip));
    let seen_c = samples.iter().any(|sample| {
        sample.elements.iter().any(|element| {
            element.addr == c_ip && element.timeout_ms == Some(PROMISED.as_millis() as u64)
        })
    });
    if !seen_a || !seen_c {
        s.inconclusive.push(format!(
            "the sampler never saw A ({seen_a}) or C with a {PROMISED:?} timeout ({seen_c})"
        ));
    }

    let flows = guest_flows(pid);
    if !flows
        .iter()
        .any(|flow| flow.proto == "tcp" && flow.dst == a_ip && flow.dport == Some(la.addr.port()))
    {
        s.inconclusive.push(format!(
            "the held flow is missing from conntrack: {flows:?}"
        ));
    }
    for flow in &flows {
        if flow.dst == b_ip || flow.dst == d_ip {
            s.violations.push(format!(
                "conntrack holds a flow to a forbidden destination: {flow:?}"
            ));
        }
    }

    let counters_after = host::counters(&vmm.name);
    let delta = |rule: &str| counter(&counters_after, rule) - counter(&counters_before, rule);
    let count = |target: Target, failed: bool| {
        s.attempts
            .iter()
            .filter(|attempt| attempt.target == target && (attempt.result != "connected") == failed)
            .count() as u64
    };
    let d_attempts = count(Target::D, true) + count(Target::D, false);
    let dropped_elsewhere =
        count(Target::B, true) + count(Target::A, true) + count(Target::C, true);
    if delta("forward/baseline") < d_attempts {
        s.violations.push(format!(
            "forward/baseline counted {} packets for {d_attempts} attempts at D",
            delta("forward/baseline")
        ));
    }
    if delta("forward/forward-drop") < dropped_elsewhere {
        s.violations.push(format!(
            "forward/forward-drop counted {} packets for {dropped_elsewhere} refused attempts at A, B and C",
            delta("forward/forward-drop")
        ));
    }

    let upstream = dns::queries(&nameserver);
    if upstream.iter().any(|name| name == BAD) {
        s.violations.push(format!(
            "{BAD}, a condemned answer's target, went upstream: {upstream:?}"
        ));
    }
    if upstream.iter().filter(|name| *name == EDGE).count() != 1 {
        s.violations.push(format!(
            "{EDGE} went upstream other than once: {upstream:?}"
        ));
    }

    let stderr = run::stderr_text(&vmm);
    for line in [
        format!("refused DNS name {NAME}, which resolved to an address that is not public"),
        format!("refused DNS name {BAD}, which this connector's egress does not permit"),
    ] {
        if !stderr.contains(&line) {
            s.violations
                .push(format!("the resolver's stderr lacks {line:?}"));
        }
    }

    let connected = |target: Target| count(target, false) as usize;
    let exchanged = s
        .exchanges
        .iter()
        .filter(|(_, reply)| reply == "pong")
        .count() as u64;
    let (a_accepts, a_bytes) = accepted(&la);
    let (c_accepts, c_bytes) = accepted(&lc);
    if a_accepts != 1 + connected(Target::A) || a_bytes != 5 * exchanged {
        s.violations.push(format!(
            "listener A: {a_accepts} accepts and {a_bytes} bytes, for 1 held + {} fresh and {exchanged} exchanges",
            connected(Target::A)
        ));
    }
    if c_accepts != connected(Target::C) || c_bytes != 0 {
        s.violations.push(format!(
            "listener C: {c_accepts} accepts and {c_bytes} bytes, for {} fresh",
            connected(Target::C)
        ));
    }
    for (label, listener) in [("B", &lb), ("D", &ld)] {
        if accepted(listener) != (1, 5) {
            s.violations.push(format!(
                "listener {label} saw more than the host's control: {}",
                accepts_json(listener)
            ));
        }
    }
    for target in [Target::B, Target::D] {
        if connected(target) != 0 {
            s.violations.push(format!(
                "the guest connected to {} {} times",
                target.label(),
                connected(target)
            ));
        }
    }

    let attempts: Vec<_> = s
        .attempts
        .iter()
        .map(|attempt| {
            serde_json::json!({
                "phase": attempt.phase, "target": attempt.target.label(),
                "sent_ms": attempt.sent.as_millis() as u64, "done_ms": attempt.done.as_millis() as u64,
                "guest_ms": attempt.guest.as_millis() as u64, "result": attempt.result,
            })
        })
        .collect();
    let summary = serde_json::json!({
        "kind": "summary",
        "sequence_ms": elapsed.as_millis() as u64,
        "controls": format!("{controls:?}"),
        "retention": [retained_a, retained_c],
        "attempts": attempts,
        "exchanges": s.exchanges.iter().map(|(at, reply)| serde_json::json!([at.as_millis() as u64, reply])).collect::<Vec<_>>(),
        "listeners": {"A": accepts_json(&la), "B": accepts_json(&lb), "C": accepts_json(&lc), "D": accepts_json(&ld)},
        "counters": {"forward/baseline": delta("forward/baseline"), "forward/forward-drop": delta("forward/forward-drop"), "egress_accept/resolved": delta("egress_accept/resolved")},
        "upstream": upstream,
        "conntrack": flows.iter().map(|flow| format!("{flow:?}")).collect::<Vec<_>>(),
        "violations": s.violations,
        "inconclusive": s.inconclusive,
    });
    append(&evidence, summary.clone());
    for sample in &samples {
        append(
            &evidence,
            serde_json::json!({"kind": "sample", "sample": sample_json(sample)}),
        );
    }
    append(
        &evidence,
        serde_json::json!({"kind": "stderr", "text": stderr}),
    );
    eprintln!(
        "dns_answers summary: {}",
        serde_json::to_string_pretty(&summary).expect("a summary serializes")
    );
    run::assert_framing(&vmm, true);

    assert!(
        s.violations.is_empty() && s.inconclusive.is_empty(),
        "dns_answers violations: {:#?}\ndns_answers inconclusive: {:#?}",
        s.violations,
        s.inconclusive
    );
}

#[test]
fn nft_durations() {
    for (text, want) in [
        ("3s", Some(3_000)),
        ("2s345ms", Some(2_345)),
        ("345ms", Some(345)),
        ("1m2s", Some(62_000)),
        ("1h", Some(3_600_000)),
        ("3", None),
        ("3x", None),
    ] {
        assert_eq!(millis(text), want, "{text}");
    }
    let listing = "table inet flow_egress {\n\tset resolved {\n\t\ttype ipv4_addr\n\t\tsize 4096\n\t\tflags timeout\n\t\telements = { 192.31.196.242 timeout 3s expires 2s345ms,\n\t\t\t     192.31.196.244 timeout 3s expires 998ms }\n\t}\n}\n";
    let elements = parse_resolved(listing);
    assert_eq!(
        elements
            .iter()
            .map(|element| (
                element.addr.to_string(),
                element.timeout_ms,
                element.expires_ms
            ))
            .collect::<Vec<_>>(),
        vec![
            ("192.31.196.242".to_string(), Some(3_000), Some(2_345)),
            ("192.31.196.244".to_string(), Some(3_000), Some(998)),
        ]
    );
    assert!(parse_resolved("table inet flow_egress {\n\tset resolved {\n\t}\n}\n").is_empty());
}

/// The two harness guards, without a VMM: the sampler is stopped and joined
/// when a panic unwinds past it, and a reading holding a forbidden address
/// is a violation whichever check took it.
#[test]
fn harness_guards() {
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let counted = reads.clone();
        let _sampler = sample_every(move || {
            counted.fetch_add(1, Ordering::Relaxed);
            None
        });
        run::eventually("a first read", Duration::from_secs(2), || {
            (Arc::strong_count(&reads) == 2 && reads.load(Ordering::Relaxed) > 0).then_some(())
        });
        panic!("a baseline that does not hold");
    }));
    assert!(unwound.is_err());
    // The thread owned the only other reference, through its closure: it is
    // gone only once the thread has returned and been joined.
    assert_eq!(
        Arc::strong_count(&reads),
        1,
        "the sampler outlived the unwind"
    );

    let listing = "table inet flow_egress {\n\tset resolved {\n\t\telements = { 192.31.196.242 timeout 3s expires 2s345ms, 198.18.255.241 timeout 3s expires 1s, 192.31.196.243 timeout 3s expires 998ms }\n\t}\n}\n";
    let sample = Sample {
        at: Duration::from_millis(1234),
        took: Duration::ZERO,
        raw: listing.to_string(),
        elements: parse_resolved(listing),
    };
    let forbidden = [
        Ipv4Addr::new(192, 31, 196, 243),
        Ipv4Addr::new(198, 18, 255, 241),
    ];
    let found = excluded(&sample, "mixed_answer", &forbidden);
    assert_eq!(found.len(), 2, "{found:?}");
    assert!(found[0].starts_with("mixed_answer: 198.18.255.241 was in @resolved at 1.234s"));
    assert!(found[1].starts_with("mixed_answer: 192.31.196.243 was in @resolved at 1.234s"));
    assert!(excluded(&sample, "mixed_answer", &[Ipv4Addr::new(192, 31, 196, 244)]).is_empty());
}
