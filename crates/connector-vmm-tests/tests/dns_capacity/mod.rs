//! The resolver's two capacities, remembered addresses and remembered
//! CNAME targets, each filled to exactly its cap, overrun, left to expire and
//! filled again.
//!
//! Both caps are the resolver's own `CAP`, which is the kernel `@resolved`
//! set's size. An answer that would take either past it is SERVFAIL and
//! authorizes nothing, so the test reads the kernel's own count at the cap,
//! checks that no overrun address ever enters the set, and keeps listeners of
//! its own behind the refused endpoint and the decoy, which must never see the
//! guest. An entry counts against the cap until `RECLAIM` past its expiry, so
//! the refill waits that long after the last authorization and then fills both
//! caps again, re-inserting the same addresses under new names.
//!
//! Fill addresses come from `FILL_NET`: public, outside the suite's fence, and
//! blackholed inside the VMM's own network namespace. The guest never connects
//! to one; it connects only to the suite's endpoints and decoy. Every scripted
//! TTL is `TTL`, which `capacity.json` does not clamp.
//!
//! The target answers are synthetic stress of the parser and the cap, not a
//! recursive resolver's chain: each carries 32 CNAME records sharing one
//! owner, which the resolver remembers as 32 targets because it reads every
//! CNAME of the answer section without following owners.
//!
//! Refusal logging is bounded on its own terms: the first 32 refused names,
//! then one line saying the rest are not reported. A SERVFAIL for capacity is
//! never logged. The test writes `dns-capacity.evidence` to the run
//! directory as it goes, then fails naming every violated bound; a baseline
//! that does not hold, or an at-capacity check that ran too late to mean
//! anything, fails as inconclusive.

use super::dns_answers::{baseline, host_exchange};
use super::*;
use std::path::PathBuf;

/// `connector_vmm::ruleset::RESOLVED_SIZE`, which the resolver also applies to
/// remembered CNAME targets.
const CAP: usize = 1024;
/// `RECLAIM` in the resolver: how long past expiry an entry still counts.
const RECLAIM: Duration = Duration::from_secs(10);
const TTL: u32 = 45;
/// Each answer's records stay well inside the guest probe's 4 KiB buffer.
const PER_ADDRESS_ANSWER: usize = 64;
const PER_TARGET_ANSWER: usize = 32;
/// From expiry to the element leaving the set's listing, as `dns_answers` measured.
const COLLECTED: Duration = Duration::from_secs(2);
/// An attempt sent this long after a promise's latest possible expiry must not
/// connect: nft timeouts are kept in jiffies.
const EXPIRY_SLACK: Duration = Duration::from_millis(100);
const REFILL_MARGIN: Duration = Duration::from_secs(1);
/// An at-capacity check must finish this long before the earliest entry could
/// expire, or it says nothing about a full resolver.
const AT_CAPACITY_GUARD: Duration = Duration::from_secs(2);
const SEQUENCE: Duration = Duration::from_secs(150);
const MAX_QUERIES: usize = 400;
const FORBIDDEN: usize = 40;
/// `REPORTED_REFUSALS` in the resolver.
const REPORTED_REFUSALS: usize = 32;
const REFUSAL_LINE: usize = 512;
const RSS_BUDGET: u64 = 32 * MIB;
const CGROUP_BUDGET: u64 = 64 * MIB;
const FRESH: f64 = 1.0;
const SAMPLE_EVERY: Duration = Duration::from_secs(1);
const FILL_NET: &str = "192.31.200.0/21";
const FILL_BASE: Ipv4Addr = Ipv4Addr::new(192, 31, 200, 0);

/// The first fill's addresses are `fill(0..CAP - 1)`, and the overruns take
/// the indices after them, so no address is in two roles.
fn fill(index: usize) -> Ipv4Addr {
    Ipv4Addr::from(u32::from(FILL_BASE) + 1 + index as u32)
}

fn a_record(addr: Ipv4Addr) -> serde_json::Value {
    serde_json::json!(["A", addr.to_string(), TTL])
}

fn cname_record(target: &str) -> serde_json::Value {
    serde_json::json!(["CNAME", target, TTL])
}

/// Address answers of at most `PER_ADDRESS_ANSWER`, named `{prefix}{k}`.
fn address_batches(prefix: &str, addrs: &[Ipv4Addr]) -> Vec<(String, Vec<Ipv4Addr>)> {
    addrs
        .chunks(PER_ADDRESS_ANSWER)
        .enumerate()
        .map(|(k, chunk)| (format!("{prefix}{k}.acmeco.example"), chunk.to_vec()))
        .collect()
}

/// `count` CNAME targets in answers of at most `PER_TARGET_ANSWER`, the
/// answer `{prefix}{k}` naming `{target}{k}x{j}.cnamefill.example`.
fn target_batches(prefix: &str, target: &str, count: usize) -> Vec<(String, Vec<String>)> {
    (0..count.div_ceil(PER_TARGET_ANSWER))
        .map(|k| {
            let targets = (0..PER_TARGET_ANSWER.min(count - k * PER_TARGET_ANSWER))
                .map(|j| format!("{target}{k}x{j}.cnamefill.example"))
                .collect();
            (format!("{prefix}{k}.acmeco.example"), targets)
        })
        .collect()
}

fn script_addresses(nameserver: &dns::Nameserver, name: &str, addrs: &[Ipv4Addr]) {
    let records = addrs.iter().map(|a| dns::Record::A(*a, TTL)).collect();
    dns::set(nameserver, name, records);
}

fn script_targets(nameserver: &dns::Nameserver, name: &str, targets: &[String]) {
    let records = targets
        .iter()
        .map(|t| dns::Record::Cname(t.clone(), TTL))
        .collect();
    dns::set(nameserver, name, records);
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

struct Answer {
    status: String,
    header: Option<(u16, u16)>,
    records: serde_json::Value,
    sent: Instant,
    done: Instant,
}

struct Sequence {
    t0: Instant,
    evidence: PathBuf,
    guest: guest::Guest,
    vmm_name: String,
    pid: u32,
    cgroup: String,
    nameserver: dns::Nameserver,
    queries: usize,
    violations: Vec<String>,
    inconclusive: Vec<String>,
    /// Every distinct name the resolver answered REFUSED, in the order sent:
    /// what its refusal log reports, up to its cap.
    refused: Vec<String>,
    servfailed: Vec<String>,
    /// Addresses that must never be in `@resolved`.
    forbidden: Vec<Ipv4Addr>,
    /// When the latest answer authorizing an address returned to the host.
    last_address: Instant,
    last_target: Instant,
}

impl Sequence {
    fn ms(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.t0).as_millis() as u64
    }

    fn upstream(&self, name: &str) -> usize {
        dns::queries(&self.nameserver)
            .iter()
            .filter(|asked| *asked == name)
            .count()
    }

    fn query(&mut self, phase: &str, qname: &str) -> Answer {
        self.queries += 1;
        assert!(
            self.queries <= MAX_QUERIES,
            "harness: more than {MAX_QUERIES} queries"
        );
        let probe = format!("{phase}-query-{}", self.queries);
        let arguments = serde_json::json!({"nameserver": VMM_TAP.to_string(), "name": qname, "qtype": 1, "timeout": BLOCKED, "raw": true});
        let sent = Instant::now();
        let (result, took) = guest::call_timed(&mut self.guest, &probe, "query", arguments);
        let done = Instant::now();

        let status = result["status"].as_str().unwrap_or_default().to_string();
        let header = result["raw"].as_str().and_then(raw_header);
        let agrees = match (status.as_str(), header) {
            ("ok", Some((0, _))) => true,
            (status, Some((rcode, _))) => status == format!("rcode:{rcode}"),
            (_, None) => false,
        };
        if !agrees {
            self.violations.push(format!(
                "harness: {probe} decoded {status:?} but its raw header is {header:?}"
            ));
        }
        append(
            &self.evidence,
            serde_json::json!({
                "kind": "query", "phase": phase, "probe": probe, "qname": qname,
                "sent_ms": self.ms(sent), "done_ms": self.ms(done),
                "guest_ms": took.as_millis() as u64, "header": header, "result": result,
            }),
        );
        Answer {
            status,
            header,
            records: result["records"].clone(),
            sent,
            done,
        }
    }

    /// An allowed answer: exactly `want`, forwarded once.
    fn resolves(&mut self, phase: &str, qname: &str, want: Vec<serde_json::Value>) -> Answer {
        let before = self.upstream(qname);
        let answer = self.query(phase, qname);
        let want = serde_json::Value::Array(want);
        if answer.status != "ok" || answer.records != want {
            self.violations.push(format!(
                "{phase}: {qname} answered {} {}, expected records {want}",
                answer.status, answer.records
            ));
        }
        self.forwarded(phase, qname, before, 1);
        if want.as_array().unwrap().iter().any(|r| r[0] == "A") {
            self.last_address = answer.done;
        }
        if want.as_array().unwrap().iter().any(|r| r[0] == "CNAME") {
            self.last_target = answer.done;
        }
        answer
    }

    /// Forwarded, then refused for capacity: SERVFAIL with no answer.
    fn servfail(&mut self, phase: &str, qname: &str) -> Answer {
        let before = self.upstream(qname);
        let answer = self.query(phase, qname);
        if answer.status != "rcode:2" || answer.header.map(|h| h.1) != Some(0) {
            self.violations.push(format!(
                "{phase}: {qname} answered {} {:?}, expected SERVFAIL with no answer",
                answer.status, answer.header
            ));
        }
        self.forwarded(phase, qname, before, 1);
        self.servfailed.push(qname.to_string());
        answer
    }

    /// REFUSED, and forwarded `upstream` times: zero for the name gate, one
    /// for an answer condemned by its address.
    fn refused(&mut self, phase: &str, qname: &str, upstream: usize) -> Answer {
        let before = self.upstream(qname);
        let answer = self.query(phase, qname);
        if answer.status != "rcode:5" {
            self.violations.push(format!(
                "{phase}: {qname} answered {} {}, expected REFUSED",
                answer.status, answer.records
            ));
        }
        self.forwarded(phase, qname, before, upstream);
        if !self.refused.iter().any(|name| name == qname) {
            self.refused.push(qname.to_string());
        }
        answer
    }

    fn forwarded(&mut self, phase: &str, qname: &str, before: usize, want: usize) {
        let after = self.upstream(qname);
        if after - before != want {
            self.violations.push(format!(
                "{phase}: {qname} reached the scripted nameserver {} times, expected {want}",
                after - before
            ));
        }
    }

    /// The kernel's `@resolved`, checked against the cap and the forbidden
    /// addresses. `list` keeps every address in the evidence.
    fn kernel(&mut self, phase: &str, list: bool) -> Vec<host::Element> {
        let at = Instant::now();
        let elements = host::resolved(&self.vmm_name);
        if elements.len() > CAP {
            self.violations.push(format!(
                "{phase}: @resolved held {} elements, over {CAP}",
                elements.len()
            ));
        }
        let leaked: Vec<String> = elements
            .iter()
            .filter(|e| self.forbidden.contains(&e.addr))
            .map(|e| e.addr.to_string())
            .collect();
        if !leaked.is_empty() {
            self.violations.push(format!(
                "{phase}: refused addresses in @resolved at {} ms: {leaked:?}",
                self.ms(at)
            ));
        }
        let addrs: Vec<String> = if list {
            elements.iter().map(|e| e.addr.to_string()).collect()
        } else {
            Vec::new()
        };
        append(
            &self.evidence,
            serde_json::json!({
                "kind": "kernel", "phase": phase, "at_ms": self.ms(at),
                "count": elements.len(), "leaked": leaked, "addrs": addrs,
            }),
        );
        elements
    }

    fn expect_count(&mut self, phase: &str, elements: &[host::Element], want: usize) {
        if elements.len() != want {
            self.violations.push(format!(
                "{phase}: @resolved held {} elements, expected {want}",
                elements.len()
            ));
        }
    }

    fn tcp(&mut self, phase: &str, addr: Ipv4Addr, port: u16) -> String {
        let probe = format!("{phase}-tcp-{addr}");
        let sent = Instant::now();
        let (result, took) = guest::call_timed(
            &mut self.guest,
            &probe,
            "tcp",
            serde_json::json!({"addr": addr.to_string(), "port": port, "timeout": FRESH}),
        );
        let result = result.as_str().unwrap_or("non-string").to_string();
        append(
            &self.evidence,
            serde_json::json!({
                "kind": "tcp", "phase": phase, "addr": addr.to_string(), "port": port,
                "sent_ms": self.ms(sent), "guest_ms": took.as_millis() as u64, "result": result,
            }),
        );
        result
    }

    /// VMM process RSS and the container cgroup's charge, in bytes.
    fn memory(&mut self, phase: &str) -> (u64, u64) {
        let status = run::sudo(&["cat", &format!("/proc/{}/status", self.pid)]);
        let kib = |key: &str| -> u64 {
            status
                .lines()
                .find_map(|line| line.strip_prefix(key))
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("{key} in /proc/{}/status", self.pid))
        };
        let (rss, hwm) = (kib("VmRSS:") * 1024, kib("VmHWM:") * 1024);
        let current: u64 = run::sudo(&["cat", &format!("{}/memory.current", self.cgroup)])
            .trim()
            .parse()
            .expect("memory.current is a number");
        append(
            &self.evidence,
            serde_json::json!({
                "kind": "memory", "phase": phase, "at_ms": self.ms(Instant::now()),
                "vm_rss": rss, "vm_hwm": hwm, "cgroup_current": current,
            }),
        );
        (rss, current)
    }

    fn within_memory(&mut self, what: &str, from: (u64, u64), to: (u64, u64)) {
        let (rss, cgroup) = (to.0.saturating_sub(from.0), to.1.saturating_sub(from.1));
        if rss > RSS_BUDGET || cgroup > CGROUP_BUDGET {
            self.violations.push(format!(
                "memory: {what} grew VmRSS by {rss} and the cgroup by {cgroup} bytes, \
                 over {RSS_BUDGET} / {CGROUP_BUDGET}"
            ));
        }
    }

    /// An at-capacity check means nothing once the earliest entry may have
    /// expired.
    fn still_full(&mut self, what: &str, earliest_sent: Instant) {
        let by = earliest_sent + Duration::from_secs(TTL.into()) - AT_CAPACITY_GUARD;
        if Instant::now() > by {
            self.inconclusive.push(format!(
                "{what} finished at {} ms, after {} ms, when the earliest entry may have expired",
                self.ms(Instant::now()),
                self.ms(by)
            ));
        }
    }
}

/// The refusal lines of the VMM's stderr: names reported, stop lines, bytes.
fn refusal_lines(stderr: &str) -> (Vec<String>, usize, usize) {
    let (mut names, mut stops, mut bytes) = (Vec::new(), 0, 0);
    for line in stderr.lines() {
        if !line.contains("refused DNS name") {
            continue;
        }
        bytes += line.len() + 1;
        let parsed: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("a refusal line is JSON: {line:?}: {e}"));
        if parsed["message"] == "further refused DNS names are not reported" {
            stops += 1;
        } else {
            names.push(parsed["fields"]["name"].as_str().unwrap_or("").to_string());
        }
    }
    (names, stops, bytes)
}

#[test]
fn dns_capacity() {
    let run = run::load();
    let [ns_ip, a_ip, r_ip, c_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let d_ip = run.decoys[0];
    let t0 = Instant::now();
    let evidence = run.run_dir.join("dns-capacity.evidence");
    let _ = std::fs::remove_file(&evidence);

    let nameserver = scripted(&run, ns_ip);
    let (la, lr, lc, ld) = (
        listen(&run, a_ip),
        listen(&run, r_ip),
        listen(&run, c_ip),
        listen(&run, d_ip),
    );
    for (label, listener) in [("C", &lc), ("D", &ld)] {
        let reply = host_exchange(listener.addr);
        baseline(
            reply == "pong",
            format!("the host's control connection to {label} answered {reply:?}"),
        );
    }
    run::eventually("the controls' accepts", Duration::from_secs(2), || {
        (endpoint::accepted(&lc).len() == 1 && endpoint::accepted(&ld).len() == 1).then_some(())
    });

    let first_fill: Vec<Ipv4Addr> = (0..CAP - 1).map(fill).collect();
    let overrun: Vec<Ipv4Addr> = (CAP - 1..CAP - 1 + PER_ADDRESS_ANSWER).map(fill).collect();
    let (o4_new, o5_new) = (
        fill(CAP - 1 + PER_ADDRESS_ANSWER),
        fill(CAP + PER_ADDRESS_ANSWER),
    );
    let address_fill = address_batches("fa", &first_fill);
    let address_refill = address_batches("ra", &first_fill);
    let target_fill = target_batches("ct", "t", CAP);
    let target_refill = target_batches("rc", "r", CAP - 1);
    let over_targets = target_batches("oc2-", "tover2-", PER_TARGET_ANSWER);
    const EDGE: &str = "edge.cnamefill.example";
    const TOVER1: &str = "tover1.cnamefill.example";
    const TOVER3: &str = "tover3.cnamefill.example";

    script_addresses(&nameserver, "a.acmeco.example", &[a_ip]);
    script_addresses(&nameserver, "decoy.acmeco.example", &[d_ip]);
    for (name, addrs) in address_fill.iter().chain(&address_refill) {
        script_addresses(&nameserver, name, addrs);
    }
    script_addresses(&nameserver, "oa1.acmeco.example", &[c_ip]);
    script_addresses(&nameserver, "oa2.acmeco.example", &overrun);
    script_addresses(&nameserver, "oa3.acmeco.example", &[a_ip]);
    script_addresses(&nameserver, "oa4.acmeco.example", &[a_ip, o4_new]);
    script_addresses(&nameserver, "oa5.acmeco.example", &[o5_new]);
    for (name, targets) in target_fill.iter().chain(&target_refill) {
        script_targets(&nameserver, name, targets);
    }
    script_targets(&nameserver, "oc1.acmeco.example", &[TOVER1.to_string()]);
    script_targets(&nameserver, "oc2.acmeco.example", &over_targets[0].1);
    script_targets(&nameserver, "oc3.acmeco.example", &[TOVER3.to_string()]);
    dns::set(
        &nameserver,
        "fresh.acmeco.example",
        vec![dns::Record::Cname(EDGE.to_string(), TTL)],
    );
    script_addresses(&nameserver, EDGE, &[r_ip]);
    append(
        &evidence,
        serde_json::json!({
            "kind": "setup", "nameserver": nameserver.addr.to_string(),
            "A": la.addr.to_string(), "R": lr.addr.to_string(),
            "C": lc.addr.to_string(), "D": ld.addr.to_string(),
            "policy": "capacity.json", "ttl": TTL, "cap": CAP, "fill_net": FILL_NET,
            "first_fill": [first_fill[0].to_string(), first_fill[CAP - 2].to_string()],
            "overrun": [overrun[0].to_string(), overrun[PER_ADDRESS_ANSWER - 1].to_string()],
            "o4_new": o4_new.to_string(), "o5_new": o5_new.to_string(),
            "controls": {"C": endpoint::accepted(&lc), "D": endpoint::accepted(&ld)},
        }),
    );

    let mut spec = run::Spec::probes(&run, "capacity.json");
    spec.flags = upstream(&nameserver);
    let (vmm, guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    // Nothing addressed to the fill range leaves the VMM's namespace, should
    // anything ever try.
    run::nsenter(pid, &["ip", "route", "add", "blackhole", FILL_NET]);
    let route = run::nsenter(pid, &["ip", "route", "show", "exact", FILL_NET]);
    baseline(
        route.trim() == format!("blackhole {FILL_NET}"),
        format!("the fill range's route in the VMM's namespace is {route:?}"),
    );
    let cgroup = format!(
        "/sys/fs/cgroup{}",
        run::inspect(&vmm.name, "{{.State.CgroupPath}}")
    );
    append(
        &evidence,
        serde_json::json!({"kind": "vmm", "name": vmm.name, "pid": pid, "cgroup": cgroup, "route": route.trim()}),
    );

    let mut s = Sequence {
        t0,
        evidence: evidence.clone(),
        guest,
        vmm_name: vmm.name.clone(),
        pid,
        cgroup,
        nameserver,
        queries: 0,
        violations: Vec::new(),
        inconclusive: Vec::new(),
        refused: Vec::new(),
        servfailed: Vec::new(),
        forbidden: [c_ip, d_ip, o4_new, o5_new]
            .into_iter()
            .chain(overrun.iter().copied())
            .collect(),
        last_address: t0,
        last_target: t0,
    };
    let (a_port, r_port, c_port, d_port) = (
        la.addr.port(),
        lr.addr.port(),
        lc.addr.port(),
        ld.addr.port(),
    );

    // An allowed public answer is authorized for its TTL and connects; a
    // forbidden name and the decoy are refused and stay unreachable.
    let initial_answer = s.resolves("initial", "a.acmeco.example", vec![a_record(a_ip)]);
    let held = s.kernel("initial", true);
    baseline(
        s.violations.is_empty()
            && held.len() == 1
            && held[0].addr == a_ip
            && held[0].timeout == Some(TTL.into()),
        format!("after the first answer: {:?}", s.violations),
    );
    let connected = s.tcp("initial", a_ip, a_port);
    baseline(
        connected == "connected",
        format!("a fresh connect to A: {connected}"),
    );
    run::eventually("listener A's accept", Duration::from_secs(2), || {
        (endpoint::accepted(&la).len() == 1).then_some(())
    });
    s.refused("initial", "nope0.forbidden.example", 0);
    s.refused("initial", "decoy.acmeco.example", 1);
    let blocked = s.tcp("initial", d_ip, d_port);
    baseline(
        blocked != "connected" && s.violations.is_empty(),
        format!("the decoy: {blocked}, {:?}", s.violations),
    );
    let m0 = s.memory("initial");

    // FILL-ADDR: with A, exactly CAP addresses, each answer whole.
    let fill_started = Instant::now();
    for (name, addrs) in &address_fill {
        s.resolves(
            "fill-addr",
            name,
            addrs.iter().copied().map(a_record).collect(),
        );
    }
    let full = s.kernel("fill-addr", true);
    s.expect_count("fill-addr", &full, CAP);
    let raw = run::sudo(&[
        "podman",
        "exec",
        &s.vmm_name,
        "nft",
        "list",
        "set",
        "inet",
        "flow_egress",
        "resolved",
    ]);
    append(
        &evidence,
        serde_json::json!({"kind": "raw-set", "phase": "fill-addr", "raw": raw}),
    );
    let missing: Vec<String> = first_fill
        .iter()
        .chain([&a_ip])
        .filter(|addr| !full.iter().any(|e| e.addr == **addr))
        .map(ToString::to_string)
        .collect();
    if !missing.is_empty() {
        s.violations.push(format!(
            "fill-addr: authorized but not in @resolved: {missing:?}"
        ));
    }

    s.servfail("over-addr", "oa1.acmeco.example");
    let blocked = s.tcp("over-addr", c_ip, c_port);
    if blocked == "connected" {
        s.violations
            .push("over-addr: the guest connected to C, whose answer was SERVFAIL".to_string());
    }
    s.servfail("over-addr", "oa2.acmeco.example");
    s.resolves("over-addr", "oa3.acmeco.example", vec![a_record(a_ip)]);
    s.servfail("over-addr", "oa4.acmeco.example");
    let connected = s.tcp("over-addr", a_ip, a_port);
    if connected != "connected" {
        s.violations.push(format!(
            "over-addr: A, held at capacity, answered {connected}"
        ));
    }
    s.refused("over-addr", "decoy.acmeco.example", 1);
    let full = s.kernel("over-addr", false);
    s.expect_count("over-addr", &full, CAP);
    s.still_full("the address overruns", initial_answer.sent);

    // FILL-CNAME: exactly CAP targets, none of which asks anything of the
    // kernel.
    let mut targets_started = None;
    for (name, targets) in &target_fill {
        let answer = s.resolves(
            "fill-cname",
            name,
            targets.iter().map(|t| cname_record(t)).collect(),
        );
        targets_started.get_or_insert(answer.sent);
    }
    let targets_started = targets_started.expect("at least one target answer");
    // Unscripted, so the upstream's NXDOMAIN, forwarded once, is the proof the
    // gate admitted it.
    let first_target = target_fill[0].1[0].clone();
    let before = s.upstream(&first_target);
    let admitted = s.query("fill-cname", &first_target);
    if admitted.status != "rcode:3" {
        s.violations.push(format!(
            "fill-cname: the remembered target {first_target} answered {}, expected the upstream's NXDOMAIN",
            admitted.status
        ));
    }
    s.forwarded("fill-cname", &first_target, before, 1);
    s.servfail("over-cname", "oc1.acmeco.example");
    s.servfail("over-cname", "oc2.acmeco.example");
    // Every target of a SERVFAIL answer, so none was remembered in part.
    s.refused("over-cname", TOVER1, 0);
    for target in &over_targets[0].1 {
        s.refused("over-cname", target, 0);
    }
    let full = s.kernel("over-cname", false);
    s.expect_count("over-cname", &full, CAP);
    s.still_full("the address checks", initial_answer.sent);
    s.still_full("the target overruns", targets_started);
    let m1 = s.memory("at-capacity");
    s.within_memory("filling both caps", m0, m1);
    append(
        &evidence,
        serde_json::json!({"kind": "timing", "fill_started_ms": s.ms(fill_started), "at_capacity_done_ms": s.ms(Instant::now())}),
    );

    // REFUSAL-LOG: more refused names than the log reports.
    for index in 1..=FORBIDDEN {
        s.refused("refusals", &format!("nope{index}.forbidden.example"), 0);
    }

    // EXPIRE: the kernel drops everything by its TTL; the resolver frees the
    // space only `RECLAIM` later.
    let ttl = Duration::from_secs(TTL.into());
    let empty_by = s.last_address + ttl + COLLECTED;
    let refill_at = s.last_address.max(s.last_target) + ttl + RECLAIM + REFILL_MARGIN;
    let mut emptied = None;
    while Instant::now() < refill_at {
        let next = Instant::now() + SAMPLE_EVERY;
        let at = Instant::now();
        let count = s.kernel("expire", false).len();
        if count == 0 {
            emptied.get_or_insert(at);
        } else if at > empty_by {
            s.violations.push(format!(
                "expire: @resolved still held {count} elements at {} ms, after {} ms",
                s.ms(at),
                s.ms(empty_by)
            ));
        }
        std::thread::sleep(next.saturating_duration_since(Instant::now()));
    }
    if emptied.is_none() {
        s.violations
            .push("expire: @resolved never emptied before the refill".to_string());
    }
    if Instant::now() < s.last_address + ttl + EXPIRY_SLACK {
        s.inconclusive
            .push("expire: A's attempt would precede its latest possible expiry".to_string());
    }
    let accepted_a = endpoint::accepted(&la).len();
    let blocked = s.tcp("expire", a_ip, a_port);
    if blocked == "connected" || endpoint::accepted(&la).len() != accepted_a {
        s.violations.push(format!(
            "expire: A, expired, answered {blocked} and listener A went from {accepted_a} to {}",
            endpoint::accepted(&la).len()
        ));
    }
    s.refused("expire", &first_target, 0);
    let m2 = s.memory("expired");

    // REFILL: a fresh name and address resolve and connect, then both caps
    // take exactly CAP again.
    let fresh = s.resolves(
        "refill",
        "fresh.acmeco.example",
        vec![cname_record(EDGE), a_record(r_ip)],
    );
    s.resolves("refill", EDGE, vec![a_record(r_ip)]);
    let connected = s.tcp("refill", r_ip, r_port);
    if connected != "connected" || endpoint::accepted(&lr).len() != 1 {
        s.violations.push(format!(
            "refill: R answered {connected}, listener R accepted {:?}",
            endpoint::accepted(&lr)
        ));
    }
    for (name, addrs) in &address_refill {
        s.resolves(
            "refill-addr",
            name,
            addrs.iter().copied().map(a_record).collect(),
        );
    }
    let full = s.kernel("refill-addr", false);
    s.expect_count("refill-addr", &full, CAP);
    s.servfail("refill-addr", "oa5.acmeco.example");
    for (name, targets) in &target_refill {
        s.resolves(
            "refill-cname",
            name,
            targets.iter().map(|t| cname_record(t)).collect(),
        );
    }
    s.servfail("refill-cname", "oc3.acmeco.example");
    s.refused("refill-cname", TOVER3, 0);
    s.refused("refill", "decoy.acmeco.example", 1);
    let blocked = s.tcp("refill", d_ip, d_port);
    if blocked == "connected" {
        s.violations
            .push("refill: the guest connected to the decoy".to_string());
    }
    let full = s.kernel("refill", false);
    s.expect_count("refill", &full, CAP);
    // R and `EDGE` are the refill's earliest entries.
    s.still_full("the refill checks", fresh.sent);
    let m3 = s.memory("refilled");
    s.within_memory("the whole sequence", m0, m3);

    // Listener-side: neither refused destination ever saw the guest.
    for (label, listener) in [("C", &lc), ("D", &ld)] {
        let accepted = endpoint::accepted(listener);
        if accepted.len() != 1 {
            s.violations.push(format!(
                "{label} accepted {accepted:?}, beyond the host's one control connection"
            ));
        }
    }
    let running = run::inspect(&vmm.name, "{{.State.Running}}");
    if running.trim() != "true" {
        s.violations
            .push(format!("the VMM is no longer running: {running}"));
    }
    if t0.elapsed() > SEQUENCE {
        s.violations.push(format!(
            "bound: the sequence took {:?}, over {SEQUENCE:?}",
            t0.elapsed()
        ));
    }

    // The refusal log reports the first refused names, then says it stopped,
    // and never mentions a capacity SERVFAIL.
    let stderr = run::stderr_text(&vmm);
    let (reported, stops, bytes) = refusal_lines(&stderr);
    let expected: Vec<String> = s.refused.iter().take(REPORTED_REFUSALS).cloned().collect();
    if reported != expected || stops != 1 || bytes > (REPORTED_REFUSALS + 1) * REFUSAL_LINE {
        s.violations.push(format!(
            "refusal log: reported {reported:?} with {stops} stop lines in {bytes} bytes, \
             expected {expected:?} with one"
        ));
    }
    let servfail_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| {
            line.to_ascii_lowercase().contains("servfail")
                || s.servfailed.iter().any(|name| line.contains(name.as_str()))
        })
        .collect();
    if !servfail_lines.is_empty() {
        s.violations.push(format!(
            "refusal log: capacity SERVFAILs were logged: {servfail_lines:?}"
        ));
    }

    append(
        &evidence,
        serde_json::json!({
            "kind": "summary",
            "queries": s.queries,
            "upstream_queries": dns::queries(&s.nameserver).len(),
            "refused_distinct": s.refused.len(),
            "servfailed": s.servfailed,
            "reported": reported, "stop_lines": stops, "refusal_bytes": bytes,
            "stderr_bytes": stderr.len(),
            "emptied_ms": emptied.map(|at| s.ms(at)),
            "empty_by_ms": s.ms(empty_by), "refill_at_ms": s.ms(refill_at),
            "memory": {"initial": m0, "at_capacity": m1, "expired": m2, "refilled": m3},
            "listeners": {
                "A": endpoint::accepted(&la), "R": endpoint::accepted(&lr),
                "C": endpoint::accepted(&lc), "D": endpoint::accepted(&ld),
            },
            "elapsed_ms": t0.elapsed().as_millis() as u64,
            "violations": s.violations, "inconclusive": s.inconclusive,
        }),
    );
    assert!(
        s.inconclusive.is_empty(),
        "INCONCLUSIVE: {:#?}",
        s.inconclusive
    );
    assert!(
        s.violations.is_empty(),
        "{} violations:\n{}",
        s.violations.len(),
        s.violations.join("\n")
    );
}
