//! C17: the resolver's two failures, which must end differently.
//!
//! `kernel_update_failure` removes `@resolved`, and the one rule that reads
//! it, from a running VMM's own network namespace, then asks for a name whose
//! answer must be inserted. The insert fails, and the resolver must abort the
//! VMM without answering: an answer whose authorization never reached the
//! kernel is a promise the ruleset cannot keep. Removing the rule only takes
//! an accept away, so the namespace's default-deny stays in place, and the
//! namespace is shown to be the VMM's and not the host's before anything in
//! it is touched. A fresh launch afterwards must authorize the same name.
//!
//! `upstream_timeout` silences one name at the scripted nameserver instead.
//! The resolver gives up on it after `UPSTREAM_TIMEOUT` and drops that one
//! query, and that is all: the VMM, its held connection and its other names
//! keep working, and once the nameserver answers again the name is authorized
//! and reachable.
//!
//! Each test writes `c17-<test>.evidence` to the run directory as it goes,
//! then fails naming every violated bound; a baseline that does not hold fails
//! at once, as inconclusive.

use super::c07::append;
use super::c15::{baseline, host_exchange};
use super::*;
use std::path::{Path, PathBuf};

const POLICY: &str = "ttl.json";
const NAME: &str = "name.acmeco.example";
/// Never asked before the fault, so its answer needs an insert.
const FRESH: &str = "fresh.acmeco.example";
const SLOW: &str = "slow.acmeco.example";
/// The most `ttl.json` leaves unclamped.
const TTL: u32 = 10;
const CONNECT: f64 = 1.0;
/// From the trigger to the VMM's exit.
const FATAL: Duration = Duration::from_secs(30);
/// `UPSTREAM_TIMEOUT` in the resolver, and the window its drop must land in.
const DROPPED_AFTER: (Duration, Duration) = (Duration::from_millis(4_500), Duration::from_secs(7));
/// Longer than the resolver waits, so the guest sees whether it answered.
const GUEST_WAIT: Duration = Duration::from_secs(8);
const GUEST_SLACK: Duration = Duration::from_secs(2);
const SILENT_CAP: Duration = Duration::from_secs(30);

fn evidence_file(run: &run::Run, test: &str) -> PathBuf {
    let path = run.run_dir.join(format!("c17-{test}.evidence"));
    let _ = std::fs::remove_file(&path);
    path
}

fn records(ip: Ipv4Addr) -> serde_json::Value {
    serde_json::json!([["A", ip.to_string(), TTL]])
}

/// A raw `A` query of the VMM's resolver, and how long the host waited.
fn ask(
    guest: &mut guest::Guest,
    probe: &str,
    qname: &str,
    timeout: f64,
) -> (serde_json::Value, Duration) {
    let sent = Instant::now();
    let (result, _) = guest::call_timed(
        guest,
        probe,
        "query",
        serde_json::json!({
            "nameserver": VMM_TAP.to_string(), "name": qname, "qtype": 1,
            "timeout": timeout, "raw": true,
        }),
    );
    (result, sent.elapsed())
}

/// `/proc/<pid>/stat`'s starttime, which with the pid names one process.
fn starttime(pid: u32) -> String {
    let stat = run::sudo(&["cat", &format!("/proc/{pid}/stat")]);
    // The command name may hold spaces; field 3 follows its closing paren.
    let rest = &stat[stat.rfind(')').expect("a stat line") + 2..];
    rest.split_whitespace()
        .nth(22 - 3)
        .expect("a starttime field")
        .to_string()
}

fn netns_link(proc_dir: &str) -> String {
    run::sudo(&["readlink", &format!("{proc_dir}/ns/net")])
        .trim()
        .to_string()
}

/// `nft` in the network namespace of `pid`, entered the same way for every
/// read and for the fault.
fn nft_in(pid: u32, args: &[&str], stdin: Option<&[u8]>) -> std::process::Output {
    let pid = pid.to_string();
    let mut argv = vec!["nsenter", "-t", &pid, "-n", "nft"];
    argv.extend_from_slice(args);
    run::sudo_output(&argv, stdin)
}

fn text(output: &std::process::Output) -> serde_json::Value {
    serde_json::json!({
        "code": output.status.code(),
        "stdout": String::from_utf8_lossy(&output.stdout),
        "stderr": String::from_utf8_lossy(&output.stderr),
    })
}

/// The host's boundary tables, without counters, which the fault must leave
/// byte-identical.
fn host_boundary() -> String {
    let tables = run::sudo(&["nft", "list", "tables"]);
    let mut out = String::new();
    for line in tables
        .lines()
        .filter(|line| line.ends_with(" flow_vmm_boundary"))
    {
        let family = line
            .split_whitespace()
            .nth(1)
            .expect("table <family> <name>");
        out.push_str(&run::sudo(&[
            "nft",
            "-s",
            "list",
            "table",
            family,
            "flow_vmm_boundary",
        ]));
    }
    out
}

fn decided(stderr: &str, qname: &str) -> Vec<String> {
    let needle = format!("name={qname} ");
    stderr
        .lines()
        .filter(|line| line.contains(&needle))
        .map(str::to_string)
        .collect()
}

struct Setup {
    run: run::Run,
    evidence: PathBuf,
    nameserver: dns::Nameserver,
    a: endpoint::Endpoint,
    a_ip: Ipv4Addr,
    silent: endpoint::Endpoint,
    silent_ip: Ipv4Addr,
}

/// Listeners on A and on the one that must stay silent until the end, whose
/// host-side control connection shows it would see the guest.
fn setup(test: &str, silent_name: &str) -> Setup {
    let run = run::load();
    let [ns_ip, a_ip, b_ip, c_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let silent_ip = if silent_name == FRESH { b_ip } else { c_ip };
    let evidence = evidence_file(&run, test);
    let nameserver = scripted(&run, ns_ip);
    dns::set(&nameserver, NAME, vec![dns::Record::A(a_ip, TTL)]);
    dns::set(
        &nameserver,
        silent_name,
        vec![dns::Record::A(silent_ip, TTL)],
    );
    let a = listen(&run, a_ip);
    let silent = listen(&run, silent_ip);

    let reply = host_exchange(silent.addr);
    baseline(
        reply == "pong",
        format!("the host's control connection answered {reply:?}"),
    );
    append(
        &evidence,
        serde_json::json!({
            "kind": "setup", "nameserver": nameserver.addr.to_string(),
            "A": a.addr.to_string(), "silent": silent.addr.to_string(),
            "silent_name": silent_name, "policy": POLICY, "ttl": TTL,
            "control": reply,
        }),
    );
    Setup {
        run,
        evidence,
        nameserver,
        a,
        a_ip,
        silent,
        silent_ip,
    }
}

fn launch(s: &Setup) -> (run::Vmm, guest::Guest) {
    let mut spec = run::Spec::probes(&s.run, POLICY);
    spec.flags = upstream(&s.nameserver);
    spec.flags.push("--debug".to_string());
    start(&s.run, spec)
}

/// B0: `NAME` is authorized and reachable, a connection to A is held, and the
/// silent listener is not reachable before any answer names it.
fn b0(s: &Setup, vmm: &run::Vmm, guest: &mut guest::Guest) -> (u32, String) {
    let pid = run::pid(vmm);
    let born = starttime(pid);
    let (first, took) = ask(guest, "b0-query", NAME, BLOCKED);
    baseline(
        first["status"] == "ok" && first["records"] == records(s.a_ip),
        format!("{NAME} answered {first}"),
    );
    let held = host::resolved(&vmm.name);
    baseline(
        held.iter()
            .any(|e| e.addr == s.a_ip && e.timeout == Some(u64::from(TTL))),
        format!(
            "@resolved after B0: {:?}",
            held.iter().map(|e| (e.addr, e.timeout)).collect::<Vec<_>>()
        ),
    );
    let hold = probe(
        guest,
        "b0-hold",
        "hold",
        serde_json::json!({"id": "held", "addr": s.a_ip.to_string(), "port": s.a.addr.port(), "timeout": CONNECT}),
    );
    baseline(hold == "connected", format!("holding A: {hold}"));
    let exchange = probe(
        guest,
        "b0-exchange",
        "exchange",
        serde_json::json!({"id": "held", "timeout": 2}),
    );
    baseline(exchange == "pong", format!("the held exchange: {exchange}"));
    let fresh = tcp(guest, "b0-fresh", s.a_ip, s.a.addr.port(), CONNECT);
    baseline(
        fresh == "connected",
        format!("a fresh connect to A: {fresh}"),
    );
    let early = tcp(
        guest,
        "b0-silent",
        s.silent_ip,
        s.silent.addr.port(),
        BLOCKED,
    );
    baseline(
        early == "timeout",
        format!("the silent listener before any answer: {early}"),
    );
    let silent = endpoint::accepted(&s.silent).len();
    baseline(
        silent == 1,
        format!("the silent listener accepted {silent}, not 1"),
    );
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "b0", "vmm": vmm.name, "pid": pid, "starttime": born,
            "query": first, "query_ms": took.as_millis() as u64,
            "hold": hold, "exchange": exchange, "fresh": fresh, "early": early,
            "a_accepts": endpoint::accepted(&s.a).len(), "silent_accepts": silent,
        }),
    );
    (pid, born)
}

#[test]
fn kernel_update_failure() {
    let s = setup("kernel_update_failure", FRESH);
    let (mut vmm, mut guest) = launch(&s);
    let (pid, born) = b0(&s, &vmm, &mut guest);
    let mut violations = Vec::new();

    // The namespace the fault enters must be the VMM's, and not the host's.
    let host_ns = netns_link("/proc/1");
    let own_ns = netns_link("/proc/self");
    let vmm_ns = netns_link(&format!("/proc/{pid}"));
    let entered = run::sudo(&[
        "nsenter",
        "-t",
        &pid.to_string(),
        "-n",
        "readlink",
        "/proc/self/ns/net",
    ])
    .trim()
    .to_string();
    let tables = nft_in(pid, &["list", "tables"], None);
    let table = nft_in(pid, &["-a", "list", "table", "inet", "flow_egress"], None);
    let table_text = String::from_utf8_lossy(&table.stdout).to_string();
    let referencing: Vec<&str> = table_text
        .lines()
        .filter(|line| line.contains("@resolved"))
        .collect();
    let boundary_before = host_boundary();
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "isolation", "host_ns": host_ns, "test_ns": own_ns, "vmm_ns": vmm_ns,
            "entered_ns": entered, "tables": text(&tables), "table": text(&table),
            "referencing": referencing, "boundary": boundary_before,
        }),
    );
    baseline(
        entered == vmm_ns && vmm_ns != host_ns && vmm_ns != own_ns,
        format!("entered {entered}, VMM {vmm_ns}, host {host_ns}, test {own_ns}"),
    );
    let tables_text = String::from_utf8_lossy(&tables.stdout).to_string();
    baseline(
        tables.status.success()
            && tables_text
                .lines()
                .any(|line| line == "table inet flow_egress")
            && !tables_text.contains("flow_vmm_boundary"),
        format!("the VMM namespace's tables: {}", text(&tables)),
    );
    baseline(
        !boundary_before.is_empty(),
        "the host boundary table is installed",
    );
    baseline(
        referencing.len() == 1 && table_text.contains("chain egress_accept {"),
        format!("rules reading @resolved: {referencing:?}"),
    );
    let handle = referencing[0]
        .rsplit_once("# handle ")
        .and_then(|(_, handle)| handle.trim().parse::<u64>().ok())
        .unwrap_or_else(|| panic!("INCONCLUSIVE: no handle in {:?}", referencing[0]));
    // The one rule must be egress_accept's: the line before it, up the
    // listing, that opens a chain.
    let chain = table_text
        .lines()
        .take_while(|line| !line.contains("@resolved"))
        .filter(|line| line.trim_start().starts_with("chain "))
        .last()
        .unwrap_or_default()
        .to_string();
    baseline(
        chain.contains("chain egress_accept"),
        format!("@resolved is read in {chain:?}"),
    );

    // The fault: one transaction, so the set is never gone while the rule
    // still reads it, and nothing outside these two objects changes.
    let batch = format!(
        "delete rule inet flow_egress egress_accept handle {handle}\n\
         delete set inet flow_egress resolved\n"
    );
    let fault = nft_in(pid, &["-f", "-"], Some(batch.as_bytes()));
    let gone = nft_in(
        pid,
        &["list", "set", "inet", "flow_egress", "resolved"],
        None,
    );
    let after = nft_in(pid, &["-a", "list", "table", "inet", "flow_egress"], None);
    let after_text = String::from_utf8_lossy(&after.stdout).to_string();
    let boundary_after = host_boundary();
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "fault", "batch": batch, "fault": text(&fault), "set_listing": text(&gone),
            "table_after": text(&after), "boundary_unchanged": boundary_after == boundary_before,
        }),
    );
    baseline(
        fault.status.success(),
        format!("the fault batch: {}", text(&fault)),
    );
    baseline(
        !gone.status.success(),
        format!("@resolved still lists: {}", text(&gone)),
    );
    baseline(
        after.status.success()
            && !after_text.contains("@resolved")
            && after_text.contains("type filter hook forward priority filter; policy drop;"),
        format!("flow_egress after the fault: {after_text}"),
    );
    if boundary_after != boundary_before {
        violations.push(format!(
            "the host boundary changed:\n{boundary_before}\n---\n{boundary_after}"
        ));
    }
    let silent_before = endpoint::accepted(&s.silent).len();

    // The trigger. The guest's VM dies with the VMM, so the call either
    // fails on a closed channel or, a violation, answers.
    let sent = Instant::now();
    let (call, status, exited) = std::thread::scope(|scope| {
        let call = scope.spawn(|| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ask(&mut guest, "nft-trigger", FRESH, 5.0)
            }))
            .map_err(|payload| {
                payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
                    .unwrap_or_default()
            })
        });
        let status = run::wait_exit(&mut vmm, FATAL);
        let exited = sent.elapsed();
        (call.join().expect("the trigger thread"), status, exited)
    });
    let stderr = run::stderr_text(&vmm);
    let fatal_lines: Vec<&str> = stderr
        .lines()
        .filter(|line| line.contains("of the batch failed"))
        .collect();
    let fresh_decisions = decided(&stderr, FRESH);
    let b0_decisions = decided(&stderr, NAME);
    let upstream_fresh = dns::queries(&s.nameserver)
        .iter()
        .filter(|q| *q == FRESH)
        .count();
    let silent_after = endpoint::accepted(&s.silent).len();
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "trigger", "exit_ms": exited.as_millis() as u64,
            "exit_code": status.code(), "exit_signal": std::os::unix::process::ExitStatusExt::signal(&status),
            "exit_success": status.success(),
            "guest": match &call { Ok((result, took)) => serde_json::json!({"result": result, "ms": took.as_millis() as u64}), Err(panic) => serde_json::json!({"panic": panic}) },
            "fatal_lines": fatal_lines, "fresh_decisions": fresh_decisions, "b0_decisions": b0_decisions,
            "upstream_fresh": upstream_fresh, "upstream": dns::queries(&s.nameserver),
            "silent_accepts": [silent_before, silent_after], "pid": pid, "starttime": born,
        }),
    );
    if exited > FATAL || status.success() {
        violations.push(format!("N1: exit after {exited:?}, status {status:?}"));
    }
    if upstream_fresh != 1 {
        violations.push(format!(
            "N2: {FRESH} reached the nameserver {upstream_fresh} times"
        ));
    }
    if fatal_lines.is_empty() {
        violations.push("N3: no fatal set-update line on stderr".to_string());
    }
    if b0_decisions.is_empty() {
        violations.push("N3: INCONCLUSIVE: no --debug decision line even for B0".to_string());
    }
    if !fresh_decisions.is_empty() {
        violations.push(format!(
            "N3: a decision was made for {FRESH}: {fresh_decisions:?}"
        ));
    }
    if let Ok((result, _)) = &call {
        violations.push(format!("N4: the guest got an answer to {FRESH}: {result}"));
    }
    if silent_after != 1 {
        violations.push(format!(
            "N5: listener B accepted {silent_after}, not only the host's 1"
        ));
    }
    drop(guest);
    drop(vmm);

    // N6: a fresh launch authorizes the same name and reaches B.
    let (vmm, mut guest) = launch(&s);
    let (again, took) = ask(&mut guest, "n6-query", FRESH, BLOCKED);
    let held = host::resolved(&vmm.name);
    let connect = tcp(
        &mut guest,
        "n6-connect",
        s.silent_ip,
        s.silent.addr.port(),
        CONNECT,
    );
    let silent_final = endpoint::accepted(&s.silent).len();
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "relaunch", "vmm": vmm.name, "pid": run::pid(&vmm),
            "query": again, "query_ms": took.as_millis() as u64,
            "resolved": held.iter().map(|e| serde_json::json!([e.addr.to_string(), e.timeout])).collect::<Vec<_>>(),
            "connect": connect, "silent_accepts": silent_final,
        }),
    );
    if again["status"] != "ok" || again["records"] != records(s.silent_ip) {
        violations.push(format!("N6: {FRESH} after relaunch answered {again}"));
    }
    if !held
        .iter()
        .any(|e| e.addr == s.silent_ip && e.timeout == Some(u64::from(TTL)))
    {
        violations.push("N6: @resolved lacks B after relaunch".to_string());
    }
    if connect != "connected" || silent_final != 2 {
        violations.push(format!("N6: connect {connect}, B accepted {silent_final}"));
    }

    finish(&s.evidence, violations);
}

#[test]
fn upstream_timeout() {
    let s = setup("upstream_timeout", SLOW);
    let (vmm, mut guest) = launch(&s);
    let (pid, born) = b0(&s, &vmm, &mut guest);
    let mut violations = Vec::new();

    let silenced = Instant::now();
    dns::silence(&s.nameserver, SLOW, true);
    let sent = Instant::now();
    let (result, wall, seen, samples) = std::thread::scope(|scope| {
        let call = scope.spawn(|| ask(&mut guest, "u-slow", SLOW, GUEST_WAIT.as_secs_f64()));
        let (mut seen, mut samples) = (None, Vec::new());
        let mut next_sample = Instant::now();
        while !call.is_finished() {
            if seen.is_none() && !decided(&run::stderr_text(&vmm), SLOW).is_empty() {
                seen = Some(sent.elapsed());
            }
            if Instant::now() >= next_sample {
                next_sample += Duration::from_secs(1);
                let held = host::resolved(&vmm.name);
                samples.push((
                    sent.elapsed(),
                    held.iter().map(|e| e.addr).collect::<Vec<_>>(),
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let (result, wall) = call.join().expect("the guest call");
        (result, wall, seen, samples)
    });
    let stderr = run::stderr_text(&vmm);
    let decisions = decided(&stderr, SLOW);
    let suppressed = dns::suppressed(&s.nameserver)
        .iter()
        .filter(|q| *q == SLOW)
        .count();
    let leaked = samples
        .iter()
        .any(|(_, addrs)| addrs.contains(&s.silent_ip));
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "fault", "result": result, "wall_ms": wall.as_millis() as u64,
            "decision_seen_ms": seen.map(|seen| seen.as_millis() as u64), "decisions": decisions,
            "suppressed": suppressed, "upstream": dns::queries(&s.nameserver),
            "fatal": stderr.contains("of the batch failed"),
            "samples": samples.iter().map(|(at, addrs)| serde_json::json!({"at_ms": at.as_millis() as u64, "addrs": addrs.iter().map(ToString::to_string).collect::<Vec<_>>()})).collect::<Vec<_>>(),
        }),
    );
    if result != serde_json::json!({"status": "timeout"})
        || wall < GUEST_WAIT
        || wall > GUEST_WAIT + GUEST_SLACK
    {
        violations.push(format!("U1: {result} after {wall:?}"));
    }
    if suppressed != 1 {
        violations.push(format!("U2: {SLOW} was suppressed {suppressed} times"));
    }
    let dropped = decisions.len() == 1
        && decisions[0].contains(&format!(
            "outcome=dropped=no answer from {}",
            s.nameserver.addr
        ));
    if !dropped || !seen.is_some_and(|seen| DROPPED_AFTER.0 <= seen && seen <= DROPPED_AFTER.1) {
        violations.push(format!(
            "U3: decisions {decisions:?} first seen after {seen:?}"
        ));
    }
    if stderr.contains("of the batch failed") {
        violations.push("U3: a fatal line on stderr".to_string());
    }
    if leaked {
        violations.push("U4: @resolved held C during the fault".to_string());
    }

    // U5: the same process, still running, still serving.
    let pid_after = run::pid(&vmm);
    let born_after = starttime(pid_after);
    let running = run::inspect(&vmm.name, "{{.State.Running}}");
    let exchange = probe(
        &mut guest,
        "u5-exchange",
        "exchange",
        serde_json::json!({"id": "held", "timeout": 2}),
    );
    let (other, _) = ask(&mut guest, "u5-query", NAME, BLOCKED);
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "alive", "pid": pid_after, "starttime": born_after, "running": running,
            "exchange": exchange, "query": other,
        }),
    );
    if (pid_after, &born_after) != (pid, &born) || running != "true" {
        violations.push(format!(
            "U5: pid {pid}/{born} -> {pid_after}/{born_after}, running {running}"
        ));
    }
    if exchange != "pong" || other["status"] != "ok" || other["records"] != records(s.a_ip) {
        violations.push(format!("U5: exchange {exchange}, {NAME} answered {other}"));
    }

    // U6: the nameserver answers again, and the name works end to end.
    dns::silence(&s.nameserver, SLOW, false);
    let silent_for = silenced.elapsed();
    let (restored, took) = ask(&mut guest, "u6-query", SLOW, BLOCKED);
    let held = host::resolved(&vmm.name);
    let connect = tcp(
        &mut guest,
        "u6-connect",
        s.silent_ip,
        s.silent.addr.port(),
        CONNECT,
    );
    let silent_final = endpoint::accepted(&s.silent).len();
    let answered = dns::queries(&s.nameserver)
        .iter()
        .filter(|q| *q == SLOW)
        .count();
    let running = run::inspect(&vmm.name, "{{.State.Running}}");
    append(
        &s.evidence,
        serde_json::json!({
            "kind": "restored", "silent_ms": silent_for.as_millis() as u64,
            "query": restored, "query_ms": took.as_millis() as u64,
            "resolved": held.iter().map(|e| serde_json::json!([e.addr.to_string(), e.timeout])).collect::<Vec<_>>(),
            "connect": connect, "silent_accepts": silent_final, "answered": answered,
            "running": running, "decisions": decided(&run::stderr_text(&vmm), SLOW),
        }),
    );
    if restored["status"] != "ok" || restored["records"] != records(s.silent_ip) {
        violations.push(format!("U6: {SLOW} after restore answered {restored}"));
    }
    if !held
        .iter()
        .any(|e| e.addr == s.silent_ip && e.timeout == Some(u64::from(TTL)))
    {
        violations.push("U6: @resolved lacks C after restore".to_string());
    }
    if connect != "connected" || silent_final != 2 || answered != 1 {
        violations.push(format!(
            "U6: connect {connect}, C accepted {silent_final}, answered {answered}"
        ));
    }
    if silent_for > SILENT_CAP || running != "true" {
        violations.push(format!("U7: silent for {silent_for:?}, running {running}"));
    }

    finish(&s.evidence, violations);
}

fn finish(evidence: &Path, violations: Vec<String>) {
    append(
        evidence,
        serde_json::json!({"kind": "verdict", "violations": violations}),
    );
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}
