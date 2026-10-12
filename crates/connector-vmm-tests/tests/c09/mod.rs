//! C09: a guest floods its console with long, partial, JSON-looking and
//! readiness-looking lines on stderr, and bulk on stdout, before
//! connector-init starts (`--as-root-exec`, which guest-init waits out),
//! while its session's own consumer reads slowly or stops reading. The path
//! under test is the launcher's: the `start --attach` client's stderr, the
//! connector crate's stderr pump, and the session's bounded response channel.
//! Bytes must reach the consumer or hold the guest back, host memory must
//! stay within what that channel explains, nothing before connector-init's
//! health may pass for readiness, and a cancelled session must release its
//! VMM. Each test writes `c09.evidence` and fails at the end naming every
//! violated bound, or as inconclusive when the flood is unproven.

use super::c07::append;
use super::*;
use connector_vmm_tests::launcher;
use proto_flow::{connector as proto, derive, flow, ops};

/// The launcher tests' bounds: a launch pulls its image; a release ends it.
const STARTED: Duration = Duration::from_secs(180);
const RELEASED: Duration = Duration::from_secs(60);
/// The offender's container allowance, of which 512 MiB is guest RAM.
const OFFENDER: u64 = 768 * MIB;
/// How long the flood script outlives its done line. connector-init starts
/// only after it exits, so a Started sooner after that line was written was
/// declared by something other than its health.
const HOLD: f64 = 8.0;
/// Host memory growth the response channel explains: 16 queued messages and
/// one in the pump, each at most a 1 MiB line, with slack for the runtime.
const GROWTH: u64 = 48 * MIB;
/// Stderr the guest wrote which the consumer has not received.
const IN_FLIGHT: u64 = 24 * MIB;
/// Growth of the guest's writes, as its VMM's and conmon's counters see it,
/// over the last `PLATEAU` of a stopped reader's window: a blocked guest.
const STALLED: u64 = 4 * MIB;
const PLATEAU: Duration = Duration::from_secs(8);
/// How long a stopped reader stays stopped before it cancels.
const WINDOW: Duration = Duration::from_secs(12);
/// Fewest complete samples (this process, the client, conmon and the
/// container's cgroup) over a fault interval for its memory bounds to count.
const MEASURED: usize = 4;
/// A slow reader's guest must take at least this share of the time the
/// consumer took to receive its flood: backpressure, not buffering, paced it.
const TRACKING: f64 = 0.5;

/// Run as guest root before connector-init. Every flood line carries
/// `seq=`, its guest time `t=` and the stderr bytes written before it `b=`.
/// Kinds cycle: a 1 MiB raw line (newline included), 256 KiB JSON-looking
/// lines, lines written in two parts, and short JSON logs; 64 KiB of stdout
/// every fourth line. It stops at whichever cap comes first, writes `done`,
/// and sleeps `hold` seconds.
const FLOOD: &str = r#"
import json, os, time
P = json.loads(PARAMS)
MIB = 1 << 20
written = 0
def w(fd, data):
    global written
    if fd == 2:
        written += len(data)
    while data:
        data = data[os.write(fd, data):]
t0 = time.monotonic()
def mark(kind, seq):
    return "c09 %s seq=%d t=%.4f b=%d" % (kind, seq, time.monotonic() - t0, written)
w(2, b" \n")
w(2, b"  SERVING\n")
w(2, b'{"level":"info","message":"started connector container"}\n')
w(2, b"connector-vmm-tests: held before connector-init\n")
w(2, b'{"level":"info","message":"c09 forged')
time.sleep(0.05)
w(2, b' partial"}\n')
seq, stdout = 0, 0
while seq < P["lines"] and written < P["stderr"] and time.monotonic() - t0 < P["seconds"]:
    if seq % 32 == 31:
        head = (mark("raw", seq) + " ").encode()
        w(2, head + b"r" * (MIB - len(head) - 1) + b"\n")
    elif seq % 8 == 7:
        line = {"level": "info", "message": mark("long", seq), "fields": {"pad": "p" * (256 << 10)}}
        w(2, json.dumps(line).encode() + b"\n")
    elif seq % 16 == 3:
        line = json.dumps({"level": "info", "message": mark("partial", seq)}).encode() + b"\n"
        w(2, line[:20])
        w(2, line[20:])
    else:
        w(2, json.dumps({"level": "info", "message": mark("json", seq)}).encode() + b"\n")
    if seq % 4 == 0 and stdout < P["stdout"]:
        chunk = (("c09 stdout seq=%d " % seq) + "o" * 1000 + "\n").encode() * 64
        w(1, chunk)
        stdout += len(chunk)
    seq += 1
done = {"level": "info", "message": mark("done", seq), "fields": {"lines": seq, "stdout": stdout}}
w(2, json.dumps(done).encode() + b"\n")
time.sleep(P["hold"])
"#;

#[derive(serde::Serialize)]
struct Flood {
    lines: u64,
    stderr: u64,
    stdout: u64,
    seconds: f64,
    hold: f64,
}

/// Which of a test's launches it is, by its own launcher root.
fn router(
    run: &run::Run,
    root: &launcher::Root,
    state_dir: &str,
    memory: u64,
    cpus: &str,
) -> connector::ServiceRouter {
    let vmm = connector::Vmm {
        image: run.vmm_image.clone(),
        podman: root.podman.clone(),
        state_dir: state_dir.to_string(),
        disk_mib: 2048,
        memory_bytes: memory,
        cpu_limit: cpus.to_string(),
        guest_memory_mib: 512,
        vcpus: 1,
        cgroup_parent: None,
    };
    let (_service, router) = connector::Service::new_local(
        String::new(),
        Some(vmm),
        service_kit::Registry::new(),
        std::sync::Arc::new(flow_client_next::secret_resolver::NoOp),
    );
    router
}

fn flood_flags(root: &launcher::Root, flood: &Flood) {
    use base64::Engine;
    let params = serde_json::to_string(flood).expect("params serialize");
    let script = FLOOD.replace(
        "PARAMS",
        &serde_json::to_string(&params).expect("a string serializes"),
    );
    let encoded = base64::engine::general_purpose::STANDARD.encode(script);
    launcher::vmm_flags(
        root,
        &[
            "--as-root-exec".to_string(),
            format!(
                "{} -c \"import base64; exec(base64.b64decode('{encoded}'))\"",
                run::PROBES[0]
            ),
        ],
    );
}

fn spec(image: &str) -> proto::Request {
    proto::Request {
        start: Some(proto::request::Start {
            execution: Some(flow::ConnectorExecution {
                vmm: true,
                egress: None,
            }),
            log_level: ops::log::Level::Info as i32,
            ..Default::default()
        }),
        kind: Some(proto::request::Kind::Derive(derive::Request {
            kind: Some(derive::request::Kind::Spec(derive::request::Spec {
                connector_type: flow::collection_spec::derivation::ConnectorType::Image as i32,
                config_json: serde_json::json!({"image": image, "config": {}})
                    .to_string()
                    .into(),
            })),
            ..Default::default()
        })),
    }
}

fn valid_spec(kind: &proto::response::Kind) -> bool {
    matches!(
        kind,
        proto::response::Kind::Derive(derive::Response {
            kind: Some(derive::response::Kind::Spec(spec)),
            ..
        }) if spec.protocol == 3032023 && !spec.config_schema_json.is_empty()
    )
}

/// Open a session as a client which reads its responses itself.
fn open(
    runtime: &tokio::runtime::Runtime,
    router: &connector::ServiceRouter,
    request: proto::Request,
) -> (
    tokio::sync::mpsc::Sender<proto::Request>,
    tokio::sync::mpsc::Receiver<tonic::Result<proto::Response>>,
) {
    use proto_grpc::connector::Router;

    let _entered = runtime.enter();
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(proto_grpc::CHANNEL_BUFFER);
    request_tx.try_send(request).expect("the channel is empty");
    let response_rx = router.open(
        ops::TaskType::Derivation,
        proto_grpc::connector::SPEC_TASK_NAME,
        request_rx,
    );
    (request_tx, response_rx)
}

/// One flood mark: its sequence, guest seconds and stderr bytes before it.
type Mark = (u64, f64, u64);

/// Each mark in `text`. A raw line may carry the lines folded into it.
fn marks(text: &str) -> Vec<Mark> {
    let digits = |word: &str, key: &str| -> Option<String> {
        Some(
            word.strip_prefix(key)?
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '.')
                .collect(),
        )
    };
    text.match_indices("seq=")
        .filter_map(|(at, _)| {
            let mut words = text[at..].split_whitespace();
            let seq = digits(words.next()?, "seq=")?.parse().ok()?;
            let t = digits(words.next()?, "t=")?.parse().ok()?;
            let b = digits(words.next()?, "b=")?.parse().ok()?;
            Some((seq, t, b))
        })
        .collect()
}

/// What the consumer received, without the bodies.
#[derive(Default)]
struct Received {
    /// Per log: when, its bytes, its marks, and the start of its message.
    logs: Vec<(Duration, u64, Vec<Mark>, String, bool)>,
    started: Option<Duration>,
    spec: Option<(Duration, bool)>,
    ended: Option<(Duration, String)>,
}

impl Received {
    fn push(&mut self, at: Duration, log: &ops::Log) {
        let bytes = log.message.len() as u64
            + log
                .fields_json_map
                .values()
                .map(|v| v.len() as u64)
                .sum::<u64>();
        let mut found = marks(&log.message);
        for value in log.fields_json_map.values() {
            if value.len() < 4096 {
                found.extend(marks(&String::from_utf8_lossy(value)));
            }
        }
        let head: String = log.message.chars().take(60).collect();
        let vmm = log.fields_json_map.contains_key("vmm");
        self.logs.push((at, bytes, found, head, vmm));
    }

    /// Stderr bytes the guest wrote before its latest received mark.
    fn position(&self) -> u64 {
        self.logs
            .iter()
            .flat_map(|(_, _, marks, _, _)| marks.iter().map(|m| m.2))
            .max()
            .unwrap_or(0)
    }

    fn all_marks(&self) -> Vec<(Duration, Mark)> {
        self.logs
            .iter()
            .flat_map(|(at, _, marks, _, _)| marks.iter().map(move |m| (*at, *m)))
            .collect()
    }

    fn summary(&self) -> serde_json::Value {
        let marks = self.all_marks();
        serde_json::json!({
            "logs": self.logs.len(),
            "bytes": self.logs.iter().map(|l| l.1).sum::<u64>(),
            "marks": marks.len(),
            "position": self.position(),
            "first_heads": self.logs.iter().take(12).map(|l| &l.3).collect::<Vec<_>>(),
            "last_heads": self.logs.iter().rev().take(6).map(|l| &l.3).collect::<Vec<_>>(),
            "started_ms": self.started.map(|d| d.as_millis()),
            "spec": self.spec.map(|(d, ok)| (d.as_millis(), ok)),
            "ended": self.ended.as_ref().map(|(d, why)| (d.as_millis(), why)),
        })
    }
}

/// Read one response into `received`; false once the session ended.
fn receive(
    received: &mut Received,
    t0: Instant,
    response: Option<tonic::Result<proto::Response>>,
) -> bool {
    let at = t0.elapsed();
    match response {
        Some(Ok(proto::Response {
            kind: Some(proto::response::Kind::Log(log)),
        })) => received.push(at, &log),
        Some(Ok(proto::Response {
            kind: Some(proto::response::Kind::Started(_)),
        })) => received.started = Some(at),
        Some(Ok(proto::Response { kind: Some(kind) })) => {
            received.spec = Some((at, valid_spec(&kind)))
        }
        Some(Ok(proto::Response { kind: None })) => {
            received.ended = Some((at, "an empty response".to_string()));
            return false;
        }
        Some(Err(status)) => {
            received.ended = Some((at, format!("{status}")));
            return false;
        }
        None => {
            received.ended = Some((at, "EOF".to_string()));
            return false;
        }
    }
    true
}

/// The VMM, conmon and `start --attach` client of a launch, and its cgroup.
#[derive(Clone, Debug, serde::Serialize)]
struct Procs {
    name: String,
    id: String,
    vmm: u32,
    conmon: u32,
    client: Option<u32>,
    cgroup: String,
}

fn find(root: &launcher::Root) -> Option<Procs> {
    let name = launcher::vmm_name(root)?;
    let output = run::sudo_output(
        &[
            "podman",
            "container",
            "inspect",
            "--format",
            "{{.Id}} {{.State.Pid}} {{.State.ConmonPid}}",
            &name,
        ],
        None,
    );
    let inspected = String::from_utf8_lossy(&output.stdout).to_string();
    let [id, vmm, conmon] = inspected.split_whitespace().collect::<Vec<_>>()[..] else {
        return None;
    };
    let (vmm, conmon): (u32, u32) = (vmm.parse().ok()?, conmon.parse().ok()?);
    if vmm == 0 || conmon == 0 {
        return None;
    }
    // Not yet, or no longer, there: the next sample tries again.
    let cgroup = run::sudo_output(&["cat", &format!("/proc/{vmm}/cgroup")], None);
    let cgroup = String::from_utf8_lossy(&cgroup.stdout).to_string();
    let cgroup = format!(
        "/sys/fs/cgroup{}",
        cgroup.lines().find_map(|l| l.strip_prefix("0::"))?
    );
    let pgrep = std::process::Command::new("pgrep")
        .args(["-f", &format!("start --attach {id}")])
        .output()
        .expect("pgrep runs");
    let client = String::from_utf8_lossy(&pgrep.stdout)
        .split_whitespace()
        .filter_map(|pid| pid.parse::<u32>().ok())
        .find(|pid| {
            std::fs::read_to_string(format!("/proc/{pid}/comm")).is_ok_and(|c| c.trim() == "podman")
        })?;
    Some(Procs {
        name,
        id: id.to_string(),
        vmm,
        conmon,
        client: Some(client),
        cgroup,
    })
}

const SAMPLE: &str = r#"
for p in "$@"; do
  case "$p" in
    /*) printf 'cgroup %s %s\n' "$(cat "$p/memory.current" 2>/dev/null)" "$(cat "$p/memory.peak" 2>/dev/null)" ;;
    *) printf 'pid %s %s %s %s %s\n' "$p" \
         "$(awk '/^VmRSS:/{print $2}' /proc/$p/status 2>/dev/null)" \
         "$(awk '/^VmHWM:/{print $2}' /proc/$p/status 2>/dev/null)" \
         "$(awk '/^rchar:/{print $2}' /proc/$p/io 2>/dev/null)" \
         "$(awk '/^wchar:/{print $2}' /proc/$p/io 2>/dev/null)" ;;
  esac
done
"#;

fn self_status() -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").expect("own status");
    let field = |key: &str| -> u64 {
        status
            .lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|v| v.trim().trim_end_matches(" kB").parse().ok())
            .unwrap_or(0)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

/// One sample: ms since t0, this process's RSS/HWM in KiB, and per role
/// (VMM, conmon, client) RSS KiB, HWM KiB, rchar and wchar, and the
/// container cgroup's memory.current and memory.peak.
fn sample(t0: Instant, procs: Option<&Procs>) -> serde_json::Value {
    let (rss, hwm) = self_status();
    let mut value = serde_json::json!({"ms": t0.elapsed().as_millis(), "self_rss_kib": rss, "self_hwm_kib": hwm});
    let Some(procs) = procs else { return value };
    let mut roles = vec![("vmm", procs.vmm), ("conmon", procs.conmon)];
    roles.extend(procs.client.map(|pid| ("client", pid)));
    let mut args = vec!["sh", "-c", SAMPLE, "sh"];
    let pids: Vec<String> = roles.iter().map(|(_, pid)| pid.to_string()).collect();
    args.extend(pids.iter().map(String::as_str));
    args.push(&procs.cgroup);
    let output = run::sudo_output(&args, None);
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let words: Vec<&str> = line.split_whitespace().collect();
        let num = |i: usize| words.get(i).and_then(|w| w.parse::<u64>().ok());
        match words.first() {
            Some(&"cgroup") => {
                value["cgroup_current"] = num(1).into();
                value["cgroup_peak"] = num(2).into();
            }
            Some(&"pid") => {
                let Some((role, _)) = roles.iter().find(|(_, pid)| Some(*pid as u64) == num(1))
                else {
                    continue;
                };
                value[*role] = serde_json::json!({"rss_kib": num(2), "hwm_kib": num(3),
                                                  "rchar": num(4), "wchar": num(5)});
            }
            _ => {}
        }
    }
    value
}

/// Run `drive` while sampling every 500 ms, finding `root`'s launch once it
/// runs. Returns its value, the samples, and the processes found.
fn sampling<T>(
    root: &launcher::Root,
    t0: Instant,
    evidence: &std::path::Path,
    drive: impl FnOnce(&std::sync::Mutex<Option<Procs>>) -> T,
) -> (T, Vec<serde_json::Value>, Option<Procs>) {
    /// Stops the sampler however `drive` ends: the scope joins it before a
    /// panic in `drive` can propagate.
    struct Stop<'a>(&'a std::sync::atomic::AtomicBool);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let stop = std::sync::atomic::AtomicBool::new(false);
    let found = std::sync::Mutex::new(None);
    std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let mut samples = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let procs = {
                    let mut found = found.lock().unwrap();
                    if found.is_none() {
                        *found = find(root);
                    }
                    found.clone()
                };
                let value = sample(t0, procs.as_ref());
                append(evidence, serde_json::json!({"sample": &value}));
                samples.push(value);
                std::thread::sleep(Duration::from_millis(500));
            }
            samples
        });
        let value = {
            let _stop = Stop(&stop);
            drive(&found)
        };
        let samples = sampler.join().expect("the sampler");
        let procs = found.lock().unwrap().clone();
        (value, samples, procs)
    })
}

/// Growth over the run of `key` beneath `role` (or this process's `key`),
/// first sample to largest, in bytes.
fn growth(samples: &[serde_json::Value], role: Option<&str>, key: &str) -> Option<(u64, u64)> {
    let values: Vec<u64> = samples
        .iter()
        .filter_map(|s| match role {
            Some(role) => s.get(role)?.get(key)?.as_u64(),
            None => s.get(key)?.as_u64(),
        })
        .collect();
    let (first, max) = (*values.first()?, *values.iter().max()?);
    Some((first * 1024, (max - first) * 1024))
}

/// Memory bounds over `samples`, from this process's RSS before the launch.
fn memory_violations(
    samples: &[serde_json::Value],
    self_before_kib: u64,
    violations: &mut Vec<String>,
) -> serde_json::Value {
    let self_max = samples
        .iter()
        .filter_map(|s| s["self_rss_kib"].as_u64())
        .max()
        .unwrap_or(0);
    let self_growth = self_max.saturating_sub(self_before_kib) * 1024;
    if self_growth > GROWTH {
        violations.push(format!(
            "this process's RSS grew {self_growth} bytes, beyond {GROWTH}"
        ));
    }
    let mut report = serde_json::json!({"self_before_kib": self_before_kib, "self_max_kib": self_max,
                                        "self_growth": self_growth});
    for role in ["client", "conmon", "vmm"] {
        let rss = growth(samples, Some(role), "rss_kib");
        report[role] = serde_json::json!({"rss_first_and_growth": rss,
                                          "hwm": growth(samples, Some(role), "hwm_kib")});
        if role != "vmm" && rss.is_some_and(|(_, grew)| grew > GROWTH) {
            violations.push(format!("the {role}'s RSS grew beyond {GROWTH}: {rss:?}"));
        }
    }
    let peak = samples
        .iter()
        .filter_map(|s| s["cgroup_peak"].as_u64())
        .max();
    report["cgroup_peak"] = peak.into();
    report
}

/// Samples within `from..=to` ms which measured this process, the client,
/// conmon and the container's cgroup.
fn measured(samples: &[serde_json::Value], from: Duration, to: Duration) -> usize {
    samples
        .iter()
        .filter(|s| {
            s["ms"]
                .as_u64()
                .is_some_and(|ms| (from.as_millis()..=to.as_millis()).contains(&(ms as u128)))
        })
        .filter(|s| {
            s["self_rss_kib"].is_u64()
                && s["client"]["rss_kib"].is_u64()
                && s["conmon"]["rss_kib"].is_u64()
                && s["cgroup_current"].is_u64()
        })
        .count()
}

/// Bytes the guest had written, by its own clock mapped to the host's at
/// the first mark, beyond the consumer's position, at each receipt.
fn in_flight(received: &Received) -> (u64, serde_json::Value) {
    let mut marks = received.all_marks();
    marks.sort_by_key(|(_, (seq, _, _))| *seq);
    let Some(&(h0, (_, g0, _))) = marks.first() else {
        return (0, serde_json::Value::Null);
    };
    let mut by_guest: Vec<(f64, u64)> = marks.iter().map(|(_, (_, t, b))| (*t, *b)).collect();
    by_guest.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut worst = (0u64, 0u128, 0u64);
    for (at, (seq, _, position)) in &marks {
        let guest_now = g0 + at.saturating_sub(h0).as_secs_f64();
        let written = by_guest
            .iter()
            .filter(|(t, _)| *t <= guest_now)
            .map(|(_, b)| *b)
            .max()
            .unwrap_or(0);
        let flight = written.saturating_sub(*position);
        if flight > worst.0 {
            worst = (flight, at.as_millis(), *seq);
        }
    }
    let (last_at, (_, last_t, _)) = marks.last().expect("not empty");
    (
        worst.0,
        serde_json::json!({"max_in_flight": worst.0, "at_ms": worst.1, "at_seq": worst.2,
                           "guest_span_s": last_t - g0,
                           "host_span_s": last_at.saturating_sub(h0).as_secs_f64()}),
    )
}

/// Whether every flood line `0..lines` was received, and the done mark.
fn completeness(received: &Received) -> (Option<Mark>, Vec<u64>) {
    let marks = received.all_marks();
    let done = received
        .logs
        .iter()
        .find(|l| l.3.starts_with("c09 done"))
        .and_then(|l| l.2.first().copied());
    let seen: std::collections::BTreeSet<u64> = marks.iter().map(|(_, m)| m.0).collect();
    let missing = match done {
        Some((lines, _, _)) => (0..lines).filter(|seq| !seen.contains(seq)).collect(),
        None => Vec::new(),
    };
    (done, missing)
}

/// Forged readiness lines must arrive as ordinary logs; the launcher's own
/// "started connector container" (with its `vmm` field) only after them.
fn forgeries(received: &Received, violations: &mut Vec<String>) -> serde_json::Value {
    let heads: Vec<&str> = received.logs.iter().map(|l| l.3.as_str()).collect();
    let forged_started = received
        .logs
        .iter()
        .position(|l| l.3 == "started connector container" && !l.4);
    let genuine = received
        .logs
        .iter()
        .position(|l| l.3 == "started connector container" && l.4);
    let serving = heads.iter().position(|h| h.contains("SERVING"));
    let partial = heads.iter().position(|h| h.contains("c09 forged partial"));
    if forged_started.is_none() || serving.is_none() || partial.is_none() {
        violations.push(format!(
            "forged lines were not delivered as logs: started {forged_started:?}, \
             SERVING {serving:?}, partial {partial:?}"
        ));
    }
    serde_json::json!({"forged_started": forged_started, "genuine_started": genuine,
                       "serving": serving, "partial": partial})
}

/// Wait up to `within` for `procs` to exit and `root`'s launch to be
/// released, returning when each happened.
fn released(
    root: &launcher::Root,
    procs: &Procs,
    t0: Instant,
    within: Duration,
) -> serde_json::Value {
    let began = Instant::now();
    let mut gone: std::collections::BTreeMap<&str, u128> = Default::default();
    loop {
        for (role, pid) in [
            ("vmm", Some(procs.vmm)),
            ("conmon", Some(procs.conmon)),
            ("client", procs.client),
        ] {
            let Some(pid) = pid else { continue };
            if !gone.contains_key(role) && !std::path::Path::new(&format!("/proc/{pid}")).exists() {
                gone.insert(role, t0.elapsed().as_millis());
            }
        }
        let left = launcher::leftovers(root, Some(&procs.name));
        if left.is_empty() && gone.len() == 2 + procs.client.is_some() as usize {
            return serde_json::json!({"released_ms": t0.elapsed().as_millis(), "gone_ms": gone});
        }
        if began.elapsed() > within {
            return serde_json::json!({"released_ms": null, "gone_ms": gone, "left": left});
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// B0 and SLOW: the consumer reads everything, pausing `pace` after each log
/// once the first flood mark arrives, through Started and the Spec response,
/// then ends the session.
fn drained(variant: &str, flood: Flood, pace: Duration) {
    let run = run::load();
    let root = launcher::root();
    // SAFETY: no other thread exists yet to read the environment.
    unsafe { std::env::set_var("TMPDIR", &root.tmp_dir) };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    let evidence = run.run_dir.join("c09.evidence");
    let router = router(&run, &root, &root.state_dir, OFFENDER, "1");
    flood_flags(&root, &flood);
    append(
        &evidence,
        serde_json::json!({"variant": variant, "flood": &flood,
                                         "pace_ms": pace.as_millis(), "offender_memory": OFFENDER}),
    );

    let self_before = self_status().0;
    let t0 = Instant::now();
    let (request_tx, mut response_rx) = open(&runtime, &router, spec(&run.derive_python_image));
    let ((received, release), samples, procs) = sampling(&root, t0, &evidence, |found| {
        let mut received = Received::default();
        runtime.block_on(async {
            let deadline = tokio::time::Instant::now() + STARTED;
            while received.spec.is_none() {
                let Ok(response) = tokio::time::timeout_at(deadline, response_rx.recv()).await
                else {
                    received.ended = Some((t0.elapsed(), "timeout".to_string()));
                    break;
                };
                if !receive(&mut received, t0, response) {
                    break;
                }
                if !received.all_marks().is_empty() && received.spec.is_none() {
                    tokio::time::sleep(pace).await;
                }
            }
        });
        let procs = found.lock().unwrap().clone();
        std::mem::drop(request_tx);
        let ended = runtime.block_on(async {
            tokio::time::timeout(RELEASED, async {
                while let Some(response) = response_rx.recv().await {
                    if !receive(&mut received, t0, Some(response)) {
                        break;
                    }
                }
            })
            .await
            .is_ok()
        });
        let release = procs.map(|procs| released(&root, &procs, t0, RELEASED));
        (received, (ended, release))
    });

    let mut violations: Vec<String> = Vec::new();
    let mut inconclusive: Vec<String> = Vec::new();
    let (done, missing) = completeness(&received);
    let Some((lines, done_t, done_b)) = done else {
        inconclusive.push("the done mark never arrived: the flood is unproven".to_string());
        report(
            &evidence,
            variant,
            &received,
            &samples,
            &procs,
            &violations,
            &inconclusive,
            serde_json::Value::Null,
        );
        unreachable!();
    };
    if !missing.is_empty() || (flood.lines < u64::MAX >> 1 && lines != flood.lines) {
        violations.push(format!("flood lines missing of {lines}: {missing:?}"));
    }
    let (worst, flight) = in_flight(&received);
    if worst > IN_FLIGHT {
        violations.push(format!("{worst} bytes were in flight, beyond {IN_FLIGHT}"));
    }
    let tracking = flight["guest_span_s"].as_f64().unwrap_or(0.0)
        / flight["host_span_s"].as_f64().unwrap_or(0.0);
    if !pace.is_zero() && !(tracking >= TRACKING) {
        violations.push(format!(
            "the guest's flood took {tracking:.3} of the consumer's time, below {TRACKING}"
        ));
    }
    // The guest's exit, mapped to the host's clock at the first mark.
    let marks = received.all_marks();
    let first = marks.iter().min_by_key(|(_, m)| m.0).expect("a mark");
    // Measured throughout: at least one complete sample a second of flood,
    // and `MEASURED` over the flood and hold together.
    let done_at = received
        .logs
        .iter()
        .find(|l| l.3.starts_with("c09 done"))
        .map(|l| l.0)
        .expect("the done mark arrived");
    let (flood_samples, held_samples) = (
        measured(&samples, first.0, done_at),
        measured(&samples, first.0, done_at + Duration::from_secs_f64(HOLD)),
    );
    let flood_secs = done_at.saturating_sub(first.0).as_secs() as usize;
    if procs.is_none() || flood_samples < flood_secs || held_samples < MEASURED {
        inconclusive.push(format!(
            "unmeasured: processes {procs:?}, {flood_samples} complete samples over \
             {flood_secs} s of flood, {held_samples} with the hold"
        ));
    }
    let exit_host = first.0.as_secs_f64() + (done_t - first.1.1) + HOLD;
    match (received.started, received.spec) {
        (Some(started), Some((spec_at, true)))
            if started.as_secs_f64() >= exit_host - 1.0 && spec_at >= started => {}
        other => violations.push(format!(
            "Started must follow the guest's exit at {exit_host:.3}s, then a valid Spec: {other:?}"
        )),
    }
    let forged = forgeries(&received, &mut violations);
    let memory = memory_violations(&samples, self_before, &mut violations);
    let (ended, release) = release;
    if !ended {
        violations.push(format!("the session did not end within {RELEASED:?}"));
    }
    match &release {
        Some(r) if !r["released_ms"].is_null() => {}
        other => violations.push(format!("not released within {RELEASED:?}: {other:?}")),
    }
    let detail = serde_json::json!({"done": {"lines": lines, "t": done_t, "b": done_b},
        "tracking": tracking, "measured": {"flood": flood_samples, "with_hold": held_samples},
        "exit_host_s": exit_host, "flight": flight, "forged": forged, "memory": memory,
        "release": release, "normalized_self_growth_per_mib": memory["self_growth"].as_f64()
            .map(|g| g / (done_b as f64 / MIB as f64))});
    report(
        &evidence,
        variant,
        &received,
        &samples,
        &procs,
        &violations,
        &inconclusive,
        detail,
    );
}

fn report(
    evidence: &std::path::Path,
    variant: &str,
    received: &Received,
    samples: &[serde_json::Value],
    procs: &Option<Procs>,
    violations: &[String],
    inconclusive: &[String],
    detail: serde_json::Value,
) {
    append(
        evidence,
        serde_json::json!({"summary": variant, "received": received.summary(), "samples": samples.len(),
                           "procs": procs, "detail": detail,
                           "inconclusive": inconclusive, "violations": violations}),
    );
    assert!(
        inconclusive.is_empty() && violations.is_empty(),
        "{variant} inconclusive {inconclusive:#?}\nviolations {violations:#?}\n{detail:#}"
    );
}

/// C09-B0: four ordinary logs and the forgeries, then derive-python's Spec.
#[test]
fn baseline() {
    drained(
        "C09-B0",
        Flood {
            lines: 4,
            stderr: 48 * MIB,
            stdout: 0,
            seconds: 30.0,
            hold: HOLD,
        },
        Duration::ZERO,
    );
}

/// C09-SLOW: 48 MiB of stderr and 8 MiB of stdout, read at most 50 logs a
/// second.
#[test]
fn slow_reader() {
    drained(
        "C09-SLOW",
        Flood {
            lines: u64::MAX >> 1,
            stderr: 48 * MIB,
            stdout: 8 * MIB,
            seconds: 30.0,
            hold: HOLD,
        },
        Duration::from_millis(20),
    );
}

/// C09-STOP: the consumer reads 2 MiB of flood, then stops for `WINDOW`
/// while a sibling launch answers its Spec, then cancels with the guest's
/// writes still blocked, and the offender must be released.
#[test]
fn stopped_reader() {
    let run = run::load();
    let (root, sibling_root) = (launcher::root(), launcher::root());
    // SAFETY: no other thread exists yet to read the environment.
    unsafe { std::env::set_var("TMPDIR", &root.tmp_dir) };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    let evidence = run.run_dir.join("c09.evidence");
    let state_dir = root.state_dir.clone();
    let offender = router(&run, &root, &state_dir, OFFENDER, "1");
    let sibling = router(&run, &sibling_root, &state_dir, OFFENDER, "2");
    let flood = Flood {
        lines: u64::MAX >> 1,
        stderr: 48 * MIB,
        stdout: 8 * MIB,
        seconds: 30.0,
        hold: HOLD,
    };
    flood_flags(&root, &flood);
    append(
        &evidence,
        serde_json::json!({"variant": "C09-STOP", "flood": &flood,
                                         "window_ms": WINDOW.as_millis(), "offender_memory": OFFENDER}),
    );

    let self_before = self_status().0;
    let t0 = Instant::now();
    let (request_tx, mut response_rx) = open(&runtime, &offender, spec(&run.derive_python_image));
    let ((received, stopped, sibling_result, cancelled, release), samples, procs) =
        sampling(&root, t0, &evidence, |found| {
            let mut received = Received::default();
            runtime.block_on(async {
                let deadline = tokio::time::Instant::now() + STARTED;
                while received.position() < 2 * MIB && received.started.is_none() {
                    let Ok(response) = tokio::time::timeout_at(deadline, response_rx.recv()).await
                    else {
                        received.ended = Some((t0.elapsed(), "timeout".to_string()));
                        break;
                    };
                    if !receive(&mut received, t0, response) {
                        break;
                    }
                }
            });
            let stopped = t0.elapsed();
            let logger = |_: &ops::Log| {};
            let sibling_result = runtime
                .block_on(proto_grpc::connector::unary(
                    &sibling,
                    &logger,
                    spec(&run.derive_python_image),
                    STARTED,
                    STARTED,
                ))
                .map(|(_started, kind)| (t0.elapsed(), valid_spec(&kind)))
                .map_err(|err| format!("{err:#}"));
            let rest = (stopped + WINDOW).saturating_sub(t0.elapsed());
            std::thread::sleep(rest);
            let procs = found.lock().unwrap().clone();
            let cancelled = t0.elapsed();
            std::mem::drop((request_tx, response_rx));
            let release = procs.map(|procs| released(&root, &procs, t0, RELEASED));
            (received, stopped, sibling_result, cancelled, release)
        });

    let mut violations: Vec<String> = Vec::new();
    let mut inconclusive: Vec<String> = Vec::new();
    if received.position() < 2 * MIB {
        inconclusive.push(format!(
            "only {} bytes of flood arrived before the stop",
            received.position()
        ));
    }
    if received.started.is_some() || received.spec.is_some() || received.ended.is_some() {
        violations.push(format!("before the stop: {:?}", received.summary()));
    }
    // The guest blocked: its VMM's writes and conmon's reads flat over the
    // window's last `PLATEAU`, and conmon's reads since the stop bounded.
    let at = |ms: u128| {
        samples
            .iter()
            .filter(|s| s["ms"].as_u64().is_some_and(|m| m as u128 <= ms))
            .last()
    };
    let counter = |s: Option<&serde_json::Value>, role: &str, key: &str| {
        s.and_then(|s| s.get(role)?.get(key)?.as_u64())
    };
    let window_end = cancelled.as_millis();
    let plateau_start = window_end.saturating_sub(PLATEAU.as_millis());
    let mut plateau = serde_json::Map::new();
    for (role, key) in [("vmm", "wchar"), ("conmon", "rchar"), ("client", "wchar")] {
        let (from, to, since_stop) = (
            counter(at(plateau_start), role, key),
            counter(at(window_end), role, key),
            counter(at(stopped.as_millis()), role, key),
        );
        plateau.insert(
            format!("{role}.{key}"),
            serde_json::json!({"at_stop": since_stop, "plateau_start": from, "window_end": to}),
        );
        match (from, to, since_stop) {
            (Some(from), Some(to), Some(stop)) => {
                if role != "client" && to - from > STALLED {
                    violations.push(format!(
                        "{role} {key} grew {} over the window's last {PLATEAU:?}: the guest was not held",
                        to - from
                    ));
                }
                if role == "conmon" && to - stop > IN_FLIGHT {
                    violations.push(format!(
                        "conmon read {} bytes after the consumer stopped, beyond {IN_FLIGHT}",
                        to - stop
                    ));
                }
            }
            other => inconclusive.push(format!("no {role} {key} counters: {other:?}")),
        }
    }
    match &sibling_result {
        Ok((at, true)) if *at <= cancelled => {}
        other => violations.push(format!("the sibling's Spec during the window: {other:?}")),
    }
    let window_samples = measured(&samples, stopped, cancelled);
    if procs.is_none() || window_samples < MEASURED {
        inconclusive.push(format!(
            "unmeasured: processes {procs:?}, {window_samples} complete samples while stopped"
        ));
    }
    let memory = memory_violations(&samples, self_before, &mut violations);
    match &release {
        Some(r) if !r["released_ms"].is_null() => {}
        other => violations.push(format!(
            "not released within {RELEASED:?} of cancelling: {other:?}"
        )),
    }
    let sibling_name = launcher::vmm_name(&sibling_root);
    if let Some(name) = &sibling_name {
        let left = launcher::left(&state_dir, &root.tmp_dir, name);
        if !left.is_empty() {
            violations.push(format!("the sibling {name} left {left:?}"));
        }
    }
    let detail = serde_json::json!({"stopped_ms": stopped.as_millis(), "cancelled_ms": window_end,
        "measured": window_samples,
        "plateau": plateau, "sibling": format!("{sibling_result:?}"), "sibling_name": sibling_name,
        "memory": memory, "release": release,
        "release_after_cancel_ms": release.as_ref().and_then(|r| r["released_ms"].as_u64())
            .map(|ms| ms as u128 - window_end)});
    report(
        &evidence,
        "C09-STOP",
        &received,
        &samples,
        &procs,
        &violations,
        &inconclusive,
        detail,
    );
}

/// A panic in `drive`, as a failed assertion there would be, stops the
/// sampler and unwinds out of `sampling`, rather than leaving its scope
/// joining a sampler which never stops. No launch: nothing is found.
#[test]
fn sampling_unwinds() {
    let root = launcher::root();
    let evidence = std::path::Path::new(&root.dir).join("c09-unwind.evidence");
    let t0 = Instant::now();
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sampling(&root, t0, &evidence, |_| -> () {
            std::thread::sleep(Duration::from_millis(1200));
            panic!("drive fails");
        })
    }));
    assert!(unwound.is_err(), "the drive's panic propagates");
    assert!(t0.elapsed() < Duration::from_secs(5), "{:?}", t0.elapsed());
    let samples = std::fs::read_to_string(&evidence).expect("samples were written");
    assert!(samples.lines().count() >= 2, "{samples}");
}
