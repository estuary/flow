//! The interposer that wraps a connector binary.
//!
//! The catalog's `local:` command names the shim with the real connector as its
//! argument, so the runtime talks to the shim and the shim talks to the
//! connector. From that position it can see every protocol message and perturb
//! the ones a scenario cares about, with no change to Flow and no change to any
//! connector.
//!
//! The shim observes and perturbs; it never synthesizes a message the runtime
//! did not send. The zombie is the sole exception — the messages it replays are
//! real runtime messages, only their *scheduling* is the shim's — and that is
//! the one place where the suite's fidelity is weaker than "a real runtime drove
//! this".

use crate::protocol::{Action, Event, FaultRule, RunDir, TraceEvent, Trigger};
use anyhow::Context;
use proto_flow::materialize;
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Append-only protocol trace, shared by every connector process of a run.
///
/// Concurrent processes — a live instance and its zombie, or a crashed instance and
/// its replacement — must interleave whole lines rather than corrupting each other.
/// The mutex is not what buys that: it is per-process, and these are separate
/// processes. What buys it is `O_APPEND` plus **one** `write` syscall per line, which
/// the kernel serialises against the file's end.
///
/// `writeln!` does not give one syscall. It is `write_fmt`, which issues a write per
/// format fragment — the line, then the newline — leaving a window for another process to
/// append between the two and splice two records into one line. Hence the explicit newline
/// and a single `write_all`.
pub struct Trace {
    file: Mutex<std::fs::File>,
}

impl Trace {
    pub fn open(run: &RunDir) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&run.root)
            .with_context(|| format!("creating run directory {:?}", run.root))?;

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(run.trace())
            .with_context(|| format!("opening trace {:?}", run.trace()))?;

        Ok(Self {
            file: Mutex::new(file),
        })
    }

    pub fn log(&self, event: Event) {
        let mut line = serde_json::to_string(&TraceEvent {
            pid: std::process::id(),
            event,
        })
        .expect("trace events serialize");
        line.push('\n');

        let mut file = self.file.lock().unwrap();
        // A failed trace write is not worth failing the connector over: the
        // harness times out waiting for the event and reports that instead.
        let _ = file.write_all(line.as_bytes());
    }
}

/// Per-session counts of each protocol event, and the fault bookkeeping that
/// rides along with them.
struct Counters {
    seen: BTreeMap<Trigger, u64>,
    /// Documents stored per binding since the last `StartCommit`.
    stored: Vec<u64>,
    /// Live-instance `StartedCommit`s remaining before a frozen zombie thaws;
    /// `None` when no zombie is frozen.
    thaw_countdown: Option<u64>,
}

impl Counters {
    fn new() -> Self {
        Self {
            seen: BTreeMap::new(),
            stored: Vec::new(),
            thaw_countdown: None,
        }
    }

    fn count(&mut self, trigger: Trigger) -> u64 {
        let n = self.seen.entry(trigger).or_default();
        *n += 1;
        *n
    }

    /// Reset the within-transaction counters, at a transaction boundary.
    ///
    /// Without this, "the 10th `Store`" happens once per session and a rule armed
    /// after a few commits could never match it — the occurrence would already have
    /// gone by. Per-transaction counting makes it recur.
    fn end_transaction(&mut self) {
        self.seen.remove(&Trigger::Store);
        self.seen.remove(&Trigger::Load);
    }
}

pub struct Shim {
    run: RunDir,
    faults: Vec<FaultRule>,
    codec: connector_init::Codec,
    trace: Trace,
    counters: Mutex<Counters>,
    /// The zombie's process id, or zero. Shared because a crash fault can fire
    /// from either pump, and a *frozen* zombie left behind would outlive the run
    /// holding a lock on the destination.
    zombie_pid: std::sync::atomic::AtomicI32,
    /// This session's shard range, as `(key_begin, key_end, r_clock_begin)`, set
    /// from `Open.range`. Rules restricted to a [`ShardTarget`] consult it.
    range: Mutex<(u32, u32, u32)>,
}

/// A connector process, and the pipes we speak to it over.
struct Instance {
    child: async_process::Child,
    stdin: async_process::ChildStdio,
    stdout: Option<async_process::ChildStdio>,
}

/// The value `materialize-boilerplate` expects in `FLOW_RUNTIME_CODEC`.
///
/// The shim relays between runtime and connector without transcoding, so both sides must
/// use one codec. A connector built on the boilerplate reads this variable and defaults to
/// protobuf when it is unset — which is why every real connector failed here while the
/// reference one, which hardcodes JSON, did not.
fn codec_name(codec: connector_init::Codec) -> &'static str {
    match codec {
        connector_init::Codec::Proto => "proto",
        connector_init::Codec::Json => "json",
    }
}

impl Instance {
    /// Spawn `command`, inheriting stderr so the connector's own logs reach the
    /// runtime unaltered — the shim is transparent to logging.
    fn spawn(command: &[String], codec: connector_init::Codec) -> anyhow::Result<Self> {
        let mut child: async_process::Child = connector_init::rpc::new_command(command)
            .env("FLOW_RUNTIME_CODEC", codec_name(codec))
            .stdin(async_process::Stdio::piped())
            .stdout(async_process::Stdio::piped())
            .stderr(async_process::Stdio::inherit())
            .spawn()
            .with_context(|| format!("spawning connector {command:?}"))?
            .into();

        Ok(Self {
            stdin: child.stdin.take().expect("stdin is piped"),
            stdout: Some(child.stdout.take().expect("stdout is piped")),
            child,
        })
    }

    fn pid(&self) -> libc::pid_t {
        self.child.id() as libc::pid_t
    }

    /// Signal the process, ignoring the ESRCH of one that already exited.
    fn signal(&self, sig: libc::c_int) {
        unsafe { libc::kill(self.pid(), sig) };
    }
}

impl Shim {
    pub fn new(
        run_dir: String,
        faults: Option<String>,
        codec: connector_init::Codec,
    ) -> anyhow::Result<Self> {
        let run = RunDir::new(run_dir);
        let trace = Trace::open(&run)?;

        let faults: Vec<FaultRule> = match faults {
            Some(json) if !json.trim().is_empty() => {
                serde_json::from_str(&json).context("parsing fault rules")?
            }
            _ => Vec::new(),
        };

        Ok(Self {
            run,
            faults,
            codec,
            trace,
            counters: Mutex::new(Counters::new()),
            zombie_pid: std::sync::atomic::AtomicI32::new(0),
            range: Mutex::new((0, u32::MAX, 0)),
        })
    }

    /// Rules matching `trigger` at occurrence `nth` which have not already
    /// fired, marking each as fired.
    ///
    /// The fired-marker is a file rather than in-process state, and that is
    /// load-bearing: a crash fault kills the connector, the runtime restarts
    /// it, and the replacement would reach the same trigger and crash again —
    /// forever — leaving the shard permanently down and the scenario with
    /// nothing to check. The marker makes every rule one-shot per run, and
    /// `create_new` makes the claim atomic between racing processes.
    fn matched(&self, trigger: Trigger, nth: u64) -> Vec<(usize, Action)> {
        let mut out = Vec::new();
        let committed = self
            .counters
            .lock()
            .unwrap()
            .seen
            .get(&Trigger::StartedCommit)
            .copied()
            .unwrap_or(0);

        for (idx, rule) in self.faults.iter().enumerate() {
            if rule.on != trigger || rule.nth != nth || committed < rule.arm_after {
                continue;
            }
            let (key_begin, key_end, r_clock_begin) = *self.range.lock().unwrap();
            if !rule.shard.admits(key_begin, key_end, r_clock_begin) {
                continue;
            }
            match std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(self.run.fired(idx))
            {
                Ok(_) => out.push((idx, rule.action.clone())),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(err) => self.trace.log(Event::Failed {
                    error: format!("marking rule {idx} fired: {err}"),
                }),
            }
        }
        out
    }

    /// The zombie rule, if one is declared and has not already fired.
    ///
    /// The fired marker is consulted for the same reason a crash consults it: this runs on
    /// every `Open`, and a session can open again for reasons of its own. Without the check,
    /// a live instance dying *after* the zombie had already raced would bring up a second
    /// zombie fed by the runtime for the rest of the run. Fencing makes that survivable for
    /// the one class this scenario applies to, which is exactly why it would go unnoticed.
    fn zombie_action(&self) -> Option<FaultRule> {
        self.faults
            .iter()
            .enumerate()
            .find(|(idx, r)| {
                matches!(r.action, Action::Zombie { .. }) && !self.run.fired(*idx).exists()
            })
            .map(|(_, r)| r.clone())
    }

    /// Proxy one session (or one unary RPC) between our stdio and the connector,
    /// resolving to the connector's own exit status.
    pub async fn run(self, command: Vec<String>) -> anyhow::Result<std::process::ExitStatus> {
        let shim = Arc::new(self);

        let mut live = Instance::spawn(&command, shim.codec)?;
        let live_pid = live.pid();
        let live_stdout = live.stdout.take().expect("stdout was piped");
        let live_stdin = live.stdin;

        let requests = tokio::spawn(pump_requests(
            shim.clone(),
            tokio::io::stdin(),
            live_stdin,
            command.clone(),
            live_pid,
        ));
        let responses = tokio::spawn(pump_responses(shim.clone(), live_stdout, live_pid));

        // The connector's exit is authoritative. Its stdout closing is the
        // normal end of a session, and a request-pump error does not override
        // the connector's status: the runtime may close our stdin at any time.
        let status = live.child.wait().await.context("waiting for connector")?;

        // Record how the connector died. The reactor's account of it — "connector exited
        // with no log output" — is identical whether the shim killed it for a fault or it
        // fell over by itself, and the difference matters when reading a failure. A rule
        // that fired is already in the trace, so a death with no preceding `fault` from
        // this process is the connector's own doing.
        if !status.success() {
            shim.trace.log(Event::Failed {
                error: match std::os::unix::process::ExitStatusExt::signal(&status) {
                    Some(signal) => format!("connector killed by signal {signal}"),
                    None => format!("connector exited with status {:?}", status.code()),
                },
            });
        }

        if let Ok(Err(err)) = responses.await {
            shim.trace.log(Event::Failed {
                error: format!("response pump: {err}"),
            });
        }
        requests.abort();

        Ok(status)
    }
}

/// A second connector process fed the same requests as the live one, frozen at
/// a chosen point and thawed later so its stale commit races.
struct Zombie {
    thaw_after_commits: u64,
    instance: Instance,
    /// Resolves once the zombie has written its first response.
    ///
    /// The live instance's `Open` waits on this, and that wait is what makes the
    /// scenario deterministic rather than a coin flip: both instances fence on `Open`,
    /// so whichever reaches the destination *second* holds the newer nonce. The zombie
    /// is handed `Open` first precisely so that it ends up holding the older one. If the
    /// live instance opened first it would be the stale one, its commits would be
    /// refused, and the scenario would be testing the opposite of what it claims.
    opened: Option<tokio::sync::oneshot::Receiver<()>>,
    /// Requests to replay on thaw. Buffered rather than written straight
    /// through: a SIGSTOPped process stops draining its stdin, and a blocking
    /// write into a full pipe would wedge the live path along with it.
    queued: Vec<Vec<u8>>,
    frozen: bool,
    /// Set once the queued work has been completed by a `StartCommit`. One
    /// transaction is all it takes to race a commit, so everything after is
    /// dropped.
    sealed: bool,
}

impl Zombie {
    fn spawn(
        command: &[String],
        rule: FaultRule,
        codec: connector_init::Codec,
    ) -> anyhow::Result<Self> {
        let Action::Zombie { thaw_after_commits } = rule.action else {
            unreachable!("caller matched a Zombie action")
        };

        let mut instance = Instance::spawn(command, codec)?;
        let mut stdout = instance.stdout.take().expect("stdout was piped");
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel();

        // Drain and discard its responses. The runtime must never see a second
        // Opened, but they still have to be consumed or the zombie blocks writing
        // to a full stdout pipe. The first response doubles as the signal that it
        // has finished opening.
        tokio::spawn(async move {
            let mut opened_tx = Some(opened_tx);
            let mut buf = vec![0u8; 16 * 1024];

            while let Ok(n) = stdout.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                if let Some(tx) = opened_tx.take() {
                    let _ = tx.send(());
                }
            }
        });

        Ok(Self {
            thaw_after_commits,
            instance,
            opened: Some(opened_rx),
            queued: Vec::new(),
            frozen: false,
            sealed: false,
        })
    }

    /// Await the zombie's first response, so that it has taken its fence before the
    /// live instance takes one. Bounded, because a connector that never responds to
    /// `Open` must not wedge the live path — a scenario that then fails is reported
    /// with the shim's trace rather than hanging.
    async fn await_opened(&mut self) {
        let Some(opened) = self.opened.take() else {
            return;
        };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(30), opened).await;
    }

    async fn offer(&mut self, encoded: &[u8], is_start_commit: bool) {
        if self.sealed {
            return;
        }
        if self.frozen {
            self.queued.push(encoded.to_vec());
            self.sealed = is_start_commit;
            return;
        }
        let _ = self.instance.stdin.write_all(encoded).await;
        let _ = self.instance.stdin.flush().await;
    }

    fn freeze(&mut self) {
        self.frozen = true;
        self.instance.signal(libc::SIGSTOP);
    }

    async fn thaw(&mut self) {
        self.instance.signal(libc::SIGCONT);

        for msg in std::mem::take(&mut self.queued) {
            let _ = self.instance.stdin.write_all(&msg).await;
        }
        let _ = self.instance.stdin.flush().await;
        // Closing its stdin ends the zombie's session once it has tried to
        // commit, rather than leaving it idle until the live instance exits.
        let _ = self.instance.stdin.shutdown().await;
        self.frozen = false;
        self.sealed = true;
    }
}

fn request_trigger(req: &materialize::Request) -> Option<Trigger> {
    if req.open.is_some() {
        Some(Trigger::Open)
    } else if req.load.is_some() {
        Some(Trigger::Load)
    } else if req.flush.is_some() {
        Some(Trigger::Flush)
    } else if req.store.is_some() {
        Some(Trigger::Store)
    } else if req.start_commit.is_some() {
        Some(Trigger::StartCommit)
    } else if req.acknowledge.is_some() {
        Some(Trigger::Acknowledge)
    } else {
        None
    }
}

fn response_trigger(resp: &materialize::Response) -> Option<Trigger> {
    if resp.started_commit.is_some() {
        Some(Trigger::StartedCommit)
    } else if resp.acknowledged.is_some() {
        Some(Trigger::Acknowledged)
    } else {
        None
    }
}

/// End the run: kill the connector processes and exit non-zero, so the runtime
/// sees a connector failure and restarts the shard.
///
/// A hard exit rather than an unwind, on purpose. A crash fault stands in for a
/// connector process dying, and anything the shim did to shut down gracefully —
/// flushing a pipe, letting the connector finish a write — would make the fault
/// gentler than the failure it models.
fn crash(shim: &Shim, live_pid: libc::pid_t) -> ! {
    unsafe { libc::kill(live_pid, libc::SIGKILL) };

    // A stopped process does not act on SIGKILL until it is continued, and this
    // exit runs no destructors — so a frozen zombie left here would outlive the run
    // holding a lock on the destination, and wedge every later scenario.
    let zombie = shim.zombie_pid.load(std::sync::atomic::Ordering::Relaxed);
    if zombie != 0 {
        unsafe { libc::kill(zombie, libc::SIGCONT) };
        unsafe { libc::kill(zombie, libc::SIGKILL) };
    }

    shim.trace.log(Event::Failed {
        error: "crash fault injected".to_string(),
    });
    std::process::exit(1);
}

async fn pump_requests<R>(
    shim: Arc<Shim>,
    mut from_runtime: R,
    mut to_connector: async_process::ChildStdio,
    command: Vec<String>,
    live_pid: libc::pid_t,
) -> anyhow::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = Vec::with_capacity(32 * 1024);
    let mut encoded = Vec::new();
    let mut zombie: Option<Zombie> = None;

    loop {
        if buffer.len() == buffer.capacity() {
            buffer.reserve(1);
        }
        if from_runtime.read_buf(&mut buffer).await? == 0 {
            // The runtime closed our stdin; pass that on so the connector can
            // finish its session.
            let _ = to_connector.shutdown().await;
            if let Some(z) = &mut zombie {
                if !z.frozen {
                    let _ = z.instance.stdin.shutdown().await;
                }
            }
            return Ok(());
        }

        for req in shim.codec.decode::<materialize::Request>(&mut buffer)? {
            encoded.clear();
            shim.codec.encode(&req, &mut encoded);

            let Some(trigger) = request_trigger(&req) else {
                // Spec / Validate / Apply pass through untouched, and get no zombie: a
                // second process re-running someone's DDL buys the suite nothing. Note they
                // are not all separate sessions — under runtime-next only `Validate` is,
                // while `Spec` is the first request of the session that later gets `Open`
                // and `Apply` runs over shard zero's same stream.
                to_connector.write_all(&encoded).await?;
                to_connector.flush().await?;
                continue;
            };

            let nth = {
                let mut counters = shim.counters.lock().unwrap();

                if let Some(store) = &req.store {
                    let binding = store.binding as usize;
                    if counters.stored.len() <= binding {
                        counters.stored.resize(binding + 1, 0);
                    }
                    counters.stored[binding] += 1;
                }
                counters.count(trigger)
            };

            // A zombie is spawned when a session opens — not at startup — and is
            // handed the `Open` *before* the live instance sees it. That ordering is
            // the scenario: both instances fence on Open, so the one that reaches
            // the destination second holds the newer nonce, and the zombie must be
            // the one holding the older.
            if trigger == Trigger::Open {
                if let Some(rule) = shim.zombie_action() {
                    let mut z = Zombie::spawn(&command, rule, shim.codec)?;
                    shim.zombie_pid
                        .store(z.instance.pid(), std::sync::atomic::Ordering::Relaxed);

                    z.offer(&encoded, false).await;
                    z.await_opened().await;
                    zombie = Some(z);
                }
            }

            if let Some(open) = &req.open {
                let range = open.range.clone().unwrap_or_default();
                // Recorded before any fault can fire, because `Open` is the first
                // request of every session and a rule restricted to a `ShardTarget`
                // consults it.
                *shim.range.lock().unwrap() = (range.key_begin, range.key_end, range.r_clock_begin);
                shim.trace.log(Event::Opened {
                    key_begin: range.key_begin,
                    key_end: range.key_end,
                    bindings: open
                        .materialization
                        .as_ref()
                        .map(|m| m.bindings.len())
                        .unwrap_or(0),
                });
            }
            if trigger == Trigger::StartCommit {
                let per_binding = {
                    let mut counters = shim.counters.lock().unwrap();
                    counters.end_transaction();
                    std::mem::take(&mut counters.stored)
                };
                shim.trace.log(Event::Stored { per_binding });
            }
            // Transaction boundaries only. A line per stored document would
            // swamp the trace, and the harness never waits on an individual
            // Load or Store.
            if matches!(
                trigger,
                Trigger::Open | Trigger::Flush | Trigger::StartCommit | Trigger::Acknowledge
            ) {
                shim.trace.log(Event::Phase { trigger, nth });
            }

            for (idx, action) in shim.matched(trigger, nth) {
                shim.trace.log(Event::Fault {
                    rule: idx,
                    action: action.clone(),
                });

                match action {
                    Action::Crash => crash(&shim, live_pid),
                    Action::Stall { millis } => {
                        tokio::time::sleep(std::time::Duration::from_millis(millis)).await
                    }
                    Action::Zombie { .. } => {
                        if let Some(z) = &mut zombie {
                            z.freeze();
                            shim.counters.lock().unwrap().thaw_countdown =
                                Some(z.thaw_after_commits);
                        }
                    }
                }
            }

            if let Some(z) = &mut zombie {
                if trigger != Trigger::Open {
                    z.offer(&encoded, trigger == Trigger::StartCommit).await;
                }

                // Thawing is checked here because the request pump owns the
                // zombie's stdin; the countdown itself is decremented by the
                // response pump as the live instance commits.
                let due = z.frozen && shim.counters.lock().unwrap().thaw_countdown == Some(0);
                if due {
                    shim.counters.lock().unwrap().thaw_countdown = None;
                    z.thaw().await;
                }
            }

            to_connector.write_all(&encoded).await?;
            to_connector.flush().await?;
        }
    }
}

async fn pump_responses(
    shim: Arc<Shim>,
    mut from_connector: async_process::ChildStdio,
    live_pid: libc::pid_t,
) -> anyhow::Result<()> {
    let mut buffer = Vec::with_capacity(32 * 1024);
    let mut encoded = Vec::new();
    let mut to_runtime = tokio::io::stdout();

    loop {
        if buffer.len() == buffer.capacity() {
            buffer.reserve(1);
        }
        if from_connector.read_buf(&mut buffer).await? == 0 {
            return Ok(());
        }

        for resp in shim.codec.decode::<materialize::Response>(&mut buffer)? {
            let Some(trigger) = response_trigger(&resp) else {
                encoded.clear();
                shim.codec.encode(&resp, &mut encoded);
                to_runtime.write_all(&encoded).await?;
                continue;
            };

            let nth = {
                let mut counters = shim.counters.lock().unwrap();

                if trigger == Trigger::StartedCommit {
                    if let Some(remaining) = &mut counters.thaw_countdown {
                        *remaining = remaining.saturating_sub(1);
                    }
                }
                counters.count(trigger)
            };

            shim.trace.log(Event::Phase { trigger, nth });

            for (idx, action) in shim.matched(trigger, nth) {
                shim.trace.log(Event::Fault {
                    rule: idx,
                    action: action.clone(),
                });

                match action {
                    // Crashing on `StartedCommit` models the window between the
                    // connector's commit and the runtime's: the connector is
                    // done, the recovery log is not.
                    Action::Crash => crash(&shim, live_pid),
                    Action::Stall { millis } => {
                        tokio::time::sleep(std::time::Duration::from_millis(millis)).await
                    }
                    // Only meaningful against a request.
                    Action::Zombie { .. } => {}
                }
            }

            encoded.clear();
            shim.codec.encode(&resp, &mut encoded);
            to_runtime.write_all(&encoded).await?;
        }
        to_runtime.flush().await?;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::{Action, ShardTarget};

    fn shim(faults: Vec<FaultRule>) -> (Shim, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let shim = Shim::new(
            dir.path().to_string_lossy().to_string(),
            Some(serde_json::to_string(&faults).unwrap()),
            connector_init::Codec::Json,
        )
        .unwrap();
        (shim, dir)
    }

    /// Drive `count` and `matched` the way the request pump does, returning whether
    /// the rule fired on this occurrence.
    fn offer(shim: &Shim, trigger: Trigger) -> bool {
        let nth = shim.counters.lock().unwrap().count(trigger);
        !shim.matched(trigger, nth).is_empty()
    }

    fn commit(shim: &Shim) {
        // A transaction ends with StartCommit (which resets the within-transaction
        // counters) and is credited by the StartedCommit response.
        let nth = shim.counters.lock().unwrap().count(Trigger::StartCommit);
        let _ = shim.matched(Trigger::StartCommit, nth);
        shim.counters.lock().unwrap().end_transaction();

        let nth = shim.counters.lock().unwrap().count(Trigger::StartedCommit);
        let _ = shim.matched(Trigger::StartedCommit, nth);
    }

    /// The bug this exists to prevent: `nth` only rises, so a rule armed after a
    /// commit could never match a `Store` occurrence that had already gone past.
    /// Counting stores per transaction makes the occurrence recur.
    #[test]
    fn an_armed_rule_fires_in_a_later_transaction_not_the_first() {
        let (shim, _dir) = shim(vec![FaultRule {
            on: Trigger::Store,
            nth: 2,
            arm_after: 1,
            shard: ShardTarget::Any,
            action: Action::Stall { millis: 0 },
        }]);

        // First transaction: the rule is not yet armed, so its occurrence passes.
        assert!(!offer(&shim, Trigger::Store));
        assert!(!offer(&shim, Trigger::Store));
        assert!(!offer(&shim, Trigger::Store));
        commit(&shim);

        // Second transaction: the same occurrence comes round again, and now fires.
        assert!(!offer(&shim, Trigger::Store));
        assert!(offer(&shim, Trigger::Store));
    }

    /// A rule aimed at the split leader must not fire in the pre-split parent, and
    /// must not fire in a non-zero split shard either. Both misfires are worse than a
    /// missed fault: the first kills the shard mid-split so the split never lands, and
    /// the second EOFs the fan-in and takes the whole task down.
    #[test]
    fn a_rule_aimed_at_the_split_leader_fires_in_no_other_shard() {
        // Two shims per case, because an offer consumes the occurrence: once `nth` has
        // moved past 1 the rule can no longer match.
        let rule = || {
            vec![FaultRule::crash_at(Trigger::StartedCommit, 1).in_shard(ShardTarget::SplitLeader)]
        };

        for (name, range) in [
            ("the unsplit parent", (0u32, u32::MAX, 0u32)),
            ("a non-zero split shard", (0x8000_0000, u32::MAX, 0)),
        ] {
            let (shim, _dir) = shim(rule());
            *shim.range.lock().unwrap() = range;
            assert!(
                !offer(&shim, Trigger::StartedCommit),
                "a rule aimed at the split leader must not fire in {name}",
            );
        }

        let (leader, _dir) = shim(rule());
        *leader.range.lock().unwrap() = (0, 0x7fff_ffff, 0);
        assert!(offer(&leader, Trigger::StartedCommit));
    }

    /// The dual: a rule aimed at a non-leader shard fires only there.
    #[test]
    fn a_rule_aimed_at_a_non_leader_fires_in_no_other_shard() {
        let rule = || {
            vec![
                FaultRule::crash_at(Trigger::StartedCommit, 1)
                    .in_shard(ShardTarget::SplitNonLeader),
            ]
        };

        for (name, range) in [
            ("the unsplit parent", (0u32, u32::MAX, 0u32)),
            ("the split leader", (0, 0x7fff_ffff, 0)),
        ] {
            let (shim, _dir) = shim(rule());
            *shim.range.lock().unwrap() = range;
            assert!(
                !offer(&shim, Trigger::StartedCommit),
                "a rule aimed at a non-leader must not fire in {name}",
            );
        }

        let (other, _dir) = shim(rule());
        *other.range.lock().unwrap() = (0x8000_0000, u32::MAX, 0);
        assert!(offer(&other, Trigger::StartedCommit));
    }

    /// One-shot per run, enforced by a marker file rather than in-process state, so
    /// that a restarted connector does not crash at the same point forever.
    #[test]
    fn a_rule_fires_only_once_even_across_processes() {
        let (shim, dir) = shim(vec![FaultRule::crash_at(Trigger::Acknowledge, 1)]);
        assert!(offer(&shim, Trigger::Acknowledge));

        // A second shim over the same run directory stands in for the replacement
        // process the runtime starts after a crash.
        let successor = Shim::new(
            dir.path().to_string_lossy().to_string(),
            Some(serde_json::to_string(&[FaultRule::crash_at(Trigger::Acknowledge, 1)]).unwrap()),
            connector_init::Codec::Json,
        )
        .unwrap();
        assert!(
            !offer(&successor, Trigger::Acknowledge),
            "the successor must not re-fire the rule",
        );
    }

    #[test]
    fn session_scoped_triggers_are_not_reset_by_a_transaction_boundary() {
        let (shim, _dir) = shim(vec![FaultRule::crash_at(Trigger::Acknowledge, 2)]);

        assert!(!offer(&shim, Trigger::Acknowledge));
        commit(&shim);
        assert!(offer(&shim, Trigger::Acknowledge));
    }
}
