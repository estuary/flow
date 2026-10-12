//! C08: the host kernel OOM-kills one VMM, confined to that VMM's own
//! cgroup, while it serves a live connector session through the launcher,
//! beside a sibling VMM whose Spec must keep being answered. Unlike a guest's
//! own OOM (`oom_panic`, C07-MEM), the host's memcg killer ends the VMM.

use super::c07::{append, cgroup_keyed, host_load, judge, keyed, probing, spec_rendering};
use super::*;
use connector_vmm_tests::launcher;
use proto_flow::{connector as proto, derive, flow, ops};

/// The launcher tests' bounds: a launch pulls its image; a release ends it.
const STARTED: Duration = Duration::from_secs(180);
const RELEASED: Duration = Duration::from_secs(60);
/// How soon a session must report the death of its VMM.
const FAILED: Duration = Duration::from_secs(30);
/// Each VMM's container allowance, 512 MiB of it guest RAM.
const ALLOWANCE: u64 = 768 * MIB;
/// The offender's lowered limit: below the guest RAM it has touched (about
/// 60 MiB once derive-python answered), which no reclaim can bring under it
/// once its swap allowance is gone.
const LOWERED: u64 = 16 * MIB;

type Session = (
    tokio::sync::mpsc::Sender<proto::Request>,
    tokio::sync::mpsc::Receiver<tonic::Result<proto::Response>>,
    proto::response::Started,
);

/// A launcher beneath the shared `state_dir`, through `root`'s podman, so
/// that each launch's name is known.
fn router(run: &run::Run, root: &launcher::Root, state_dir: &str) -> connector::ServiceRouter {
    let vmm = connector::Vmm {
        image: run.vmm_image.clone(),
        podman: root.podman.clone(),
        state_dir: state_dir.to_string(),
        disk_mib: 2048,
        memory_bytes: ALLOWANCE,
        cpu_limit: "2".to_string(),
        guest_memory_mib: 512,
        vcpus: 2,
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

/// A derive-python Spec session in a VMM, kept open once its Spec response,
/// which follows Started, arrives: its RPC into the guest stays established
/// until the sender drops.
fn session(
    runtime: &tokio::runtime::Runtime,
    router: &connector::ServiceRouter,
    image: &str,
    logs: &std::sync::Mutex<Vec<String>>,
) -> Session {
    let logger = |log: &ops::Log| {
        logs.lock()
            .unwrap()
            .push(format!("{}: {}", log.level().as_str_name(), log.message))
    };
    let request = proto::Request {
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
    };
    // Within the runtime: a timeout's sleep needs its timer.
    let mut session = runtime
        .block_on(async {
            tokio::time::timeout(
                STARTED,
                proto_grpc::connector::start(
                    router,
                    &logger,
                    proto_grpc::connector::SPEC_TASK_NAME,
                    request,
                ),
            )
            .await
        })
        .unwrap_or_else(|_| panic!("no Started within {STARTED:?}: {:#?}", logs.lock()))
        .unwrap_or_else(|err| panic!("starting: {err:#}\n{:#?}", logs.lock()));
    let Some(proto::response::started::Spec::Derive(spec)) = &session.2.spec else {
        panic!("expected a derivation's Spec, got {:?}", session.2.spec);
    };
    assert!(!spec.config_schema_json.is_empty(), "a config schema");

    let response = runtime.block_on(async {
        tokio::time::timeout(STARTED, async {
            loop {
                match session.1.recv().await {
                    Some(Ok(proto::Response {
                        kind: Some(proto::response::Kind::Log(log)),
                    })) => logger(&log),
                    other => return other,
                }
            }
        })
        .await
    });
    match response {
        Ok(Some(Ok(proto::Response {
            kind:
                Some(proto::response::Kind::Derive(derive::Response {
                    kind: Some(derive::response::Kind::Spec(spec)),
                    ..
                })),
        }))) if spec.protocol == 3032023 && !spec.config_schema_json.is_empty() => session,
        other => panic!("expected the session's Spec response, got {other:?}"),
    }
}

/// Whatever a session has sent since it was last asked, without waiting:
/// its logs, then anything else, and whether it has ended.
fn pending(session: &mut Session, logs: &std::sync::Mutex<Vec<String>>) -> (Vec<String>, bool) {
    let mut other = Vec::new();
    loop {
        match session.1.try_recv() {
            Ok(Ok(proto::Response {
                kind: Some(proto::response::Kind::Log(log)),
            })) => {
                logs.lock()
                    .unwrap()
                    .push(format!("{}: {}", log.level().as_str_name(), log.message))
            }
            Ok(response) => other.push(format!("{response:?}")),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return (other, false),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return (other, true),
        }
    }
}

/// Ends a session as a client done with it, awaiting its release.
fn end(runtime: &tokio::runtime::Runtime, session: Session) -> bool {
    let (request_tx, mut response_rx, _started) = session;
    std::mem::drop(request_tx);
    runtime
        .block_on(async {
            tokio::time::timeout(RELEASED, async {
                while response_rx.recv().await.is_some() {}
            })
            .await
        })
        .is_ok()
}

fn read_events(file: &std::fs::File) -> Result<std::collections::BTreeMap<String, u64>, String> {
    let mut buffer = vec![0u8; 4096];
    std::os::unix::fs::FileExt::read_at(file, &mut buffer, 0)
        .map(|n| keyed(&String::from_utf8_lossy(&buffer[..n])))
        .map_err(|e| e.to_string())
}

/// Whether `marker` is the empty `oom` file conmon makes in its working
/// directory, inherited from this test's (the crate's), on an OOM it sees:
/// root's, regular, empty, absent before and made since `since`. Anything
/// else there (preexisting, written, linked or another's) isn't the test's.
fn ours(existed: bool, marker: &std::fs::Metadata, since: std::time::SystemTime) -> bool {
    !existed
        && marker.file_type().is_file()
        && marker.len() == 0
        && std::os::unix::fs::MetadataExt::uid(marker) == 0
        && marker.modified().is_ok_and(|at| at >= since)
}

fn kernel_log() -> Vec<String> {
    run::sudo(&["dmesg"]).lines().map(str::to_string).collect()
}

/// Polls `done` until it holds or `within` passes, and when it held.
fn until(t0: Instant, within: Duration, mut done: impl FnMut() -> bool) -> Option<Duration> {
    let began = Instant::now();
    loop {
        if done() {
            return Some(t0.elapsed());
        }
        if began.elapsed() > within {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// C08: the host kernel OOM-kills an offender VMM, by a lowered memory.max
/// of its own cgroup without swap, while its derive-python session is live.
/// The session must fail promptly, its VMM be released or recovered by a
/// launch sharing its state, a fresh launch serve, and a sibling launch keep
/// answering its Spec throughout. Writes `c08.evidence`; fails at the end
/// naming every violated bound, or as inconclusive if the kill is unproven.
#[test]
fn host_oom() {
    let run = run::load();
    let marker = std::env::current_dir()
        .expect("a working directory")
        .join("oom");
    let (marker_existed, since) = (
        std::fs::symlink_metadata(&marker).is_ok(),
        std::time::SystemTime::now(),
    );
    let (offender_root, sibling_root, fresh_root) =
        (launcher::root(), launcher::root(), launcher::root());
    // SAFETY: no other thread exists yet to read the environment.
    unsafe { std::env::set_var("TMPDIR", &offender_root.tmp_dir) };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    let (state_dir, tmp_dir) = (&offender_root.state_dir, &offender_root.tmp_dir);
    let socket_of = |name: &str| format!("{state_dir}/{name}/sock/init.sock");
    let path = run.run_dir.join("c08.evidence");
    let image = run.derive_python_image.as_str();
    let t0 = Instant::now();
    let mut violations: Vec<String> = Vec::new();
    let mut inconclusive: Vec<String> = Vec::new();

    let routers =
        [&sibling_root, &offender_root, &fresh_root].map(|root| router(&run, root, state_dir));
    let [sibling_router, offender_router, fresh_router] = &routers;
    let (sibling_logs, offender_logs, fresh_logs) = Default::default();
    let (sibling_logs, offender_logs, fresh_logs): (
        &std::sync::Mutex<Vec<String>>,
        &std::sync::Mutex<Vec<String>>,
        &std::sync::Mutex<Vec<String>>,
    ) = (&sibling_logs, &offender_logs, &fresh_logs);

    let mut sibling = session(&runtime, sibling_router, image, sibling_logs);
    let sibling_name = launcher::vmm_name(&sibling_root).expect("the launch named its network");
    let sibling_print = launcher::footprint(state_dir, tmp_dir, &sibling_name);
    let mut offender = session(&runtime, offender_router, image, offender_logs);
    let offender_name = launcher::vmm_name(&offender_root).expect("the launch named its network");
    let offender_print = launcher::footprint(state_dir, tmp_dir, &offender_name);
    let (vmm_pid, vmm_start) = (
        offender_print.processes[0].1,
        offender_print.processes[0].2.clone(),
    );
    let cgroup = offender_print.cgroup.clone();
    let parent = std::path::Path::new(&cgroup)
        .parent()
        .expect("a parent cgroup")
        .to_string_lossy()
        .into_owned();
    let sibling_socket = socket_of(&sibling_name);

    let limits = |cgroup: &str| {
        let cat = |file: &str| {
            run::sudo(&["cat", &format!("{cgroup}/{file}")])
                .trim()
                .to_string()
        };
        let stat = cgroup_keyed(cgroup, "memory.stat");
        serde_json::json!({
            "memory.max": cat("memory.max"), "memory.swap.max": cat("memory.swap.max"),
            "memory.current": cat("memory.current"), "memory.swap.current": cat("memory.swap.current"),
            "memory.oom.group": cat("memory.oom.group"), "anon": stat["anon"], "file": stat["file"],
            "memory.events": cgroup_keyed(cgroup, "memory.events"),
        })
    };
    let (offender_limits, sibling_limits) = (limits(&cgroup), limits(&sibling_print.cgroup));
    append(
        &path,
        serde_json::json!({
            "offender": {"tenant": "acmeCo/c08-offender", "footprint": format!("{offender_print:?}"),
                         "limits": offender_limits, "started": format!("{:?}", offender.2)},
            "sibling": {"tenant": "acmeCo/c08-sibling", "footprint": format!("{sibling_print:?}"),
                        "limits": sibling_limits},
            "parent": parent, "host": host_load(),
        }),
    );

    // C08-B0: both sessions live; the sibling's Spec, once a second.
    let ((), served) = probing(&sibling_socket, t0, Duration::from_secs(5), &path, || {
        std::thread::sleep(Duration::from_secs(9))
    });
    let reference = served
        .first()
        .and_then(|probe| probe.result.clone().ok())
        .unwrap_or_else(|| panic!("C08-B0 inconclusive: the first Spec failed"));
    let slowest = served
        .iter()
        .map(|p| p.end - p.start)
        .max()
        .unwrap_or_default();
    let budget = (slowest * 10).max(Duration::from_secs(2));
    let span = served.last().map(|p| p.end).unwrap_or_default()
        - served.first().map(|p| p.start).unwrap_or_default();
    let b0: Vec<String> = judge(
        "B0",
        &served,
        &reference,
        Duration::from_secs(5),
        (Duration::ZERO, Duration::ZERO),
        0,
    )
    .into_iter()
    // B0 has no fault window, so nothing can be before it.
    .filter(|violation| !violation.contains("no probe before or after"))
    .collect();
    let available = host_load()["mem_available_kib"]
        .as_u64()
        .unwrap_or_default()
        << 10;
    let anon = offender_limits["anon"].as_u64().unwrap_or_default();
    append(
        &path,
        serde_json::json!({"summary": "B0", "probes": served.len(), "span_ms": span.as_millis(),
                           "slowest_ms": slowest.as_millis(), "budget_ms": budget.as_millis(),
                           "fingerprint": format!("{:016x}", super::c07::fingerprint(&reference)),
                           "violations": b0}),
    );
    assert!(
        b0.is_empty()
            && served.len() >= 8
            && span >= Duration::from_secs(8)
            && budget <= Duration::from_secs(5)
            && offender_limits["memory.max"] == ALLOWANCE.to_string()
            && sibling_limits["memory.max"] == ALLOWANCE.to_string()
            && anon >= 2 * LOWERED
            && available >= 4 << 30,
        "C08-B0 inconclusive, fault not run: {} probes over {span:?}, slowest {slowest:?}, \
         budget {budget:?}, {b0:?}, offender {offender_limits}, sibling {sibling_limits}, \
         available {available}",
        served.len()
    );

    // C08-OOM. Counters are read through descriptors opened beforehand: the
    // container is `--rm`, so its cgroup may be gone moments after the kill.
    let events_file = format!("{cgroup}/memory.events");
    let open = || {
        std::fs::File::open(&events_file).unwrap_or_else(|e| panic!("opening {events_file}: {e}"))
    };
    let (events_fd, sampler_fd) = (open(), open());
    let events_before = read_events(&events_fd).expect("reading memory.events before the fault");
    let parent_before = cgroup_keyed(&parent, "memory.events");
    let kernel_before: std::collections::BTreeSet<String> = kernel_log().into_iter().collect();
    append(
        &path,
        serde_json::json!({"pre_fault": {"memory.events": events_before,
                           "parent.memory.events": parent_before, "dmesg_lines": kernel_before.len()}}),
    );

    let boundary = |name: &str, violations: &mut Vec<String>| {
        let start = t0.elapsed();
        let result = spec_rendering(&runtime, &sibling_socket, budget);
        let took = t0.elapsed() - start;
        if result.as_ref() != Ok(&reference) || took > budget {
            violations.push(format!("{name} boundary probe: {took:?} {result:?}"));
        }
        serde_json::json!({"start_ms": start.as_millis(), "ms": took.as_millis(),
                           "correct": result.as_ref() == Ok(&reference)})
    };

    let stop = std::sync::atomic::AtomicBool::new(false);
    let (fault, served) = probing(&sibling_socket, t0, budget, &path, || {
        std::thread::sleep(Duration::from_secs(3));
        std::thread::scope(|scope| {
            let sampler = scope.spawn(|| {
                let (mut samples, mut last) = (0, None);
                let error = loop {
                    match read_events(&sampler_fd) {
                        Ok(events) => (samples, last) = (samples + 1, Some(events)),
                        Err(error) => break Some(error),
                    }
                    if stop.load(std::sync::atomic::Ordering::SeqCst) {
                        break None;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                };
                serde_json::json!({"samples": samples, "last": last, "error": error})
            });
            // A panic below must still end the sampler, which the scope joins.
            struct Stop<'a>(&'a std::sync::atomic::AtomicBool);
            impl Drop for Stop<'_> {
                fn drop(&mut self) {
                    self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
            let stopping = Stop(&stop);

            let before = boundary("before", &mut violations);
            let (sent, ended) = pending(&mut offender, offender_logs);
            let alive = launcher::start_time(vmm_pid).as_ref() == Some(&vmm_start);
            if !sent.is_empty() || ended || !alive {
                inconclusive.push(format!(
                    "the offender's session was not live at the fault: sent {sent:?}, \
                     ended {ended}, VMM alive {alive}"
                ));
            }
            let write = t0.elapsed();
            let wrote = run::sudo_output(
                &[
                    "timeout",
                    "10",
                    "sh",
                    "-c",
                    &format!(
                        "echo 0 > {cgroup}/memory.swap.max && echo {LOWERED} > {cgroup}/memory.max; \
                         echo status=$?; cat {cgroup}/memory.events {cgroup}/memory.current"
                    ),
                ],
                None,
            );
            let wrote_at = t0.elapsed();
            let events_after = read_events(&events_fd);
            let after = boundary("after", &mut violations);
            let gone = until(t0, Duration::from_secs(5), || {
                launcher::start_time(vmm_pid).as_ref() != Some(&vmm_start)
            });

            // The session: its first failure, anything else it sends, its end.
            let (mut failed, mut ended, mut responses) = (None, None, Vec::new());
            runtime.block_on(async {
                let deadline =
                    tokio::time::Instant::now() + RELEASED.saturating_sub(t0.elapsed() - write);
                while let Ok(item) = tokio::time::timeout_at(deadline, offender.1.recv()).await {
                    let at = t0.elapsed();
                    match item {
                        None => {
                            failed.get_or_insert(at);
                            ended = Some(at);
                            break;
                        }
                        Some(Err(status)) => {
                            failed.get_or_insert(at);
                            responses.push(format!(
                                "{at:?} {:?}: {}",
                                status.code(),
                                status.message()
                            ));
                        }
                        Some(Ok(proto::Response {
                            kind: Some(proto::response::Kind::Log(log)),
                        })) => offender_logs.lock().unwrap().push(format!(
                            "{}: {}",
                            log.level().as_str_name(),
                            log.message
                        )),
                        Some(Ok(response)) => {
                            violations.push(format!("a response after the kill: {response:?}"));
                            responses.push(format!("{at:?} {response:?}"));
                        }
                    }
                }
            });
            let released = until(t0, RELEASED.saturating_sub(t0.elapsed() - write), || {
                launcher::remaining(&offender_print).is_empty()
            });
            let left = launcher::remaining(&offender_print);
            std::mem::drop(stopping);
            let sampled = sampler.join().expect("the sampler");

            append(
                &path,
                serde_json::json!({
                    "fault": {"write_ms": write.as_millis(), "wrote_ms": wrote_at.as_millis(),
                              "lowered": LOWERED, "cgroup": cgroup,
                              "status": wrote.status.code(),
                              "stdout": String::from_utf8_lossy(&wrote.stdout),
                              "stderr": String::from_utf8_lossy(&wrote.stderr)},
                    "events_after_fd": format!("{events_after:?}"), "sampled": sampled,
                    "boundary_before": before, "boundary_after": after,
                    "vmm_gone_ms": gone.map(|at| at.as_millis()),
                    "failed_ms": failed.map(|at: Duration| at.as_millis()),
                    "ended_ms": ended.map(|at: Duration| at.as_millis()), "responses": responses,
                    "released_ms": released.map(|at| at.as_millis()), "left": left,
                }),
            );

            // A launch sharing the state directory, which releases whatever
            // the dead one's owner left, then serves.
            let fresh = session(&runtime, fresh_router, image, fresh_logs);
            let fresh_started = t0.elapsed();
            let fresh_name = launcher::vmm_name(&fresh_root).expect("the launch named its network");
            let fresh_spec = spec_rendering(&runtime, &socket_of(&fresh_name), budget);
            let fresh_served = t0.elapsed();
            append(
                &path,
                serde_json::json!({"fresh": {"name": fresh_name, "started_ms": fresh_started.as_millis(),
                                   "served_ms": fresh_served.as_millis(),
                                   "spec_correct": fresh_spec.as_ref() == Ok(&reference),
                                   "spec_error": fresh_spec.as_ref().err()}}),
            );
            std::thread::sleep(Duration::from_secs(3));
            let sampled_kills = sampled["last"]["oom_kill"].as_u64();
            (
                write,
                wrote_at,
                events_after,
                sampled_kills,
                gone,
                failed,
                ended,
                released,
                left,
                fresh,
                fresh_name,
                fresh_spec,
                fresh_served,
            )
        })
    });
    let (
        write,
        wrote_at,
        events_after,
        sampled_kills,
        gone,
        failed,
        ended,
        released,
        left,
        fresh,
        fresh_name,
        fresh_spec,
        fresh_served,
    ) = fault;

    // The kill's proof: the offender cgroup's own counter, read through a
    // descriptor opened before the fault, the parent's, which survives the
    // child, and the kernel's own account of whom it killed in which cgroup.
    let killed = |events: &std::collections::BTreeMap<String, u64>| {
        events["oom_kill"].saturating_sub(events_before["oom_kill"])
    };
    let own = events_after
        .as_ref()
        .map(killed)
        .ok()
        .or(sampled_kills.map(|kills| kills.saturating_sub(events_before["oom_kill"])));
    let parent_after = cgroup_keyed(&parent, "memory.events");
    let parent_killed = parent_after["oom_kill"] - parent_before["oom_kill"];
    let relative = cgroup.strip_prefix("/sys/fs/cgroup").unwrap_or(&cgroup);
    let kernel: Vec<String> = kernel_log()
        .into_iter()
        .filter(|line| !kernel_before.contains(line))
        .filter(|line| {
            line.contains("oom")
                || line.contains("Out of memory")
                || line.contains("Killed process")
        })
        .collect();
    let charged = kernel.iter().any(|line| {
        line.contains(&format!("oom_memcg={relative},"))
            && line.contains(&format!(",pid={vmm_pid},"))
    });
    let victim = kernel.iter().any(|line| {
        line.contains("Memory cgroup out of memory")
            && line.contains(&format!("Killed process {vmm_pid} "))
    });
    let guest_text: Vec<String> = offender_logs
        .lock()
        .unwrap()
        .iter()
        .filter(|log| {
            log.contains("Kernel panic")
                || log.contains("Out of memory")
                || log.contains("oom-kill")
        })
        .cloned()
        .collect();
    if !own.is_some_and(|kills| kills >= 1) {
        inconclusive.push(format!(
            "no oom_kill delta in the offender's own memory.events: fd {events_after:?}, \
             sampled {sampled_kills:?}, before {events_before:?}"
        ));
    }
    if parent_killed < 1 {
        inconclusive.push(format!(
            "no oom_kill delta in {parent}: {parent_before:?} -> {parent_after:?}"
        ));
    }
    if !(charged && victim) {
        inconclusive.push(format!(
            "no kernel line of VMM {vmm_pid} killed in {relative}: {kernel:#?}"
        ));
    }
    if gone.is_none() {
        inconclusive.push(format!("VMM {vmm_pid} still ran 5 s after the fault"));
    }
    if !guest_text.is_empty() {
        inconclusive.push(format!("guest OOM or panic text: {guest_text:?}"));
    }

    // The product's response to it.
    match failed {
        Some(at) if at - wrote_at <= FAILED => {}
        _ => violations.push(format!(
            "the session reported no failure within {FAILED:?} of the kill: {failed:?}"
        )),
    }
    if ended.is_none() {
        violations.push(format!(
            "the session did not end within {RELEASED:?} of the fault"
        ));
    }
    let recovered = launcher::remaining(&offender_print);
    if !recovered.is_empty() {
        violations.push(format!(
            "the dead VMM's remains outlived a fresh launch: {recovered:?}"
        ));
    }
    let socket = socket_of(&offender_name);
    if launcher::path_exists(&socket) {
        violations.push(format!("the dead VMM's socket remains: {socket}"));
    }
    if released.is_none() {
        let removal = format!("rm --force --time=0 --ignore {}", offender_print.id);
        if !launcher::calls(&fresh_root).contains(&removal) {
            violations.push(format!(
                "the owner left {left:?}, and the fresh launch did not remove the container"
            ));
        }
    }
    if fresh_name == offender_name || fresh_spec.as_ref() != Ok(&reference) {
        violations.push(format!("the fresh launch {fresh_name}: {fresh_spec:?}"));
    }

    // The sibling throughout: before the fault, within the fault, release and
    // recovery, and after the fresh launch's successful Spec.
    violations.extend(judge(
        "OOM",
        &served,
        &reference,
        budget,
        (write, fresh_served),
        1,
    ));
    for (role, pid, start) in &sibling_print.processes {
        if launcher::start_time(*pid).as_ref() != Some(start) {
            violations.push(format!("the sibling's {role} is no longer {pid}"));
        }
    }
    let present = launcher::remaining(&sibling_print).len();
    if present != 8 + sibling_print.veths.len() + sibling_print.paths.len() {
        violations.push(format!(
            "the sibling's footprint is no longer whole: {present} remain"
        ));
    }
    let sibling_after = limits(&sibling_print.cgroup);
    if sibling_after["memory.events"]["oom_kill"] != 0
        || sibling_after["memory.max"] != ALLOWANCE.to_string()
    {
        violations.push(format!("the sibling's cgroup: {sibling_after}"));
    }
    let (sent, sibling_ended) = pending(&mut sibling, sibling_logs);
    if !sent.is_empty() || sibling_ended {
        violations.push(format!(
            "the sibling's session: sent {sent:?}, ended {sibling_ended}"
        ));
    }

    // Cleanup, as clients done with their sessions.
    std::mem::drop(offender);
    for (name, session) in [("fresh", fresh), ("sibling", sibling)] {
        if !end(&runtime, session) {
            violations.push(format!(
                "the {name} session did not end within {RELEASED:?}"
            ));
        }
    }
    for name in [&sibling_name, &offender_name, &fresh_name] {
        let left = launcher::left(state_dir, tmp_dir, name);
        if !left.is_empty() {
            violations.push(format!("{name} left {left:?}"));
        }
    }
    let marker_found = std::fs::symlink_metadata(&marker).ok();
    let removed_marker = marker_found
        .as_ref()
        .is_some_and(|found| ours(marker_existed, found, since));
    if removed_marker {
        run::sudo(&["rm", "--", &marker.to_string_lossy()]);
    }
    append(
        &path,
        serde_json::json!({"oom_marker": {"path": marker, "existed": marker_existed,
                           "found": marker_found.is_some(), "removed": removed_marker}}),
    );
    append(
        &path,
        serde_json::json!({"summary": "OOM", "own_oom_kill_delta": own, "parent_oom_kill_delta": parent_killed,
                           "parent.memory.events.after": parent_after, "kernel": kernel,
                           "charged": charged, "victim": victim, "guest_text": guest_text,
                           "sibling_after": sibling_after, "probes": served.len(),
                           "offender_logs": *offender_logs.lock().unwrap(),
                           "inconclusive": inconclusive, "violations": violations}),
    );
    assert!(
        inconclusive.is_empty() && violations.is_empty(),
        "C08 inconclusive {inconclusive:#?}\nviolations {violations:#?}"
    );
}

/// `ours` over synthetic markers in a directory of this test's own: only an
/// empty regular file of root's, absent before and made since, is removable.
#[test]
fn oom_marker() {
    let dir = std::env::temp_dir().join(format!("c08-oom-marker-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap_or_else(|e| panic!("creating {}: {e}", dir.display()));
    let since = std::time::SystemTime::now() - Duration::from_secs(1);
    let path = |name: &str| dir.join(name).to_string_lossy().into_owned();
    run::sudo(&["touch", &path("fresh")]);
    run::sudo(&["sh", "-c", &format!("echo x > {}", path("written"))]);
    run::sudo(&["touch", "-d", "1 hour ago", &path("old")]);
    std::fs::write(path("mine"), b"").expect("writing mine");
    std::os::unix::fs::symlink(path("fresh"), path("link")).expect("linking");

    let ours_at = |name: &str, existed: bool| {
        ours(
            existed,
            &std::fs::symlink_metadata(path(name)).expect(name),
            since,
        )
    };
    let verdicts = [
        ours_at("fresh", false),
        ours_at("fresh", true),
        ours_at("written", false),
        ours_at("old", false),
        ours_at("mine", false),
        ours_at("link", false),
    ];
    run::sudo(&["rm", "-r", "--", &dir.to_string_lossy()]);
    assert_eq!(verdicts, [true, false, false, false, false, false]);
}
