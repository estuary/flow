//! C07: a noisy tenant beside a healthy one. An offender's guest at its CPU
//! quota, out of guest memory, then out of scratch disk, one at a time beside
//! a real derive-python VMM which must keep serving its Spec, correctly and
//! within a budget set by a healthy paired baseline. That Spec is connector
//! work, not a derivation's throughput.

use super::*;

/// The launcher tests' `RELEASED`: an offender's removal must finish within it.
const REMOVED: Duration = Duration::from_secs(60);
/// What the host's root filesystem must have free before a scratch fill.
const RESERVE: u64 = 10 << 30;

/// One derive-python Spec over `socket`: the noisy-tenant peer's useful work.
/// Its rendering, or why there is none, rather than a panic, so a prober
/// thread keeps every failure.
pub(super) fn spec_rendering(
    runtime: &tokio::runtime::Runtime,
    socket: &str,
    timeout: Duration,
) -> Result<String, String> {
    let socket = socket.to_string();
    runtime.block_on(async move {
        let call = async move {
            let channel = tonic::transport::Endpoint::from_static("http://[::1]:0")
                .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                    let socket = socket.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(socket).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }))
                .await
                .map_err(|e| format!("connecting: {e}"))?;
            let mut client = proto_grpc::derive::connector_client::ConnectorClient::new(channel);
            let request = proto_flow::derive::Request {
                kind: Some(proto_flow::derive::request::Kind::Spec(
                    proto_flow::derive::request::Spec {
                        connector_type:
                            proto_flow::flow::collection_spec::derivation::ConnectorType::Image
                                as i32,
                        config_json: "{}".into(),
                    },
                )),
                ..Default::default()
            };
            let responses = client
                .derive(futures::stream::once(async { request }))
                .await
                .map_err(|status| format!("the Spec RPC: {status}"))?
                .into_inner();
            let responses: Vec<proto_flow::derive::Response> =
                futures::TryStreamExt::try_collect(responses)
                    .await
                    .map_err(|status| format!("the Spec responses: {status}"))?;
            match responses.as_slice() {
                [
                    proto_flow::derive::Response {
                        kind: Some(proto_flow::derive::response::Kind::Spec(spec)),
                        ..
                    },
                ] if spec.protocol == 3032023 && !spec.config_schema_json.is_empty() => {
                    Ok(format!("{spec:?}"))
                }
                other => Err(format!("unexpected responses {other:?}")),
            }
        };
        tokio::time::timeout(timeout, call)
            .await
            .unwrap_or_else(|_| Err(format!("no Spec within {timeout:?}")))
    })
}

pub(super) struct Served {
    pub(super) start: Duration,
    pub(super) end: Duration,
    pub(super) result: Result<String, String>,
}

pub(super) fn fingerprint(rendering: &str) -> u64 {
    std::hash::BuildHasher::hash_one(
        &std::hash::BuildHasherDefault::<std::hash::DefaultHasher>::default(),
        rendering,
    )
}

pub(super) fn append(path: &std::path::Path, line: serde_json::Value) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap_or_else(|e| panic!("opening {}: {e}", path.display()));
    std::io::Write::write_all(&mut file, format!("{line}\n").as_bytes())
        .unwrap_or_else(|e| panic!("appending to {}: {e}", path.display()));
}

/// Ask the peer at `socket` for a Spec once a second while `drive` runs,
/// appending each answer to `evidence` as it arrives, so a failing run keeps
/// them. One that overruns its second is followed at once.
pub(super) fn probing<T>(
    socket: &str,
    t0: Instant,
    timeout: Duration,
    evidence: &std::path::Path,
    drive: impl FnOnce() -> T,
) -> (T, Vec<Served>) {
    struct Stop<'a>(&'a std::sync::atomic::AtomicBool);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let stop = std::sync::atomic::AtomicBool::new(false);

    std::thread::scope(|scope| {
        let prober = scope.spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a tokio runtime");
            let mut served = Vec::new();
            let mut next = Instant::now();
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                let start = t0.elapsed();
                let result = spec_rendering(&runtime, socket, timeout);
                let end = t0.elapsed();
                append(
                    evidence,
                    serde_json::json!({
                        "probe": served.len(),
                        "start_ms": start.as_millis(),
                        "end_ms": end.as_millis(),
                        "ms": (end - start).as_millis(),
                        "fingerprint": result.as_ref().ok().map(|r| format!("{:016x}", fingerprint(r))),
                        "bytes": result.as_ref().ok().map(String::len),
                        "error": result.as_ref().err(),
                    }),
                );
                served.push(Served { start, end, result });
                next += Duration::from_secs(1);
                let now = Instant::now();
                if next > now {
                    std::thread::sleep(next - now);
                } else {
                    next = now;
                }
            }
            served
        });
        let value = {
            let _stop = Stop(&stop);
            drive()
        };
        (value, prober.join().expect("the prober"))
    })
}

/// What `served` violated: a wrong, failed or slow answer, more than two
/// seconds between asks, fewer than `inside` answers wholly within the
/// pressure `window`, or none before or after it.
pub(super) fn judge(
    variant: &str,
    served: &[Served],
    reference: &str,
    budget: Duration,
    window: (Duration, Duration),
    inside: usize,
) -> Vec<String> {
    let mut violations = Vec::new();
    for (i, probe) in served.iter().enumerate() {
        let took = probe.end - probe.start;
        match &probe.result {
            Err(error) => violations.push(format!("{variant} probe {i}: {error}")),
            Ok(rendering) if rendering != reference => {
                violations.push(format!("{variant} probe {i}: a different Spec {rendering}"))
            }
            Ok(_) if took > budget => violations.push(format!(
                "{variant} probe {i}: {took:?} beyond the {budget:?} budget"
            )),
            Ok(_) => {}
        }
    }
    for pair in served.windows(2) {
        let gap = pair[1].start - pair[0].start;
        if gap > Duration::from_secs(2) {
            violations.push(format!(
                "{variant}: {gap:?} between probes at {:?}",
                pair[0].start
            ));
        }
    }
    let within = served
        .iter()
        .filter(|p| p.start >= window.0 && p.end <= window.1)
        .count();
    if within < inside {
        violations.push(format!(
            "{variant}: {within} probes within {window:?}, fewer than {inside}"
        ));
    }
    if !served.iter().any(|p| p.end <= window.0) || !served.iter().any(|p| p.start >= window.1) {
        violations.push(format!("{variant}: no probe before or after {window:?}"));
    }
    violations
}

/// A noisy tenant's VMM: 256 MiB of guest RAM and scratch disk, two vCPUs and,
/// unlike the reference line, one host CPU of quota.
fn offender(run: &run::Run) -> (run::Vmm, guest::Guest) {
    start(
        run,
        run::Spec {
            memory_mib: 256,
            disk_mib: 256,
            edit: |argv| {
                for argument in argv.iter_mut() {
                    if argument == "--cpus=2" {
                        *argument = "--cpus=1".to_string();
                    }
                }
            },
            ..run::Spec::probes(run, "none.json")
        },
    )
}

fn cgroup_of(vmm: &run::Vmm) -> String {
    format!(
        "/sys/fs/cgroup{}",
        run::inspect(&vmm.name, "{{.State.CgroupPath}}")
    )
}

/// A flat-keyed file such as `cpu.stat`, `memory.events` or guest `vmstat`.
pub(super) fn keyed(text: &str) -> std::collections::BTreeMap<String, u64> {
    text.lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(' ')?;
            Some((key.to_string(), value.trim().parse().ok()?))
        })
        .collect()
}

pub(super) fn cgroup_keyed(cgroup: &str, file: &str) -> std::collections::BTreeMap<String, u64> {
    keyed(&run::sudo(&["cat", &format!("{cgroup}/{file}")]))
}

/// What else the host was doing, to tell its contention from the offender's.
pub(super) fn host_load() -> serde_json::Value {
    let read = |path: &str| std::fs::read_to_string(path).unwrap_or_else(|e| e.to_string());
    serde_json::json!({
        "loadavg": read("/proc/loadavg").trim(),
        "cpu_pressure": read("/proc/pressure/cpu"),
        "memory_pressure": read("/proc/pressure/memory"),
        "mem_available_kib": keyed(&read("/proc/meminfo").replace(":", "").replace(" kB", ""))
            .get("MemAvailable"),
        "root_avail_bytes": root_avail(),
    })
}

/// The bytes free on the filesystem beneath the suite's root, where scratch
/// backings live.
fn root_avail() -> u64 {
    let output = std::process::Command::new("df")
        .args(["-B1", "--output=avail", run::ROOT])
        .output()
        .expect("running df");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .last()
        .and_then(|line| line.trim().parse().ok())
        .unwrap_or_else(|| panic!("df of {}: {output:?}", run::ROOT))
}

/// Remove a VMM as a release would, and how long until its container, its
/// process and its cgroup were each seen gone: within `REMOVED` in all, the
/// `podman rm` included, which `timeout` bounds.
fn remove(vmm: &run::Vmm, pid: u32, cgroup: &str) -> Duration {
    let began = Instant::now();
    let _ = run::sudo_output(
        &[
            "timeout",
            &REMOVED.as_secs().to_string(),
            "podman",
            "rm",
            "-f",
            "--time",
            "0",
            "--ignore",
            &vmm.name,
        ],
        None,
    );
    run::eventually(
        &format!("{} gone within {REMOVED:?} of its removal", vmm.name),
        REMOVED.saturating_sub(began.elapsed()),
        || {
            let container = run::sudo_output(&["podman", "container", "exists", &vmm.name], None);
            (container.status.code() == Some(1)
                && !exists(&format!("/proc/{pid}"))
                && !exists(cgroup))
            .then_some(())
        },
    );
    began.elapsed()
}

/// A fresh offender after a variant, with the same limits: how long it took
/// to be ready, and whether its own scratch disk takes a write.
fn successor(run: &run::Run, variant: &str, violations: &mut Vec<String>) -> serde_json::Value {
    let (vmm, mut guest) = offender(run);
    let ready = vmm.stderr.lock().unwrap().ready.expect("start waited") - vmm.started;
    let fill = probe(
        &mut guest,
        "successor-fill",
        "fill",
        serde_json::json!({"path": "/tmp/successor", "mib": 1}),
    );
    if fill != serde_json::json!({"written": MIB, "stop": "done"}) {
        violations.push(format!("{variant}: the successor's write {fill}"));
    }
    serde_json::json!({"successor": vmm.name, "ready_ms": ready.as_millis(), "fill": fill})
}

/// The peer is the VMM it was, and its cgroup has not OOM-killed anything.
fn peer_intact(
    peer: &run::Vmm,
    pid: u32,
    cgroup: &str,
    variant: &str,
    violations: &mut Vec<String>,
) {
    if run::pid(peer) != pid {
        violations.push(format!("{variant}: the peer's VMM is no longer {pid}"));
    }
    let events = cgroup_keyed(cgroup, "memory.events");
    if events.get("oom_kill") != Some(&0) {
        violations.push(format!("{variant}: the peer's memory.events {events:?}"));
    }
}

/// C07: a noisy tenant's guest at its CPU quota, out of guest memory, then
/// out of scratch disk, one at a time beside a real derive-python VMM that
/// must keep serving its Spec, correctly and within a budget set by a healthy
/// paired baseline. That Spec is connector work, not a derivation's
/// throughput. One test, so no variant overlaps another; each writes
/// `c07-<variant>.evidence`, and the test fails at the end naming every
/// violated bound.
#[test]
fn noisy_tenant() {
    let run = run::load();
    let t0 = Instant::now();
    let evidence = |variant: &str| run.run_dir.join(format!("c07-{variant}.evidence"));
    let mut violations = Vec::new();

    let peer = run::start(
        &run,
        run::Spec {
            connector_image: run.derive_python_image.clone(),
            exec: None,
            ..run::Spec::probes(&run, "none.json")
        },
    );
    run::wait_ready(&peer, READY);
    let peer_pid = run::pid(&peer);
    let peer_cgroup = cgroup_of(&peer);
    let socket = run::socket_path(&peer).to_string_lossy().into_owned();
    let runtime = tokio::runtime::Runtime::new().expect("a tokio runtime");
    // connector-init ends a VMM serving no RPC within seconds.
    let _held = rpc::hold(&runtime, &socket, READY);

    // C07-B0: the peer beside an idle offender.
    let path = evidence("b0");
    let (vmm, guest) = offender(&run);
    let (pid, cgroup) = (run::pid(&vmm), cgroup_of(&vmm));
    append(
        &path,
        serde_json::json!({
            "peer": {"tenant": "acmeCo/c07-healthy", "name": peer.name, "pid": peer_pid,
                     "cgroup": peer_cgroup, "image": run.derive_python_image,
                     "cpu.max": run::sudo(&["cat", &format!("{peer_cgroup}/cpu.max")]).trim(),
                     "memory.max": run::sudo(&["cat", &format!("{peer_cgroup}/memory.max")]).trim()},
            "offender": {"tenant": "acmeCo/c07-noisy", "name": vmm.name, "pid": pid, "cgroup": cgroup},
            "host": host_load(),
        }),
    );
    let ((), served) = probing(&socket, t0, Duration::from_secs(5), &path, || {
        std::thread::sleep(Duration::from_secs(12))
    });
    drop(guest);
    let removal = remove(&vmm, pid, &cgroup);
    drop(vmm);

    let reference = served
        .first()
        .and_then(|probe| probe.result.clone().ok())
        .unwrap_or_else(|| panic!("C07-B0 inconclusive: the first Spec failed"));
    let slowest = served
        .iter()
        .map(|p| p.end - p.start)
        .max()
        .unwrap_or_default();
    let budget = (slowest * 10).max(Duration::from_secs(2));
    let span = served.last().map(|p| p.end).unwrap_or_default()
        - served.first().map(|p| p.start).unwrap_or_default();
    let b0 = judge(
        "B0",
        &served,
        &reference,
        Duration::from_secs(5),
        (Duration::ZERO, Duration::ZERO),
        0,
    )
    .into_iter()
    // B0 has no pressure window, so nothing can be before it.
    .filter(|violation| !violation.contains("no probe before or after"))
    .collect::<Vec<_>>();
    append(
        &path,
        serde_json::json!({"summary": "B0", "probes": served.len(), "span_ms": span.as_millis(),
                           "slowest_ms": slowest.as_millis(), "budget_ms": budget.as_millis(),
                           "fingerprint": format!("{:016x}", fingerprint(&reference)),
                           "offender_removal_ms": removal.as_millis(), "violations": b0}),
    );
    assert!(
        b0.is_empty()
            && served.len() >= 10
            && span >= Duration::from_secs(10)
            && budget <= Duration::from_secs(5),
        "C07-B0 inconclusive, faults not run: {} probes over {span:?}, slowest {slowest:?}, \
         budget {budget:?}, {b0:?}",
        served.len()
    );

    // C07-CPU: four busy workers on the offender's two vCPUs, against one
    // host CPU of quota, held for over twenty seconds, then removed while busy.
    let path = evidence("cpu");
    let (vmm, mut guest) = offender(&run);
    let (pid, cgroup) = (run::pid(&vmm), cgroup_of(&vmm));
    let cpu_max = run::sudo(&["cat", &format!("{cgroup}/cpu.max")]);
    if cpu_max.trim() != "100000 100000" {
        violations.push(format!("CPU: the offender's cpu.max is {cpu_max:?}"));
    }
    append(
        &path,
        serde_json::json!({"offender": vmm.name, "pid": pid, "cgroup": cgroup,
                           "cpu.max": cpu_max.trim(), "host": host_load()}),
    );
    let (cpu, served) = probing(&socket, t0, budget, &path, || {
        std::thread::sleep(Duration::from_secs(3));
        let fault = t0.elapsed();
        let workers = probe(
            &mut guest,
            "spin",
            "spin",
            serde_json::json!({"workers": 4, "seconds": 45}),
        );
        std::thread::sleep(Duration::from_secs(2));
        let (begin, before) = (t0.elapsed(), cgroup_keyed(&cgroup, "cpu.stat"));
        std::thread::sleep(Duration::from_secs(22));
        let (end, after) = (t0.elapsed(), cgroup_keyed(&cgroup, "cpu.stat"));
        let removal = remove(&vmm, pid, &cgroup);
        let removed = t0.elapsed();
        std::thread::sleep(Duration::from_secs(3));
        (fault, workers, begin, end, before, after, removal, removed)
    });
    let (fault, workers, begin, end, before, after, removal, removed) = cpu;
    drop(guest);
    drop(vmm);
    let delta = |key: &str| after[key] - before[key];
    let used = delta("usage_usec") as f64 / (end - begin).as_micros() as f64;
    if !(0.8..=1.1).contains(&used) {
        violations.push(format!(
            "CPU: the offender used {used:.3} CPUs over the window"
        ));
    }
    if end - begin < Duration::from_secs(20) {
        violations.push(format!("CPU: pressure measured for only {:?}", end - begin));
    }
    if delta("nr_throttled") == 0 || delta("throttled_usec") == 0 {
        violations.push(format!("CPU: no throttling {before:?} -> {after:?}"));
    }
    if removed - fault > Duration::from_secs(60) {
        violations.push(format!("CPU: a fault window of {:?}", removed - fault));
    }
    let cpu_violations = judge("CPU", &served, &reference, budget, (begin, end), 10);
    let successor_cpu = successor(&run, "CPU", &mut violations);
    peer_intact(&peer, peer_pid, &peer_cgroup, "CPU", &mut violations);
    append(
        &path,
        serde_json::json!({"summary": "CPU", "workers": workers, "window_ms": [begin.as_millis(), end.as_millis()],
                           "cpus_used": used, "cpu.stat.before": before, "cpu.stat.after": after,
                           "removal_ms": removal.as_millis(), "successor": successor_cpu,
                           "probes": served.len(), "violations": cpu_violations, "host": host_load()}),
    );
    violations.extend(cpu_violations);

    // C07-MEM: a guest process touching 384 MiB, more than the guest's 256
    // and less than its container's 512, while the guest survives that, for
    // twenty seconds at most. Either the guest's kernel kills it, or the
    // guest dies; the host cgroup must kill nothing either way.
    let path = evidence("mem");
    let (mut vmm, mut guest) = offender(&run);
    let (pid, cgroup) = (run::pid(&vmm), cgroup_of(&vmm));
    let parent = std::path::Path::new(&cgroup)
        .parent()
        .expect("a parent cgroup")
        .to_string_lossy()
        .into_owned();
    let memory_max = run::sudo(&["cat", &format!("{cgroup}/memory.max")]);
    if memory_max.trim() != (512 * MIB).to_string() {
        violations.push(format!("MEM: the offender's memory.max is {memory_max:?}"));
    }
    let host_ooms = || {
        run::sudo(&["dmesg"])
            .lines()
            .filter(|line| line.contains("Memory cgroup out of memory"))
            .count()
    };
    let (events_before, parent_before, dmesg_before) = (
        cgroup_keyed(&cgroup, "memory.events"),
        cgroup_keyed(&parent, "memory.events"),
        host_ooms(),
    );
    let vmstat = |guest: &mut guest::Guest| {
        let read = probe(
            guest,
            "vmstat",
            "read",
            serde_json::json!({"path": "/proc/vmstat"}),
        );
        keyed(read["content"].as_str().unwrap_or_default())["oom_kill"]
    };
    let guest_before = vmstat(&mut guest);
    append(
        &path,
        serde_json::json!({"offender": vmm.name, "pid": pid, "cgroup": cgroup, "memory.max": memory_max.trim(),
                           "memory.events.before": events_before, "parent": parent,
                           "parent.memory.events.before": parent_before, "dmesg_memcg_ooms_before": dmesg_before,
                           "guest_oom_kill_before": guest_before, "host": host_load()}),
    );
    let allocate =
        "chunks = []\nfor _ in range(24):\n    chunks.append(bytearray(b'\\x01') * (16 << 20))";
    let (mem, served) = probing(&socket, t0, budget, &path, || {
        std::thread::sleep(Duration::from_secs(3));
        let begin = t0.elapsed();
        let mut cycles = Vec::new();
        let mut died = false;
        while t0.elapsed() - begin < Duration::from_secs(20) {
            let exec = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                probe(
                    &mut guest,
                    "allocate",
                    "exec",
                    serde_json::json!({"argv": ["/usr/local/bin/python", "-c", allocate]}),
                )
            }));
            match exec {
                Ok(result) => {
                    let last = result != "exit:-9";
                    cycles.push(result);
                    if last {
                        break;
                    }
                }
                Err(_) => {
                    died = true;
                    break;
                }
            }
        }
        let end = t0.elapsed();
        // The guest's own count, and the host's while the cgroup still exists.
        let guest_after = (!died).then(|| vmstat(&mut guest));
        let events_after = (!died).then(|| cgroup_keyed(&cgroup, "memory.events"));
        let peak = (!died).then(|| run::sudo(&["cat", &format!("{cgroup}/memory.peak")]));
        let removal = (!died).then(|| remove(&vmm, pid, &cgroup));
        let exited = died.then(|| run::wait_exit(&mut vmm, Duration::from_secs(30)).to_string());
        let gone = t0.elapsed();
        std::thread::sleep(Duration::from_secs(3));
        (
            begin,
            end,
            gone,
            cycles,
            died,
            guest_after,
            events_after,
            peak,
            removal,
            exited,
        )
    });
    let (begin, end, gone, cycles, died, guest_after, events_after, peak, removal, exited) = mem;
    drop(guest);
    let output = run::stderr_text(&vmm) + &run::stdout_text(&vmm);
    let guest_text: Vec<&str> = output
        .lines()
        .filter(|line| {
            line.contains("Out of memory")
                || line.contains("oom-kill")
                || line.contains("Kernel panic")
        })
        .take(8)
        .collect();
    drop(vmm);
    let (parent_after, dmesg_after) = (cgroup_keyed(&parent, "memory.events"), host_ooms());
    let host_killed = parent_after["oom_kill"] - parent_before["oom_kill"]
        + (dmesg_after - dmesg_before) as u64
        + events_after
            .as_ref()
            .map(|events| events["oom_kill"] - events_before["oom_kill"])
            .unwrap_or_default();
    let killed = cycles.iter().filter(|cycle| **cycle == "exit:-9").count() as u64;
    let source = match (host_killed, died, guest_after) {
        (n, _, _) if n > 0 => "host cgroup OOM".to_string(),
        (0, false, Some(after))
            if after - guest_before >= 1 && after - guest_before >= killed && killed > 0 =>
        {
            "guest OOM kill, VMM survived".to_string()
        }
        (0, true, _) if !guest_text.is_empty() => "guest death after guest OOM".to_string(),
        _ => "unattributed".to_string(),
    };
    if !source.starts_with("guest") {
        violations.push(format!(
            "MEM: {source}: cycles {cycles:?}, guest text {guest_text:?}"
        ));
    }
    if cycles.iter().any(|cycle| *cycle != "exit:-9") {
        violations.push(format!("MEM: an allocation not killed: {cycles:?}"));
    }
    if gone - begin > Duration::from_secs(60) {
        violations.push(format!("MEM: a fault window of {:?}", gone - begin));
    }
    // Sustained, the guest outlived twenty seconds of allocations; a guest
    // which died is a transient, held to answers around it and to one within
    // it only if it lasted a second.
    let pressure = end - begin;
    if !died && pressure < Duration::from_secs(20) {
        violations.push(format!(
            "MEM: the guest survived only {pressure:?} of allocations"
        ));
    }
    let inside = match (died, pressure >= Duration::from_secs(1)) {
        (false, _) => 10,
        (true, true) => 1,
        (true, false) => 0,
    };
    let mem_violations = judge("MEM", &served, &reference, budget, (begin, end), inside);
    let successor_mem = successor(&run, "MEM", &mut violations);
    peer_intact(&peer, peer_pid, &peer_cgroup, "MEM", &mut violations);
    append(
        &path,
        serde_json::json!({"summary": "MEM", "source": source, "window_ms": [begin.as_millis(), end.as_millis()],
                           "pressure_ms": pressure.as_millis(), "inside_required": inside, "cycles": cycles, "died": died,
                           "vmm_exit": exited, "guest_oom_kill_after": guest_after,
                           "memory.events.after": events_after, "memory.peak": peak.map(|p| p.trim().to_string()),
                           "parent.memory.events.after": parent_after, "dmesg_memcg_ooms_after": dmesg_after,
                           "guest_text": guest_text, "removal_ms": removal.map(|r| r.as_millis()),
                           "successor": successor_mem, "probes": served.len(),
                           "violations": mem_violations, "host": host_load()}),
    );
    violations.extend(mem_violations);

    // C07-DISK: the offender's 256 MiB scratch disk filled to ENOSPC, then
    // held full for twenty seconds, then removed. A write of a few bytes can
    // still land in what the fill left, so being full is shown by a bounded
    // 1 MiB allocation refused each second, which truncates and so measures
    // the same leftover each time.
    let path = evidence("disk");
    let avail = root_avail();
    assert!(
        avail >= RESERVE,
        "C07-DISK inconclusive, not run: {avail} bytes free beneath {}, under the {RESERVE} reserve",
        run::ROOT
    );
    let (vmm, mut guest) = offender(&run);
    let (pid, cgroup) = (run::pid(&vmm), cgroup_of(&vmm));
    let scratch = format!("{}/scratch", vmm.state_dir);
    append(
        &path,
        serde_json::json!({"offender": vmm.name, "pid": pid, "cgroup": cgroup, "host": host_load()}),
    );
    let (disk, served) = probing(&socket, t0, budget, &path, || {
        std::thread::sleep(Duration::from_secs(3));
        let fault = t0.elapsed();
        let fill = probe(
            &mut guest,
            "fill",
            "fill",
            serde_json::json!({"path": "/tmp/fill", "mib": 1024}),
        );
        let begin = t0.elapsed();
        let inode = host::fd_inode(pid, 3);
        let full = host_load();
        // Recorded, not asserted: what a write of a few bytes does now.
        let tiny = probe(
            &mut guest,
            "tiny",
            "write",
            serde_json::json!({"path": "/tmp/tiny", "data": "acmeCo"}),
        );
        let mut refused = Vec::new();
        while fill["stop"] == "error:ENOSPC" && t0.elapsed() - begin < Duration::from_secs(21) {
            let at = t0.elapsed();
            let attempt = probe(
                &mut guest,
                "full",
                "fill",
                serde_json::json!({"path": "/tmp/full", "mib": 1}),
            );
            refused.push(serde_json::json!({"at_ms": at.as_millis(), "attempt": attempt}));
            std::thread::sleep(Duration::from_secs(1));
        }
        let end = t0.elapsed();
        let removal = remove(&vmm, pid, &cgroup);
        let removed = t0.elapsed();
        let holders = host::inode_references(inode);
        let backing = run::sudo(&["ls", "-A", &scratch]);
        std::thread::sleep(Duration::from_secs(3));
        (
            fault, begin, end, removed, fill, inode, full, tiny, refused, removal, holders, backing,
        )
    });
    let (fault, begin, end, removed, fill, inode, full, tiny, refused, removal, holders, backing) =
        disk;
    drop(guest);
    drop(vmm);
    let written = fill["written"].as_u64().unwrap_or_default();
    if fill["stop"] != "error:ENOSPC" || written > 256 * MIB || written < 256 * MIB * 8 / 10 {
        violations.push(format!("DISK: the fill {fill}"));
    }
    if end - begin < Duration::from_secs(20) {
        violations.push(format!("DISK: full for only {:?}", end - begin));
    }
    if removed - fault > Duration::from_secs(60) {
        violations.push(format!("DISK: a fault window of {:?}", removed - fault));
    }
    if inode.links != 0 || inode.bytes < written * 9 / 10 {
        violations.push(format!(
            "DISK: the scratch file {inode:?} for {written} written"
        ));
    }
    let saturated = |refusal: &serde_json::Value| {
        refusal["attempt"]["stop"] == "error:ENOSPC"
            && refusal["attempt"]["written"]
                .as_u64()
                .is_some_and(|bytes| bytes < MIB)
    };
    // Refused throughout: from the fill's end to the window's, never more
    // than two seconds apart.
    let mut marks: Vec<u128> = vec![begin.as_millis()];
    marks.extend(
        refused
            .iter()
            .filter_map(|refusal| refusal["at_ms"].as_u64().map(u128::from)),
    );
    marks.push(end.as_millis());
    if refused.is_empty()
        || !refused.iter().all(saturated)
        || marks.windows(2).any(|pair| pair[1] - pair[0] > 2000)
    {
        violations.push(format!("DISK: allocations while full {refused:?}"));
    }
    if !holders.is_empty() || !backing.is_empty() {
        violations.push(format!(
            "DISK: after removal, holders {holders:?}, backing {backing:?}"
        ));
    }
    let disk_violations = judge("DISK", &served, &reference, budget, (begin, end), 10);
    let successor_disk = successor(&run, "DISK", &mut violations);
    peer_intact(&peer, peer_pid, &peer_cgroup, "DISK", &mut violations);
    append(
        &path,
        serde_json::json!({"summary": "DISK", "window_ms": [begin.as_millis(), end.as_millis()],
                           "fault_ms": fault.as_millis(), "full_ms": (end - begin).as_millis(), "removed_ms": removed.as_millis(),
                           "fill": fill, "tiny_write": tiny, "refusals": refused, "scratch_bytes": inode.bytes, "scratch_links": inode.links,
                           "host_when_full": full,                            "removal_ms": removal.as_millis(), "holders": holders, "backing": backing,
                           "successor": successor_disk, "probes": served.len(),
                           "violations": disk_violations, "host": host_load()}),
    );
    violations.extend(disk_violations);

    assert!(violations.is_empty(), "C07 violations: {violations:#?}");
}
