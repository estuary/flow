//! Replacement generations of one task racing the old generation's cleanup.
//! Each generation is a real VMM launch in an owner process of its own, as
//! the same task, beneath one state directory and TMPDIR, with a fresh id
//! and token of the launcher's, and egress alternating between two
//! controlled endpoints. The old generation's container removal is held
//! until its replacement serves; then its VMM, and in the second transition
//! its owner too, is killed and the removal let go. Nothing of the
//! replacement may change, and the replacement must keep its own egress.
//! This approximates a republication through the launcher: no reactor,
//! shard or publication is involved.

use super::{
    Evidence, Owner, PROBE, case, destinations, hold_rpc, launch, removed, router, served, setup,
    vmm_at,
};
use connector_vmm_tests::{endpoint, launcher, run};
use std::os::unix::process::ExitStatusExt;
use std::time::{Duration, Instant};

const READY: Duration = Duration::from_secs(60);
const USEFUL: Duration = Duration::from_millis(5000);
/// Well inside connector's 60 s engine command timeout.
const GATE: Duration = Duration::from_secs(30);
const API: &str = "api.acmeco.example";
const SVC: &str = "svc.acmeco.example";

/// One running generation, and what identifies it.
struct Generation {
    label: String,
    owner: Option<Owner>,
    root: launcher::Root,
    name: String,
    footprint: launcher::Footprint,
    token: String,
    policy: String,
}

fn policy(root: &launcher::Root, name: &str) -> String {
    run::sudo(&[
        "cat",
        &format!("{}/{name}/init/policy.json", root.state_dir),
    ])
}

/// A generation's owner, launched as the task declaring only `host`, its
/// guest probing both controlled names before connector-init. It must be
/// Started within `READY`, and only `host`'s endpoint may see it connect.
fn start(
    run: &run::Run,
    shared: &launcher::Root,
    d: &super::Destinations,
    evidence: &Evidence,
    label: &str,
    host: &str,
) -> Generation {
    use base64::Engine;
    let fresh = launcher::root();
    let root = launcher::Root {
        dir: fresh.dir,
        podman: fresh.podman,
        state_dir: shared.state_dir.clone(),
        tmp_dir: shared.tmp_dir.clone(),
    };
    let cases = serde_json::to_string(&[case(API, &d.a), case(SVC, &d.b)]).unwrap();
    let script = PROBE.replace("CASES", &serde_json::to_string(&cases).unwrap());
    let encoded = base64::engine::general_purpose::STANDARD.encode(script);
    launcher::vmm_flags(
        &root,
        &[
            "--resolver-upstream".to_string(),
            d.nameserver.addr.to_string(),
            "--as-root-exec".to_string(),
            format!(
                "{} -c \"import base64; exec(base64.b64decode('{encoded}'))\"",
                run::PROBES[0]
            ),
        ],
    );

    let (a, b) = (
        endpoint::accepted(&d.a).len(),
        endpoint::accepted(&d.b).len(),
    );
    let at = Instant::now();
    let owner = Owner::spawn_at(
        run,
        &root,
        &root.state_dir,
        &run.vmm_image,
        false,
        None,
        Some(&[host]),
    );
    owner.report("started", READY);
    let ready = at.elapsed();
    let name = launcher::vmm_name(&root).expect("the launch named its network");
    let footprint = launcher::footprint(&root.state_dir, &root.tmp_dir, &name);
    let token = launcher::token(&launcher::record(&root.state_dir, &name));
    let labels = run::podman(&[
        "container",
        "inspect",
        "--format",
        "{{index .Config.Labels \"task-name\"}} {{index .Config.Labels \"dev.estuary.vmm-owner\"}}",
        &footprint.id,
    ]);
    assert_eq!(
        labels.trim(),
        format!("{} {token}", proto_grpc::connector::SPEC_TASK_NAME),
        "{name}: the task's name, and the record's token as its owner label"
    );

    let policy = policy(&root, &name);
    let allowed: serde_json::Value = serde_json::from_str(&policy).expect("policy is JSON");
    let allowed: Vec<&str> = allowed["allowedNames"]
        .as_array()
        .expect("allowedNames")
        .iter()
        .map(|name| name.as_str().expect("a name"))
        .collect();
    let other = if host == API { SVC } else { API };
    assert!(
        allowed.contains(&host) && !allowed.contains(&other),
        "{label}'s policy {policy}"
    );

    let (now_a, now_b) = (endpoint::accepted(&d.a), endpoint::accepted(&d.b));
    let deltas = (now_a.len() - a, now_b.len() - b);
    let expected = if host == API { (1, 0) } else { (0, 1) };
    assert_eq!(deltas, expected, "{label} ({host}): new A and B accepts");
    let peers = if host == API {
        &now_a[a..]
    } else {
        &now_b[b..]
    };
    assert_eq!(peers, [run.endpoint_peer], "{label}'s connection");

    evidence.note(format!(
        "{label} ready {}ms name {name} id {} token {token} owner {} processes {:?} \
         egress [{host}] accepts A+{} B+{} policy {policy}",
        ready.as_millis(),
        footprint.id,
        owner.host_pid,
        footprint.processes,
        deltas.0,
        deltas.1,
    ));
    assert!(ready <= READY, "{label} Started in {ready:?}");
    Generation {
        label: label.to_string(),
        owner: Some(owner),
        root,
        name,
        footprint,
        token,
        policy,
    }
}

/// The generation is exactly as it started, and serves a Spec in `USEFUL`.
fn check_live(runtime: &tokio::runtime::Runtime, g: &Generation, evidence: &Evidence, when: &str) {
    launcher::assert_whole(&g.footprint);
    let record = launcher::record(&g.root.state_dir, &g.name);
    assert_eq!(launcher::token(&record), g.token, "{}'s token", g.label);
    assert!(
        launcher::locked(&record),
        "{}'s owner holds its record",
        g.label
    );
    assert_eq!(policy(&g.root, &g.name), g.policy, "{}'s policy", g.label);
    let at = Instant::now();
    let served = served(runtime, &g.root.state_dir, &g.name);
    let took = at.elapsed();
    evidence.note(format!(
        "{} {when}: whole (same processes {:?}, id, network, netns, cgroup, layer, bridge, veths, \
         state, record, mount), token and policy unchanged, {served}",
        g.label, g.footprint.processes
    ));
    assert!(took <= USEFUL, "{} served in {took:?} {when}", g.label);
}

fn names(root: &launcher::Root, g: &Generation) -> Vec<String> {
    launcher::calls(root)
        .into_iter()
        .filter(|call| call.contains(&g.name) || call.contains(&g.footprint.id))
        .collect()
}

/// Hold `old`'s container removal, and end its session so its teardown
/// reaches it, with an RPC keeping its VMM from connector-init's idle exit.
fn gate(
    runtime: &tokio::runtime::Runtime,
    old: &mut Generation,
    evidence: &Evidence,
) -> (
    launcher::Hold,
    tonic::Streaming<proto_flow::derive::Response>,
    Instant,
) {
    let rpc = hold_rpc(runtime, &old.root.state_dir, &old.name);
    let hold = launcher::hold(&old.root, "rm");
    let owner = old.owner.as_mut().expect("a live owner");
    std::mem::drop(owner.stdin.take());
    launcher::wait_held(&old.root, "rm", GATE);
    let t0 = Instant::now();
    assert!(
        removed(&old.root, &old.footprint.id),
        "held at its own removal"
    );
    launcher::assert_whole(&old.footprint);
    evidence.note(format!(
        "{} gate: held-rm {:?}, last call {:?}, footprint whole, record locked {}",
        old.label,
        launcher::held_by(&old.root, "rm"),
        launcher::calls(&old.root).last(),
        launcher::locked(&launcher::record(&old.root.state_dir, &old.name)),
    ));
    (hold, rpc, t0)
}

/// Kill the VMM of `g`, whose removal is held. Its container is created with
/// `--rm`, so podman may remove it once it exits; what the hold still keeps
/// is the network, state and record, which must remain.
fn kill_vmm(g: &Generation, evidence: &Evidence) {
    let (_, pid, started) = &g.footprint.processes[0];
    let at = Instant::now();
    run::sudo(&["kill", "-KILL", &pid.to_string()]);
    run::eventually("the killed VMM to exit", GATE, || {
        (launcher::start_time(*pid).as_ref() != Some(started)).then_some(())
    });
    while launcher::container_exists(&g.footprint.id) && at.elapsed() < Duration::from_secs(10) {
        std::thread::sleep(Duration::from_millis(100));
    }
    let record = launcher::record(&g.root.state_dir, &g.name);
    let state = format!("{}/{}", g.root.state_dir, g.name);
    assert!(
        launcher::network_exists(&g.name)
            && launcher::path_exists(&record)
            && launcher::path_exists(&state),
        "{}'s held teardown keeps its network, record and state",
        g.label
    );
    evidence.note(format!(
        "{} VMM {pid} SIGKILLed, gone; container present {} after {}ms; network, record and state present",
        g.label,
        launcher::container_exists(&g.footprint.id),
        at.elapsed().as_millis()
    ));
}

#[test]
fn replacement_races_old_cleanup() {
    let (run, shared, runtime) = setup();
    let evidence = Evidence(run.run_dir.join("c12.evidence"));
    let d = destinations(&run);
    let mut tokens = std::collections::BTreeSet::new();
    let mut ids = std::collections::BTreeSet::new();

    // B0: two healthy generations, each ended before the next.
    for (label, host) in [("B0a", API), ("B0b", SVC)] {
        let mut g = start(&run, &shared, &d, &evidence, label, host);
        check_live(&runtime, &g, &evidence, "serving");
        g.owner.take().unwrap().end();
        assert_eq!(launcher::remaining(&g.footprint), Vec::<String>::new());
        evidence.note(format!("{label} ended, footprint gone"));
        assert!(tokens.insert(g.token.clone()) && ids.insert(g.name.clone()));
    }

    // T1: the old owner lives on to finish its own teardown.
    let mut g1 = start(&run, &shared, &d, &evidence, "G1", API);
    let (hold, rpc, t0) = gate(&runtime, &mut g1, &evidence);
    let g2 = start(&run, &shared, &d, &evidence, "G2", SVC);
    assert_eq!(names(&g2.root, &g1), Vec::<String>::new(), "G2 skipped G1");
    kill_vmm(&g1, &evidence);
    std::mem::drop(rpc);
    let owner = g1.owner.as_mut().unwrap();
    assert!(
        owner.child.try_wait().unwrap().is_none(),
        "G1's owner lives"
    );
    assert!(owner.reports.try_recv().is_err(), "G1's teardown is held");
    check_live(&runtime, &g2, &evidence, "before G1's release");
    let held = t0.elapsed();
    assert!(held <= GATE, "G1's removal held {held:?}");
    std::mem::drop(hold);
    evidence.note(format!("G1 released after {}ms held", held.as_millis()));
    let mut owner = g1.owner.take().unwrap();
    owner.report("released", GATE);
    let status = owner.child.wait().expect("waiting for G1's owner");
    assert!(status.success(), "G1's owner: {status}");
    assert_eq!(launcher::remaining(&g1.footprint), Vec::<String>::new());
    let calls = names(&g1.root, &g1);
    let rm = format!("rm --force --time=0 --ignore {}", g1.footprint.id);
    let network_rm = format!("network rm {}", g1.name);
    let at = |call: &str| calls.iter().position(|c| c == call);
    assert!(at(&rm).is_some() && at(&rm) < at(&network_rm), "{calls:#?}");
    assert_eq!(
        names(&g1.root, &g2),
        Vec::<String>::new(),
        "G1 never named G2"
    );
    evidence.note(format!(
        "G1 owner {} released and exited {status}; footprint gone; its calls {calls:?}",
        owner.host_pid
    ));
    check_live(&runtime, &g2, &evidence, "after G1's release");

    // T2: the old owner and VMM both killed while its removal is held.
    let mut g2 = g2;
    let (hold, rpc, t0) = gate(&runtime, &mut g2, &evidence);
    let g3 = start(&run, &shared, &d, &evidence, "G3", API);
    assert_eq!(names(&g3.root, &g2), Vec::<String>::new(), "G3 skipped G2");
    let owner = g2.owner.take().unwrap();
    let owner_pid = owner.host_pid;
    let status = owner.kill();
    assert_eq!(status.signal(), Some(libc::SIGKILL), "G2's owner: {status}");
    kill_vmm(&g2, &evidence);
    std::mem::drop(rpc);
    let g2_record = launcher::record(&g2.root.state_dir, &g2.name);
    evidence.note(format!(
        "G2 owner {owner_pid} {status}; container present {}, record locked {}",
        launcher::container_exists(&g2.footprint.id),
        launcher::locked(&g2_record)
    ));
    check_live(&runtime, &g3, &evidence, "before G2's release");
    let held = t0.elapsed();
    assert!(held <= GATE, "G2's removal held {held:?}");
    std::mem::drop(hold);
    evidence.note(format!("G2 released after {}ms held", held.as_millis()));
    run::eventually("G2's orphaned removal to finish", GATE, || {
        (!launcher::container_exists(&g2.footprint.id) && !launcher::locked(&g2_record))
            .then_some(())
    });
    let left = launcher::remaining(&g2.footprint);
    evidence.note(format!("G2 after its orphaned removal, left: {left:?}"));
    assert!(launcher::network_exists(&g2.name) && launcher::path_exists(&g2_record));
    check_live(&runtime, &g3, &evidence, "after G2's release");

    // A last launch of the task recovers what G2 left, beside G3.
    let fresh = launcher::root();
    let last = launcher::Root {
        dir: fresh.dir,
        podman: fresh.podman,
        state_dir: shared.state_dir.clone(),
        tmp_dir: shared.tmp_dir.clone(),
    };
    launch(
        &runtime,
        &router(vmm_at(&last.podman, &last.state_dir, &run.vmm_image)),
        &run.derive_python_image,
    );
    assert_eq!(launcher::remaining(&g2.footprint), Vec::<String>::new());
    assert!(!names(&last, &g2).is_empty(), "the last launch released G2");
    assert_eq!(names(&last, &g3), Vec::<String>::new(), "it never named G3");
    evidence.note(format!(
        "last launch recovered G2: {:?}; G2 footprint gone",
        names(&last, &g2)
    ));
    check_live(&runtime, &g3, &evidence, "after G2's recovery");

    let mut g3 = g3;
    g3.owner.take().unwrap().end();
    assert_eq!(launcher::remaining(&g3.footprint), Vec::<String>::new());
    assert_eq!(launcher::leftovers(&shared, None), Vec::<String>::new());
    for g in [&g1, &g2, &g3] {
        assert!(tokens.insert(g.token.clone()) && ids.insert(g.name.clone()));
    }
    evidence.note(format!(
        "G3 ended, footprint gone, shared state and TMPDIR empty; {} unique tokens and names",
        tokens.len()
    ));
}
