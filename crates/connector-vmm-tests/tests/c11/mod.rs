//! One hundred seeded lifecycle cycles through the launcher, each stopping a
//! launch at one boundary beneath a state directory and TMPDIR all of them
//! share, then abandoning it or killing its owner. Each cycle must leave
//! nothing of its launch, and this process, which hosts every in-process
//! launch and recovery, must not accumulate descriptors or memory.
//! Opt-in: `mise run ci:connector-vmm-kvm --ignored c11::lifecycle_cycles`.

use super::{
    Evidence, Logs, Owner, RELEASED, assert_started, held, hold_rpc, launch, open, removed, router,
    setup, spec, vmm_at,
};
use connector_vmm_tests::{host, launcher, run};
use proto_flow::ops;
use std::os::unix::process::ExitStatusExt;
use std::time::Instant;

const SEED: u64 = 0x0C11_5EED;
const PER_BOUNDARY: usize = 20;
/// Enforced by a watchdog: the helpers' own waits allow a launch 180 s.
const CYCLE: std::time::Duration = std::time::Duration::from_secs(120);
const CHECKPOINT: usize = 10;
/// Fewer than one descriptor per cycle between checkpoints.
const FD_SLACK: usize = 8;
/// Allocator and runtime caches, in KiB.
const RSS_SLACK: u64 = 64 << 10;

#[derive(Clone, Copy, Debug)]
enum Boundary {
    /// Abandoned while podman, held, is about to create the network. Pre-start:
    /// no container, VMM or guest.
    BeforeNetwork,
    /// Abandoned once `podman create` has finished, before `start`. Pre-start:
    /// the container is never started.
    AfterCreate,
    /// Abandoned with the guest held before connector-init.
    BeforeHealth,
    /// Abandoned after `Started`, the guest serving.
    Serving,
    /// Its owner, another process, SIGKILLed while serving, with an RPC held
    /// on its VMM; the cycle's next launch must recover it and serve a Spec.
    OwnerDeath,
}

const BOUNDARIES: [(Boundary, char); 5] = [
    (Boundary::BeforeNetwork, 'N'),
    (Boundary::AfterCreate, 'C'),
    (Boundary::BeforeHealth, 'H'),
    (Boundary::Serving, 'S'),
    (Boundary::OwnerDeath, 'O'),
];

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `PER_BOUNDARY` of each boundary, Fisher-Yates shuffled from `seed`.
fn order(seed: u64) -> Vec<(Boundary, char)> {
    let mut order: Vec<(Boundary, char)> = BOUNDARIES
        .iter()
        .flat_map(|boundary| [*boundary; PER_BOUNDARY])
        .collect();
    let mut state = seed;
    for i in (1..order.len()).rev() {
        let j = (splitmix64(&mut state) % (i as u64 + 1)) as usize;
        order.swap(i, j);
    }
    order
}

/// This process's open descriptors, resident KiB and threads.
fn supervisor() -> (usize, u64, u64) {
    let fds = std::fs::read_dir("/proc/self/fd")
        .expect("reading /proc/self/fd")
        .count();
    let status = std::fs::read_to_string("/proc/self/status").expect("reading /proc/self/status");
    let field = |key: &str| -> u64 {
        status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("no {key} in {status}"))
    };
    (fds, field("VmRSS:"), field("Threads:"))
}

/// Host-wide counts, for diagnosis only: unrelated activity on the host
/// moves them, so no assertion rests on them.
fn host_counts() -> String {
    let containers = run::podman(&["ps", "--all", "--quiet"]).lines().count();
    let networks = run::podman(&["network", "ls", "--quiet"]).lines().count();
    let output = |program: &str, args: &[&str]| {
        let output = std::process::Command::new(program)
            .args(args)
            .output()
            .unwrap_or_else(|e| panic!("running {program}: {e}"));
        String::from_utf8_lossy(&output.stdout).into_owned()
    };
    let bridges = output("ip", &["-o", "link", "show"])
        .lines()
        .filter(|line| line.contains(": fvm"))
        .count();
    let used = output("df", &["-B1", "--output=used", "/var/lib/containers"]);
    let used = used.lines().nth(1).unwrap_or_default().trim().to_string();
    format!(
        "host containers={containers} networks={networks} fvm_links={bridges} storage_used={used}"
    )
}

/// Ends the test once a cycle outlives `CYCLE`, after letting go of the
/// cycle's holds so that no podman it held waits on beyond the test.
fn watchdog(
    evidence: std::path::PathBuf,
    number: usize,
    dir: String,
) -> std::sync::mpsc::Sender<()> {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if finished.recv_timeout(CYCLE) != Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
            return;
        }
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            if entry.file_name().to_string_lossy().starts_with("hold") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        Evidence(evidence).note(format!("cycle {number:03} exceeded {CYCLE:?}"));
        eprintln!("c11: cycle {number:03} exceeded {CYCLE:?}; ending the test");
        std::process::exit(1);
    });
    done
}

/// The VMM's scratch disk, its fd 3, after checking that the walk which
/// later proves it freed does see it.
fn scratch(footprint: &launcher::Footprint) -> host::Inode {
    let (_, pid, _) = &footprint.processes[0];
    let inode = host::fd_inode(*pid, 3);
    assert_eq!(inode.links, 0, "the scratch disk is an unnamed O_TMPFILE");
    let holders = host::inode_references(inode);
    assert!(
        holders.contains(&format!("/proc/{pid}/fd/3")),
        "the walk misses the VMM's own scratch descriptor: {holders:?}"
    );
    inode
}

/// Everything of a booted launch must be gone: its footprint and the last
/// reference to its scratch disk.
fn assert_gone(footprint: &launcher::Footprint, inode: host::Inode) {
    assert_eq!(launcher::remaining(footprint), Vec::<String>::new());
    let holders = host::inode_references(inode);
    assert!(
        holders.is_empty(),
        "the scratch disk is held by {holders:?}"
    );
}

/// One cycle through `cycle`'s podman, beneath the shared state directory:
/// it notes the boundary's proof before abandoning or killing the launch,
/// and returns what it observed of the release.
fn one(
    run: &run::Run,
    runtime: &tokio::runtime::Runtime,
    evidence: &Evidence,
    number: usize,
    boundary: Boundary,
    cycle: &launcher::Root,
) -> String {
    let router = router(vmm_at(&cycle.podman, &cycle.state_dir, &run.vmm_image));
    let image = &run.derive_python_image;
    let gate =
        |proof: String| evidence.note(format!("cycle {number:03} {boundary:?} gate {proof}"));
    let named = || launcher::vmm_name(cycle).expect("the launch named its network");

    match boundary {
        Boundary::BeforeNetwork => {
            let hold = launcher::hold(cycle, "network-create");
            let (request_tx, mut response_rx) = open(runtime, &router, spec(image));
            held(cycle, "network-create", &mut response_rx).unwrap_or_else(|err| panic!("{err}"));
            let name = named();
            assert!(
                !launcher::network_exists(&name),
                "held before {name} exists"
            );
            gate(format!("{name} held network-create, network absent"));

            let at = Instant::now();
            drop((request_tx, response_rx));
            drop(hold);
            launcher::wait_released(cycle, &name, RELEASED);
            let released = at.elapsed();
            let calls = launcher::calls(cycle);
            assert!(
                !calls.iter().any(|call| call.contains("--name=")),
                "the VMM was created: {calls:#?}"
            );
            assert_eq!(calls.last(), Some(&format!("network rm {name}")));
            format!("{name} released {}ms", released.as_millis())
        }
        Boundary::AfterCreate => {
            let hold = launcher::hold_after(cycle, "create", 1);
            let (request_tx, mut response_rx) = open(runtime, &router, spec(image));
            held(cycle, "after-create", &mut response_rx).unwrap_or_else(|err| panic!("{err}"));
            let name = named();
            let marked = launcher::held_by(cycle, "after-create");
            assert_eq!(marked.get(1), Some(&0), "create succeeded: {marked:?}");
            assert!(launcher::container_exists(&name), "{name} was created");
            let started = |calls: &[String]| calls.iter().any(|call| call.starts_with("start "));
            assert!(!started(&launcher::calls(cycle)), "held before start");
            gate(format!(
                "{name} held after create {marked:?}, container present"
            ));

            let at = Instant::now();
            drop((request_tx, response_rx));
            drop(hold);
            launcher::wait_released(cycle, &name, RELEASED);
            let released = at.elapsed();
            let calls = launcher::calls(cycle);
            assert!(
                calls.iter().any(|call| call.starts_with("rm --force")),
                "the container was removed: {calls:#?}"
            );
            assert!(!started(&calls), "the container was started: {calls:#?}");
            format!("{name} released {}ms", released.as_millis())
        }
        Boundary::BeforeHealth => {
            launcher::gate_readiness(cycle);
            let (request_tx, mut response_rx) = open(runtime, &router, spec(image));
            held(cycle, "readiness", &mut response_rx).unwrap_or_else(|err| panic!("{err}"));
            let name = named();
            let footprint = launcher::footprint(&cycle.state_dir, &cycle.tmp_dir, &name);
            let inode = scratch(&footprint);
            gate(format!(
                "{name} held readiness, footprint whole, id {} processes {:?}",
                footprint.id, footprint.processes
            ));

            let at = Instant::now();
            drop((request_tx, response_rx));
            launcher::wait_released(cycle, &name, RELEASED);
            let released = at.elapsed();
            assert_gone(&footprint, inode);
            format!(
                "{name} released {}ms, footprint and scratch gone",
                released.as_millis()
            )
        }
        Boundary::Serving => {
            let logs = Logs::default();
            let logger = |log: &ops::Log| logs.push(log);
            let (request_tx, response_rx, started) = runtime
                .block_on(proto_grpc::connector::start(
                    &router,
                    &logger,
                    proto_grpc::connector::SPEC_TASK_NAME,
                    spec(image),
                ))
                .unwrap_or_else(|err| panic!("starting: {err:#}\n{:#?}", logs.messages()));
            assert_started(&started);
            let name = named();
            let footprint = launcher::footprint(&cycle.state_dir, &cycle.tmp_dir, &name);
            let inode = scratch(&footprint);
            gate(format!(
                "{name} Started with a Spec, footprint whole, id {} processes {:?}",
                footprint.id, footprint.processes
            ));

            let at = Instant::now();
            drop((request_tx, response_rx));
            launcher::wait_released(cycle, &name, RELEASED);
            let released = at.elapsed();
            assert_gone(&footprint, inode);
            format!(
                "{name} released {}ms, footprint and scratch gone",
                released.as_millis()
            )
        }
        Boundary::OwnerDeath => {
            let owner = Owner::spawn(run, cycle, &cycle.state_dir, &run.vmm_image, false);
            owner.started();
            let name = named();
            let footprint = launcher::footprint(&cycle.state_dir, &cycle.tmp_dir, &name);
            let inode = scratch(&footprint);
            // connector-init's idle watchdog would otherwise end the VMM
            // before the recovery, which must find it serving.
            let rpc = hold_rpc(runtime, &cycle.state_dir, &name);
            let owner_pid = owner.host_pid;
            gate(format!(
                "{name} owner {owner_pid} Started, rpc held, footprint whole, id {} processes {:?}",
                footprint.id, footprint.processes
            ));

            let at = Instant::now();
            let status = owner.kill();
            assert_eq!(
                status.signal(),
                Some(libc::SIGKILL),
                "owner {owner_pid}: {status}"
            );
            launcher::assert_whole(&footprint);
            evidence.note(format!(
                "cycle {number:03} {boundary:?} owner {owner_pid} killed ({status}), footprint whole"
            ));

            launch(runtime, &router, image);
            let recovered = at.elapsed();
            assert!(
                removed(cycle, &footprint.id),
                "the recovering launch removed it"
            );
            drop(rpc);
            launcher::wait_released(cycle, &name, RELEASED);
            assert_gone(&footprint, inode);
            format!(
                "{name} recovered by a launch serving a Spec {}ms after the kill, footprint and scratch gone",
                recovered.as_millis()
            )
        }
    }
}

#[test]
#[ignore = "opt-in: mise run ci:connector-vmm-kvm --ignored c11::lifecycle_cycles"]
fn lifecycle_cycles() {
    let (run, shared, runtime) = setup();
    let evidence = Evidence(run.run_dir.join("c11.evidence"));
    // Each cycle's own podman, so that its holds catch only its launch.
    let cycle_root = || {
        let fresh = launcher::root();
        launcher::Root {
            dir: fresh.dir,
            podman: fresh.podman,
            state_dir: shared.state_dir.clone(),
            tmp_dir: shared.tmp_dir.clone(),
        }
    };

    let order = order(SEED);
    let codes: String = order.iter().map(|(_, code)| code).collect();
    evidence.note(format!("seed {SEED:#x} order {codes}"));

    // What the first launch pulls and caches, every later one reuses, so the
    // baseline follows it.
    let warm = cycle_root();
    let warm_router = router(vmm_at(&warm.podman, &warm.state_dir, &run.vmm_image));
    launch(&runtime, &warm_router, &run.derive_python_image);
    assert_eq!(launcher::leftovers(&warm, None), Vec::<String>::new());
    let (fds, rss, threads) = supervisor();
    evidence.note(format!(
        "B0 fds={fds} rss_kib={rss} threads={threads} {}",
        host_counts()
    ));

    for (index, (boundary, _)) in order.into_iter().enumerate() {
        let number = index + 1;
        let cycle = cycle_root();
        let done = watchdog(evidence.0.clone(), number, cycle.dir.clone());
        let at = Instant::now();
        let result = one(&run, &runtime, &evidence, number, boundary, &cycle);
        let elapsed = at.elapsed();
        drop(done);
        evidence.note(format!(
            "cycle {number:03} {boundary:?} ok {result}, cycle {}ms",
            elapsed.as_millis()
        ));

        if number % CHECKPOINT == 0 {
            let (now_fds, now_rss, now_threads) = supervisor();
            evidence.note(format!(
                "checkpoint {number:03} fds={now_fds} rss_kib={now_rss} threads={now_threads} {}",
                host_counts()
            ));
            assert!(
                now_fds <= fds + FD_SLACK,
                "{now_fds} descriptors after cycle {number}, from {fds}"
            );
        }
    }

    // A fresh launch beneath the shared state directory recovers whatever a
    // cycle may have left, and must still serve a Spec.
    let last = cycle_root();
    let last_router = router(vmm_at(&last.podman, &last.state_dir, &run.vmm_image));
    launch(&runtime, &last_router, &run.derive_python_image);
    assert_eq!(launcher::leftovers(&last, None), Vec::<String>::new());
    let (end_fds, end_rss, end_threads) = supervisor();
    evidence.note(format!(
        "final fds={end_fds} rss_kib={end_rss} threads={end_threads} {}",
        host_counts()
    ));
    assert!(
        end_fds <= fds + FD_SLACK,
        "{end_fds} descriptors at the end, from {fds}"
    );
    assert!(
        end_rss <= rss + RSS_SLACK,
        "{end_rss} KiB resident at the end, from {rss}"
    );
}
