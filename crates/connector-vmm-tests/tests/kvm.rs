//! The connector VMM's KVM integration suite. Each test launches its own VMM
//! containers on the reference launch line.

use connector_vmm_tests::{dns, endpoint, guest, host, run};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

/// The tap's two ends, which the VMM pins.
const VMM_TAP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const GUEST: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);

/// Generous next to a boot's usual second or two; `readiness` asserts the
/// tighter bound itself.
const READY: Duration = Duration::from_secs(30);

/// Long enough for a handshake across the tap. A blocked probe's `timeout`
/// is a lower bound on how long the drop takes to observe, nothing more.
const BLOCKED: f64 = 2.0;

const MIB: u64 = 1 << 20;

fn start(run: &run::Run, spec: run::Spec) -> (run::Vmm, guest::Guest) {
    let vmm = run::start(run, spec);
    run::wait_ready(&vmm, READY);
    let guest = guest::dial(&vmm);
    (vmm, guest)
}

fn probe(guest: &mut guest::Guest, name: &str, op: &str, arguments: Value) -> Value {
    guest::call(guest, name, op, arguments)
}

fn tcp(
    guest: &mut guest::Guest,
    name: &str,
    addr: impl ToString,
    port: u16,
    timeout: f64,
) -> Value {
    probe(
        guest,
        name,
        "tcp",
        json!({"addr": addr.to_string(), "port": port, "timeout": timeout}),
    )
}

fn query(guest: &mut guest::Guest, name: &str, qname: &str, qtype: u16) -> Value {
    probe(
        guest,
        name,
        "query",
        json!({"nameserver": VMM_TAP.to_string(), "name": qname, "qtype": qtype, "timeout": BLOCKED}),
    )
}

fn upstream(nameserver: &dns::Nameserver) -> Vec<String> {
    vec![
        "--resolver-upstream".to_string(),
        nameserver.addr.to_string(),
    ]
}

fn network_field(vmm: &run::Vmm, field: &str) -> Ipv4Addr {
    run::inspect(
        &vmm.name,
        &format!(
            "{{{{(index .NetworkSettings.Networks \"{}\").{field}}}}}",
            connector_vmm_tests::launch::NETWORK
        ),
    )
    .parse()
    .unwrap_or_else(|e| panic!("{field} of {}: {e}", vmm.name))
}

/// The guest's flows the VMM's kernel let through, from its conntrack table.
fn guest_flows(pid: u32) -> Vec<host::Flow> {
    host::conntrack(pid)
        .into_iter()
        .filter(|flow| flow.src == GUEST)
        .collect()
}

fn counter(counters: &std::collections::BTreeMap<String, u64>, rule: &str) -> u64 {
    *counters
        .get(rule)
        .unwrap_or_else(|| panic!("no counter for {rule} in {counters:?}"))
}

/// Whether one of the objects `teardown` follows is still there.
type Present = Box<dyn Fn() -> bool>;

fn exists(path: &str) -> bool {
    run::sudo_output(&["test", "-e", path], None)
        .status
        .success()
}

/// A probe result the guest wrote to its stderr, for the `--as-root-exec`
/// probes that run before the control channel exists.
fn stderr_probe(vmm: &run::Vmm, name: &str) -> Value {
    run::stderr_text(vmm)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|line| line["probe"] == name)
        .unwrap_or_else(|| panic!("no {name} probe line on the guest's stderr"))["result"]
        .take()
}

/// What the launch line gives the VMM, with and without the task's
/// persistent disk.
#[test]
fn launch() {
    let run = run::load();
    let (vmm, _guest) = start(&run, run::Spec::probes(&run, "none.json"));
    assert_launch(&vmm, &["/rootfs", "/scratch-backing", "/sock"]);
    insta::assert_snapshot!(mount_matrix(&vmm));
    run::assert_framing(&vmm, true);
}

#[test]
fn launch_with_persistent_disk() {
    let run = run::load();
    let spec = run::Spec {
        persistent_disk: Some("/acmeCo/state"),
        ..run::Spec::probes(&run, "none.json")
    };
    let (vmm, _guest) = start(&run, spec);
    assert_launch(
        &vmm,
        &["/persistent-disk", "/rootfs", "/scratch-backing", "/sock"],
    );
    insta::assert_snapshot!(mount_matrix(&vmm));
    run::assert_framing(&vmm, true);
}

fn assert_launch(vmm: &run::Vmm, writable: &[&str]) {
    let pid = run::pid(vmm);

    let status = run::sudo(&["cat", &format!("/proc/{pid}/status")]);
    let cap_eff = status
        .lines()
        .find_map(|line| line.strip_prefix("CapEff:"))
        .map(str::trim);
    // Podman's eleven defaults plus CAP_NET_ADMIN: no CAP_SYS_ADMIN, MKNOD,
    // NET_RAW, DAC_READ_SEARCH or SYS_PTRACE.
    assert_eq!(cap_eff, Some("00000000800415fb"), "CapEff of the VMM");

    let devices = run::inspect(
        &vmm.name,
        "{{range .HostConfig.Devices}}{{.PathOnHost}} {{end}}",
    );
    assert_eq!(devices, "/dev/kvm /dev/net/tun", "devices added to the VMM");

    for (sysctl, want) in [
        ("net/ipv4/ip_forward", "1"),
        ("net/ipv4/conf/default/rp_filter", "1"),
        ("net/ipv6/conf/default/disable_ipv6", "1"),
        // What the tap inherited from the defaults above.
        ("net/ipv4/conf/tap0/rp_filter", "1"),
        ("net/ipv6/conf/tap0/disable_ipv6", "1"),
    ] {
        let value = run::nsenter(pid, &["cat", &format!("/proc/sys/{sysctl}")]);
        assert_eq!(
            value.trim(),
            want,
            "{sysctl} in the VMM's network namespace"
        );
    }

    // The writable set, by trying to write: it must agree with mountinfo.
    let mounts = host::mountinfo(pid);
    let mut candidates: BTreeSet<String> = ["/", "/tmp", "/run", "/var/tmp"]
        .into_iter()
        .map(str::to_string)
        .collect();
    candidates.extend(
        mounts
            .iter()
            .map(|mount| mount.point.clone())
            .filter(|point| {
                !point.starts_with("/proc")
                    && !point.starts_with("/sys")
                    && !point.starts_with("/dev/pts")
                    && !point.starts_with("/dev/mqueue")
            }),
    );
    let script = r#"for path in "$@"; do
        if [ -d "$path" ]; then
            probe="$path/.connector-vmm-kvm-probe"
            if touch "$probe" 2>/dev/null; then rm -f "$probe"; echo "rw $path"; else echo "ro $path"; fi
        elif touch -c "$path" 2>/dev/null; then echo "rw $path"; else echo "ro $path"; fi
    done"#;
    let mut argv = vec!["exec", &vmm.name, "sh", "-c", script, "sh"];
    argv.extend(candidates.iter().map(String::as_str));
    let probed: Vec<String> = run::podman(&argv)
        .lines()
        .filter_map(|line| line.strip_prefix("rw ").map(str::to_string))
        .collect();
    assert_eq!(probed, writable, "paths the VMM can write");
}

/// The mount matrix from the VMM's mountinfo, plus its /dev, for a snapshot:
/// a podman upgrade that changes it is a reviewed diff.
fn mount_matrix(vmm: &run::Vmm) -> String {
    let pid = run::pid(vmm);
    let matrix = host::matrix(
        &host::mountinfo(pid),
        &[(vmm.connector_mount.as_str(), "$CONNECTOR_MOUNT")],
    );
    let dev = run::podman(&["exec", &vmm.name, "ls", "/dev"]);
    format!(
        "{matrix}\n# /dev\n{}\n",
        dev.split_whitespace().collect::<Vec<_>>().join(" ")
    )
}

/// The launch-line guards: each refuses before anything is built, with one
/// framed line.
#[test]
fn refuses_a_writable_root() {
    let run = run::load();
    let spec = run::Spec {
        edit: |argv| argv.retain(|argument| argument != "--read-only"),
        ..run::Spec::probes(&run, "none.json")
    };
    let mut vmm = run::start(&run, spec);
    assert_refused(
        &mut vmm,
        "flow-connector-vmm: / is writable; the VMM container must be launched with `--read-only`\n"
            .to_string(),
    );
}

#[test]
fn refuses_a_writable_connector_mount() {
    let run = run::load();
    let spec = run::Spec {
        edit: |argv| {
            for argument in argv.iter_mut() {
                if argument.starts_with("--mount=") && argument.contains("/connector-mounts-0/") {
                    *argument = argument.trim_end_matches(",readonly").to_string();
                }
            }
        },
        ..run::Spec::probes(&run, "none.json")
    };
    let mut vmm = run::start(&run, spec);
    let want = format!(
        "flow-connector-vmm: {} is writable; the VMM container must be launched with \
         `--mount type=bind,source=M,target=M,readonly`\n",
        vmm.connector_mount
    );
    assert_refused(&mut vmm, want);
}

fn assert_refused(vmm: &mut run::Vmm, want: String) {
    let status = run::wait_exit(vmm, READY);
    assert_eq!(status.code(), Some(2), "exit code of a refused launch");
    assert_eq!(run::stderr_text(vmm), want);
    assert_eq!(
        run::stdout_text(vmm),
        "",
        "a refused launch writes no stdout"
    );
    let sock = run::sudo(&["ls", "-A", &format!("{}/sock", vmm.state_dir)]);
    assert_eq!(sock, "", "a refused launch binds no socket");
    run::assert_framing(vmm, false);
}

/// A workload replacing connector-init signals readiness itself.
#[test]
fn readiness() {
    let run = run::load();
    let vmm = run::start(&run, run::Spec::probes(&run, "none.json"));
    let ready = run::wait_ready(&vmm, Duration::from_secs(5));
    eprintln!("readiness {ready:?} after podman run");
    run::assert_framing(&vmm, true);
}

/// The default workload: connector-init from the connector mount,
/// serving over vsock, answering a Spec RPC dialed unprivileged.
#[test]
fn spec_rpc_through_init_sock() {
    let run = run::load();
    let spec = run::Spec {
        connector_image: run.hello_world_image.clone(),
        exec: None,
        ..run::Spec::probes(&run, "none.json")
    };
    let vmm = run::start(&run, spec);
    run::wait_ready(&vmm, READY);

    let path = run::socket_path(&vmm);
    let responses = tokio::runtime::Runtime::new()
        .expect("a tokio runtime")
        .block_on(async move {
            let channel = tonic::transport::Endpoint::from_static("http://[::1]:0")
                .connect_with_connector(tower::service_fn(move |_: tonic::transport::Uri| {
                    let path = path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }))
                .await
                .expect("connecting over init.sock");
            let mut client = proto_grpc::capture::connector_client::ConnectorClient::new(channel);
            let request = proto_flow::capture::Request {
                kind: Some(proto_flow::capture::request::Kind::Spec(
                    proto_flow::capture::request::Spec {
                        config_json: "{}".into(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            };
            let responses = client
                .capture(futures::stream::once(async { request }))
                .await
                .expect("the Spec RPC")
                .into_inner();
            futures::TryStreamExt::try_collect::<Vec<_>>(responses)
                .await
                .expect("the Spec responses")
        });

    let Some(proto_flow::capture::response::Kind::Spec(spec)) =
        responses.first().and_then(|response| response.kind.clone())
    else {
        panic!("expected a Spec response, got {responses:?}");
    };
    assert_eq!(spec.protocol, 3032023, "the capture protocol");
    assert!(!spec.config_schema_json.is_empty(), "a config schema");
    run::assert_framing(&vmm, true);
}

/// Where the guest's bytes land, and that the cgroup holds.
#[test]
fn storage() {
    let run = run::load();
    let mut spec = run::Spec::probes(&run, "none.json");
    spec.memory_mib = 512;
    spec.disk_mib = 256;
    let (mut vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    // More than the guest's RAM, to its writable root.
    let root = probe(
        &mut guest,
        "root-fill",
        "fill",
        json!({"path": "/tmp/fill", "mib": 768}),
    );
    assert_eq!(
        root,
        json!({"written": 768 * MIB, "stop": "done"}),
        "root-fill"
    );

    let mounts = host::mountinfo(pid);
    let upper = host::upperdir(&mounts, "/rootfs")
        .unwrap_or_else(|| panic!("no upperdir for /rootfs in {mounts:?}"));
    let size = run::sudo(&["stat", "-c", "%s", &format!("{upper}/tmp/fill")]);
    assert_eq!(
        size.trim(),
        (768 * MIB).to_string(),
        "the fill in podman's layer {upper}"
    );

    let cgroup = format!(
        "/sys/fs/cgroup{}",
        run::inspect(&vmm.name, "{{.State.CgroupPath}}")
    );
    let max = run::sudo(&["cat", &format!("{cgroup}/memory.max")]);
    assert_eq!(
        max.trim(),
        ((512 + 256) * MIB).to_string(),
        "memory.max of {cgroup}"
    );
    let events = run::sudo(&["cat", &format!("{cgroup}/memory.events")]);
    assert!(
        events.lines().any(|line| line == "oom_kill 0"),
        "the VMM was OOM-killed writing more than guest RAM: {events}"
    );
    eprintln!(
        "memory.peak after the root fill: {}",
        run::sudo(&["cat", &format!("{cgroup}/memory.peak")]).trim()
    );

    let scratch = probe(
        &mut guest,
        "scratch-fill",
        "fill",
        json!({"path": "/scratch/fill", "mib": 300}),
    );
    assert_eq!(
        scratch["stop"], "error:ENOSPC",
        "scratch-fill stops at the end of the disk"
    );
    let written = scratch["written"].as_u64().expect("bytes written");
    assert!(
        written >= 256 * MIB * 9 / 10,
        "scratch-fill wrote only {written} of 256 MiB"
    );

    let inode = host::fd_inode(pid, 3);
    assert_eq!(inode.links, 0, "the scratch disk is an unnamed O_TMPFILE");
    assert!(
        inode.bytes >= written * 9 / 10,
        "the scratch file holds {} bytes for {written} written",
        inode.bytes
    );

    let mounts = probe(&mut guest, "mounts", "mounts", json!({}));
    assert!(
        !mounts.to_string().contains("persistent-disk"),
        "no persistent disk was asked for: {mounts}"
    );

    // Closing the channel ends the workload, and with it the VM.
    drop(guest);
    let status = run::wait_exit(&mut vmm, READY);
    assert!(status.success(), "the workload's own exit: {status}");

    let holders = host::inode_references(inode);
    assert!(
        holders.is_empty(),
        "the scratch file is still referenced by {holders:?}"
    );
    let backing = run::sudo(&["ls", "-A", &format!("{}/scratch", vmm.state_dir)]);
    assert_eq!(
        backing, "",
        "nothing is left in the scratch backing directory"
    );
    run::eventually(
        "the rootfs layer to leave with the container",
        READY,
        || (!exists(&upper)).then_some(()),
    );
    run::assert_framing(&vmm, true);
}

/// The task's persistent disk over a plain host directory.
#[test]
fn persistent_disk() {
    let run = run::load();
    let spec = run::Spec {
        persistent_disk: Some("/acmeCo/state"),
        ..run::Spec::probes(&run, "none.json")
    };
    let (vmm, mut guest) = start(&run, spec);
    let dir = vmm.persistent_dir.clone().expect("a persistent directory");

    let mounts = probe(&mut guest, "mounts", "mounts", json!({}));
    let line = mounts
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .find(|line| line.starts_with("persistent-disk /acmeCo/state virtiofs "))
        .unwrap_or_else(|| panic!("no persistent-disk mount in {mounts}"));
    let options: BTreeSet<&str> = line.split(' ').nth(3).unwrap_or("").split(',').collect();
    for option in ["rw", "nosuid", "nodev", "noexec"] {
        assert!(
            options.contains(option),
            "persistent-disk lacks {option}: {line}"
        );
    }

    let wrote = probe(
        &mut guest,
        "persistent-write",
        "write",
        json!({"path": "/acmeCo/state/written-by-guest", "data": "acmeCo"}),
    );
    assert_eq!(wrote, "ok", "persistent-write");
    let seen = run::sudo(&["cat", &format!("{dir}/written-by-guest")]);
    assert_eq!(seen, "acmeCo", "the guest's write, seen on the host");

    run::sudo_write(&format!("{dir}/run.sh"), b"#!/bin/sh\nexit 0\n", "0755");
    let exec = probe(
        &mut guest,
        "persistent-exec",
        "exec",
        json!({"argv": ["/acmeCo/state/run.sh"]}),
    );
    assert_eq!(exec, "exit:126", "persistent-exec is refused by noexec");

    let devices = probe(&mut guest, "devices", "devices", json!({}));
    assert_eq!(
        devices, "balloon:1 blk:1 console:1 fs:3 net:1 rng:1 vsock:1",
        "devices"
    );
    run::assert_framing(&vmm, true);
}

/// What the guest can reach of the VMM besides its network.
#[test]
fn control_channel() {
    let run = run::load();
    let (vmm, mut guest) = start(&run, run::Spec::probes(&run, "none.json"));
    let pid = run::pid(&vmm);

    // The mapped port is served by the VMM listening, so a connection the
    // guest initiates to it is reset.
    let mapped = probe(
        &mut guest,
        "vsock-mapped-port",
        "vsock",
        json!({"cid": 2, "port": 49092, "timeout": 5}),
    );
    assert_eq!(mapped, "error:ECONNRESET", "vsock-mapped-port");
    let unmapped = probe(
        &mut guest,
        "vsock-unmapped-port",
        "vsock",
        json!({"cid": 2, "port": 1234, "timeout": 3}),
    );
    assert_eq!(unmapped, "timeout", "vsock-unmapped-port");

    // A TSI proxy would be a new inet socket in the VMM's namespace.
    let before = run::nsenter(pid, &["ss", "-tuanp"]);
    let tsi = probe(&mut guest, "tsi-proxy-create", "tsi", json!({}));
    eprintln!("tsi-proxy-create: {tsi}");
    std::thread::sleep(Duration::from_secs(1));
    let after = run::nsenter(pid, &["ss", "-tuanp"]);
    assert_eq!(
        after, before,
        "the TSI datagram opened an inet socket in the VMM"
    );

    let devices = probe(&mut guest, "devices", "devices", json!({}));
    assert_eq!(
        devices, "balloon:1 blk:1 console:1 fs:2 net:1 rng:1 vsock:1",
        "devices"
    );
    let mounts = probe(&mut guest, "mounts", "mounts", json!({}));
    assert!(
        !mounts.to_string().contains("persistent-disk"),
        "no persistent disk was asked for: {mounts}"
    );

    // Path handling, resolved by the guest's own VFS: this says nothing about
    // a guest whose kernel is compromised.
    let mount = &vmm.connector_mount;
    for (name, a, b) in [
        ("dotdot-root", "/".to_string(), "/..".to_string()),
        (
            "dotdot-connector-mount",
            "/etc/hosts".to_string(),
            format!("{mount}/../../../../../../etc/hosts"),
        ),
    ] {
        let result = probe(&mut guest, name, "same", json!({"a": a, "b": b}));
        assert_eq!(result, "same-file", "{name}");
    }
    run::assert_framing(&vmm, true);
}

/// What goes when the VMM dies, and what only removal clears. The one
/// launch without `--rm`, so the container outlives its process.
#[test]
fn teardown() {
    let run = run::load();
    let spec = run::Spec {
        rm: false,
        ..run::Spec::probes(&run, "none.json")
    };
    let (vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    // Mid-flight: a guest holding a scratch file open and a connection up.
    let fill = probe(
        &mut guest,
        "scratch-held",
        "fill",
        json!({"path": "/scratch/held", "mib": 64, "keep": true}),
    );
    assert_eq!(
        fill,
        json!({"written": 64 * MIB, "stop": "done"}),
        "scratch-held"
    );

    let inode = host::fd_inode(pid, 3);
    // The name podman bound the VMM's network namespace to, under /run/netns.
    let netns = run::sudo(&["ip", "netns", "identify", &pid.to_string()]);
    assert!(
        !netns.trim().is_empty(),
        "the VMM's network namespace has no name"
    );
    let netns = format!("/run/netns/{}", netns.trim());
    // `eth0@if<N>`: N is the host-side peer's ifindex. The namespace's own
    // /sys is not reachable through nsenter, which keeps the host's mounts.
    let eth0 = run::nsenter(pid, &["ip", "-o", "link", "show", "eth0"]);
    let veth = eth0
        .split_once("@if")
        .and_then(|(_, rest)| rest.split(':').next())
        .unwrap_or_else(|| panic!("no peer index in {eth0:?}"))
        .to_string();
    let tap = run::nsenter(pid, &["ip", "-o", "link", "show", "tap0"]);
    assert!(tap.contains("tap0"), "tap0 before the kill: {tap}");
    let cgroup = format!(
        "/sys/fs/cgroup{}",
        run::inspect(&vmm.name, "{{.State.CgroupPath}}")
    );
    let upper = host::upperdir(&host::mountinfo(pid), "/rootfs").expect("an upperdir for /rootfs");
    let sock = run::socket_path(&vmm).to_string_lossy().into_owned();

    let host_link = |ifindex: &str| {
        run::sudo(&["ip", "-o", "link"])
            .lines()
            .any(|line| line.starts_with(&format!("{ifindex}:")))
    };
    let name = vmm.name.clone();
    let objects: Vec<(&str, Present)> = vec![
        (
            "the VMM process",
            Box::new(move || exists(&format!("/proc/{pid}"))),
        ),
        (
            "its scratch file",
            Box::new(move || !host::inode_references(inode).is_empty()),
        ),
        (
            "its network namespace",
            Box::new({
                let netns = netns.clone();
                move || exists(&netns)
            }),
        ),
        (
            "its host-side veth, and so the tap beside it",
            Box::new(move || host_link(&veth)),
        ),
        (
            "its cgroup",
            Box::new({
                let cgroup = cgroup.clone();
                move || exists(&cgroup)
            }),
        ),
        (
            "its rootfs layer",
            Box::new({
                let upper = upper.clone();
                move || exists(&upper)
            }),
        ),
        (
            "the container record",
            Box::new({
                let name = name.clone();
                move || {
                    run::sudo_output(&["podman", "container", "exists", &name], None)
                        .status
                        .success()
                }
            }),
        ),
        ("init.sock", Box::new(move || exists(&sock))),
    ];
    for (object, present) in &objects {
        assert!(present(), "{object} before the kill");
    }

    run::podman(&["kill", "-s", "KILL", &vmm.name]);
    run::eventually("the container to exit", READY, || {
        (run::inspect(&vmm.name, "{{.State.Status}}") == "exited").then_some(())
    });
    let mut split = String::new();
    split.push_str(&format!(
        "the guest, seen as its held init.sock connection: {}\n",
        if guest::closed_within(&mut guest, Duration::from_secs(10)) {
            "gone at process death"
        } else {
            "still open after process death"
        }
    ));

    // podman's own exit cleanup runs after the process has gone, so each
    // object gets the same window to follow it before removal is tried.
    let window = Instant::now() + Duration::from_secs(10);
    let mut gone = vec![false; objects.len()];
    while Instant::now() < window && gone.iter().any(|gone| !gone) {
        for (i, (_, present)) in objects.iter().enumerate() {
            gone[i] = gone[i] || !present();
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    run::podman(&["rm", &vmm.name]);
    for (i, (object, present)) in objects.iter().enumerate() {
        let fate = match (gone[i], present()) {
            (true, _) => "gone at process death",
            (false, false) => "gone at container removal",
            (false, true) => "still present after container removal",
        };
        split.push_str(&format!("{object}: {fate}\n"));
    }
    insta::assert_snapshot!(split);
    run::assert_framing(&vmm, true);
}

/// A guest kernel that panics on OOM ends its VMM promptly. The exit
/// code is recorded, not asserted: a panicking guest does not report one.
#[test]
fn oom_panic() {
    let run = run::load();
    let spec = run::Spec {
        memory_mib: 256,
        flags: vec!["--run-as-root".to_string()],
        exec: Some(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo 1 > /proc/sys/vm/panic_on_oom && exec /usr/local/bin/python -c \
             'a = [bytearray(64 << 20) for _ in range(1000)]'"
                .to_string(),
        ]),
        ..run::Spec::probes(&run, "none.json")
    };
    let mut vmm = run::start(&run, spec);
    let status = run::wait_exit(&mut vmm, Duration::from_secs(60));
    eprintln!(
        "the VMM exited {status} {:?} after launch",
        vmm.started.elapsed()
    );
    // libkrun relays the guest kernel's last words as its own ERROR records
    // on stderr, besides whatever reached the console on stdout.
    let output = run::stderr_text(&vmm) + &run::stdout_text(&vmm);
    assert!(
        output.contains("Kernel panic"),
        "the guest ended without a kernel panic"
    );
    run::assert_framing(&vmm, false);
}

/// `public` with `allowedNames`: an exact name and a wildcard against
/// controlled endpoints, and everything a connector must not reach.
#[test]
fn egress_allowlist() {
    let run = run::load();
    let [allowed_ip, unresolved_ip, wild_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let nameserver = dns::serve(allowed_ip);
    let allowed = endpoint::listen(allowed_ip);
    let unresolved = endpoint::listen(unresolved_ip);
    let wild = endpoint::listen(wild_ip);
    // Every refused name is scripted too, so a refusal is the gate's rather
    // than the nameserver's.
    for (name, ip) in [
        ("ok.acmeco.example", allowed_ip),
        ("ok.elsewhere.example", allowed_ip),
        ("below.ok.acmeco.example", allowed_ip),
        ("wild.acmeco.example", wild_ip),
        ("api.wild.acmeco.example", wild_ip),
        ("a.b.wild.acmeco.example", wild_ip),
    ] {
        dns::set(&nameserver, name, vec![dns::Record::A(ip, 300)]);
    }

    let mut spec = run::Spec::probes(&run, "allowlist.json");
    spec.flags = upstream(&nameserver);
    // Raw sockets need guest root, which only lasts until the workload runs.
    spec.flags.push("--as-root-exec".to_string());
    spec.flags.push(format!(
        "{} once '{}'",
        run::PROBES.join(" "),
        json!({"probe": "icmp-blocked", "op": "icmp", "name": "ok.acmeco.example", "timeout": BLOCKED})
    ));
    let (vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);
    let gateway = network_field(&vmm, "Gateway");
    let uplink = network_field(&vmm, "IPAddress");
    let port = allowed.addr.port();

    let resolved = probe(
        &mut guest,
        "resolve-allowed",
        "resolve",
        json!({"name": "ok.acmeco.example"}),
    );
    assert_eq!(resolved, format!("ok:{allowed_ip}"), "resolve-allowed");
    let held = probe(
        &mut guest,
        "connect-allowed",
        "hold",
        json!({"id": "allowed", "addr": allowed_ip.to_string(), "port": port, "timeout": 5}),
    );
    assert_eq!(held, "connected", "connect-allowed");
    let reply = probe(
        &mut guest,
        "exchange-allowed",
        "exchange",
        json!({"id": "allowed", "timeout": 5}),
    );
    assert_eq!(reply, "pong", "exchange-allowed");

    let (unlisted, took) = guest::call_timed(
        &mut guest,
        "unlisted-refused",
        "query",
        json!({"nameserver": VMM_TAP.to_string(), "name": "ok.elsewhere.example", "qtype": 1, "timeout": BLOCKED}),
    );
    assert_eq!(unlisted["status"], "rcode:5", "unlisted-refused");
    assert!(
        took < Duration::from_secs(1),
        "unlisted-refused took {took:?}"
    );

    for (name, qname) in [
        ("exact-subdomain-refused", "below.ok.acmeco.example"),
        ("wildcard-base-refused", "wild.acmeco.example"),
    ] {
        let (refused, took) = guest::call_timed(
            &mut guest,
            name,
            "query",
            json!({"nameserver": VMM_TAP.to_string(), "name": qname, "qtype": 1, "timeout": BLOCKED}),
        );
        assert_eq!(refused["status"], "rcode:5", "{name}");
        assert!(took < Duration::from_secs(1), "{name} took {took:?}");
    }
    for (name, qname) in [
        ("wildcard-resolved", "api.wild.acmeco.example"),
        ("wildcard-nested-resolved", "a.b.wild.acmeco.example"),
    ] {
        let resolved = probe(&mut guest, name, "resolve", json!({"name": qname}));
        assert_eq!(resolved, format!("ok:{wild_ip}"), "{name}");
    }
    assert_eq!(
        tcp(
            &mut guest,
            "connect-wildcard",
            wild_ip,
            wild.addr.port(),
            2.0
        ),
        "connected",
        "connect-wildcard"
    );

    let aaaa = query(&mut guest, "aaaa-empty", "ok.acmeco.example", 28);
    assert_eq!(aaaa, json!({"status": "ok", "records": []}), "aaaa-empty");

    let mut blocked = vec![
        ("connect-unresolved", unresolved_ip, unresolved.addr.port()),
        ("connect-gateway", gateway, 443),
        ("connect-vmm-uplink", uplink, 443),
        ("connect-vmm-tap", VMM_TAP, 443),
        ("connect-metadata", Ipv4Addr::new(169, 254, 169, 254), 80),
        ("connect-rfc1918-10", Ipv4Addr::new(10, 1, 2, 3), 443),
        ("connect-rfc1918-172", Ipv4Addr::new(172, 16, 0, 1), 443),
        ("connect-rfc1918-192", Ipv4Addr::new(192, 168, 0, 1), 443),
    ];
    for (name, addr, port) in &blocked {
        assert_eq!(
            tcp(&mut guest, name, addr, *port, BLOCKED),
            "timeout",
            "{name}"
        );
    }
    assert_eq!(
        tcp(&mut guest, "connect-smtp", allowed_ip, 25, BLOCKED),
        "timeout",
        "connect-smtp"
    );
    blocked.push(("connect-smtp", allowed_ip, 25));

    let ipv6 = probe(
        &mut guest,
        "connect-ipv6",
        "ipv6",
        json!({"addr": "2606:4700:4700::1111", "port": 443, "timeout": BLOCKED}),
    );
    assert_ne!(ipv6, "connected", "connect-ipv6");
    eprintln!("connect-ipv6: {ipv6}");

    // Inbound, from inside the VMM's own network namespace toward the guest.
    assert_eq!(
        probe(
            &mut guest,
            "inbound-listen",
            "listen",
            json!({"port": 34567})
        ),
        "listening"
    );
    let dial = r#"import socket
s = socket.socket()
s.settimeout(2)
try:
    s.connect(("192.0.2.2", 34567))
    print("connected")
except socket.timeout:
    print("timeout")
except OSError as e:
    print("error:%s" % e.errno)"#;
    assert_eq!(
        run::nsenter(pid, &["python3", "-c", dial]).trim(),
        "timeout",
        "inbound-dial"
    );
    let inbound = probe(
        &mut guest,
        "inbound-accepted",
        "accepted",
        json!({"port": 34567, "wait": 1}),
    );
    assert_eq!(inbound, "no-connection", "inbound-accepted");
    assert_eq!(
        stderr_probe(&vmm, "icmp-blocked"),
        "timeout",
        "icmp-blocked"
    );

    // Read while the allowed flow is still held open, so the table shows it.
    let flows = guest_flows(pid);
    assert!(
        flows
            .iter()
            .any(|flow| flow.proto == "tcp" && flow.dst == allowed_ip && flow.dport == Some(port)),
        "the allowed flow is missing from conntrack, so the table proves nothing: {flows:?}"
    );
    for flow in &flows {
        let dropped = blocked.iter().any(|(_, addr, port)| {
            flow.proto == "tcp" && flow.dst == *addr && flow.dport == Some(*port)
        });
        assert!(!dropped, "a blocked destination got through: {flow:?}");
        assert_ne!(flow.proto, "icmp", "ICMP got through: {flow:?}");
        // The guest's one local destination is the VMM's nameserver.
        if flow.dst == VMM_TAP {
            assert_eq!(
                (flow.proto.as_str(), flow.dport),
                ("udp", Some(53)),
                "{flow:?}"
            );
        }
    }
    let counters = host::counters(&vmm.name);
    for rule in [
        "forward/baseline",
        "forward/smtp",
        "forward/tcp-udp-only",
        "forward/forward-drop",
        "egress_accept/resolved",
        "input/tap-dns",
        "input/input-drop",
        "output/no-inbound",
        "postrouting/masquerade",
    ] {
        assert!(
            counter(&counters, rule) > 0,
            "{rule} never matched: {counters:?}"
        );
    }

    let upstream = dns::queries(&nameserver);
    for refused in [
        "ok.elsewhere.example",
        "below.ok.acmeco.example",
        "wild.acmeco.example",
    ] {
        assert!(
            !upstream.iter().any(|name| name == refused),
            "the refused name {refused} went upstream"
        );
    }
    assert!(
        endpoint::accepted(&allowed).contains(&uplink),
        "the allowed endpoint never saw the VMM's uplink"
    );
    assert!(
        endpoint::accepted(&wild).contains(&uplink),
        "the wildcard's endpoint never saw the VMM's uplink"
    );
    assert_eq!(
        endpoint::accepted(&unresolved),
        Vec::<Ipv4Addr>::new(),
        "connect-unresolved arrived"
    );
    run::assert_framing(&vmm, true);
}

/// TTL clamping, expiry, and a refresh that extends authorization under an
/// open connection.
#[test]
fn egress_ttl() {
    let run = run::load();
    let [dns_ip, _, short_ip, long_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let nameserver = dns::serve(dns_ip);
    let short = endpoint::listen(short_ip);
    let _long = endpoint::listen(long_ip);
    dns::set(
        &nameserver,
        "short.acmeco.example",
        vec![dns::Record::A(short_ip, 1)],
    );
    dns::set(
        &nameserver,
        "long.acmeco.example",
        vec![dns::Record::A(long_ip, 3600)],
    );

    let mut spec = run::Spec::probes(&run, "ttl.json");
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);

    let answered = Instant::now();
    let floor = query(&mut guest, "ttl-floor", "short.acmeco.example", 1);
    assert_eq!(
        floor["records"],
        json!([["A", short_ip.to_string(), 3]]),
        "ttl-floor: clamped up to ttlFloorSecs"
    );
    assert_eq!(
        element(&vmm, short_ip).timeout,
        Some(3),
        "the kernel's authorization for {short_ip}"
    );

    let cap = query(&mut guest, "ttl-cap", "long.acmeco.example", 1);
    assert_eq!(
        cap["records"],
        json!([["A", long_ip.to_string(), 10]]),
        "ttl-cap: clamped down to ttlCapSecs"
    );
    assert_eq!(
        element(&vmm, long_ip).timeout,
        Some(10),
        "the kernel's authorization for {long_ip}"
    );

    let port = short.addr.port();
    let held = probe(
        &mut guest,
        "connect-held",
        "hold",
        json!({"id": "held", "addr": short_ip.to_string(), "port": port, "timeout": 2}),
    );
    assert_eq!(held, "connected", "connect-held");

    dns::set(
        &nameserver,
        "short.acmeco.example",
        vec![dns::Record::A(short_ip, 8)],
    );
    let refresh = query(&mut guest, "ttl-refresh", "short.acmeco.example", 1);
    assert_eq!(
        refresh["records"],
        json!([["A", short_ip.to_string(), 8]]),
        "ttl-refresh"
    );
    assert_eq!(
        element(&vmm, short_ip).timeout,
        Some(8),
        "the refreshed authorization for {short_ip}"
    );

    // Past the first answer's expiry, the refresh is what admits a new flow.
    std::thread::sleep(
        (answered + Duration::from_secs(4)).saturating_duration_since(Instant::now()),
    );
    assert_eq!(
        tcp(&mut guest, "connect-after-refresh", short_ip, port, 2.0),
        "connected",
        "connect-after-refresh"
    );
    let reply = probe(
        &mut guest,
        "exchange-after-refresh",
        "exchange",
        json!({"id": "held", "timeout": 2}),
    );
    assert_eq!(reply, "pong", "exchange-after-refresh");

    run::eventually(
        &format!("{short_ip} to expire from @resolved"),
        Duration::from_secs(12),
        || {
            (!host::resolved(&vmm.name)
                .iter()
                .any(|element| element.addr == short_ip))
            .then_some(())
        },
    );
    let accepted = endpoint::accepted(&short).len();
    assert_eq!(
        tcp(&mut guest, "connect-after-expiry", short_ip, port, BLOCKED),
        "timeout",
        "connect-after-expiry"
    );
    assert_eq!(
        endpoint::accepted(&short).len(),
        accepted,
        "connect-after-expiry arrived"
    );
    // Established flows are admitted before the set is consulted, so this
    // shows expiry leaves them alone; the refresh's lack of a gap rests on
    // its delete and add being one transaction.
    let reply = probe(
        &mut guest,
        "exchange-after-expiry",
        "exchange",
        json!({"id": "held", "timeout": 2}),
    );
    assert_eq!(reply, "pong", "exchange-after-expiry");
    run::assert_framing(&vmm, true);
}

fn element(vmm: &run::Vmm, addr: Ipv4Addr) -> host::Element {
    let elements = host::resolved(&vmm.name);
    elements
        .iter()
        .find(|element| element.addr == addr)
        .cloned()
        .unwrap_or_else(|| panic!("{addr} is not in @resolved: {elements:?}"))
}

/// Remembered CNAME targets, and private answers refused under every policy
/// that could otherwise admit them.
#[test]
fn egress_cname() {
    let run = run::load();
    let [dns_ip, unresolved_ip, _, _, target_ip, spare_ip] = run.endpoints[..] else {
        panic!("expected six endpoints in run.json");
    };
    let nameserver = dns::serve(dns_ip);
    let target = endpoint::listen(target_ip);
    dns::set(
        &nameserver,
        "alias.acmeco.example",
        vec![dns::Record::Cname("target.cdn.example".into(), 5)],
    );
    dns::set(
        &nameserver,
        "target.cdn.example",
        vec![dns::Record::A(target_ip, 5)],
    );
    dns::set(
        &nameserver,
        "below.target.cdn.example",
        vec![dns::Record::A(target_ip, 5)],
    );
    dns::set(
        &nameserver,
        "private.acmeco.example",
        vec![dns::Record::A(Ipv4Addr::new(10, 1, 2, 3), 300)],
    );
    dns::set(
        &nameserver,
        "alias2.acmeco.example",
        vec![dns::Record::Cname("target2.cdn.example".into(), 60)],
    );
    dns::set(
        &nameserver,
        "target2.cdn.example",
        vec![dns::Record::A(spare_ip, 60)],
    );

    let mut spec = run::Spec::probes(&run, "ttl.json");
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);

    let answered = Instant::now();
    let chain = query(&mut guest, "cname-chain", "alias.acmeco.example", 1);
    assert_eq!(
        chain["records"],
        json!([
            ["CNAME", "target.cdn.example", 5],
            ["A", target_ip.to_string(), 5]
        ]),
        "cname-chain"
    );
    let remembered = query(
        &mut guest,
        "cname-target-remembered",
        "target.cdn.example",
        1,
    );
    assert_eq!(
        remembered["status"], "ok",
        "cname-target-remembered: {remembered}"
    );
    let below = query(
        &mut guest,
        "cname-target-subdomain-refused",
        "below.target.cdn.example",
        1,
    );
    assert_eq!(below["status"], "rcode:5", "cname-target-subdomain-refused");
    assert_eq!(
        tcp(
            &mut guest,
            "connect-cname-target",
            target_ip,
            target.addr.port(),
            2.0
        ),
        "connected",
        "connect-cname-target"
    );

    let private = query(&mut guest, "private-refused", "private.acmeco.example", 1);
    assert_eq!(private["status"], "rcode:5", "private-refused");
    let chain2 = query(&mut guest, "cname-chain-2", "alias2.acmeco.example", 1);
    assert_eq!(chain2["status"], "ok", "cname-chain-2: {chain2}");
    dns::set(
        &nameserver,
        "target2.cdn.example",
        vec![dns::Record::A(Ipv4Addr::new(10, 1, 2, 4), 60)],
    );
    let turned = query(
        &mut guest,
        "remembered-target-private",
        "target2.cdn.example",
        1,
    );
    assert_eq!(turned["status"], "rcode:5", "remembered-target-private");
    let elements = host::resolved(&vmm.name);
    assert!(
        elements.iter().all(|element| !element.addr.is_private()),
        "a private answer reached @resolved: {elements:?}"
    );

    // The chain's CNAME answered with 5s, so by now its memory has lapsed.
    std::thread::sleep(
        (answered + Duration::from_secs(6)).saturating_duration_since(Instant::now()),
    );
    let upstream_before = dns::queries(&nameserver).len();
    let forgotten = query(
        &mut guest,
        "cname-target-forgotten",
        "target.cdn.example",
        1,
    );
    assert_eq!(forgotten["status"], "rcode:5", "cname-target-forgotten");
    assert_eq!(
        dns::queries(&nameserver).len(),
        upstream_before,
        "the forgotten target went upstream"
    );
    assert!(
        !dns::queries(&nameserver)
            .iter()
            .any(|name| name == "below.target.cdn.example"),
        "the remembered target's subdomain went upstream"
    );
    run::assert_framing(&vmm, true);
    drop(guest);
    drop(vmm);

    // allowAll lifts the name gate, never the baseline.
    dns::set(
        &nameserver,
        "private.anywhere.example",
        vec![dns::Record::A(Ipv4Addr::new(10, 1, 2, 3), 300)],
    );
    dns::set(
        &nameserver,
        "public.anywhere.example",
        vec![dns::Record::A(dns_ip, 300)],
    );
    let unresolved = endpoint::listen(unresolved_ip);
    let mut spec = run::Spec::probes(&run, "allow-all.json");
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    let private = query(
        &mut guest,
        "allow-all-private-refused",
        "private.anywhere.example",
        1,
    );
    assert_eq!(private["status"], "rcode:5", "allow-all-private-refused");
    let public = query(
        &mut guest,
        "allow-all-unlisted-name",
        "public.anywhere.example",
        1,
    );
    assert_eq!(public["status"], "ok", "allow-all-unlisted-name: {public}");
    assert_eq!(
        tcp(
            &mut guest,
            "allow-all-unresolved-address",
            unresolved_ip,
            unresolved.addr.port(),
            2.0
        ),
        "connected",
        "allow-all-unresolved-address"
    );
    let metadata = Ipv4Addr::new(169, 254, 169, 254);
    assert_eq!(
        tcp(&mut guest, "allow-all-metadata", metadata, 80, BLOCKED),
        "timeout",
        "allow-all-metadata"
    );
    let flows = guest_flows(pid);
    assert!(
        flows.iter().all(|flow| flow.dst != metadata),
        "the metadata service got through under allowAll: {flows:?}"
    );
    run::assert_framing(&vmm, true);
}

/// `egress: none`: nothing resolves and nothing leaves.
#[test]
fn egress_none() {
    let run = run::load();
    let nameserver = dns::serve(run.endpoints[0]);
    let target = endpoint::listen(run.endpoints[0]);
    dns::set(
        &nameserver,
        "ok.acmeco.example",
        vec![dns::Record::A(run.endpoints[0], 300)],
    );

    let mut spec = run::Spec::probes(&run, "none.json");
    // Ignored: no resolver runs without public egress.
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    let resolved = probe(
        &mut guest,
        "resolve-none",
        "resolve",
        json!({"name": "ok.acmeco.example"}),
    );
    assert!(
        resolved.as_str().is_some_and(|r| r.starts_with("error:")),
        "resolve-none: {resolved}"
    );
    let raw = query(&mut guest, "query-none", "ok.acmeco.example", 1);
    assert_eq!(raw["status"], "timeout", "query-none");
    let port = target.addr.port();
    assert_eq!(
        tcp(&mut guest, "connect-none", run.endpoints[0], port, BLOCKED),
        "timeout",
        "connect-none"
    );
    assert_eq!(
        tcp(
            &mut guest,
            "connect-none-metadata",
            Ipv4Addr::new(169, 254, 169, 254),
            80,
            BLOCKED
        ),
        "timeout",
        "connect-none-metadata"
    );

    let flows = guest_flows(pid);
    assert!(
        flows.is_empty(),
        "guest flows got through with egress none: {flows:?}"
    );
    let counters = host::counters(&vmm.name);
    assert_eq!(
        counter(&counters, "postrouting/masquerade"),
        0,
        "masqueraded packets: {counters:?}"
    );
    for rule in [
        "forward/forward-drop",
        "forward/baseline",
        "input/input-drop",
    ] {
        assert!(
            counter(&counters, rule) > 0,
            "{rule} never matched: {counters:?}"
        );
    }
    assert!(
        dns::queries(&nameserver).is_empty(),
        "a query went upstream"
    );
    assert!(
        endpoint::accepted(&target).is_empty(),
        "connect-none arrived"
    );
    run::assert_framing(&vmm, true);
}

/// The one test that uses the real internet, through the real resolver path,
/// so an outage fails it alone and says so.
#[test]
fn public_https_smoke() {
    let run = run::load();
    let (vmm, mut guest) = start(&run, run::Spec::probes(&run, "smoke.json"));

    let resolved = probe(
        &mut guest,
        "resolve-pypi",
        "resolve",
        json!({"name": "pypi.org"}),
    );
    let addr = resolved
        .as_str()
        .and_then(|r| r.strip_prefix("ok:"))
        .and_then(|r| r.split(',').next())
        .unwrap_or_else(|| panic!("pypi.org did not resolve through podman's resolver (an external dependency): {resolved}"))
        .to_string();
    let connected = tcp(&mut guest, "connect-pypi-443", &addr, 443, 10.0);
    assert_eq!(
        connected, "connected",
        "pypi.org:443 ({addr}) from the guest; this depends on the internet, not only on policy"
    );
    run::assert_framing(&vmm, true);
}

/// The shared connector-mount contract, as a guest sees it.
#[test]
fn connector_mount() {
    let run = run::load();
    let generation = |n: u32| {
        json!({
            "token": format!("acmeCo-generation-{n}"),
            "control_plane_url": "https://control.acmeco.example",
            "config_encryption_url": "https://encryption.acmeco.example",
        })
        .to_string()
    };
    let first = generation(1);
    let spec = run::Spec {
        task_update: Some(&first),
        ..run::Spec::probes(&run, "none.json")
    };
    let (vmm, mut guest) = start(&run, spec);
    let mount = vmm.connector_mount.clone();

    // The test guest's image sets CONNECTOR_MOUNT and LOG_LEVEL itself; the
    // contract's values must win.
    let env = probe(&mut guest, "contract-env", "env", json!({}));
    assert_eq!(env["CONNECTOR_MOUNT"], mount.as_str(), "CONNECTOR_MOUNT");
    assert_eq!(env["LOG_FORMAT"], "json", "LOG_FORMAT");
    assert_eq!(env["LOG_LEVEL"], "warn", "LOG_LEVEL");

    let mounts = probe(&mut guest, "mounts", "mounts", json!({}));
    let line = mounts
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .find(|line| line.starts_with(&format!("connector-mount {mount} virtiofs ")))
        .unwrap_or_else(|| panic!("no connector-mount share at {mount} in {mounts}"))
        .to_string();
    let options: BTreeSet<&str> = line.split(' ').nth(3).unwrap_or("").split(',').collect();
    for option in ["ro", "nosuid", "nodev"] {
        assert!(
            options.contains(option),
            "the connector mount lacks {option}: {line}"
        );
    }
    assert!(
        !options.contains("noexec"),
        "flow-connector-init is executed from here: {line}"
    );

    let write = probe(
        &mut guest,
        "mount-write",
        "write",
        json!({"path": format!("{mount}/x"), "data": "x"}),
    );
    assert_eq!(write, "error:EROFS", "mount-write");
    let list = probe(&mut guest, "mount-list", "list", json!({"path": &mount}));
    assert_eq!(list, "error:EACCES", "mount-list: 0711 hides the listing");
    let inspect = probe(
        &mut guest,
        "mount-read",
        "read",
        json!({"path": format!("{mount}/image-inspect.json")}),
    );
    assert!(
        inspect["content"]
            .as_str()
            .is_some_and(|content| content.starts_with('[')),
        "mount-read: {inspect}"
    );
    let exec = probe(
        &mut guest,
        "mount-exec",
        "exec",
        json!({"argv": [format!("{mount}/flow-connector-init"), "--help"]}),
    );
    assert_eq!(exec, "exit:0", "mount-exec");

    let path = format!("{mount}/task-update.json");
    let read = probe(
        &mut guest,
        "task-update-read",
        "read",
        json!({"path": &path}),
    );
    assert_eq!(read["content"], first.as_str(), "task-update-read");

    // Renamed in, as the runtime refreshes it: a new inode each time, which the
    // guest sees once virtiofs revalidates the name. The latency is reported,
    // not asserted beyond the probe's bound.
    for n in [2, 3] {
        let next = generation(n);
        let renamed = Instant::now();
        run::replace_task_update(&mount, &next);
        let seen = probe(
            &mut guest,
            &format!("task-update-generation-{n}"),
            "await",
            json!({"path": &path, "content": &next, "timeout": 15}),
        );
        assert_eq!(
            seen,
            json!({"seen": true}),
            "generation {n} never reached the guest"
        );
        eprintln!(
            "task-update.json generation {n} visible after {:?}",
            renamed.elapsed()
        );
    }
    run::assert_framing(&vmm, true);
}
