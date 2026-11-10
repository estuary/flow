//! The connector VMM's KVM integration suite. Each test launches its own VMM
//! containers on the reference launch line.

mod dns_answers;
mod dns_capacity;

use connector_vmm_tests::{dns, endpoint, guest, host, netns, run};
use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

/// The tap's two ends, which the VMM pins.
const VMM_TAP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const GUEST: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);

/// A backstop for boot and process exit, not a latency requirement.
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

fn probe(
    guest: &mut guest::Guest,
    name: &str,
    op: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    guest::call(guest, name, op, arguments)
}

fn tcp(
    guest: &mut guest::Guest,
    name: &str,
    addr: impl ToString,
    port: u16,
    timeout: f64,
) -> serde_json::Value {
    probe(
        guest,
        name,
        "tcp",
        serde_json::json!({"addr": addr.to_string(), "port": port, "timeout": timeout}),
    )
}

fn udp(
    guest: &mut guest::Guest,
    name: &str,
    addr: impl ToString,
    port: u16,
    timeout: f64,
) -> serde_json::Value {
    probe(
        guest,
        name,
        "udp",
        serde_json::json!({"addr": addr.to_string(), "port": port, "timeout": timeout}),
    )
}

fn query(guest: &mut guest::Guest, name: &str, qname: &str, qtype: u16) -> serde_json::Value {
    probe(
        guest,
        name,
        "query",
        serde_json::json!({"nameserver": VMM_TAP.to_string(), "name": qname, "qtype": qtype, "timeout": BLOCKED}),
    )
}

fn upstream(nameserver: &dns::Nameserver) -> Vec<String> {
    vec![
        "--resolver-upstream".to_string(),
        nameserver.addr.to_string(),
    ]
}

fn network_field(vmm: &run::Vmm, field: &str) -> Ipv4Addr {
    address(&vmm.name, &vmm.network, field)
}

fn address(container: &str, network: &str, field: &str) -> Ipv4Addr {
    run::inspect(
        container,
        &format!("{{{{(index .NetworkSettings.Networks \"{network}\").{field}}}}}"),
    )
    .parse()
    .unwrap_or_else(|e| panic!("{field} of {container}: {e}"))
}

fn listen(run: &run::Run, ip: Ipv4Addr) -> endpoint::Endpoint {
    endpoint::listen(netns::tcp(
        netns::Namespace::Named(&run.endpoint_netns),
        (ip, 0).into(),
    ))
}

fn echo(run: &run::Run, ip: Ipv4Addr) -> endpoint::Endpoint {
    endpoint::echo(netns::udp(
        netns::Namespace::Named(&run.endpoint_netns),
        (ip, 0).into(),
    ))
}

fn scripted(run: &run::Run, ip: Ipv4Addr) -> dns::Nameserver {
    dns::serve(netns::udp(
        netns::Namespace::Named(&run.endpoint_netns),
        (ip, 0).into(),
    ))
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

/// A probe result the guest wrote to its stderr, for the `once` probes a
/// workload script runs before the control channel exists.
fn stderr_probe(vmm: &run::Vmm, name: &str) -> serde_json::Value {
    run::stderr_text(vmm)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|line| line["probe"] == name)
        .unwrap_or_else(|| panic!("no {name} probe line on the guest's stderr"))["result"]
        .take()
}

/// What the launch line gives the VMM, with and without the task's
/// persistent disk.
#[test]
fn launch() {
    let run = run::load();
    let (vmm, _guest) = start(&run, run::Spec::probes(&run, run::policy::EMPTY));
    assert_launch(&vmm, &["/scratch-backing", "/sock"]);
    insta::assert_snapshot!(mount_matrix(&vmm));
    run::assert_framing(&vmm, true);
}

#[test]
fn launch_with_persistent_disk() {
    let run = run::load();
    let spec = run::Spec {
        persistent_disk: Some("/acmeCo/state"),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, _guest) = start(&run, spec);
    assert_launch(&vmm, &["/persistent-disk", "/scratch-backing", "/sock"]);
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
    // Exactly the launch line's CHOWN, DAC_OVERRIDE, FOWNER, FSETID, SETGID,
    // SETUID, NET_BIND_SERVICE and NET_ADMIN: none of podman's other defaults.
    assert_eq!(cap_eff, Some("00000000000014db"), "CapEff of the VMM");

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
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let mut vmm = run::start(&run, spec);
    assert_refused(
        &mut vmm,
        "flow-connector-vmm: / is writable; the VMM container must be launched with `--read-only`\n"
            .to_string(),
    );
}

#[test]
fn refuses_a_writable_root_image() {
    let run = run::load();
    let spec = run::Spec {
        edit: |argv| {
            for argument in argv.iter_mut() {
                if argument.starts_with("--mount=type=volume,") {
                    *argument = argument.replace(",readonly", ",rw");
                }
            }
        },
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let mut vmm = run::start(&run, spec);
    assert_refused(
        &mut vmm,
        "flow-connector-vmm: /rootfs is writable; the VMM container must be launched with \
         `--mount type=volume,source=V,destination=/rootfs,readonly`\n"
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
        ..run::Spec::probes(&run, run::policy::EMPTY)
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
    let vmm = run::start(&run, run::Spec::probes(&run, run::policy::EMPTY));
    let ready = run::wait_ready(&vmm, READY);
    eprintln!("readiness {ready:?} after podman run");
    run::assert_framing(&vmm, true);
}

/// An image whose `User` resolves only through links: `/etc/passwd` is an
/// absolute link and `/etc/group` a relative one climbing above the root.
/// Neither target exists in the VMM's own root, so the guest runs as that
/// user and group only if both resolved in the image.
#[test]
fn image_user_through_linked_account_files() {
    let run = run::load();
    let image = format!(
        "localhost/connector-vmm-kvm-linked-accounts:{}",
        run::random_hex()
    );
    run::record("image", &image);
    let containerfile = format!(
        "FROM {}\n\
         USER root\n\
         RUN mkdir /usr/lib/acme && cp /etc/passwd /etc/group /usr/lib/acme/ \
         && echo 'acmesvc:x:4321:4321::/:/usr/sbin/nologin' >> /usr/lib/acme/passwd \
         && echo 'acmegrp:x:4322:' >> /usr/lib/acme/group \
         && ln -sf /usr/lib/acme/passwd /etc/passwd \
         && ln -sf ../../../usr/lib/acme/group /etc/group\n\
         USER acmesvc:acmegrp\n",
        run.guest_image
    );
    let context = run::fixture("");
    let built = run::sudo_output(
        &[
            "podman",
            "build",
            "-q",
            "-t",
            &image,
            "-f",
            "-",
            &context.to_string_lossy(),
        ],
        Some(containerfile.as_bytes()),
    );
    assert!(
        built.status.success(),
        "building {image}: {}",
        String::from_utf8_lossy(&built.stderr)
    );

    let spec = run::Spec {
        connector_image: image.clone(),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);
    let status = probe(
        &mut guest,
        "status",
        "read",
        serde_json::json!({"path": "/proc/self/status"}),
    );
    let ids: Vec<&str> = status["content"]
        .as_str()
        .unwrap_or_else(|| panic!("reading /proc/self/status: {status}"))
        .lines()
        .filter(|line| line.starts_with("Uid:") || line.starts_with("Gid:"))
        .collect();
    assert_eq!(
        ids,
        [
            "Uid:\t4321\t4321\t4321\t4321",
            "Gid:\t4322\t4322\t4322\t4322"
        ]
    );

    std::mem::drop(guest);
    run::stop(vmm);
    run::podman(&["image", "rm", &image]);
}

/// No exec beneath the workload gains privilege from a setuid or setgid bit or
/// a file capability, and an image whose user is root keeps root's privilege.
///
/// The control runs each program, as the same user, in a plain podman
/// container without no_new_privs, where each grants its privilege. That is
/// the host's kernel and container storage, not the guest's virtiofs root.
/// The guest workload always has no_new_privs, so it also checks the bits and
/// capability on a root mounted without `nosuid`.
#[test]
fn no_new_privileges() {
    let run = run::load();
    let tag = run::random_hex();
    let user_image = format!("localhost/connector-vmm-kvm-no-new-privs:{tag}");
    let root_image = format!("localhost/connector-vmm-kvm-no-new-privs-root:{tag}");
    // Copies of cat, so each prints the credentials its own exec gave it. The
    // image has no setcap, so the xattr is written as a v2 `vfs_cap_data`:
    // CAP_NET_BIND_SERVICE, permitted and effective.
    let containerfile = format!(
        "FROM {} AS acmesvc\n\
         USER root\n\
         RUN echo 'acmesvc:x:4321:4321::/:/usr/sbin/nologin' >> /etc/passwd \
         && echo 'acmesvc:x:4321:' >> /etc/group \
         && echo 'acmeextra:x:4323:acmesvc' >> /etc/group \
         && cp /usr/bin/cat /usr/local/bin/setuid-cat && chmod 4755 /usr/local/bin/setuid-cat \
         && cp /usr/bin/cat /usr/local/bin/setgid-cat && chmod 2755 /usr/local/bin/setgid-cat \
         && cp /usr/bin/cat /usr/local/bin/filecap-cat \
         && /usr/local/bin/python -c 'import os, struct; os.setxattr(\"/usr/local/bin/filecap-cat\", \"security.capability\", struct.pack(\"<5I\", 0x02000001, 1 << 10, 0, 0, 0))'\n\
         USER acmesvc\n\
         FROM acmesvc AS root\n\
         USER root\n",
        run.guest_image
    );
    let context = run::fixture("");
    for (image, target) in [(&user_image, "acmesvc"), (&root_image, "root")] {
        run::record("image", image);
        let built = run::sudo_output(
            &[
                "podman",
                "build",
                "-q",
                "--target",
                target,
                "-t",
                image,
                "-f",
                "-",
                &context.to_string_lossy(),
            ],
            Some(containerfile.as_bytes()),
        );
        assert!(
            built.status.success(),
            "building {image}: {}",
            String::from_utf8_lossy(&built.stderr)
        );
    }
    let ran = |name: &str, result: serde_json::Value| {
        let stdout = result["stdout"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: {result}"));
        format!("{name}, exit {}:\n{}", result["exit"], credentials(stdout))
    };
    let read = |name: &str, result: serde_json::Value| {
        let content = result["content"]
            .as_str()
            .unwrap_or_else(|| panic!("{name}: {result}"));
        credentials(content)
    };
    let programs = ["setuid-cat", "setgid-cat", "filecap-cat"];
    let path = |program: &str| format!("/usr/local/bin/{program}");

    let mut table = String::from("a podman container, as acmesvc:\n");
    for program in programs {
        let output = run::sudo_output(
            &[
                "podman",
                "run",
                "--rm",
                "--network=none",
                "--entrypoint",
                &path(program),
                &user_image,
                "/proc/self/status",
            ],
            None,
        );
        assert!(
            output.status.success(),
            "{program} in a container: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("status is UTF-8");
        table.push_str(&format!("{program}:\n{}", credentials(&stdout)));
    }

    let spec = run::Spec {
        connector_image: user_image.clone(),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);
    table.push_str("the guest's view:\n");
    for program in programs {
        let stat = probe(
            &mut guest,
            program,
            "stat",
            serde_json::json!({"path": path(program)}),
        );
        table.push_str(&format!(
            "{program}: mode {} capability {}\n",
            stat["mode"], stat["capability"]
        ));
    }
    let mounts = probe(&mut guest, "mounts", "mounts", serde_json::json!({}));
    let root = mounts
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .find(|line| line.split(' ').nth(1) == Some("/"))
        .unwrap_or_else(|| panic!("no / in {mounts}"));
    table.push_str(&format!("/: {root}\n"));

    let workload = probe(
        &mut guest,
        "workload",
        "read",
        serde_json::json!({"path": "/proc/self/status"}),
    );
    table.push_str("the workload, as acmesvc:\n");
    table.push_str(&read("workload", workload));
    for program in programs {
        let result = probe(
            &mut guest,
            program,
            "run",
            serde_json::json!({"argv": [path(program), "/proc/self/status"]}),
        );
        table.push_str(&ran(program, result));
    }
    std::mem::drop(guest);
    run::stop(vmm);

    let spec = run::Spec {
        connector_image: root_image.clone(),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);
    let workload = probe(
        &mut guest,
        "workload",
        "read",
        serde_json::json!({"path": "/proc/self/status"}),
    );
    table.push_str("the workload, as root:\n");
    table.push_str(&read("workload", workload));
    let cat = probe(
        &mut guest,
        "cat",
        "run",
        serde_json::json!({"argv": ["/usr/bin/cat", "/proc/self/status"]}),
    );
    table.push_str(&ran("cat", cat));
    std::mem::drop(guest);
    run::stop(vmm);
    run::podman(&["image", "rm", &root_image, &user_image]);

    insta::assert_snapshot!(table, @r#"
    a podman container, as acmesvc:
    setuid-cat:
    Uid: 4321 0 0 0
    Gid: 4321 4321 4321 4321
    Groups: 4321 4323
    CapPrm: 00000000800405fb
    CapEff: 00000000800405fb
    CapBnd: 00000000800405fb
    NoNewPrivs: 0
    setgid-cat:
    Uid: 4321 4321 4321 4321
    Gid: 4321 0 0 0
    Groups: 4321 4323
    CapPrm: 0000000000000000
    CapEff: 0000000000000000
    CapBnd: 00000000800405fb
    NoNewPrivs: 0
    filecap-cat:
    Uid: 4321 4321 4321 4321
    Gid: 4321 4321 4321 4321
    Groups: 4321 4323
    CapPrm: 0000000000000400
    CapEff: 0000000000000400
    CapBnd: 00000000800405fb
    NoNewPrivs: 0
    the guest's view:
    setuid-cat: mode "4755" capability "error:ENODATA"
    setgid-cat: mode "2755" capability "error:ENODATA"
    filecap-cat: mode "0755" capability "0100000200040000000000000000000000000000"
    /: /dev/root / virtiofs rw,relatime 0 0
    the workload, as acmesvc:
    Uid: 4321 4321 4321 4321
    Gid: 4321 4321 4321 4321
    Groups: 4321 4323
    CapPrm: 0000000000000000
    CapEff: 0000000000000000
    CapBnd: 000001ffffffffff
    NoNewPrivs: 1
    setuid-cat, exit 0:
    Uid: 4321 4321 4321 4321
    Gid: 4321 4321 4321 4321
    Groups: 4321 4323
    CapPrm: 0000000000000000
    CapEff: 0000000000000000
    CapBnd: 000001ffffffffff
    NoNewPrivs: 1
    setgid-cat, exit 0:
    Uid: 4321 4321 4321 4321
    Gid: 4321 4321 4321 4321
    Groups: 4321 4323
    CapPrm: 0000000000000000
    CapEff: 0000000000000000
    CapBnd: 000001ffffffffff
    NoNewPrivs: 1
    filecap-cat, exit 0:
    Uid: 4321 4321 4321 4321
    Gid: 4321 4321 4321 4321
    Groups: 4321 4323
    CapPrm: 0000000000000000
    CapEff: 0000000000000000
    CapBnd: 000001ffffffffff
    NoNewPrivs: 1
    the workload, as root:
    Uid: 0 0 0 0
    Gid: 0 0 0 0
    Groups: 0
    CapPrm: 000001ffffffffff
    CapEff: 000001ffffffffff
    CapBnd: 000001ffffffffff
    NoNewPrivs: 1
    cat, exit 0:
    Uid: 0 0 0 0
    Gid: 0 0 0 0
    Groups: 0
    CapPrm: 000001ffffffffff
    CapEff: 000001ffffffffff
    CapBnd: 000001ffffffffff
    NoNewPrivs: 1
    "#);
}

/// The identity lines of a `/proc/<pid>/status`, with whitespace collapsed.
fn credentials(status: &str) -> String {
    status
        .lines()
        .filter(|line| {
            [
                "Uid:",
                "Gid:",
                "Groups:",
                "CapPrm:",
                "CapEff:",
                "CapBnd:",
                "NoNewPrivs:",
            ]
            .iter()
            .any(|field| line.starts_with(field))
        })
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" ") + "\n")
        .collect()
}

/// Environment defaults reach the workload.
#[test]
fn image_environment() {
    let run = run::load();
    let image = format!(
        "localhost/connector-vmm-kvm-environment:{}",
        run::random_hex()
    );
    run::record("image", &image);
    let containerfile = format!(
        "FROM {}\n\
         USER root\n\
         RUN echo 'acmesvc:x:4321:4321::/home/acmesvc:/usr/sbin/nologin' >> /etc/passwd\n\
         USER acmesvc\n",
        run.guest_image
    );
    let context = run::fixture("");
    let built = run::sudo_output(
        &[
            "podman",
            "build",
            "-q",
            "-t",
            &image,
            "-f",
            "-",
            &context.to_string_lossy(),
        ],
        Some(containerfile.as_bytes()),
    );
    assert!(
        built.status.success(),
        "building {image}: {}",
        String::from_utf8_lossy(&built.stderr)
    );

    let spec = run::Spec {
        connector_image: image.clone(),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);
    let kernel = probe(
        &mut guest,
        "hostname",
        "read",
        serde_json::json!({"path": "/proc/sys/kernel/hostname"}),
    );
    let kernel = kernel["content"]
        .as_str()
        .unwrap_or_else(|| panic!("reading /proc/sys/kernel/hostname: {kernel}"))
        .trim()
        .to_string();
    let env = probe(&mut guest, "env", "env", serde_json::json!({}));
    assert_eq!(env["HOME"], "/home/acmesvc", "HOME: {env}");
    assert_eq!(env["HOSTNAME"], kernel.as_str(), "HOSTNAME: {env}");

    std::mem::drop(guest);
    run::stop(vmm);
    run::podman(&["image", "rm", &image]);
}

/// The workload's `UV_CACHE_DIR` is its image's, or unset as in the image.
/// `TMPDIR` is `/tmp` either way.
#[test]
fn image_uv_cache_dir() {
    let run = run::load();
    let tag = run::random_hex();
    let context = run::fixture("");
    let mut table = String::new();
    // `FROM scratch` copies the guest image's files without its `Env`, from
    // which no instruction can remove a variable.
    for (name, containerfile) in [
        (
            "custom",
            format!(
                "FROM {}\nENV UV_CACHE_DIR=/var/tmp/acme-uv-cache\n",
                run.guest_image
            ),
        ),
        (
            "absent",
            format!(
                "FROM scratch\nCOPY --from={} / /\nUSER nobody\n",
                run.guest_image
            ),
        ),
    ] {
        let image = format!("localhost/connector-vmm-kvm-uv-cache:{tag}-{name}");
        run::record("image", &image);
        let built = run::sudo_output(
            &[
                "podman",
                "build",
                "-q",
                "-t",
                &image,
                "-f",
                "-",
                &context.to_string_lossy(),
            ],
            Some(containerfile.as_bytes()),
        );
        assert!(
            built.status.success(),
            "building {image}: {}",
            String::from_utf8_lossy(&built.stderr)
        );

        let spec = run::Spec {
            connector_image: image.clone(),
            ..run::Spec::probes(&run, run::policy::EMPTY)
        };
        let (vmm, mut guest) = start(&run, spec);
        let env = probe(&mut guest, "env", "env", serde_json::json!({}));
        table.push_str(&format!(
            "{name}: TMPDIR={} UV_CACHE_DIR={}\n",
            env["TMPDIR"], env["UV_CACHE_DIR"]
        ));
        std::mem::drop(guest);
        run::stop(vmm);
        run::podman(&["image", "rm", &image]);
    }
    insta::assert_snapshot!(table, @r#"
    custom: TMPDIR="/tmp" UV_CACHE_DIR="/var/tmp/acme-uv-cache"
    absent: TMPDIR="/tmp" UV_CACHE_DIR=null
    "#);
}

/// The packaged VMM's default workload: connector-init from the connector
/// mount, serving over vsock, answering a Spec RPC dialed unprivileged.
#[test]
fn spec_rpc_through_init_sock() {
    let run = run::load();
    let spec = run::Spec {
        vmm_image: run.production_vmm_image.clone(),
        connector_image: run.hello_world_image.clone(),
        exec: None,
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let vmm = run::start(&run, spec);
    run::wait_ready(&vmm, READY);

    let spec = spec_rpc(&vmm);
    assert_eq!(spec.protocol, 3032023, "the capture protocol");
    assert!(!spec.config_schema_json.is_empty(), "a config schema");
    run::assert_framing(&vmm, true);
}

/// A capture Spec RPC to connector-init, dialed unprivileged over init.sock.
fn spec_rpc(vmm: &run::Vmm) -> proto_flow::capture::response::Spec {
    let path = run::socket_path(vmm);
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
    *spec
}

/// The packaged VMM's default workload, as the connector process it starts
/// sees itself: the image's user and groups, no_new_privs, its `WorkingDir`
/// as mounted, and the guest's mounts.
#[test]
fn production_guest() {
    let run = run::load();
    let image = format!(
        "localhost/connector-vmm-kvm-production-guest:{}",
        run::random_hex()
    );
    run::record("image", &image);
    let containerfile = format!(
        "FROM {}\n\
         USER root\n\
         RUN echo 'acmesvc:x:4321:4321::/:/usr/sbin/nologin' >> /etc/passwd \
         && echo 'acmesvc:x:4321:' >> /etc/group \
         && echo 'acmeextra:x:4323:acmesvc' >> /etc/group\n\
         USER acmesvc\n\
         WORKDIR /tmp\n\
         ENTRYPOINT [\"{}\", \"{}\", \"spec\"]\n",
        run.guest_image,
        run::PROBES[0],
        run::PROBES[1],
    );
    let context = run::fixture("");
    let built = run::sudo_output(
        &[
            "podman",
            "build",
            "-q",
            "-t",
            &image,
            "-f",
            "-",
            &context.to_string_lossy(),
        ],
        Some(containerfile.as_bytes()),
    );
    assert!(
        built.status.success(),
        "building {image}: {}",
        String::from_utf8_lossy(&built.stderr)
    );

    let spec = run::Spec {
        vmm_image: run.production_vmm_image.clone(),
        connector_image: image.clone(),
        exec: None,
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let vmm = run::start(&run, spec);
    run::wait_ready(&vmm, READY);
    let spec = spec_rpc(&vmm);
    let seen: serde_json::Value =
        serde_json::from_slice(&spec.config_schema_json).expect("the probe answers JSON");

    let mut table = credentials(seen["status"].as_str().expect("a status"));
    table.push_str(&format!("cwd {}: {}\n", seen["cwd"], seen["cwd-by-path"]));
    for line in seen["mounts"]
        .as_array()
        .expect("mounts")
        .iter()
        .filter_map(serde_json::Value::as_str)
    {
        let point = line.split(' ').nth(1).unwrap_or("");
        if [
            "/",
            "/scratch",
            "/tmp",
            "/var/tmp",
            "/etc",
            run::GUEST_CONNECTOR_MOUNT,
        ]
        .contains(&point)
        {
            table.push_str(&format!("{line}\n"));
        }
    }
    run::assert_framing(&vmm, true);
    run::stop(vmm);
    run::podman(&["image", "rm", &image]);

    insta::assert_snapshot!(table, @r#"
    Uid: 4321 4321 4321 4321
    Gid: 4321 4321 4321 4321
    Groups: 4321 4323
    CapPrm: 0000000000000000
    CapEff: 0000000000000000
    CapBnd: 000001ffffffffff
    NoNewPrivs: 1
    cwd "/tmp": "same-file"
    /dev/root / virtiofs rw,relatime 0 0
    /dev/vda /scratch ext4 rw,nosuid,nodev,relatime,discard 0 0
    /dev/vda /tmp ext4 rw,nosuid,nodev,relatime,discard 0 0
    /dev/vda /var/tmp ext4 rw,nosuid,nodev,relatime,discard 0 0
    /dev/vda /etc ext4 ro,nosuid,nodev,relatime,discard 0 0
    connector-mount /connector-mount virtiofs ro,nosuid,nodev,relatime 0 0
    "#);
}

/// The packaged VMM refuses each test control as it parses, and neither image
/// takes `--run-as-root` or `--as-root-exec`.
#[test]
fn production_refuses_test_controls() {
    let run = run::load();
    let mut table = String::new();

    for (name, image, flags) in [
        ("production", &run.production_vmm_image, &["--debug"][..]),
        (
            "production",
            &run.production_vmm_image,
            &["--resolver-upstream", "192.0.2.53:53"],
        ),
        (
            "production",
            &run.production_vmm_image,
            &["--exec", "/bin/true"],
        ),
        (
            "production",
            &run.production_vmm_image,
            &["--ttl-floor-secs", "3"],
        ),
        ("production", &run.production_vmm_image, &["--run-as-root"]),
        (
            "production",
            &run.production_vmm_image,
            &["--as-root-exec", "id"],
        ),
        ("test", &run.vmm_image, &["--exec", "/bin/true"]),
        ("test", &run.vmm_image, &["--run-as-root"]),
        ("test", &run.vmm_image, &["--as-root-exec", "id"]),
    ] {
        let mut argv = vec![
            "podman",
            "run",
            "--rm",
            "--network=none",
            image,
            "run",
            "--connector-mount",
            "/connector-mounts-0/mount-acme",
            "--memory-mib",
            "512",
            "--vcpus",
            "2",
            "--disk-mib",
            "256",
        ];
        argv.extend(flags);
        let output = run::sudo_output(&argv, None);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let first = stderr
            .lines()
            .find(|line| line.starts_with("flow-connector-vmm:"))
            .unwrap_or("");
        table.push_str(&format!(
            "{name} {}: exit {:?}, {first}\n",
            flags.join(" "),
            output.status.code()
        ));
    }
    insta::assert_snapshot!(table, @r"
    production --debug: exit Some(2), flow-connector-vmm: error: unexpected argument '--debug' found
    production --resolver-upstream 192.0.2.53:53: exit Some(2), flow-connector-vmm: error: unexpected argument '--resolver-upstream' found
    production --exec /bin/true: exit Some(2), flow-connector-vmm: error: unexpected argument '--exec' found
    production --ttl-floor-secs 3: exit Some(2), flow-connector-vmm: error: unexpected argument '--ttl-floor-secs' found
    production --run-as-root: exit Some(2), flow-connector-vmm: error: unexpected argument '--run-as-root' found
    production --as-root-exec id: exit Some(2), flow-connector-vmm: error: unexpected argument '--as-root-exec' found
    test --exec /bin/true: exit Some(2), flow-connector-vmm: / is writable; the VMM container must be launched with `--read-only`
    test --run-as-root: exit Some(2), flow-connector-vmm: error: unexpected argument '--run-as-root' found
    test --as-root-exec id: exit Some(2), flow-connector-vmm: error: unexpected argument '--as-root-exec' found
    ");
}

/// Real libc and raw clients see complete UDP answers or a prompt SERVFAIL.
#[test]
fn dns_udp_sizes() {
    let run = run::load();
    let nameserver = scripted(&run, run.endpoints[0]);
    dns::limit_udp(&nameserver);
    let address = run.endpoints[1];
    for (name, count) in [
        ("small.acmeco.example", 4),
        ("medium.acmeco.example", 20),
        ("large.acmeco.example", 256),
    ] {
        dns::set(&nameserver, name, vec![dns::Record::A(address, 300); count]);
    }
    let mut spec = run::Spec::probes(&run, run::policy::CAPACITY);
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    for (name, edns, want) in [
        ("large.acmeco.example", 1232, "rcode:2"),
        ("medium.acmeco.example", 0, "rcode:2"),
        ("small.acmeco.example", 0, "ok"),
        ("medium.acmeco.example", 1232, "ok"),
    ] {
        let answer = probe(
            &mut guest,
            "size",
            "query",
            serde_json::json!({
                "name": name, "nameserver": "192.0.2.1", "edns": edns, "timeout": 5
            }),
        );
        assert_eq!(answer["status"], want, "{name} edns={edns}: {answer}");
        if want == "rcode:2" {
            assert!(
                host::resolved(&vmm.name).is_empty(),
                "truncation authorized an address"
            );
        }
    }
    for name in ["small.acmeco.example", "medium.acmeco.example"] {
        assert_eq!(
            probe(
                &mut guest,
                "libc",
                "resolve",
                serde_json::json!({"name": name})
            ),
            format!("ok:{address}"),
            "libc {name}"
        );
    }
    let large = probe(
        &mut guest,
        "libc-large",
        "resolve",
        serde_json::json!({"name": "large.acmeco.example"}),
    );
    assert!(large.as_str().unwrap().starts_with("error:gai"), "{large}");
}

/// Discard returns data blocks while the VM still owns the backing inode.
#[test]
fn scratch_reclaims() {
    let run = run::load();
    let (vmm, mut guest) = start(&run, run::Spec::probes(&run, run::policy::EMPTY));
    let pid = run::pid(&vmm);
    let features = probe(
        &mut guest,
        "block-features",
        "read",
        serde_json::json!({
            "path": "/sys/block/vda/device/features"
        }),
    );
    let features = features["content"].as_str().expect("virtio block features");
    assert_eq!(
        features.as_bytes()[9],
        b'0',
        "scratch must not advertise FLUSH: {features}"
    );
    assert_eq!(
        features.as_bytes()[13],
        b'1',
        "scratch must advertise DISCARD: {features}"
    );
    let before = host::fd_inode(pid, 3).bytes;
    let written = 128 * MIB;
    assert_eq!(
        probe(
            &mut guest,
            "fill",
            "fill",
            serde_json::json!({"path": "/tmp/reclaim", "mib": 128})
        ),
        serde_json::json!({"written": written, "stop": "done"})
    );
    let filled = host::fd_inode(pid, 3).bytes;
    assert!(
        filled >= before + written * 9 / 10,
        "before={before} filled={filled}"
    );
    assert_eq!(
        probe(
            &mut guest,
            "delete",
            "exec",
            serde_json::json!({
                "argv": ["/usr/local/bin/python", "-c", "import os; os.unlink('/tmp/reclaim'); os.sync()"]
            })
        ),
        "exit:0"
    );
    let after = host::fd_inode(pid, 3).bytes;
    eprintln!("scratch allocated bytes: before={before} filled={filled} after={after}");
    // ext4 retains metadata; only the large data allocation must be reclaimed.
    assert!(
        filled >= after + written * 9 / 10,
        "filled={filled} after={after}"
    );
    assert!(
        after < before + written / 10,
        "before={before} after={after}"
    );
}

#[test]
fn azure_platform_egress() {
    let run = run::load();
    let azure = Ipv4Addr::new(168, 63, 129, 16);
    assert_eq!(
        run.decoys[2], azure,
        "the Azure endpoint is isolated in the lab"
    );
    let nameserver = scripted(&run, run.endpoints[0]);
    let name = "name.acmeco.example";
    dns::set(&nameserver, name, vec![dns::Record::A(azure, 300)]);
    let endpoint = listen(&run, azure);
    let datagrams = echo(&run, azure);
    for (policy, cidrs) in [
        (run::policy::CAPACITY, vec![]),
        (run::policy::AZURE, vec!["168.63.129.16/32"]),
        (run::policy::EVERY_ADDRESS, vec!["0.0.0.0/0"]),
    ] {
        let mut spec = run::Spec::probes(&run, policy);
        spec.flags = upstream(&nameserver);
        let (vmm, mut guest) = start(&run, spec);
        let _permissions = (!cidrs.is_empty()).then(|| run::ingress(&run, &vmm, &[], &cidrs));
        assert_eq!(
            query(&mut guest, "azure", name, 1)["status"],
            if cidrs.is_empty() { "rcode:5" } else { "ok" }
        );
        assert!(
            host::resolved(&vmm.name).is_empty(),
            "Azure is never DNS-authorized"
        );
        for compromised in [false, true] {
            if compromised {
                flush(&vmm);
                attacker_nat(&vmm);
            }
            assert_eq!(
                tcp(
                    &mut guest,
                    "azure-tcp",
                    azure,
                    endpoint.addr.port(),
                    BLOCKED
                ),
                if cidrs.is_empty() {
                    "timeout"
                } else {
                    "connected"
                },
                "{policy:?} compromised={compromised}"
            );
            assert_eq!(
                udp(
                    &mut guest,
                    "azure-udp",
                    azure,
                    datagrams.addr.port(),
                    BLOCKED
                ),
                if cidrs.is_empty() { "timeout" } else { "pong" },
                "{policy:?} compromised={compromised}"
            );
        }
    }
}

/// Where the guest's bytes land, and that the cgroup holds.
#[test]
fn storage() {
    let run = run::load();
    let mut spec = run::Spec::probes(&run, run::policy::EMPTY);
    spec.memory_mib = 512;
    spec.disk_mib = 768;
    let (mut vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    let mounts = host::mountinfo(pid);
    assert!(
        mounts
            .iter()
            .any(|mount| mount.point == "/rootfs"
                && mount.options.iter().any(|option| option == "ro")),
        "podman gave /rootfs a writable mount: {mounts:?}"
    );
    let env = probe(&mut guest, "env", "env", serde_json::json!({}));
    assert_eq!(env["TMPDIR"], "/tmp", "TMPDIR: {env}");
    // The image's own. mkdir fails if anything created it before the workload.
    assert_eq!(env["UV_CACHE_DIR"], "/tmp/uv-cache", "UV_CACHE_DIR: {env}");
    let mkdir = probe(
        &mut guest,
        "uv-cache-mkdir",
        "exec",
        serde_json::json!({"argv": ["/usr/bin/mkdir", "/tmp/uv-cache"]}),
    );
    assert_eq!(mkdir, "exit:0", "the workload creates uv's cache");

    let mut written = 0;
    for (name, path) in [
        ("var-tmp-fill", "/var/tmp/fill"),
        ("uv-cache-fill", "/tmp/uv-cache/fill"),
    ] {
        let fill = probe(
            &mut guest,
            name,
            "fill",
            serde_json::json!({"path": path, "mib": 64}),
        );
        assert_eq!(
            fill,
            serde_json::json!({"written": 64 * MIB, "stop": "done"}),
            "{name}"
        );
        written += 64 * MIB;
    }
    // More than the guest's RAM, to /tmp: it stops where the disk, less what
    // the other two hold, runs out.
    let tmp = probe(
        &mut guest,
        "tmp-fill",
        "fill",
        serde_json::json!({"path": "/tmp/fill", "mib": 1024}),
    );
    assert_eq!(
        tmp["stop"], "error:ENOSPC",
        "tmp-fill stops at the end of the disk"
    );
    let tmp = tmp["written"].as_u64().expect("bytes written");
    let left = 768 * MIB - written;
    assert!(
        tmp <= left && tmp >= left * 9 / 10,
        "tmp-fill wrote {tmp} where {left} were left"
    );
    written += tmp;

    for (name, path) in [
        ("var-tmp-full", "/var/tmp/after"),
        ("uv-cache-full", "/tmp/uv-cache/after"),
    ] {
        let write = probe(
            &mut guest,
            name,
            "write",
            serde_json::json!({"path": path, "data": "acmeCo"}),
        );
        assert_eq!(write, "error:ENOSPC", "{name}: one disk is full");
    }

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
        "memory.peak after the fills: {}",
        run::sudo(&["cat", &format!("{cgroup}/memory.peak")]).trim()
    );

    let inode = host::fd_inode(pid, 3);
    assert_eq!(inode.links, 0, "the scratch disk is an unnamed O_TMPFILE");
    assert!(
        inode.bytes >= written * 9 / 10,
        "the scratch file holds {} bytes for {written} written",
        inode.bytes
    );
    // Without this, the walk could miss the file and its freeing prove nothing.
    let holders = host::inode_references(inode);
    assert!(
        holders.contains(&format!("/proc/{pid}/fd/3")),
        "the walk misses the VMM's own scratch descriptor: {holders:?}"
    );

    let mounts = probe(&mut guest, "mounts", "mounts", serde_json::json!({}));
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
    run::assert_framing(&vmm, true);
}

/// The root is read-only outside the guest. A root workload's writes fail
/// though its own view mounts `/` read-write, and still fail once it has
/// unmounted what flow-guest-init bound over the image.
#[test]
fn read_only_root() {
    let run = run::load();
    let write = |name: &str, path: &str| serde_json::json!({"probe": name, "op": "write", "path": path, "data": "acmeCo"});
    let (mounted, unmounted) = (
        [
            write("root-slash", "/acmeCo"),
            write("root-usr", "/usr/acmeCo"),
        ],
        [
            write("root-etc-unmounted", "/etc/acmeCo"),
            write("root-tmp-unmounted", "/tmp/acmeCo"),
        ],
    );
    let quoted = |requests: &[serde_json::Value]| {
        requests
            .iter()
            .map(|request| format!("'{request}'"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let probes = run::PROBES.join(" ");
    // The unmounts are in a mount namespace of their own, so the probe server
    // keeps its `/etc` and `/tmp`.
    let script = format!(
        "for r in {}; do {probes} once \"$r\"; done; \
         unshare -m sh -c 'umount /etc /tmp && for r in \"$@\"; do {probes} once \"$r\"; done' sh {}; \
         exec {probes} serve",
        quoted(&mounted),
        quoted(&unmounted),
    );
    let spec = run::Spec {
        connector_image: run.root_guest_image.clone(),
        exec: Some(vec!["/bin/sh".to_string(), "-c".to_string(), script]),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);

    let mounts = probe(&mut guest, "mounts", "mounts", serde_json::json!({}));
    let root = mounts
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .find(|line| line.split(' ').nth(1) == Some("/"))
        .unwrap_or_else(|| panic!("no / in {mounts}"));
    assert!(
        root.split(' ')
            .nth(3)
            .unwrap_or("")
            .split(',')
            .any(|o| o == "rw"),
        "the guest's own view of / is read-write: {root}"
    );

    for request in mounted.iter().chain(&unmounted) {
        let name = request["probe"].as_str().unwrap();
        assert_eq!(stderr_probe(&vmm, name), "error:EROFS", "{name}");
    }
    run::assert_framing(&vmm, true);
}

/// The workload starts in its image's `WorkingDir` as mounted in the guest, not
/// in the image directory a guest mount covers. A `WorkingDir` that only the
/// image had fails the launch.
#[test]
fn working_dir() {
    let run = run::load();
    let tag = run::random_hex();
    let context = run::fixture("");
    let build = |name: &str, working_dir: &str| {
        let image = format!("localhost/connector-vmm-kvm-working-dir:{tag}-{name}");
        run::record("image", &image);
        let containerfile = format!(
            "FROM {}\n\
             USER root\n\
             RUN ln -s /tmp /work && mkdir /tmp/project\n\
             USER nobody\n\
             WORKDIR {working_dir}\n",
            run.guest_image
        );
        let built = run::sudo_output(
            &[
                "podman",
                "build",
                "-q",
                "-t",
                &image,
                "-f",
                "-",
                &context.to_string_lossy(),
            ],
            Some(containerfile.as_bytes()),
        );
        assert!(
            built.status.success(),
            "building {image}: {}",
            String::from_utf8_lossy(&built.stderr)
        );
        image
    };

    let mut table = String::new();
    for (name, working_dir, mounted) in [
        ("root", "/", "/"),
        ("tmp", "/tmp", "/tmp"),
        ("var-tmp", "/var/tmp", "/var/tmp"),
        ("etc", "/etc", "/etc"),
        ("work", "/work", "/tmp"),
    ] {
        let image = build(name, working_dir);
        let spec = run::Spec {
            connector_image: image.clone(),
            ..run::Spec::probes(&run, run::policy::EMPTY)
        };
        let (vmm, mut guest) = start(&run, spec);
        let same = probe(
            &mut guest,
            "same",
            "same",
            serde_json::json!({"a": ".", "b": mounted}),
        );
        let write = probe(
            &mut guest,
            "write",
            "write",
            serde_json::json!({"path": "acmeCo-working-dir", "data": "acmeCo"}),
        );
        table.push_str(&format!(
            "{working_dir}: . and {mounted} are {same}, writing in . is {write}\n"
        ));
        std::mem::drop(guest);
        run::stop(vmm);
        run::podman(&["image", "rm", &image]);
    }
    insta::assert_snapshot!(table, @r#"
    /: . and / are "same-file", writing in . is "error:EACCES"
    /tmp: . and /tmp are "same-file", writing in . is "ok"
    /var/tmp: . and /var/tmp are "same-file", writing in . is "ok"
    /etc: . and /etc are "same-file", writing in . is "error:EROFS"
    /work: . and /tmp are "same-file", writing in . is "ok"
    "#);

    let image = build("covered", "/tmp/project");
    let spec = run::Spec {
        connector_image: image.clone(),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let mut vmm = run::start(&run, spec);
    let status = run::wait_exit(&mut vmm, READY);
    let stderr = run::stderr_text(&vmm);
    let reported: Vec<&str> = stderr
        .lines()
        .filter(|line| line.starts_with("flow-guest-init:"))
        .collect();
    assert_eq!(
        (status.code(), reported),
        (
            Some(125),
            vec![
                "flow-guest-init: re-entering the working directory /tmp/project after mounting: \
                 No such file or directory (os error 2)"
            ]
        ),
        "a WorkingDir beneath the scratch mount"
    );
    run::assert_framing(&vmm, false);
    run::stop(vmm);
    run::podman(&["image", "rm", &image]);
}

/// The task's persistent disk over a plain host directory.
#[test]
fn persistent_disk() {
    let run = run::load();
    let spec = run::Spec {
        persistent_disk: Some("/acmeCo/state"),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);
    let dir = vmm.persistent_dir.clone().expect("a persistent directory");

    let mounts = probe(&mut guest, "mounts", "mounts", serde_json::json!({}));
    let line = mounts
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
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
        serde_json::json!({"path": "/acmeCo/state/written-by-guest", "data": "acmeCo"}),
    );
    assert_eq!(wrote, "ok", "persistent-write");
    let seen = run::sudo(&["cat", &format!("{dir}/written-by-guest")]);
    assert_eq!(seen, "acmeCo", "the guest's write, seen on the host");

    run::sudo_write(&format!("{dir}/run.sh"), b"#!/bin/sh\nexit 0\n", "0755");
    let exec = probe(
        &mut guest,
        "persistent-exec",
        "exec",
        serde_json::json!({"argv": ["/acmeCo/state/run.sh"]}),
    );
    assert_eq!(exec, "exit:126", "persistent-exec is refused by noexec");

    let devices = probe(&mut guest, "devices", "devices", serde_json::json!({}));
    assert_eq!(
        devices, "balloon:1 blk:1 console:1 fs:3 net:1 rng:1 vsock:1",
        "devices"
    );
    run::assert_framing(&vmm, true);
}

/// Operations of a guest on the image's restrictive files, and on the
/// persistent disk at /state, each printed with its result or errno. The
/// VMM's virtiofs server performs each on the host, in its own process.
const FILE_OPERATIONS: &str = r#"
import errno, os, struct

def attempt(name, operation):
    try:
        result = operation()
    except OSError as error:
        result = errno.errorcode[error.errno]
    print(f"{name}: {'ok' if result is None else result}")

def read(path):
    with open(path) as f:
        return f.read()

def write(path):
    with open(path, "w") as f:
        f.write("acmeCo")

def owned(path):
    st = os.stat(path)
    return f"{st.st_mode & 0o7777:04o} {st.st_uid}:{st.st_gid}"

attempt("read the user's 0600 file", lambda: read("/acmeCo/user-only"))
attempt("read root's 0600 file", lambda: read("/acmeCo/root-only"))
attempt("list the user's 0700 directory", lambda: os.listdir("/acmeCo/private"))
attempt("read the user's 0600 file within it", lambda: read("/acmeCo/private/file"))
attempt("create a file", lambda: write("/state/file"))
attempt("its mode and owner", lambda: owned("/state/file"))
attempt("chmod it 0640", lambda: os.chmod("/state/file", 0o640))
attempt("set its times", lambda: os.utime("/state/file", (1000000000, 1000000000)))
attempt("its mode, owner and mtime", lambda: f"{owned('/state/file')} {int(os.stat('/state/file').st_mtime)}")
attempt("make a directory", lambda: os.mkdir("/state/dir"))
attempt("chmod it 2775", lambda: os.chmod("/state/dir", 0o2775))
attempt("its mode and owner", lambda: owned("/state/dir"))
attempt("make a file within it", lambda: write("/state/dir/inner"))
attempt("whose mode and owner", lambda: owned("/state/dir/inner"))
attempt("chown the file to 1000:1000", lambda: os.chown("/state/file", 1000, 1000))
attempt("rename it into the directory", lambda: os.rename("/state/file", "/state/dir/file"))
attempt("its mode and owner", lambda: owned("/state/dir/file"))
attempt("remove both", lambda: [os.unlink(f"/state/dir/{name}") for name in ("file", "inner")] and None)
attempt("remove the directory", lambda: os.rmdir("/state/dir"))
attempt("create a file the host reads", lambda: write("/state/kept"))
# CAP_NET_BIND_SERVICE, permitted and effective, as a v2 `vfs_cap_data`.
attempt("set a file capability on it", lambda: os.setxattr("/state/kept", "security.capability", struct.pack("<5I", 0x02000001, 1 << 10, 0, 0, 0)))
"#;

/// The VMM container's capabilities serve a nonroot image user and root
/// alike: each reads what the image lets it, its restrictive files
/// included, though the VMM's own virtiofs server opens them as host root.
/// Each writes the persistent disk as itself, the server switching its
/// thread to the guest's uid and gid for each create. Without
/// `CAP_SETFCAP`, root sets no file capability there: the guest mounts the
/// disk `nosuid`, which would ignore one anyway.
#[test]
fn guest_users_files_and_storage() {
    let run = run::load();
    // acmesvc's and root's files of modes 0600 and 0700 under /acmeCo.
    let tag = run::random_hex();
    let containerfile = format!(
        "FROM {} AS acmesvc\n\
         USER root\n\
         RUN echo 'acmesvc:x:4321:4321::/:/usr/sbin/nologin' >> /etc/passwd \
         && echo 'acmesvc:x:4321:' >> /etc/group \
         && mkdir -m 0755 /acmeCo \
         && printf user > /acmeCo/user-only && chown 4321:4321 /acmeCo/user-only \
         && chmod 0600 /acmeCo/user-only \
         && printf root > /acmeCo/root-only && chmod 0600 /acmeCo/root-only \
         && mkdir -m 0700 /acmeCo/private && printf private > /acmeCo/private/file \
         && chmod 0600 /acmeCo/private/file && chown -R 4321:4321 /acmeCo/private\n\
         USER acmesvc\n\
         FROM acmesvc AS root\n\
         USER root\n",
        run.guest_image
    );
    let context = run::fixture("");
    let images = ["acmesvc", "root"].map(|target| {
        let image = format!("localhost/connector-vmm-kvm-restrictive-{target}:{tag}");
        run::record("image", &image);
        let built = run::sudo_output(
            &[
                "podman",
                "build",
                "-q",
                "--target",
                target,
                "-t",
                &image,
                "-f",
                "-",
                &context.to_string_lossy(),
            ],
            Some(containerfile.as_bytes()),
        );
        assert!(
            built.status.success(),
            "building {image}: {}",
            String::from_utf8_lossy(&built.stderr)
        );
        (target, image)
    });

    let mut table = String::new();
    for (user, image) in images {
        let spec = run::Spec {
            connector_image: image,
            persistent_disk: Some("/state"),
            ..run::Spec::probes(&run, run::policy::EMPTY)
        };
        let (vmm, mut guest) = start(&run, spec);
        let ran = probe(
            &mut guest,
            "file-operations",
            "run",
            serde_json::json!({"argv": [run::PROBES[0], "-c", FILE_OPERATIONS]}),
        );
        assert_eq!(ran["exit"], 0, "{ran}");
        let dir = vmm.persistent_dir.clone().expect("a persistent directory");
        let kept = run::sudo(&["stat", "-c", "%a %u:%g %s", &format!("{dir}/kept")]);
        table.push_str(&format!(
            "# {user}\n{}on the host, kept: {}\n",
            ran["stdout"].as_str().expect("stdout"),
            kept.trim()
        ));
        run::assert_framing(&vmm, true);
    }
    insta::assert_snapshot!(table, @r"
    # acmesvc
    read the user's 0600 file: user
    read root's 0600 file: EACCES
    list the user's 0700 directory: ['file']
    read the user's 0600 file within it: private
    create a file: ok
    its mode and owner: 0644 4321:4321
    chmod it 0640: ok
    set its times: ok
    its mode, owner and mtime: 0640 4321:4321 1000000000
    make a directory: ok
    chmod it 2775: ok
    its mode and owner: 2775 4321:4321
    make a file within it: ok
    whose mode and owner: 0644 4321:4321
    chown the file to 1000:1000: EPERM
    rename it into the directory: ok
    its mode and owner: 0640 4321:4321
    remove both: ok
    remove the directory: ok
    create a file the host reads: ok
    set a file capability on it: EPERM
    on the host, kept: 644 4321:4321 6
    # root
    read the user's 0600 file: user
    read root's 0600 file: root
    list the user's 0700 directory: ['file']
    read the user's 0600 file within it: private
    create a file: ok
    its mode and owner: 0644 0:0
    chmod it 0640: ok
    set its times: ok
    its mode, owner and mtime: 0640 0:0 1000000000
    make a directory: ok
    chmod it 2775: ok
    its mode and owner: 2775 0:0
    make a file within it: ok
    whose mode and owner: 0644 0:0
    chown the file to 1000:1000: ok
    rename it into the directory: ok
    its mode and owner: 0640 1000:1000
    remove both: ok
    remove the directory: ok
    create a file the host reads: ok
    set a file capability on it: EPERM
    on the host, kept: 644 0:0 6
    ");
}

/// What the guest can reach of the VMM besides its network.
#[test]
fn control_channel() {
    let run = run::load();
    let (vmm, mut guest) = start(&run, run::Spec::probes(&run, run::policy::EMPTY));
    let pid = run::pid(&vmm);

    // The mapped port is served by the VMM listening, so a connection the
    // guest initiates to it is reset.
    let mapped = probe(
        &mut guest,
        "vsock-mapped-port",
        "vsock",
        serde_json::json!({"cid": 2, "port": 49092, "timeout": 5}),
    );
    assert_eq!(mapped, "error:ECONNRESET", "vsock-mapped-port");
    let unmapped = probe(
        &mut guest,
        "vsock-unmapped-port",
        "vsock",
        serde_json::json!({"cid": 2, "port": 1234, "timeout": 3}),
    );
    assert_eq!(unmapped, "timeout", "vsock-unmapped-port");

    // A TSI proxy would be a new inet socket in the VMM's namespace.
    let before = run::nsenter(pid, &["ss", "-tuanp"]);
    let tsi = probe(&mut guest, "tsi-proxy-create", "tsi", serde_json::json!({}));
    eprintln!("tsi-proxy-create: {tsi}");
    std::thread::sleep(Duration::from_secs(1));
    let after = run::nsenter(pid, &["ss", "-tuanp"]);
    assert_eq!(
        after, before,
        "the TSI datagram opened an inet socket in the VMM"
    );

    let devices = probe(&mut guest, "devices", "devices", serde_json::json!({}));
    assert_eq!(
        devices, "balloon:1 blk:1 console:1 fs:2 net:1 rng:1 vsock:1",
        "devices"
    );
    let mounts = probe(&mut guest, "mounts", "mounts", serde_json::json!({}));
    assert!(
        !mounts.to_string().contains("persistent-disk"),
        "no persistent disk was asked for: {mounts}"
    );

    // Path handling, resolved by the guest's own VFS: this says nothing about
    // a guest whose kernel is compromised.
    let mount = run::GUEST_CONNECTOR_MOUNT;
    for (name, a, b) in [
        ("dotdot-root", "/".to_string(), "/..".to_string()),
        (
            "dotdot-connector-mount",
            "/etc/hosts".to_string(),
            format!("{mount}/../../../../../../etc/hosts"),
        ),
    ] {
        let result = probe(
            &mut guest,
            name,
            "same",
            serde_json::json!({"a": a, "b": b}),
        );
        assert_eq!(result, "same-file", "{name}");
    }
    run::assert_framing(&vmm, true);
}

/// Kernel resources are released after process death and container removal.
/// The one launch without `--rm`, so removal is explicit.
#[test]
fn teardown() {
    let run = run::load();
    let spec = run::Spec {
        rm: false,
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    // Mid-flight: a guest holding a scratch file open and a connection up.
    let fill = probe(
        &mut guest,
        "scratch-held",
        "fill",
        serde_json::json!({"path": "/tmp/held", "mib": 64, "keep": true}),
    );
    assert_eq!(
        fill,
        serde_json::json!({"written": 64 * MIB, "stop": "done"}),
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
    assert!(
        guest::closed_within(&mut guest, READY),
        "the guest connection closed"
    );
    run::podman(&["rm", &vmm.name]);
    // The socket's directory entry belongs to the launcher. Kernel resources
    // must be gone, regardless of when podman's asynchronous exit cleanup ran.
    let remaining = run::eventually("kernel resources to be released", READY, || {
        let remaining: Vec<_> = objects
            .iter()
            .filter_map(|(name, present)| present().then_some(*name))
            .collect();
        (remaining == ["init.sock"]).then_some(remaining)
    });
    insta::assert_debug_snapshot!(remaining);
    run::assert_framing(&vmm, true);
}

/// A guest kernel that panics on OOM ends its VMM promptly. The exit
/// code is recorded, not asserted: a panicking guest does not report one.
#[test]
fn oom_panic() {
    let run = run::load();
    let spec = run::Spec {
        memory_mib: 256,
        connector_image: run.root_guest_image.clone(),
        exec: Some(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo 1 > /proc/sys/vm/panic_on_oom && exec /usr/local/bin/python -c \
             'a = [bytearray(64 << 20) for _ in range(1000)]'"
                .to_string(),
        ]),
        ..run::Spec::probes(&run, run::policy::EMPTY)
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

/// Permitted names: an exact name and a wildcard against
/// controlled endpoints, and everything a connector must not reach.
#[test]
fn egress_allowlist() {
    let run = run::load();
    let [allowed_ip, unresolved_ip, wild_ip, ..] = run.endpoints[..] else {
        panic!("too few endpoints in run.json");
    };
    let nameserver = scripted(&run, allowed_ip);
    let allowed = listen(&run, allowed_ip);
    let unresolved = listen(&run, unresolved_ip);
    let wild = listen(&run, wild_ip);
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

    // Raw sockets need a root workload, so a guest of its own, under the same
    // policy and nameserver, sends the echo request.
    let spec = run::Spec {
        connector_image: run.root_guest_image.clone(),
        flags: upstream(&nameserver),
        ..run::Spec::probes(&run, run::policy::ALLOWLIST)
    };
    let (root_vmm, mut root_guest) = start(&run, spec);
    let icmp = probe(
        &mut root_guest,
        "icmp-blocked",
        "icmp",
        serde_json::json!({"name": "ok.acmeco.example", "timeout": BLOCKED}),
    );
    assert_eq!(icmp, "timeout", "icmp-blocked");
    let counters = host::counters(&root_vmm.name);
    assert!(
        counter(&counters, "forward/tcp-udp-only") > 0,
        "the echo request never met forward/tcp-udp-only: {counters:?}"
    );
    std::mem::drop(root_guest);
    run::stop(root_vmm);

    let mut spec = run::Spec::probes(&run, run::policy::ALLOWLIST);
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);
    let gateway = network_field(&vmm, "Gateway");
    let uplink = network_field(&vmm, "IPAddress");
    let port = allowed.addr.port();

    let resolved = probe(
        &mut guest,
        "resolve-allowed",
        "resolve",
        serde_json::json!({"name": "ok.acmeco.example"}),
    );
    assert_eq!(resolved, format!("ok:{allowed_ip}"), "resolve-allowed");
    let held = probe(
        &mut guest,
        "connect-allowed",
        "hold",
        serde_json::json!({"id": "allowed", "addr": allowed_ip.to_string(), "port": port, "timeout": 5}),
    );
    assert_eq!(held, "connected", "connect-allowed");
    let reply = probe(
        &mut guest,
        "exchange-allowed",
        "exchange",
        serde_json::json!({"id": "allowed", "timeout": 5}),
    );
    assert_eq!(reply, "pong", "exchange-allowed");

    let unlisted = guest::call(
        &mut guest,
        "unlisted-refused",
        "query",
        serde_json::json!({"nameserver": VMM_TAP.to_string(), "name": "ok.elsewhere.example", "qtype": 1, "timeout": BLOCKED}),
    );
    assert_eq!(unlisted["status"], "rcode:5", "unlisted-refused");
    let unlisted_mx = query(
        &mut guest,
        "unlisted-mx-refused",
        "ok.elsewhere.example",
        15,
    );
    assert_eq!(unlisted_mx["status"], "rcode:5", "unlisted-mx-refused");

    for (name, qname) in [
        ("exact-subdomain-refused", "below.ok.acmeco.example"),
        ("wildcard-base-refused", "wild.acmeco.example"),
    ] {
        let refused = guest::call(
            &mut guest,
            name,
            "query",
            serde_json::json!({"nameserver": VMM_TAP.to_string(), "name": qname, "qtype": 1, "timeout": BLOCKED}),
        );
        assert_eq!(refused["status"], "rcode:5", "{name}");
    }
    for (name, qname) in [
        ("wildcard-resolved", "api.wild.acmeco.example"),
        ("wildcard-nested-resolved", "a.b.wild.acmeco.example"),
    ] {
        let resolved = probe(
            &mut guest,
            name,
            "resolve",
            serde_json::json!({"name": qname}),
        );
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
    assert_eq!(
        aaaa,
        serde_json::json!({"status": "ok", "records": []}),
        "aaaa-empty"
    );

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
        serde_json::json!({"addr": "2606:4700:4700::1111", "port": 443, "timeout": BLOCKED}),
    );
    assert_ne!(ipv6, "connected", "connect-ipv6");
    eprintln!("connect-ipv6: {ipv6}");

    // Inbound, from inside the VMM's own network namespace toward the guest.
    assert_eq!(
        probe(
            &mut guest,
            "inbound-listen",
            "listen",
            serde_json::json!({"port": 34567})
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
        serde_json::json!({"port": 34567, "wait": 1}),
    );
    assert_eq!(inbound, "no-connection", "inbound-accepted");

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
    // Forwarded one hop by the host, which masquerades the VMM's uplink.
    assert!(
        endpoint::accepted(&allowed).contains(&run.endpoint_peer),
        "the allowed endpoint never saw the host's end of its hop"
    );
    run::eventually("the wildcard endpoint's accept", READY, || {
        endpoint::accepted(&wild)
            .contains(&run.endpoint_peer)
            .then_some(())
    });
    assert_eq!(
        endpoint::accepted(&unresolved),
        Vec::<Ipv4Addr>::new(),
        "connect-unresolved arrived"
    );
    run::assert_framing(&vmm, true);
}

/// Real kernel expiry closes new flows while an established connection survives.
#[test]
fn egress_ttl() {
    let run = run::load();
    let nameserver = scripted(&run, run.endpoints[0]);
    let ip = run.endpoints[1];
    let target = listen(&run, ip);
    dns::set(
        &nameserver,
        "short.acmeco.example",
        vec![dns::Record::A(ip, 1)],
    );
    let mut spec = run::Spec::probes(&run, run::policy::TTL);
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    let answer = query(&mut guest, "authorize", "short.acmeco.example", 1);
    assert_eq!(
        answer["records"],
        serde_json::json!([["A", ip.to_string(), 30]])
    );
    assert_eq!(
        host::resolved(&vmm.name)
            .iter()
            .map(|e| (e.addr, e.timeout))
            .collect::<Vec<_>>(),
        [(ip, Some(30))]
    );
    assert_eq!(
        probe(
            &mut guest,
            "hold",
            "hold",
            serde_json::json!({
                "id": "held", "addr": ip.to_string(), "port": target.addr.port(), "timeout": 30
            })
        ),
        "connected"
    );
    assert_eq!(
        probe(
            &mut guest,
            "before-expiry",
            "exchange",
            serde_json::json!({"id": "held", "timeout": 30})
        ),
        "pong"
    );
    assert_eq!(endpoint::accepted(&target), [run.endpoint_peer]);

    run::eventually(
        "the authorization to expire from the kernel",
        Duration::from_secs(90),
        || host::resolved(&vmm.name).is_empty().then_some(()),
    );
    assert_eq!(
        tcp(&mut guest, "after-expiry", ip, target.addr.port(), BLOCKED),
        "timeout"
    );
    assert_eq!(endpoint::accepted(&target), [run.endpoint_peer]);
    assert_eq!(
        probe(
            &mut guest,
            "held-after-expiry",
            "exchange",
            serde_json::json!({"id": "held", "timeout": 30})
        ),
        "pong"
    );
    run::assert_framing(&vmm, true);
}

/// Remembered CNAME targets, and private answers refused under every policy
/// that could otherwise admit them.
#[test]
fn egress_cname() {
    let run = run::load();
    let [dns_ip, _, _, _, target_ip, spare_ip] = run.endpoints[..] else {
        panic!("expected six endpoints in run.json");
    };
    let nameserver = scripted(&run, dns_ip);
    let target = listen(&run, target_ip);
    dns::set(
        &nameserver,
        "alias.acmeco.example",
        vec![dns::Record::Cname("target.cdn.example".into(), 3600)],
    );
    dns::set(
        &nameserver,
        "target.cdn.example",
        vec![dns::Record::A(target_ip, 3600)],
    );
    dns::set(
        &nameserver,
        "below.target.cdn.example",
        vec![dns::Record::A(target_ip, 3600)],
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

    let mut spec = run::Spec::probes(&run, run::policy::CAPACITY);
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);

    let chain = query(&mut guest, "cname-chain", "alias.acmeco.example", 1);
    assert_eq!(
        chain["records"],
        serde_json::json!([
            ["CNAME", "target.cdn.example", 3600],
            ["A", target_ip.to_string(), 3600]
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

    assert!(
        !dns::queries(&nameserver)
            .iter()
            .any(|name| name == "below.target.cdn.example"),
        "the remembered target's subdomain went upstream"
    );
    run::assert_framing(&vmm, true);
}

/// The empty policy, which `egress: {}` launches with: nothing resolves and
/// nothing leaves.
#[test]
fn egress_empty() {
    let run = run::load();
    let nameserver = scripted(&run, run.endpoints[0]);
    let target = listen(&run, run.endpoints[0]);
    dns::set(
        &nameserver,
        "ok.acmeco.example",
        vec![dns::Record::A(run.endpoints[0], 300)],
    );

    let mut spec = run::Spec::probes(&run, run::policy::EMPTY);
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    let pid = run::pid(&vmm);

    let resolved = probe(
        &mut guest,
        "resolve-empty",
        "resolve",
        serde_json::json!({"name": "ok.acmeco.example"}),
    );
    assert!(
        resolved.as_str().is_some_and(|r| r.starts_with("error:")),
        "resolve-empty: {resolved}"
    );
    let raw = query(&mut guest, "query-empty", "ok.acmeco.example", 1);
    assert_eq!(raw["status"], "rcode:5", "query-empty");
    let port = target.addr.port();
    assert_eq!(
        tcp(&mut guest, "connect-empty", run.endpoints[0], port, BLOCKED),
        "timeout",
        "connect-empty"
    );
    assert_eq!(
        tcp(
            &mut guest,
            "connect-empty-metadata",
            Ipv4Addr::new(169, 254, 169, 254),
            80,
            BLOCKED
        ),
        "timeout",
        "connect-empty-metadata"
    );

    let flows = guest_flows(pid);
    assert!(
        flows
            .iter()
            .all(|flow| flow.proto == "udp" && flow.dst == VMM_TAP && flow.dport == Some(53)),
        "guest flows other than its refused queries got through: {flows:?}"
    );
    let counters = host::counters(&vmm.name);
    assert_eq!(
        counter(&counters, "postrouting/masquerade"),
        0,
        "masqueraded packets: {counters:?}"
    );
    for rule in ["forward/forward-drop", "forward/baseline", "input/tap-dns"] {
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
        "connect-empty arrived"
    );
    run::assert_framing(&vmm, true);
}

/// The one test that uses the real internet, through the real resolver path,
/// so an outage fails it alone and says so.
#[test]
fn public_https_smoke() {
    let run = run::load();
    let (vmm, mut guest) = start(&run, run::Spec::probes(&run, run::policy::SMOKE));

    let resolved = probe(
        &mut guest,
        "resolve-pypi",
        "resolve",
        serde_json::json!({"name": "pypi.org"}),
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
        serde_json::json!({
            "token": format!("acmeCo-generation-{n}"),
            "control_plane_url": "https://control.acmeco.example",
            "config_encryption_url": "https://encryption.acmeco.example",
        })
        .to_string()
    };
    let first = generation(1);
    let spec = run::Spec {
        task_update: Some(&first),
        ..run::Spec::probes(&run, run::policy::EMPTY)
    };
    let (vmm, mut guest) = start(&run, spec);
    let mount = run::GUEST_CONNECTOR_MOUNT;

    // The test guest's image sets CONNECTOR_MOUNT and LOG_LEVEL itself; the
    // contract's values must win.
    let env = probe(&mut guest, "contract-env", "env", serde_json::json!({}));
    assert_eq!(env["CONNECTOR_MOUNT"], mount, "CONNECTOR_MOUNT");
    assert_eq!(env["LOG_FORMAT"], "json", "LOG_FORMAT");
    assert_eq!(env["LOG_LEVEL"], "warn", "LOG_LEVEL");

    let mounts = probe(&mut guest, "mounts", "mounts", serde_json::json!({}));
    let line = mounts
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
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
        serde_json::json!({"path": format!("{mount}/x"), "data": "x"}),
    );
    assert_eq!(write, "error:EROFS", "mount-write");
    let list = probe(
        &mut guest,
        "mount-list",
        "list",
        serde_json::json!({"path": mount}),
    );
    assert_eq!(list, "error:EACCES", "mount-list: 0711 hides the listing");
    let inspect = probe(
        &mut guest,
        "mount-read",
        "read",
        serde_json::json!({"path": format!("{mount}/image-inspect.json")}),
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
        serde_json::json!({"argv": [format!("{mount}/flow-connector-init"), "--help"]}),
    );
    assert_eq!(exec, "exit:0", "mount-exec");

    let path = format!("{mount}/task-update.json");
    let read = probe(
        &mut guest,
        "task-update-read",
        "read",
        serde_json::json!({"path": &path}),
    );
    assert_eq!(read["content"], first.as_str(), "task-update-read");

    // Renamed in, as the runtime refreshes it: a new inode each time, which the
    // guest sees once virtiofs revalidates the name. The latency is reported,
    // not asserted beyond the probe's bound.
    for n in [2, 3] {
        let next = generation(n);
        let renamed = Instant::now();
        run::replace_task_update(&vmm.connector_mount, &next);
        let seen = probe(
            &mut guest,
            &format!("task-update-generation-{n}"),
            "await",
            serde_json::json!({"path": &path, "content": &next, "timeout": 15}),
        );
        assert_eq!(
            seen,
            serde_json::json!({"seen": true}),
            "generation {n} never reached the guest"
        );
        eprintln!(
            "task-update.json generation {n} visible after {:?}",
            renamed.elapsed()
        );
    }
    run::assert_framing(&vmm, true);
}

/// What a compromised VMM needs to put its guest back on the network once it
/// has flushed its own rules: a masquerade, and nothing that restricts.
const ATTACKER_NAT: &str = "table ip attacker {
\tchain postrouting {
\t\ttype nat hook postrouting priority srcnat; policy accept;
\t\toifname \"eth0\" masquerade
\t}
}
";

fn flush(vmm: &run::Vmm) {
    run::podman(&["exec", &vmm.name, "nft", "flush", "ruleset"]);
    assert_eq!(
        run::podman(&["exec", &vmm.name, "nft", "list", "ruleset"]).trim(),
        "",
        "{} kept rules after a flush",
        vmm.name
    );
}

fn attacker_nat(vmm: &run::Vmm) {
    let output = run::sudo_output(
        &["podman", "exec", "-i", &vmm.name, "nft", "-f", "-"],
        Some(ATTACKER_NAT.as_bytes()),
    );
    assert!(
        output.status.success(),
        "installing the attacker's NAT: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The verifier refuses every change to the tables, and install and remove
/// keep their promises about VMM bridges. In a scratch network namespace, so
/// the host's own tables are never weakened, even for a moment.
#[test]
fn boundary_detects_tampering() {
    let run = run::load();
    let scratch = run::netns();
    let network = format!("ns:/run/netns/{}", scratch.name);
    let boundary = |action: &str| run::boundary(&run, &network, action);
    let nft = |command: &str| {
        let mut argv = vec![
            "podman".to_string(),
            "run".to_string(),
            "--rm".to_string(),
            format!("--network={network}"),
            "--cap-drop=all".to_string(),
            "--cap-add=CAP_NET_ADMIN".to_string(),
            "--entrypoint=nft".to_string(),
            run.vmm_image.clone(),
        ];
        argv.extend(command.split_whitespace().map(str::to_string));
        run::sudo(&argv.iter().map(String::as_str).collect::<Vec<_>>());
    };
    let ok = |output: std::process::Output, what: &str| {
        assert!(
            output.status.success(),
            "{what}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    let refused = |output: std::process::Output, what: &str, says: &str| {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "{what}: {stderr}");
        assert!(
            stderr.contains(says),
            "{what} does not say {says:?}: {stderr}"
        );
        for line in stderr.lines() {
            assert!(
                line.starts_with("flow-connector-vmm:"),
                "{what}: unframed {line:?}"
            );
        }
    };

    refused(
        boundary("verify"),
        "verify before install",
        "table is absent",
    );
    ok(boundary("install"), "install");
    ok(boundary("install"), "a second install");
    ok(boundary("verify"), "verify");

    for (edit, says) in [
        (
            "insert rule inet flow_vmm_boundary forward accept",
            "inet forward, rule 0",
        ),
        (
            "flush chain inet flow_vmm_boundary input",
            "inet input, rule 0",
        ),
        (
            "add table inet flow_vmm_boundary { flags dormant ; }",
            "dormant",
        ),
        (
            "delete element inet flow_vmm_boundary baseline { 169.254.0.0/16 }",
            "unexpected: {\"set\"",
        ),
        (
            "chain inet flow_vmm_boundary input { policy drop ; }",
            "\"policy\":\"drop\"",
        ),
        (
            "delete table bridge flow_vmm_boundary",
            "the bridge flow_vmm_boundary table is absent",
        ),
    ] {
        nft(edit);
        refused(boundary("verify"), edit, says);
        ok(boundary("install"), &format!("install after {edit}"));
        ok(
            boundary("verify"),
            &format!("verify after reinstalling over {edit}"),
        );
    }

    // A VMM bridge in this namespace: removal is refused and changes nothing.
    let link = format!("fvm{}", &run::random_hex()[..12]);
    run::sudo(&[
        "ip",
        "-n",
        &scratch.name,
        "link",
        "add",
        &link,
        "type",
        "dummy",
    ]);
    refused(boundary("remove"), "remove beside a VMM bridge", &link);
    ok(boundary("verify"), "verify after a refused remove");

    // The table vanished under that bridge: install restores it, and says so.
    nft("delete table inet flow_vmm_boundary");
    refused(boundary("install"), "install over a gap", &link);
    ok(boundary("verify"), "verify after installing over a gap");

    run::sudo(&["ip", "-n", &scratch.name, "link", "del", &link]);
    ok(boundary("remove"), "remove");
    ok(boundary("remove"), "a second remove");
    refused(boundary("verify"), "verify after remove", "table is absent");
}

/// Replacing the host's tables, as a restart or an upgrade does, never opens
/// them: a held flow keeps flowing and a blocked destination stays blocked
/// throughout. The replacements are of identical content, in one transaction
/// each, which is what makes them safe to run beside the rest of the suite.
#[test]
fn boundary_holds_across_reinstalls() {
    let run = run::load();
    let endpoint_ip = run.endpoints[0];
    let public = listen(&run, endpoint_ip);
    let decoy = listen(&run, run.decoys[0]);
    let (vmm, mut guest) = start(&run, run::Spec::probes(&run, run::policy::EMPTY));
    flush(&vmm);
    attacker_nat(&vmm);

    let held = probe(
        &mut guest,
        "hold",
        "hold",
        serde_json::json!({"id": "held", "addr": endpoint_ip.to_string(), "port": public.addr.port(), "timeout": 5}),
    );
    assert_eq!(held, "connected");

    let mut rounds = 0;
    std::thread::scope(|scope| {
        let reinstalls = scope.spawn(|| {
            (0..8)
                .map(|_| run::boundary(&run, "host", "install"))
                .collect::<Vec<_>>()
        });
        while !reinstalls.is_finished() {
            rounds += 1;
            let reply = probe(
                &mut guest,
                "exchange",
                "exchange",
                serde_json::json!({"id": "held", "timeout": 2}),
            );
            assert_eq!(reply, "pong", "the held flow stopped during a reinstall");
            assert_eq!(
                tcp(&mut guest, "decoy", run.decoys[0], decoy.addr.port(), 0.5),
                "timeout",
                "the decoy was reached during a reinstall"
            );
        }
        for output in reinstalls.join().expect("the reinstall thread") {
            assert!(
                output.status.success(),
                "a reinstall failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    });
    eprintln!("{rounds} rounds of probes across 8 reinstalls");

    // netavark's own rules, reapplied, leave the tables as they were.
    run::podman(&["network", "reload", &vmm.name]);
    let verified = run::boundary(&run, "host", "verify");
    assert!(
        verified.status.success(),
        "verify after a network reload: {}",
        String::from_utf8_lossy(&verified.stderr)
    );
    let reply = probe(
        &mut guest,
        "exchange-after-reload",
        "exchange",
        serde_json::json!({"id": "held", "timeout": 2}),
    );
    assert_eq!(reply, "pong");
    assert_eq!(
        tcp(
            &mut guest,
            "decoy-after-reload",
            run.decoys[0],
            decoy.addr.port(),
            BLOCKED
        ),
        "timeout"
    );
    assert_eq!(
        endpoint::accepted(&decoy),
        Vec::<Ipv4Addr>::new(),
        "the decoy was reached"
    );
}

#[test]
fn ingress_from_host() {
    use std::io::{Read, Write};

    let run = run::load();
    let (sibling, mut sibling_guest) = start(
        &run,
        run::Spec {
            flags: vec!["--ingress-port=8080".to_string()],
            ..run::Spec::probes(&run, run::policy::EMPTY)
        },
    );
    let _sibling_permissions = run::ingress(&run, &sibling, &[8080], &[]);
    for policy in [run::policy::EMPTY, run::policy::ALLOWLIST] {
        let (vmm, mut guest) = start(
            &run,
            run::Spec {
                flags: vec!["--ingress-port=8080".to_string()],
                ..run::Spec::probes(&run, policy)
            },
        );
        let permissions = run::ingress(&run, &vmm, &[8080], &[]);
        let ip = network_field(&vmm, "IPAddress");
        assert_eq!(
            probe(
                &mut guest,
                "undeclared-listen",
                "listen",
                serde_json::json!({"port": 8081})
            ),
            "listening"
        );

        for compromised in [false, true] {
            if compromised {
                flush(&vmm);
                // An escaped VMM can forward every port, but cannot authorize
                // another port or a remote caller in the host boundary.
                let rules = format!(
                    "table ip attacker {{\n\
                     chain prerouting {{\n\
                     type nat hook prerouting priority dstnat; policy accept;\n\
                     iifname eth0 tcp dport {{ 8080, 8081 }} dnat to {GUEST}\n\
                     }}\n\
                     chain postrouting {{\n\
                     type filter hook postrouting priority mangle; policy accept;\n\
                     meta mark set 0x46564d49\n\
                     }}\n\
                     chain guest_nat {{\n\
                     type nat hook postrouting priority srcnat; policy accept;\n\
                     oifname eth0 masquerade\n\
                     }}\n}}\n"
                );
                let output = run::sudo_output(
                    &["podman", "exec", "-i", &vmm.name, "nft", "-f", "-"],
                    Some(rules.as_bytes()),
                );
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            assert_eq!(
                probe(
                    &mut guest,
                    "declared-listen",
                    "listen",
                    serde_json::json!({"port": 8080})
                ),
                "listening"
            );
            assert!(
                std::net::TcpStream::connect_timeout(
                    &(ip, 8081).into(),
                    Duration::from_millis(500)
                )
                .is_err(),
                "undeclared port admitted ({policy:?}, compromised={compromised})"
            );
            let remote = format!(
                "import socket\ns=socket.socket(); s.settimeout(0.5)\ntry:\n s.connect(('{ip}', 8080)); print('connected')\nexcept OSError:\n print('blocked')"
            );
            assert_eq!(
                run::sudo(&[
                    "ip",
                    "netns",
                    "exec",
                    &run.endpoint_netns,
                    "python3",
                    "-c",
                    &remote
                ])
                .trim(),
                "blocked"
            );

            assert_eq!(
                tcp(
                    &mut sibling_guest,
                    "sibling-cannot-open-ingress",
                    ip,
                    8080,
                    BLOCKED
                ),
                "timeout"
            );

            let mut stream =
                std::net::TcpStream::connect_timeout(&(ip, 8080).into(), Duration::from_secs(5))
                    .expect("host reaches the declared port");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            stream.write_all(b"ingest documents").unwrap();
            stream.shutdown(std::net::Shutdown::Write).unwrap();
            let accepted = probe(
                &mut guest,
                "ingress-reply",
                "accepted",
                serde_json::json!({"port": 8080, "wait": 5, "exchange": true}),
            );
            assert!(accepted.as_str().unwrap().starts_with("connection-from:"));
            let mut reply = String::new();
            stream.read_to_string(&mut reply).unwrap();
            assert_eq!(
                reply, "reply:ingest documents",
                "{policy:?}, compromised={compromised}"
            );

            let gateway = network_field(&vmm, "Gateway");
            assert_eq!(
                tcp(
                    &mut guest,
                    "guest-cannot-open-host-connection",
                    gateway,
                    8080,
                    BLOCKED
                ),
                "timeout"
            );
        }
        assert_eq!(
            probe(
                &mut guest,
                "listen-before-revoke",
                "listen",
                serde_json::json!({"port": 8080})
            ),
            "listening"
        );
        let mut held =
            std::net::TcpStream::connect_timeout(&(ip, 8080).into(), Duration::from_secs(5))
                .unwrap();
        held.set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        held.write_all(b"before revoke").unwrap();
        held.shutdown(std::net::Shutdown::Write).unwrap();
        drop(permissions);
        let accepted = probe(
            &mut guest,
            "reply-after-revoke",
            "accepted",
            serde_json::json!({"port": 8080, "wait": 5, "exchange": true}),
        );
        assert!(accepted.as_str().unwrap().starts_with("connection-from:"));
        assert!(
            held.read(&mut [0u8; 64]).is_err(),
            "stale connection retained ingress permission"
        );
        assert_eq!(
            probe(
                &mut guest,
                "listen-after-revoke",
                "listen",
                serde_json::json!({"port": 8080})
            ),
            "listening"
        );
        assert!(
            std::net::TcpStream::connect_timeout(&(ip, 8080).into(), Duration::from_millis(500))
                .is_err(),
            "revoked ingress admitted a connection"
        );
        assert_eq!(
            probe(
                &mut sibling_guest,
                "sibling-still-authorized",
                "listen",
                serde_json::json!({"port": 8080})
            ),
            "listening"
        );
        let sibling_ip = network_field(&sibling, "IPAddress");
        let stream = std::net::TcpStream::connect_timeout(
            &(sibling_ip, 8080).into(),
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(
            probe(
                &mut sibling_guest,
                "sibling-accepted",
                "accepted",
                serde_json::json!({"port": 8080, "wait": 5})
            )
            .as_str()
            .unwrap()
            .starts_with("connection-from:")
        );
        drop(stream);
    }
}

/// A declared `/32` reaches its private address on TCP and UDP, directly and
/// by an unlisted name, and nothing beside it. The host boundary holds the
/// same line for a VMM that has removed its own ruleset, and only while the
/// launch's grant stands: a sibling cannot use it, and revoking it ends a held
/// connection.
#[test]
fn egress_cidrs() {
    let run = run::load();
    let [granted, beside, ..] = run.decoys[..] else {
        panic!("too few decoys in run.json");
    };
    assert_eq!(
        granted,
        Ipv4Addr::new(198, 18, 255, 241),
        "policies/cidrs.json declares the first decoy"
    );
    let nameserver = scripted(&run, run.endpoints[0]);
    for (name, ip) in [
        ("db.corp.example", granted),
        ("beside.corp.example", beside),
        ("ok.acmeco.example", beside),
    ] {
        dns::set(&nameserver, name, vec![dns::Record::A(ip, 300)]);
    }
    let tcp_granted = listen(&run, granted);
    let tcp_beside = listen(&run, beside);
    let udp_granted = echo(&run, granted);
    let udp_beside = echo(&run, beside);

    let mut spec = run::Spec::probes(&run, run::policy::CIDRS);
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    let permissions = run::ingress(&run, &vmm, &[], &["198.18.255.241/32"]);
    let (tcp_port, udp_port) = (tcp_granted.addr.port(), udp_granted.addr.port());

    let resolved = probe(
        &mut guest,
        "resolve-unlisted-within",
        "resolve",
        serde_json::json!({"name": "db.corp.example"}),
    );
    assert_eq!(resolved, format!("ok:{granted}"), "resolve-unlisted-within");
    for (name, qname) in [
        ("unlisted-beside-refused", "beside.corp.example"),
        ("listed-beside-refused", "ok.acmeco.example"),
    ] {
        assert_eq!(
            query(&mut guest, name, qname, 1)["status"],
            "rcode:5",
            "{name}"
        );
    }

    for compromised in [false, true] {
        if compromised {
            flush(&vmm);
            attacker_nat(&vmm);
        }
        let case = |name: &str| format!("{name} (compromised={compromised})");
        for (name, result, want) in [
            (
                "tcp-granted",
                tcp(&mut guest, &case("tcp-granted"), granted, tcp_port, 5.0),
                "connected",
            ),
            (
                "udp-granted",
                udp(&mut guest, &case("udp-granted"), granted, udp_port, 5.0),
                "pong",
            ),
            (
                "tcp-beside",
                tcp(
                    &mut guest,
                    &case("tcp-beside"),
                    beside,
                    tcp_beside.addr.port(),
                    BLOCKED,
                ),
                "timeout",
            ),
            (
                "udp-beside",
                udp(
                    &mut guest,
                    &case("udp-beside"),
                    beside,
                    udp_beside.addr.port(),
                    BLOCKED,
                ),
                "timeout",
            ),
            (
                "smtp-granted",
                tcp(&mut guest, &case("smtp-granted"), granted, 25, BLOCKED),
                "timeout",
            ),
        ] {
            assert_eq!(result, want, "{}", case(name));
        }
    }

    // A sibling with no grant of its own, as compromised as this VMM.
    let (sibling, mut sibling_guest) = start(&run, run::Spec::probes(&run, run::policy::CIDRS));
    flush(&sibling);
    attacker_nat(&sibling);
    assert_eq!(
        tcp(
            &mut sibling_guest,
            "sibling-tcp-granted",
            granted,
            tcp_port,
            BLOCKED
        ),
        "timeout",
        "a sibling used another launch's grant"
    );
    assert_eq!(
        udp(
            &mut sibling_guest,
            "sibling-udp-granted",
            granted,
            udp_port,
            BLOCKED
        ),
        "timeout",
        "a sibling used another launch's grant"
    );

    let held = probe(
        &mut guest,
        "hold-granted",
        "hold",
        serde_json::json!({"id": "granted", "addr": granted.to_string(), "port": tcp_port, "timeout": 5}),
    );
    assert_eq!(held, "connected");
    let exchange = |guest: &mut guest::Guest, name: &str, timeout: f64| {
        probe(
            guest,
            name,
            "exchange",
            serde_json::json!({"id": "granted", "timeout": timeout}),
        )
    };
    assert_eq!(exchange(&mut guest, "exchange-before-revoke", 5.0), "pong");

    drop(permissions);
    assert_eq!(
        exchange(&mut guest, "exchange-after-revoke", BLOCKED),
        "timeout",
        "a held connection outlived its grant"
    );
    assert_eq!(
        tcp(&mut guest, "tcp-after-revoke", granted, tcp_port, BLOCKED),
        "timeout"
    );
    assert_eq!(
        udp(&mut guest, "udp-after-revoke", granted, udp_port, BLOCKED),
        "timeout"
    );

    for endpoint in [&tcp_granted, &udp_granted] {
        assert!(
            endpoint::accepted(endpoint).contains(&run.endpoint_peer),
            "the granted decoy never saw the host's end of its hop"
        );
    }
    for endpoint in [&tcp_beside, &udp_beside] {
        assert_eq!(
            endpoint::accepted(endpoint),
            Vec::<Ipv4Addr>::new(),
            "the decoy beside the grant was reached"
        );
    }
    let upstream = dns::queries(&nameserver);
    assert!(
        upstream.iter().any(|name| name == "beside.corp.example"),
        "an unlisted name goes upstream to be judged by its answer: {upstream:?}"
    );
}

/// `0.0.0.0/0` reaches every public and private address, and still nothing
/// excluded: not the metadata service, the host, the VMM itself or another
/// VMM. The host boundary holds those for a VMM that has removed its own
/// ruleset, though the launch's grant covers every address.
#[test]
fn egress_every_address() {
    let run = run::load();
    let public = run.endpoints[1];
    let decoy = run.decoys[0];
    let metadata = Ipv4Addr::new(169, 254, 169, 254);
    let nameserver = scripted(&run, run.endpoints[0]);
    for (name, ip) in [
        ("public.corp.example", public),
        ("private.corp.example", decoy),
        ("metadata.corp.example", metadata),
    ] {
        dns::set(&nameserver, name, vec![dns::Record::A(ip, 300)]);
    }
    let tcp_public = listen(&run, public);
    let tcp_decoy = listen(&run, decoy);
    let udp_decoy = echo(&run, decoy);
    let (sibling, _sibling_guest) = start(&run, run::Spec::probes(&run, run::policy::EMPTY));
    let sibling_ip = network_field(&sibling, "IPAddress");

    let mut spec = run::Spec::probes(&run, run::policy::EVERY_ADDRESS);
    spec.flags = upstream(&nameserver);
    let (vmm, mut guest) = start(&run, spec);
    let _permissions = run::ingress(&run, &vmm, &[], &["0.0.0.0/0"]);
    let gateway = network_field(&vmm, "Gateway");
    let uplink = network_field(&vmm, "IPAddress");

    for (name, qname, want) in [
        (
            "resolve-public",
            "public.corp.example",
            format!("ok:{public}"),
        ),
        (
            "resolve-private",
            "private.corp.example",
            format!("ok:{decoy}"),
        ),
    ] {
        let resolved = probe(
            &mut guest,
            name,
            "resolve",
            serde_json::json!({"name": qname}),
        );
        assert_eq!(resolved, want, "{name}");
    }
    assert_eq!(
        query(&mut guest, "metadata-refused", "metadata.corp.example", 1)["status"],
        "rcode:5"
    );
    // The VMM's own addresses are its input chain's, which a flush removes.
    for (name, addr) in [("vmm-uplink", uplink), ("vmm-tap", VMM_TAP)] {
        assert_eq!(
            tcp(&mut guest, name, addr, 443, BLOCKED),
            "timeout",
            "{name}"
        );
    }

    for compromised in [false, true] {
        if compromised {
            flush(&vmm);
            attacker_nat(&vmm);
        }
        let case = |name: &str| format!("{name} (compromised={compromised})");
        for (name, result, want) in [
            (
                "tcp-public",
                tcp(
                    &mut guest,
                    &case("tcp-public"),
                    public,
                    tcp_public.addr.port(),
                    5.0,
                ),
                "connected",
            ),
            (
                "tcp-private",
                tcp(
                    &mut guest,
                    &case("tcp-private"),
                    decoy,
                    tcp_decoy.addr.port(),
                    5.0,
                ),
                "connected",
            ),
            (
                "udp-private",
                udp(
                    &mut guest,
                    &case("udp-private"),
                    decoy,
                    udp_decoy.addr.port(),
                    5.0,
                ),
                "pong",
            ),
            (
                "tcp-metadata",
                tcp(&mut guest, &case("tcp-metadata"), metadata, 80, BLOCKED),
                "timeout",
            ),
            (
                "tcp-gateway",
                tcp(&mut guest, &case("tcp-gateway"), gateway, 443, BLOCKED),
                "timeout",
            ),
            (
                "tcp-sibling",
                tcp(&mut guest, &case("tcp-sibling"), sibling_ip, 8080, BLOCKED),
                "timeout",
            ),
            (
                "smtp-private",
                tcp(&mut guest, &case("smtp-private"), decoy, 25, BLOCKED),
                "timeout",
            ),
        ] {
            assert_eq!(result, want, "{}", case(name));
        }
    }
    for endpoint in [&tcp_public, &tcp_decoy, &udp_decoy] {
        assert!(
            endpoint::accepted(endpoint).contains(&run.endpoint_peer),
            "an endpoint never saw the host's end of its hop"
        );
    }
}
