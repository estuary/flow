//! The plan of one VMM launch: the policy its guest is held to, the state it
//! is given, and the podman lines that verify the host boundary, create its
//! network and create its container. Planning is pure. It creates nothing and
//! probes nothing, so whether this host can carry the plan out is for the
//! launch.
//!
//! `crates/connector-vmm-tests` holds the reference lines the plan reproduces;
//! the README records where and why it departs from them.

use super::{Eligible, Vmm};
use anyhow::Context;

/// Must match `boundary::BRIDGE_PREFIX` of `flow-connector-vmm`: a bridge so
/// named is behind the host boundary, and nothing else puts it there.
const BRIDGE_PREFIX: &str = "fvm";

/// Image label declaring the hosts a connector may reach beyond those it is
/// eligible with: a JSON array of host names, each bare or `*.`-prefixed.
const EGRESS_HOSTS_LABEL: &str = "dev.estuary.egress-hosts";

/// `sun_path` holds 108 bytes, its terminating NUL among them.
const SOCKET_PATH_MAX: usize = 107;

/// What a launch's guest may reach.
#[derive(Debug, PartialEq)]
pub(crate) enum Egress {
    /// Only these names, each with where it was first declared.
    Names(Vec<(egress::AllowedName, Source)>),
    /// Any public destination: the task declares no egress, and its data
    /// plane does not require one.
    AnyPublic,
}

/// Where a permitted name was declared.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Source {
    Connector,
    /// The image's `dev.estuary.egress-hosts` label.
    Image,
    Task,
}

impl std::fmt::Display for Egress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names = match self {
            Egress::AnyPublic => {
                return f.write_str(
                    "VMM egress permits any public destination: the task declares no \
                     egress, and this data plane does not require one",
                );
            }
            Egress::Names(names) if names.is_empty() => {
                return f.write_str("VMM egress permits no host names");
            }
            Egress::Names(names) => names,
        };
        f.write_str("VMM egress permits only these host names: ")?;

        let groups = [
            (Source::Connector, "connector defaults"),
            (Source::Image, "image label"),
            (Source::Task, "task egress.hosts"),
        ];
        let mut first = true;
        for (source, what) in groups {
            let group: Vec<String> = names
                .iter()
                .filter(|(_, from)| *from == source)
                .map(|(name, _)| name.to_string())
                .collect();
            if group.is_empty() {
                continue;
            }
            if !first {
                f.write_str("; ")?;
            }
            first = false;
            write!(f, "{} ({what})", group.join(", "))?;
        }
        Ok(())
    }
}

/// One VMM launch of an admitted connector.
pub(crate) struct Launch<'a> {
    /// Names the container, its network, its state and its record:
    /// `fv_<16 hex>`.
    pub id: u64,
    /// Marks the network and container as the launch's own.
    pub token: u128,
    pub eligible: &'static Eligible,
    pub image: &'a str,
    /// `podman image inspect` of `image`, which the connector mount also
    /// carries as `image-inspect.json`.
    pub inspection: &'a [u8],
    /// Absolute host directory of connector mounts, in which the launch's
    /// own is `mount-fv_<16 hex>`.
    pub connector_mounts: &'a str,
    pub log_level: ops::LogLevel,
    pub plane: crate::Plane,
    pub task_name: &'a str,
    pub persistent_disk: Option<PersistentDisk<'a>>,
    /// The hosts the task's egress declares, or None if it declares none.
    pub egress: Option<&'a [egress::AllowedName]>,
}

/// The task's persistent storage: a host directory, and where the guest
/// mounts it.
pub(crate) struct PersistentDisk<'a> {
    pub host_dir: &'a str,
    pub guest_path: &'a str,
}

#[derive(Debug)]
pub(crate) struct Plan {
    /// The container's name, and its network's.
    pub name: String,
    /// The network's bridge, `fvm` and twelve of the id's digits: an
    /// interface name holds fifteen bytes.
    pub interface: String,
    /// The launch's own state directory, `fv_<16 hex>` beneath the
    /// configured one.
    pub state: String,
    /// The launch's ownership record, beside its state directory.
    pub record: String,
    /// The value of the owner label on the launch's network and container.
    pub token: String,
    /// The connector mount, shared at that same path into the container and
    /// the guest.
    pub mount: String,
    /// The state directory, then the three the container binds, each with
    /// its mode. `sock/` is traversable so that an unprivileged process can
    /// dial the socket, which the VMM leaves at mode 0777.
    pub directories: Vec<(String, u32)>,
    /// Where the VMM opens its `O_TMPFILE` scratch disk.
    pub scratch: String,
    /// What the guest may reach, from which `policy` is written.
    pub egress: Egress,
    /// Written to `policy_path`, the VMM's `/init/policy.json`.
    pub policy: Vec<u8>,
    pub policy_path: String,
    /// The connector-init socket the launch dials.
    pub socket: String,
    /// What the connector mount must hold, with modes.
    pub mount_files: Vec<(String, u32)>,
    /// Arguments after `podman`, in the order they run. The container is
    /// started, once created, by its ID.
    pub verify: Vec<String>,
    pub network: Vec<String>,
    pub create: Vec<String>,
}

pub(crate) fn plan(vmm: &Vmm, launch: &Launch) -> anyhow::Result<Plan> {
    let Launch {
        id,
        token,
        eligible,
        image,
        inspection,
        connector_mounts,
        log_level,
        plane,
        task_name,
        persistent_disk,
        egress,
    } = launch;

    let image_inspection = connector_init::inspect::Image::parse_from_json_slice(inspection)?;
    let inspected = crate::image::Declarations::parse(&image_inspection)?;
    if !matches!(
        (inspected.runtime_protocol, eligible.task_type),
        (crate::RuntimeProtocol::Capture, ops::TaskType::Capture)
            | (crate::RuntimeProtocol::Derive, ops::TaskType::Derivation)
            | (
                crate::RuntimeProtocol::Materialize,
                ops::TaskType::Materialization
            )
    ) {
        anyhow::bail!(
            "connector protocol {:?} does not match requested type {:?}",
            inspected.runtime_protocol,
            eligible.task_type
        );
    }

    // An ordinary connector's ports are reached at its container's address;
    // a VMM is reached only through its socket, so they would silently vanish.
    if !inspected.network_ports.is_empty() {
        let ports: Vec<String> = inspected
            .network_ports
            .iter()
            .map(|port| port.number.to_string())
            .collect();
        anyhow::bail!(
            "connector image '{image}' exposes network ports {}, which VMM execution cannot serve",
            ports.join(", ")
        );
    }

    let labels = connector_init::inspect::Image::parse_from_json_slice(inspection)
        .context("parsing image inspection")?
        .config
        .labels;
    let image_hosts = label_hosts(labels.get(EGRESS_HOSTS_LABEL).map(String::as_str))
        .with_context(|| format!("connector image '{image}'"))?;
    let egress = plan_egress(eligible, &image_hosts, *egress, *plane);

    let name = format!("fv_{id:016x}");
    let mount = format!("{connector_mounts}/mount-{name}");

    check_mount_value(image).with_context(|| format!("connector image {image:?}"))?;
    check_share(&mount).with_context(|| format!("connector mount {mount:?}"))?;
    if let Some(PersistentDisk {
        host_dir,
        guest_path,
    }) = persistent_disk
    {
        check_share(host_dir).with_context(|| format!("persistent disk {host_dir:?}"))?;
        // A flag of the VMM's, not a mount option.
        check_absolute(guest_path)
            .with_context(|| format!("persistent disk guest path {guest_path:?}"))?;
    }

    let interface = format!("{BRIDGE_PREFIX}{}", &name[3..15]);
    let state = format!("{}/{name}", vmm.state_dir);
    let socket = format!("{state}/sock/init.sock");
    let token = format!("{token:032x}");
    let owner = format!("--label={}={token}", super::record::OWNER_LABEL);
    assert!(
        socket.len() <= SOCKET_PATH_MAX,
        "check_state_dir bounds every socket path"
    );

    let log_level = log_level.or(match plane {
        crate::Plane::Local => ops::LogLevel::Info,
        crate::Plane::Public | crate::Plane::Private => ops::LogLevel::Warn,
    });

    let mut create: Vec<String> = vec![
        "create".to_string(),
        "--rm".to_string(),
        format!("--name={name}"),
        format!("--network={name}"),
        "--log-driver=none".to_string(),
        "--read-only".to_string(),
        // Without it podman mounts writable tmpfs at /tmp, /run and /var/tmp.
        "--read-only-tmpfs=false".to_string(),
        "--device=/dev/kvm".to_string(),
        "--device=/dev/net/tun".to_string(),
        "--cap-add=CAP_NET_ADMIN".to_string(),
        "--sysctl=net.ipv4.ip_forward=1".to_string(),
        "--sysctl=net.ipv4.conf.default.rp_filter=1".to_string(),
        // /proc/sys is read-only in the container, so the tap can only
        // inherit disabled IPv6 from here.
        "--sysctl=net.ipv6.conf.default.disable_ipv6=1".to_string(),
        format!("--memory={}", vmm.memory_limit),
        format!("--cpus={}", vmm.cpu_limit),
        format!("--mount=type=image,source={image},destination=/rootfs,rw=true"),
        format!("--mount=type=bind,source={state}/init,target=/init,readonly"),
        format!("--mount=type=bind,source={mount},target={mount},readonly"),
        format!("--mount=type=bind,source={state}/sock,target=/sock"),
        format!("--mount=type=bind,source={state}/scratch,target=/scratch-backing"),
    ];
    if let Some(PersistentDisk { host_dir, .. }) = persistent_disk {
        create.push(format!(
            "--mount=type=bind,source={host_dir},target=/persistent-disk"
        ));
    }
    create.extend([
        format!("--env=CONNECTOR_MOUNT={mount}"),
        "--env=LOG_FORMAT=json".to_string(),
        format!("--env=LOG_LEVEL={}", log_level.as_str_name()),
        format!("--platform={}", crate::container::CONNECTOR_PLATFORM),
        format!("--label=image={image}"),
        format!("--label=task-name={task_name}"),
        format!("--label=task-type={}", eligible.task_type.as_str_name()),
        owner.clone(),
    ]);
    if let Some(cgroup_parent) = &vmm.cgroup_parent {
        create.push(format!("--cgroup-parent={cgroup_parent}"));
    }
    create.extend([
        vmm.image.clone(),
        "run".to_string(),
        "--policy".to_string(),
        "/init/policy.json".to_string(),
        "--connector-mount".to_string(),
        mount.clone(),
        "--memory-mib".to_string(),
        vmm.guest_memory_mib.to_string(),
        "--vcpus".to_string(),
        vmm.vcpus.to_string(),
        "--disk-mib".to_string(),
        vmm.disk_mib.to_string(),
    ]);
    if let Some(PersistentDisk { guest_path, .. }) = persistent_disk {
        create.push("--persistent-disk".to_string());
        create.push(guest_path.to_string());
    }

    Ok(Plan {
        directories: vec![
            (state.clone(), 0o711),
            (format!("{state}/init"), 0o700),
            (format!("{state}/sock"), 0o711),
            (format!("{state}/scratch"), 0o700),
        ],
        scratch: format!("{state}/scratch"),
        record: super::record::path(&vmm.state_dir, &name),
        token,
        policy: match &egress {
            Egress::Names(names) => {
                let names: Vec<egress::AllowedName> =
                    names.iter().map(|(name, _)| name.clone()).collect();
                egress::public_policy(&names)
            }
            Egress::AnyPublic => egress::any_public_policy(),
        },
        egress,
        policy_path: format!("{state}/init/policy.json"),
        socket,
        mount_files: vec![
            (format!("{mount}/flow-connector-init"), 0o555),
            (format!("{mount}/image-inspect.json"), 0o444),
        ],
        verify: vec![
            "run".to_string(),
            "--rm".to_string(),
            "--network=host".to_string(),
            "--log-driver=none".to_string(),
            "--read-only".to_string(),
            "--cap-drop=all".to_string(),
            "--cap-add=CAP_NET_ADMIN".to_string(),
            vmm.image.clone(),
            "boundary".to_string(),
            "verify".to_string(),
        ],
        network: vec![
            "network".to_string(),
            "create".to_string(),
            "--driver=bridge".to_string(),
            format!("--interface-name={interface}"),
            owner,
            name.clone(),
        ],
        create,
        name,
        interface,
        state,
        mount,
    })
}

/// A state directory every launch's socket path fits beneath.
pub(super) fn check_state_dir(state_dir: &str) -> anyhow::Result<()> {
    check_share(state_dir)?;

    if state_dir.ends_with('/') {
        anyhow::bail!("ends in a slash; write it without one");
    }
    let socket = format!("{state_dir}/fv_{:016x}/sock/init.sock", 0);
    if socket.len() > SOCKET_PATH_MAX {
        anyhow::bail!(
            "is {} bytes, so a VMM's socket path would be {} bytes; \
             a Unix socket path holds at most {SOCKET_PATH_MAX}",
            state_dir.len(),
            socket.len(),
        );
    }
    Ok(())
}

/// What the guest may reach. A task which declares egress, or any task on a
/// public data plane, is held to the hosts of `eligible`, the image's label
/// and the task, in that order and without repeats. Otherwise the data plane
/// leaves its egress open to any public destination.
fn plan_egress(
    eligible: &Eligible,
    image_hosts: &[egress::AllowedName],
    task_hosts: Option<&[egress::AllowedName]>,
    plane: crate::Plane,
) -> Egress {
    // Exhaustive, so that a new kind of plane must decide.
    let undeclared_is_open = match plane {
        crate::Plane::Public => false,
        crate::Plane::Private | crate::Plane::Local => true,
    };
    if task_hosts.is_none() && undeclared_is_open {
        return Egress::AnyPublic;
    }
    let defaults: Vec<String> = eligible
        .egress_hosts
        .iter()
        .map(ToString::to_string)
        .collect();
    let defaults =
        egress::hosts("default hosts", &defaults).expect("eligible default hosts are valid");

    let mut names: Vec<(egress::AllowedName, Source)> = Vec::new();
    for (hosts, source) in [
        (defaults.as_slice(), Source::Connector),
        (image_hosts, Source::Image),
        (task_hosts.unwrap_or_default(), Source::Task),
    ] {
        for host in hosts {
            if !names.iter().any(|(name, _)| name == host) {
                names.push((host.clone(), source));
            }
        }
    }
    Egress::Names(names)
}

/// A missing, blank or empty label declares nothing.
fn label_hosts(label: Option<&str>) -> anyhow::Result<Vec<egress::AllowedName>> {
    let Some(label) = label.map(str::trim).filter(|label| !label.is_empty()) else {
        return Ok(Vec::new());
    };
    let hosts: Vec<String> = serde_json::from_str(label).with_context(|| {
        format!("image label '{EGRESS_HOSTS_LABEL}' must be a JSON array of host names: {label:?}")
    })?;

    egress::hosts(&format!("image label '{EGRESS_HOSTS_LABEL}'"), &hosts)
}

/// A host path bound into the container, and on into the guest.
fn check_share(path: &str) -> anyhow::Result<()> {
    check_absolute(path)?;
    check_mount_value(path)
}

/// The root is refused because the share would mount over it.
fn check_absolute(path: &str) -> anyhow::Result<()> {
    if !path.starts_with('/') {
        anyhow::bail!("is not an absolute path");
    }
    if path.trim_end_matches('/').is_empty() {
        anyhow::bail!("is the root");
    }
    Ok(())
}

/// podman reads `--mount` as comma-separated options, so a comma in a value
/// would end it and begin an option of the value's choosing.
fn check_mount_value(value: &str) -> anyhow::Result<()> {
    if value.contains(',') {
        anyhow::bail!("contains a comma, which podman's --mount cannot carry");
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::{Launch, PersistentDisk, Plan};
    use crate::vmm::Vmm;
    use serde_json::json;

    const IMAGE: &str = "ghcr.io/estuary/derive-python:stable";
    const MOUNTS: &str = "/tmp/connector-mounts-0";
    const ID: u64 = 0x0123456789abcdef;
    const TOKEN: u128 = 0x00112233445566778899aabbccddeeff;

    fn vmm(vars: &[(&str, &str)]) -> Vmm {
        let vars: std::collections::BTreeMap<&str, &str> = [
            ("CONNECTOR_VMM_IMAGE", "ghcr.io/estuary/connector-vmm:dev"),
            ("CONNECTOR_VMM_STATE_DIR", "/var/lib/flow/connector-vmm"),
        ]
        .into_iter()
        .chain(vars.iter().copied())
        .collect();

        Vmm::from_vars(|name| Ok(vars.get(name).map(ToString::to_string)))
            .expect("valid configuration")
            .expect("capable")
    }

    /// `podman image inspect` of a Python derivation image, as it would be
    /// with `labels` and `exposed_ports`.
    fn inspection(labels: serde_json::Value, exposed_ports: serde_json::Value) -> Vec<u8> {
        let mut all_labels = json!({
            "FLOW_RUNTIME_CODEC": "json",
            "FLOW_RUNTIME_PROTOCOL": "derive",
        });
        all_labels
            .as_object_mut()
            .unwrap()
            .extend(labels.as_object().unwrap().clone());

        serde_json::to_vec(&json!([{
            "Id": "sha256:0123abcd",
            "Created": "2026-09-30T00:00:00Z",
            "Config": {
                "Entrypoint": ["/derive-python"],
                "Env": ["PATH=/usr/local/bin:/usr/bin", "UV_CACHE_DIR=/tmp/uv-cache"],
                "User": "nobody",
                "Labels": all_labels,
                "ExposedPorts": exposed_ports,
            },
        }]))
        .unwrap()
    }

    fn launch<'a>(inspection: &'a [u8]) -> Launch<'a> {
        Launch {
            id: ID,
            token: TOKEN,
            eligible: crate::vmm::eligible(ops::TaskType::Derivation, IMAGE)
                .expect("derive-python is eligible"),
            image: IMAGE,
            inspection,
            connector_mounts: MOUNTS,
            log_level: ops::LogLevel::UndefinedLevel,
            plane: crate::Plane::Public,
            task_name: "acmeCo/anvils/derivation",
            persistent_disk: None,
            egress: None,
        }
    }

    fn task_hosts(hosts: &[&str]) -> Vec<egress::AllowedName> {
        let hosts: Vec<String> = hosts.iter().map(ToString::to_string).collect();
        egress::hosts("task egress.hosts", &hosts).expect("valid task hosts")
    }

    /// Given the reference's own inputs, the plan's lines are the reference
    /// lines plus the arguments the reference leaves to a launcher, and the
    /// plan creates the container the reference runs. The reference sizes the
    /// container from guest RAM and this plan sizes guest RAM from the
    /// container's limit, so the inputs are chosen to meet.
    #[test]
    fn plan_reproduces_the_reference_lines() {
        use connector_vmm_tests::launch as reference;

        assert_eq!(super::BRIDGE_PREFIX, reference::BRIDGE_PREFIX);
        assert_eq!(
            crate::vmm::DEFAULT_MEMORY_OVERHEAD_MIB,
            reference::MEMORY_OVERHEAD_MIB
        );

        let vmm = vmm(&[
            ("CONNECTOR_MEMORY_LIMIT", "1280m"),
            ("CONNECTOR_CPU_LIMIT", "2"),
            ("CONNECTOR_VMM_DISK_MIB", "4096"),
            ("CONNECTOR_CGROUP_PARENT", "estuary-connectors.slice"),
        ]);
        let inspection = inspection(json!({}), json!({}));

        for persistent_disk in [
            None,
            Some(("/var/lib/flow/disks/acmeCo-state", "/acmeCo/state")),
        ] {
            let plan = super::plan(
                &vmm,
                &Launch {
                    persistent_disk: persistent_disk.map(|(host_dir, guest_path)| PersistentDisk {
                        host_dir,
                        guest_path,
                    }),
                    ..launch(&inspection)
                },
            )
            .expect("plans");

            let expected = reference::reference(&reference::Launch {
                name: &plan.name,
                network: &plan.name,
                state_dir: &format!("/var/lib/flow/connector-vmm/{}", plan.name),
                connector_mount: &plan.mount,
                connector_image: IMAGE,
                vmm_image: &vmm.image,
                memory_mib: 1024,
                vcpus: 2,
                disk_mib: 4096,
                log_level: "warn",
                persistent_disk,
            });
            let launcher_only = |arg: &String| {
                ["--platform=", "--label=", "--cgroup-parent="]
                    .iter()
                    .any(|prefix| arg.starts_with(prefix))
            };
            let owner = "--label=dev.estuary.vmm-owner=00112233445566778899aabbccddeeff";

            let (added, create): (Vec<String>, Vec<String>) =
                plan.create.iter().cloned().partition(launcher_only);
            assert_eq!(
                (create[0].as_str(), expected[0].as_str()),
                ("create", "run")
            );
            assert_eq!(create[1..], expected[1..], "{persistent_disk:?}");
            assert_eq!(
                added,
                [
                    "--platform=linux/amd64",
                    "--label=image=ghcr.io/estuary/derive-python:stable",
                    "--label=task-name=acmeCo/anvils/derivation",
                    "--label=task-type=derivation",
                    owner,
                    "--cgroup-parent=estuary-connectors.slice",
                ]
            );

            let (added, network): (Vec<String>, Vec<String>) =
                plan.network.iter().cloned().partition(launcher_only);
            assert_eq!(network, reference::network(&plan.name, &plan.interface));
            assert_eq!(added, [owner]);
            assert_eq!(
                plan.verify,
                reference::boundary(&vmm.image, "host", "verify")
            );
        }
    }

    /// Everything one plan holds, where the limits are spelled as an operator
    /// might write them and the guest's differ from the container's, and the
    /// task's egress repeats a default and a label host.
    #[test]
    fn a_plan() {
        let vmm = vmm(&[
            ("CONNECTOR_MEMORY_LIMIT", "1g"),
            ("CONNECTOR_CPU_LIMIT", "1.5"),
        ]);
        let inspection = inspection(
            json!({"dev.estuary.egress-hosts": r#"["api.acmeco.example", "*.cdn.acmeco.example"]"#}),
            json!({"9000/udp": {}}),
        );
        let task_hosts = task_hosts(&["API.acmeco.example", "*.svc.acmeco.example", "pypi.org"]);
        let plan = super::plan(
            &vmm,
            &Launch {
                plane: crate::Plane::Local,
                egress: Some(&task_hosts),
                ..launch(&inspection)
            },
        )
        .expect("plans");

        insta::assert_snapshot!(describe(&plan));

        let policy = egress::parse(&plan.policy).expect("the VMM's parser accepts the policy");
        assert_eq!(policy.egress, egress::Mode::Public);
        assert!(!policy.allow_all);
        let names: Vec<String> = policy
            .allowed_names
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            names,
            [
                "pypi.org",
                "files.pythonhosted.org",
                "api.acmeco.example",
                "*.cdn.acmeco.example",
                "*.svc.acmeco.example",
            ]
        );
    }

    #[test]
    fn egress_by_plane_and_declaration() {
        let vmm = vmm(&[]);
        let label = json!({"dev.estuary.egress-hosts": r#"["api.acmeco.example"]"#});
        let declared = task_hosts(&[
            "*.svc.acmeco.example",
            "API.acmeco.example",
            "files.pythonhosted.org",
            "ok.acmeco.example",
        ]);
        let declarations: [(&str, Option<&[egress::AllowedName]>); 3] = [
            ("omitted", None),
            ("empty", Some(&[])),
            ("hosts", Some(&declared)),
        ];

        let mut table = String::new();
        for (labels, what) in [(json!({}), "no label"), (label, "a label")] {
            let inspection = inspection(labels, json!({}));
            for plane in [
                crate::Plane::Public,
                crate::Plane::Private,
                crate::Plane::Local,
            ] {
                for (declaration, egress) in declarations {
                    let plan = super::plan(
                        &vmm,
                        &Launch {
                            plane,
                            egress,
                            ..launch(&inspection)
                        },
                    )
                    .expect("plans");
                    table.push_str(&format!(
                        "# {what}, {} plane, egress {declaration}
{}
{}

",
                        plane.as_str_name(),
                        plan.egress,
                        String::from_utf8(plan.policy).unwrap(),
                    ));
                }
            }
        }
        insta::assert_snapshot!(table);
    }

    fn describe(plan: &Plan) -> String {
        let Plan {
            name,
            interface,
            state,
            record,
            token,
            mount,
            directories,
            scratch,
            egress,
            policy,
            policy_path,
            socket,
            mount_files,
            verify,
            network,
            create,
        } = plan;
        let modes = |entries: &[(String, u32)]| -> String {
            entries
                .iter()
                .map(|(path, mode)| format!("{mode:04o} {path}\n"))
                .collect()
        };

        format!(
            "name: {name}\ninterface: {interface}\nstate: {state}\nrecord: {record}\n\
             token: {token}\nmount: {mount}\nscratch: {scratch}\n\
             socket: {socket} ({} bytes)\n\
             \n## directories\n{}\n## egress\n{egress}\n\n## {policy_path}\n{}\n\n## connector mount\n{}\
             \n## verify\npodman {}\n\n## network\npodman {}\n\n## create\npodman {}\n",
            socket.len(),
            modes(directories),
            String::from_utf8_lossy(policy),
            modes(mount_files),
            verify.join(" "),
            network.join(" "),
            create.join(" \\\n  "),
        )
    }

    /// What an image's inspection lets a launch reach, or why it cannot launch.
    #[test]
    fn image_inspections() {
        let label = |value: &str| json!({ "dev.estuary.egress-hosts": value });
        let cases: Vec<(&str, serde_json::Value, serde_json::Value)> = vec![
            ("no label: the defaults alone", json!({}), json!({})),
            (
                "hosts beyond the defaults, lowercased, without repeats",
                label(
                    r#"["api.acmeco.example", "*.ACMEco.example", "PyPI.org", "api.acmeco.example"]"#,
                ),
                json!({}),
            ),
            ("an empty label", label(""), json!({})),
            ("a blank label", label("  "), json!({})),
            ("an empty array", label("[]"), json!({})),
            (
                "not JSON",
                label("pypi.org, files.pythonhosted.org"),
                json!({}),
            ),
            (
                "not an array",
                label(r#"{"hosts": ["pypi.org"]}"#),
                json!({}),
            ),
            ("not strings", label(r#"["pypi.org", 443]"#), json!({})),
            (
                "a URL",
                label(r#"["https://api.acmeco.example"]"#),
                json!({}),
            ),
            ("a port", label(r#"["api.acmeco.example:443"]"#), json!({})),
            ("an address", label(r#"["93.184.216.34"]"#), json!({})),
            ("a CIDR", label(r#"["93.184.216.0/24"]"#), json!({})),
            ("everything", label(r#"["*"]"#), json!({})),
            (
                "a wildcard over a public suffix",
                label(r#"["*.github.io"]"#),
                json!({}),
            ),
            (
                "a wildcard beneath a private suffix",
                label(r#"["*.acmeco.github.io"]"#),
                json!({}),
            ),
            ("a single label", label(r#"["localhost"]"#), json!({})),
            (
                "a public port",
                json!({"dev.estuary.port-public.8080": "true"}),
                json!({"8080/tcp": {}}),
            ),
            (
                "a private port",
                json!({}),
                json!({"8080/tcp": {}, "9090": {}}),
            ),
            (
                "only a UDP port, which ordinary launches ignore too",
                json!({}),
                json!({"9000/udp": {}}),
            ),
            (
                "another protocol",
                json!({"FLOW_RUNTIME_PROTOCOL": "capture"}),
                json!({}),
            ),
        ];

        let vmm = vmm(&[]);
        let mut table = String::new();

        for (name, labels, ports) in cases {
            let inspection = inspection(labels.clone(), ports.clone());
            let outcome = match super::plan(&vmm, &launch(&inspection)) {
                Ok(plan) => String::from_utf8(plan.policy).unwrap(),
                Err(error) => format!("refused: {error:#}"),
            };
            table.push_str(&format!(
                "# {name}\nlabels {labels} ports {ports}\n=> {outcome}\n\n"
            ));
        }
        insta::assert_snapshot!(table);
    }

    /// Path limits are in bytes, and every path the container binds is one
    /// podman's `--mount` can carry.
    #[test]
    fn paths() {
        let mut table = String::new();

        let long = format!("/var/lib/flow/{}", "a".repeat(58));
        let longer = format!("{long}a");
        let wide = format!("/var/lib/flow/{}", "é".repeat(29));
        let wider = format!("{wide}é");

        table.push_str("## state directory\n");
        for state_dir in [
            "/var/lib/flow/connector-vmm",
            long.as_str(),
            longer.as_str(),
            wide.as_str(),
            wider.as_str(),
            "var/lib/flow",
            "/",
            "/var/lib/flow/",
            "/var/lib/flow,rw=true",
        ] {
            let outcome = match super::check_state_dir(state_dir) {
                Ok(()) => {
                    let vmm = Vmm {
                        state_dir: state_dir.to_string(),
                        ..vmm(&[])
                    };
                    let inspection = inspection(json!({}), json!({}));
                    let plan = super::plan(&vmm, &launch(&inspection)).expect("plans");
                    format!("socket {} bytes", plan.socket.len())
                }
                Err(error) => format!("refused: {error:#}"),
            };
            table.push_str(&format!(
                "{state_dir:?} ({} bytes, {} chars) => {outcome}\n",
                state_dir.len(),
                state_dir.chars().count(),
            ));
        }

        let vmm = vmm(&[]);
        let inspection = inspection(json!({}), json!({}));
        let cases: Vec<(&str, Launch)> = vec![
            (
                "a relative mounts directory",
                Launch {
                    connector_mounts: "tmp/connector-mounts-0",
                    ..launch(&inspection)
                },
            ),
            (
                "a comma in the mounts directory",
                Launch {
                    connector_mounts: "/tmp/connector-mounts-0,rw=true",
                    ..launch(&inspection)
                },
            ),
            (
                "a comma in the image",
                Launch {
                    image: "ghcr.io/estuary/derive-python:stable,rw=true",
                    ..launch(&inspection)
                },
            ),
            (
                "a comma in the persistent disk",
                Launch {
                    persistent_disk: Some(PersistentDisk {
                        host_dir: "/var/lib/disk,rw",
                        guest_path: "/state",
                    }),
                    ..launch(&inspection)
                },
            ),
            (
                "a relative persistent disk guest path",
                Launch {
                    persistent_disk: Some(PersistentDisk {
                        host_dir: "/var/lib/disk",
                        guest_path: "state",
                    }),
                    ..launch(&inspection)
                },
            ),
            (
                "a comma in the persistent disk guest path, which is not a mount option",
                Launch {
                    persistent_disk: Some(PersistentDisk {
                        host_dir: "/var/lib/disk",
                        guest_path: "/a,b",
                    }),
                    ..launch(&inspection)
                },
            ),
        ];
        table.push_str("\n## launch\n");
        for (name, launch) in cases {
            let outcome = match super::plan(&vmm, &launch) {
                Ok(_) => "plans".to_string(),
                Err(error) => format!("refused: {error:#}"),
            };
            table.push_str(&format!("{name} => {outcome}\n"));
        }
        insta::assert_snapshot!(table);
    }
}
