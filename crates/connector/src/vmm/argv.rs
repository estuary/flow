//! Podman argument builders shared by VMM launch planning and the KVM harness.

/// Must match `boundary::BRIDGE_PREFIX` of `flow-connector-vmm`: a bridge so
/// named is behind the host boundary, and nothing else puts it there.
pub const BRIDGE_PREFIX: &str = "fvm";

/// Container memory above guest RAM: the VMM's own allocations, its virtiofs
/// servers, guest page tables, and page cache charged to the container.
pub const DEFAULT_MEMORY_OVERHEAD_MIB: u32 = 256;

/// The VMM container's only capabilities: its line drops podman's
/// defaults, and adds these. libkrun's virtiofs server runs in the VMM's
/// process, as host root, so a guest's file operations on a share are the
/// VMM's own. The guest's privileges, and guest-init's NO_NEW_PRIVS, are
/// its kernel's affair, and grant nothing here.
const CAPABILITIES: &[&str] = &[
    // The server's chown, for a guest's on a writable share.
    "CAP_CHOWN",
    // The launcher's 0700 state directories, and image files whatever their
    // mode, which the server opens for any guest user.
    "CAP_DAC_OVERRIDE",
    // The server's chmod and times, for a guest user on its own files.
    "CAP_FOWNER",
    // The server's chmod keeps the setgid bit a guest user sets.
    "CAP_FSETID",
    // The tap, its addresses and the VMM's nftables, including the
    // resolver's sets.
    "CAP_NET_ADMIN",
    // The resolver's port 53.
    "CAP_NET_BIND_SERVICE",
    // The server creates a nonroot guest user's files as that user, and
    // refuses them without these.
    "CAP_SETGID",
    "CAP_SETUID",
];

pub struct Container<'a> {
    /// `fv_<16 hex>`, which also names its network and rootfs volume.
    pub name: &'a str,
    /// The launch's own state directory, holding the `sock/` and
    /// `scratch/` the container binds.
    pub state: &'a str,
    /// Shared at that same path into the container and the guest.
    pub connector_mount: &'a str,
    pub vmm_image: &'a str,
    /// The container's cgroup cap, and the guest RAM within it.
    pub memory: super::Memory,
    /// The container's CPU quota, as podman's `--cpus` reads it.
    pub cpus: &'a str,
    pub vcpus: u8,
    pub disk_mib: u64,
    pub log_level: &'a str,
    pub persistent_disk: Option<PersistentDisk<'a>>,
    /// Podman options before the image.
    pub options: &'a [String],
}

/// The task's persistent storage: a host directory, and where the guest
/// mounts it.
#[derive(Clone, Copy)]
pub struct PersistentDisk<'a> {
    pub host_dir: &'a str,
    pub guest_path: &'a str,
}

/// `podman <command>` of the VMM's container: `create` or `run`. Its last
/// arguments are the VMM's own `run`, to which a caller may append.
pub fn container(command: &str, container: &Container) -> Vec<String> {
    let Container {
        name,
        state,
        connector_mount: mount,
        vmm_image,
        memory,
        cpus,
        vcpus,
        disk_mib,
        log_level,
        persistent_disk,
        options,
    } = container;

    let mut args: Vec<String> = vec![
        command.to_string(),
        "--rm".to_string(),
        format!("--name={name}"),
        format!("--network={name}"),
        "--log-driver=none".to_string(),
        "--read-only".to_string(),
        // Without it podman mounts writable tmpfs at /tmp, /run and /var/tmp.
        "--read-only-tmpfs=false".to_string(),
        "--device=/dev/kvm".to_string(),
        "--device=/dev/net/tun".to_string(),
        "--cap-drop=all".to_string(),
    ];
    args.extend(CAPABILITIES.iter().map(|cap| format!("--cap-add={cap}")));
    args.extend([
        "--sysctl=net.ipv4.ip_forward=1".to_string(),
        "--sysctl=net.ipv4.conf.default.rp_filter=1".to_string(),
        // /proc/sys is read-only in the container, so the tap can only
        // inherit disabled IPv6 from here.
        "--sysctl=net.ipv6.conf.default.disable_ipv6=1".to_string(),
        format!("--memory={}", memory.bytes),
        format!("--cpus={cpus}"),
        format!("--mount=type=volume,source={name},destination=/rootfs,readonly"),
        format!("--mount=type=bind,source={mount},target={mount},readonly"),
        format!("--mount=type=bind,source={state}/sock,target=/sock"),
        format!("--mount=type=bind,source={state}/scratch,target=/scratch-backing"),
    ]);
    if let Some(PersistentDisk { host_dir, .. }) = persistent_disk {
        args.push(format!(
            "--mount=type=bind,source={host_dir},target=/persistent-disk"
        ));
    }
    args.extend([
        "--env=LOG_FORMAT=json".to_string(),
        format!("--env=LOG_LEVEL={log_level}"),
    ]);
    args.extend_from_slice(options);
    args.extend([
        vmm_image.to_string(),
        "run".to_string(),
        "--connector-mount".to_string(),
        mount.to_string(),
        "--memory-mib".to_string(),
        memory.guest_mib.to_string(),
        "--vcpus".to_string(),
        vcpus.to_string(),
        "--disk-mib".to_string(),
        disk_mib.to_string(),
    ]);
    if let Some(PersistentDisk { guest_path, .. }) = persistent_disk {
        args.push("--persistent-disk".to_string());
        args.push(guest_path.to_string());
    }
    args
}

/// The network one VMM container joins, alone, on bridge `interface`.
/// Podman's resolver stays enabled on it: its gateway is the VMM's
/// nameserver, the boundary's one exception.
pub fn network(name: &str, interface: &str, options: &[String]) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "network".to_string(),
        "create".to_string(),
        "--driver=bridge".to_string(),
        format!("--interface-name={interface}"),
    ];
    args.extend_from_slice(options);
    args.push(name.to_string());
    args
}

/// The VMM's rootfs volume, of connector image `image`. type=image shares a
/// mounted image across launches. Podman 4.9 unmounts it even when removing
/// a container which never started, invalidating a sibling's pending OCI
/// mount. The image volume driver gives each launch its own storage
/// container instead.
pub fn rootfs(name: &str, image: &str, options: &[String]) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "volume".to_string(),
        "create".to_string(),
        "--driver=image".to_string(),
        format!("--opt=image={image}"),
    ];
    args.extend_from_slice(options);
    args.push(name.to_string());
    args
}

/// What becomes of a boundary operation's container once it exits.
#[derive(Clone, Copy, PartialEq)]
pub enum Exited {
    Removed,
    Kept,
}

/// Run a host-side boundary operation from the same image as the VMM, with
/// `options` for podman's `run`.
pub fn host_boundary(image: &str, action: &str, exited: Exited, options: &[String]) -> Vec<String> {
    let mut args: Vec<String> = vec!["run".into()];
    if exited == Exited::Removed {
        args.push("--rm".into());
    }
    args.extend([
        "--network=host".into(),
        "--log-driver=none".into(),
        "--read-only".into(),
        "--cap-drop=all".into(),
        "--cap-add=CAP_NET_ADMIN".into(),
    ]);
    args.extend_from_slice(options);
    args.extend([image.into(), "boundary".into(), action.into()]);
    args
}

#[cfg(test)]
mod test {
    fn container(persistent_disk: Option<super::PersistentDisk>) -> String {
        super::container(
            "create",
            &super::Container {
                name: "fv_0123456789abcdef",
                state: "/var/lib/flow/connector-vmm/fv_0123456789abcdef",
                connector_mount: "/tmp/connector-mounts-0/mount-acme",
                vmm_image: "ghcr.io/estuary/connector-vmm:dev",
                memory: crate::vmm::Memory {
                    bytes: 1280 << 20,
                    guest_mib: 1024,
                },
                cpus: "2",
                vcpus: 2,
                disk_mib: 4096,
                log_level: "warn",
                persistent_disk,
                options: &["--label=acme=anvils".to_string()],
            },
        )
        .join("\n")
    }

    #[test]
    fn container_line() {
        insta::assert_snapshot!(container(None));
    }

    #[test]
    fn container_line_with_a_persistent_disk() {
        insta::assert_snapshot!(container(Some(super::PersistentDisk {
            host_dir: "/var/lib/flow/disks/acmeCo-state",
            guest_path: "/acmeCo/state",
        })));
    }

    #[test]
    fn network_line() {
        insta::assert_snapshot!(
            super::network("fv_0123456789abcdef", "fvm0123456789ab", &[]).join("\n")
        );
    }

    #[test]
    fn boundary_line() {
        insta::assert_snapshot!(
            super::host_boundary(
                "ghcr.io/estuary/connector-vmm:dev",
                "verify",
                super::Exited::Removed,
                &[]
            )
            .join("\n")
        );
    }
}
