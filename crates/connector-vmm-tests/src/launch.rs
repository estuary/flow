//! The reference podman lines for a VMM container: its network, its `run`,
//! and the host boundary it sits behind.
//!
//! A launcher must produce these lines byte for byte apart from their ids and
//! the container's `--memory` and `--cpus`, which it takes from its own limits,
//! so they are pure functions and each production shape is a snapshot. They
//! carry every flag that bears on the VMM; labels, `--cgroup-parent` and
//! `--platform` are the launcher's own business and are absent. A launcher
//! may `podman create` with the `run` line's arguments and then start the
//! container, as `crates/connector`'s does.

/// Must match `boundary::BRIDGE_PREFIX` in `flow-connector-vmm`: a bridge
/// named so is behind the host boundary, and nothing else puts it there.
pub const BRIDGE_PREFIX: &str = "fvm";

/// The network one VMM container joins, alone. Podman's resolver stays enabled
/// on it: its gateway is the VMM's nameserver, the boundary's one exception.
pub fn network(name: &str, interface: &str) -> Vec<String> {
    vec![
        "network".to_string(),
        "create".to_string(),
        "--driver=bridge".to_string(),
        format!("--interface-name={interface}"),
        name.to_string(),
    ]
}

/// `flow-connector-vmm boundary ACTION` from the VMM image, in the network
/// namespace `network` names: `host` in production, `ns:<path>` in the
/// suite's tamper test. A launcher verifies with this line before it creates
/// each VMM network.
pub fn boundary(vmm_image: &str, network: &str, action: &str) -> Vec<String> {
    vec![
        "run".to_string(),
        "--rm".to_string(),
        format!("--network={network}"),
        "--log-driver=none".to_string(),
        "--read-only".to_string(),
        "--cap-drop=all".to_string(),
        "--cap-add=CAP_NET_ADMIN".to_string(),
        vmm_image.to_string(),
        "boundary".to_string(),
        action.to_string(),
    ]
}

/// Container memory above guest RAM: the VMM's own allocations, the virtiofs
/// servers and guest page tables. The launcher's default.
pub const MEMORY_OVERHEAD_MIB: u32 = 256;

pub struct Launch<'a> {
    /// `fv_<16 hex>`.
    pub name: &'a str,
    /// The container's own network, from `network`.
    pub network: &'a str,
    /// `<root>/state/<name>`, holding `init/`, `sock/` and `scratch/`.
    pub state_dir: &'a str,
    pub connector_mount: &'a str,
    pub connector_image: &'a str,
    pub vmm_image: &'a str,
    pub memory_mib: u32,
    pub vcpus: u8,
    pub disk_mib: u64,
    pub log_level: &'a str,
    /// The task's persistent disk: its host directory and its guest path.
    pub persistent_disk: Option<(&'a str, &'a str)>,
}

/// The argv after `podman`. Test-only VMM flags are appended by the caller,
/// after everything here.
pub fn reference(launch: &Launch) -> Vec<String> {
    let Launch {
        name,
        network,
        state_dir,
        connector_mount: mount,
        connector_image,
        vmm_image,
        memory_mib,
        vcpus,
        disk_mib,
        log_level,
        persistent_disk,
    } = launch;

    let mut argv: Vec<String> = [
        "run".to_string(),
        "--rm".to_string(),
        format!("--name={name}"),
        format!("--network={network}"),
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
        format!("--memory={}m", memory_mib + MEMORY_OVERHEAD_MIB),
        format!("--cpus={vcpus}"),
        format!("--mount=type=image,source={connector_image},destination=/rootfs,rw=true"),
        format!("--mount=type=bind,source={state_dir}/init,target=/init,readonly"),
        format!("--mount=type=bind,source={mount},target={mount},readonly"),
        format!("--mount=type=bind,source={state_dir}/sock,target=/sock"),
        format!("--mount=type=bind,source={state_dir}/scratch,target=/scratch-backing"),
    ]
    .into();

    if let Some((host_dir, _)) = persistent_disk {
        argv.push(format!(
            "--mount=type=bind,source={host_dir},target=/persistent-disk"
        ));
    }
    argv.extend([
        format!("--env=CONNECTOR_MOUNT={mount}"),
        "--env=LOG_FORMAT=json".to_string(),
        format!("--env=LOG_LEVEL={log_level}"),
        vmm_image.to_string(),
        "run".to_string(),
        "--policy".to_string(),
        "/init/policy.json".to_string(),
        "--connector-mount".to_string(),
        mount.to_string(),
        "--memory-mib".to_string(),
        memory_mib.to_string(),
        "--vcpus".to_string(),
        vcpus.to_string(),
        "--disk-mib".to_string(),
        disk_mib.to_string(),
    ]);
    if let Some((_, guest_path)) = persistent_disk {
        argv.push("--persistent-disk".to_string());
        argv.push(guest_path.to_string());
    }
    argv
}

#[cfg(test)]
mod tests {
    fn launch(persistent_disk: Option<(&'static str, &'static str)>) -> String {
        let argv = super::reference(&super::Launch {
            name: "fv_0123456789abcdef",
            network: "fv_0123456789abcdef",
            state_dir: "/var/lib/flow/connector-vmm/fv_0123456789abcdef",
            connector_mount: "/tmp/connector-mounts-0/mount-acme",
            connector_image: "ghcr.io/acmeco/source-acme:v1",
            vmm_image: "ghcr.io/estuary/connector-vmm:dev",
            memory_mib: 1024,
            vcpus: 2,
            disk_mib: 4096,
            log_level: "warn",
            persistent_disk,
        });
        argv.join("\n")
    }

    #[test]
    fn reference_launch_line() {
        insta::assert_snapshot!(launch(None));
    }

    #[test]
    fn reference_network_line() {
        insta::assert_snapshot!(
            super::network("fv_0123456789abcdef", "fvm0123456789ab").join("\n")
        );
    }

    #[test]
    fn reference_boundary_line() {
        insta::assert_snapshot!(
            super::boundary("ghcr.io/estuary/connector-vmm:dev", "host", "verify").join("\n")
        );
    }

    #[test]
    fn reference_launch_line_with_a_persistent_disk() {
        insta::assert_snapshot!(launch(Some((
            "/var/lib/flow/disks/acmeCo-state",
            "/acmeCo/state"
        ))));
    }
}
