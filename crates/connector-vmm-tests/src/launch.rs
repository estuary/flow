//! The reference `podman run` line for a VMM container.
//!
//! A launcher must produce this line byte for byte apart from its ids, so it
//! is a pure function and each production shape is a snapshot. It carries
//! every flag that bears on the VMM; labels, `--cgroup-parent` and
//! `--platform` are the launcher's own business and are absent.

/// The podman network VMM containers join, as they do in production.
pub const NETWORK: &str = "flow-connectors";

/// Container memory above guest RAM: the VMM's own allocations, the virtiofs
/// servers and guest page tables. The launcher's default.
pub const MEMORY_OVERHEAD_MIB: u32 = 256;

pub struct Launch<'a> {
    /// `fv_<16 hex>`.
    pub name: &'a str,
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
        format!("--network={NETWORK}"),
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
    fn reference_launch_line_with_a_persistent_disk() {
        insta::assert_snapshot!(launch(Some((
            "/var/lib/flow/disks/acmeCo-state",
            "/acmeCo/state"
        ))));
    }
}
