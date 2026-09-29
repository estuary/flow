//! The addressing the VMM, its guest and its ruleset are pinned to, and the
//! network the guest meets before it boots.
//!
//! Pinned rather than configurable: the guest init is handed the same two
//! addresses, the enclosing `192.0.2.0/24` is a baseline exclusion in its own
//! right, and the ruleset, the resolver and the guest's `resolv.conf` all have
//! to agree on them or the guest has no working network.
//!
//! Every routine here runs before `krun_start_enter`, because the guest's
//! first packet has to meet a finished ruleset.

use ipnetwork::Ipv4Network;
use std::net::{Ipv4Addr, SocketAddr};

/// The point-to-point tap between the VMM and its guest, a `/30`.
pub const TAP: &str = "tap0";
pub const VMM_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
pub const GUEST_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);
pub const PREFIX_LEN: u8 = 30;

/// The interface the guest is masqueraded out of: podman's primary interface
/// in the VMM container.
pub const UPLINK: &str = "eth0";

/// Locally administered, and its last octet matches the guest's address so a
/// packet capture reads the same on both layers.
pub const GUEST_MAC: [u8; 6] = [0x02, 0xf1, 0x0f, 0x00, 0x00, 0x02];

/// libkrun creates its own tap descriptor inside `krun_start_enter` and sets
/// no address, so the device is created persistent here and libkrun's
/// `TUNSETIFF` attaches to it.
pub fn create_tap() -> anyhow::Result<()> {
    let address = format!("{VMM_IP}/{PREFIX_LEN}");

    crate::sys::run("ip", &["tuntap", "add", "dev", TAP, "mode", "tap"], None)?;
    crate::sys::run("ip", &["addr", "add", &address, "dev", TAP], None)?;
    crate::sys::run("ip", &["link", "set", TAP, "up"], None)
}

/// Podman mounts `/proc/sys` read-only whether or not the container is
/// `--read-only`, so the VMM cannot turn IPv6 off itself: the launcher has to
/// pass `--sysctl net.ipv6.conf.default.disable_ipv6=1`, which a tap created
/// afterwards inherits. Reading is still allowed, so a launch line missing the
/// flag is caught here rather than surfacing later as a tap that emits MLD
/// reports the ruleset has to drop on every VM.
pub fn check_ipv6_disabled() -> anyhow::Result<()> {
    let path = format!("/proc/sys/net/ipv6/conf/{TAP}/disable_ipv6");

    let disabled = match std::fs::read_to_string(&path) {
        Ok(content) => content,
        // No such knob means the kernel has no IPv6 at all, which is the
        // outcome this check is asking for.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => anyhow::bail!("reading {path}: {error}"),
    };
    if disabled.trim() == "1" {
        return Ok(());
    }
    anyhow::bail!(
        "{TAP} has IPv6 enabled; the VMM container must be launched with \
         --sysctl net.ipv6.conf.default.disable_ipv6=1"
    )
}

/// Replace the kernel's ruleset with this policy's, wholesale.
pub fn apply_ruleset(ruleset: &str) -> anyhow::Result<()> {
    crate::sys::run("nft", &["-f", "-"], Some(ruleset.as_bytes()))
}

/// Discover the VMM's IPv4 subnets so the baseline covers whatever podman
/// assigned this container.
pub fn vmm_subnets() -> anyhow::Result<Vec<Ipv4Network>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();

    // SAFETY: `head` is a live pointer for the call to write through.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        anyhow::bail!("getifaddrs: {}", std::io::Error::last_os_error());
    }
    let mut subnets: Vec<Ipv4Network> = Vec::new();
    let mut cursor = head;

    while !cursor.is_null() {
        // SAFETY: the list is owned by this thread until `freeifaddrs` below,
        // and every non-null entry is a valid `ifaddrs`.
        let entry = unsafe { &*cursor };
        cursor = entry.ifa_next;

        if entry.ifa_flags & libc::IFF_LOOPBACK as u32 != 0 {
            continue;
        }
        let (Some(address), Some(netmask)) =
            (sockaddr_in(entry.ifa_addr), sockaddr_in(entry.ifa_netmask))
        else {
            continue;
        };
        let prefix_len = u32::from(netmask).count_ones() as u8;
        let Ok(subnet) = Ipv4Network::new(address, prefix_len) else {
            continue;
        };
        if !subnets.contains(&subnet) {
            subnets.push(subnet);
        }
    }
    // SAFETY: `head` is exactly what getifaddrs returned and is not used after.
    unsafe { libc::freeifaddrs(head) };

    Ok(subnets)
}

/// The VMM's own nameserver, which the resolver forwards to. `resolv.conf` has
/// no port syntax, so 53 is implied.
pub fn upstream_nameserver() -> anyhow::Result<SocketAddr> {
    let path = "/etc/resolv.conf";
    let content =
        std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;

    let Some(address) = first_nameserver(&content) else {
        anyhow::bail!("no nameserver line in {path}");
    };
    let address: Ipv4Addr = address
        .parse()
        .map_err(|e| anyhow::anyhow!("the nameserver in {path} is not an IPv4 address: {e}"))?;

    Ok(SocketAddr::from((address, 53)))
}

fn first_nameserver(resolv_conf: &str) -> Option<&str> {
    resolv_conf
        .lines()
        .filter_map(|line| line.strip_prefix("nameserver"))
        .map(str::trim)
        .find(|address| !address.is_empty())
}

fn sockaddr_in(address: *const libc::sockaddr) -> Option<Ipv4Addr> {
    // SAFETY: getifaddrs hands back either null or a sockaddr whose family
    // field is initialized; nothing past the family is read unless it is INET.
    if address.is_null() || unsafe { (*address).sa_family } != libc::AF_INET as libc::sa_family_t {
        return None;
    }
    let address = address.cast::<libc::sockaddr_in>();
    // SAFETY: an AF_INET sockaddr is a sockaddr_in.
    Some(Ipv4Addr::from(u32::from_be(unsafe {
        (*address).sin_addr.s_addr
    })))
}
