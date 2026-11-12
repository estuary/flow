//! Static configuration of the guest end of the tap.
//!
//! The classic `ifreq`/`rtentry` ioctls rather than netlink: three addresses
//! and one route do not justify an rtnetlink implementation, and the guest
//! kernel offers both.

use std::ffi::CString;
use std::net::Ipv4Addr;
use std::os::fd::{AsFd, AsRawFd};

use crate::sys::{self, Result};

/// The tap's guest side. libkrun names its only virtio-net device this.
const INTERFACE: &str = "eth0";

pub fn configure(address: Ipv4Addr, prefix_len: u8, gateway: Ipv4Addr) -> Result<()> {
    let socket = rustix::net::socket(
        rustix::net::AddressFamily::INET,
        rustix::net::SocketType::DGRAM,
        None,
    )
    .map_err(|e| format!("socket(AF_INET, SOCK_DGRAM): {e}"))?;

    let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
    for (slot, byte) in request.ifr_name.iter_mut().zip(INTERFACE.bytes()) {
        *slot = byte as _;
    }

    request.ifr_ifru.ifru_addr = sockaddr(address);
    ioctl(
        socket.as_fd(),
        libc::SIOCSIFADDR as libc::Ioctl,
        &request,
        "SIOCSIFADDR",
    )?;

    request.ifr_ifru.ifru_netmask = sockaddr(netmask(prefix_len));
    ioctl(
        socket.as_fd(),
        libc::SIOCSIFNETMASK as libc::Ioctl,
        &request,
        "SIOCSIFNETMASK",
    )?;

    request.ifr_ifru.ifru_flags = (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
    ioctl(
        socket.as_fd(),
        libc::SIOCSIFFLAGS as libc::Ioctl,
        &request,
        "SIOCSIFFLAGS",
    )?;

    // `rt_dev` is borrowed by the ioctl, not copied, so the CString outlives it.
    let device = CString::new(INTERFACE).expect("an interface name contains no NUL");
    let mut route: libc::rtentry = unsafe { std::mem::zeroed() };
    route.rt_dst = sockaddr(Ipv4Addr::UNSPECIFIED);
    route.rt_genmask = sockaddr(Ipv4Addr::UNSPECIFIED);
    route.rt_gateway = sockaddr(gateway);
    route.rt_flags = libc::RTF_UP | libc::RTF_GATEWAY;
    route.rt_dev = device.as_ptr().cast_mut();
    ioctl(
        socket.as_fd(),
        libc::SIOCADDRT as libc::Ioctl,
        &route,
        "SIOCADDRT(default)",
    )
}

/// The tap carries IPv4 only and the VMM's ruleset has no IPv6 rules, so an
/// address the guest could form is an address it could try and hang on.
pub fn disable_ipv6() -> Result<()> {
    for scope in ["all", "default"] {
        let path = format!("/proc/sys/net/ipv6/conf/{scope}/disable_ipv6");
        match std::fs::write(&path, "1\n") {
            Ok(()) => (),
            // A kernel built without IPv6 has already done the job.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(format!("writing {path}: {e}")),
        }
    }
    Ok(())
}

/// `libc::Ioctl` is the request type of the target's `ioctl`, which musl
/// declares as `c_int` where glibc uses `c_ulong`; the `SIOC*` constants are
/// `c_ulong` either way, so the cast is at the call sites.
fn ioctl<T>(
    socket: std::os::fd::BorrowedFd<'_>,
    request: libc::Ioctl,
    argument: &T,
    what: &str,
) -> Result<()> {
    // Safety: `argument` is the struct this ioctl expects, by the caller's
    // construction, and is only read for the duration of the call.
    if unsafe { libc::ioctl(socket.as_raw_fd(), request, argument as *const T) } < 0 {
        return Err(sys::last_error(format!("ioctl {what} on {INTERFACE}")));
    }
    Ok(())
}

/// The generic `sockaddr` an `ifreq` or `rtentry` carries, holding the AF_INET
/// view of one address. Both structs are the same 16 bytes.
fn sockaddr(address: Ipv4Addr) -> libc::sockaddr {
    let inet = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr {
            // `s_addr` is network order, which is the octets in memory order.
            s_addr: u32::from_ne_bytes(address.octets()),
        },
        sin_zero: [0; 8],
    };
    // Safety: `sockaddr_in` is `sockaddr`'s AF_INET variant and the same size.
    unsafe { std::mem::transmute(inet) }
}

fn netmask(prefix_len: u8) -> Ipv4Addr {
    Ipv4Addr::from(u32::MAX << (32 - u32::from(prefix_len)))
}
