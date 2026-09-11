//! The helper's own IPv4 subnets, read from its interfaces rather than
//! assumed: PLAN experiment 6 requires the baseline to cover whatever podman
//! actually gave this container, and the tap /30 comes back from the same
//! call.

use crate::cidr::Cidr;
use std::net::Ipv4Addr;

pub fn helper_subnets() -> anyhow::Result<Vec<Cidr>> {
    let mut head: *mut libc::ifaddrs = std::ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        anyhow::bail!("getifaddrs: {}", std::io::Error::last_os_error());
    }

    let mut subnets: Vec<Cidr> = Vec::new();
    let mut cursor = head;
    while !cursor.is_null() {
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
        let subnet = Cidr::new(address, prefix_len);
        if !subnets.contains(&subnet) {
            subnets.push(subnet);
        }
    }

    unsafe { libc::freeifaddrs(head) };
    Ok(subnets)
}

fn sockaddr_in(address: *const libc::sockaddr) -> Option<Ipv4Addr> {
    if address.is_null() || unsafe { (*address).sa_family } != libc::AF_INET as libc::sa_family_t {
        return None;
    }
    let address = address as *const libc::sockaddr_in;
    Some(Ipv4Addr::from(u32::from_be(unsafe {
        (*address).sin_addr.s_addr
    })))
}
