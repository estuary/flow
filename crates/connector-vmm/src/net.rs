//! The addressing the VMM, its guest and its ruleset are pinned to.
//!
//! Pinned rather than configurable: the guest init is handed the same two
//! addresses, the enclosing `192.0.2.0/24` is a baseline exclusion in its own
//! right, and the ruleset, the resolver and the guest's `resolv.conf` all have
//! to agree on them or the guest has no working network.

use std::net::Ipv4Addr;

/// The point-to-point tap between the VMM and its guest, a `/30`.
pub const TAP: &str = "tap0";
pub const VMM_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
pub const GUEST_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 2);

/// The interface the guest is masqueraded out of: podman's primary interface
/// in the VMM container.
pub const UPLINK: &str = "eth0";
