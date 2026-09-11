//! IPv4 prefixes, enough of them for a denylist: parse, contain, overlap.

use std::net::Ipv4Addr;
use std::str::FromStr;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Cidr {
    /// Already masked to `prefix_len`, so equal prefixes compare equal.
    pub network: Ipv4Addr,
    pub prefix_len: u8,
}

impl Cidr {
    pub fn new(address: Ipv4Addr, prefix_len: u8) -> Self {
        let mask = mask_of(prefix_len);
        Cidr {
            network: Ipv4Addr::from(u32::from(address) & mask),
            prefix_len,
        }
    }

    pub fn contains(&self, address: Ipv4Addr) -> bool {
        let mask = mask_of(self.prefix_len);
        u32::from(address) & mask == u32::from(self.network)
    }

    /// True when either prefix is a subset of the other. nft rejects
    /// overlapping elements in an interval set, and a declared CIDR that
    /// overlaps the baseline is a policy the ruleset cannot honor.
    pub fn overlaps(&self, other: &Cidr) -> bool {
        let shorter = self.prefix_len.min(other.prefix_len);
        let mask = mask_of(shorter);
        u32::from(self.network) & mask == u32::from(other.network) & mask
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix_len)
    }
}

impl FromStr for Cidr {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, String> {
        let (address, prefix_len) = raw
            .split_once('/')
            .ok_or_else(|| format!("{raw:?} is not A.B.C.D/N"))?;
        let address: Ipv4Addr = address.parse().map_err(|e| format!("{raw:?}: {e}"))?;
        let prefix_len: u8 = prefix_len.parse().map_err(|e| format!("{raw:?}: {e}"))?;
        if prefix_len > 32 {
            return Err(format!("{raw:?}: prefix out of range"));
        }
        Ok(Cidr::new(address, prefix_len))
    }
}

fn mask_of(prefix_len: u8) -> u32 {
    match prefix_len {
        0 => 0,
        n => u32::MAX << (32 - n as u32),
    }
}
