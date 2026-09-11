//! The policy JSON of CONTRACTS.md, and the baseline denylist that is not in
//! it: the baseline belongs to the ruleset, not to the tenant.

use crate::cidr::Cidr;

/// The prefixes no policy can reach, before the helper's own subnets are added
/// to them. PLAN experiment 6 writes the same list down.
pub const BASELINE: &[&str] = &[
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "224.0.0.0/4",
    "240.0.0.0/4",
];

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Policy {
    pub egress: Mode,
    #[serde(default)]
    pub allow_all: bool,
    #[serde(default)]
    pub declared_cidrs: Vec<Declared>,
    #[serde(default)]
    pub connections_per_minute: Option<u32>,
    #[serde(default)]
    pub distinct_destinations_per_minute: Option<u32>,
    #[serde(default = "default_ttl_floor")]
    pub ttl_floor_secs: u32,
    #[serde(default = "default_ttl_cap")]
    pub ttl_cap_secs: u32,
}

#[derive(serde::Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    None,
    Public,
}

#[derive(serde::Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Declared {
    pub cidr: String,
    pub ports: Vec<u16>,
}

fn default_ttl_floor() -> u32 {
    90
}

fn default_ttl_cap() -> u32 {
    3600
}

pub fn load(path: &std::path::Path) -> anyhow::Result<Policy> {
    let content =
        std::fs::read(path).map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
    let policy: Policy = serde_json::from_slice(&content)
        .map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;

    if policy.ttl_floor_secs > policy.ttl_cap_secs {
        anyhow::bail!(
            "ttlFloorSecs {} is above ttlCapSecs {}",
            policy.ttl_floor_secs,
            policy.ttl_cap_secs
        );
    }
    Ok(policy)
}

impl Policy {
    pub fn clamp_ttl(&self, ttl: u32) -> u32 {
        ttl.clamp(self.ttl_floor_secs, self.ttl_cap_secs)
    }
}

/// The baseline as the ruleset and the resolver both see it: the constant
/// prefixes plus whatever subnets the helper's own interfaces carry, which on
/// `flow-connectors` means its eth0 /24 and always means the tap /30.
pub fn denied_prefixes(helper_subnets: &[Cidr]) -> anyhow::Result<Vec<Cidr>> {
    let mut denied = Vec::new();
    for raw in BASELINE {
        denied.push(raw.parse::<Cidr>().map_err(|e| anyhow::anyhow!("{e}"))?);
    }
    denied.extend_from_slice(helper_subnets);
    Ok(denied)
}
