//! The egress policy one VMM enforces, as its launcher passes it, and the
//! destinations no policy reaches. The crate README names each prefix.

/// Destinations no host name reaches, before the VMM's own subnets are folded
/// in. Every entry is a prefix IANA's IPv4 Special-Purpose Address Registry
/// marks as not globally reachable, plus multicast and Azure's host-node
/// virtual IP.
const BASELINE: &[&str] = &[
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "168.63.129.16/32",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.0.2.0/24",
    "192.88.99.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
];

/// The part of `BASELINE` no declared CIDR reaches either. Each is not a
/// destination a network routes to, or holds one that must stay unreachable:
/// link-local holds every cloud's instance metadata service, and
/// `192.0.2.0/24` holds the tap.
const EXCLUDED: &[&str] = &[
    "0.0.0.0/8",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "192.0.2.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
];

const TTL_FLOOR_SECS: u32 = 90;
const TTL_CAP_SECS: u32 = 3600;

/// An nft set element with `timeout 0s` never expires, and a cap far above any
/// record a public nameserver serves is that same permanent element with a
/// longer fuse. One day is well clear of real TTLs.
const TTL_CAP_MAX: u32 = 86_400;

#[derive(clap::Args, Debug)]
#[group(id = "policy")]
pub struct Args {
    /// A host name the guest may reach, bare or `*.`-prefixed. Repeatable.
    #[arg(long, value_name = "NAME", value_parser = egress_host)]
    pub egress_host: Vec<String>,

    /// An IPv4 range the guest may reach, private addresses included.
    /// Repeatable.
    #[arg(long, value_name = "CIDR", value_parser = egress_cidr)]
    pub egress_cidr: Vec<String>,

    // The TTL bounds parse only with the `test-controls` feature. Without it
    // they keep their defaults.
    /// The shortest time a resolved address stays reachable.
    #[cfg_attr(
        feature = "test-controls",
        arg(long, value_name = "SECS", default_value_t = TTL_FLOOR_SECS)
    )]
    #[cfg_attr(not(feature = "test-controls"), arg(skip = TTL_FLOOR_SECS))]
    pub ttl_floor_secs: u32,

    /// The longest time a resolved address stays reachable.
    #[cfg_attr(
        feature = "test-controls",
        arg(long, value_name = "SECS", default_value_t = TTL_CAP_SECS)
    )]
    #[cfg_attr(not(feature = "test-controls"), arg(skip = TTL_CAP_SECS))]
    pub ttl_cap_secs: u32,
}

/// A validated, normalized policy: names lowercased and deduplicated, prefixes
/// canonical and disjoint.
#[derive(Debug)]
pub struct Policy {
    pub allowed_names: Vec<models::egress::AllowedName>,
    pub declared_cidrs: Vec<ipnetwork::Ipv4Network>,
    pub ttl_floor_secs: u32,
    pub ttl_cap_secs: u32,
}

fn egress_host(value: &str) -> Result<String, String> {
    models::egress::hosts("--egress-host", &[value.to_string()])
        .map(|_| value.to_string())
        .map_err(|error| format!("{error:#}"))
}

fn egress_cidr(value: &str) -> Result<String, String> {
    models::egress::cidrs("--egress-cidr", &[value.to_string()])
        .map(|_| value.to_string())
        .map_err(|error| format!("{error:#}"))
}

pub fn check(args: &Args) -> anyhow::Result<Policy> {
    let Args {
        egress_host,
        egress_cidr,
        ttl_floor_secs,
        ttl_cap_secs,
    } = args;

    if *ttl_floor_secs == 0 {
        anyhow::bail!("--ttl-floor-secs is 0, which would hold a resolved address forever");
    }
    if ttl_floor_secs > ttl_cap_secs {
        anyhow::bail!("--ttl-floor-secs {ttl_floor_secs} is above --ttl-cap-secs {ttl_cap_secs}");
    }
    if *ttl_cap_secs > TTL_CAP_MAX {
        anyhow::bail!("--ttl-cap-secs {ttl_cap_secs} is above the maximum {TTL_CAP_MAX}");
    }

    Ok(Policy {
        allowed_names: models::egress::hosts("--egress-host", egress_host)?,
        declared_cidrs: models::egress::cidrs("--egress-cidr", egress_cidr)?,
        ttl_floor_secs: *ttl_floor_secs,
        ttl_cap_secs: *ttl_cap_secs,
    })
}

/// The baseline as the ruleset sees it: the constant prefixes plus whatever
/// subnets the VMM's own interfaces carry. Under podman the latter is the
/// container's bridge, which sits inside `10.0.0.0/8` and is why the rendered
/// set needs `auto-merge`.
pub fn baseline(vmm_subnets: &[ipnetwork::Ipv4Network]) -> Vec<ipnetwork::Ipv4Network> {
    with_subnets(BASELINE, vmm_subnets)
}

/// The part of the baseline no policy reaches, declared CIDRs included: the
/// constant prefixes plus the VMM's own subnets, as for `baseline`.
pub fn excluded(vmm_subnets: &[ipnetwork::Ipv4Network]) -> Vec<ipnetwork::Ipv4Network> {
    with_subnets(EXCLUDED, vmm_subnets)
}

fn with_subnets(
    constants: &[&str],
    vmm_subnets: &[ipnetwork::Ipv4Network],
) -> Vec<ipnetwork::Ipv4Network> {
    let mut prefixes: Vec<ipnetwork::Ipv4Network> = constants
        .iter()
        .map(|raw| raw.parse().expect("prefix lists are constants"))
        .collect();

    for subnet in vmm_subnets {
        let subnet = canonical(*subnet);
        if !prefixes.contains(&subnet) {
            prefixes.push(subnet);
        }
    }
    prefixes
}

/// `ipnetwork` keeps whatever host bits it parsed and prints them back, which
/// is what `getifaddrs` hands us for an interface address. nft wants the
/// network address of the prefix.
fn canonical(network: ipnetwork::Ipv4Network) -> ipnetwork::Ipv4Network {
    ipnetwork::Ipv4Network::new(network.network(), network.prefix())
        .expect("a prefix length that already parsed is in range")
}

#[cfg(test)]
pub mod tests {
    pub fn policy(hosts: &[&str], cidrs: &[&str], ttl: Option<(u32, u32)>) -> super::Policy {
        let (ttl_floor_secs, ttl_cap_secs) =
            ttl.unwrap_or((super::TTL_FLOOR_SECS, super::TTL_CAP_SECS));

        super::check(&super::Args {
            egress_host: hosts.iter().map(ToString::to_string).collect(),
            egress_cidr: cidrs.iter().map(ToString::to_string).collect(),
            ttl_floor_secs,
            ttl_cap_secs,
        })
        .expect("fixture policy is valid")
    }

    /// A VMM on podman's bridge, given as the interface address `getifaddrs`
    /// returns, plus one sitting on public space.
    fn vmm_subnets() -> Vec<ipnetwork::Ipv4Network> {
        ["10.89.0.4/24", "172.104.9.9/16"]
            .iter()
            .map(|raw| raw.parse().expect("test constant"))
            .collect()
    }

    /// Every prefix no CIDR lifts is one no host name reaches either.
    #[test]
    fn excluded_is_within_the_baseline() {
        let baseline = super::baseline(&vmm_subnets());
        let excluded = super::excluded(&vmm_subnets());

        for prefix in &excluded {
            assert!(baseline.contains(prefix), "{prefix} is not in the baseline");
        }
        for subnet in vmm_subnets() {
            let subnet = super::canonical(subnet);
            assert!(excluded.contains(&subnet), "{subnet} is not excluded");
        }
    }

    #[test]
    fn check_normalizes_and_refuses() {
        let mut table = String::new();

        for (hosts, cidrs, ttl) in [
            (vec![], vec![], (90, 3600)),
            (
                vec!["PyPI.ORG", "pypi.org", "*.ACMEco.example"],
                vec!["192.168.4.0/24", "10.0.0.0/8", "10.2.0.0/16"],
                (90, 3600),
            ),
            (vec![], vec!["10.89.0.0/24", "0.0.0.0/0"], (3, 10)),
            (vec!["*.com"], vec![], (90, 3600)),
            (vec![], vec!["10.1.2.3/8"], (90, 3600)),
            (vec![], vec![], (0, 3600)),
            (vec![], vec![], (7200, 3600)),
            (vec![], vec![], (90, 604_800)),
        ] {
            let args = super::Args {
                egress_host: hosts.iter().map(ToString::to_string).collect(),
                egress_cidr: cidrs.iter().map(ToString::to_string).collect(),
                ttl_floor_secs: ttl.0,
                ttl_cap_secs: ttl.1,
            };
            table.push_str(&format!("{hosts:?} {cidrs:?} ttl={}..{}\n", ttl.0, ttl.1));

            match super::check(&args) {
                Ok(policy) => table.push_str(&format!(
                    "accepted: allowedNames={:?} declaredCidrs={:?} ttl={}..{}\n\n",
                    policy.allowed_names,
                    policy
                        .declared_cidrs
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                    policy.ttl_floor_secs,
                    policy.ttl_cap_secs,
                )),
                Err(error) => table.push_str(&format!("{error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(table);
    }
}
