//! Task egress declarations and the policy JSON carried into a VMM.
//!
//! Host names reach public destinations. Declared CIDRs also reach private
//! destinations, subject to `excluded`. See the crate README for prefix lists.

/// Destinations no host name reaches, before the VMM's own subnets are folded
/// in. Every entry is a prefix IANA's IPv4 Special-Purpose Address Registry
/// marks as not globally reachable, plus multicast and Azure's host-node
/// virtual IP. The crate README names each one.
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

/// A validated, normalized policy: names lowercased and deduplicated, prefixes
/// canonical and disjoint.
#[derive(Debug)]
pub struct Policy {
    pub egress: Mode,
    pub allow_all: bool,
    pub allowed_names: Vec<AllowedName>,
    pub declared_cidrs: Vec<ipnetwork::Ipv4Network>,
    pub ttl_floor_secs: u32,
    pub ttl_cap_secs: u32,
}

#[derive(serde::Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    None,
    Public,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllowedName {
    Exact(String),
    /// `*.pypi.org`, held as `pypi.org`: every name beneath it at any depth,
    /// and not the name itself.
    Subdomains(String),
}

impl std::fmt::Display for AllowedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllowedName::Exact(name) => f.write_str(name),
            AllowedName::Subdomains(base) => write!(f, "*.{base}"),
        }
    }
}

/// The wire shape, kept separate from `Policy` so that nothing downstream can
/// read a field that has not been through `parse`.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Document {
    egress: Mode,
    #[serde(default)]
    allow_all: bool,
    #[serde(default)]
    allowed_names: Vec<String>,
    #[serde(default)]
    declared_cidrs: Vec<String>,
    #[serde(default = "default_ttl_floor")]
    ttl_floor_secs: u32,
    #[serde(default = "default_ttl_cap")]
    ttl_cap_secs: u32,
}

fn default_ttl_floor() -> u32 {
    90
}

fn default_ttl_cap() -> u32 {
    3600
}

/// An nft set element with `timeout 0s` never expires, and a cap far above any
/// record a public nameserver serves is that same permanent element with a
/// longer fuse. One day is well clear of real TTLs.
const TTL_CAP_MAX: u32 = 86_400;

pub fn load(path: &std::path::Path) -> anyhow::Result<Policy> {
    let content =
        std::fs::read(path).map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;

    parse(&content).map_err(|e| anyhow::anyhow!("{}: {e:#}", path.display()))
}

pub fn parse(content: &[u8]) -> anyhow::Result<Policy> {
    let document: Document = serde_json::from_slice(content)?;

    // Lists may remain dormant under `egress: none`, but `allowAll`
    // contradicts that mode.
    if document.egress == Mode::None && document.allow_all {
        anyhow::bail!(
            "allowAll is set with egress \"none\", which allows nothing; \
             set egress to \"public\" or clear allowAll"
        );
    }
    if document.ttl_floor_secs == 0 {
        anyhow::bail!("ttlFloorSecs is 0, which would hold a resolved address forever");
    }
    if document.ttl_floor_secs > document.ttl_cap_secs {
        anyhow::bail!(
            "ttlFloorSecs {} is above ttlCapSecs {}",
            document.ttl_floor_secs,
            document.ttl_cap_secs
        );
    }
    if document.ttl_cap_secs > TTL_CAP_MAX {
        anyhow::bail!(
            "ttlCapSecs {} is above the maximum {TTL_CAP_MAX}",
            document.ttl_cap_secs
        );
    }

    Ok(Policy {
        egress: document.egress,
        allow_all: document.allow_all,
        allowed_names: hosts("allowedNames", &document.allowed_names)?,
        declared_cidrs: cidrs("declaredCidrs", &document.declared_cidrs)?,
        ttl_floor_secs: document.ttl_floor_secs,
        ttl_cap_secs: document.ttl_cap_secs,
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

/// Validate and normalize host names wherever they are declared, naming
/// `field` in every refusal. Lowercases, and drops repeats after the first.
///
/// `allowedNames` gates the resolver on the question name, so a name that no
/// query can carry is a rule that can never fire. Each refusal below is a name
/// that would otherwise sit in the policy looking like access.
pub fn hosts(field: &str, raw: &[String]) -> anyhow::Result<Vec<AllowedName>> {
    let mut names: Vec<AllowedName> = Vec::with_capacity(raw.len());

    for name in raw {
        let allowed = check_name(field, name, &name.to_ascii_lowercase())?;

        if !names.contains(&allowed) {
            names.push(allowed);
        }
    }
    Ok(names)
}

/// Validate IPv4 CIDRs, naming `field` in errors. Returned prefixes are sorted
/// and disjoint for nft interval sets. Runtime exclusions may leave holes
/// within declared prefixes.
pub fn cidrs(field: &str, raw: &[String]) -> anyhow::Result<Vec<ipnetwork::Ipv4Network>> {
    let mut prefixes = raw
        .iter()
        .map(|cidr| check_cidr(field, cidr))
        .collect::<anyhow::Result<Vec<_>>>()?;
    prefixes.sort_by_key(|prefix| (u32::from(prefix.network()), prefix.prefix()));

    // Two prefixes nest or are disjoint. Sorted, one within any kept prefix is
    // within the last kept.
    let mut kept: Vec<ipnetwork::Ipv4Network> = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if !kept
            .last()
            .is_some_and(|last| last.contains(prefix.network()))
        {
            kept.push(prefix);
        }
    }
    Ok(kept)
}

/// The policy document a launcher writes: public destinations, reachable only
/// by the names in `names`, and every destination within `cidrs`. Empty lists
/// admit no name and no range.
pub fn public_policy(names: &[AllowedName], cidrs: &[ipnetwork::Ipv4Network]) -> Vec<u8> {
    let names: Vec<String> = names.iter().map(AllowedName::to_string).collect();

    let mut document = serde_json::json!({
        "egress": "public",
        "allowedNames": names,
    });
    if !cidrs.is_empty() {
        let cidrs: Vec<String> = cidrs.iter().map(ToString::to_string).collect();
        document["declaredCidrs"] = cidrs.into();
    }
    serde_json::to_vec(&document).expect("a map of strings always serializes")
}

fn check_name(field: &str, original: &str, name: &str) -> anyhow::Result<AllowedName> {
    if name.is_empty() {
        anyhow::bail!("{field} contains an empty name");
    }
    if !name.is_ascii() {
        anyhow::bail!(
            "{field} {original:?} is not ASCII; write the punycode (xn--) form, \
             which is what a query carries"
        );
    }
    let (base, wildcard) = match name.strip_prefix("*.") {
        Some(base) => (base, true),
        None => (name, false),
    };
    if base.contains('*') {
        anyhow::bail!(
            "{field} {original:?} has a wildcard other than a leading \"*.\", \
             the only form supported"
        );
    }
    if name.ends_with('.') {
        anyhow::bail!("{field} {original:?} has a trailing dot; write the name without it");
    }
    if name.len() > 253 {
        anyhow::bail!(
            "{field} {original:?} is {} bytes; a name is at most 253",
            name.len()
        );
    }

    let labels: Vec<&str> = base.split('.').collect();
    if !wildcard && labels.len() < 2 {
        anyhow::bail!("{field} {original:?} is a single label, which cannot resolve publicly");
    }
    for label in &labels {
        if label.is_empty() {
            anyhow::bail!("{field} {original:?} has an empty label");
        }
        if label.len() > 63 {
            anyhow::bail!("{field} {original:?} has a label longer than 63 bytes: {label:?}");
        }
        if !label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            anyhow::bail!("{field} {original:?} has a label outside [a-z0-9-]: {label:?}");
        }
        if label.starts_with('-') || label.ends_with('-') {
            anyhow::bail!("{field} {original:?} has a label bounded by a hyphen: {label:?}");
        }
    }

    // RFC 1123's rule against an all-numeric top label, which here catches
    // somebody writing an address where a name goes.
    let top = labels.last().expect("split yields at least one label");
    if top.bytes().all(|b| b.is_ascii_digit()) {
        anyhow::bail!(
            "{field} {original:?} ends in an all-numeric label; \
             {field} takes names, not addresses"
        );
    }

    if !wildcard {
        return Ok(AllowedName::Exact(base.to_string()));
    }
    check_wildcard_base(field, original, base)?;
    Ok(AllowedName::Subdomains(base.to_string()))
}

/// A wildcard over a public suffix grants names controlled by unrelated
/// registrants. Check only the base: `*.amazonaws.com` is valid even though
/// `s3.amazonaws.com` beneath it is a suffix.
fn check_wildcard_base(field: &str, original: &str, base: &str) -> anyhow::Result<()> {
    let suffix = psl::suffix(base.as_bytes()).expect("every validated name has a suffix");
    if suffix.as_bytes() != base.as_bytes() {
        return Ok(());
    }
    let source = match suffix.typ() {
        Some(psl::Type::Icann) => "in the Public Suffix List's ICANN section",
        Some(psl::Type::Private) => "in the Public Suffix List's private section",
        None => {
            "by the Public Suffix List's default rule, which makes every top-level label a suffix"
        }
    };
    anyhow::bail!(
        "{field} {original:?} is a wildcard over the public suffix {base:?}, {source}; \
         names beneath a public suffix belong to unrelated registrants"
    );
}

/// One prefix, written as an IPv4 address and a decimal prefix length. Bare
/// addresses and netmasks are refused rather than read, so that what was
/// written is the range granted.
fn check_cidr(field: &str, raw: &str) -> anyhow::Result<ipnetwork::Ipv4Network> {
    if raw.contains(':') {
        anyhow::bail!("{field} {raw:?} is IPv6; only IPv4 ranges are supported");
    }
    let Some((address, length)) = raw.split_once('/') else {
        anyhow::bail!(
            "{field} {raw:?} has no prefix length; write an address and a prefix length, \
             such as 10.0.0.0/8, or 10.0.0.1/32 for one address"
        );
    };
    let Ok(address) = address.parse::<std::net::Ipv4Addr>() else {
        anyhow::bail!("{field} {raw:?} does not begin with an IPv4 address");
    };
    let Some(length) = Some(length)
        .filter(|length| !length.is_empty() && length.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|length| length.parse::<u8>().ok())
        .filter(|length| *length <= 32)
    else {
        anyhow::bail!("{field} {raw:?} has a prefix length other than a number from 0 to 32");
    };
    let cidr = ipnetwork::Ipv4Network::new(address, length).expect("a prefix length of at most 32");

    // `10.1.2.3/8` may mean one host or the whole range. Masking it would
    // grant the range.
    if cidr.ip() != cidr.network() {
        anyhow::bail!(
            "{field} {raw:?} sets bits below its prefix length; write {}/{length}, \
             or {address}/32 for one address",
            cidr.network()
        );
    }
    Ok(cidr)
}

#[cfg(test)]
mod tests {
    /// A VMM on podman's bridge, given as the interface address `getifaddrs`
    /// returns, plus one sitting on public space so that a refusal can name a
    /// VMM subnet rather than a constant.
    fn vmm_subnets() -> Vec<ipnetwork::Ipv4Network> {
        ["10.89.0.4/24", "172.104.9.9/16"]
            .iter()
            .map(|raw| raw.parse().expect("test constant"))
            .collect()
    }

    #[test]
    fn refusals() {
        let mut table = String::new();

        for (label, document) in cases() {
            table.push_str(&format!("# {label}\n{document}\n"));

            match super::parse(document.as_bytes()) {
                Ok(policy) => table.push_str(&format!("accepted: {}\n\n", describe(&policy))),
                Err(error) => table.push_str(&format!("{error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(table);
    }

    /// What an accepted policy normalized to, so the snapshot shows the
    /// lowercasing, deduplication and prefix ordering as well as the refusals.
    fn describe(policy: &super::Policy) -> String {
        let declared: Vec<String> = policy
            .declared_cidrs
            .iter()
            .map(ToString::to_string)
            .collect();

        format!(
            "egress={:?} allowAll={} allowedNames={:?} declaredCidrs=[{}] ttl={}..{}",
            policy.egress,
            policy.allow_all,
            policy.allowed_names,
            declared.join(", "),
            policy.ttl_floor_secs,
            policy.ttl_cap_secs,
        )
    }

    /// What a launcher writes is what the VMM reads back: the same names and
    /// prefixes, and every other field at its default.
    #[test]
    fn public_policy_round_trips() {
        let names = super::hosts(
            "dev.estuary.egress-hosts",
            &[
                "pypi.org".to_string(),
                "*.ACMEco.example".to_string(),
                "api.acmeco.example".to_string(),
            ],
        )
        .expect("valid names");
        let document = super::public_policy(&names, &[]);

        insta::assert_snapshot!(
            String::from_utf8(document.clone()).expect("JSON is UTF-8"),
            @r#"{"allowedNames":["pypi.org","*.acmeco.example","api.acmeco.example"],"egress":"public"}"#
        );

        let policy = super::parse(&document).expect("the parser accepts a launcher's policy");
        assert_eq!(policy.egress, super::Mode::Public);
        assert!(!policy.allow_all);
        assert_eq!(policy.allowed_names, names);
        assert!(policy.declared_cidrs.is_empty());
        assert_eq!((policy.ttl_floor_secs, policy.ttl_cap_secs), (90, 3600));

        let empty = super::parse(&super::public_policy(&[], &[])).expect("an empty list parses");
        assert_eq!(empty.egress, super::Mode::Public);
        assert!(empty.allowed_names.is_empty());

        let cidrs = super::cidrs(
            "task egress.cidrs",
            &["192.168.4.0/24".to_string(), "10.0.0.0/8".to_string()],
        )
        .expect("valid prefixes");
        let document = super::public_policy(&names[..1], &cidrs);

        insta::assert_snapshot!(
            String::from_utf8(document.clone()).expect("JSON is UTF-8"),
            @r#"{"allowedNames":["pypi.org"],"declaredCidrs":["10.0.0.0/8","192.168.4.0/24"],"egress":"public"}"#
        );
        let policy = super::parse(&document).expect("the parser accepts a launcher's prefixes");
        assert_eq!(policy.declared_cidrs, cidrs);
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
    fn cidrs_name_the_field_they_came_from() {
        let rows: Vec<String> = [
            "fd00::/8",
            "::ffff:10.0.0.0/104",
            "10.0.0.1",
            "10.0.0.0/33",
            "10.0.0.0/",
            "10.0.0.0/+8",
            "10.0.0.0/255.0.0.0",
            "10/8",
            "010.0.0.0/8",
            " 10.0.0.0/8",
            "10.0.0.0/8/8",
            "10.1.2.3/8",
            "",
        ]
        .into_iter()
        .map(|cidr| {
            let error = super::cidrs("task egress.cidrs", &[cidr.to_string()])
                .expect_err("each is refused");
            format!("{error:#}")
        })
        .collect();

        insta::assert_snapshot!(rows.join("\n"), @r#"
        task egress.cidrs "fd00::/8" is IPv6; only IPv4 ranges are supported
        task egress.cidrs "::ffff:10.0.0.0/104" is IPv6; only IPv4 ranges are supported
        task egress.cidrs "10.0.0.1" has no prefix length; write an address and a prefix length, such as 10.0.0.0/8, or 10.0.0.1/32 for one address
        task egress.cidrs "10.0.0.0/33" has a prefix length other than a number from 0 to 32
        task egress.cidrs "10.0.0.0/" has a prefix length other than a number from 0 to 32
        task egress.cidrs "10.0.0.0/+8" has a prefix length other than a number from 0 to 32
        task egress.cidrs "10.0.0.0/255.0.0.0" has a prefix length other than a number from 0 to 32
        task egress.cidrs "10/8" does not begin with an IPv4 address
        task egress.cidrs "010.0.0.0/8" does not begin with an IPv4 address
        task egress.cidrs " 10.0.0.0/8" does not begin with an IPv4 address
        task egress.cidrs "10.0.0.0/8/8" has a prefix length other than a number from 0 to 32
        task egress.cidrs "10.1.2.3/8" sets bits below its prefix length; write 10.0.0.0/8, or 10.1.2.3/32 for one address
        task egress.cidrs "" has no prefix length; write an address and a prefix length, such as 10.0.0.0/8, or 10.0.0.1/32 for one address
        "#);
    }

    #[test]
    fn cidrs_normalize() {
        let rows: Vec<String> = [
            vec!["10.0.0.0/8"],
            vec!["10.1.2.3/32", "10.1.2.3/32"],
            vec!["10.2.0.0/16", "10.0.0.0/8", "10.0.0.0/8", "10.2.3.0/24"],
            vec!["10.0.0.0/9", "10.128.0.0/9"],
            vec!["192.168.1.0/24", "172.16.0.0/12", "10.0.0.0/8"],
            vec!["169.254.169.254/32", "127.0.0.1/32", "224.0.0.0/4"],
            vec!["10.89.0.0/24", "0.0.0.0/0", "93.184.216.0/24"],
            vec![],
        ]
        .into_iter()
        .map(|raw| {
            let raw: Vec<String> = raw.into_iter().map(str::to_string).collect();
            let kept = super::cidrs("task egress.cidrs", &raw).expect("each is accepted");
            let kept: Vec<String> = kept.iter().map(ToString::to_string).collect();
            format!("{raw:?} => {kept:?}")
        })
        .collect();

        insta::assert_snapshot!(rows.join("\n"), @r#"
        ["10.0.0.0/8"] => ["10.0.0.0/8"]
        ["10.1.2.3/32", "10.1.2.3/32"] => ["10.1.2.3/32"]
        ["10.2.0.0/16", "10.0.0.0/8", "10.0.0.0/8", "10.2.3.0/24"] => ["10.0.0.0/8"]
        ["10.0.0.0/9", "10.128.0.0/9"] => ["10.0.0.0/9", "10.128.0.0/9"]
        ["192.168.1.0/24", "172.16.0.0/12", "10.0.0.0/8"] => ["10.0.0.0/8", "172.16.0.0/12", "192.168.1.0/24"]
        ["169.254.169.254/32", "127.0.0.1/32", "224.0.0.0/4"] => ["127.0.0.1/32", "169.254.169.254/32", "224.0.0.0/4"]
        ["10.89.0.0/24", "0.0.0.0/0", "93.184.216.0/24"] => ["0.0.0.0/0"]
        [] => []
        "#);
    }

    #[test]
    fn hosts_name_the_field_they_came_from() {
        let rows: Vec<String> = ["*.com", "api.*.acmeco.example", "", "169.254.169.254"]
            .into_iter()
            .map(|name| {
                let error = super::hosts("dev.estuary.egress-hosts", &[name.to_string()])
                    .expect_err("each is refused");
                format!("{error:#}")
            })
            .collect();

        insta::assert_snapshot!(rows.join("\n"), @r#"
        dev.estuary.egress-hosts "*.com" is a wildcard over the public suffix "com", in the Public Suffix List's ICANN section; names beneath a public suffix belong to unrelated registrants
        dev.estuary.egress-hosts "api.*.acmeco.example" has a wildcard other than a leading "*.", the only form supported
        dev.estuary.egress-hosts contains an empty name
        dev.estuary.egress-hosts "169.254.169.254" ends in an all-numeric label; dev.estuary.egress-hosts takes names, not addresses
        "#);
    }

    #[test]
    fn load_reads_a_file() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("policy.json");
        std::fs::write(&path, br#"{"egress":"public"}"#).expect("write");

        let policy = super::load(&path).expect("a minimal policy loads");
        assert_eq!(policy.egress, super::Mode::Public);
        assert_eq!(policy.ttl_floor_secs, 90);
        assert_eq!(policy.ttl_cap_secs, 3600);

        let missing = directory.path().join("absent.json");
        let error = super::load(&missing).expect_err("a missing file is an error");
        assert!(
            error.to_string().starts_with("reading "),
            "unexpected error: {error:#}"
        );
    }

    /// One line of JSON per case so the snapshot states its own inputs.
    fn cases() -> Vec<(&'static str, &'static str)> {
        vec![
            ("unknown field", r#"{"egress":"public","allowNone":true}"#),
            ("unknown egress mode", r#"{"egress":"partial"}"#),
            ("egress absent", r#"{"allowAll":true}"#),
            ("malformed json", r#"{"egress":"public",}"#),
            (
                "allowAll under egress none",
                r#"{"egress":"none","allowAll":true}"#,
            ),
            (
                "allowedNames and declaredCidrs are dormant under egress none, not refused",
                r#"{"egress":"none","allowedNames":["pypi.org"],"declaredCidrs":["93.184.216.0/24"]}"#,
            ),
            (
                "ttlFloorSecs zero",
                r#"{"egress":"public","ttlFloorSecs":0}"#,
            ),
            (
                "ttlFloorSecs above ttlCapSecs",
                r#"{"egress":"public","ttlFloorSecs":7200,"ttlCapSecs":3600}"#,
            ),
            (
                "ttlCapSecs above the maximum",
                r#"{"egress":"public","ttlCapSecs":604800}"#,
            ),
            (
                "allowedNames non-ASCII",
                r#"{"egress":"public","allowedNames":["café.example"]}"#,
            ),
            (
                "allowedNames trailing dot",
                r#"{"egress":"public","allowedNames":["pypi.org."]}"#,
            ),
            (
                "allowedNames single label",
                r#"{"egress":"public","allowedNames":["localhost"]}"#,
            ),
            (
                "allowedNames all-numeric top label",
                r#"{"egress":"public","allowedNames":["169.254.169.254"]}"#,
            ),
            (
                "allowedNames empty label",
                r#"{"egress":"public","allowedNames":["files..acmeco.example"]}"#,
            ),
            (
                "allowedNames hyphen-bounded label",
                r#"{"egress":"public","allowedNames":["-files.acmeco.example"]}"#,
            ),
            (
                "allowedNames underscore",
                r#"{"egress":"public","allowedNames":["build_cache.acmeco.example"]}"#,
            ),
            (
                "allowedNames empty string",
                r#"{"egress":"public","allowedNames":[""]}"#,
            ),
            (
                "allowedNames lowercased and deduplicated",
                r#"{"egress":"public","allowedNames":["PyPI.ORG","pypi.org","artifacts.acmeco.example"]}"#,
            ),
            (
                "allowedNames a name and its wildcard are distinct grants",
                r#"{"egress":"public","allowedNames":["acmeco.example","*.acmeco.example"]}"#,
            ),
            (
                "allowedNames wildcard lowercased and deduplicated",
                r#"{"egress":"public","allowedNames":["*.ACMEco.example","*.acmeco.example"]}"#,
            ),
            (
                "allowedNames wildcard alone",
                r#"{"egress":"public","allowedNames":["*"]}"#,
            ),
            (
                "allowedNames wildcard with no base",
                r#"{"egress":"public","allowedNames":["*."]}"#,
            ),
            (
                "allowedNames two wildcard labels",
                r#"{"egress":"public","allowedNames":["*.*.acmeco.example"]}"#,
            ),
            (
                "allowedNames doubled star",
                r#"{"egress":"public","allowedNames":["**.acmeco.example"]}"#,
            ),
            (
                "allowedNames star without its dot",
                r#"{"egress":"public","allowedNames":["*acmeco.example"]}"#,
            ),
            (
                "allowedNames star inside a label",
                r#"{"egress":"public","allowedNames":["api*.acmeco.example"]}"#,
            ),
            (
                "allowedNames wildcard label in the middle",
                r#"{"egress":"public","allowedNames":["api.*.acmeco.example"]}"#,
            ),
            (
                "allowedNames wildcard label last",
                r#"{"egress":"public","allowedNames":["acmeco.*"]}"#,
            ),
            (
                "allowedNames wildcard with a trailing dot",
                r#"{"egress":"public","allowedNames":["*.acmeco.example."]}"#,
            ),
            (
                "allowedNames wildcard over an invalid label",
                r#"{"egress":"public","allowedNames":["*.-acmeco.example"]}"#,
            ),
            (
                "allowedNames wildcard over a top-level domain",
                r#"{"egress":"public","allowedNames":["*.com"]}"#,
            ),
            (
                "allowedNames wildcard over an ICANN second-level suffix",
                r#"{"egress":"public","allowedNames":["*.co.uk"]}"#,
            ),
            (
                "allowedNames wildcard over a private suffix",
                r#"{"egress":"public","allowedNames":["*.github.io"]}"#,
            ),
            (
                "allowedNames wildcard over a domain registered beneath a private suffix",
                r#"{"egress":"public","allowedNames":["*.acmeco.github.io"]}"#,
            ),
            (
                "allowedNames wildcard over a domain holding private suffixes: only the base is checked",
                r#"{"egress":"public","allowedNames":["*.amazonaws.com"]}"#,
            ),
            (
                "allowedNames wildcard over a suffix made by a wildcard rule",
                r#"{"egress":"public","allowedNames":["*.acmeco.ck"]}"#,
            ),
            (
                "allowedNames wildcard over an exception to a wildcard rule",
                r#"{"egress":"public","allowedNames":["*.www.ck"]}"#,
            ),
            (
                "allowedNames wildcard over a domain holding a wildcard rule: only the base is checked",
                r#"{"egress":"public","allowedNames":["*.kawasaki.jp"]}"#,
            ),
            (
                "allowedNames wildcard over an exception beneath that wildcard rule",
                r#"{"egress":"public","allowedNames":["*.city.kawasaki.jp"]}"#,
            ),
            (
                "allowedNames wildcard over an internationalized suffix, in punycode",
                r#"{"egress":"public","allowedNames":["*.xn--55qx5d.cn"]}"#,
            ),
            (
                "allowedNames wildcard over an unlisted top-level domain",
                r#"{"egress":"public","allowedNames":["*.example"]}"#,
            ),
            (
                "allowedNames wildcard beneath an unlisted top-level domain",
                r#"{"egress":"public","allowedNames":["*.acmeco.example"]}"#,
            ),
            (
                "allowedNames exact names are not checked against the list",
                r#"{"egress":"public","allowedNames":["s3.amazonaws.com","github.io","co.uk"]}"#,
            ),
            (
                "declaredCidrs unparsable",
                r#"{"egress":"public","declaredCidrs":["not-a-cidr"]}"#,
            ),
            (
                "declaredCidrs with host bits set",
                r#"{"egress":"public","declaredCidrs":["93.184.216.5/24"]}"#,
            ),
            (
                "declaredCidrs IPv6",
                r#"{"egress":"public","declaredCidrs":["fd00::/8"]}"#,
            ),
            (
                "declaredCidrs in the former shape, with ports",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs overlapping and repeated, kept disjoint",
                r#"{"egress":"public","declaredCidrs":["93.184.216.128/25","93.184.216.0/24","93.184.216.0/24"]}"#,
            ),
            // The baseline's exclusions are the ruleset's to apply, not a
            // reason to refuse a declaration that overlaps them.
            (
                "declaredCidrs private (RFC1918)",
                r#"{"egress":"public","declaredCidrs":["10.2.0.0/16"]}"#,
            ),
            (
                "declaredCidrs cloud metadata and loopback",
                r#"{"egress":"public","declaredCidrs":["169.254.169.254/32","127.0.0.1/32"]}"#,
            ),
            (
                "declaredCidrs every address",
                r#"{"egress":"public","declaredCidrs":["10.89.0.0/24","0.0.0.0/0"]}"#,
            ),
            (
                "declaredCidrs beside allowAll",
                r#"{"egress":"public","allowAll":true,"declaredCidrs":["10.2.0.0/16"]}"#,
            ),
        ]
    }
}
