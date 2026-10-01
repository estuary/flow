//! The host names a connector may reach, the policy JSON the launcher writes
//! into a VMM's `/init/policy.json`, and the baseline exclusions that are not
//! in it: the baseline belongs to the ruleset, not to the tenant.
//!
//! Everything a connector may reach is a public unicast destination. Every
//! refusal in this module is a policy the ruleset could not have honored, so
//! it is refused here rather than rendered into text that means something
//! narrower than it reads. See the crate README for the exclusion list and
//! the rationale behind each prefix.

use ipnetwork::Ipv4Network;

/// Destinations no policy reaches, before the VMM's own subnets are folded in.
/// Every entry is a prefix IANA's IPv4 Special-Purpose Address Registry marks
/// as not globally reachable, plus multicast, which is not a unicast
/// destination at all. The crate README names each one.
const BASELINE: &[&str] = &[
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
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

/// A validated, normalized policy: names lowercased and deduplicated, prefixes
/// canonical, ports sorted.
#[derive(Debug)]
pub struct Policy {
    pub egress: Mode,
    pub allow_all: bool,
    pub allowed_names: Vec<AllowedName>,
    pub declared_cidrs: Vec<Declared>,
    pub connections_per_minute: Option<u32>,
    pub distinct_destinations_per_minute: Option<u32>,
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

#[derive(Debug)]
pub struct Declared {
    pub cidr: Ipv4Network,
    pub ports: Vec<u16>,
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
    declared_cidrs: Vec<DocumentDeclared>,
    #[serde(default)]
    connections_per_minute: Option<u32>,
    #[serde(default)]
    distinct_destinations_per_minute: Option<u32>,
    #[serde(default = "default_ttl_floor")]
    ttl_floor_secs: u32,
    #[serde(default = "default_ttl_cap")]
    ttl_cap_secs: u32,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DocumentDeclared {
    cidr: String,
    ports: Vec<u16>,
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

/// The ruleset drops tcp/25 ahead of every accept, so a policy that declares
/// it means something narrower than it reads.
const SMTP_PORT: u16 = 25;

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
    for (field, limit) in [
        ("connectionsPerMinute", document.connections_per_minute),
        (
            "distinctDestinationsPerMinute",
            document.distinct_destinations_per_minute,
        ),
    ] {
        if limit == Some(0) {
            anyhow::bail!("{field} is 0, which drops every connection; use null for no limit");
        }
    }

    Ok(Policy {
        egress: document.egress,
        allow_all: document.allow_all,
        allowed_names: hosts("allowedNames", &document.allowed_names)?,
        declared_cidrs: normalize_declared(&document.declared_cidrs)?,
        connections_per_minute: document.connections_per_minute,
        distinct_destinations_per_minute: document.distinct_destinations_per_minute,
        ttl_floor_secs: document.ttl_floor_secs,
        ttl_cap_secs: document.ttl_cap_secs,
    })
}

/// The baseline as the ruleset sees it: the constant prefixes plus whatever
/// subnets the VMM's own interfaces carry. Under podman the latter is the
/// container's bridge, which sits inside `10.0.0.0/8` and is why the rendered
/// set needs `auto-merge`.
pub fn baseline(vmm_subnets: &[Ipv4Network]) -> Vec<Ipv4Network> {
    let mut baseline: Vec<Ipv4Network> = BASELINE
        .iter()
        .map(|raw| raw.parse().expect("BASELINE entries are constants"))
        .collect();

    for subnet in vmm_subnets {
        let subnet = canonical(*subnet);
        if !baseline.contains(&subnet) {
            baseline.push(subnet);
        }
    }
    baseline
}

/// Reject grants the baseline drop would prevent the ruleset from honoring.
pub fn check_declared(policy: &Policy, baseline: &[Ipv4Network]) -> anyhow::Result<()> {
    for declared in &policy.declared_cidrs {
        let Some(hit) = baseline.iter().find(|entry| entry.overlaps(declared.cidr)) else {
            continue;
        };
        anyhow::bail!(
            "declaredCidrs {} overlaps the baseline exclusion {hit}, which the ruleset drops \
             ahead of every accept",
            declared.cidr
        );
    }
    Ok(())
}

/// `ipnetwork` keeps whatever host bits it parsed and prints them back, which
/// is what `getifaddrs` hands us for an interface address. nft wants the
/// network address of the prefix.
fn canonical(network: Ipv4Network) -> Ipv4Network {
    Ipv4Network::new(network.network(), network.prefix())
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

/// The policy document a launcher writes: public destinations, reachable only
/// by the names in `names`. An empty list admits no name at all.
pub fn public_policy(names: &[AllowedName]) -> Vec<u8> {
    let names: Vec<String> = names.iter().map(AllowedName::to_string).collect();

    serde_json::to_vec(&serde_json::json!({
        "egress": "public",
        "allowedNames": names,
    }))
    .expect("a map of strings always serializes")
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

fn normalize_declared(raw: &[DocumentDeclared]) -> anyhow::Result<Vec<Declared>> {
    let mut declared: Vec<Declared> = Vec::with_capacity(raw.len());

    // Merging runs to completion before anything is compared: a repeated
    // prefix contributes ports that the overlap check below has to see, so an
    // entry validated as it arrived would be judged against a port set still
    // being assembled, and the same three entries would pass or fail on their
    // order alone.
    for entry in raw {
        let cidr: Ipv4Network = entry
            .cidr
            .parse()
            .map_err(|e| anyhow::anyhow!("declaredCidrs {:?}: {e}", entry.cidr))?;

        // Masking host bits would widen the grant; retaining them makes nft
        // reject the element.
        if cidr.ip() != cidr.network() {
            anyhow::bail!(
                "declaredCidrs {:?} sets bits below its prefix; write {}/{}",
                entry.cidr,
                cidr.network(),
                cidr.prefix()
            );
        }
        if entry.ports.is_empty() {
            anyhow::bail!("declaredCidrs {cidr} lists no ports");
        }
        if entry.ports.contains(&0) {
            anyhow::bail!("declaredCidrs {cidr} declares port 0, which is not a destination");
        }
        if entry.ports.contains(&SMTP_PORT) {
            anyhow::bail!(
                "declaredCidrs {cidr} declares port {SMTP_PORT}, which the ruleset drops ahead \
                 of every accept"
            );
        }

        // Repeated prefixes merge because nft interval sets reject duplicate
        // elements.
        match declared.iter_mut().find(|existing| existing.cidr == cidr) {
            Some(existing) => existing.ports.extend(&entry.ports),
            None => declared.push(Declared {
                cidr,
                ports: entry.ports.clone(),
            }),
        }
    }
    for entry in &mut declared {
        entry.ports.sort_unstable();
        entry.ports.dedup();
    }

    // Distinct prefixes that overlap are only a conflict where their ports
    // meet, because the set is keyed on the pair.
    for (index, entry) in declared.iter().enumerate() {
        let Some(other) = declared[..index].iter().find(|other| {
            other.cidr.overlaps(entry.cidr) && other.ports.iter().any(|p| entry.ports.contains(p))
        }) else {
            continue;
        };
        anyhow::bail!(
            "declaredCidrs {} overlaps {} on a shared port; nft rejects overlapping \
             elements of an interval set",
            entry.cidr,
            other.cidr
        );
    }
    Ok(declared)
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
        let baseline = super::baseline(&vmm_subnets());
        let mut table = String::new();

        for (label, document) in cases() {
            table.push_str(&format!("# {label}\n{document}\n"));

            let outcome = super::parse(document.as_bytes())
                .and_then(|policy| super::check_declared(&policy, &baseline).map(|()| policy));

            match outcome {
                Ok(policy) => table.push_str(&format!("accepted: {}\n\n", describe(&policy))),
                Err(error) => table.push_str(&format!("{error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(table);
    }

    /// What an accepted policy normalized to, so the snapshot shows the
    /// lowercasing, deduplication and port ordering as well as the refusals.
    fn describe(policy: &super::Policy) -> String {
        let declared: Vec<String> = policy
            .declared_cidrs
            .iter()
            .map(|declared| format!("{} ports {:?}", declared.cidr, declared.ports))
            .collect();

        format!(
            "egress={:?} allowAll={} allowedNames={:?} declaredCidrs=[{}] \
             connectionsPerMinute={:?} distinctDestinationsPerMinute={:?} ttl={}..{}",
            policy.egress,
            policy.allow_all,
            policy.allowed_names,
            declared.join("; "),
            policy.connections_per_minute,
            policy.distinct_destinations_per_minute,
            policy.ttl_floor_secs,
            policy.ttl_cap_secs,
        )
    }

    /// What a launcher writes is what the VMM reads back: the same names, and
    /// every other field at its default.
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
        let document = super::public_policy(&names);

        insta::assert_snapshot!(
            String::from_utf8(document.clone()).expect("JSON is UTF-8"),
            @r#"{"allowedNames":["pypi.org","*.acmeco.example","api.acmeco.example"],"egress":"public"}"#
        );

        let policy = super::parse(&document).expect("the parser accepts a launcher's policy");
        assert_eq!(policy.egress, super::Mode::Public);
        assert!(!policy.allow_all);
        assert_eq!(policy.allowed_names, names);
        assert!(policy.declared_cidrs.is_empty());
        assert_eq!(policy.connections_per_minute, None);
        assert_eq!(policy.distinct_destinations_per_minute, None);
        assert_eq!((policy.ttl_floor_secs, policy.ttl_cap_secs), (90, 3600));

        let empty = super::parse(&super::public_policy(&[])).expect("an empty list parses");
        assert_eq!(empty.egress, super::Mode::Public);
        assert!(empty.allowed_names.is_empty());
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
                r#"{"egress":"none","allowedNames":["pypi.org"],"declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443]}]}"#,
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
                "connectionsPerMinute zero",
                r#"{"egress":"public","connectionsPerMinute":0}"#,
            ),
            (
                "distinctDestinationsPerMinute zero",
                r#"{"egress":"public","distinctDestinationsPerMinute":0}"#,
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
                r#"{"egress":"public","declaredCidrs":[{"cidr":"not-a-cidr","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs with host bits set",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.5/24","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs with no ports",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[]}]}"#,
            ),
            (
                "declaredCidrs port 0",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[0]}]}"#,
            ),
            (
                "declaredCidrs port 25",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443,25]}]}"#,
            ),
            (
                "declaredCidrs overlapping on a shared port",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443]},{"cidr":"93.184.216.128/25","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs overlapping on disjoint ports",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443]},{"cidr":"93.184.216.128/25","ports":[8443]}]}"#,
            ),
            (
                "declaredCidrs repeating a prefix",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443]},{"cidr":"93.184.216.0/24","ports":[8443,443]}]}"#,
            ),
            // The merged port set has to be complete before any prefix is
            // compared, so these two differ only in where the repeat sits and
            // must be refused identically.
            (
                "declaredCidrs repeating a prefix into an overlap, repeat last",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443]},{"cidr":"93.184.216.0/25","ports":[8443]},{"cidr":"93.184.216.0/24","ports":[8443]}]}"#,
            ),
            (
                "declaredCidrs repeating a prefix into an overlap, repeat first",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"93.184.216.0/24","ports":[443]},{"cidr":"93.184.216.0/24","ports":[8443]},{"cidr":"93.184.216.0/25","ports":[8443]}]}"#,
            ),
            (
                "declaredCidrs private (RFC1918)",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"10.2.0.0/16","ports":[5432]}]}"#,
            ),
            (
                "declaredCidrs cloud metadata",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"169.254.169.254/32","ports":[80]}]}"#,
            ),
            (
                "declaredCidrs loopback",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"127.0.0.1/32","ports":[8080]}]}"#,
            ),
            (
                "declaredCidrs shared address space (CGNAT)",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"100.100.0.0/16","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs IETF protocol assignments",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"192.0.0.170/32","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs the tap subnet (TEST-NET-1)",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"192.0.2.0/30","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs benchmarking",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"198.18.0.0/15","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs documentation (TEST-NET-3)",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"203.0.113.0/24","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs multicast",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"224.0.0.1/32","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs reserved",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"240.0.0.0/4","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs the VMM's own bridge",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"10.89.0.0/24","ports":[443]}]}"#,
            ),
            (
                "declaredCidrs a VMM subnet on public space",
                r#"{"egress":"public","declaredCidrs":[{"cidr":"172.104.9.0/24","ports":[443]}]}"#,
            ),
            (
                "allowAll does not rescue a private destination",
                r#"{"egress":"public","allowAll":true,"declaredCidrs":[{"cidr":"10.2.0.0/16","ports":[5432]}]}"#,
            ),
        ]
    }
}
