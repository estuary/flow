use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Egress a task declares: the host names and address ranges its connector
/// may reach.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Egress {
    /// # Host names the connector may reach.
    /// These add to the hosts the connector and its image already declare.
    /// When both `hosts` and `cidrs` are empty, the connector may reach no
    /// external destination at all. `api.acmeco.example` matches
    /// only itself. `*.acmeco.example` matches every name beneath
    /// `acmeco.example`, at any depth, but not `acmeco.example` itself: list
    /// both to reach both. A wildcard may not cover a public suffix, such as
    /// `*.com`. Names are case-insensitive, have no trailing dot, and are
    /// written in punycode (`xn--`) where they are not ASCII.
    ///
    /// A permitted name is reachable on any port, but only at public
    /// addresses, or at addresses within `cidrs`. Loopback, link-local and
    /// other addresses which are not destinations, and TCP port 25, are never
    /// reachable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hosts: Vec<String>,
    /// # IPv4 address ranges the connector may reach.
    /// Each is an address and a prefix length, such as `10.0.0.0/8` or
    /// `10.1.2.3/32`, with no bits set below the prefix. Every address within
    /// a range is reachable on any TCP or UDP port, private addresses
    /// included, both directly and by any name which resolves only into
    /// declared ranges. Ranges may overlap, and `0.0.0.0/0` covers every
    /// address. Loopback, link-local and other addresses which are not
    /// destinations, the machine running the connector, and TCP port 25,
    /// remain unreachable.
    ///
    /// Only private and local data planes accept address ranges.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cidrs: Vec<String>,
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

/// Validate and normalize host names wherever they are declared, naming
/// `field` in every refusal. Lowercases, and drops repeats after the first.
///
/// A permitted name gates a resolver on the question name, so a name that no
/// query can carry is a rule that can never fire. Each refusal below is a name
/// that would otherwise sit in a declaration looking like access.
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
mod test {
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
    fn hosts_refuse_and_normalize() {
        let mut table = String::new();

        for (label, names) in host_cases() {
            let names: Vec<String> = names.iter().map(ToString::to_string).collect();
            table.push_str(&format!("# {label}\n{names:?}\n"));

            match super::hosts("egress.hosts", &names) {
                Ok(names) => table.push_str(&format!("accepted: {names:?}\n\n")),
                Err(error) => table.push_str(&format!("{error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(table);
    }

    fn host_cases() -> Vec<(&'static str, &'static [&'static str])> {
        vec![
            ("non-ASCII", &["café.example"]),
            ("trailing dot", &["pypi.org."]),
            ("single label", &["localhost"]),
            ("all-numeric top label", &["169.254.169.254"]),
            ("empty label", &["files..acmeco.example"]),
            ("hyphen-bounded label", &["-files.acmeco.example"]),
            ("underscore", &["build_cache.acmeco.example"]),
            ("empty string", &[""]),
            (
                "lowercased and deduplicated",
                &["PyPI.ORG", "pypi.org", "artifacts.acmeco.example"],
            ),
            (
                "a name and its wildcard are distinct grants",
                &["acmeco.example", "*.acmeco.example"],
            ),
            (
                "wildcard lowercased and deduplicated",
                &["*.ACMEco.example", "*.acmeco.example"],
            ),
            ("wildcard alone", &["*"]),
            ("wildcard with no base", &["*."]),
            ("two wildcard labels", &["*.*.acmeco.example"]),
            ("doubled star", &["**.acmeco.example"]),
            ("star without its dot", &["*acmeco.example"]),
            ("star inside a label", &["api*.acmeco.example"]),
            ("wildcard label in the middle", &["api.*.acmeco.example"]),
            ("wildcard label last", &["acmeco.*"]),
            ("wildcard with a trailing dot", &["*.acmeco.example."]),
            ("wildcard over an invalid label", &["*.-acmeco.example"]),
            ("wildcard over a top-level domain", &["*.com"]),
            ("wildcard over an ICANN second-level suffix", &["*.co.uk"]),
            ("wildcard over a private suffix", &["*.github.io"]),
            (
                "wildcard over a domain registered beneath a private suffix",
                &["*.acmeco.github.io"],
            ),
            (
                "wildcard over a domain holding private suffixes: only the base is checked",
                &["*.amazonaws.com"],
            ),
            (
                "wildcard over a suffix made by a wildcard rule",
                &["*.acmeco.ck"],
            ),
            (
                "wildcard over an exception to a wildcard rule",
                &["*.www.ck"],
            ),
            (
                "wildcard over a domain holding a wildcard rule: only the base is checked",
                &["*.kawasaki.jp"],
            ),
            (
                "wildcard over an exception beneath that wildcard rule",
                &["*.city.kawasaki.jp"],
            ),
            (
                "wildcard over an internationalized suffix, in punycode",
                &["*.xn--55qx5d.cn"],
            ),
            ("wildcard over an unlisted top-level domain", &["*.example"]),
            (
                "wildcard beneath an unlisted top-level domain",
                &["*.acmeco.example"],
            ),
            (
                "exact names are not checked against the list",
                &["s3.amazonaws.com", "github.io", "co.uk"],
            ),
        ]
    }

    use serde_json::json;

    #[test]
    fn egress_round_trips_across_task_models() {
        fn round_trip<M: serde::Serialize + serde::de::DeserializeOwned>(
            mut fixture: serde_json::Value,
            egress: Option<serde_json::Value>,
            declared: impl Fn(&M) -> &Option<super::Egress>,
        ) -> String {
            if let Some(egress) = egress {
                fixture["egress"] = egress;
            }
            let model: M = match serde_json::from_value(fixture) {
                Ok(model) => model,
                Err(err) => return format!("refused: {err}"),
            };
            let serialized = serde_json::to_value(&model).unwrap();
            format!(
                "{:?} serialized={}",
                declared(&model),
                serialized
                    .get("egress")
                    .map_or("<omitted>".to_string(), ToString::to_string)
            )
        }

        let capture = json!({
            "endpoint": {"connector": {"image": "an/image", "config": {}}},
            "bindings": [],
        });
        let derivation = json!({
            "using": {"connector": {"image": "an/image", "config": {}}},
            "transforms": [],
        });
        let materialization = json!({
            "endpoint": {"connector": {"image": "an/image", "config": {}}},
            "bindings": [],
        });

        let mut rows = Vec::new();
        for egress in [
            None,
            Some(json!(null)),
            Some(json!({"hosts": []})),
            Some(json!({"hosts": ["api.acmeco.example", "*.svc.acmeco.example"]})),
            Some(json!({"cidrs": ["10.0.0.0/8", "0.0.0.0/0"]})),
            Some(json!({"hosts": ["api.acmeco.example"], "cidrs": ["10.1.2.3/32"]})),
            Some(json!({"hosts": [], "cidrs": []})),
            Some(json!({})),
            Some(json!({"hosts": [], "ports": [443]})),
            Some(json!({"hosts": "api.acmeco.example"})),
            Some(json!({"cidrs": "10.0.0.0/8"})),
        ] {
            let label = egress
                .as_ref()
                .map_or("<omitted>".to_string(), ToString::to_string);
            rows.push(format!(
                "capture {label} => {}",
                round_trip::<crate::CaptureDef>(capture.clone(), egress.clone(), |m| &m.egress)
            ));
            rows.push(format!(
                "derivation {label} => {}",
                round_trip::<crate::Derivation>(derivation.clone(), egress.clone(), |m| &m.egress)
            ));
            rows.push(format!(
                "materialization {label} => {}",
                round_trip::<crate::MaterializationDef>(materialization.clone(), egress, |m| {
                    &m.egress
                })
            ));
        }

        insta::assert_snapshot!(rows.join("\n"), @r#"
        capture <omitted> => None serialized=<omitted>
        derivation <omitted> => None serialized=<omitted>
        materialization <omitted> => None serialized=<omitted>
        capture null => None serialized=<omitted>
        derivation null => None serialized=<omitted>
        materialization null => None serialized=<omitted>
        capture {"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        derivation {"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        materialization {"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        capture {"hosts":["api.acmeco.example","*.svc.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example", "*.svc.acmeco.example"], cidrs: [] }) serialized={"hosts":["api.acmeco.example","*.svc.acmeco.example"]}
        derivation {"hosts":["api.acmeco.example","*.svc.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example", "*.svc.acmeco.example"], cidrs: [] }) serialized={"hosts":["api.acmeco.example","*.svc.acmeco.example"]}
        materialization {"hosts":["api.acmeco.example","*.svc.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example", "*.svc.acmeco.example"], cidrs: [] }) serialized={"hosts":["api.acmeco.example","*.svc.acmeco.example"]}
        capture {"cidrs":["10.0.0.0/8","0.0.0.0/0"]} => Some(Egress { hosts: [], cidrs: ["10.0.0.0/8", "0.0.0.0/0"] }) serialized={"cidrs":["10.0.0.0/8","0.0.0.0/0"]}
        derivation {"cidrs":["10.0.0.0/8","0.0.0.0/0"]} => Some(Egress { hosts: [], cidrs: ["10.0.0.0/8", "0.0.0.0/0"] }) serialized={"cidrs":["10.0.0.0/8","0.0.0.0/0"]}
        materialization {"cidrs":["10.0.0.0/8","0.0.0.0/0"]} => Some(Egress { hosts: [], cidrs: ["10.0.0.0/8", "0.0.0.0/0"] }) serialized={"cidrs":["10.0.0.0/8","0.0.0.0/0"]}
        capture {"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example"], cidrs: ["10.1.2.3/32"] }) serialized={"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]}
        derivation {"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example"], cidrs: ["10.1.2.3/32"] }) serialized={"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]}
        materialization {"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]} => Some(Egress { hosts: ["api.acmeco.example"], cidrs: ["10.1.2.3/32"] }) serialized={"cidrs":["10.1.2.3/32"],"hosts":["api.acmeco.example"]}
        capture {"cidrs":[],"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        derivation {"cidrs":[],"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        materialization {"cidrs":[],"hosts":[]} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        capture {} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        derivation {} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        materialization {} => Some(Egress { hosts: [], cidrs: [] }) serialized={}
        capture {"hosts":[],"ports":[443]} => refused: unknown field `ports`, expected `hosts` or `cidrs`
        derivation {"hosts":[],"ports":[443]} => refused: unknown field `ports`, expected `hosts` or `cidrs`
        materialization {"hosts":[],"ports":[443]} => refused: unknown field `ports`, expected `hosts` or `cidrs`
        capture {"hosts":"api.acmeco.example"} => refused: invalid type: string "api.acmeco.example", expected a sequence
        derivation {"hosts":"api.acmeco.example"} => refused: invalid type: string "api.acmeco.example", expected a sequence
        materialization {"hosts":"api.acmeco.example"} => refused: invalid type: string "api.acmeco.example", expected a sequence
        capture {"cidrs":"10.0.0.0/8"} => refused: invalid type: string "10.0.0.0/8", expected a sequence
        derivation {"cidrs":"10.0.0.0/8"} => refused: invalid type: string "10.0.0.0/8", expected a sequence
        materialization {"cidrs":"10.0.0.0/8"} => refused: invalid type: string "10.0.0.0/8", expected a sequence
        "#);
    }
}
