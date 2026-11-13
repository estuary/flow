//! Compile a policy into a complete nftables ruleset without touching the kernel.

use crate::net::{GUEST_IP, TAP, UPLINK, VMM_IP};
use crate::policy::Policy;

/// The most addresses the resolver may hold at once. Kept beside the rendered
/// text because the resolver's own cap has to agree with it.
pub const RESOLVED_SIZE: usize = 1024;

/// `vmm_subnets` are the VMM's own interface subnets, folded into the
/// baseline. `run` reads them from `getifaddrs`; `print-ruleset` takes them
/// from `--vmm-subnet`.
///
/// The table is declared and then deleted before it is defined: `nft delete`
/// on a table that was never created is an error, and a second apply must
/// replace the first rather than append to it.
///
/// Sets:
/// - `baseline` and `excluded` take `auto-merge` because the VMM's bridge
///   sits inside 10.0.0.0/8 and the tap inside 192.0.2.0/24, and nft rejects
///   overlapping interval elements.
/// - `excluded` is the part of the baseline a declared CIDR does not lift.
///   Without one it is within `baseline` and goes unrendered.
/// - `resolved` is filled by the resolver, one element per answered address,
///   with the clamped TTL as its timeout.
/// - `declared` needs no merge: `models::egress::cidrs` leaves the prefixes
///   disjoint.
///
/// `forward`:
/// - "replies" precedes the private-address exclusions, so replies to
///   permitted egress and ingress pass. Ingress works independently of the
///   guest's egress policy.
/// - "tcp-udp-only" kills ICMP, which is why it sits after "replies": ICMP
///   errors belonging to a tracked connection are still useful.
///
/// `input`:
/// - "anti-spoof" is ahead of "replies", and that ordering is the point: a
///   guest that guessed the resolver's source port and a query id could
///   otherwise forge an upstream answer, and conntrack would call it
///   established.
/// - "tap-dns" is the only packet the guest may address to the VMM, and the
///   one local exception to public-only destinations.
///
/// `output` has policy accept: the VMM's own uplink traffic (the resolver's
/// queries upstream) must keep working. Only the tap side is constrained, and
/// there to replies, so nothing in the VMM can open a connection to the guest.
pub fn render(
    policy: &Policy,
    vmm_subnets: &[ipnetwork::Ipv4Network],
    ingress_ports: &[u16],
) -> String {
    let allowed_names = policy.allowed_names.len();
    let declared_cidrs = policy.declared_cidrs.len();
    let ttl_floor_secs = policy.ttl_floor_secs;
    let ttl_cap_secs = policy.ttl_cap_secs;
    let baseline = join(&crate::policy::baseline(vmm_subnets));

    let mut excluded_set = String::new();
    let mut declared_set = String::new();
    let mut baseline_drops = "\t\tip daddr @baseline counter drop comment \"baseline\"\n";
    let mut declared_accept = "";

    if !policy.declared_cidrs.is_empty() {
        let excluded = join(&crate::policy::excluded(vmm_subnets));
        let declared = join(&policy.declared_cidrs);

        excluded_set = format!(
            r#"	set excluded {{
		type ipv4_addr
		flags interval
		auto-merge
		elements = {{ {excluded} }}
	}}

"#
        );
        declared_set = format!(
            r#"	set declared {{
		type ipv4_addr
		flags interval
		elements = {{ {declared} }}
	}}

"#
        );
        baseline_drops = r#"		ip daddr @excluded counter drop comment "excluded"
		ip daddr @baseline ip daddr != @declared counter drop comment "baseline"
"#;
        declared_accept = "\t\tip daddr @declared counter accept comment \"declared\"\n";
    }

    let mut ingress_accept = String::new();
    let mut ingress_dnat = String::new();

    if !ingress_ports.is_empty() {
        let ports = ingress_ports
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(", ");

        ingress_accept = format!(
            r#"		iifname "{UPLINK}" oifname "{TAP}" ip daddr {GUEST_IP} ct status dnat tcp dport {{ {ports} }} counter accept comment "ingress"
"#
        );
        ingress_dnat = format!(
            r#"
	chain prerouting {{
		type nat hook prerouting priority dstnat; policy accept;
		iifname "{UPLINK}" fib daddr type local tcp dport {{ {ports} }} counter dnat ip to {GUEST_IP} comment "ingress"
	}}
"#
        );
    }

    format!(
        r#"# flow-connector-vmm egress ruleset: allowedNames={allowed_names} declaredCidrs={declared_cidrs} ttlFloorSecs={ttl_floor_secs} ttlCapSecs={ttl_cap_secs}
table inet flow_egress {{}}
delete table inet flow_egress

table inet flow_egress {{
	set baseline {{
		type ipv4_addr
		flags interval
		auto-merge
		elements = {{ {baseline} }}
	}}

{excluded_set}	set resolved {{
		type ipv4_addr
		flags timeout
		size {RESOLVED_SIZE}
	}}

{declared_set}	chain forward {{
		type filter hook forward priority filter; policy drop;
		iifname "{TAP}" ip saddr != {GUEST_IP} counter drop comment "anti-spoof"
		ct state established,related counter accept comment "replies"
		meta nfproto ipv6 counter drop comment "no-ipv6"
{ingress_accept}		meta l4proto != {{ tcp, udp }} counter drop comment "tcp-udp-only"
{baseline_drops}		tcp dport 25 counter drop comment "smtp"
		ct state new counter jump egress_accept comment "egress"
		counter comment "forward-drop"
	}}

	chain egress_accept {{
		ip daddr @resolved counter accept comment "resolved"
{declared_accept}	}}

	chain input {{
		type filter hook input priority filter; policy drop;
		iif lo counter accept comment "loopback"
		iifname "{TAP}" ip saddr != {GUEST_IP} counter drop comment "anti-spoof"
		ct state established,related counter accept comment "replies"
		iifname "{TAP}" ip saddr {GUEST_IP} ip daddr {VMM_IP} udp dport 53 counter accept comment "tap-dns"
		counter comment "input-drop"
	}}

	chain output {{
		type filter hook output priority filter; policy accept;
		oifname "{TAP}" ct state != established counter drop comment "no-inbound"
	}}

	chain postrouting {{
		type nat hook postrouting priority srcnat; policy accept;
		ip saddr {GUEST_IP} oifname "{UPLINK}" counter masquerade comment "masquerade"
	}}
{ingress_dnat}}}
"#
    )
}

fn join(prefixes: &[ipnetwork::Ipv4Network]) -> String {
    prefixes
        .iter()
        .map(ipnetwork::Ipv4Network::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use crate::policy::tests::policy;

    /// Interface addresses with host bits set, overlapping the baseline to
    /// exercise canonicalization and `auto-merge`.
    fn vmm_subnets() -> Vec<ipnetwork::Ipv4Network> {
        ["10.89.0.4/24", "192.0.2.1/30"]
            .iter()
            .map(|raw| raw.parse().expect("test constant"))
            .collect()
    }

    fn render(policy: &crate::policy::Policy) -> String {
        super::render(policy, &vmm_subnets(), &[])
    }

    fn empty() -> crate::policy::Policy {
        policy(&[], &[], None)
    }

    /// Private, public and single-address prefixes, one of which covers the
    /// VMM's own bridge.
    fn declared() -> crate::policy::Policy {
        policy(
            &["pypi.org"],
            &["192.168.4.7/32", "10.0.0.0/8", "93.184.216.0/24"],
            None,
        )
    }

    fn every_address() -> crate::policy::Policy {
        policy(&[], &["0.0.0.0/0"], None)
    }

    fn allowed_names() -> crate::policy::Policy {
        policy(
            &[
                "PyPI.ORG",
                "pypi.org",
                "files.pythonhosted.org",
                "artifacts.acmeco.example",
            ],
            &[],
            Some((120, 1800)),
        )
    }

    /// The declared policy's whole ruleset, then how each other policy's
    /// differs from it: the lines it lacks, and those it has instead.
    #[test]
    fn rulesets() {
        let base = render(&declared());
        let mut table = format!("# declared\n{base}");
        for (name, other) in [
            ("empty", render(&empty())),
            ("every address", render(&every_address())),
            ("allowed names", render(&allowed_names())),
            (
                "empty, with ingress",
                super::render(&empty(), &vmm_subnets(), &[8080, 9090]),
            ),
        ] {
            table.push_str(&format!("\n# {name}\n"));
            for line in base
                .lines()
                .filter(|line| !other.lines().any(|o| o == *line))
            {
                table.push_str(&format!("- {line}\n"));
            }
            for line in other
                .lines()
                .filter(|line| !base.lines().any(|b| b == *line))
            {
                table.push_str(&format!("+ {line}\n"));
            }
        }
        insta::assert_snapshot!(table);
    }

    /// Every accept must be accounted for, and entry to egress_accept must
    /// follow the baseline drop.
    #[test]
    fn every_accept_is_accounted_for() {
        const NAMED: &[&str] = &[
            "forward:replies",
            "egress_accept:resolved",
            "input:loopback",
            "input:replies",
            "input:tap-dns",
        ];
        const DECLARED: &[&str] = &[
            "forward:replies",
            "egress_accept:resolved",
            "egress_accept:declared",
            "input:loopback",
            "input:replies",
            "input:tap-dns",
        ];
        let expected: [(&str, crate::policy::Policy, &[&str]); 4] = [
            ("empty", empty(), NAMED),
            ("declared", declared(), DECLARED),
            ("every address", every_address(), DECLARED),
            ("allowed names", allowed_names(), NAMED),
        ];

        for (document, policy, accepts) in &expected {
            let text = render(policy);

            let actual = accepted(&text);
            let actual: Vec<&str> = actual.iter().map(String::as_str).collect();
            assert_eq!(actual.as_slice(), *accepts, "accepts of {document}");

            let forward = chain(&text, "forward");
            let baseline = forward
                .iter()
                .position(|rule| rule.contains("@baseline") && rule.contains(" drop "))
                .expect("every forward chain drops the baseline");
            // A declared CIDR lifts the baseline drop where it meets it, so
            // what no CIDR lifts is dropped outright, and before it.
            let excluded = forward
                .iter()
                .position(|rule| rule.contains("@excluded") && rule.contains(" drop "));
            if text.contains("set declared") {
                let excluded = excluded.expect("a declared CIDR's chain drops the exclusions");
                assert!(excluded < baseline, "{document}");
                assert!(!forward[excluded].contains("@declared"), "{document}");
            } else {
                assert_eq!(excluded, None, "{document}");
                assert!(!forward[baseline].contains("@declared"), "{document}");
            }

            for (index, rule) in forward.iter().enumerate() {
                assert!(
                    !rule.contains("egress_accept") || index > baseline,
                    "{rule:?} reaches egress_accept above the baseline drop of {document}"
                );
            }
            // Nothing outside `forward` may grant a verdict into that chain.
            let forward_jumps = forward
                .iter()
                .filter(|rule| rule.contains("jump egress_accept"))
                .count();
            assert_eq!(
                text.matches("jump egress_accept").count(),
                forward_jumps,
                "a jump into egress_accept from outside forward, in {document}"
            );
            assert!(
                !text.contains("goto egress_accept"),
                "a goto into egress_accept in {document}"
            );
        }
    }

    /// `chain:comment` for every rule with an `accept` verdict, in the order
    /// the chains are rendered. A chain declaration carries `policy accept`
    /// and is not a rule.
    fn accepted(text: &str) -> Vec<String> {
        let mut accepts = Vec::new();

        for name in ["forward", "egress_accept", "input", "output", "postrouting"] {
            for rule in chain(text, name) {
                if rule.contains(" accept") {
                    accepts.push(format!("{name}:{}", comment(&rule)));
                }
            }
        }
        accepts
    }

    /// The rules of one chain, in order, without its `type ... hook ...` line.
    /// Empty when the chain is not in the ruleset at all.
    fn chain(text: &str, name: &str) -> Vec<String> {
        let opening = format!("chain {name} {{");
        let mut rules = Vec::new();
        let mut inside = false;

        for line in text.lines() {
            let line = line.trim();
            if line == opening {
                inside = true;
            } else if inside && line == "}" {
                break;
            } else if inside && !line.starts_with("type ") {
                rules.push(line.to_string());
            }
        }
        rules
    }

    fn comment(rule: &str) -> String {
        let Some((_, rest)) = rule.split_once("comment \"") else {
            panic!("rule without a comment: {rule:?}");
        };
        rest.trim_end_matches('"').to_string()
    }
}
