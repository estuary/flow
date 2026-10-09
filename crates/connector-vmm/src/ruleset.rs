//! Compile a policy into a complete nftables ruleset without touching the kernel.

use egress::{Mode, Policy};
use std::fmt::Write;

use crate::net::{GUEST_IP, TAP, UPLINK, VMM_IP};

/// The most addresses the resolver may hold at once. Kept beside the rendered
/// text because the resolver's own cap has to agree with it.
pub const RESOLVED_SIZE: usize = 1024;

/// `vmm_subnets` are the VMM's own interface subnets, folded into the
/// baseline. `run` reads them from `getifaddrs`; `print-ruleset` takes them
/// from `--vmm-subnet`.
pub fn render(policy: &Policy, vmm_subnets: &[ipnetwork::Ipv4Network]) -> anyhow::Result<String> {
    let baseline = egress::baseline(vmm_subnets);
    egress::check_declared(policy, &baseline)?;

    let public = policy.egress == Mode::Public;
    let declared = declared_elements(policy);
    let mut out = String::new();

    writeln!(
        out,
        "# flow-connector-vmm egress ruleset: egress={} allowAll={} allowedNames={} \
         declaredCidrs={} connectionsPerMinute={:?} distinctDestinationsPerMinute={:?} \
         ttlFloorSecs={} ttlCapSecs={}",
        if public { "public" } else { "none" },
        policy.allow_all,
        policy.allowed_names.len(),
        policy.declared_cidrs.len(),
        policy.connections_per_minute,
        policy.distinct_destinations_per_minute,
        policy.ttl_floor_secs,
        policy.ttl_cap_secs,
    )?;
    // Declare-then-delete: `nft delete` on a table that was never created is
    // an error, and a second apply must replace the first rather than append
    // to it.
    out.push_str(
        "table inet flow_egress {}\n\
         delete table inet flow_egress\n\n\
         table inet flow_egress {\n",
    );

    // auto-merge because the VMM's bridge sits inside 10.0.0.0/8 and the tap
    // inside 192.0.2.0/24, and nft rejects overlapping interval elements.
    out.push_str(
        "\tset baseline {\n\
         \t\ttype ipv4_addr\n\
         \t\tflags interval\n\
         \t\tauto-merge\n",
    );
    writeln!(out, "\t\telements = {{ {} }}", join(&baseline))?;
    out.push_str("\t}\n\n");

    if public {
        // Filled by the resolver, one element per answered address, with the
        // clamped TTL as its timeout.
        writeln!(
            out,
            "\tset resolved {{\n\
             \t\ttype ipv4_addr\n\
             \t\tflags timeout\n\
             \t\tsize {RESOLVED_SIZE}\n\
             \t}}\n"
        )?;
    }
    if !declared.is_empty() {
        out.push_str(
            "\tset declared {\n\
             \t\ttype ipv4_addr . inet_service\n\
             \t\tflags interval\n",
        );
        writeln!(out, "\t\telements = {{ {} }}", declared.join(", "))?;
        out.push_str("\t}\n\n");
    }
    if let Some(limit) = fan_out_limit(policy) {
        // When the set is full, insertion fails and the packet falls through
        // to the chain's drop policy instead of jumping to egress_accept.
        writeln!(
            out,
            "\tset dests {{\n\
             \t\ttype ipv4_addr\n\
             \t\tflags dynamic, timeout\n\
             \t\ttimeout 1m\n\
             \t\tsize {limit}\n\
             \t}}\n"
        )?;
    }

    out.push_str("\tchain forward {\n");
    out.push_str("\t\ttype filter hook forward priority filter; policy drop;\n");
    writeln!(
        out,
        "\t\tiifname \"{TAP}\" ip saddr != {GUEST_IP} counter drop comment \"anti-spoof\""
    )?;
    // Ahead of the baseline drop because a reply, un-NAT'd back to the guest,
    // is addressed into 192.0.2.0/24, which the baseline itself covers. Only a
    // connection the guest was allowed to open can be in this state.
    out.push_str("\t\tct state established,related counter accept comment \"replies\"\n");
    out.push_str("\t\tmeta nfproto ipv6 counter drop comment \"no-ipv6\"\n");
    // Kills ICMP, which is why it sits after the established/related accept:
    // ICMP errors belonging to a tracked connection are still useful.
    out.push_str("\t\tmeta l4proto != { tcp, udp } counter drop comment \"tcp-udp-only\"\n");
    out.push_str("\t\tip daddr @baseline counter drop comment \"baseline\"\n");
    out.push_str("\t\ttcp dport 25 counter drop comment \"smtp\"\n");

    if public {
        if let Some(rate) = policy.connections_per_minute {
            // nft's `limit rate over` allows a default burst of 5 packets.
            writeln!(
                out,
                "\t\tct state new limit rate over {rate}/minute counter drop comment \"rate-limit\""
            )?;
        }
        match fan_out_limit(policy) {
            Some(_) => out.push_str(
                "\t\tct state new update @dests { ip daddr } counter jump egress_accept comment \"fan-out\"\n",
            ),
            None => out.push_str("\t\tct state new counter jump egress_accept comment \"egress\"\n"),
        }
        // A jump that returns without a verdict lands here, and on the policy.
    }
    out.push_str("\t\tcounter comment \"forward-drop\"\n");
    out.push_str("\t}\n\n");

    if public {
        out.push_str("\tchain egress_accept {\n");
        if policy.allow_all {
            out.push_str("\t\tcounter accept comment \"allow-all\"\n");
        }
        out.push_str("\t\tip daddr @resolved counter accept comment \"resolved\"\n");
        if !declared.is_empty() {
            out.push_str(
                "\t\tip daddr . tcp dport @declared counter accept comment \"declared\"\n",
            );
        }
        out.push_str("\t}\n\n");
    }

    out.push_str("\tchain input {\n");
    out.push_str("\t\ttype filter hook input priority filter; policy drop;\n");
    out.push_str("\t\tiif lo counter accept comment \"loopback\"\n");
    // Ahead of the conntrack accept, and that ordering is the point: a guest
    // that guessed the resolver's source port and a query id could otherwise
    // forge an upstream answer, and conntrack would call it established.
    writeln!(
        out,
        "\t\tiifname \"{TAP}\" ip saddr != {GUEST_IP} counter drop comment \"anti-spoof\""
    )?;
    out.push_str("\t\tct state established,related counter accept comment \"replies\"\n");
    if public {
        // The only packet the guest may address to the VMM, and the one local
        // exception to public-only destinations. With `egress: none` no
        // resolver runs, so the rule is absent and the query is dropped rather
        // than refused, which makes the guest pay its resolver's full retry
        // budget instead of failing fast into a retry loop.
        writeln!(
            out,
            "\t\tiifname \"{TAP}\" ip saddr {GUEST_IP} ip daddr {VMM_IP} udp dport 53 \
             counter accept comment \"tap-dns\""
        )?;
    }
    out.push_str("\t\tcounter comment \"input-drop\"\n");
    out.push_str("\t}\n\n");

    // Policy accept: the VMM's own uplink traffic (the resolver's queries
    // upstream) must keep working. Only the tap side is constrained, and there
    // to replies, so nothing in the VMM can open a connection to the guest.
    out.push_str("\tchain output {\n");
    out.push_str("\t\ttype filter hook output priority filter; policy accept;\n");
    writeln!(
        out,
        "\t\toifname \"{TAP}\" ct state != established counter drop comment \"no-inbound\""
    )?;
    out.push_str("\t}\n\n");

    out.push_str("\tchain postrouting {\n");
    out.push_str("\t\ttype nat hook postrouting priority srcnat; policy accept;\n");
    writeln!(
        out,
        "\t\tip saddr {GUEST_IP} oifname \"{UPLINK}\" counter masquerade comment \"masquerade\""
    )?;
    out.push_str("\t}\n");

    out.push_str("}\n");
    Ok(out)
}

fn fan_out_limit(policy: &Policy) -> Option<u32> {
    policy
        .distinct_destinations_per_minute
        .filter(|_| policy.egress == Mode::Public)
}

/// One `cidr . port` element per declared port. `egress: none` renders none of
/// them: the chain that would match them does not exist.
fn declared_elements(policy: &Policy) -> Vec<String> {
    if policy.egress != Mode::Public {
        return Vec::new();
    }
    policy
        .declared_cidrs
        .iter()
        .flat_map(|declared| {
            declared
                .ports
                .iter()
                .map(move |port| format!("{} . {port}", declared.cidr))
        })
        .collect()
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
    /// Interface addresses with host bits set, overlapping the baseline to
    /// exercise canonicalization and `auto-merge`.
    fn vmm_subnets() -> Vec<ipnetwork::Ipv4Network> {
        ["10.89.0.4/24", "192.0.2.1/30"]
            .iter()
            .map(|raw| raw.parse().expect("test constant"))
            .collect()
    }

    fn render(document: &str) -> String {
        let policy = egress::parse(document.as_bytes()).expect("fixture parses");
        super::render(&policy, &vmm_subnets()).expect("fixture renders")
    }

    const PUBLIC: &str = r#"{"egress":"public"}"#;
    const NONE: &str = r#"{"egress":"none"}"#;
    const ALLOW_ALL: &str = r#"{"egress":"public","allowAll":true}"#;
    const DECLARED: &str = r#"{
        "egress": "public",
        "declaredCidrs": [ { "cidr": "93.184.216.0/24", "ports": [8443, 443] } ]
    }"#;
    const RATELIMIT: &str = r#"{
        "egress": "public",
        "declaredCidrs": [ { "cidr": "93.184.216.0/24", "ports": [443] } ],
        "connectionsPerMinute": 60,
        "distinctDestinationsPerMinute": 5
    }"#;
    const ALLOWED_NAMES: &str = r#"{
        "egress": "public",
        "allowedNames": ["PyPI.ORG", "pypi.org", "files.pythonhosted.org", "artifacts.acmeco.example"],
        "ttlFloorSecs": 120,
        "ttlCapSecs": 1800
    }"#;
    /// Lists configured under `egress: none` are dormant, not refused. The
    /// header comment is the only place they appear.
    const NONE_DORMANT: &str = r#"{
        "egress": "none",
        "allowedNames": ["pypi.org"],
        "declaredCidrs": [ { "cidr": "93.184.216.0/24", "ports": [443] } ],
        "connectionsPerMinute": 60,
        "distinctDestinationsPerMinute": 5
    }"#;

    #[test]
    fn public_ruleset() {
        insta::assert_snapshot!(render(PUBLIC));
    }

    #[test]
    fn none_ruleset() {
        insta::assert_snapshot!(render(NONE));
    }

    #[test]
    fn allow_all_ruleset() {
        insta::assert_snapshot!(render(ALLOW_ALL));
    }

    #[test]
    fn declared_ruleset() {
        insta::assert_snapshot!(render(DECLARED));
    }

    #[test]
    fn ratelimit_ruleset() {
        insta::assert_snapshot!(render(RATELIMIT));
    }

    #[test]
    fn allowed_names_ruleset() {
        insta::assert_snapshot!(render(ALLOWED_NAMES));
    }

    #[test]
    fn none_dormant_ruleset() {
        insta::assert_snapshot!(render(NONE_DORMANT));
    }

    /// Every accept must be accounted for, and entry to egress_accept must
    /// follow the baseline drop.
    #[test]
    fn every_accept_is_accounted_for() {
        let expected: &[(&str, &[&str])] = &[
            (
                PUBLIC,
                &[
                    "forward:replies",
                    "egress_accept:resolved",
                    "input:loopback",
                    "input:replies",
                    "input:tap-dns",
                ],
            ),
            (
                NONE,
                &["forward:replies", "input:loopback", "input:replies"],
            ),
            (
                NONE_DORMANT,
                &["forward:replies", "input:loopback", "input:replies"],
            ),
            (
                ALLOW_ALL,
                &[
                    "forward:replies",
                    "egress_accept:allow-all",
                    "egress_accept:resolved",
                    "input:loopback",
                    "input:replies",
                    "input:tap-dns",
                ],
            ),
            (
                DECLARED,
                &[
                    "forward:replies",
                    "egress_accept:resolved",
                    "egress_accept:declared",
                    "input:loopback",
                    "input:replies",
                    "input:tap-dns",
                ],
            ),
            (
                RATELIMIT,
                &[
                    "forward:replies",
                    "egress_accept:resolved",
                    "egress_accept:declared",
                    "input:loopback",
                    "input:replies",
                    "input:tap-dns",
                ],
            ),
            (
                ALLOWED_NAMES,
                &[
                    "forward:replies",
                    "egress_accept:resolved",
                    "input:loopback",
                    "input:replies",
                    "input:tap-dns",
                ],
            ),
        ];

        for (document, accepts) in expected {
            let text = render(document);

            let actual = accepted(&text);
            let actual: Vec<&str> = actual.iter().map(String::as_str).collect();
            assert_eq!(actual.as_slice(), *accepts, "accepts of {document}");

            let forward = chain(&text, "forward");
            let baseline = forward
                .iter()
                .position(|rule| rule.contains("@baseline") && rule.contains(" drop "))
                .expect("every forward chain drops the baseline");

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

    #[test]
    fn egress_none_drops_the_public_machinery() {
        for document in [NONE, NONE_DORMANT] {
            let text = render(document);

            for absent in [
                "chain egress_accept",
                "set resolved",
                "set declared",
                "set dests",
                "tap-dns",
                "rate-limit",
            ] {
                assert!(!text.contains(absent), "{absent:?} survives {document}");
            }
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
