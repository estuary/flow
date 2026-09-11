//! Policy in, complete nftables ruleset out. Nothing here touches the kernel:
//! the caller applies the text, or prints it for the report.

use crate::cidr::Cidr;
use crate::policy::{denied_prefixes, Mode, Policy};
use std::fmt::Write;
use std::net::Ipv4Addr;

pub struct Params<'a> {
    pub tap: &'a str,
    pub uplink: &'a str,
    pub guest_ip: Ipv4Addr,
    pub helper_ip: Ipv4Addr,
    /// From `ifaddrs::helper_subnets()`; folded into the baseline.
    pub helper_subnets: &'a [Cidr],
}

pub fn render(policy: &Policy, params: &Params) -> anyhow::Result<String> {
    let baseline = denied_prefixes(params.helper_subnets)?;
    let declared = declared_elements(policy, &baseline)?;
    let Params {
        tap,
        uplink,
        guest_ip,
        helper_ip,
        ..
    } = *params;
    let public = policy.egress == Mode::Public;

    let mut out = String::new();
    writeln!(
        out,
        "# flow-sandbox egress ruleset: egress={} allowAll={} declaredCidrs={} \
         connectionsPerMinute={:?} distinctDestinationsPerMinute={:?}",
        if public { "public" } else { "none" },
        policy.allow_all,
        policy.declared_cidrs.len(),
        policy.connections_per_minute,
        policy.distinct_destinations_per_minute,
    )?;
    // Declare-then-delete: `nft delete` on a table that was never created is an
    // error, and a second apply must replace the first rather than append to it.
    out.push_str(
        "table inet flow_sandbox {}\n\
         delete table inet flow_sandbox\n\n\
         table inet flow_sandbox {\n",
    );

    // auto-merge because the helper's eth0 subnet sits inside 10/8 on
    // `flow-connectors`, and nft rejects overlapping interval elements.
    out.push_str(
        "\tset baseline {\n\
         \t\ttype ipv4_addr\n\
         \t\tflags interval\n\
         \t\tauto-merge\n",
    );
    writeln!(out, "\t\telements = {{ {} }}", join(&baseline))?;
    out.push_str("\t}\n\n");

    if public {
        // Filled by flow-sandbox-resolver, one element per answered address,
        // with the clamped TTL as its timeout.
        out.push_str(
            "\tset resolved {\n\
             \t\ttype ipv4_addr\n\
             \t\tflags timeout\n\
             \t}\n\n",
        );
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
        // `size` is the whole point: the (limit+1)th distinct destination
        // cannot be added, the rule that adds it is not taken, and the packet
        // falls through to the chain's drop policy.
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
        "\t\tiifname \"{tap}\" ip saddr != {guest_ip} counter drop comment \"anti-spoof\""
    )?;
    out.push_str("\t\tct state established,related counter accept comment \"replies\"\n");
    out.push_str("\t\tmeta nfproto ipv6 counter drop comment \"no-ipv6\"\n");
    // Kills ICMP, which is why it sits after the established/related accept:
    // ICMP errors belonging to a tracked connection are still useful.
    out.push_str("\t\tmeta l4proto != { tcp, udp } counter drop comment \"tcp-udp-only\"\n");
    out.push_str("\t\tip daddr @baseline counter drop comment \"baseline\"\n");
    out.push_str("\t\ttcp dport 25 counter drop comment \"smtp\"\n");

    if public {
        if let Some(rate) = policy.connections_per_minute {
            // nft's `limit rate over` carries a default burst of 5 packets, so
            // the claim is "beyond the rate", not "exactly the (N+1)th".
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
        out.push_str("\t\tcounter comment \"forward-drop\"\n");
    } else {
        out.push_str("\t\tcounter comment \"forward-drop\"\n");
    }
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
        "\t\tiifname \"{tap}\" ip saddr != {guest_ip} counter drop comment \"anti-spoof\""
    )?;
    out.push_str("\t\tct state established,related counter accept comment \"replies\"\n");
    if public {
        // The only packet the guest may address to the helper. With
        // `egress: none` no resolver runs, so the rule is absent and the query
        // is dropped rather than refused - which is what makes the guest pay
        // its resolver's full retry budget (PLAN experiment 7).
        writeln!(
            out,
            "\t\tiifname \"{tap}\" ip saddr {guest_ip} ip daddr {helper_ip} udp dport 53 \
             counter accept comment \"tap-dns\""
        )?;
    }
    out.push_str("\t\tcounter comment \"input-drop\"\n");
    out.push_str("\t}\n\n");

    // Policy accept: the helper's own uplink traffic (the resolver's queries
    // upstream) must keep working. Only the tap side is constrained, and there
    // to replies, so nothing in the helper can open a connection to the guest.
    out.push_str("\tchain output {\n");
    out.push_str("\t\ttype filter hook output priority filter; policy accept;\n");
    writeln!(
        out,
        "\t\toifname \"{tap}\" ct state != established counter drop comment \"no-inbound\""
    )?;
    out.push_str("\t}\n\n");

    out.push_str("\tchain postrouting {\n");
    out.push_str("\t\ttype nat hook postrouting priority srcnat; policy accept;\n");
    writeln!(
        out,
        "\t\tip saddr {guest_ip} oifname \"{uplink}\" counter masquerade comment \"masquerade\""
    )?;
    out.push_str("\t}\n");

    out.push_str("}\n");
    Ok(out)
}

/// `None` when the policy sets no fan-out limit, which leaves the `dests` set
/// and its rule out of the ruleset entirely.
fn fan_out_limit(policy: &Policy) -> Option<u32> {
    policy
        .distinct_destinations_per_minute
        .filter(|_| policy.egress == Mode::Public)
}

/// One `cidr . port` element per declared port, rejecting any CIDR that
/// overlaps the baseline: the baseline drop runs first, so such a policy would
/// silently not mean what it says.
fn declared_elements(policy: &Policy, baseline: &[Cidr]) -> anyhow::Result<Vec<String>> {
    let mut elements = Vec::new();
    if policy.egress != Mode::Public {
        return Ok(elements);
    }

    for declared in &policy.declared_cidrs {
        let cidr: Cidr = declared
            .cidr
            .parse()
            .map_err(|e| anyhow::anyhow!("declaredCidrs: {e}"))?;

        if let Some(hit) = baseline.iter().find(|entry| entry.overlaps(&cidr)) {
            anyhow::bail!("declared CIDR {cidr} overlaps the baseline denylist entry {hit}");
        }
        if declared.ports.is_empty() {
            anyhow::bail!("declared CIDR {cidr} lists no ports");
        }
        for port in &declared.ports {
            elements.push(format!("{cidr} . {port}"));
        }
    }
    Ok(elements)
}

fn join(prefixes: &[Cidr]) -> String {
    prefixes
        .iter()
        .map(Cidr::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}
