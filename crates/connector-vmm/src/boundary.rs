//! The host's side of every VMM's network: two nftables tables in the host
//! network namespace that bound whatever a VMM container sends, whatever it
//! has done to its own ruleset, addresses or routes.
//!
//! A VMM's `inet flow_egress` lives in its container's network namespace,
//! where the VMM holds `CAP_NET_ADMIN`, so a compromised VMM can remove it.
//! These tables are out of its reach. They recognize VMM traffic by the host
//! interface it crosses, never by an address the VMM chose: each VMM container
//! sits alone on its own podman bridge, named with `BRIDGE_PREFIX`. Keyed on
//! that prefix, the tables depend on no particular network or launch. One
//! installation covers every VMM bridge the host will have, before any exists,
//! and every owner of such a network installs the same tables.
//!
//! Every rule but one drops, and a drop in any base chain is final: nothing
//! another table accepts (netavark's, Docker's) undoes it, and nothing here
//! widens what those tables allow. The exception is the VMM's nameserver,
//! which is podman's resolver on the gateway address of the VMM's own bridge,
//! the address podman writes into the container's `resolv.conf`. The rule
//! names the bridge the query arrived on rather than an address, so the
//! exception cannot drift from what the VMM's resolver uses.

use serde_json::{Value, json};
use std::collections::BTreeMap;

pub const TABLE: &str = "flow_vmm_boundary";

/// The start of every VMM bridge's interface name. Nothing else on the host
/// may use it: an interface named so is treated as a VMM bridge.
pub const BRIDGE_PREFIX: &str = "fvm";

const FAMILIES: [&str; 2] = ["inet", "bridge"];

/// After conntrack's defragmentation (-400) and before conntrack itself
/// (-200), so a packet with a spoofed source never creates a flow.
const PREROUTING_PRIORITY: i32 = -300;
/// Ahead of the filter chains of netavark, Docker and iptables-nft (0), so
/// these counters see every packet first. Order is not what makes a drop
/// hold; it keeps the counters attributable.
const FILTER_PRIORITY: i32 = -10;
/// The bridge family's filter priority is -200.
const BRIDGE_PRIORITY: i32 = -210;

#[derive(clap::Subcommand, Debug)]
pub enum Action {
    /// Replace both tables in one transaction, then verify them. Fails, having
    /// installed them, if VMM bridges already existed while a table was absent:
    /// those VMMs ran without the boundary and must be replaced.
    Install,
    /// Exit zero only if both tables are exactly those this binary installs.
    Verify,
    /// Delete both tables. Refused while any VMM bridge exists.
    Remove,
}

pub fn run(action: &Action) -> anyhow::Result<()> {
    match action {
        Action::Install => install(),
        Action::Verify => verify(),
        Action::Remove => remove(),
    }
}

/// Declare, delete and define both tables in one transaction, so a reinstall
/// or an upgrade replaces them with no permissive moment between. The
/// declaration exists because deleting a table that was never created is an
/// error.
pub fn document() -> Value {
    let mut commands = replace_tables();
    commands.extend(
        expected()
            .into_iter()
            .map(|object| json!({ "add": object })),
    );
    json!({ "nftables": commands })
}

/// Every difference between a `nft -j list ruleset` document and the tables
/// `document` installs, ignoring handles and counter values. Other tables are
/// not compared: they can add drops, but none can undo one of these.
pub fn differences(listing: &Value) -> Vec<String> {
    let found: Vec<Value> = listing["nftables"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(ours)
        .collect();
    let mut out = Vec::new();

    // An absent table is one difference, not one per object it should hold.
    let mut expected = expected();
    for family in FAMILIES {
        if !found
            .iter()
            .any(|object| object["table"]["family"] == family)
        {
            out.push(format!("the {family} {TABLE} table is absent"));
            expected.retain(|object| family_of(object) != family);
        }
    }
    let (want_rules, want_objects) = split(expected);
    let (found_rules, found_objects) = split(found);

    for object in &want_objects {
        if !found_objects.contains(object) {
            out.push(format!("missing: {object}"));
        }
    }
    for object in &found_objects {
        if !want_objects.contains(object) {
            out.push(format!("unexpected: {object}"));
        }
    }
    let (want_rules, found_rules) = (chains(want_rules), chains(found_rules));
    let names: std::collections::BTreeSet<&(String, String)> =
        want_rules.keys().chain(found_rules.keys()).collect();

    for name @ (family, chain) in names {
        let (want, found) = (
            want_rules.get(name).map(Vec::as_slice).unwrap_or_default(),
            found_rules.get(name).map(Vec::as_slice).unwrap_or_default(),
        );
        let Some(position) =
            (0..want.len().max(found.len())).find(|i| want.get(*i) != found.get(*i))
        else {
            continue;
        };
        out.push(format!(
            "{family} {chain}, rule {position}: expected {}, found {}",
            want.get(position)
                .map_or("nothing".to_string(), Value::to_string),
            found
                .get(position)
                .map_or("nothing".to_string(), Value::to_string),
        ));
    }
    out
}

fn install() -> anyhow::Result<()> {
    let before = list()?;
    let absent = FAMILIES.iter().any(|family| !has_table(&before, family));
    let bridges = vmm_links()?;

    crate::sys::run(
        "nft",
        &["-j", "-f", "-"],
        Some(document().to_string().as_bytes()),
    )?;
    verify()?;

    if absent && !bridges.is_empty() {
        anyhow::bail!(
            "installed {TABLE}, but VMM bridges already existed without it: {}. \
             Their VMMs ran unprotected and must be replaced",
            bridges.join(", ")
        );
    }
    Ok(())
}

fn verify() -> anyhow::Result<()> {
    let differences = differences(&list()?);
    if differences.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "the {TABLE} tables do not match this flow-connector-vmm's:\n{}",
        differences.join("\n")
    )
}

fn remove() -> anyhow::Result<()> {
    let bridges = vmm_links()?;
    if !bridges.is_empty() {
        anyhow::bail!(
            "not removing {TABLE}: VMM bridges still exist: {}",
            bridges.join(", ")
        );
    }
    let commands = json!({ "nftables": replace_tables() });
    crate::sys::run(
        "nft",
        &["-j", "-f", "-"],
        Some(commands.to_string().as_bytes()),
    )
}

fn list() -> anyhow::Result<Value> {
    let listing = crate::sys::output("nft", &["-j", "list", "ruleset"])?;
    serde_json::from_slice(&listing).map_err(|e| anyhow::anyhow!("parsing nft's listing: {e}"))
}

fn has_table(listing: &Value, family: &str) -> bool {
    listing["nftables"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|item| item["table"]["family"] == family && item["table"]["name"] == TABLE)
}

/// Interfaces in this network namespace that the tables treat as VMM bridges.
fn vmm_links() -> anyhow::Result<Vec<String>> {
    // SAFETY: if_nameindex takes no arguments and returns null or an array
    // terminated by a zeroed entry, owned by this thread until freed below.
    let head = unsafe { libc::if_nameindex() };
    if head.is_null() {
        anyhow::bail!("if_nameindex: {}", std::io::Error::last_os_error());
    }
    let mut links = Vec::new();
    let mut cursor = head;

    // SAFETY: every entry before the terminator has a valid NUL-terminated
    // name, and the terminator is never read past.
    unsafe {
        while (*cursor).if_index != 0 {
            let name = std::ffi::CStr::from_ptr((*cursor).if_name).to_string_lossy();
            if name.starts_with(BRIDGE_PREFIX) {
                links.push(name.into_owned());
            }
            cursor = cursor.add(1);
        }
        libc::if_freenameindex(head);
    }
    Ok(links)
}

fn replace_tables() -> Vec<Value> {
    FAMILIES
        .iter()
        .flat_map(|family| {
            let table = json!({ "family": family, "name": TABLE });
            [
                json!({ "add": { "table": table } }),
                json!({ "delete": { "table": table } }),
            ]
        })
        .collect()
}

/// Both tables, as objects in the shape `nft -j list` prints them once handles
/// and counter values are dropped. `document` installs exactly these, which is
/// what lets `differences` compare against them directly.
fn expected() -> Vec<Value> {
    let vmm = Value::from(format!("{BRIDGE_PREFIX}*"));
    let from = || meta("iifname", vmm.clone());
    let to = || meta("oifname", vmm.clone());
    let replies = json!({ "match": {
        "op": "!=",
        "left": { "ct": { "key": "state" } },
        "right": { "set": ["established", "related"] },
    }});
    let ipv4_or_arp = json!({ "match": {
        "op": "!=",
        "left": { "payload": { "protocol": "ether", "field": "type" } },
        "right": { "set": ["ip", "arp"] },
    }});
    let baseline: Vec<Value> = egress::baseline(&[])
        .iter()
        .map(|prefix| json!({ "prefix": { "addr": prefix.ip().to_string(), "len": prefix.prefix() } }))
        .collect();

    vec![
        json!({ "table": { "family": "inet", "name": TABLE } }),
        json!({ "set": {
            "family": "inet",
            "table": TABLE,
            "name": "baseline",
            "type": "ipv4_addr",
            "flags": ["interval"],
            "elem": baseline,
        }}),
        chain("inet", "prerouting", PREROUTING_PRIORITY),
        chain("inet", "input", FILTER_PRIORITY),
        chain("inet", "forward", FILTER_PRIORITY),
        chain("inet", "output", FILTER_PRIORITY),
        // IPv6 is dropped rather than filtered: nothing here would bound it.
        rule(
            "inet",
            "prerouting",
            "no-ipv6",
            vec![from(), meta("nfproto", "ipv6".into())],
            "drop",
        ),
        // The source must be one that routes back out of the bridge it came
        // in on: the VMM chooses its addresses, so nothing below may assume one.
        rule(
            "inet",
            "prerouting",
            "anti-spoof",
            vec![from(), fib("oif", &["saddr", "iif"], false.into())],
            "drop",
        ),
        // Never matches while conntrack reassembles; ports below assume it.
        rule(
            "inet",
            "prerouting",
            "fragment",
            vec![
                from(),
                json!({ "match": {
                    "op": "!=",
                    "left": { "&": [{ "payload": { "protocol": "ip", "field": "frag-off" } }, 0x1fff] },
                    "right": 0,
                }}),
            ],
            "drop",
        ),
        rule(
            "inet",
            "input",
            "gateway-dns",
            vec![
                from(),
                fib("type", &["daddr", "iif"], "local".into()),
                payload("==", "udp", "dport", 53.into()),
            ],
            "accept",
        ),
        rule("inet", "input", "host", vec![from()], "drop"),
        // Between two VMM bridges, and back out of the same one.
        rule("inet", "forward", "vmm-to-vmm", vec![from(), to()], "drop"),
        rule(
            "inet",
            "forward",
            "baseline",
            vec![from(), payload("==", "ip", "daddr", "@baseline".into())],
            "drop",
        ),
        rule(
            "inet",
            "forward",
            "smtp",
            vec![from(), payload("==", "tcp", "dport", 25.into())],
            "drop",
        ),
        rule(
            "inet",
            "forward",
            "no-ipv6-inbound",
            vec![to(), meta("nfproto", "ipv6".into())],
            "drop",
        ),
        // Nothing opens a connection into a VMM, from anywhere.
        rule(
            "inet",
            "forward",
            "no-inbound",
            vec![to(), replies.clone()],
            "drop",
        ),
        rule(
            "inet",
            "output",
            "no-ipv6-inbound",
            vec![to(), meta("nfproto", "ipv6".into())],
            "drop",
        ),
        rule("inet", "output", "no-inbound", vec![to(), replies], "drop"),
        json!({ "table": { "family": "bridge", "name": TABLE } }),
        chain("bridge", "prerouting", BRIDGE_PRIORITY),
        chain("bridge", "forward", BRIDGE_PRIORITY),
        chain("bridge", "output", BRIDGE_PRIORITY),
        // A VMM can bridge its guest onto its uplink and send any frame.
        rule(
            "bridge",
            "prerouting",
            "ipv4-arp-only",
            vec![meta("ibrname", vmm.clone()), ipv4_or_arp.clone()],
            "drop",
        ),
        // A second port on a VMM bridge is never a destination.
        rule(
            "bridge",
            "forward",
            "no-peers",
            vec![meta("ibrname", vmm.clone())],
            "drop",
        ),
        rule(
            "bridge",
            "output",
            "ipv4-arp-only",
            vec![meta("obrname", vmm), ipv4_or_arp],
            "drop",
        ),
    ]
}

fn chain(family: &str, name: &str, prio: i32) -> Value {
    json!({ "chain": {
        "family": family,
        "table": TABLE,
        "name": name,
        "type": "filter",
        "hook": name,
        "prio": prio,
        "policy": "accept",
    }})
}

fn rule(family: &str, chain: &str, comment: &str, mut expr: Vec<Value>, verdict: &str) -> Value {
    expr.push(json!({ "counter": null }));
    expr.push(json!({ verdict: null }));
    json!({ "rule": {
        "family": family,
        "table": TABLE,
        "chain": chain,
        "comment": comment,
        "expr": expr,
    }})
}

fn meta(key: &str, right: Value) -> Value {
    json!({ "match": { "op": "==", "left": { "meta": { "key": key } }, "right": right } })
}

fn payload(op: &str, protocol: &str, field: &str, right: Value) -> Value {
    json!({ "match": {
        "op": op,
        "left": { "payload": { "protocol": protocol, "field": field } },
        "right": right,
    }})
}

fn fib(result: &str, flags: &[&str], right: Value) -> Value {
    json!({ "match": {
        "op": "==",
        "left": { "fib": { "result": result, "flags": flags } },
        "right": right,
    }})
}

/// One listed object of these tables, without its handle, and with anonymous
/// counters' values dropped. `None` for anything else in the listing.
fn ours(item: &Value) -> Option<Value> {
    let (kind, body) = item.as_object()?.iter().next()?;
    let family = body["family"].as_str()?;
    let name = if kind == "table" {
        &body["name"]
    } else {
        &body["table"]
    };

    if !FAMILIES.contains(&family) || name != TABLE {
        return None;
    }
    let mut body = body.clone();
    body.as_object_mut()?.remove("handle");

    // `get_mut`, because indexing a JSON object mutably inserts the key.
    for expr in body
        .get_mut("expr")
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
    {
        if expr["counter"].is_object() {
            *expr = json!({ "counter": null });
        }
    }
    Some(json!({ kind: body }))
}

fn family_of(object: &Value) -> &str {
    object
        .as_object()
        .and_then(|object| object.values().next())
        .and_then(|body| body["family"].as_str())
        .unwrap_or_default()
}

fn split(objects: Vec<Value>) -> (Vec<Value>, Vec<Value>) {
    objects
        .into_iter()
        .partition(|object| object.get("rule").is_some())
}

fn chains(rules: Vec<Value>) -> BTreeMap<(String, String), Vec<Value>> {
    let mut chains: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();

    for rule in rules {
        let key = (
            rule["rule"]["family"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            rule["rule"]["chain"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        );
        chains.entry(key).or_default().push(rule);
    }
    chains
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    /// The kernel's own listing of the installed tables, captured from Linux
    /// 7.0 with nft 1.0.9 and trimmed to them: handles, counter values and
    /// the `metainfo` header as the kernel reported them.
    const LISTING: &str = include_str!("boundary/listing.json");

    #[test]
    fn document() {
        let document = super::document();
        let lines: Vec<String> = document["nftables"]
            .as_array()
            .expect("an array")
            .iter()
            .map(Value::to_string)
            .collect();
        insta::assert_snapshot!(lines.join("\n"));
    }

    #[test]
    fn the_kernels_listing_verifies() {
        let listing: Value = serde_json::from_str(LISTING).expect("fixture parses");
        assert_eq!(super::differences(&listing), Vec::<String>::new());
    }

    #[test]
    fn other_tables_are_ignored() {
        let mut listing: Value = serde_json::from_str(LISTING).expect("fixture parses");
        let items = listing["nftables"].as_array_mut().expect("an array");
        items.push(json!({ "table": { "family": "ip", "name": "filter", "handle": 1 } }));
        items.push(json!({ "chain": {
            "family": "ip", "table": "filter", "name": "FORWARD", "handle": 2,
            "type": "filter", "hook": "forward", "prio": 0, "policy": "accept",
        }}));
        items.push(json!({ "rule": {
            "family": "inet", "table": "firewalld", "chain": "forward", "handle": 3,
            "expr": [{ "accept": null }],
        }}));
        assert_eq!(super::differences(&listing), Vec::<String>::new());
    }

    #[test]
    fn tampering_is_detected() {
        type Edit = fn(&mut Vec<Value>);
        let edits: &[(&str, Edit)] =
            &[
                ("no tables at all", |items| {
                    items.retain(|item| item.get("metainfo").is_some())
                }),
                ("the bridge table is gone", |items| {
                    items.retain(|item| super::family_of(item) != "bridge")
                }),
                ("a dormant table", |items| {
                    items[1]["table"]["flags"] = json!(["dormant"]);
                }),
                ("a baseline element removed", |items| {
                    items[2]["set"]["elem"].as_array_mut().unwrap().remove(4);
                }),
                ("an accept inserted ahead of the forward drops", |items| {
                    let at = position(items, "forward", "vmm-to-vmm");
                    items.insert(at, json!({ "rule": {
                    "family": "inet", "table": super::TABLE, "chain": "forward", "handle": 99,
                    "expr": [{ "accept": null }],
                }}));
                }),
                ("a rule deleted", |items| {
                    let at = position(items, "input", "host");
                    items.remove(at);
                }),
                ("a rule's match rewritten under the same comment", |items| {
                    let at = position(items, "forward", "baseline");
                    items[at]["rule"]["expr"][0]["match"]["right"] = json!("fvx*");
                }),
                ("a chain's priority moved", |items| {
                    let at = items
                        .iter()
                        .position(|item| item["chain"]["name"] == "input")
                        .unwrap();
                    items[at]["chain"]["prio"] = json!(100);
                }),
                ("an extra chain", |items| {
                    items.push(json!({ "chain": {
                        "family": "inet", "table": super::TABLE, "name": "extra", "handle": 98,
                    }}));
                }),
            ];
        let mut report = String::new();

        for (name, edit) in edits {
            let mut listing: Value = serde_json::from_str(LISTING).expect("fixture parses");
            edit(listing["nftables"].as_array_mut().expect("an array"));
            let differences = super::differences(&listing);
            assert!(!differences.is_empty(), "{name} went unnoticed");

            report.push_str(&format!("# {name}\n"));
            for difference in differences {
                report.push_str(&format!("{difference}\n"));
            }
            report.push('\n');
        }
        insta::assert_snapshot!(report);
    }

    #[test]
    fn every_rule_is_keyed_on_a_bridge_and_drops() {
        let mut accepts = Vec::new();

        for object in super::expected() {
            let Some(rule) = object.get("rule") else {
                continue;
            };
            let expr = rule["expr"].as_array().expect("an expression list");
            let key = &expr[0]["match"]["left"]["meta"]["key"];
            assert!(
                ["iifname", "oifname", "ibrname", "obrname"]
                    .iter()
                    .any(|k| key == k)
                    && expr[0]["match"]["right"] == format!("{}*", super::BRIDGE_PREFIX),
                "{rule} is not keyed on a VMM bridge"
            );
            let verdict = expr.last().expect("a verdict");
            if verdict.get("accept").is_some() {
                accepts.push(format!("{}/{}", rule["chain"], rule["comment"]));
            } else {
                assert!(
                    verdict.get("drop").is_some(),
                    "{rule} neither drops nor accepts"
                );
            }
        }
        assert_eq!(accepts, vec![r#""input"/"gateway-dns""#]);
    }

    fn position(items: &[Value], chain: &str, comment: &str) -> usize {
        items
            .iter()
            .position(|item| item["rule"]["chain"] == chain && item["rule"]["comment"] == comment)
            .unwrap_or_else(|| panic!("no {chain}/{comment} in the fixture"))
    }
}
