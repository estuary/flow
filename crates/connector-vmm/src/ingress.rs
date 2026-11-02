//! Launch-owned host permissions, checked on every packet: ingress to declared
//! TCP ports, and forwarded egress to a task's declared CIDRs. The shared
//! boundary clears the reserved packet mark before these chains run, so an
//! authorization cannot be supplied by a VMM or outlive its launch in
//! conntrack.

pub const MARK: u32 = 0x46564d49;

pub fn token(value: &str) -> Result<String, String> {
    if value.len() == 32
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(value.to_string())
    } else {
        Err("expected a 32-digit lowercase hexadecimal owner token".to_string())
    }
}

pub fn bridge(value: &str) -> Result<String, String> {
    if value.len() == 15
        && value.starts_with(crate::boundary::BRIDGE_PREFIX)
        && value[3..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(value.to_string())
    } else {
        Err("expected an fvm bridge followed by twelve lowercase hexadecimal digits".to_string())
    }
}

pub fn cidr(value: &str) -> Result<String, String> {
    egress::cidrs("--cidr", &[value.to_string()])
        .map(|_| value.to_string())
        .map_err(|error| format!("{error:#}"))
}

pub fn grant(owner: &str, bridge: &str, ports: &[u16], cidrs: &[String]) -> anyhow::Result<()> {
    let cidrs = egress::cidrs("--cidr", cidrs)?;
    crate::boundary::run(&crate::boundary::Action::Verify)?;
    crate::sys::run(
        "nft",
        &["-f", "-"],
        Some(document(owner, bridge, ports, &cidrs).as_bytes()),
    )
}

pub fn revoke(owner: &str) -> anyhow::Result<()> {
    // Declare-then-delete makes recovery idempotent, including a crash before
    // grant. The full owner token keeps a replacement's permissions distinct.
    let table = format!("flow_vmm_ingress_{owner}");
    crate::sys::run(
        "nft",
        &["-f", "-"],
        Some(format!("table inet {table} {{}}\ndelete table inet {table}\n").as_bytes()),
    )
}

fn document(owner: &str, bridge: &str, ports: &[u16], cidrs: &[ipnetwork::Ipv4Network]) -> String {
    let mut out = format!("add table inet flow_vmm_ingress_{owner} {{\n");
    if !ports.is_empty() {
        let ports = ports
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "chain output {{\n\
             type filter hook output priority -11; policy accept;\n\
             oifname \"{bridge}\" meta nfproto ipv4 ct direction original ct state new,established tcp dport {{ {ports} }} meta mark set {MARK}\n\
             }}\n\
             chain input {{\n\
             type filter hook input priority -11; policy accept;\n\
             iifname \"{bridge}\" meta nfproto ipv4 ct direction reply ct state established tcp sport {{ {ports} }} meta mark set {MARK}\n\
             }}\n"
        ));
    }
    if !cidrs.is_empty() {
        let cidrs = cidrs
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "chain forward {{\n\
             type filter hook forward priority -11; policy accept;\n\
             iifname \"{bridge}\" meta nfproto ipv4 ip daddr {{ {cidrs} }} meta mark set {MARK}\n\
             }}\n"
        ));
    }
    out.push_str("}\n");
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_permissions() {
        insta::assert_snapshot!(super::document(
            "00112233445566778899aabbccddeeff",
            "fvm0123456789ab",
            &[8080, 9000],
            &[]
        ));
    }

    #[test]
    fn host_permissions_with_cidrs() {
        let cidrs = egress::cidrs(
            "--cidr",
            &["192.168.4.7/32".to_string(), "10.0.0.0/8".to_string()],
        )
        .unwrap();
        let mut table = super::document(
            "00112233445566778899aabbccddeeff",
            "fvm0123456789ab",
            &[8080],
            &cidrs,
        );
        table.push('\n');
        table.push_str(&super::document(
            "00112233445566778899aabbccddeeff",
            "fvm0123456789ab",
            &[],
            &egress::cidrs("--cidr", &["0.0.0.0/0".to_string()]).unwrap(),
        ));
        insta::assert_snapshot!(table);
    }
}
