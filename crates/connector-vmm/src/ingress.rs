//! Launch-owned host permissions, checked on every packet. The shared boundary
//! clears the reserved packet mark before these chains run, so an authorization
//! cannot be supplied by a VMM or outlive its launch in conntrack.

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

pub fn grant(owner: &str, bridge: &str, ports: &[u16]) -> anyhow::Result<()> {
    crate::boundary::run(&crate::boundary::Action::Verify)?;
    crate::sys::run(
        "nft",
        &["-f", "-"],
        Some(document(owner, bridge, ports).as_bytes()),
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

fn document(owner: &str, bridge: &str, ports: &[u16]) -> String {
    let ports = ports
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "add table inet flow_vmm_ingress_{owner} {{\n\
         chain output {{\n\
         type filter hook output priority -11; policy accept;\n\
         oifname \"{bridge}\" meta nfproto ipv4 ct direction original ct state new,established tcp dport {{ {ports} }} meta mark set {MARK}\n\
         }}\n\
         chain input {{\n\
         type filter hook input priority -11; policy accept;\n\
         iifname \"{bridge}\" meta nfproto ipv4 ct direction reply ct state established tcp sport {{ {ports} }} meta mark set {MARK}\n\
         }}\n}}\n"
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_permissions() {
        insta::assert_snapshot!(super::document(
            "00112233445566778899aabbccddeeff",
            "fvm0123456789ab",
            &[8080, 9000]
        ));
    }
}
