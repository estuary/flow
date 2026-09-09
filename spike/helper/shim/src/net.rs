//! Tap device and egress setup, all of which must be complete before the VM
//! starts: the guest's first packet has to meet a finished ruleset.
//!
//! libkrun creates its own tap fd inside `krun_start_enter` and sets no
//! address, so the device is created persistent here and libkrun's `TUNSETIFF`
//! attaches to it.

use std::path::Path;
use std::process::{Child, Command, Stdio};

pub const TAP: &str = "tap0";
pub const UPLINK: &str = "eth0";
pub const HELPER_IP: &str = "192.0.2.1";
pub const HELPER_CIDR: &str = "192.0.2.1/30";
pub const GUEST_IP: &str = "192.0.2.2";
pub const GUEST_CIDR: &str = "192.0.2.2/30";
pub const GUEST_MAC: [u8; 6] = [0x02, 0xf1, 0x0f, 0x00, 0x00, 0x02];

pub fn create_tap() -> anyhow::Result<()> {
    run("ip", &["tuntap", "add", "dev", TAP, "mode", "tap"])?;
    run("ip", &["addr", "add", HELPER_CIDR, "dev", TAP])?;
    run("ip", &["link", "set", TAP, "up"])
}

pub fn load_egress(policy: &Path) -> anyhow::Result<()> {
    run(
        "flow-sandbox-egress",
        &[
            "--policy",
            &policy.display().to_string(),
            "--tap",
            TAP,
            "--uplink",
            UPLINK,
            "--guest-ip",
            GUEST_IP,
            "--helper-ip",
            HELPER_IP,
        ],
    )
}

/// The resolver runs until killed. It is not waited on: `krun_start_enter`
/// never returns, and podman tears the whole namespace down when the shim
/// exits, so the child cannot outlive the sandbox.
pub fn spawn_resolver(policy: &Path, upstream: &str) -> anyhow::Result<Child> {
    Command::new("flow-sandbox-resolver")
        .args(["--policy", &policy.display().to_string()])
        .args(["--listen", &format!("{HELPER_IP}:53")])
        .args(["--upstream", upstream])
        .stdin(Stdio::null())
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawning flow-sandbox-resolver: {e}"))
}

/// The helper's own resolver, which on `flow-connectors` is podman's
/// aardvark-dns. `resolv.conf` has no port syntax, so 53 is implied.
pub fn upstream_nameserver() -> anyhow::Result<String> {
    let content = std::fs::read_to_string("/etc/resolv.conf")
        .map_err(|e| anyhow::anyhow!("reading /etc/resolv.conf: {e}"))?;

    for line in content.lines() {
        let Some(address) = line.strip_prefix("nameserver") else {
            continue;
        };
        let address = address.trim();
        if !address.is_empty() {
            return Ok(format!("{address}:53"));
        }
    }
    anyhow::bail!("no nameserver line in /etc/resolv.conf")
}

fn run(program: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(program)
        .args(args)
        .status()
        .map_err(|e| anyhow::anyhow!("running {program}: {e}"))?;
    if !status.success() {
        anyhow::bail!("{program} {}: {status}", args.join(" "));
    }
    Ok(())
}
