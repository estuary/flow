//! `flow-sandbox-egress`: policy JSON in, `inet flow_sandbox` loaded.
//!
//! The helper runs this before the VM starts, so the guest's first packet
//! meets a finished ruleset. `--print` writes the same text to stdout without
//! touching the kernel.

use flow_sandbox_egress::{ifaddrs, policy, ruleset};
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::process::{Command, Stdio};

const USAGE: &str = "usage: flow-sandbox-egress --policy PATH [--tap NAME] [--uplink NAME] \
     [--guest-ip A.B.C.D] [--helper-ip A.B.C.D] [--print]";

struct Args {
    policy: PathBuf,
    tap: String,
    uplink: String,
    guest_ip: Ipv4Addr,
    helper_ip: Ipv4Addr,
    print: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("flow-sandbox-egress: {error:#}");
        std::process::exit(2);
    }
}

fn run() -> anyhow::Result<()> {
    let args = parse(std::env::args().skip(1).collect()).map_err(|e| anyhow::anyhow!("{e}"))?;
    let policy = policy::load(&args.policy)?;
    let helper_subnets = ifaddrs::helper_subnets()?;

    let text = ruleset::render(
        &policy,
        &ruleset::Params {
            tap: &args.tap,
            uplink: &args.uplink,
            guest_ip: args.guest_ip,
            helper_ip: args.helper_ip,
            helper_subnets: &helper_subnets,
        },
    )?;

    if args.print {
        print!("{text}");
        return Ok(());
    }
    apply(&text)
}

fn apply(text: &str) -> anyhow::Result<()> {
    let mut nft = Command::new("nft")
        .arg("-f")
        .arg("-")
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("running nft: {e}"))?;

    nft.stdin
        .take()
        .expect("stdin was piped")
        .write_all(text.as_bytes())
        .map_err(|e| anyhow::anyhow!("writing the ruleset to nft: {e}"))?;

    let status = nft
        .wait()
        .map_err(|e| anyhow::anyhow!("waiting on nft: {e}"))?;
    if !status.success() {
        anyhow::bail!("nft -f -: {status}");
    }
    Ok(())
}

fn parse(argv: Vec<String>) -> Result<Args, String> {
    let mut policy = None;
    let mut tap = "tap0".to_string();
    let mut uplink = "eth0".to_string();
    let mut guest_ip: Ipv4Addr = "192.0.2.2".parse().unwrap();
    let mut helper_ip: Ipv4Addr = "192.0.2.1".parse().unwrap();
    let mut print = false;

    let value = |i: usize| -> Result<&str, String> {
        argv.get(i + 1)
            .map(String::as_str)
            .ok_or_else(|| format!("{} requires a value; {USAGE}", argv[i]))
    };
    let address = |i: usize| -> Result<Ipv4Addr, String> {
        let raw = value(i)?;
        raw.parse()
            .map_err(|e| format!("{} {raw:?}: {e}; {USAGE}", argv[i]))
    };

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--policy" => (policy, i) = (Some(PathBuf::from(value(i)?)), i + 2),
            "--tap" => (tap, i) = (value(i)?.to_string(), i + 2),
            "--uplink" => (uplink, i) = (value(i)?.to_string(), i + 2),
            "--guest-ip" => (guest_ip, i) = (address(i)?, i + 2),
            "--helper-ip" => (helper_ip, i) = (address(i)?, i + 2),
            "--print" => (print, i) = (true, i + 1),
            other => return Err(format!("unrecognized argument {other:?}; {USAGE}")),
        }
    }

    Ok(Args {
        policy: policy.ok_or_else(|| format!("--policy is required; {USAGE}"))?,
        tap,
        uplink,
        guest_ip,
        helper_ip,
        print,
    })
}
