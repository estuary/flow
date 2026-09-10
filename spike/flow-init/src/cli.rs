//! Argument parsing for the flow-init CLI in CONTRACTS.md.
//!
//! Hand-rolled in the same shape as the helper shim's `cli.rs`: everything
//! after `--` is the workload's argv and must not be looked at.

use std::net::Ipv4Addr;

pub struct Args {
    pub guest_ip: Ipv4Addr,
    pub prefix_len: u8,
    pub gateway: Ipv4Addr,
    pub nameserver: Ipv4Addr,
    pub upper_mib: u32,
    pub uid: u32,
    pub gid: u32,
    pub venv_dax: bool,
    pub run_as_root: bool,
    /// Run by `/bin/sh -c` as guest root before the uid drop.
    pub as_root_exec: Option<String>,
    pub argv: Vec<String>,
}

pub const USAGE: &str = "usage: /flow-init --guest-ip A.B.C.D/N --gateway A.B.C.D \
     --nameserver A.B.C.D --upper-mib N --uid U --gid G [--venv-dax] [--run-as-root] \
     [--as-root-exec CMD] -- ARGV...";

pub fn parse(argv: Vec<String>) -> Result<Args, String> {
    let mut guest_ip = None;
    let mut gateway = None;
    let mut nameserver = None;
    let mut upper_mib = None;
    let mut uid = None;
    let mut gid = None;
    let mut venv_dax = false;
    let mut run_as_root = false;
    let mut as_root_exec = None;
    let mut workload = None;

    let value = |i: usize| -> Result<&str, String> {
        argv.get(i + 1)
            .map(String::as_str)
            .ok_or_else(|| format!("{} requires a value; {USAGE}", argv[i]))
    };
    let number = |i: usize| -> Result<u32, String> {
        let raw = value(i)?;
        raw.parse()
            .map_err(|e| format!("{} {raw:?}: {e}; {USAGE}", argv[i]))
    };
    let address = |i: usize| -> Result<Ipv4Addr, String> {
        let raw = value(i)?;
        raw.parse()
            .map_err(|e| format!("{} {raw:?}: {e}; {USAGE}", argv[i]))
    };

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--guest-ip" => {
                let raw = value(i)?;
                let (address, prefix) = raw
                    .split_once('/')
                    .ok_or_else(|| format!("--guest-ip {raw:?} is not A.B.C.D/N; {USAGE}"))?;
                let parsed: Ipv4Addr = address
                    .parse()
                    .map_err(|e| format!("--guest-ip {raw:?}: {e}; {USAGE}"))?;
                let prefix: u8 = prefix
                    .parse()
                    .map_err(|e| format!("--guest-ip {raw:?}: {e}; {USAGE}"))?;
                if !(1..=32).contains(&prefix) {
                    return Err(format!("--guest-ip {raw:?}: prefix out of range; {USAGE}"));
                }
                (guest_ip, i) = (Some((parsed, prefix)), i + 2);
            }
            "--gateway" => (gateway, i) = (Some(address(i)?), i + 2),
            "--nameserver" => (nameserver, i) = (Some(address(i)?), i + 2),
            "--upper-mib" => (upper_mib, i) = (Some(number(i)?), i + 2),
            "--uid" => (uid, i) = (Some(number(i)?), i + 2),
            "--gid" => (gid, i) = (Some(number(i)?), i + 2),
            "--venv-dax" => (venv_dax, i) = (true, i + 1),
            "--run-as-root" => (run_as_root, i) = (true, i + 1),
            "--as-root-exec" => (as_root_exec, i) = (Some(value(i)?.to_string()), i + 2),
            "--" => {
                workload = Some(argv[i + 1..].to_vec());
                break;
            }
            other => return Err(format!("unrecognized argument {other:?}; {USAGE}")),
        }
    }

    let missing = |flag: &str| format!("{flag} is required; {USAGE}");
    let (guest_ip, prefix_len) = guest_ip.ok_or_else(|| missing("--guest-ip"))?;
    let argv = workload.ok_or_else(|| missing("-- ARGV"))?;
    if argv.is_empty() {
        return Err(format!("-- requires a workload argv; {USAGE}"));
    }

    Ok(Args {
        guest_ip,
        prefix_len,
        gateway: gateway.ok_or_else(|| missing("--gateway"))?,
        nameserver: nameserver.ok_or_else(|| missing("--nameserver"))?,
        upper_mib: upper_mib.ok_or_else(|| missing("--upper-mib"))?,
        uid: uid.ok_or_else(|| missing("--uid"))?,
        gid: gid.ok_or_else(|| missing("--gid"))?,
        venv_dax,
        run_as_root,
        as_root_exec,
        argv,
    })
}
