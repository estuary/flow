//! Argument parsing for the helper CLI in CONTRACTS.md.
//!
//! Hand-rolled rather than clap because `--exec` swallows every remaining
//! argument, including ones that look like helper flags
//! (`--exec /bin/sh -c 'exit 7'`).

use std::path::PathBuf;

pub struct Args {
    pub policy: PathBuf,
    pub memory_mib: u32,
    pub vcpus: u8,
    pub disk_mib: u64,
    pub upper_mib: u32,
    pub venv_dax: bool,
    pub thp_disable: bool,
    pub run_as_root: bool,
    pub debug: bool,
    pub no_flow_init: bool,
    /// Replaces the default workload argv when present.
    pub exec: Option<Vec<String>>,
}

pub const USAGE: &str = "usage: flow-sandbox-helper --policy PATH --memory-mib N --vcpus N \
     --disk-mib N --upper-mib N [--venv-dax] [--thp-disable] [--run-as-root] [--debug] \
     [--no-flow-init] [--exec ARGV...]";

pub fn parse(argv: Vec<String>) -> anyhow::Result<Args> {
    let mut policy = None;
    let mut memory_mib = None;
    let mut vcpus = None;
    let mut disk_mib = None;
    let mut upper_mib = None;
    let mut venv_dax = false;
    let mut thp_disable = false;
    let mut run_as_root = false;
    let mut debug = false;
    let mut no_flow_init = false;
    let mut exec = None;

    let value = |i: usize| -> anyhow::Result<&str> {
        argv.get(i + 1)
            .map(String::as_str)
            .ok_or_else(|| anyhow::anyhow!("{} requires a value\n{USAGE}", argv[i]))
    };

    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--policy" => (policy, i) = (Some(PathBuf::from(value(i)?)), i + 2),
            "--memory-mib" => (memory_mib, i) = (Some(value(i)?.parse()?), i + 2),
            "--vcpus" => (vcpus, i) = (Some(value(i)?.parse()?), i + 2),
            "--disk-mib" => (disk_mib, i) = (Some(value(i)?.parse()?), i + 2),
            "--upper-mib" => (upper_mib, i) = (Some(value(i)?.parse()?), i + 2),
            "--venv-dax" => (venv_dax, i) = (true, i + 1),
            "--thp-disable" => (thp_disable, i) = (true, i + 1),
            "--run-as-root" => (run_as_root, i) = (true, i + 1),
            "--debug" => (debug, i) = (true, i + 1),
            "--no-flow-init" => (no_flow_init, i) = (true, i + 1),
            "--exec" => {
                exec = Some(argv[i + 1..].to_vec());
                break;
            }
            other => anyhow::bail!("unrecognized argument {other:?}\n{USAGE}"),
        }
    }

    let missing = |flag: &str| anyhow::anyhow!("{flag} is required\n{USAGE}");
    let args = Args {
        policy: policy.ok_or_else(|| missing("--policy"))?,
        memory_mib: memory_mib.ok_or_else(|| missing("--memory-mib"))?,
        vcpus: vcpus.ok_or_else(|| missing("--vcpus"))?,
        disk_mib: disk_mib.ok_or_else(|| missing("--disk-mib"))?,
        upper_mib: upper_mib.ok_or_else(|| missing("--upper-mib"))?,
        venv_dax,
        thp_disable,
        run_as_root,
        debug,
        no_flow_init,
        exec,
    };

    if args.exec.as_ref().is_some_and(Vec::is_empty) {
        anyhow::bail!("--exec requires at least one argument\n{USAGE}");
    }
    if args.no_flow_init && args.exec.is_none() {
        anyhow::bail!("--no-flow-init requires --exec\n{USAGE}");
    }
    Ok(args)
}
