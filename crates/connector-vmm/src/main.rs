//! `flow-connector-vmm` runs one connector in a micro-VM: it compiles the
//! connector's egress policy into the nftables ruleset that bounds the guest's
//! network, serves the guest's DNS, prepares its disks and shares, and enters
//! the VM. The crate README maps out how the pieces fit.

mod console;
mod disk;
mod image;
mod krun;
mod launch;
mod net;
mod policy;
mod resolver;
mod ruleset;
mod sys;

/// Every failure before the VM starts, so that a caller reading the exit code
/// cannot confuse one with a workload's.
const EXIT_FAILED: u8 = 2;

#[derive(clap::Parser, Debug)]
#[command(
    name = "flow-connector-vmm",
    about = "Run one connector in a micro-VM."
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Run one connector in a micro-VM. Does not return: libkrun takes over
    /// the process and exits with the guest workload's code.
    Run(launch::Args),

    /// Compile a policy into its nftables ruleset and write it to stdout.
    PrintRuleset {
        /// The policy JSON, as the launcher writes it into /init/policy.json.
        #[arg(long, value_name = "PATH")]
        policy: std::path::PathBuf,

        /// An IPv4 subnet of the VMM's own interfaces, excluded alongside the
        /// baseline. Repeatable. Stands in for the getifaddrs read that `run`
        /// does, and takes an interface address as getifaddrs returns it.
        #[arg(long, value_name = "CIDR", value_parser = vmm_subnet)]
        vmm_subnet: Vec<ipnetwork::Ipv4Network>,
    },
}

fn main() -> std::process::ExitCode {
    let args = match <Args as clap::Parser>::try_parse() {
        Ok(args) => args,
        // `--help` and `--version` are clap's other "errors", and go to stdout.
        Err(error) if !error.use_stderr() => {
            let _ = error.print();
            return std::process::ExitCode::SUCCESS;
        }
        Err(error) => {
            eprint!("{}", framed(&error.render().to_string()));
            return std::process::ExitCode::from(EXIT_FAILED);
        }
    };

    if let Err(error) = run(&args) {
        eprint!("{}", framed(&format!("{error:#}")));
        return std::process::ExitCode::from(EXIT_FAILED);
    }
    std::process::ExitCode::SUCCESS
}

fn run(args: &Args) -> anyhow::Result<()> {
    match &args.command {
        Command::Run(args) => {
            let Err(error) = launch::run(args);
            Err(error)
        }
        Command::PrintRuleset {
            policy,
            vmm_subnet: vmm_subnets,
        } => {
            let policy = policy::load(policy)?;
            print!("{}", ruleset::render(&policy, vmm_subnets)?);
            Ok(())
        }
    }
}

/// IPv4 only: the guest has IPv6 disabled and the ruleset drops it outright,
/// so an IPv6 subnet has nothing to exclude.
fn vmm_subnet(raw: &str) -> Result<ipnetwork::Ipv4Network, String> {
    if raw.contains(':') {
        return Err("expected an IPv4 subnet; the ruleset drops IPv6 outright".to_string());
    }
    raw.parse().map_err(|e| format!("{e}"))
}

/// Prefix every line so neither clap's indentation nor a diagnostic of our
/// own can trigger the launcher's readiness signal (a leading space on
/// stderr). connector-init's own marker and the workload's stderr pass through
/// the guest console untouched.
fn framed(message: &str) -> String {
    let mut framed = String::with_capacity(message.len());

    for line in message.trim_end().lines() {
        framed.push_str("flow-connector-vmm:");
        if !line.is_empty() {
            framed.push(' ');
            framed.push_str(line);
        }
        framed.push('\n');
    }
    framed
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn parse_table() {
        let mut table = String::new();

        for case in cases() {
            table.push_str(&format!("$ {}\n", command_line(&case)));
            match super::Args::try_parse_from(&case) {
                Ok(args) => table.push_str(&describe(&args)),
                Err(error) => table.push_str(&crate::framed(&error.render().to_string())),
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    #[test]
    fn no_rendered_error_line_begins_with_a_space() {
        for case in cases() {
            let Err(error) = super::Args::try_parse_from(&case) else {
                continue;
            };
            for line in crate::framed(&error.render().to_string()).lines() {
                assert!(
                    !line.starts_with(' '),
                    "{case:?} rendered a line beginning with a space: {line:?}"
                );
            }
        }
    }

    fn command_line(argv: &[&str]) -> String {
        argv.iter()
            .map(|argument| match argument.contains(' ') {
                true => format!("{argument:?}"),
                false => argument.to_string(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn cases() -> Vec<Vec<&'static str>> {
        vec![
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--policy",
                "/init/policy.json",
            ],
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--policy",
                "/init/policy.json",
                "--vmm-subnet",
                "10.89.0.4/24",
                "--vmm-subnet",
                "192.0.2.1/30",
            ],
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--policy",
                "/init/policy.json",
                "--vmm-subnet",
                "fd00::/8",
            ],
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--policy",
                "/init/policy.json",
                "--vmm-subnet",
                "10.89.0.0/33",
            ],
            vec!["flow-connector-vmm", "print-ruleset"],
            run(&[]),
            run(&["--persistent-disk", "/acmeCo/state"]),
            // `--exec` takes every remaining argument, flag-shaped ones
            // included, which is why it has to come last.
            run(&["--debug", "--exec", "/bin/sh", "-c", "exit 7"]),
            run(&["--exec", "/bin/sh", "-c", "exit 7", "--debug"]),
            run(&["--exec"]),
            with_mount("relative/mount"),
            with_mount("/"),
            vec!["flow-connector-vmm", "run", "--policy", "/init/policy.json"],
            vec!["flow-connector-vmm"],
        ]
    }

    fn run(extra: &[&'static str]) -> Vec<&'static str> {
        const REQUIRED: &[&str] = &[
            "flow-connector-vmm",
            "run",
            "--policy",
            "/init/policy.json",
            "--connector-mount",
            "/tmp/connector-mounts-0/mount-acme",
            "--memory-mib",
            "1024",
            "--vcpus",
            "2",
            "--disk-mib",
            "512",
        ];
        [REQUIRED, extra].concat()
    }

    /// A `run` line whose `--connector-mount` value is replaced, so a
    /// rejection reaches the value parser rather than clap's duplicate check.
    fn with_mount(mount: &'static str) -> Vec<&'static str> {
        let mut argv = run(&[]);
        let value = argv
            .iter()
            .position(|argument| *argument == "--connector-mount")
            .expect("run() passes --connector-mount")
            + 1;
        argv[value] = mount;
        argv
    }

    fn describe(args: &super::Args) -> String {
        match &args.command {
            super::Command::PrintRuleset {
                policy,
                vmm_subnet: vmm_subnets,
            } => format!(
                "ok: print-ruleset policy={} vmm_subnets={:?}\n",
                policy.display(),
                vmm_subnets
                    .iter()
                    .map(ipnetwork::Ipv4Network::to_string)
                    .collect::<Vec<_>>(),
            ),
            super::Command::Run(args) => format!(
                "ok: run policy={} connector_mount={} memory_mib={} vcpus={} disk_mib={} \
                 persistent_disk={:?} run_as_root={} as_root_exec={:?} debug={} \
                 resolver_upstream={:?} exec={:?}\n",
                args.policy.display(),
                args.connector_mount,
                args.memory_mib,
                args.vcpus,
                args.disk_mib,
                args.persistent_disk,
                args.run_as_root,
                args.as_root_exec,
                args.debug,
                args.resolver_upstream.map(|u| u.to_string()),
                args.exec,
            ),
        }
    }
}
