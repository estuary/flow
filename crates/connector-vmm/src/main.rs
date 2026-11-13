//! `flow-connector-vmm` runs one connector in a micro-VM: it compiles the
//! connector's egress policy into the nftables ruleset that bounds the guest's
//! network, serves the guest's DNS, prepares its disks and shares, and enters
//! the VM. The crate README maps out how the pieces fit.
//!
//! `boundary` is the other half of that network, run on the host rather than
//! in a VMM container: the tables outside every VMM's reach.

mod boundary;
mod console;
mod disk;
mod image;
mod ingress;
mod krun;
mod launch;
mod net;
mod policy;
mod resolver;
mod ruleset;
mod sys;

/// CLI and command failures exit 2. Guest workloads may also return this code.
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
        #[command(flatten)]
        policy: policy::Args,

        /// An IPv4 subnet of the VMM's own interfaces, excluded alongside the
        /// baseline. Repeatable. Stands in for the getifaddrs read that `run`
        /// does, and takes an interface address as getifaddrs returns it.
        #[arg(long, value_name = "CIDR", value_parser = vmm_subnet)]
        vmm_subnet: Vec<ipnetwork::Ipv4Network>,
    },

    /// Install, verify or remove the host's boundary around every VMM bridge.
    /// Runs in the host's network namespace, never in a VMM container.
    Boundary {
        #[command(subcommand)]
        action: boundary::Action,
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
            let policy = policy::check(policy)?;
            print!("{}", ruleset::render(&policy, vmm_subnets, &[]));
            Ok(())
        }
        Command::Boundary { action } => boundary::run(action),
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
    #[test]
    fn parse_table() {
        let mut table = String::new();

        for case in cases() {
            table.push_str(&format!("$ {}\n", command_line(&case)));
            table.push_str(&outcome(&case));
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    /// The test controls parse only with the `test-controls` feature.
    /// `--run-as-root` and `--as-root-exec` parse in neither build.
    #[test]
    fn test_controls() {
        let mut table = String::new();

        for case in [
            // `--exec` takes every remaining argument, flag-shaped ones
            // included, which is why it has to come last.
            run(&["--debug", "--exec", "/bin/sh", "-c", "exit 7"]),
            run(&["--exec", "/bin/sh", "-c", "exit 7", "--debug"]),
            run(&["--exec"]),
            run(&["--resolver-upstream", "192.0.2.53:53"]),
            run(&["--ttl-floor-secs", "3", "--ttl-cap-secs", "10"]),
            run(&["--run-as-root"]),
            run(&["--as-root-exec", "id"]),
        ] {
            table.push_str(&format!("$ {}\n", command_line(&case)));
            table.push_str(&outcome(&case));
            table.push('\n');
        }
        #[cfg(feature = "test-controls")]
        insta::assert_snapshot!("test_controls_enabled", table);
        #[cfg(not(feature = "test-controls"))]
        insta::assert_snapshot!("test_controls_disabled", table);
    }

    #[test]
    fn no_rendered_error_line_begins_with_a_space() {
        for case in cases() {
            let Err(error) = <super::Args as clap::Parser>::try_parse_from(&case) else {
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

    fn outcome(argv: &[&str]) -> String {
        match <super::Args as clap::Parser>::try_parse_from(argv) {
            Ok(args) => describe(&args),
            Err(error) => crate::framed(&error.render().to_string()),
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
            vec!["flow-connector-vmm", "print-ruleset"],
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--vmm-subnet",
                "10.89.0.4/24",
                "--vmm-subnet",
                "192.0.2.1/30",
            ],
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--vmm-subnet",
                "fd00::/8",
            ],
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--vmm-subnet",
                "10.89.0.0/33",
            ],
            vec![
                "flow-connector-vmm",
                "print-ruleset",
                "--egress-host",
                "pypi.org",
                "--egress-cidr",
                "10.0.0.0/8",
            ],
            run(&[]),
            run(&[
                "--egress-host",
                "pypi.org",
                "--egress-host",
                "*.ACMEco.example",
                "--egress-cidr",
                "192.168.4.7/32",
                "--egress-cidr",
                "10.0.0.0/8",
            ]),
            run(&["--egress-host", "*.com"]),
            run(&["--egress-host", "169.254.169.254"]),
            run(&["--egress-cidr", "10.1.2.3/8"]),
            run(&["--egress-cidr", "fd00::/8"]),
            run(&["--policy", "/init/policy.json"]),
            run(&["--persistent-disk", "/acmeCo/state"]),
            with("--connector-mount", "relative/mount"),
            with("--connector-mount", "/"),
            with("--vcpus", "256"),
            with("--memory-mib", "4294967296"),
            with("--disk-mib", "-1"),
            run(&["--vcpus", "4"]),
            vec!["flow-connector-vmm", "run"],
            vec!["flow-connector-vmm"],
            vec!["flow-connector-vmm", "boundary", "install"],
            vec!["flow-connector-vmm", "boundary", "verify"],
            vec!["flow-connector-vmm", "boundary", "remove"],
            vec!["flow-connector-vmm", "boundary"],
            vec!["flow-connector-vmm", "boundary", "flush"],
            grant(&["--port", "8080"]),
            grant(&["--cidr", "10.0.0.0/8", "--cidr", "192.168.4.7/32"]),
            grant(&["--port", "8080", "--cidr", "0.0.0.0/0"]),
            grant(&[]),
            grant(&["--cidr", "fd00::/8"]),
            grant(&["--cidr", "10.1.2.3/8"]),
        ]
    }

    fn grant(extra: &[&'static str]) -> Vec<&'static str> {
        const REQUIRED: &[&str] = &[
            "flow-connector-vmm",
            "boundary",
            "grant",
            "--owner",
            "00112233445566778899aabbccddeeff",
            "--bridge",
            "fvm0123456789ab",
        ];
        [REQUIRED, extra].concat()
    }

    fn run(extra: &[&'static str]) -> Vec<&'static str> {
        const REQUIRED: &[&str] = &[
            "flow-connector-vmm",
            "run",
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

    /// A `run` line with one flag's value replaced, so a rejection reaches
    /// the value parser rather than clap's duplicate check.
    fn with(flag: &'static str, value: &'static str) -> Vec<&'static str> {
        let mut argv = run(&[]);
        let position = argv
            .iter()
            .position(|argument| *argument == flag)
            .expect("run() passes the flag")
            + 1;
        argv[position] = value;
        argv
    }

    fn describe(args: &super::Args) -> String {
        match &args.command {
            super::Command::PrintRuleset {
                policy,
                vmm_subnet: vmm_subnets,
            } => format!(
                "ok: print-ruleset {} vmm_subnets={:?}\n",
                describe_policy(policy),
                vmm_subnets
                    .iter()
                    .map(ipnetwork::Ipv4Network::to_string)
                    .collect::<Vec<_>>(),
            ),
            super::Command::Run(args) => describe_launch("run", args),
            super::Command::Boundary { action } => format!("ok: boundary {action:?}\n"),
        }
    }

    fn describe_policy(policy: &crate::policy::Args) -> String {
        format!(
            "egress_hosts={:?} egress_cidrs={:?} ttl={}..{}",
            policy.egress_host, policy.egress_cidr, policy.ttl_floor_secs, policy.ttl_cap_secs,
        )
    }

    fn describe_launch(command: &str, args: &crate::launch::Args) -> String {
        format!(
            "ok: {command} {} connector_mount={} memory_mib={} vcpus={} disk_mib={} \
             persistent_disk={:?} debug={} resolver_upstream={:?} exec={:?}\n",
            describe_policy(&args.policy),
            args.connector_mount,
            args.memory_mib,
            args.vcpus,
            args.disk_mib,
            args.persistent_disk,
            args.debug,
            args.resolver_upstream.map(|u| u.to_string()),
            args.exec,
        )
    }
}
