//! The CLI the VMM builds when it boots a connector image.
//!
//! Everything after `--` is the workload's argv and is not looked at.

use std::net::Ipv4Addr;

#[derive(clap::Parser, Debug)]
#[command(
    name = "flow-guest-init",
    about = "Guest init for a connector micro-VM: network, mounts, environment and user, then exec."
)]
pub struct Args {
    /// The guest end of the tap.
    #[arg(long, value_name = "A.B.C.D/N")]
    pub guest_ip: Cidr,

    /// Next hop for the default route: the VMM's end of the tap.
    #[arg(long, value_name = "A.B.C.D")]
    pub gateway: Ipv4Addr,

    /// Written to /etc/resolv.conf. The VMM's resolver, not an upstream.
    #[arg(long, value_name = "A.B.C.D")]
    pub nameserver: Ipv4Addr,

    /// The image's user, which owns the scratch disk and runs the workload.
    #[arg(long)]
    pub uid: u32,

    /// The image's group.
    #[arg(long)]
    pub gid: u32,

    /// Mount the connector mount share at this absolute guest path, which is
    /// the same path it has on the host. Required: every connector run
    /// receives one, and CONNECTOR_MOUNT in the workload's environment names
    /// it, so a guest without the mount would name a directory that is not
    /// there.
    #[arg(long, value_name = "GUEST_PATH", value_parser = guest_path)]
    pub connector_mount: String,

    /// Mount the task's persistent disk share at this absolute guest path.
    #[arg(long, value_name = "GUEST_PATH", value_parser = guest_path)]
    pub persistent_disk: Option<String>,

    /// Test only: run the workload as guest root instead of --uid/--gid.
    #[arg(long)]
    pub run_as_root: bool,

    /// Test only: run CMD under /bin/sh as guest root before dropping privileges.
    #[arg(long, value_name = "CMD")]
    pub as_root_exec: Option<String>,

    /// The workload to become.
    #[arg(last = true, required = true, value_name = "ARGV")]
    pub argv: Vec<String>,
}

/// An address and prefix length, as `--guest-ip` is written.
#[derive(Clone, Debug)]
pub struct Cidr {
    pub address: Ipv4Addr,
    pub prefix_len: u8,
}

impl std::str::FromStr for Cidr {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let (address, prefix_len) = raw.split_once('/').ok_or("expected A.B.C.D/N")?;
        let address: Ipv4Addr = address.parse().map_err(|e| format!("{e}"))?;
        let prefix_len: u8 = prefix_len.parse().map_err(|e| format!("{e}"))?;

        if !(1..=32).contains(&prefix_len) {
            return Err(format!("prefix length {prefix_len} is not in 1..=32"));
        }
        Ok(Self {
            address,
            prefix_len,
        })
    }
}

/// A relative path would be resolved against the image's `WorkingDir`, and
/// mounting over `/` would replace the root of the shared mount namespace,
/// which silently costs the workload's exit code (see the crate README).
fn guest_path(raw: &str) -> Result<String, String> {
    if !raw.starts_with('/') {
        return Err("expected an absolute guest path".to_string());
    }
    if raw.trim_end_matches('/').is_empty() {
        return Err("the guest root is not a mount point for this share".to_string());
    }
    Ok(raw.to_string())
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

    const FLAGS: &[&str] = &[
        "flow-guest-init",
        "--guest-ip",
        "192.0.2.2/30",
        "--gateway",
        "192.0.2.1",
        "--nameserver",
        "192.0.2.1",
        "--uid",
        "1000",
        "--gid",
        "1000",
        "--connector-mount",
        "/tmp/connector-mounts-0/mount-acme",
    ];

    const WORKLOAD: &[&str] = &[
        "--",
        "/flow-connector-init",
        "--image-inspect-json-path=/image-inspect.json",
        "--vsock-port=49092",
    ];

    fn cases() -> Vec<Vec<&'static str>> {
        vec![
            with(&[]),
            with(&["--persistent-disk", "/acmeCo/state"]),
            with(&[
                "--run-as-root",
                "--as-root-exec",
                "sysctl -w vm.drop_caches=3",
            ]),
            with(&["--persistent-disk", "state"]),
            with(&["--persistent-disk", "/"]),
            with(&["--venv-dax"]),
            without_connector_mount(),
            with_guest_ip("192.0.2.2"),
            with_guest_ip("192.0.2.2/33"),
            FLAGS.to_vec(),
            vec!["flow-guest-init"],
        ]
    }

    fn with(extra: &[&'static str]) -> Vec<&'static str> {
        [FLAGS, extra, WORKLOAD].concat()
    }

    fn without_connector_mount() -> Vec<&'static str> {
        let mut argv = with(&[]);
        let flag = argv
            .iter()
            .position(|argument| *argument == "--connector-mount")
            .expect("FLAGS passes --connector-mount");
        argv.drain(flag..flag + 2);
        argv
    }

    fn with_guest_ip(raw: &'static str) -> Vec<&'static str> {
        let mut argv = with(&[]);
        let value = argv
            .iter()
            .position(|argument| *argument == "--guest-ip")
            .expect("FLAGS passes --guest-ip")
            + 1;
        argv[value] = raw;
        argv
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

    fn describe(args: &super::Args) -> String {
        format!(
            "ok: guest_ip={}/{} gateway={} nameserver={} uid={} gid={} \
             connector_mount={} persistent_disk={:?} run_as_root={} \
             as_root_exec={:?} argv={:?}\n",
            args.guest_ip.address,
            args.guest_ip.prefix_len,
            args.gateway,
            args.nameserver,
            args.uid,
            args.gid,
            args.connector_mount,
            args.persistent_disk,
            args.run_as_root,
            args.as_root_exec,
            args.argv,
        )
    }
}
