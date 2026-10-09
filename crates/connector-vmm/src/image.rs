//! The connector image's OCI config, and the two things built from it: the
//! guest's argv and the `.krun_config.json` that carries it.
//!
//! `image-inspect.json` is `podman inspect`'s output verbatim - a one-element
//! array - written by the runtime into the connector mount.

use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::path::Path;

/// What the guest init needs to know about the workload before it execs it.
#[derive(Debug)]
pub struct ImageConfig {
    pub env: Vec<String>,
    pub working_dir: String,
    pub uid: u32,
    pub gid: u32,
    /// The supplementary groups, which replace the guest's own.
    pub groups: Vec<u32>,
    /// The user's home, for an image whose `Env` gives `HOME` no value.
    pub home: String,
}

pub struct Guest<'a> {
    pub connector_mount: &'a str,
    pub persistent_disk: Option<&'a str>,
    pub run_as_root: bool,
    pub as_root_exec: Option<&'a str>,
    /// Replaces the default workload when present.
    pub exec: Option<&'a [String]>,
    pub uid: u32,
    pub gid: u32,
    pub groups: &'a [u32],
    pub vsock_port: u32,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InspectConfig {
    #[serde(default)]
    env: Vec<String>,
    #[serde(default)]
    working_dir: String,
    #[serde(default)]
    user: String,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Inspect {
    config: InspectConfig,
}

/// Parse the inspect config without resolving image accounts.
pub fn read_inspect(path: &Path) -> anyhow::Result<InspectConfig> {
    let content =
        std::fs::read(path).map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
    let (Inspect { config },): (Inspect,) = serde_json::from_slice(&content)
        .map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
    Ok(config)
}

/// Resolve the inspect config's `User` against the image's own passwd and
/// group databases, which are reachable because the image is mounted at
/// `rootfs`.
pub fn load(config: InspectConfig, rootfs: &Path) -> anyhow::Result<ImageConfig> {
    let working_dir = match config.working_dir.as_str() {
        "" => "/".to_string(),
        dir => dir.to_string(),
    };
    let root = std::fs::File::open(rootfs)
        .map_err(|e| anyhow::anyhow!("opening {}: {e}", rootfs.display()))?;
    let (uid, gid, groups, home) = resolve_user(
        &config.user,
        &read_db(&root, c"/etc/passwd")?,
        &read_db(&root, c"/etc/group")?,
        &working_dir,
    )?;

    Ok(ImageConfig {
        env: config.env,
        working_dir,
        uid,
        gid,
        groups,
        home,
    })
}

/// `User` is `[user][:group]`, either side numeric or a name.
///
/// Empty means root, which is what a container gives an image that set no
/// user, and so does an empty user before a colon. A name that does not
/// resolve is an error rather than a silent fall back to root: escalating the
/// workload is the wrong way to fail.
///
/// A colon suppresses memberships; an empty group preserves the passwd gid.
/// An empty `User` applies uid 0's memberships without adding its primary gid.
///
/// HOME keeps the selected entry's home, including empty. Without an entry,
/// an explicit uid defaults to `working_dir`, an empty user to `/`.
pub fn resolve_user(
    user: &str,
    passwd: &str,
    group: &str,
    working_dir: &str,
) -> anyhow::Result<(u32, u32, Vec<u32>, String)> {
    if user.is_empty() {
        let root = passwd_by_uid(passwd, 0);
        let groups = supplementary_groups(group, None, root.map(|(name, ..)| name));
        return Ok((
            0,
            0,
            groups,
            root.map_or("/", |(.., home)| home).to_string(),
        ));
    }
    let (name, wanted_group) = match user.split_once(':') {
        Some((name, group)) => (name, Some(group)),
        None => (user, None),
    };
    let (name, no_entry_home) = match name {
        "" => ("0", "/"),
        name => (name, working_dir),
    };

    // The entry's name is what member lists hold, whichever way it was found.
    let (uid, primary_gid, entry, home) = match name.parse::<u32>() {
        // A numeric user is still looked up, because its passwd entry carries
        // the primary group: an image whose `USER` is `4` runs as 4:100 when
        // passwd holds `sync:x:4:100`, not as 4:0. Only a uid with no entry
        // falls back to group 0.
        Ok(uid) => match passwd_by_uid(passwd, uid) {
            Some((entry, gid, home)) => (uid, gid, Some(entry), home),
            None => (uid, 0, None, no_entry_home),
        },
        Err(_) => {
            let (uid, gid, home) = lookup_passwd(passwd, name).ok_or_else(|| {
                anyhow::anyhow!("user {name:?} is not in the image's /etc/passwd")
            })?;
            (uid, gid, Some(name), home)
        }
    };
    let home = home.to_string();

    let Some(wanted_group) = wanted_group else {
        let groups = supplementary_groups(group, Some(primary_gid), entry);
        return Ok((uid, primary_gid, groups, home));
    };
    let gid = match wanted_group {
        "" => primary_gid,
        wanted_group => match wanted_group.parse::<u32>() {
            Ok(gid) => gid,
            Err(_) => lookup_field(group, wanted_group, 2).ok_or_else(|| {
                anyhow::anyhow!("group {wanted_group:?} is not in the image's /etc/group")
            })?,
        },
    };
    Ok((uid, gid, vec![gid], home))
}

/// The argv libkrun's guest init execs: `flow-guest-init` always leads, and
/// everything after `--` is the workload it becomes.
///
/// `krun_set_exec` is deliberately never called. libkrun's init consults `Cmd`
/// only when `KRUN_INIT` - which `krun_set_exec` sets - is absent, so setting
/// both would silently discard this argv.
pub fn guest_argv(guest: &Guest) -> Vec<String> {
    let mount = guest.connector_mount;
    let workload = guest.exec.map(<[String]>::to_vec).unwrap_or_else(|| {
        vec![
            format!("{mount}/flow-connector-init"),
            format!("--image-inspect-json-path={mount}/image-inspect.json"),
            format!("--vsock-port={}", guest.vsock_port),
        ]
    });

    let mut argv = vec![
        crate::launch::GUEST_INIT.to_string(),
        "--guest-ip".to_string(),
        format!("{}/{}", crate::net::GUEST_IP, crate::net::PREFIX_LEN),
        "--gateway".to_string(),
        crate::net::VMM_IP.to_string(),
        "--nameserver".to_string(),
        crate::net::VMM_IP.to_string(),
        "--uid".to_string(),
        guest.uid.to_string(),
        "--gid".to_string(),
        guest.gid.to_string(),
    ];
    for gid in guest.groups {
        argv.push("--supplementary-gid".to_string());
        argv.push(gid.to_string());
    }
    argv.push("--connector-mount".to_string());
    argv.push(mount.to_string());
    if let Some(guest_path) = guest.persistent_disk {
        argv.push("--persistent-disk".to_string());
        argv.push(guest_path.to_string());
    }
    if guest.run_as_root {
        argv.push("--run-as-root".to_string());
    }
    if let Some(command) = guest.as_root_exec {
        argv.push("--as-root-exec".to_string());
        argv.push(command.to_string());
    }
    argv.push("--".to_string());
    argv.extend(workload);
    argv
}

/// `/.krun_config.json`, the only channel into libkrun's guest init: it reads
/// `Cmd`, `WorkingDir` and `Env` from here and execs.
///
/// `overrides` lead the array deliberately. The init applies most entries with
/// `setenv(name, value, 0)`, so the first occurrence of a name wins and the
/// runtime's environment contract overrides anything baked into the image.
///
/// `HOME` and `TERM` use last-wins `setenv`. Append the user's home when the
/// image's `HOME` is missing or empty, as podman does.
pub fn krun_config(config: &ImageConfig, cmd: &[String], overrides: &[(&str, String)]) -> Vec<u8> {
    let mut env: Vec<String> = overrides
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    env.extend(config.env.iter().cloned());

    let image_home = config
        .env
        .iter()
        .rev()
        .find_map(|entry| entry.strip_prefix("HOME="));
    if image_home.is_none_or(str::is_empty) {
        env.push(format!("HOME={}", config.home));
    }

    serde_json::to_vec(&serde_json::json!({
        "Cmd": cmd,
        "WorkingDir": config.working_dir,
        "Env": env,
    }))
    .expect("a map of strings always serializes")
}

/// Read the image's account database at `path` as the guest will see it.
/// `RESOLVE_IN_ROOT` resolves absolute links and `..` against `root` and never
/// above it, so an image's `/etc/passwd -> /usr/share/...` reads the image's
/// file and never this VMM's. The kernel enforces that on every step of the
/// walk, which a check of the resolved path could not. There is no fallback
/// without `openat2` (Linux 5.6): the error names it instead.
///
/// A missing passwd or group file, or a link to nothing, is normal for a
/// scratch-based image; it just means no name can resolve.
fn read_db(root: &std::fs::File, path: &std::ffi::CStr) -> anyhow::Result<String> {
    // SAFETY: open_how is three integers, for which zero is valid.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = (libc::O_RDONLY | libc::O_CLOEXEC) as u64;
    // RESOLVE_IN_ROOT also refuses magic links today, but its documentation
    // reserves the right to change that.
    how.resolve = libc::RESOLVE_IN_ROOT | libc::RESOLVE_NO_MAGICLINKS;

    // EAGAIN is the kernel declining to vouch for a `..` that raced a rename
    // or mount anywhere on the host, and asking the caller to retry.
    let mut tries = 0;
    let fd = loop {
        // SAFETY: `path` is NUL-terminated and `how` outlives the call, which
        // is given its size.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                root.as_raw_fd(),
                path.as_ptr(),
                &how,
                std::mem::size_of::<libc::open_how>(),
            )
        };
        if fd >= 0 {
            break fd as RawFd;
        }
        let e = std::io::Error::last_os_error();
        tries += 1;
        if e.raw_os_error() == Some(libc::EAGAIN) && tries < 32 {
            continue;
        }
        if e.kind() == std::io::ErrorKind::NotFound {
            return Ok(String::new());
        }
        anyhow::bail!(
            "opening the image's {} with openat2: {e}",
            path.to_string_lossy()
        );
    };

    let mut content = String::new();
    // SAFETY: openat2 returned a descriptor nothing else owns.
    unsafe { std::fs::File::from_raw_fd(fd) }
        .read_to_string(&mut content)
        .map_err(|e| anyhow::anyhow!("reading the image's {}: {e}", path.to_string_lossy()))?;
    Ok(content)
}

/// The name, primary group and home of the first passwd entry for `uid`,
/// found by uid rather than by name. `None` when no entry has it.
///
/// Numeric lookups accept truncated records with an empty missing home.
fn passwd_by_uid(db: &str, uid: u32) -> Option<(&str, u32, &str)> {
    for line in db.lines() {
        let fields: Vec<&str> = line.split(':').collect();

        if fields.get(2).and_then(|value| value.parse::<u32>().ok()) == Some(uid) {
            let home = fields.get(5).copied().unwrap_or("");
            return Some((fields[0], fields.get(3)?.parse().ok()?, home));
        }
    }
    None
}

/// `primary`, then the gid of every group whose member list names `user`,
/// each once and in file order. No `user` means no entry to be listed under.
fn supplementary_groups(db: &str, primary: Option<u32>, user: Option<&str>) -> Vec<u32> {
    let mut groups: Vec<u32> = primary.into_iter().collect();
    let Some(user) = user else {
        return groups;
    };
    for line in db.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        let listed = fields
            .get(3)
            .is_some_and(|members| members.split(',').any(|member| member == user));
        let Some(gid) = fields.get(2).and_then(|value| value.parse().ok()) else {
            continue;
        };
        if listed && !groups.contains(&gid) {
            groups.push(gid);
        }
    }
    groups
}

/// The uid, primary group and home of the first passwd entry named `name`.
///
/// Podman's named lookup stops at the first record without seven fields;
/// numeric lookup remains lenient.
fn lookup_passwd<'a>(db: &'a str, name: &str) -> Option<(u32, u32, &'a str)> {
    for line in db.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() != 7 {
            return None;
        }
        if fields[0] == name {
            return Some((fields[2].parse().ok()?, fields[3].parse().ok()?, fields[5]));
        }
    }
    None
}

/// `name:x:gid:...`, as group holds it.
fn lookup_field(db: &str, name: &str, field: usize) -> Option<u32> {
    for line in db.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.first() == Some(&name) {
            return fields.get(field).and_then(|value| value.parse().ok());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{Guest, ImageConfig};

    const MOUNT: &str = "/tmp/connector-mounts-0/mount-acme";

    #[test]
    fn guest_argv() {
        let exec: Vec<String> = ["/bin/sh", "-c", "exit 7"]
            .iter()
            .map(ToString::to_string)
            .collect();
        let mut table = String::new();

        for (name, guest) in [
            ("default workload", base()),
            (
                "with a persistent disk",
                Guest {
                    persistent_disk: Some("/acmeCo/state"),
                    ..base()
                },
            ),
            (
                "as root, with a root command first",
                Guest {
                    run_as_root: true,
                    as_root_exec: Some("sysctl -w vm.drop_caches=3"),
                    ..base()
                },
            ),
            (
                "--exec replaces the workload, not the init",
                Guest {
                    exec: Some(&exec),
                    ..base()
                },
            ),
            (
                "root image user, in no supplementary groups",
                Guest {
                    uid: 0,
                    gid: 0,
                    groups: &[],
                    ..base()
                },
            ),
        ] {
            table.push_str(&format!("## {name}\n"));
            for argument in super::guest_argv(&guest) {
                table.push_str(&format!("{argument}\n"));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    #[test]
    fn krun_config() {
        let config = ImageConfig {
            env: vec![
                "PATH=/usr/local/bin:/usr/bin".to_string(),
                "LOG_LEVEL=trace".to_string(),
                "CONNECTOR_MOUNT=/somewhere/else".to_string(),
            ],
            working_dir: "/opt/acmeCo".to_string(),
            uid: 1000,
            gid: 1000,
            groups: vec![1000],
            home: "/home/acmesvc".to_string(),
        };
        let mut table = String::new();

        for (name, guest, overrides) in [
            (
                "the full contract",
                base(),
                vec![
                    ("CONNECTOR_MOUNT", MOUNT.to_string()),
                    ("LOG_FORMAT", "json".to_string()),
                    ("LOG_LEVEL", "warn".to_string()),
                ],
            ),
            (
                "the runtime set no log variables",
                base(),
                vec![("CONNECTOR_MOUNT", MOUNT.to_string())],
            ),
            (
                "--run-as-root keeps the image user's identity and HOME",
                Guest {
                    run_as_root: true,
                    as_root_exec: Some("env"),
                    ..base()
                },
                vec![("CONNECTOR_MOUNT", MOUNT.to_string())],
            ),
        ] {
            let cmd = super::guest_argv(&guest);
            let rendered = super::krun_config(&config, &cmd, &overrides);
            let parsed: serde_json::Value =
                serde_json::from_slice(&rendered).expect("krun_config emits JSON");

            table.push_str(&format!("## {name}\n"));
            table.push_str(&serde_json::to_string_pretty(&parsed).unwrap());
            table.push_str("\n\n");
        }
        insta::assert_snapshot!(table);
    }

    /// Image HOME precedence under libkrun's last-wins parser.
    #[test]
    fn krun_config_home() {
        let cmd = super::guest_argv(&base());
        let mut table = String::new();

        for (name, image_env) in [
            ("the image sets no HOME", vec![]),
            ("the image's own HOME", vec!["HOME=/opt/acmeCo"]),
            ("an empty image HOME, which podman replaces", vec!["HOME="]),
        ] {
            let config = ImageConfig {
                env: ["PATH=/usr/bin"]
                    .into_iter()
                    .chain(image_env)
                    .map(ToString::to_string)
                    .collect(),
                working_dir: "/".to_string(),
                uid: 1000,
                gid: 1001,
                groups: vec![1001],
                home: "/home/acmesvc".to_string(),
            };
            let rendered =
                super::krun_config(&config, &cmd, &[("CONNECTOR_MOUNT", MOUNT.to_string())]);
            let parsed: serde_json::Value =
                serde_json::from_slice(&rendered).expect("krun_config emits JSON");

            table.push_str(&format!("## {name}\n"));
            for entry in parsed["Env"].as_array().expect("Env is an array") {
                table.push_str(&format!("{}\n", entry.as_str().expect("Env holds strings")));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    /// Matches identities observed with rootful Podman 4.9.3 and runc 1.3.4.
    #[test]
    fn user_resolution() {
        const PASSWD: &str = "root:x:0:0:root:/root:/bin/sh\n\
            acmesvc:x:1000:1001::/home/acmesvc:/bin/sh\n\
            acmealias:x:1000:1005::/:/bin/sh\n\
            acmesolo:x:1006:1006::/:/bin/sh\n";
        const GROUP: &str = "root:x:0:\n\
            acmeprimary:x:1001:\n\
            acmedata:x:1002:acmesvc\n\
            acmeextra:x:1004:acmeother,acmesvc\n\
            acmealiasgrp:x:1005:acmealias\n\
            acmedup:x:1002:acmesvc\n\
            acmeroot:x:1007:root\n\
            acmesolo:x:1006:\n";

        let cases: &[(&str, &str, &str, &[&str])] = &[
            (
                "both files",
                PASSWD,
                GROUP,
                &[
                    "",
                    "0",
                    "root",
                    "0:0",
                    "acmesvc",
                    "1000",
                    "acmealias",
                    "acmesolo",
                    "4242",
                    "acmesvc:acmedata",
                    "acmesvc:1004",
                    "acmesvc:4343",
                    "1000:acmeextra",
                    "4242:acmedata",
                    "4242:4343",
                    "acmesvc:",
                    "1000:",
                    "4242:",
                    "root:",
                    ":acmedata",
                    ":1002",
                    ":4343",
                    ":",
                    "nobody",
                    "acmesvc:nogroup",
                    ":nogroup",
                ],
            ),
            (
                "no /etc/group",
                PASSWD,
                "",
                &[
                    "",
                    "0",
                    "acmesvc",
                    "1000",
                    "acmesvc:acmedata",
                    "acmesvc:1002",
                    "acmesvc:",
                    "1000:",
                    ":acmedata",
                    ":1002",
                    ":",
                ],
            ),
            (
                "no /etc/passwd",
                "",
                GROUP,
                &[
                    "",
                    "0",
                    "acmesvc",
                    "1000",
                    "1000:acmedata",
                    "acmesvc:",
                    "1000:",
                    ":acmedata",
                    ":",
                ],
            ),
            (
                "neither, as in a scratch image",
                "",
                "",
                &[
                    "",
                    "0",
                    "1000",
                    "1000:1002",
                    "acmesvc",
                    "acmesvc:",
                    "1000:",
                    ":1002",
                    ":",
                ],
            ),
        ];

        let mut table = String::new();
        for (name, passwd, group, users) in cases {
            table.push_str(&format!("## {name}\n"));
            for user in *users {
                let outcome = match super::resolve_user(user, passwd, group, "/") {
                    Ok((uid, gid, groups, _)) => identity(uid, gid, &groups),
                    Err(error) => format!("refused: {error:#}"),
                };
                table.push_str(&format!("{user:?} -> {outcome}\n"));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    /// A named alias keeps its home; a uid selects the first matching entry.
    #[test]
    fn home_resolution() {
        const PASSWD: &str = "root:x:0:0:root:/acmeroot:/bin/sh\n\
            acmesvc:x:1000:1001::/home/acmesvc:/bin/sh\n\
            acmealias:x:1000:1005::/home/acmealias:/bin/sh\n\
            acmeempty:x:1006:1006:::/bin/sh\n\
            acmefirst:x:1008:1008:::/bin/sh\n\
            acmesecond:x:1008:1008::/home/acmesecond:/bin/sh\n\
            acmeshort:x:1009:1009\n";
        const NO_ROOT: &str = "acmesvc:x:1000:1001::/home/acmesvc:/bin/sh\n";
        const GROUP: &str = "root:x:0:\nacmedata:x:1002:acmesvc\n";

        let cases: &[(&str, &str, &str, &[&str])] = &[
            (
                "both files, working dir /acmework",
                PASSWD,
                "/acmework",
                &[
                    "",
                    "0",
                    "root",
                    "0:0",
                    ":",
                    ":1002",
                    "acmesvc",
                    "1000",
                    "acmealias",
                    "acmealias:1002",
                    "acmeempty",
                    "1006",
                    "1008",
                    "acmefirst",
                    "acmesecond",
                    "1009",
                    "acmesvc:acmedata",
                    "1000:1002",
                    "acmesvc:",
                    "1000:",
                    "4242",
                    "4242:1002",
                ],
            ),
            (
                "both files, no working dir",
                PASSWD,
                "/",
                &["4242", "4242:", "4242:1002"],
            ),
            (
                "no entry for uid 0, working dir /acmework",
                NO_ROOT,
                "/acmework",
                &["", "0", ":1002", "root"],
            ),
            (
                "no /etc/passwd, working dir /acmework",
                "",
                "/acmework",
                &["", "0", "1000", ":", ":1002"],
            ),
            (
                "no /etc/passwd, no working dir",
                "",
                "/",
                &["", "0", "1000", "1000:1002", ":1002"],
            ),
        ];

        let mut table = String::new();
        for (name, passwd, working_dir, users) in cases {
            table.push_str(&format!("## {name}\n"));
            for user in *users {
                let outcome = match super::resolve_user(user, passwd, GROUP, working_dir) {
                    Ok((uid, .., home)) => format!("uid {uid} HOME={home:?}"),
                    Err(error) => format!("refused: {error:#}"),
                };
                table.push_str(&format!("{user:?} -> {outcome}\n"));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    #[test]
    fn passwd_field_counts() {
        const ROOT: &str = "root:x:0:0:root:/acmeroot:/bin/sh\n";

        let cases: &[(&str, &str, &[&str])] = &[
            (
                "seven fields, one with an empty home and one with an empty shell",
                "acmeemptyhome:x:1006:1006:::/bin/sh\n\
                 acmeemptyshell:x:1012:1012::/home/acmeemptyshell:\n",
                &["acmeemptyhome", "1006", "acmeemptyshell", "1012"],
            ),
            (
                "six fields, no shell",
                "acmesix:x:1011:1011::/home/acmesix\n",
                &["acmesix", "1011"],
            ),
            (
                "eight fields",
                "acmeeight:x:1014:1014::/home/acmeeight:/bin/sh:extra\n",
                &["acmeeight", "1014"],
            ),
            (
                "seven fields below six",
                "acmesix:x:1011:1011::/home/acmesix\n\
                 acmeafter:x:1020:1020::/home/acmeafter:/bin/sh\n",
                &["root", "acmeafter", "acmeafter:0", "1020"],
            ),
        ];

        let mut table = String::new();
        for (name, entries, users) in cases {
            table.push_str(&format!("## {name}\n"));
            let passwd = format!("{ROOT}{entries}");
            for user in *users {
                let outcome = match super::resolve_user(user, &passwd, "root:x:0:\n", "/acmework") {
                    Ok((uid, gid, groups, home)) => {
                        format!("{} HOME={home:?}", identity(uid, gid, &groups))
                    }
                    Err(error) => format!("refused: {error:#}"),
                };
                table.push_str(&format!("{user:?} -> {outcome}\n"));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    fn identity(uid: u32, gid: u32, groups: &[u32]) -> String {
        let groups: Vec<String> = groups.iter().map(u32::to_string).collect();
        format!("{uid}:{gid} groups [{}]", groups.join(","))
    }

    /// `load` over real directory trees. `rootfs` is the image root, and
    /// `{host}` in a path or link target is the test directory's absolute
    /// path: inside the image it names a directory that exists only there.
    /// Every decoy sits where a link would land if the host resolved it, and
    /// holds different ids, so each outcome says which database was read.
    #[test]
    fn account_files_resolve_in_the_image_root() {
        use Entry::{File, Link};

        const PASSWD: &str = "root:x:0:0::/root:/bin/sh\nacmesvc:x:1000:1001::/:/bin/sh\n";
        const GROUP: &str = "root:x:0:\nacmegrp:x:1002:acmesvc\n";
        const DECOY_PASSWD: &str = "acmesvc:x:6000:6001::/:/bin/sh\n";
        const DECOY_GROUP: &str = "acmegrp:x:6002:acmesvc\n";

        let cases: &[(&str, &[(&str, Entry)], &[&str])] = &[
            (
                "plain files",
                &[
                    ("rootfs/etc/passwd", File(PASSWD)),
                    ("rootfs/etc/group", File(GROUP)),
                ],
                &["acmesvc:acmegrp", "acmesvc", "1000", "nobody"],
            ),
            (
                "absolute links",
                &[
                    ("rootfs/etc/passwd", Link("{host}/accounts/passwd")),
                    ("rootfs/etc/group", Link("{host}/accounts/group")),
                    ("rootfs{host}/accounts/passwd", File(PASSWD)),
                    ("rootfs{host}/accounts/group", File(GROUP)),
                    ("accounts/passwd", File(DECOY_PASSWD)),
                    ("accounts/group", File(DECOY_GROUP)),
                ],
                &["acmesvc:acmegrp", "acmesvc"],
            ),
            (
                "relative links climbing above the root",
                &[
                    ("rootfs/etc/passwd", Link("../../accounts/passwd")),
                    ("rootfs/etc/group", Link("../../accounts/group")),
                    ("rootfs/accounts/passwd", File(PASSWD)),
                    ("rootfs/accounts/group", File(GROUP)),
                    ("accounts/passwd", File(DECOY_PASSWD)),
                    ("accounts/group", File(DECOY_GROUP)),
                ],
                &["acmesvc:acmegrp"],
            ),
            (
                "/etc an absolute link",
                &[
                    ("rootfs/etc", Link("{host}/accounts")),
                    ("rootfs{host}/accounts/passwd", File(PASSWD)),
                    ("rootfs{host}/accounts/group", File(GROUP)),
                    ("accounts/passwd", File(DECOY_PASSWD)),
                    ("accounts/group", File(DECOY_GROUP)),
                ],
                &["acmesvc:acmegrp"],
            ),
            (
                "/etc a relative link climbing above the root",
                &[
                    ("rootfs/etc", Link("../accounts")),
                    ("rootfs/accounts/passwd", File(PASSWD)),
                    ("rootfs/accounts/group", File(GROUP)),
                    ("accounts/passwd", File(DECOY_PASSWD)),
                    ("accounts/group", File(DECOY_GROUP)),
                ],
                &["acmesvc:acmegrp"],
            ),
            (
                "links dangling in the image, whose targets exist on the host",
                &[
                    ("rootfs/etc/passwd", Link("{host}/accounts/passwd")),
                    ("rootfs/etc/group", Link("{host}/accounts/group")),
                    ("accounts/passwd", File(DECOY_PASSWD)),
                    ("accounts/group", File(DECOY_GROUP)),
                ],
                &["acmesvc", "1000:acmegrp", "1000:1002"],
            ),
            (
                "no account files, as in a scratch image",
                &[("rootfs/bin/connector", File(""))],
                &["", "1000", "1000:1002", "acmesvc"],
            ),
            (
                "a link loop",
                &[
                    ("rootfs/etc/passwd", Link("passwd.d")),
                    ("rootfs/etc/passwd.d", Link("/etc/passwd")),
                    ("rootfs/etc/group", File(GROUP)),
                ],
                &["1000"],
            ),
        ];

        let mut table = String::new();
        for (name, layout, users) in cases {
            table.push_str(&format!("## {name}\n"));
            for user in *users {
                let dir = tempfile::tempdir().unwrap();
                let host = dir.path().to_str().unwrap();
                for (path, entry) in *layout {
                    let path = dir.path().join(path.replace("{host}", host));
                    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                    match entry {
                        File(content) => std::fs::write(&path, content).unwrap(),
                        Link(target) => {
                            std::os::unix::fs::symlink(target.replace("{host}", host), &path)
                                .unwrap()
                        }
                    }
                }
                let inspect = dir.path().join("image-inspect.json");
                std::fs::write(
                    &inspect,
                    serde_json::json!([{"Config": {"User": user}}]).to_string(),
                )
                .unwrap();

                let config = super::read_inspect(&inspect).unwrap();
                let outcome = match super::load(config, &dir.path().join("rootfs")) {
                    Ok(config) => identity(config.uid, config.gid, &config.groups),
                    Err(error) => format!("refused: {error:#}"),
                };
                table.push_str(&format!("{user:?} -> {outcome}\n"));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    enum Entry {
        File(&'static str),
        Link(&'static str),
    }

    fn base() -> Guest<'static> {
        Guest {
            connector_mount: MOUNT,
            persistent_disk: None,
            run_as_root: false,
            as_root_exec: None,
            exec: None,
            uid: 1000,
            gid: 1001,
            groups: &[1001, 1002, 1004],
            vsock_port: 49092,
        }
    }
}
