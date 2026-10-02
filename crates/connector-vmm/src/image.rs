//! The connector image's OCI config, and the two things built from it: the
//! guest's argv and the `.krun_config.json` that carries it.
//!
//! `image-inspect.json` is `podman inspect`'s output verbatim - a one-element
//! array - written by the runtime into the connector mount.

use std::path::Path;

/// What the guest init needs to know about the workload before it execs it.
#[derive(Debug)]
pub struct ImageConfig {
    pub env: Vec<String>,
    pub working_dir: String,
    pub uid: u32,
    pub gid: u32,
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
    pub vsock_port: u32,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectConfig {
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

/// Read the inspect JSON and resolve its `User` against the image's own
/// passwd and group databases, which are reachable because the image is
/// mounted at `rootfs`.
pub fn load(inspect_path: &Path, rootfs: &Path) -> anyhow::Result<ImageConfig> {
    let content = std::fs::read(inspect_path)
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", inspect_path.display()))?;
    let (Inspect { config },): (Inspect,) = serde_json::from_slice(&content)
        .map_err(|e| anyhow::anyhow!("parsing {}: {e}", inspect_path.display()))?;

    let (uid, gid) = resolve_user(
        &config.user,
        &read_db(&rootfs.join("etc/passwd"))?,
        &read_db(&rootfs.join("etc/group"))?,
    )?;

    Ok(ImageConfig {
        env: config.env,
        working_dir: match config.working_dir.as_str() {
            "" => "/".to_string(),
            dir => dir.to_string(),
        },
        uid,
        gid,
    })
}

/// `User` is `[user][:group]`, either side numeric or a name.
///
/// Empty means root, which is what a container gives an image that set no
/// user. A name that does not resolve is an error rather than a silent fall
/// back to root: escalating the workload is the wrong way to fail.
pub fn resolve_user(user: &str, passwd: &str, group: &str) -> anyhow::Result<(u32, u32)> {
    if user.is_empty() {
        return Ok((0, 0));
    }
    let (name, wanted_group) = match user.split_once(':') {
        Some((name, group)) => (name, Some(group)),
        None => (user, None),
    };

    let (uid, primary_gid) = match name.parse::<u32>() {
        // A numeric user is still looked up, because its passwd entry carries
        // the primary group: an image whose `USER` is `4` runs as 4:100 when
        // passwd holds `sync:x:4:100`, not as 4:0. Only a uid with no entry
        // falls back to group 0.
        Ok(uid) => (uid, primary_group(passwd, uid).unwrap_or(0)),
        Err(_) => lookup_passwd(passwd, name)
            .ok_or_else(|| anyhow::anyhow!("user {name:?} is not in the image's /etc/passwd"))?,
    };

    let Some(wanted_group) = wanted_group else {
        return Ok((uid, primary_gid));
    };
    let gid = match wanted_group.parse::<u32>() {
        Ok(gid) => gid,
        Err(_) => lookup_field(group, wanted_group, 2).ok_or_else(|| {
            anyhow::anyhow!("group {wanted_group:?} is not in the image's /etc/group")
        })?,
    };
    Ok((uid, gid))
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
        "--connector-mount".to_string(),
        mount.to_string(),
    ];
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
/// `overrides` lead the array deliberately. The init applies each entry with
/// `setenv(name, value, 0)`, so the first occurrence of a name wins and the
/// runtime's environment contract overrides anything baked into the image.
pub fn krun_config(config: &ImageConfig, cmd: &[String], overrides: &[(&str, String)]) -> Vec<u8> {
    let mut env: Vec<String> = overrides
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    env.extend(config.env.iter().cloned());

    serde_json::to_vec(&serde_json::json!({
        "Cmd": cmd,
        "WorkingDir": config.working_dir,
        "Env": env,
    }))
    .expect("a map of strings always serializes")
}

/// A missing passwd or group file is normal for a scratch-based image; it just
/// means no name can resolve.
fn read_db(path: &Path) -> anyhow::Result<String> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(anyhow::anyhow!("reading {}: {e}", path.display())),
    }
}

/// The primary group of the passwd entry for `uid`, found by uid rather than
/// by name. `None` when no entry has it.
fn primary_group(db: &str, uid: u32) -> Option<u32> {
    for line in db.lines() {
        let fields: Vec<&str> = line.split(':').collect();

        if fields.get(2).and_then(|value| value.parse::<u32>().ok()) == Some(uid) {
            return fields.get(3).and_then(|value| value.parse().ok());
        }
    }
    None
}

fn lookup_passwd(db: &str, name: &str) -> Option<(u32, u32)> {
    Some((lookup_field(db, name, 2)?, lookup_field(db, name, 3)?))
}

/// `name:x:uid:gid:...` for passwd, `name:x:gid:...` for group.
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
                "root image user",
                Guest {
                    uid: 0,
                    gid: 0,
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
        };
        let cmd = super::guest_argv(&base());
        let mut table = String::new();

        for (name, overrides) in [
            (
                "the full contract",
                vec![
                    ("CONNECTOR_MOUNT", MOUNT.to_string()),
                    ("LOG_FORMAT", "json".to_string()),
                    ("LOG_LEVEL", "warn".to_string()),
                ],
            ),
            (
                "the runtime set no log variables",
                vec![("CONNECTOR_MOUNT", MOUNT.to_string())],
            ),
        ] {
            let rendered = super::krun_config(&config, &cmd, &overrides);
            let parsed: serde_json::Value =
                serde_json::from_slice(&rendered).expect("krun_config emits JSON");

            table.push_str(&format!("## {name}\n"));
            table.push_str(&serde_json::to_string_pretty(&parsed).unwrap());
            table.push_str("\n\n");
        }
        insta::assert_snapshot!(table);
    }

    #[test]
    fn user_resolution() {
        const PASSWD: &str =
            "root:x:0:0:root:/root:/bin/sh\nacme:x:1000:1001::/home/acme:/bin/sh\n";
        const GROUP: &str = "root:x:0:\nacmegrp:x:1002:\n";

        let mut table = String::new();
        for user in [
            "",
            "acme",
            "1000",
            "0",
            "1234",
            "acme:acmegrp",
            "acme:1002",
            "1234:acmegrp",
            "1000:acmegrp",
            "root",
            "nobody",
            "acme:nogroup",
        ] {
            let outcome = match super::resolve_user(user, PASSWD, GROUP) {
                Ok((uid, gid)) => format!("{uid}:{gid}"),
                Err(error) => format!("refused: {error:#}"),
            };
            table.push_str(&format!("{user:?} -> {outcome}\n"));
        }

        // A scratch-based image has no databases at all, so no name resolves.
        for user in ["", "acme"] {
            let outcome = match super::resolve_user(user, "", "") {
                Ok((uid, gid)) => format!("{uid}:{gid}"),
                Err(error) => format!("refused: {error:#}"),
            };
            table.push_str(&format!("{user:?} with no passwd file -> {outcome}\n"));
        }
        insta::assert_snapshot!(table);
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
            vsock_port: 49092,
        }
    }
}
