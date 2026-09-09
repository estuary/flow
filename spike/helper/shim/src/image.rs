//! The connector image's OCI config: what libkrun's guest init needs to know
//! about the workload before it execs it.
//!
//! `image-inspect.json` is `podman inspect`'s output verbatim - a one-element
//! array - written by the reactor and bind-mounted at `/init`.

use std::path::Path;

pub struct ImageConfig {
    pub env: Vec<String>,
    pub working_dir: String,
    pub uid: u32,
    pub gid: u32,
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

/// Reads the inspect JSON and resolves its `User` against the image's own
/// passwd/group databases, which are reachable because the image is mounted at
/// `rootfs`.
pub fn load(inspect_path: &Path, rootfs: &Path) -> anyhow::Result<ImageConfig> {
    let content = std::fs::read(inspect_path)
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", inspect_path.display()))?;
    let (Inspect { config },): (Inspect,) = serde_json::from_slice(&content)
        .map_err(|e| anyhow::anyhow!("parsing {}: {e}", inspect_path.display()))?;

    let (uid, gid) = resolve_user(&config.user, rootfs)?;

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

/// `USER` is `[user][:group]`, either side numeric or a name. An empty or
/// unresolvable name is an error rather than a silent fall back to root.
fn resolve_user(user: &str, rootfs: &Path) -> anyhow::Result<(u32, u32)> {
    if user.is_empty() {
        return Ok((0, 0));
    }
    let (name, group) = match user.split_once(':') {
        Some((name, group)) => (name, Some(group)),
        None => (user, None),
    };

    let passwd = read_db(&rootfs.join("etc/passwd"))?;
    let (uid, primary_gid) = match name.parse::<u32>() {
        Ok(uid) => (uid, 0),
        Err(_) => lookup_passwd(&passwd, name)
            .ok_or_else(|| anyhow::anyhow!("user {name:?} not in the image's /etc/passwd"))?,
    };

    let Some(group) = group else {
        return Ok((uid, primary_gid));
    };
    let gid = match group.parse::<u32>() {
        Ok(gid) => gid,
        Err(_) => {
            let db = read_db(&rootfs.join("etc/group"))?;
            lookup_field(&db, group, 2)
                .ok_or_else(|| anyhow::anyhow!("group {group:?} not in the image's /etc/group"))?
        }
    };
    Ok((uid, gid))
}

/// A missing passwd/group file is normal for a scratch-based image; it just
/// means no name can resolve.
fn read_db(path: &Path) -> anyhow::Result<String> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(anyhow::anyhow!("reading {}: {e}", path.display())),
    }
}

fn lookup_passwd(db: &str, name: &str) -> Option<(u32, u32)> {
    let uid = lookup_field(db, name, 2)?;
    let gid = lookup_field(db, name, 3)?;
    Some((uid, gid))
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

/// `/.krun_config.json`, which is the only channel into libkrun's guest init:
/// it reads `Cmd`, `WorkingDir` and `Env` from here and execs.
///
/// `LOG_FORMAT`/`LOG_LEVEL` lead the array deliberately. The init applies each
/// entry with `setenv(name, value, 0)`, so the first occurrence of a name wins
/// and the reactor's choice overrides anything baked into the image.
pub fn krun_config(config: &ImageConfig, cmd: &[String]) -> Vec<u8> {
    let mut env: Vec<String> = ["LOG_FORMAT", "LOG_LEVEL"]
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| format!("{name}={value}"))
        })
        .collect();
    env.extend(config.env.iter().cloned());

    serde_json::to_vec(&serde_json::json!({
        "Cmd": cmd,
        "WorkingDir": config.working_dir,
        "Env": env,
    }))
    .expect("a map of strings always serializes")
}
