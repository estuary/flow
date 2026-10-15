//! The ownership record of one VMM launch: `<STATE_DIR>/fv_<id>.owner`.
//!
//! A launch claims its id by making its record, and holds an exclusive
//! `flock` on it for as long as it lives. The kernel drops the lock with the
//! last descriptor of the record's open file, however its process ends, so a
//! record whose lock can be taken is one whose owner is dead: no PID is
//! involved, and PID namespaces don't matter. The commands which create a
//! launch's network and container are given that same open file as their
//! stdin, so the lock is also held for as long as any of them runs, even past
//! the owner's death. Whoever takes the lock may therefore treat what it then
//! finds as final.
//!
//! The record is append-only JSON lines. The first claims the id, and names
//! the owner token and the connector mount. Each later line marks a directory
//! the launch made by exclusive creation, written before anything is put in
//! it. A container or network is the launch's if it carries the token as its
//! `OWNER_LABEL`, which podman stores with it as it's created, so neither
//! needs a line of its own. Nothing a record doesn't prove the launch made is
//! ever removed on its say-so.

use anyhow::Context;
use std::io::{Read, Write};

/// The label podman's records of a launch's network and container carry,
/// whose value is the launch's owner token.
pub(crate) const OWNER_LABEL: &str = "dev.estuary.vmm-owner";

const VERSION: u32 = 1;
const SUFFIX: &str = ".owner";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Claim {
    version: u32,
    id: String,
    token: String,
    mount: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Created {
    created: Dir,
}

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Dir {
    /// The connector mount, `mount-fv_<id>`.
    Mount,
    /// The state directory, `<STATE_DIR>/fv_<id>`.
    State,
}

/// What a well-formed record says its launch owns.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Record {
    /// `fv_<16 hex>`, the name of the launch's container and network.
    pub name: String,
    /// 32 hex digits, the value of `OWNER_LABEL`.
    pub token: String,
    pub mount: String,
    /// `<STATE_DIR>/fv_<id>`, beside the record.
    pub state: String,
    pub created_mount: bool,
    pub created_state: bool,
}

pub(crate) fn path(state_dir: &str, name: &str) -> String {
    format!("{state_dir}/{name}{SUFFIX}")
}

pub(crate) fn claim_line(name: &str, token: &str, mount: &str) -> Vec<u8> {
    line(&Claim {
        version: VERSION,
        id: name.to_string(),
        token: token.to_string(),
        mount: mount.to_string(),
    })
}

pub(crate) fn created_line(dir: Dir) -> Vec<u8> {
    line(&Created { created: dir })
}

fn line(value: &impl serde::Serialize) -> Vec<u8> {
    let mut line = serde_json::to_vec(value).expect("a record line serializes");
    line.push(b'\n');
    line
}

pub(crate) fn launch_name(file_name: &str) -> Option<&str> {
    file_name.strip_suffix(SUFFIX).filter(|name| is_name(name))
}

fn is_name(name: &str) -> bool {
    name.strip_prefix("fv_")
        .is_some_and(|id| is_lower_hex(id, 16))
}

fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Read the record of launch `name` in `state_dir` from its `content`.
/// Anything other than exactly what a launch writes is refused, so that a
/// damaged record never directs a removal. A last line without its newline is
/// an append its writer never finished, and is not read.
pub(crate) fn parse(state_dir: &str, name: &str, content: &[u8]) -> anyhow::Result<Record> {
    let complete = match content.iter().rposition(|b| *b == b'\n') {
        Some(end) => &content[..end],
        None => anyhow::bail!("holds no complete claim line"),
    };
    let mut lines = complete.split(|b| *b == b'\n');

    let line = lines.next().expect("split yields a line");

    // First, so that another version is refused as that, whatever its fields.
    #[derive(serde::Deserialize)]
    struct Version {
        version: u32,
    }
    let Version { version } =
        serde_json::from_slice(line).context("reading its claim line's version")?;
    if version != VERSION {
        anyhow::bail!("is version {version}, not {VERSION}");
    }
    let claim: Claim = serde_json::from_slice(line).context("reading its claim line")?;
    if claim.id != name {
        anyhow::bail!("claims id {:?}, not {name:?}", claim.id);
    }
    if !is_lower_hex(&claim.token, 32) {
        anyhow::bail!("has a malformed owner token {:?}", claim.token);
    }
    check_mount(&claim.mount, name)
        .with_context(|| format!("names connector mount {:?}", claim.mount))?;

    let (mut created_mount, mut created_state) = (false, false);
    for (index, line) in lines.enumerate() {
        let Created { created } =
            serde_json::from_slice(line).with_context(|| format!("reading line {}", index + 2))?;
        let seen = match created {
            Dir::Mount => &mut created_mount,
            Dir::State => &mut created_state,
        };
        if std::mem::replace(seen, true) {
            anyhow::bail!("marks {created:?} as created twice");
        }
    }

    Ok(Record {
        name: name.to_string(),
        token: claim.token,
        mount: claim.mount,
        state: format!("{state_dir}/{name}"),
        created_mount,
        created_state,
    })
}

/// A launch's connector mount is `mount-<name>` in a `connector-mounts-<uid>`
/// directory, given by an absolute path of plain components.
fn check_mount(mount: &str, name: &str) -> anyhow::Result<()> {
    let Some(relative) = mount.strip_prefix('/') else {
        anyhow::bail!("is not an absolute path");
    };
    let components: Vec<&str> = relative.split('/').collect();
    if components
        .iter()
        .any(|component| matches!(*component, "" | "." | ".."))
    {
        anyhow::bail!("is not a plain path");
    }
    let [.., parent, base] = components[..] else {
        anyhow::bail!("is not within a connector-mounts directory");
    };
    if base != format!("mount-{name}") {
        anyhow::bail!("is not named mount-{name}");
    }
    let uid = parent.strip_prefix("connector-mounts-").unwrap_or_default();
    if uid.is_empty() || !uid.bytes().all(|b| b.is_ascii_digit()) {
        anyhow::bail!("is not within a connector-mounts-<uid> directory");
    }
    Ok(())
}

/// Make the record of launch `name` in `state_dir`, holding `claim`, and
/// return it locked. It's written, locked and synced as an unnamed file and
/// only then linked under its name, which fails if the name exists: no one
/// can find it unlocked or empty, and a claim is exclusive.
pub(crate) fn claim(state_dir: &str, name: &str, claim: &[u8]) -> std::io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        // Its offset is shared with each fenced command, which has it as
        // stdin, so only appending can be relied on to write at the end.
        .append(true)
        .custom_flags(libc::O_TMPFILE)
        // Readable by any launcher sharing the state directory, which needs
        // only to lock it.
        .mode(0o644)
        .open(state_dir)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o644))?;
    file.write_all(claim)?;
    file.lock()?;
    file.sync_all()?;

    // Linking an unnamed file by descriptor takes CAP_DAC_READ_SEARCH;
    // linking it through /proc takes nothing.
    let from = std::ffi::CString::new(format!("/proc/self/fd/{}", file.as_raw_fd()))
        .expect("a /proc path has no NUL");
    let to = std::ffi::CString::new(path(state_dir, name)).map_err(std::io::Error::other)?;
    // SAFETY: both paths are NUL-terminated and outlive the call.
    let linked = unsafe {
        libc::linkat(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::AT_SYMLINK_FOLLOW,
        )
    };
    if linked != 0 {
        return Err(std::io::Error::last_os_error());
    }
    sync_dir(state_dir)?;
    Ok(file)
}

/// Append `line` to a record its writer holds.
pub(crate) fn append(file: &mut std::fs::File, line: &[u8]) -> std::io::Result<()> {
    file.write_all(line)?;
    file.sync_data()
}

/// Take the record at `path` if its owner is dead, returning it locked with
/// its content. None if another holds it, being alive or releasing it, or if
/// it was removed or replaced since its name was read.
pub(crate) fn take(path: &str) -> std::io::Result<Option<(std::fs::File, Vec<u8>)>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let file = match std::fs::OpenOptions::new()
        .read(true)
        // Never through a link, nor waiting on a FIFO planted in its place.
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    match file.try_lock() {
        Ok(()) => (),
        Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
        Err(std::fs::TryLockError::Error(err)) => return Err(err),
    }

    // A releaser which finished first removed the record under its lock.
    let held = file.metadata()?;
    let named = match std::fs::symlink_metadata(path) {
        Ok(named) => named,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if (held.dev(), held.ino()) != (named.dev(), named.ino()) {
        return Ok(None);
    }
    if !held.file_type().is_file() {
        return Err(std::io::Error::other("is not a regular file"));
    }

    let mut content = Vec::new();
    (&file).read_to_end(&mut content)?;
    Ok(Some((file, content)))
}

/// Remove a record its remover holds, once nothing it names remains.
pub(crate) fn remove(state_dir: &str, path: &str) -> std::io::Result<()> {
    std::fs::remove_file(path)?;
    sync_dir(state_dir)
}

fn sync_dir(dir: &str) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(test)]
mod test {
    use super::{Dir, claim_line, created_line, launch_name, parse};

    const NAME: &str = "fv_0123456789abcdef";
    const TOKEN: &str = "00112233445566778899aabbccddeeff";
    const MOUNT: &str = "/tmp/connector-mounts-1000/mount-fv_0123456789abcdef";

    #[test]
    fn records() {
        let claim = String::from_utf8(claim_line(NAME, TOKEN, MOUNT)).unwrap();
        let mount = String::from_utf8(created_line(Dir::Mount)).unwrap();
        let state = String::from_utf8(created_line(Dir::State)).unwrap();
        let with_mount = |mount: &str| String::from_utf8(claim_line(NAME, TOKEN, mount)).unwrap();

        let cases: Vec<(&str, String)> = vec![
            ("just claimed", claim.clone()),
            ("with its mount", format!("{claim}{mount}")),
            ("with both directories", format!("{claim}{mount}{state}")),
            ("a torn marker", format!("{claim}{}", mount.trim_end())),
            ("a torn claim", claim.trim_end().to_string()),
            ("empty", String::new()),
            ("a marker twice", format!("{claim}{mount}{mount}")),
            (
                "an unknown line",
                format!("{claim}{{\"created\":\"network\"}}\n"),
            ),
            ("not JSON", format!("{claim}created mount\n")),
            ("a blank line", format!("{claim}\n{mount}")),
            (
                "another version",
                claim.replace("\"version\":1", "\"version\":2"),
            ),
            (
                "another version, with fields of its own",
                claim.replace("\"version\":1", "\"version\":2,\"pid\":42"),
            ),
            (
                "an unknown field",
                claim.replace("\"version\":1", "\"version\":1,\"pid\":42"),
            ),
            (
                "another id",
                claim.replace(
                    "\"id\":\"fv_0123456789abcdef\"",
                    "\"id\":\"fv_fedcba9876543210\"",
                ),
            ),
            (
                "an upper-case token",
                claim.replace(TOKEN, &TOKEN.to_uppercase()),
            ),
            ("a short token", claim.replace(TOKEN, &TOKEN[..31])),
            (
                "a relative mount",
                with_mount("tmp/connector-mounts-1000/mount-fv_0123456789abcdef"),
            ),
            (
                "another launch's mount",
                with_mount("/tmp/connector-mounts-1000/mount-fv_fedcba9876543210"),
            ),
            (
                "a mount outside connector-mounts",
                with_mount("/home/acme/mount-fv_0123456789abcdef"),
            ),
            (
                "a mount in connector-mounts-",
                with_mount("/tmp/connector-mounts-/mount-fv_0123456789abcdef"),
            ),
            (
                "a mount with ..",
                with_mount(
                    "/tmp/connector-mounts-1000/../connector-mounts-1000/mount-fv_0123456789abcdef",
                ),
            ),
            (
                "a mount with //",
                with_mount("/tmp//connector-mounts-1000/mount-fv_0123456789abcdef"),
            ),
            (
                "a mount with a trailing slash",
                with_mount(&format!("{MOUNT}/")),
            ),
            ("a mount alone", with_mount("/mount-fv_0123456789abcdef")),
        ];

        let mut table = String::new();
        for (label, content) in cases {
            let outcome = match parse("/var/lib/flow/connector-vmm", NAME, content.as_bytes()) {
                Ok(record) => format!("{record:?}"),
                Err(err) => format!("refused: {err:#}"),
            };
            table.push_str(&format!("# {label}\n{content:?}\n=> {outcome}\n\n"));
        }
        insta::assert_snapshot!(table);
    }

    #[test]
    fn launch_names() {
        let names: Vec<String> = [
            "fv_0123456789abcdef.owner",
            "fv_0123456789abcdef",
            "fv_0123456789ABCDEF.owner",
            "fv_0123456789abcde.owner",
            "fv_0123456789abcdef0.owner",
            "fv_0123456789abcdef.owner.tmp",
            "xv_0123456789abcdef.owner",
            ".owner",
        ]
        .iter()
        .map(|file| format!("{file} => {:?}", launch_name(file)))
        .collect();

        insta::assert_snapshot!(names.join("\n"), @r#"
        fv_0123456789abcdef.owner => Some("fv_0123456789abcdef")
        fv_0123456789abcdef => None
        fv_0123456789ABCDEF.owner => None
        fv_0123456789abcde.owner => None
        fv_0123456789abcdef0.owner => None
        fv_0123456789abcdef.owner.tmp => None
        xv_0123456789abcdef.owner => None
        .owner => None
        "#);
    }

    /// A claim is exclusive and arrives locked; a record is taken only once
    /// its holder lets go, and not after it is removed.
    #[test]
    fn claims_and_takes() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_str().unwrap();
        let path = super::path(state_dir, NAME);
        let line = claim_line(NAME, TOKEN, MOUNT);

        let mut held = super::claim(state_dir, NAME, &line).expect("claims");
        assert_eq!(
            super::claim(state_dir, NAME, &line).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists,
        );
        assert!(super::take(&path).unwrap().is_none(), "held by its owner");

        super::append(&mut held, &created_line(Dir::Mount)).unwrap();
        std::mem::drop(held);

        let (taken, content) = super::take(&path).unwrap().expect("its owner let go");
        assert!(parse(state_dir, NAME, &content).unwrap().created_mount);
        assert!(super::take(&path).unwrap().is_none(), "held by its taker");

        super::remove(state_dir, &path).unwrap();
        std::mem::drop(taken);
        assert!(super::take(&path).unwrap().is_none(), "removed");

        let elsewhere = format!("{state_dir}/elsewhere");
        std::fs::write(&elsewhere, &line).unwrap();
        std::os::unix::fs::symlink(&elsewhere, &path).unwrap();
        assert!(super::take(&path).is_err());
    }
}
