//! What the host reads about a running VMM container: its mounts, its
//! network namespace's conntrack table, its ruleset's counters and sets, and
//! who still holds a file.

use crate::run::sudo;
use std::collections::BTreeMap;
use std::net::Ipv4Addr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    /// The path within the source filesystem, which names a bound file.
    pub root: String,
    pub point: String,
    pub fstype: String,
    /// Per-mount options, which is where a read-only bind says so.
    pub options: Vec<String>,
    pub super_options: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flow {
    pub proto: String,
    /// The original direction's addresses: the guest's own view.
    pub src: Ipv4Addr,
    pub dst: Ipv4Addr,
    pub dport: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Element {
    pub addr: Ipv4Addr,
    pub timeout: Option<u64>,
    pub expires: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inode {
    pub dev: u64,
    pub ino: u64,
    /// Birth time since the epoch disambiguates reused inode numbers.
    pub born: std::time::Duration,
    pub links: u64,
    pub bytes: u64,
}

pub fn mountinfo(pid: u32) -> Vec<Mount> {
    parse_mountinfo(&sudo(&["cat", &format!("/proc/{pid}/mountinfo")]))
}

/// Entered through the network namespace only, so the table is the VMM's
/// and the reading tools are the host's. Read over netlink rather than from
/// `/proc/net/nf_conntrack`, which kernels without `NF_CONNTRACK_PROCFS` (the
/// CI runner's) lack; `-o extended` keeps the procfs line format.
pub fn conntrack(pid: u32) -> Vec<Flow> {
    parse_conntrack(&sudo(&[
        "nsenter",
        "-t",
        &pid.to_string(),
        "-n",
        "conntrack",
        "-L",
        "-f",
        "ipv4",
        "-o",
        "extended",
    ]))
}

/// The runner has no host `nft`, so the image's own runs in the container.
pub fn counters(name: &str) -> BTreeMap<String, u64> {
    rule_counters(&sudo(&[
        "podman",
        "exec",
        name,
        "nft",
        "-j",
        "list",
        "table",
        "inet",
        "flow_egress",
    ]))
}

pub fn resolved(name: &str) -> Vec<Element> {
    resolved_elements(&sudo(&[
        "podman",
        "exec",
        name,
        "nft",
        "-j",
        "list",
        "set",
        "inet",
        "flow_egress",
        "resolved",
    ]))
}

pub fn fd_inode(pid: u32, fd: u32) -> Inode {
    let path = format!("/proc/{pid}/fd/{fd}");
    let stat = sudo(&["stat", "-L", "-c", "%d %i %.9W %h %b %B", &path]);
    let fields: Vec<&str> = stat.split_whitespace().collect();
    let [dev, ino, born, links, blocks, block_size] = fields[..] else {
        panic!("unexpected stat output {stat:?}");
    };
    let number = |field: &str| -> u64 { field.parse().expect("stat prints numbers") };
    let born = birth(born).unwrap_or_else(|| panic!("unexpected stat output {stat:?}"));
    assert!(
        !born.is_zero(),
        "{path} has no birth time, so a later file given its inode number would pass for it"
    );
    Inode {
        dev: number(dev),
        ino: number(ino),
        born,
        links: number(links),
        bytes: number(blocks) * number(block_size),
    }
}

/// Every descriptor and mapping on the host that still refers to `inode`.
/// An unlinked file with none left has been freed by the kernel, which is
/// the only way to observe an `O_TMPFILE` going away.
pub fn inode_references(inode: Inode) -> Vec<String> {
    // Processes come and go during the walk, so find's exit status is noise.
    let output = crate::run::sudo_output(
        &[
            "sh",
            "-c",
            "find /proc/[0-9]*/fd /proc/[0-9]*/map_files -mindepth 1 -maxdepth 1 \
             -exec stat -L -c '%d %i %.9W %n' {} + 2>/dev/null",
        ],
        None,
    );
    references(&String::from_utf8_lossy(&output.stdout), inode)
}

/// Paths from a `%d %i %.9W %n` listing matching `inode`.
pub fn references(listing: &str, inode: Inode) -> Vec<String> {
    let wanted = format!("{} {} ", inode.dev, inode.ino);
    listing
        .lines()
        .filter_map(|line| line.strip_prefix(&wanted)?.split_once(' '))
        .filter(|(born, _)| birth(born) == Some(inode.born))
        .map(|(_, path)| path.to_string())
        .collect()
}

/// `stat`'s `%.9W`, which is zero where the filesystem records no birth time.
fn birth(field: &str) -> Option<std::time::Duration> {
    let (secs, nanos) = field.split_once('.')?;
    Some(std::time::Duration::new(
        secs.parse().ok()?,
        nanos.parse().ok()?,
    ))
}

pub fn parse_mountinfo(text: &str) -> Vec<Mount> {
    text.lines()
        .filter_map(|line| {
            // id parent major:minor root point options [optional...] - fstype source super
            let (head, tail) = line.split_once(" - ")?;
            let head: Vec<&str> = head.split(' ').collect();
            let tail: Vec<&str> = tail.split(' ').collect();

            Some(Mount {
                root: unescape(head.get(3)?),
                point: unescape(head.get(4)?),
                fstype: tail.first()?.to_string(),
                options: head.get(5)?.split(',').map(str::to_string).collect(),
                super_options: tail
                    .get(2)
                    .map(|options| options.split(',').map(str::to_string).collect())
                    .unwrap_or_default(),
            })
        })
        .collect()
}

/// One line per mount, sorted by path: the path with `redactions` applied,
/// the filesystem type, and the flags that decide what the VMM can do there.
/// Sorted because podman does not make its binds in a stable order, and
/// sources are left out because device names and layer ids vary by host.
/// A file masked by a bound null device prints only as `masked`: podman binds
/// either the container's `/dev/null` (tmpfs) or the host's (devtmpfs), with
/// that filesystem's flags, depending on its version.
pub fn matrix(mounts: &[Mount], redactions: &[(&str, &str)]) -> String {
    let mut lines = Vec::new();

    for mount in mounts {
        let mut out = String::new();
        let mut point = mount.point.clone();
        for (from, to) in redactions {
            point = point.replace(from, to);
        }
        if mount.root == "/null" && matches!(mount.fstype.as_str(), "tmpfs" | "devtmpfs") {
            lines.push(format!("{point} masked"));
            continue;
        }
        let read_only = has(&mount.options, "ro") || has(&mount.super_options, "ro");

        out.push_str(&format!(
            "{point} {} {}",
            mount.fstype,
            if read_only { "ro" } else { "rw" }
        ));
        for flag in ["nosuid", "nodev", "noexec"] {
            if has(&mount.options, flag) {
                out.push_str(&format!(",{flag}"));
            }
        }
        lines.push(out);
    }
    lines.sort();
    lines.iter().map(|line| format!("{line}\n")).collect()
}

/// The writable layer behind an overlay mount, which is where a guest's root
/// writes land.
pub fn upperdir(mounts: &[Mount], point: &str) -> Option<String> {
    mounts
        .iter()
        .find(|mount| mount.point == point)?
        .super_options
        .iter()
        .find_map(|option| option.strip_prefix("upperdir=").map(str::to_string))
}

pub fn parse_conntrack(text: &str) -> Vec<Flow> {
    text.lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            // The first occurrence of each key is the original direction.
            let first = |key: &str| {
                fields
                    .iter()
                    .find_map(|field| field.strip_prefix(key).map(str::to_string))
            };
            Some(Flow {
                proto: fields.get(2)?.to_string(),
                src: first("src=")?.parse().ok()?,
                dst: first("dst=")?.parse().ok()?,
                dport: first("dport=").and_then(|port| port.parse().ok()),
            })
        })
        .collect()
}

/// Packet counts keyed `chain/comment`, from `nft -j list table`.
pub fn rule_counters(json: &str) -> BTreeMap<String, u64> {
    let document: serde_json::Value = serde_json::from_str(json).expect("nft prints JSON");
    let mut counters = BTreeMap::new();

    for item in document["nftables"].as_array().into_iter().flatten() {
        let rule = &item["rule"];
        let (Some(chain), Some(comment)) = (rule["chain"].as_str(), rule["comment"].as_str())
        else {
            continue;
        };
        let packets = rule["expr"]
            .as_array()
            .into_iter()
            .flatten()
            .find_map(|expr| expr["counter"]["packets"].as_u64());

        if let Some(packets) = packets {
            counters.insert(format!("{chain}/{comment}"), packets);
        }
    }
    counters
}

/// The elements of a set from `nft -j list set`. An element without a timeout
/// is printed as a bare address.
pub fn resolved_elements(json: &str) -> Vec<Element> {
    let document: serde_json::Value = serde_json::from_str(json).expect("nft prints JSON");

    document["nftables"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item["set"]["elem"].as_array())
        .flatten()
        .filter_map(|element| {
            let (addr, detail) = match element.as_str() {
                Some(addr) => (addr, &serde_json::Value::Null),
                None => (element["elem"]["val"].as_str()?, &element["elem"]),
            };
            Some(Element {
                addr: addr.parse().ok()?,
                timeout: detail["timeout"].as_u64(),
                expires: detail["expires"].as_u64(),
            })
        })
        .collect()
}

fn has(options: &[String], flag: &str) -> bool {
    options.iter().any(|option| option == flag)
}

/// mountinfo escapes space, tab, newline and backslash as octal.
fn unescape(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && bytes[i + 1..i + 4].iter().all(u8::is_ascii_digit)
        {
            let octal = std::str::from_utf8(&bytes[i + 1..i + 4]).expect("ASCII digits");
            out.push(u8::from_str_radix(octal, 8).expect("octal digits"));
            i += 4;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    #[test]
    fn mount_matrix() {
        let mountinfo = "\
1 0 0:40 / / ro,relatime - overlay overlay rw,lowerdir=/l,upperdir=/u,workdir=/w
2 1 0:41 / /rootfs rw,relatime - overlay overlay rw,lowerdir=/l2,upperdir=/var/lib/containers/storage/overlay/abc/diff,workdir=/w2
3 1 8:1 /var/tmp/kvm/connector-mounts-0/mount-1 /var/tmp/kvm/connector-mounts-0/mount-1 ro,nosuid,nodev,relatime - ext4 /dev/root rw
4 1 0:42 / /dev rw,nosuid - tmpfs tmpfs ro,size=65536k
5 4 0:43 / /dev/shm rw,nosuid,nodev,noexec - tmpfs shm rw
6 1 8:1 /a\\040b /with\\040space rw - ext4 /dev/root rw
7 1 0:42 /null /proc/kcore ro,nosuid - tmpfs tmpfs ro,size=65536k
8 1 0:5 /null /proc/keys ro,relatime - devtmpfs udev rw,size=4096k
";
        let mounts = super::parse_mountinfo(mountinfo);
        let matrix = super::matrix(
            &mounts,
            &[("/var/tmp/kvm/connector-mounts-0/mount-1", "$M")],
        );

        insta::assert_snapshot!(matrix, @r"
        $M ext4 ro,nosuid,nodev
        / overlay ro
        /dev tmpfs ro,nosuid
        /dev/shm tmpfs rw,nosuid,nodev,noexec
        /proc/kcore masked
        /proc/keys masked
        /rootfs overlay rw
        /with space ext4 rw
        ");
        assert_eq!(
            super::upperdir(&mounts, "/rootfs").as_deref(),
            Some("/var/lib/containers/storage/overlay/abc/diff")
        );
    }

    #[test]
    fn inode_references() {
        let scratch = super::Inode {
            dev: 41,
            ino: 101,
            born: std::time::Duration::new(100, 25),
            links: 0,
            bytes: 0,
        };
        // Reusing an inode number must not count as retaining the original file.
        let listing = "\
41 101 100.000000025 /proc/2001/fd/3
41 101 101.000000025 /proc/2002/fd/4
41 102 100.000000025 /proc/2003/fd/5
42 101 100.000000025 /proc/2004/fd/6
41 101 0.000000000 /proc/2005/fd/7
";
        assert_eq!(super::references(listing, scratch), ["/proc/2001/fd/3"]);
    }

    #[test]
    fn conntrack_flows() {
        let table = "\
ipv4     2 udp      17 17 src=10.89.0.2 dst=192.31.196.241 sport=56858 dport=15353 src=192.31.196.241 dst=10.89.0.2 sport=15353 dport=56858 mark=0 zone=0 use=2
ipv4     2 tcp      6 431987 ESTABLISHED src=192.0.2.2 dst=192.31.196.241 sport=40944 dport=18443 src=192.31.196.241 dst=10.89.0.2 sport=18443 dport=40944 [ASSURED] mark=0 zone=0 use=2
ipv4     2 icmp     1 29 src=192.0.2.2 dst=192.31.196.241 type=8 code=0 id=7 src=192.31.196.241 dst=10.89.0.2 type=0 code=0 id=7 mark=0 zone=0 use=2
";
        insta::assert_debug_snapshot!(super::parse_conntrack(table), @r#"
        [
            Flow {
                proto: "udp",
                src: 10.89.0.2,
                dst: 192.31.196.241,
                dport: Some(
                    15353,
                ),
            },
            Flow {
                proto: "tcp",
                src: 192.0.2.2,
                dst: 192.31.196.241,
                dport: Some(
                    18443,
                ),
            },
            Flow {
                proto: "icmp",
                src: 192.0.2.2,
                dst: 192.31.196.241,
                dport: None,
            },
        ]
        "#);
    }
}
