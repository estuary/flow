use std::fs;
use std::io::{self, Write};

use anyhow::Error;
use futures::TryStreamExt;
use ifstructs::ifreq;
use ipnetwork::IpNetwork;
use nix::mount::{mount as nix_mount, MsFlags};
use nix::sys::socket::{AddressFamily, SockFlag, SockType};
use nix::sys::stat::Mode;
use nix::unistd::{
    chdir as nix_chdir, chroot as nix_chroot, close, mkdir as nix_mkdir, symlinkat, Gid, Group,
    Uid, User,
};
use nix::{ioctl_write_ptr_bad, sys, NixPath};
use tracing::{debug, warn};

use crate::config::{GuestConfig, ImageConfig};

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error("couldn't mount {} onto {}, because: {}", source, target, error)]
    Mount {
        source: String,
        target: String,
        #[source]
        error: nix::Error,
    },

    #[error("couldn't mkdir {}, because: {}", path, error)]
    Mkdir {
        path: String,
        #[source]
        error: nix::Error,
    },

    #[error("couldn't chroot to {}, because: {}", path, error)]
    Chroot {
        path: String,
        #[source]
        error: nix::Error,
    },

    #[error("couldn't chdir to {}, because: {}", path, error)]
    Chdir {
        path: String,
        #[source]
        error: nix::Error,
    },

    #[error(r#"couldn't find user "{}""#, 0)]
    UserNotFound(String),
    #[error(r#"couldn't find group "{}""#, 0)]
    GroupNotFound(String),

    #[error("an unhandled error occurred: {}", 0)]
    UnhandledNixError(#[from] nix::Error),

    #[error("an unhandled IO error occurred: {}", 0)]
    UnhandledIoError(#[from] io::Error),

    #[error("an unhandled netlink error occurred: {}", 0)]
    UnhandledNetlinkError(#[from] rtnetlink::Error),

    #[error("an unhandled error occurred: {}", 0)]
    UnhandledError(#[from] Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("error reading fly json config: {}", 0)]
    Read(#[from] io::Error),
    #[error("error parsing fly json config: {}", 0)]
    Parse(#[from] serde_json::Error),
}

pub fn mount<
    P1: ?Sized + NixPath,
    P2: ?Sized + NixPath,
    P3: ?Sized + NixPath,
    P4: ?Sized + NixPath,
>(
    source: Option<&P1>,
    target: &P2,
    fstype: Option<&P3>,
    flags: MsFlags,
    data: Option<&P4>,
) -> Result<(), InitError> {
    nix_mount(source, target, fstype, flags, data).map_err(|error| InitError::Mount {
        source: source
            .map(|p| {
                p.with_nix_path(|cs| {
                    cs.to_owned()
                        .into_string()
                        .ok()
                        .unwrap_or_else(|| String::new())
                })
                .unwrap_or_else(|_| String::new())
            })
            .unwrap_or_else(|| String::new()),
        target: target
            .with_nix_path(|cs| {
                cs.to_owned()
                    .into_string()
                    .ok()
                    .unwrap_or_else(|| String::new())
            })
            .unwrap_or_else(|_| String::new()),
        error,
    })
}

pub fn chdir<P: ?Sized + NixPath>(path: &P) -> Result<(), InitError> {
    nix_chdir(path).map_err(|error| InitError::Chdir {
        path: path
            .with_nix_path(|cs| {
                cs.to_owned()
                    .into_string()
                    .ok()
                    .unwrap_or_else(|| String::new())
            })
            .unwrap_or_else(|_| String::new()),
        error,
    })
}

pub fn mkdir<P: ?Sized + NixPath>(path: &P, mode: Mode) -> Result<(), InitError> {
    nix_mkdir(path, mode).map_err(|error| InitError::Mkdir {
        path: path
            .with_nix_path(|cs| {
                cs.to_owned()
                    .into_string()
                    .ok()
                    .unwrap_or_else(|| String::new())
            })
            .unwrap_or_else(|_| String::new()),
        error,
    })
}

pub fn chroot<P: ?Sized + NixPath>(path: &P) -> Result<(), InitError> {
    nix_chroot(path).map_err(|error| InitError::Chroot {
        path: path
            .with_nix_path(|cs| {
                cs.to_owned()
                    .into_string()
                    .ok()
                    .unwrap_or_else(|| String::new())
            })
            .unwrap_or_else(|_| String::new()),
        error,
    })
}

pub fn setup_device_mounts() -> Result<(), InitError> {
    let chmod_0755: Mode =
        Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IXGRP | Mode::S_IROTH | Mode::S_IXOTH;
    let chmod_0555: Mode = Mode::S_IRUSR
        | Mode::S_IXUSR
        | Mode::S_IRGRP
        | Mode::S_IXGRP
        | Mode::S_IROTH
        | Mode::S_IXOTH;
    let chmod_1777: Mode = Mode::S_IRWXU | Mode::S_IRWXG | Mode::S_IRWXO | Mode::S_ISVTX;
    let common_mnt_flags: MsFlags = MsFlags::MS_NODEV | MsFlags::MS_NOEXEC | MsFlags::MS_NOSUID;

    debug!("Mounting /dev/pts");
    mkdir("/dev/pts", chmod_0755).ok();
    mount(
        Some("devpts"),
        "/dev/pts",
        Some("devpts"),
        MsFlags::MS_NOEXEC | MsFlags::MS_NOSUID | MsFlags::MS_NOATIME,
        Some("mode=0620,gid=5,ptmxmode=666"),
    )?;

    debug!("Mounting /dev/mqueue");
    mkdir("/dev/mqueue", chmod_0755).ok();
    mount::<_, _, _, [u8]>(
        Some("mqueue"),
        "/dev/mqueue",
        Some("mqueue"),
        common_mnt_flags,
        None,
    )?;

    // debug!("Mounting /dev/[u]random");
    // mkdir("/dev/random", chmod_0555).ok();
    // mount::<_, _, _, [u8]>(
    //     Some("random"),
    //     "/dev/random",
    //     Some("random"),
    //     common_mnt_flags,
    //     None,
    // )?;
    // mkdir("/dev/urandom", chmod_0555).ok();
    // mount::<_, _, _, [u8]>(
    //     Some("urandom"),
    //     "/dev/urandom",
    //     Some("urandom"),
    //     common_mnt_flags,
    //     None,
    // )?;

    debug!("Mounting /dev/shm");
    mkdir("/dev/shm", chmod_1777).ok();
    mount::<_, _, _, [u8]>(
        Some("shm"),
        "/dev/shm",
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
        None,
    )?;

    debug!("Mounting /dev/hugepages");
    mkdir("/dev/hugepages", chmod_0755).ok();
    mount(
        Some("hugetlbfs"),
        "/dev/hugepages",
        Some("hugetlbfs"),
        MsFlags::MS_RELATIME,
        Some("pagesize=2M"),
    )?;

    debug!("Mounting /proc");
    mkdir("/proc", chmod_0555).ok();
    mount::<_, _, _, [u8]>(Some("proc"), "/proc", Some("proc"), common_mnt_flags, None)?;
    mount::<_, _, _, [u8]>(
        Some("binfmt_misc"),
        "/proc/sys/fs/binfmt_misc",
        Some("binfmt_misc"),
        common_mnt_flags | MsFlags::MS_RELATIME,
        None,
    )?;

    debug!("Mounting /sys");
    mkdir("/sys", chmod_0555).ok();
    mount::<_, _, _, [u8]>(Some("sys"), "/sys", Some("sysfs"), common_mnt_flags, None)?;

    debug!("Mounting /run");
    mkdir("/run", chmod_0755).ok();
    mount(
        Some("run"),
        "/run",
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
        Some("mode=0755"),
    )?;
    mkdir("/run/lock", Mode::all()).ok();

    symlinkat("/proc/self/fd", None, "/dev/fd").ok();
    symlinkat("/proc/self/fd/0", None, "/dev/stdin").ok();
    symlinkat("/proc/self/fd/1", None, "/dev/stdout").ok();
    symlinkat("/proc/self/fd/2", None, "/dev/stderr").ok();

    mkdir("/root", Mode::S_IRWXU).ok();

    let common_cgroup_mnt_flags =
        MsFlags::MS_NODEV | MsFlags::MS_NOEXEC | MsFlags::MS_NOSUID | MsFlags::MS_RELATIME;

    debug!("Mounting cgroup");
    mount(
        Some("tmpfs"),
        "/sys/fs/cgroup",
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC | MsFlags::MS_NODEV, // | MsFlags::MS_RDONLY,
        Some("mode=755"),
    )?;

    debug!("Mounting cgroup2");
    mkdir("/sys/fs/cgroup/unified", chmod_0555)?;
    mount(
        Some("cgroup2"),
        "/sys/fs/cgroup/unified",
        Some("cgroup2"),
        common_mnt_flags | MsFlags::MS_RELATIME,
        Some("nsdelegate"),
    )?;

    debug!("Mounting /sys/fs/cgroup/net_cls,net_prio");
    mkdir("/sys/fs/cgroup/net_cls,net_prio", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/net_cls,net_prio",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("net_cls,net_prio"),
    )?;

    debug!("Mounting /sys/fs/cgroup/hugetlb");
    mkdir("/sys/fs/cgroup/hugetlb", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/hugetlb",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("hugetlb"),
    )?;

    debug!("Mounting /sys/fs/cgroup/pids");
    mkdir("/sys/fs/cgroup/pids", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/pids",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("pids"),
    )?;

    debug!("Mounting /sys/fs/cgroup/freezer");
    mkdir("/sys/fs/cgroup/freezer", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/freezer",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("freezer"),
    )?;

    debug!("Mounting /sys/fs/cgroup/cpu,cpuacct");
    mkdir("/sys/fs/cgroup/cpu,cpuacct", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/cpu,cpuacct",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("cpu,cpuacct"),
    )?;

    debug!("Mounting /sys/fs/cgroup/devices");
    mkdir("/sys/fs/cgroup/devices", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/devices",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("devices"),
    )?;

    debug!("Mounting /sys/fs/cgroup/blkio");
    mkdir("/sys/fs/cgroup/blkio", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/blkio",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("blkio"),
    )?;

    debug!("Mounting cgroup/memory");
    mkdir("/sys/fs/cgroup/memory", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/memory",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("memory"),
    )?;

    debug!("Mounting /sys/fs/cgroup/perf_event");
    mkdir("/sys/fs/cgroup/perf_event", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/perf_event",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("perf_event"),
    )?;

    debug!("Mounting /sys/fs/cgroup/cpuset");
    mkdir("/sys/fs/cgroup/cpuset", chmod_0555)?;
    mount(
        Some("cgroup"),
        "/sys/fs/cgroup/cpuset",
        Some("cgroup"),
        common_cgroup_mnt_flags,
        Some("cpuset"),
    )?;

    rlimit::setrlimit(rlimit::Resource::NOFILE, 10240, 10240).ok();

    mkdir("/etc", chmod_0755).ok();
    Ok(())
}

pub fn setup_rootfs(conf: &GuestConfig) -> Result<(), InitError> {
    let root_device = conf.root_device.clone().unwrap_or("/dev/vdb".to_owned());

    let chmod_0755: Mode =
        Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IXGRP | Mode::S_IROTH | Mode::S_IXOTH;

    debug!("Mounting /dev");
    mkdir("/dev", chmod_0755).ok();
    mount(
        Some("devtmpfs"),
        "/dev",
        Some("devtmpfs"),
        MsFlags::MS_NOSUID,
        Some("mode=0755"),
    )?;

    mkdir("/newroot", chmod_0755)?;

    debug!("Mounting newroot fs");
    mount::<_, _, _, [u8]>(
        Some(root_device.as_str()),
        "/newroot",
        Some("ext4"),
        MsFlags::MS_RELATIME,
        None,
    )?;

    // Move /dev so we don't have to re-mount it
    debug!("Mounting (move) /dev");
    mkdir("/newroot/dev", chmod_0755).ok();
    mount::<_, _, [u8], [u8]>(Some("/dev"), "/newroot/dev", None, MsFlags::MS_MOVE, None)?;

    // Our own hacky switch_root
    debug!("Switching root");
    // Change directory to the new root
    chdir("/newroot")?;
    // Mount the new root over /
    mount::<_, _, [u8], [u8]>(Some("."), "/", None, MsFlags::MS_MOVE, None)?;
    // Change root to the current directory (new root)
    chroot(".")?;
    // Change directory to /
    chdir("/")?;

    Ok(())
}

pub fn setup_user_group(conf: &mut ImageConfig) -> Result<(Uid, Gid, String), InitError> {
    let user = conf.user.clone().unwrap_or("root".to_owned());

    let mut user_split = user.split(":");

    let user = user_split
        .next()
        .expect("no user defined, this should not happen, please contact support!");
    let group = user_split.next();

    debug!("searching for user '{}", user);

    let (uid, mut gid, home_dir) = match User::from_name(user) {
        Ok(Some(u)) => (u.uid, u.gid, u.dir),
        Ok(None) => {
            if let Ok(uid) = user.parse::<u32>() {
                match User::from_uid(Uid::from_raw(uid)) {
                    Ok(Some(u)) => (u.uid, u.gid, u.dir),
                    _ => (Uid::from_raw(uid), Gid::from_raw(uid), "/".into()),
                }
            } else {
                return Err(InitError::UserNotFound(user.into()).into());
            }
        }
        Err(e) => {
            if user != "root" {
                return Err(InitError::UserNotFound(user.into()).into());
            }
            debug!("error getting user '{}' by name => {}", user, e);
            match User::from_name("root") {
                Ok(Some(u)) => (u.uid, u.gid, u.dir),
                _ => (Uid::from_raw(0), Gid::from_raw(0), "/root".into()),
            }
        }
    };

    if let Some(group) = group {
        debug!("searching for group '{}'", group);
        match Group::from_name(group) {
            Err(_e) => {
                return Err(InitError::GroupNotFound(group.into()).into());
            }
            Ok(Some(g)) => gid = g.gid,
            Ok(None) => {
                if let Ok(raw_gid) = group.parse::<u32>() {
                    gid = Gid::from_raw(raw_gid);
                } else {
                    return Err(InitError::GroupNotFound(group.into()).into());
                }
            }
        }
    }

    // if we have a PATH, set it on the OS to be able to find argv[0]
    conf.env
        .entry("HOME".to_owned())
        .or_insert(home_dir.to_string_lossy().into_owned());

    Ok((uid, gid, user.to_owned()))
}

pub async fn setup_networking(conf: &GuestConfig) -> Result<(), InitError> {
    if let Some(ref etc_hosts) = conf.etc_hosts {
        debug!("Populating /etc/hosts");
        let mut f = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open("/etc/hosts")?;

        for entry in etc_hosts {
            if let Some(ref desc) = entry.desc {
                write!(&mut f, "\n# {}\n{}\t{}\n", desc, entry.ip, entry.host).ok();
            } else {
                write!(&mut f, "\n{}\t{}\n", entry.ip, entry.host).ok();
            }
        }
    }

    if let Some(ref etc_resolv) = conf.etc_resolv {
        debug!("Populating /etc/resolv.conf");
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open("/etc/resolv.conf")?;

        for ns in etc_resolv.nameservers.iter() {
            write!(&mut f, "\nnameserver\t{}", ns).ok();
        }

        write!(&mut f, "\n").ok();
    }

    let (connection, netlink_handle, _) = rtnetlink::new_connection().unwrap();
    tokio::spawn(connection);

    debug!("netlink: getting lo link");
    let lo = netlink_handle
        .link()
        .get()
        .match_name("lo".into())
        .execute()
        .try_next()
        .await?
        .expect("no lo link found");

    debug!("netlink: setting lo link \"up\"");
    netlink_handle
        .link()
        .set(lo.header.index)
        .up()
        .execute()
        .await?;

    debug!("netlink: getting eth0 link");
    let eth0 = netlink_handle
        .link()
        .get()
        .match_name("eth0".into())
        .execute()
        .try_next()
        .await?
        .expect("no eth0 link found");

    debug!("netlink: setting eth0 link \"up\"");
    netlink_handle
        .link()
        .set(eth0.header.index)
        .up()
        .mtu(1420)
        .execute()
        .await?;

    if let Some(ref ip_configs) = conf.ip_configs {
        let address = netlink_handle.address();
        let route = netlink_handle.route();

        for ipc in ip_configs {
            if let IpNetwork::V4(ipn) = ipc.ip {
                debug!("netlink: adding ip {}/{}", ipc.ip.ip(), ipc.ip.prefix());
                address
                    .add(eth0.header.index, ipc.ip.ip(), ipn.prefix())
                    .execute()
                    .await?;

                if ipn.prefix() < 30 {
                    let ipint: u32 = ipn.ip().into();
                    let nextip: std::net::Ipv4Addr = (ipint + 1).into();

                    address
                        .add(
                            eth0.header.index,
                            std::net::IpAddr::V4(nextip),
                            ipn.prefix(),
                        )
                        .execute()
                        .await?;
                }
            } else {
                warn!("IPv6 not supported for ip: {:?}", ipc.ip)
            }

            match ipc.gateway {
                IpNetwork::V4(gateway) => {
                    debug!("netlink: adding default route via {}", ipc.gateway);
                    route.add().v4().gateway(gateway.ip()).execute().await?;
                }
                IpNetwork::V6(gateway) => {
                    warn!("IPv6 not supported for gateway: {:?}", gateway)
                }
            }
        }
    }

    Ok(())
}
