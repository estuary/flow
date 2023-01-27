use std::{
    collections::HashMap,
    fs::File,
    net::Ipv4Addr,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{anyhow, bail, Context};
use bollard::container::RemoveContainerOptions;
use cmd_lib::{init_builtin_logger, run_cmd, run_fun};
use connector_init::config::{
    EtcHost, EtcResolv, GuestConfig, GuestConfigBuilder, IPConfig, Image, ImageConfig,
};
use firec::{config::network::Interface, Machine};
use futures::TryStreamExt;
use ipnetwork::{IpNetwork, Ipv4Network};
use nftnl::{
    nft_expr, nftnl_sys::libc, Batch, Chain, ChainType, FinalizedBatch, Hook, MsgType, Policy,
    ProtoFamily, Rule, Table,
};
use serde_json::json;
use std::ffi::CString;
use tokio::{signal::unix, time::sleep};
use tokio_tun::TunBuilder;
use tracing::{debug, error, info, metadata::LevelFilter, trace};
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

pub mod cni;

/// Copy init binary, image inspect JSON, and guest config JSON
#[tracing::instrument(err, skip_all)]
fn setup_init_fs(
    temp_dir: &Path,
    init_bin_path: String,
    inspect_output: Image,
    guest_conf: GuestConfig,
) -> anyhow::Result<PathBuf> {
    serde_json::to_writer(
        &File::create(temp_dir.join("image_inspect.json"))?,
        &json!([inspect_output]), //Image::parse_from_json_file expects data wrapped in an array for some reason
    )?;
    serde_json::to_writer(
        &File::create(temp_dir.join("guest_config.json"))?,
        &guest_conf,
    )?;

    let user = std::env::var("USER")?;

    run_cmd!(
        cd $temp_dir;
        fallocate -l 64M initfs;
        mkfs.ext2 initfs;
        mkdir initmount;
        sudo mount -o loop,noatime initfs initmount;
        sudo chown $user:$user initmount;
        mkdir initmount/flow;
        cp $init_bin_path initmount/flow/init;
        cp $temp_dir/image_inspect.json initmount/flow/image_inspect.json;
        cp $temp_dir/guest_config.json initmount/flow/guest_config.json;
        sudo umount initmount;
    )?;

    let init_fs = temp_dir.join("initfs");
    let init_fs_str = init_fs.display().to_string();
    debug!(init_fs = init_fs_str, "Boot filesystem setup");

    Ok(init_fs)
}

/// NOTE: This logic will eventually be replaced by containerd,
/// there were just too many moving pieces to get that working correctly
/// in addition to everything else
#[tracing::instrument(err, skip_all)]
async fn setup_root_fs(temp_dir: &Path, image_name: String) -> anyhow::Result<PathBuf> {
    let docker = bollard::Docker::connect_with_local_defaults()?;
    let container = docker
        .create_container::<String, String>(
            None,
            bollard::container::Config {
                image: Some(image_name),
                ..Default::default()
            },
        )
        .await?;

    let container_id = container.id;

    run_cmd!(
        cd $temp_dir;
        docker export $container_id --output="rootfs.tar";
        sudo virt-make-fs --type=ext4 rootfs.tar rootfs.ext4;
    )?;

    docker
        .remove_container(
            container_id.as_ref(),
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await?;

    let root_fs = temp_dir.join("rootfs.ext4");
    let root_fs_str = root_fs.display().to_string();
    debug!(root_fs = root_fs_str, "Root filesystem setup");

    Ok(root_fs)
}

#[tracing::instrument(err, skip_all)]
async fn get_image_config(image: String) -> anyhow::Result<Image> {
    let inspect_res = bollard::Docker::connect_with_local_defaults()?
        .inspect_image(image.as_ref())
        .await?;

    let img_config = inspect_res
        .config
        .ok_or(anyhow::anyhow!("Missing image config for {}", image))?;

    let mut img = Image {
        config: ImageConfig {
            cmd: img_config.cmd,
            entrypoint: img_config.entrypoint,
            _env: img_config.env.unwrap_or_default(),
            labels: img_config.labels.unwrap_or_default(),
            working_dir: img_config.working_dir,
            user: img_config.user,
            env: HashMap::new(),
        },
        repo_tags: inspect_res.repo_tags.unwrap_or(vec![]),
    };
    img.parse_env();

    debug!(image_name = image, "Image inspected");

    Ok(img)
}

const NAT_TABLE_NAME: &str = "flow-firecracker-nat";
const NAT_CHAIN_NAME: &str = "nat";

#[tracing::instrument(err, skip_all)]
async fn get_default_route_device_name() -> anyhow::Result<String> {
    // Get WAN device by issuing the equivalent to `ip route show`
    let (connection, handle, _) = rtnetlink::new_connection().unwrap();
    tokio::spawn(connection);

    let res: Vec<rtnetlink::packet::RouteMessage> = handle
        .route()
        .get(rtnetlink::IpVersion::V4)
        .execute()
        .try_collect()
        .await?;

    let default = res
        .iter()
        .find(|&msg| {
            msg.nlas
                .iter()
                .find(|nla| {
                    if let rtnetlink::packet::route::Nla::Destination(_) = nla {
                        true
                    } else {
                        false
                    }
                })
                .is_none()
                && msg.destination_prefix().is_none()
        })
        .ok_or(anyhow!("Unable to find default route"))?;

    let output_interface_id = default
        .nlas
        .iter()
        .find_map(|nla| {
            if let rtnetlink::packet::route::Nla::Oif(oif) = nla {
                Some(oif)
            } else {
                None
            }
        })
        .ok_or(anyhow!("Default route has no output interface"))?;

    // We could do this by calling `if_indextoname`, but that's scary and this isn't that bad
    let all_interfaces = nix::net::if_::if_nameindex()?;
    let interface_name = all_interfaces
        .iter()
        .find_map(|iface| {
            if iface.index().eq(output_interface_id) {
                Some(iface.name().to_str())
            } else {
                None
            }
        })
        .ok_or(anyhow!(
            "Could not find name for interface {output_interface_id}"
        ))??;

    debug!(device = interface_name, "Default gateway device determined");

    Ok(interface_name.to_owned())
}

fn socket_recv<'a>(socket: &mnl::Socket, buf: &'a mut [u8]) -> anyhow::Result<Option<&'a [u8]>> {
    let ret = socket.recv(buf)?;
    trace!("Read {} bytes from netlink", ret);
    if ret > 0 {
        Ok(Some(&buf[..ret]))
    } else {
        Ok(None)
    }
}

#[tracing::instrument(err, skip_all)]
fn send_and_process_netfilter(batch: &FinalizedBatch) -> anyhow::Result<()> {
    let socket = mnl::Socket::new(mnl::Bus::Netfilter)?;
    socket.send_all(batch)?;

    let portid = socket.portid();
    let mut buffer = vec![0; nftnl::nft_nlmsg_maxsize() as usize];

    let seq = 0;
    while let Some(message) = socket_recv(&socket, &mut buffer[..])? {
        match mnl::cb_run(message, seq, portid)? {
            mnl::CbResult::Stop => {
                trace!("cb_run STOP");
                break;
            }
            mnl::CbResult::Ok => trace!("cb_run OK"),
        };
    }
    Ok(())
}

/*
We end up adding a netfilter config that looks like:
table inet flow-firecracker-nat {
    chain nat-tun[xxxx] {
            type nat hook postrouting priority srcnat; policy drop;
            saddr [gateway] oifname "tun[xxxx]*" accept
            iifname "tun[xxxx]*" oifname "enp1s0*" accept
            ct state established,related accept
            oifname "enp1s0*" masquerade
    }
}
*/

#[tracing::instrument(err, skip_all)]
async fn setup_networking(id: &Uuid) -> anyhow::Result<(String, String, IPConfig)> {
    let mut id_prefix = id.to_string();
    id_prefix.truncate(4);

    let netns_name = format!("ns-{id_prefix}");

    rtnetlink::NetworkNamespace::add(netns_name.clone()).await?;

    debug!(netns = netns_name, "Created network namespace");

    let cni_path = "/home/js/cniplugins";
    let cni_cfg = "fcnet";

    let netns_path = format!("/var/run/netns/{netns_name}");

    let res = run_fun!(CNI_PATH=$cni_path cnitool add $cni_cfg $netns_path)?;

    let parsed = serde_json::from_str::<crate::cni::Result>(res.as_ref()).map_err(|e| {
        error!("Failed to load JSON result from cnitool. Response: {res}");
        e
    })?;

    // This is probably always going to be `tap0`, which is fine because this is
    // the name of the interface INSIDE of the network namespace
    let tap_iface = parsed
        .interfaces
        .iter()
        .find(|iface| {
            iface
                .sandbox
                .to_owned()
                // the cnitool binary prefixes all of its ContainerIDs with cnitool
                // note: This is dumb and we should either put in the work to write a
                // Rust client to use cni plugins, or PR cnitool to support CNI_CONTAINERID env
                .map(|sbx| sbx.starts_with("cnitool"))
                .unwrap_or(false)
        })
        .ok_or(anyhow!("Unable to find name of tap interface"))?;

    let ip = parsed
        .ips
        .first()
        .ok_or(anyhow!("No IPs were created for some reason"))?;

    Ok((
        tap_iface.name.to_owned(),
        netns_path.to_owned(),
        IPConfig {
            ip: ip.address,
            gateway: IpNetwork::V4(match ip.gateway {
                std::net::IpAddr::V4(v4ip) => Ipv4Network::new(v4ip, 0)?,
                std::net::IpAddr::V6(v6ip) => {
                    bail!("Got an unexpected IPV6 gateway from CNI: {v6ip}")
                }
            }),
        },
    ))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    LogTracer::init()?;

    let image_name = "hello-world";

    let vm_id = Uuid::new_v4();

    let tempdir = tempfile::Builder::new()
        // .prefix(&format!("vm-{}", vm_id_short))
        .rand_bytes(2)
        .tempdir_in("/tmp")
        .unwrap();

    let image_conf = get_image_config(image_name.to_owned()).await?;

    let (tap_dev_name, netns_path, ip_config) = setup_networking(&vm_id).await?;

    let guest_conf = GuestConfigBuilder::default()
        .root_device("/dev/vdb") // Assuming that the second disk becomes vdb...
        .hostname("test-hello".to_owned())
        .ip_configs(vec![ip_config])
        .etc_resolv(EtcResolv {
            nameservers: vec!["8.8.8.8".to_owned()],
        })
        .etc_hosts(vec![EtcHost {
            host: "localhost".to_owned(),
            ip: "127.0.0.1".to_owned(),
            desc: Some("Loopback".to_owned()),
        }])
        .build()
        .context("error building GuestConfig")?;

    let init_fs = setup_init_fs(
        &tempdir.path(),
        "/home/js/estuary/flow/target/x86_64-unknown-linux-musl/release/flow-connector-init"
            .to_owned(),
        image_conf,
        guest_conf,
    )?;

    let main_fs = setup_root_fs(&tempdir.path(), image_name.to_owned()).await?;

    let kernel_args = "console=ttyS0 reboot=k panic=1 pci=off random.trust_cpu=on \
        RUST_LOG=debug RUST_BACKTRACE=full LOG_LEVEL=debug \
        init=/flow/init -- \
            --firecracker \
            --image-inspect-json-path /flow/image_inspect.json \
            --guest-config-json-path /flow/guest_config.json";

    let iface = Interface::new(tap_dev_name, "eth0");

    let config = firec::config::Config::builder(Some(vm_id), Path::new("vmlinux-self-5.19.bin"))
        .jailer_cfg()
        .chroot_base_dir(tempdir.path())
        .exec_file(Path::new("/usr/bin/firecracker"))
        .build()
        .net_ns(netns_path)
        .kernel_args(kernel_args)
        .machine_cfg()
        .vcpu_count(1)
        .mem_size_mib(1024)
        .build()
        .add_drive("root", init_fs)
        .is_root_device(true)
        .build()
        .add_drive("main", main_fs)
        .build()
        .add_network_interface(iface)
        .socket_path(tempdir.path().join("firecracker.socket"))
        .build();
    let mut machine = Machine::create(config).await?;

    machine.start().await?;

    // Gracefully exit on either SIGINT (ctrl-c) or SIGTERM.
    let mut sigint = unix::signal(unix::SignalKind::interrupt()).unwrap();
    let mut sigterm = unix::signal(unix::SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigint.recv() => (),
        _ = sigterm.recv() => (),
    }

    info!("Caught exit signal, shutting down");

    machine.force_shutdown().await?;

    Ok(())
}

//https://github.com/superfly/init-snapshot#usage
/*
Build image
Push to registry
Pull from registry
Unroll image rootfs
    Get a tarball of the image with:
        docker export $(docker create <image name>) --output="<tarball name>.tar"
    Convert tarball to ext4 rootfs with
        virt-make-fs --type=ext4 hello-world.tar hello-world.ext4
        OR
        a bunch of fancy dd and mkfs and mount commands
        OR
        Some native Rust stuff
Fetch a kernel (vmlinux.bin)
Build (or realistically use prebuilt) init binary
Set up the boot volume
    See here: https://github.com/superfly/init-snapshot#usage
    Basically, create a volume with the init binary and config.json
Set up firecracker VM
    Provide kernel vmlinux.bin
    Mount boot volume to /dev/vda
    Mount program volume to /dev/vdb
        Make sure to indicate the volume name in config.json, or use vdb as default
    Attach vsock virtio device beecause init will communicate over it
Specify init in kernel cmd line

TODO:
    - Flesh out init binary, including the server that communicates over vsock
    - Build bare-bones host-side tooling to run all of this
        Generating config json
    - Build CLI to deal with bundling etc
*/
