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
use cmd_lib::{init_builtin_logger, run_cmd};
use connector_init::config::{
    EtcHost, EtcResolv, GuestConfig, GuestConfigBuilder, IPConfig, Image, ImageConfig,
};
use firec::{config::network::Interface, Machine};
use futures::TryStreamExt;
use ipnetwork::IpNetwork;
use nftnl::{
    nft_expr, nftnl_sys::libc, Batch, Chain, ChainType, FinalizedBatch, Hook, MsgType, Policy,
    ProtoFamily, Rule, Table,
};
use serde_json::json;
use std::ffi::CString;
use tokio::{signal::unix, time::sleep};
use tokio_tun::TunBuilder;
use tracing::{debug, info, metadata::LevelFilter, trace};
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

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
async fn setup_networking(
    id: &Uuid,
    network_conf: &IPConfig,
    wan_interface_name: Option<String>,
) -> anyhow::Result<String> {
    let mut id_prefix = id.to_string();
    id_prefix.truncate(4);

    let gateway_addr = match network_conf.gateway.ip() {
        std::net::IpAddr::V4(ip) => ip,
        std::net::IpAddr::V6(_) => {
            bail!("Only IPV4 networks are supported for firecracker VMs at this point")
        }
    };
    let netmask = match network_conf.gateway.mask() {
        std::net::IpAddr::V4(mask) => mask,
        // We should never get here because of the bail above
        std::net::IpAddr::V6(_) => {
            bail!("Only IPV4 networks are supported for firecracker VMs at this point")
        }
    };

    let tap = TunBuilder::new()
        .name(format!("tun{}", id_prefix).as_ref())
        .tap(true)
        .address(gateway_addr)
        .netmask(netmask)
        .persist() // would love if this wasn't needed but it appears to not do anything without it :(
        // .up() // or set it up manually using `sudo ip link set <tun-name> up`.
        .try_build()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    let devname = tap.name();

    debug!(tap_device = devname, "TUN/TAP device created");

    // // Apparently the ".up()" method on TunBuilder doesn't work?
    let (connection, netlink_handle, _) = rtnetlink::new_connection().unwrap();
    tokio::spawn(connection);

    let tap_link = netlink_handle
        .link()
        .get()
        .match_name(devname.into())
        .execute()
        .try_next()
        .await?
        .context(format!(
            "rtnetlink could not find newly created TUN/TAP device {devname}"
        ))?;

    netlink_handle
        .link()
        .set(tap_link.header.index)
        .up()
        .execute()
        .await
        .context(format!(
            "rtnetlink could not set TUN/TAP device {devname} 'up'"
        ))?;

    debug!(tap_device = devname, "TUN/TAP device set 'up'");

    // Now that we have the tap device for routing all traffic from this VM, let's NAT it to the world
    // "Source NAT is most commonly used for translating private IP address to a public routable address"
    // See https://wiki.nftables.org/wiki-nftables/index.php/Performing_Network_Address_Translation_(NAT)

    // Heavily inspired by: https://github.com/mullvad/nftnl-rs/blob/master/nftnl/examples/add-rules.rs

    // Create a batch. This is used to store all the netlink messages we will later send.
    // Creating a new batch also automatically writes the initial batch begin message needed
    // to tell netlink this is a single transaction that might arrive over multiple netlink packets.
    let mut batch = Batch::new();

    // Create a netfilter table operating on both IPv4 and IPv6 (ProtoFamily::Inet)
    let nat_table_name = format!("{NAT_TABLE_NAME}-{devname}");
    let table = Table::new(&CString::new(nat_table_name.clone())?, ProtoFamily::Inet);
    // Add the table to the batch with the `MsgType::Add` type, thus instructing netfilter to add
    // this table under its `ProtoFamily::Inet` ruleset.
    batch.add(&table, MsgType::Add);

    let nat_prefilter_chain_name = format!("{NAT_CHAIN_NAME}-prefilter");
    let mut nat_prefilter_chain =
        Chain::new(&CString::new(nat_prefilter_chain_name.clone())?, &table);
    nat_prefilter_chain.set_hook(Hook::Forward, 0);
    nat_prefilter_chain.set_type(ChainType::Filter);
    nat_prefilter_chain.set_policy(Policy::Drop);
    batch.add(&nat_prefilter_chain, MsgType::Add);

    let wan_interface = match wan_interface_name {
        Some(iface) => iface,
        None => get_default_route_device_name().await?,
    };

    // nft add rule $nat_chain iifname "tapx" oifname "wan" accept
    let mut tap_nat_filter_rule = Rule::new(&nat_prefilter_chain);
    tap_nat_filter_rule.add_expr(&nft_expr!(meta iifname)); // load input interface name
    tap_nat_filter_rule.add_expr(&nft_expr!(cmp == devname)); // select packets with iifname equal to the name of the tap device
    tap_nat_filter_rule.add_expr(&nft_expr!(meta oifname));
    tap_nat_filter_rule.add_expr(&nft_expr!(cmp == wan_interface.as_str()));
    tap_nat_filter_rule.add_expr(&nft_expr!(verdict accept));
    batch.add(&tap_nat_filter_rule, MsgType::Add);

    // nft add rule inet $nat_chain ct state related,established accept
    let mut tap_nat_conntrack_rule = Rule::new(&nat_prefilter_chain);
    tap_nat_conntrack_rule.add_expr(&nft_expr!(ct state));
    let allowed_states =
        nftnl::expr::ct::States::ESTABLISHED.bits() | nftnl::expr::ct::States::RELATED.bits();
    tap_nat_conntrack_rule.add_expr(&nft_expr!(bitwise mask allowed_states, xor 0u32));
    tap_nat_conntrack_rule.add_expr(&nft_expr!(cmp != 0u32));
    tap_nat_conntrack_rule.add_expr(&nft_expr!(verdict accept));
    batch.add(&tap_nat_conntrack_rule, MsgType::Add);

    // Here I'm working from the "Source NAT" section
    // https://wiki.nftables.org/wiki-nftables/index.php/Performing_Network_Address_Translation_(NAT)
    // Also see https://github.com/mullvad/mullvadvpn-app/blob/master/talpid-core/src/firewall/linux.rs#L287
    // 'add chain nat postrouting { type nat hook postrouting priority 100 ; }'
    let nat_chain_name = format!("{NAT_CHAIN_NAME}");
    let mut nat_chain = Chain::new(&CString::new(nat_chain_name.clone())?, &table);
    // Not entirely sure why, but mullvad sets this to NF_IP_PRI_NAT_SRC instead of 0
    nat_chain.set_hook(Hook::PostRouting, libc::NF_IP_PRI_NAT_SRC);
    nat_chain.set_type(ChainType::Nat);
    nat_chain.set_policy(Policy::Drop);
    batch.add(&nat_chain, MsgType::Add);

    // Now that we've set up the table and chain (which we should only have to do once)
    // Let's actually add the NAT rule for our tap device

    // nft add rule inet $nat_chain oifname "wan" masquerade
    let mut tap_nat_rule = Rule::new(&nat_chain);

    tap_nat_rule.add_expr(&nft_expr!(meta oifname)); // Load output interface name
    tap_nat_rule.add_expr(&nft_expr!(cmp == wan_interface.as_str())); // Set output interface to default interface

    tap_nat_rule.add_expr(&nft_expr!(meta oifname));
    tap_nat_rule.add_expr(&nft_expr!(cmp != "lo")); // Don't masquerade packets on the loopback device.

    tap_nat_rule.add_expr(&nft_expr!(masquerade)); // Rewrite such that "source address is automagically set to the address of the output interface"

    batch.add(&tap_nat_rule, MsgType::Add);

    // saddr [gateway] oifname "tun[xxxx]*" accept
    let mut allow_host_to_guest_rule = Rule::new(&nat_chain);

    // Load the `nfproto` metadata into the netfilter register. This metadata denotes which layer3
    // protocol the packet being processed is using.
    allow_host_to_guest_rule.add_expr(&nft_expr!(meta nfproto));
    // Check if the currently processed packet is an IPv4 packet. This must be done before payload
    // data assuming the packet uses IPv4 can be loaded in the next expression.
    allow_host_to_guest_rule.add_expr(&nft_expr!(cmp == libc::NFPROTO_IPV4 as u8));
    // Load the IPv4 destination address into the netfilter register.
    allow_host_to_guest_rule.add_expr(&nft_expr!(payload ipv4 saddr));
    // Allow requests to guest so long as they originate from host (i.e gateway ip)
    allow_host_to_guest_rule.add_expr(&nft_expr!(cmp == gateway_addr));
    // and they are destined for the guest
    allow_host_to_guest_rule.add_expr(&nft_expr!(meta oifname));
    allow_host_to_guest_rule.add_expr(&nft_expr!(cmp == devname));
    allow_host_to_guest_rule.add_expr(&nft_expr!(verdict accept));
    batch.add(&allow_host_to_guest_rule, MsgType::Add);

    // iifname lo accept
    let mut allow_loopback = Rule::new(&nat_chain);

    allow_loopback.add_expr(&nft_expr!(meta oifname));
    allow_loopback.add_expr(&nft_expr!(cmp == "lo"));
    allow_loopback.add_expr(&nft_expr!(verdict accept));
    batch.add(&allow_loopback, MsgType::Add);

    let finalized_batch = batch.finalize();

    // Send the entire batch and process any returned messages.
    send_and_process_netfilter(&finalized_batch)?;

    info!("To debug, run: sudo nft add rule inet {nat_table_name} {nat_chain_name} meta nftrace set 1");
    info!("To watch trace, run: sudo nft monitor trace");

    let guest_ip = match network_conf.ip.ip() {
        std::net::IpAddr::V4(v4) => v4,
        std::net::IpAddr::V6(_) => bail!("IPV6 not supported"),
    };

    debug!("Trying to add route to {guest_ip} via {gateway_addr} {devname}");

    // ip r add {guest_ip} via {gateway_ip} dev {tapx}
    netlink_handle
        .route()
        .add()
        .v4()
        .replace()
        .destination_prefix(guest_ip, 32)
        .gateway(gateway_addr)
        .output_interface(nix::net::if_::if_nametoindex(devname)?)
        .execute()
        .await?;

    debug!("Added route to {guest_ip} via {gateway_addr} {devname}");

    sleep(Duration::from_secs(10)).await;

    Ok(tap.name().to_owned())
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

    let ip_config = IPConfig {
        gateway: IpNetwork::from_str("172.50.0.1")?,
        ip: IpNetwork::from_str("172.50.0.2/24")?,
    };

    let tap_dev_name = setup_networking(&vm_id, &ip_config, None).await?;

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

    let iface = Interface::new(tap_dev_name, "tap0");

    let config = firec::config::Config::builder(Some(vm_id), Path::new("vmlinux-self-5.19.bin"))
        .jailer_cfg()
        .chroot_base_dir(tempdir.path())
        .exec_file(Path::new("/usr/bin/firecracker"))
        .build()
        .kernel_args(kernel_args)
        .machine_cfg()
        .vcpu_count(2)
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
