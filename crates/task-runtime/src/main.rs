use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::Context;
use bollard::container::RemoveContainerOptions;
use cmd_lib::{init_builtin_logger, run_cmd};
use connector_init::config::{
    EtcHost, EtcResolv, GuestConfig, GuestConfigBuilder, IPConfig, Image, ImageConfig,
};
use firec::{config::network::Interface, Machine};
use ipnetwork::IpNetwork;
use serde_json::json;
use tokio::time::sleep;
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Copy init binary, image inspect JSON, and guest config JSON
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

    Ok(temp_dir.join("initfs"))
}

/// NOTE: This logic will eventually be replaced by containerd,
/// there were just too many moving pieces to get that working correctly
/// in addition to everything else
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

    Ok(temp_dir.join("rootfs.ext4"))
}

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
    Ok(img)
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

    init_builtin_logger();

    let image_name = "hello-world";

    let vm_id = Uuid::new_v4();

    let tempdir = tempfile::Builder::new()
        // .prefix(&format!("vm-{}", vm_id_short))
        .tempdir()
        .unwrap();

    let image_conf = get_image_config(image_name.to_owned()).await?;

    let guest_conf = GuestConfigBuilder::default()
        .root_device("/dev/vdb") // Assuming that the second disk becomes vdb...
        .hostname("test-hello".to_owned())
        .ip_configs(vec![IPConfig {
            gateway: IpNetwork::from_str("172.16.0.1")?,
            ip: IpNetwork::from_str("172.16.0.2")?,
        }])
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

    let iface = Interface::new("eth0", "tap0");

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

    // Let the machine run for a bit before we KILL IT :)
    sleep(Duration::from_secs(30)).await;

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
