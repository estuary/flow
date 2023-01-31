use anyhow::{anyhow, bail, Context};
use bollard::container::RemoveContainerOptions;
use cmd_lib::{run_cmd, run_fun};
use connector_init::config::{GuestConfig, IPConfig, Image, ImageConfig};
use ipnetwork::{IpNetwork, Ipv4Network};
use serde_json::json;
use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};
use tokio::runtime::Handle;
use tracing::{debug, error};
use uuid::Uuid;

/// Copy init binary, image inspect JSON, and guest config JSON
#[tracing::instrument(err, skip_all)]
pub fn setup_init_fs(
    temp_dir: &PathBuf,
    init_bin_path: &PathBuf,
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
pub async fn setup_root_fs(temp_dir: &Path, image_name: String) -> anyhow::Result<PathBuf> {
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
pub async fn get_image_config(image: String) -> anyhow::Result<Image> {
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

fn generate_cni_config(name: String, subnet: Ipv4Network) -> Result<String, serde_json::Error> {
    serde_json::to_string(&serde_json::json!({
        "name": name,
        "cniVersion": "1.0.0",
        "plugins": [
            {
                "type": "ptp",
                "ipMasq": true,
                "ipam": {
                  "type": "host-local",
                  "subnet": subnet.to_string(),
                  "resolvConf": "/etc/resolv.conf"
                }
            },
            {
                "type": "firewall"
            },
            {
                "type": "tc-redirect-tap"
            }
        ]
    }))
}

#[derive(Debug)]
pub struct FirecrackerNetworking {
    vm_id: Uuid,
    temp_dir: PathBuf,
    cni_plugins_path: PathBuf,
    guest_subnet: Ipv4Network,
}

pub struct FirecrackerNetworkingDropHandle {
    networking: FirecrackerNetworking,
    network_namespace_name: String,
}

impl Drop for FirecrackerNetworkingDropHandle {
    fn drop(&mut self) {
        // Rust doesn't support async drops, so theoretically this is a solution
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async move {
                self.networking
                    .teardown_networking(self.network_namespace_name.clone())
                    .await
                    .context("Tearing down CNI network")
                    .unwrap()
            });
        });
    }
}

const CNI_CFG_NAME: &str = "firecracker_networking";

impl FirecrackerNetworking {
    pub fn new(
        vm_id: Uuid,
        temp_dir: PathBuf,
        cni_plugins_path: PathBuf,
        guest_subnet: Ipv4Network,
    ) -> Self {
        FirecrackerNetworking {
            vm_id: vm_id,
            temp_dir: temp_dir,
            cni_plugins_path: cni_plugins_path,
            guest_subnet: guest_subnet,
        }
    }

    #[tracing::instrument(err, skip_all)]
    pub async fn setup_networking(
        self,
    ) -> anyhow::Result<(FirecrackerNetworkingDropHandle, String, String, IPConfig)> {
        let mut id_prefix = self.vm_id.to_string();
        id_prefix.truncate(4);

        let netns_name = format!("ns-{id_prefix}");
        rtnetlink::NetworkNamespace::add(netns_name.clone()).await?;
        debug!(netns = netns_name, "Created network namespace");
        let netns_path = format!("/var/run/netns/{netns_name}");

        // let cni_path = "/home/js/cniplugins";
        let cni_config = generate_cni_config(CNI_CFG_NAME.to_owned(), self.guest_subnet)?;
        let cni_config_filename = self.temp_dir.join(format!("{CNI_CFG_NAME}.conflist"));
        std::fs::write(cni_config_filename, cni_config)?;

        let confpath = self.temp_dir.display().to_string();
        let plugins_path = self.cni_plugins_path.clone();

        let cni_response = run_fun!(CNI_PATH=$plugins_path NETCONFPATH=$confpath cnitool add $CNI_CFG_NAME $netns_path)?;

        let parsed =
            serde_json::from_str::<crate::cni::Result>(cni_response.as_ref()).map_err(|e| {
                error!("Failed to load JSON result from cnitool. Response: {cni_response}");
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
            FirecrackerNetworkingDropHandle {
                networking: self,
                network_namespace_name: netns_name,
            },
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

    #[tracing::instrument(err, skip_all)]
    async fn teardown_networking(&self, netns_name: String) -> anyhow::Result<()> {
        let confpath = self.temp_dir.display().to_string();
        let plugins_path = self.cni_plugins_path.clone();
        let netns_path = format!("/var/run/netns/{netns_name}");

        run_fun!(CNI_PATH=$plugins_path NETCONFPATH=$confpath cnitool del $CNI_CFG_NAME $netns_path)?;
        debug!(vm_id=?self.vm_id, network_name=CNI_CFG_NAME, "CNI network has been deleted");
        rtnetlink::NetworkNamespace::del(netns_name.clone()).await?;
        debug!(netns_name, "Network namespace has been deleted");
        Ok(())
    }
}
