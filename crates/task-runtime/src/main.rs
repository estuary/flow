use std::{
    fmt,
    path::{Path, PathBuf},
    process::Stdio,
    str::FromStr,
};

use anyhow::{anyhow, Context};
use clap::Parser;
use cni::PortMapping;
use connector_init::config::{EtcHost, EtcResolv, GuestConfigBuilder};
use fancy_regex::Regex;
use firec::{
    config::{network::Interface, JailerMode},
    Machine,
};
use futures::future::OptionFuture;
use ipnetwork::Ipv4Network;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::signal::unix;
use tracing::{debug, error, info, metadata::LevelFilter};
use tracing_subscriber::{
    fmt::format::Format, prelude::__tracing_subscriber_SubscriberExt, EnvFilter, Layer,
};
use uuid::Uuid;

pub mod cni;
pub mod firecracker;

// There's probably a crate for this
#[derive(Clone, Debug)]
struct EnvVar {
    key: String,
    val: String,
}

impl FromStr for EnvVar {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split("=");
        let key = split
            .next()
            .ok_or(anyhow!("Invalid environment variable {s}"))?
            .to_owned();
        let val = split
            .next()
            .ok_or(anyhow!("Invalid environment variable {s}"))?
            .to_owned();

        Ok(Self { key, val })
    }
}

impl fmt::Display for EnvVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.val)
    }
}

#[derive(Debug, Clone, clap::ValueEnum)]
#[clap(rename_all = "lower")]
enum LogFormat {
    Default,
    Json,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to a directory containing the CNI plugins needed to set up firecracker networking.
    /// Currently these are: ptp, host-local, firewall, and tc-redirect-tap
    #[clap(
        long = "cni-path",
        env = "CNI_PATH",
        verbatim_doc_comment,
        default_value = "/opt/cni/bin"
    )]
    cni_path: PathBuf,
    /// Path to the firecracker binary. If not specified, PATH will be searched
    #[clap(long = "firecracker-path", env = "FIRECRACKER_PATH")]
    firecracker_path: Option<PathBuf>,
    /// Path to a built `flow-connector-init` binary to inject as the init program
    #[clap(long = "init-program", env = "INIT_PROGRAM")]
    init_program: PathBuf,
    /// Path to an uncompressed linux kernel build
    #[clap(long = "kernel", env = "KERNEL")]
    kernel_path: PathBuf,
    /// The name of the image to build and run, as understood by a docker-like registry
    /// e.g `hello-world`, `quay.io/podman/hello`
    #[clap(long = "image-name", env = "IMAGE_NAME")]
    image_name: String,
    /// Ports to expose from the guest to the host, in the format of:
    /// 8080:80 - Map TCP port 80 in the guest to port 8080 on the host.
    /// 8080:80/udp - Map UDP port 80 in the guest to port 8080 on the host.
    #[clap(short = 'p', long="publish", name="PORT_MAPPING", action = clap::ArgAction::Append, required = false, verbatim_doc_comment)]
    port_mappings: Vec<PortMapping>,
    /// Environment variables to set inside the running VM
    #[clap(long="env", name="ENV_VAR", action = clap::ArgAction::Append, required = false)]
    env_vars: Vec<EnvVar>,
    /// Allocate and assign VMs IPs from this range
    #[clap(long = "subnet", env = "SUBNET", default_value = "192.168.200.0/24")]
    subnet: Ipv4Network,
    /// Attach to VM stdout/stderr. If `only-vm-logs` is not set, then VM output will be
    /// logged as normal log messages
    #[clap(long = "attach", env = "ATTACH", action)]
    attach: bool,
    /// Stream raw VM stdout/stderr without wrapping with tracing
    #[clap(long = "raw-vm-logs", env = "RAW_VM_LOGS", action, requires("attach"))]
    raw_vm_logs: bool,
    /// Memory size in mb
    #[clap(long = "memory", env = "MEMORY_SIZE_MB", default_value = "1024")]
    mem_size_mb: usize,
    /// Number of virtual CPU cores
    #[clap(long = "cpus", env = "CPU_CORE_COUNT", default_value = "1")]
    cpus: usize,
    /// Log format
    #[clap(
        long = "log-format",
        env = "LOG_FORMAT",
        default_value = "default",
        value_enum
    )]
    log_format: LogFormat,
    /// Args to pass to the init program
    #[clap(raw(true), required(false), action = clap::ArgAction::Append)]
    init_args: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let env_filter_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let output_layer = match args.log_format {
        LogFormat::Default => tracing_subscriber::fmt::layer()
            .event_format(Format::default().compact())
            .boxed(),
        LogFormat::Json => {
            ops::tracing::Layer::new(ops::stderr_log_handler, time::OffsetDateTime::now_utc).boxed()
        }
    };

    let subscriber = tracing_subscriber::registry()
        .with(env_filter_layer)
        .with(output_layer);

    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");

    tracing_log::LogTracer::init()?;

    let vm_id = Uuid::new_v4();
    info!(?args, ?vm_id, "Starting!");

    let tempdir = tempfile::Builder::new()
        .rand_bytes(2)
        .tempdir_in("/tmp")
        .unwrap();

    // Is there a better way to do this?
    let firecracker_path = match args.firecracker_path {
        Some(path) => path,
        None => which::which("firecracker").context("Finding firecracker executable")?,
    };

    let image_conf = firecracker::get_image_config(args.image_name.to_owned()).await?;

    let (cleanup_handle, tap_dev_name, netns_path, ip_config) =
        firecracker::FirecrackerNetworking::new(
            vm_id.clone(),
            tempdir.path().to_path_buf(),
            args.cni_path,
            args.subnet,
            Some(args.port_mappings),
        )
        .setup_networking()
        .await?;

    let guest_conf = GuestConfigBuilder::default()
        .root_device("/dev/vdb") // Assuming that the second disk becomes vdb...
        .hostname(vm_id.to_string()[..5].to_string())
        .ip_configs(vec![ip_config.clone()])
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

    let init_fs = firecracker::setup_init_fs(
        &tempdir.path().to_path_buf(),
        &args.init_program,
        image_conf,
        guest_conf,
    )?;

    let main_fs = firecracker::setup_root_fs(&tempdir.path(), args.image_name.to_owned()).await?;

    let custom_env = args
        .env_vars
        .iter()
        .map(EnvVar::to_string)
        .collect::<Vec<String>>()
        .join(" ");

    let init_args = args.init_args.join(" ");

    let kernel_args = format!(
        "console=ttyS0 reboot=k panic=1 pci=off random.trust_cpu=on loglevel=0 \
        i8042.noaux i8042.nomux i8042.nopnp i8042.dumbkbd \
        RUST_LOG=debug RUST_BACKTRACE=full LOG_LEVEL=debug {custom_env} \
        init=/flow/init -- \
            --firecracker \
            --image-inspect-json-path /flow/image_inspect.json \
            --guest-config-json-path /flow/guest_config.json {init_args}"
    );

    debug!(args = kernel_args, "Kernel args:");

    let iface = Interface::new(tap_dev_name, "eth0");

    let stdio = if args.attach {
        firec::config::Stdio {
            stdout: Some(Stdio::piped()),
            stderr: Some(Stdio::piped()),
            stdin: Some(Stdio::null()),
        }
    } else {
        firec::config::Stdio {
            stdout: Some(Stdio::null()),
            stderr: Some(Stdio::null()),
            stdin: Some(Stdio::null()),
        }
    };

    let config = firec::config::Config::builder(Some(vm_id), args.kernel_path)
        .jailer_cfg()
        .chroot_base_dir(tempdir.path())
        .exec_file(firecracker_path)
        .mode(JailerMode::Attached(stdio))
        .build()
        .net_ns(netns_path)
        .kernel_args(kernel_args)
        .machine_cfg()
        // TODO: `firec` doesn't actually configure firecracker to use these values
        .vcpu_count(args.cpus)
        .mem_size_mib(args.mem_size_mb.try_into().unwrap())
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

    let mut child = machine.start().await?;

    let mut stdout_lines = child.stdout.take().map(|s| BufReader::new(s).lines());
    let mut stderr_lines = child.stderr.take().map(|s| BufReader::new(s).lines());

    // We have to drive child's future so data gets copied to its stdout and stderr streams
    let handle = tokio::spawn(async move {
        child
            .wait_with_output()
            .await
            .expect("child process encountered an error");
    });

    if args.attach {
        info!(?vm_id, "VM is running in attached mode. Output follows:");
    } else {
        info!(?vm_id, "VM is running in detached mode.");
    }

    info!(ip=?ip_config.ip, "VM was assigned IP");

    // Gracefully exit on either SIGINT (ctrl-c) or SIGTERM.
    let mut sigint = unix::signal(unix::SignalKind::interrupt()).unwrap();
    let mut sigterm = unix::signal(unix::SignalKind::terminate()).unwrap();

    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    loop {
        let res: anyhow::Result<bool> = tokio::select! {
            Some(maybe_line) = OptionFuture::from(stdout_lines.as_mut().map(|s|s.next_line())) => {
                if let Some(line) = maybe_line? {
                    if args.raw_vm_logs  {
                        stdout.write_all(format!("{line}\n").as_ref()).await?;
                    } else {
                        info!(stream="stdout",line)
                    }
                }
                Ok(false)
            }
            Some(maybe_line) = OptionFuture::from(stderr_lines.as_mut().map(|s|s.next_line())) => {
                if let Some(line) = maybe_line? {
                    if args.raw_vm_logs  {
                        stderr.write_all(format!("{line}\n").as_ref()).await?;
                    } else {
                        info!(stream="stdout",line)
                    }
                }
                Ok(false)
            },
            _ = sigint.recv() => Ok(true),
            _ = sigterm.recv() => Ok(true)
        };

        match res {
            Ok(true) => {
                info!("Caught exit signal, shutting down");
                break;
            }
            Ok(false) => {
                // Successfully logged a line from stdout/stderr
                continue;
            }
            Err(e) => {
                error!("Error reading from process stdout/stderr: {e}");
                break;
            }
        }
    }

    handle.abort();

    machine.force_shutdown().await?;

    // Clean up networking
    drop(cleanup_handle);
    // Clean up filesystem
    drop(tempdir);

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
