//! VMM capability, connector start admission, and the plan, launch, ownership
//! and release of a VMM.
use anyhow::Context;
use proto_flow::flow;

pub(crate) mod launch;
mod plan;
mod record;
mod release;

/// VMM capability of a connector service, and the configuration every VMM it
/// launches shares.
#[derive(Clone, Debug, PartialEq)]
pub struct Vmm {
    /// Image of `flow-connector-vmm`, which hosts each VMM.
    pub image: String,
    /// The podman program VMM containers run under: the host's rootful
    /// podman, however this process reaches it.
    pub podman: String,
    /// Host directory holding each VMM's `fv_<id>` state, at the same path
    /// for this process and the engine.
    pub state_dir: String,
    /// Size of each VMM's scratch disk.
    pub disk_mib: u64,
    /// `CONNECTOR_MEMORY_LIMIT`, the container's cgroup cap, as an ordinary
    /// connector container is given it.
    pub memory_limit: String,
    /// `CONNECTOR_CPU_LIMIT`, the container's CPU quota, likewise.
    pub cpu_limit: String,
    /// Guest RAM: the memory limit less the VMM's own overhead.
    pub guest_memory_mib: u32,
    /// Guest vCPUs: the CPU limit, rounded up.
    pub vcpus: u8,
    /// `CONNECTOR_CGROUP_PARENT`, likewise.
    pub cgroup_parent: Option<String>,
}

const IMAGE: &str = "CONNECTOR_VMM_IMAGE";
const PODMAN: &str = "CONNECTOR_VMM_PODMAN";
const STATE_DIR: &str = "CONNECTOR_VMM_STATE_DIR";
const DISK_MIB: &str = "CONNECTOR_VMM_DISK_MIB";
const MEMORY_OVERHEAD_MIB: &str = "CONNECTOR_VMM_MEMORY_OVERHEAD_MIB";

/// Room for the uv cache, the virtual environment and TMPDIR of a Python
/// derivation. The file is sparse, so the host pays for what the guest writes.
const DEFAULT_DISK_MIB: u64 = 2048;
/// Container memory above guest RAM: the VMM's own allocations, its virtiofs
/// servers, guest page tables, and page cache charged to the container.
const DEFAULT_MEMORY_OVERHEAD_MIB: u32 = 256;

const MIB: u64 = 1 << 20;

impl Vmm {
    /// Read `CONNECTOR_VMM_IMAGE`; unset or empty means no VMM capability,
    /// and nothing else is read. Otherwise read and check the remaining
    /// settings, failing on any that is malformed.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        Self::from_vars(|name| match std::env::var(name) {
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(err) => Err(anyhow::Error::new(err).context(format!("reading {name}"))),
            Ok(value) => Ok(Some(value)),
        })
    }

    /// The configuration `var` describes. Checks are of values alone: whether
    /// the host can run a VMM is decided by each launch.
    fn from_vars(
        var: impl Fn(&str) -> anyhow::Result<Option<String>>,
    ) -> anyhow::Result<Option<Self>> {
        let setting = |name: &str| Ok::<_, anyhow::Error>(var(name)?.filter(|v| !v.is_empty()));

        let Some(image) = setting(IMAGE)? else {
            return Ok(None);
        };
        let podman = setting(PODMAN)?.unwrap_or_else(|| "podman".to_string());

        let Some(state_dir) = setting(STATE_DIR)? else {
            anyhow::bail!("{STATE_DIR} is required when {IMAGE} is set");
        };
        plan::check_state_dir(&state_dir).with_context(|| format!("{STATE_DIR} {state_dir:?}"))?;

        let disk_mib = match setting(DISK_MIB)? {
            None => DEFAULT_DISK_MIB,
            Some(raw) => disk_mib(&raw).with_context(|| format!("{DISK_MIB} {raw:?}"))?,
        };
        let overhead_mib = match setting(MEMORY_OVERHEAD_MIB)? {
            None => DEFAULT_MEMORY_OVERHEAD_MIB,
            Some(raw) => {
                overhead_mib(&raw).with_context(|| format!("{MEMORY_OVERHEAD_MIB} {raw:?}"))?
            }
        };

        // Shared with ordinary connectors, so read as their launch reads them:
        // a set value is used as it is, empty or not.
        let memory_limit = var("CONNECTOR_MEMORY_LIMIT")?
            .unwrap_or_else(|| crate::container::DEFAULT_MEMORY_LIMIT.to_string());
        let cpu_limit = var("CONNECTOR_CPU_LIMIT")?
            .unwrap_or_else(|| crate::container::DEFAULT_CPU_LIMIT.to_string());
        let cgroup_parent = var("CONNECTOR_CGROUP_PARENT")?;

        let guest_memory_mib = guest_memory_mib(&memory_limit, overhead_mib).with_context(|| {
            format!("CONNECTOR_MEMORY_LIMIT {memory_limit:?}, less {overhead_mib} MiB of VMM overhead")
        })?;
        let vcpus =
            vcpus(&cpu_limit).with_context(|| format!("CONNECTOR_CPU_LIMIT {cpu_limit:?}"))?;

        Ok(Some(Self {
            image,
            podman,
            state_dir,
            disk_mib,
            memory_limit,
            cpu_limit,
            guest_memory_mib,
            vcpus,
            cgroup_parent,
        }))
    }
}

/// Guest RAM within a container capped at `limit`: whole MiB of the limit,
/// less the VMM's overhead.
fn guest_memory_mib(limit: &str, overhead_mib: u32) -> anyhow::Result<u32> {
    let guest = (memory_bytes(limit)? / MIB)
        .checked_sub(overhead_mib as u64)
        .filter(|guest| *guest > 0)
        .context("leaves no guest RAM")?;
    u32::try_from(guest)
        .map_err(|_| anyhow::anyhow!("leaves {guest} MiB of guest RAM, above {}", u32::MAX))
}

/// The bytes of a `--memory` value, as podman and Docker read it with
/// go-units' `RAMInBytes`, so that the guest is sized within the very cap the
/// engine applies: a number, which may have a fraction or an exponent, then
/// optionally one space and a binary unit `b`, `k`, `m`, `g`, `t` or `p` in
/// either case, which may end in `b` or `ib`. A fraction of a byte truncates.
/// Of Go's float syntax, only hexadecimal is not read.
fn memory_bytes(limit: &str) -> anyhow::Result<u64> {
    // go-units splits after the last digit, dot or space, and drops one space.
    let Some(split) = limit.rfind(|c: char| c.is_ascii_digit() || c == '.' || c == ' ') else {
        anyhow::bail!("expected a number of bytes, optionally with a unit");
    };
    let (number, unit) = match limit.as_bytes()[split] {
        b' ' => (&limit[..split], &limit[split + 1..]),
        _ => (&limit[..=split], &limit[split + 1..]),
    };

    let size = go_float(number)?;
    if size < 0.0 {
        anyhow::bail!("is negative");
    }
    let multiplier: u64 = match unit.to_ascii_lowercase().as_bytes() {
        [] | [b'b'] => 1,
        [scale] | [scale, b'b'] | [scale, b'i', b'b'] => match scale {
            b'k' => 1 << 10,
            b'm' => 1 << 20,
            b'g' => 1 << 30,
            b't' => 1 << 40,
            b'p' => 1 << 50,
            _ => anyhow::bail!("{unit:?} is not a unit"),
        },
        _ => anyhow::bail!("{unit:?} is not a unit"),
    };

    // The same float multiplication as go-units, whose conversion to int64 is
    // undefined beyond its range rather than refused.
    let bytes = size * multiplier as f64;
    if bytes >= i64::MAX as f64 {
        anyhow::bail!("exceeds the largest byte count");
    }
    Ok(bytes as u64)
}

/// A decimal number as Go's `strconv.ParseFloat` reads one, which Rust's
/// parser matches except that Go also takes `_` between digits.
fn go_float(number: &str) -> anyhow::Result<f64> {
    if number.contains(['x', 'X']) {
        anyhow::bail!("{number:?} is hexadecimal; write it in decimal");
    }
    let bytes = number.as_bytes();
    for (index, _) in number.match_indices('_') {
        let digit = |at: Option<&u8>| at.is_some_and(u8::is_ascii_digit);
        if !digit(index.checked_sub(1).and_then(|i| bytes.get(i))) || !digit(bytes.get(index + 1)) {
            anyhow::bail!("{number:?} has an underscore that does not separate digits");
        }
    }
    match number.replace('_', "").parse::<f64>() {
        Ok(size) if size.is_finite() => Ok(size),
        Ok(_) => anyhow::bail!("{number:?} is not a finite number"),
        Err(_) => anyhow::bail!("{number:?} is not a number"),
    }
}

/// Guest vCPUs for a CPU quota of `limit`: a decimal number of CPUs, rounded
/// up so that the guest can use all of the quota.
fn vcpus(limit: &str) -> anyhow::Result<u8> {
    const EXPECTED: &str = "a decimal number of CPUs, with at most nine fraction digits";

    let (whole, fraction) = limit.split_once('.').unwrap_or((limit, ""));
    let whole = digits(whole, EXPECTED)?;

    if limit.contains('.') && (fraction.is_empty() || fraction.len() > 9) {
        anyhow::bail!("expected {EXPECTED}");
    }
    let fraction = if fraction.is_empty() {
        0
    } else {
        digits(fraction, EXPECTED)?
    };

    let vcpus = whole + u64::from(fraction != 0);
    if vcpus == 0 {
        anyhow::bail!("is no CPU at all");
    }
    u8::try_from(vcpus).map_err(|_| anyhow::anyhow!("is {vcpus} vCPUs, above {}", u8::MAX))
}

/// The scratch disk's size is a file length, so it must fit `off_t`.
fn disk_mib(raw: &str) -> anyhow::Result<u64> {
    let max = i64::MAX as u64 / MIB;

    match digits(raw, "a whole number of MiB")? {
        0 => anyhow::bail!("is an empty disk"),
        mib if mib > max => anyhow::bail!("is above the largest disk, {max} MiB"),
        mib => Ok(mib),
    }
}

/// Zero would leave the VMM process nothing beside its guest.
fn overhead_mib(raw: &str) -> anyhow::Result<u32> {
    match u32::try_from(digits(raw, "a whole number of MiB")?) {
        Ok(0) => anyhow::bail!("leaves the VMM no memory of its own"),
        Ok(mib) => Ok(mib),
        Err(_) => anyhow::bail!("is above {} MiB", u32::MAX),
    }
}

/// ASCII digits only: `str::parse` would also take a leading `+`.
/// `expected` describes the whole value in the refusal of one that isn't.
fn digits(raw: &str, expected: &str) -> anyhow::Result<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        anyhow::bail!("expected {expected}");
    }
    raw.parse().context("is too large")
}

/// A connector which may request VMM execution, by protocol and normalized
/// image repository, and the hosts it may always reach.
#[derive(Debug)]
pub(crate) struct Eligible {
    pub task_type: ops::TaskType,
    pub repository: &'static str,
    pub egress_hosts: &'static [&'static str],
}

/// Python derivations are the initial workload, and install their
/// dependencies from the package index at start.
const ELIGIBLE: &[Eligible] = &[Eligible {
    task_type: ops::TaskType::Derivation,
    repository: "ghcr.io/estuary/derive-python",
    egress_hosts: &["pypi.org", "files.pythonhosted.org"],
}];

fn eligible(task_type: ops::TaskType, image: &str) -> Option<&'static Eligible> {
    let (repository, _tag) = models::split_image_tag(image);

    ELIGIBLE
        .iter()
        .find(|eligible| eligible.task_type == task_type && eligible.repository == repository)
}

/// How an admitted connector start runs.
#[derive(Debug)]
pub(crate) enum Execution<'a> {
    Ordinary,
    /// In a VMM of this service's configuration, as the eligible connector.
    Vmm {
        vmm: &'a Vmm,
        eligible: &'static Eligible,
        egress: Option<Vec<egress::AllowedName>>,
    },
}

/// Decide whether a connector start may proceed, and how, given the
/// `execution` it requests and the VMM capability `vmm` of this service.
/// `image` is the connector's image, or None if its endpoint isn't an image.
/// A request which embeds the task's built spec passes that spec's execution
/// as `spec_execution`, which must agree with `execution`, so that a caller
/// cannot drop the task's settings.
///
/// Every connector start is decided here, before any image is pulled or any
/// connector is started. A request for VMM execution is either launched in a
/// VMM or fails: it never falls back to an ordinary container. Declared
/// egress is refused by any execution which cannot enforce it, and its hosts
/// are validated here, so that a direct caller cannot skip either check.
pub(crate) fn vmm_for<'a>(
    vmm: Option<&'a Vmm>,
    task_type: ops::TaskType,
    image: Option<&str>,
    execution: &flow::ConnectorExecution,
    spec_execution: Option<&flow::ConnectorExecution>,
) -> anyhow::Result<Execution<'a>> {
    check_spec_execution(execution, spec_execution)?;

    if execution.egress.is_some() && !execution.vmm {
        return Err(crate::invalid_argument(
            "the task declares egress, which ordinary execution cannot enforce; \
             egress is enforced only by VMM execution"
                .to_string(),
        ));
    }
    if !execution.vmm {
        return Ok(Execution::Ordinary);
    }
    let Some(image) = image else {
        return Err(crate::invalid_argument(
            "VMM execution requires an image connector".to_string(),
        ));
    };
    let Some(eligible) = eligible(task_type, image) else {
        return Err(crate::invalid_argument(format!(
            "connector image '{image}' is not eligible for VMM execution as a {}",
            task_type.as_str_name()
        )));
    };
    let Some(vmm) = vmm else {
        return Err(proto_grpc::status_to_anyhow(
            tonic::Status::failed_precondition("this data plane does not support VMM execution"),
        ));
    };
    let egress = execution
        .egress
        .as_ref()
        .map(|flow::connector_execution::Egress { hosts }| {
            egress::hosts("task egress.hosts", hosts)
        })
        .transpose()
        .map_err(|err| crate::invalid_argument(format!("{err:#}")))?;

    Ok(Execution::Vmm {
        vmm,
        eligible,
        egress,
    })
}

/// The connector's execution and egress policy are fixed at start; later
/// built specs cannot change them.
pub(crate) fn check_spec_execution(
    execution: &flow::ConnectorExecution,
    spec_execution: Option<&flow::ConnectorExecution>,
) -> anyhow::Result<()> {
    if let Some(spec_execution) = spec_execution
        && spec_execution != execution
    {
        return Err(crate::invalid_argument(format!(
            "Start.execution {execution:?} differs from the execution {spec_execution:?} of the task's built spec"
        )));
    }
    Ok(())
}

/// A capable service's configuration, with every default but a podman and a
/// state directory which do not exist, so that a launch fails at its first
/// step without ever reaching an engine.
#[cfg(test)]
pub(crate) fn fixture() -> Vmm {
    Vmm::from_vars(|name| {
        Ok(match name {
            IMAGE => Some("ghcr.io/estuary/connector-vmm:dev".to_string()),
            PODMAN => Some("/nonexistent/connector-vmm-tests/podman".to_string()),
            STATE_DIR => Some("/nonexistent/connector-vmm-tests/state".to_string()),
            _ => None,
        })
    })
    .expect("the fixture is valid")
    .expect("the fixture is capable")
}

#[cfg(test)]
mod test {
    use super::{Vmm, vmm_for};
    use proto_flow::flow::{ConnectorExecution, connector_execution::Egress};

    #[test]
    fn configuration() {
        const BASE: &[&str] = &[
            "CONNECTOR_VMM_IMAGE=ghcr.io/estuary/connector-vmm:dev",
            "CONNECTOR_VMM_STATE_DIR=/var/lib/flow/connector-vmm",
        ];
        let cases: &[(&str, &[&str])] = &[
            ("nothing set", &[]),
            (
                "no image, so nothing else is read",
                &[
                    "CONNECTOR_VMM_STATE_DIR=relative",
                    "CONNECTOR_VMM_DISK_MIB=x",
                ],
            ),
            ("an empty image", &["CONNECTOR_VMM_IMAGE="]),
            (
                "an image without a state directory",
                &["CONNECTOR_VMM_IMAGE=ghcr.io/estuary/connector-vmm:dev"],
            ),
            (
                "an empty state directory",
                &[BASE[0], "CONNECTOR_VMM_STATE_DIR="],
            ),
            ("defaults", BASE),
            (
                "every setting",
                &[
                    BASE[0],
                    BASE[1],
                    "CONNECTOR_VMM_PODMAN=/usr/local/bin/podman",
                    "CONNECTOR_VMM_DISK_MIB=4096",
                    "CONNECTOR_VMM_MEMORY_OVERHEAD_MIB=128",
                    "CONNECTOR_MEMORY_LIMIT=2g",
                    "CONNECTOR_CPU_LIMIT=1.5",
                    "CONNECTOR_CGROUP_PARENT=estuary-connectors.slice",
                ],
            ),
            (
                "empty VMM settings take their defaults",
                &[
                    BASE[0],
                    BASE[1],
                    "CONNECTOR_VMM_PODMAN=",
                    "CONNECTOR_VMM_DISK_MIB=",
                    "CONNECTOR_VMM_MEMORY_OVERHEAD_MIB=",
                ],
            ),
            (
                "an empty memory limit is used as an ordinary launch uses it",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT="],
            ),
            (
                "a relative state directory",
                &[BASE[0], "CONNECTOR_VMM_STATE_DIR=var/lib/flow"],
            ),
            (
                "a malformed disk size",
                &[BASE[0], BASE[1], "CONNECTOR_VMM_DISK_MIB=2g"],
            ),
            (
                "a malformed overhead",
                &[BASE[0], BASE[1], "CONNECTOR_VMM_MEMORY_OVERHEAD_MIB=-1"],
            ),
            (
                "an overhead consuming the whole limit",
                &[BASE[0], BASE[1], "CONNECTOR_VMM_MEMORY_OVERHEAD_MIB=1024"],
            ),
            (
                "a fractional memory limit",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=1.5g"],
            ),
            (
                "a memory limit podman cannot read either",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=1.5gg"],
            ),
            (
                "a malformed CPU limit",
                &[BASE[0], BASE[1], "CONNECTOR_CPU_LIMIT=two"],
            ),
        ];

        let mut table = String::new();
        for (label, vars) in cases {
            let vars: std::collections::BTreeMap<&str, &str> = vars
                .iter()
                .map(|var| var.split_once('=').expect("NAME=value"))
                .collect();
            let outcome = Vmm::from_vars(|name| Ok(vars.get(name).map(ToString::to_string)));

            table.push_str(&format!("# {label}\n"));
            for (name, value) in &vars {
                table.push_str(&format!("{name}={value}\n"));
            }
            match outcome {
                Ok(vmm) => table.push_str(&format!("=> {vmm:#?}\n\n")),
                Err(error) => table.push_str(&format!("=> error: {error:#}\n\n")),
            }
        }
        insta::assert_snapshot!(table);
    }

    /// `--memory` readings observed from the Docker 29.1.3 CLI, which parses
    /// with the go-units `RAMInBytes` that podman 4.9 also uses. None is a
    /// spelling the CLI refused, or read as a value beyond int64 that the
    /// daemon then refused. Spellings read below the daemon's 6 MB minimum
    /// could not be observed and are absent.
    const DOCKER_READINGS: &[(&str, Option<u64>)] = &[
        ("1g", Some(1073741824)),
        ("1.5g", Some(1610612736)),
        ("1.5G", Some(1610612736)),
        ("1gb", Some(1073741824)),
        ("1GiB", Some(1073741824)),
        ("1.5gib", Some(1610612736)),
        ("512 m", Some(536870912)),
        ("1.5 GiB", Some(1610612736)),
        ("1e3m", Some(1048576000)),
        ("1E3m", Some(1048576000)),
        ("1e+3m", Some(1048576000)),
        ("1e-1g", Some(107374182)),
        ("+1g", Some(1073741824)),
        (".5g", Some(536870912)),
        ("+.5g", Some(536870912)),
        ("0.1g", Some(107374182)),
        ("1073741824", Some(1073741824)),
        ("1073741823.7", Some(1073741823)),
        ("6291456.", Some(6291456)),
        ("1e7", Some(10000000)),
        ("1e7 b", Some(10000000)),
        ("1e9 ", Some(1000000000)),
        ("10000k", Some(10240000)),
        ("10000kb", Some(10240000)),
        ("10000kib", Some(10240000)),
        ("10mib", Some(10485760)),
        ("10mB", Some(10485760)),
        ("10 mb", Some(10485760)),
        ("10MIB", Some(10485760)),
        ("1tb", Some(1099511627776)),
        ("0.000001p", Some(1125899906)),
        ("4294967552m", Some(4503599895805952)),
        ("8.5e18", Some(8500000000000000000)),
        ("9.2e18", Some(9200000000000000000)),
        ("1_000m", Some(1048576000)),
        ("1_000.5m", Some(1049100288)),
        ("1_0e1m", Some(104857600)),
        ("1e1_0", Some(10000000000)),
        ("-0g", Some(0)),
        ("0.5 b", Some(0)),
        ("1__0m", None),
        ("_10m", None),
        ("10_m", None),
        ("1bb", None),
        ("1ib", None),
        ("1x", None),
        ("1kibb", None),
        ("1e400g", None),
        (" 1g", None),
        ("1g ", None),
        ("1  g", None),
        ("\t10m", None),
        ("1.2.3g", None),
        ("0x40000000", None),
        ("-1g", None),
        ("inf", None),
        ("inf g", None),
        ("nan g", None),
        ("Infinity g", None),
        ("9.3e18", None),
        ("1e19", None),
        ("9223372036854775807", None),
        ("17179869184g", None),
    ];

    #[test]
    fn memory_limits_read_as_docker_reads_them() {
        for (limit, docker) in DOCKER_READINGS {
            let ours = super::memory_bytes(limit);
            assert_eq!(ours.as_ref().ok(), docker.as_ref(), "{limit:?}: {ours:?}");
        }
        // The one spelling the CLI reads and the VMM path does not.
        assert!(super::memory_bytes("0x1p30").is_err());
    }

    #[test]
    fn resources() {
        fn show<T: std::fmt::Display>(result: anyhow::Result<T>) -> String {
            match result {
                Ok(value) => value.to_string(),
                Err(error) => format!("error: {error:#}"),
            }
        }
        let mut table = String::new();

        table.push_str("## guest memory MiB, less 256 MiB of overhead\n");
        for limit in [
            "1g",
            "1.5g",
            "1.5 GiB",
            "1280m",
            "1073741823",
            "0.1g",
            "257m",
            "256m",
            "4294967551m",
            "4294967552m",
            "17179869184g",
            "1e400g",
            "nan g",
            "0x1p30",
            "1_000m",
            "1__0m",
            "-1g",
            "1.5gg",
            "1  g",
            "g",
            "",
        ] {
            let guest = super::guest_memory_mib(limit, 256);
            table.push_str(&format!("{limit:?} => {}\n", show(guest)));
        }

        table.push_str("\n## guest vCPUs\n");
        for limit in [
            "2",
            "1.5",
            "0.5",
            "0.000000001",
            "0.0000000001",
            "1.0",
            "255",
            "254.1",
            "256",
            "0",
            "0.0",
            "1.",
            ".5",
            "-1",
            "+1",
            "1e3",
            "",
            "99999999999999999999",
        ] {
            table.push_str(&format!("{limit:?} => {}\n", show(super::vcpus(limit))));
        }

        table.push_str("\n## disk MiB\n");
        for raw in [
            "2048",
            "1",
            "0",
            "8796093022207",
            "8796093022208",
            "+5",
            "2g",
            " 5",
        ] {
            table.push_str(&format!("{raw:?} => {}\n", show(super::disk_mib(raw))));
        }

        table.push_str("\n## memory overhead MiB\n");
        for raw in ["256", "1", "0", "4294967295", "4294967296", "64m"] {
            table.push_str(&format!("{raw:?} => {}\n", show(super::overhead_mib(raw))));
        }
        insta::assert_snapshot!(table);
    }

    fn outcome(result: anyhow::Result<super::Execution>) -> String {
        match result {
            Ok(super::Execution::Ordinary) => "ordinary".to_string(),
            Ok(super::Execution::Vmm { eligible, .. }) => {
                format!("vmm, eligible as {}", eligible.repository)
            }
            Err(err) => {
                let status = err.downcast_ref::<proto_grpc::StatusError>().unwrap();
                format!("{:?}: {}", status.code(), status.message())
            }
        }
    }

    fn execution(vmm: bool, hosts: Option<&[&str]>) -> ConnectorExecution {
        ConnectorExecution {
            vmm,
            egress: hosts.map(|hosts| Egress {
                hosts: hosts.iter().map(ToString::to_string).collect(),
            }),
        }
    }

    #[test]
    fn vmm_for_matrix() {
        let capable = super::fixture();
        let ordinary = execution(false, None);
        let vmm = execution(true, None);

        let images = [
            Some("ghcr.io/estuary/derive-python:stable"),
            Some("ghcr.io/estuary/derive-python:local"),
            Some("ghcr.io/estuary/derive-python@sha256:0123abcd"),
            Some("ghcr.io/estuary/derive-python:stable@sha256:0123abcd"),
            Some("ghcr.io/estuary/derive-python"),
            Some("ghcr.io/estuary/derive-typescript:stable"),
            Some("ghcr.io/estuary/derive-python-extra:stable"),
            Some("GHCR.IO/estuary/derive-python:stable"),
            Some("example.com/estuary/derive-python:stable"),
            Some("ghcr.io/estuary/source-hello-world:dev"),
            None,
        ];
        let task_types = [
            ops::TaskType::Capture,
            ops::TaskType::Derivation,
            ops::TaskType::Materialization,
        ];

        let mut rows = Vec::new();
        for capability in [None, Some(&capable)] {
            for task_type in task_types {
                for image in images {
                    for execution in [&ordinary, &vmm] {
                        let outcome =
                            outcome(vmm_for(capability, task_type, image, execution, None));
                        if !execution.vmm {
                            assert_eq!(outcome, "ordinary", "{task_type:?} {image:?}");
                            continue;
                        }
                        rows.push(format!(
                            "{capable:<7} {task_type:<15} {image:<53} => {outcome}",
                            capable = if capability.is_some() { "capable" } else { "-" },
                            task_type = task_type.as_str_name(),
                            image = image.unwrap_or("<not an image>"),
                        ));
                    }
                }
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }

    #[test]
    fn vmm_for_egress() {
        let capable = super::fixture();
        let images = [
            Some("ghcr.io/estuary/derive-python:stable"),
            Some("ghcr.io/estuary/derive-typescript:stable"),
            Some("ghcr.io/estuary/source-hello-world:dev"),
            None,
        ];
        let executions = [
            ("ordinary, egress []", execution(false, Some(&[]))),
            (
                "ordinary, egress [api]",
                execution(false, Some(&["api.acmeco.example"])),
            ),
            ("vmm, egress []", execution(true, Some(&[]))),
            (
                "vmm, egress [API, *.svc, api]",
                execution(
                    true,
                    Some(&[
                        "API.acmeco.example",
                        "*.svc.acmeco.example",
                        "api.acmeco.example",
                    ]),
                ),
            ),
            (
                "vmm, egress [*.com]",
                execution(true, Some(&["api.acmeco.example", "*.com"])),
            ),
        ];

        let mut rows = Vec::new();
        for capability in [None, Some(&capable)] {
            for image in images {
                for (name, execution) in &executions {
                    let outcome = match vmm_for(
                        capability,
                        ops::TaskType::Derivation,
                        image,
                        execution,
                        None,
                    ) {
                        Ok(super::Execution::Vmm { egress, .. }) => {
                            let hosts: Option<Vec<String>> =
                                egress.map(|hosts| hosts.iter().map(ToString::to_string).collect());
                            format!("vmm, task hosts {hosts:?}")
                        }
                        result => outcome(result),
                    };
                    rows.push(format!(
                        "{capable:<7} {image:<40} {name:<29} => {outcome}",
                        capable = if capability.is_some() { "capable" } else { "-" },
                        image = image.unwrap_or("<not an image>"),
                    ));
                }
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }

    #[test]
    fn vmm_for_requires_the_built_spec_execution() {
        let image = Some("ghcr.io/estuary/derive-python:stable");
        let derivation = ops::TaskType::Derivation;
        let executions = [
            ("ordinary", execution(false, None)),
            ("vmm", execution(true, None)),
            ("vmm egress []", execution(true, Some(&[]))),
            (
                "vmm egress [api]",
                execution(true, Some(&["api.acmeco.example"])),
            ),
            (
                "vmm egress [API]",
                execution(true, Some(&["API.acmeco.example"])),
            ),
        ];

        let mut rows = Vec::new();
        for (start_name, start) in &executions {
            for (spec_name, spec) in &executions {
                let outcome = outcome(vmm_for(None, derivation, image, start, Some(spec)));
                rows.push(format!("start {start_name} spec {spec_name} => {outcome}"));
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }
}
