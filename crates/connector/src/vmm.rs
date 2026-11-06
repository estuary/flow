//! VMM capability, connector start admission, and the plan, launch, ownership,
//! release and recovery of a VMM.
use anyhow::Context;
use proto_flow::flow;

pub(crate) mod launch;
mod plan;
mod record;
mod recovery;
mod release;

pub use recovery::{Metrics as RecoveryMetrics, Recovery};

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
    /// The container's memory cap and the guest RAM within it, or why
    /// `CONNECTOR_MEMORY_LIMIT` cannot size a VMM. `vmm_for` refuses every
    /// VMM start with the latter.
    pub memory: Result<Memory, String>,
    /// `CONNECTOR_CPU_LIMIT`, the container's CPU quota, as an ordinary
    /// connector container is given it.
    pub cpu_limit: String,
    /// Guest vCPUs: the CPU limit, rounded up.
    pub vcpus: u8,
    /// `CONNECTOR_CGROUP_PARENT`, likewise.
    pub cgroup_parent: Option<String>,
    /// Launch a VMM image whose build is not paired with this launcher's
    /// (`check_pairing`). For development only.
    pub allow_unpaired_build: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Memory {
    /// `CONNECTOR_MEMORY_LIMIT` in bytes, the container's cgroup cap.
    pub bytes: u64,
    /// Guest RAM: the cap's whole MiB less the VMM's own overhead.
    pub guest_mib: u32,
}

const IMAGE: &str = "CONNECTOR_VMM_IMAGE";
const PODMAN: &str = "CONNECTOR_VMM_PODMAN";
const STATE_DIR: &str = "CONNECTOR_VMM_STATE_DIR";
const DISK_MIB: &str = "CONNECTOR_VMM_DISK_MIB";
const MEMORY_OVERHEAD_MIB: &str = "CONNECTOR_VMM_MEMORY_OVERHEAD_MIB";
const ALLOW_UNPAIRED_BUILD: &str = "CONNECTOR_VMM_ALLOW_UNPAIRED_BUILD";

/// Source identity from `mise/tasks/build/identity-lib.sh`.
/// Missing or empty identities are unidentified builds.
pub const BUILD_IDENTITY: Option<&str> = option_env!("FLOW_BUILD_IDENTITY");

/// Compiled launcher identity, or `unidentified` when missing or empty.
pub const REPORTED_IDENTITY: &str = match BUILD_IDENTITY {
    Some(identity) if !identity.is_empty() => identity,
    _ => "unidentified",
};

/// Begins the line in which `flow-connector-vmm boundary verify` reports its
/// own build, as `BUILD_IDENTITY` is, or `unidentified`. Must match its
/// `REPORTED_BUILD`.
const REPORTED_BUILD: &str = "flow-connector-vmm build ";

/// Room for the uv cache, the virtual environment and TMPDIR of a Python
/// connector's project. The file is sparse, so the host pays for what the guest writes.
const DEFAULT_DISK_MIB: u64 = 2048;
/// Container memory above guest RAM: the VMM's own allocations, its virtiofs
/// servers, guest page tables, and page cache charged to the container.
const DEFAULT_MEMORY_OVERHEAD_MIB: u32 = 256;

const MIB: u64 = 1 << 20;

impl Vmm {
    /// Read `CONNECTOR_VMM_IMAGE`; unset or empty reads nothing else.
    /// Malformed settings fail construction. Memory sizing errors are held
    /// for VMM admission.
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
        let allow_unpaired_build = match setting(ALLOW_UNPAIRED_BUILD)?.as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            Some(raw) => anyhow::bail!("{ALLOW_UNPAIRED_BUILD} {raw:?}: expected true or false"),
        };

        // Shared with ordinary connectors, so read as their launch reads them:
        // a set value is used as it is, empty or not.
        let memory_limit = var("CONNECTOR_MEMORY_LIMIT")?
            .unwrap_or_else(|| crate::container::DEFAULT_MEMORY_LIMIT.to_string());
        let cpu_limit = var("CONNECTOR_CPU_LIMIT")?
            .unwrap_or_else(|| crate::container::DEFAULT_CPU_LIMIT.to_string());
        let cgroup_parent = var("CONNECTOR_CGROUP_PARENT")?;

        // Ordinary connectors run under any limit their engine takes, so one
        // which cannot size a VMM refuses VMM starts, and not this service.
        let memory = memory_bytes(&memory_limit)
            .with_context(|| format!("CONNECTOR_MEMORY_LIMIT {memory_limit:?}"))
            .and_then(|bytes| {
                let guest_mib = guest_memory_mib(bytes, overhead_mib).with_context(|| {
                    format!(
                        "CONNECTOR_MEMORY_LIMIT {memory_limit:?}, less {overhead_mib} MiB of VMM overhead"
                    )
                })?;
                Ok(Memory { bytes, guest_mib })
            })
            .map_err(|err| format!("{err:#}"));
        let vcpus =
            vcpus(&cpu_limit).with_context(|| format!("CONNECTOR_CPU_LIMIT {cpu_limit:?}"))?;

        Ok(Some(Self {
            image,
            podman,
            state_dir,
            disk_mib,
            memory,
            cpu_limit,
            vcpus,
            cgroup_parent,
            allow_unpaired_build,
        }))
    }

    /// Recover what dead launches left beneath this configuration's
    /// `STATE_DIR`, on `runtime`, until the returned `Recovery` is dropped:
    /// one pass at once, then another 30 seconds after each ends. Its owner
    /// must outlive the shard runtimes it recovers.
    pub fn spawn_recovery(&self, runtime: &tokio::runtime::Handle) -> Recovery {
        recovery::spawn(self.clone(), runtime)
    }

    /// One recovery pass, now and to its end, as a `Recovery` runs them.
    /// Returns how many problems it left.
    pub async fn recover(&self) -> u64 {
        recovery::pass(self, &Default::default()).await
    }

    /// The metrics of this process's recovery passes.
    pub fn recovery_metrics() -> RecoveryMetrics {
        recovery::metrics()
    }

    /// Wait at most `timeout` for every VMM launch of this process to end,
    /// teardown included, returning whether they all did. For a process about
    /// to exit, whose runtime would otherwise drop them where they are.
    pub async fn drain_launches(timeout: std::time::Duration) -> bool {
        launch::drain(timeout).await
    }
}

/// Bounding guest RAM to u32 MiB also keeps the container cap within
/// podman's signed 64-bit `--memory` range.
fn guest_memory_mib(bytes: u64, overhead_mib: u32) -> anyhow::Result<u32> {
    if bytes == 0 {
        anyhow::bail!(
            "is no limit at all to the engine, and a VMM's guest RAM is sized within one"
        );
    }
    let guest = (bytes / MIB)
        .checked_sub(overhead_mib as u64)
        .filter(|guest| *guest > 0)
        .context("leaves no guest RAM")?;
    u32::try_from(guest)
        .map_err(|_| anyhow::anyhow!("leaves {guest} MiB of guest RAM, above {}", u32::MAX))
}

/// Match decimal `RAMInBytes` inputs from docker/go-units v0.5.0 so the
/// container cap and guest sizing agree. It splits at a space or after the
/// last digit or dot, multiplies a float64 by the binary unit, and truncates.
/// Hexadecimal floats are unsupported. Counts at least 2^63 are rejected
/// because the engines convert them to a negative limit, read as unlimited.
fn memory_bytes(limit: &str) -> anyhow::Result<u64> {
    const EXPECTED: &str = "a decimal number as Go's ParseFloat reads one, then optionally a \
                            unit: b, or k, m, g, t or p with an optional b or ib, in any case";

    let (number, unit) = match limit.split_once(' ') {
        Some(split) => split,
        None => {
            let Some(last) = limit.rfind(|c: char| c.is_ascii_digit() || c == '.') else {
                anyhow::bail!("expected {EXPECTED}");
            };
            limit.split_at(last + 1)
        }
    };
    let Some(number) = go_decimal(number) else {
        anyhow::bail!("expected {EXPECTED}");
    };
    if number < 0.0 {
        anyhow::bail!("is negative");
    }

    let power = match unit.to_ascii_lowercase().as_bytes() {
        [] | [b'b'] => 0,
        [prefix, rest @ ..] if matches!(rest, [] | [b'b'] | [b'i', b'b']) => match prefix {
            b'k' => 1,
            b'm' => 2,
            b'g' => 3,
            b't' => 4,
            b'p' => 5,
            _ => anyhow::bail!("expected {EXPECTED}"),
        },
        _ => anyhow::bail!("expected {EXPECTED}"),
    };

    let bytes = number * (1u64 << (10 * power)) as f64;
    // Infinity and NaN too, which ParseFloat reads from "inf" and "nan".
    if !(bytes < 9223372036854775808.0) {
        anyhow::bail!("is at least 2^63 bytes, which the engines wrap to no limit at all");
    }
    Ok(bytes as u64)
}

/// `number` as Go's `ParseFloat` reads a decimal float. Rust's float grammar
/// is Go's but for Go's digit separators, each of which stands between two
/// digits, and its hexadecimal form. Both round correctly.
fn go_decimal(number: &str) -> Option<f64> {
    let bytes = number.as_bytes();
    let digit = |at: Option<usize>| {
        at.and_then(|at| bytes.get(at))
            .is_some_and(u8::is_ascii_digit)
    };
    for (at, byte) in bytes.iter().enumerate() {
        if *byte == b'_' && !(digit(at.checked_sub(1)) && digit(Some(at + 1))) {
            return None;
        }
    }
    number.replace('_', "").parse().ok()
}

/// Whether a VMM image whose `boundary verify` printed `verified` is paired
/// with a launcher built as `launcher` (`BUILD_IDENTITY`): both built from the
/// same commit, neither from a modified tree. This catches a launcher and
/// image built from different sources by accident; it does not prove them
/// compatible. The error says why they are not paired.
pub(crate) fn check_pairing(launcher: Option<&str>, verified: &[u8]) -> Result<(), String> {
    let launcher = match launcher.and_then(source) {
        None => {
            return Err(format!(
                "this launcher's build has no source identity ({launcher:?})"
            ));
        }
        Some((commit, true)) => {
            return Err(format!(
                "this launcher was built from a tree modified from commit {commit}"
            ));
        }
        Some((commit, false)) => commit,
    };
    let verified = String::from_utf8_lossy(verified);
    let reported: Vec<&str> = verified
        .lines()
        .filter_map(|line| line.strip_prefix(REPORTED_BUILD))
        .collect();
    let image = match reported[..] {
        [] => return Err("the image reports no build identity".to_string()),
        [image] => image,
        _ => return Err(format!("the image reports builds {reported:?}")),
    };

    match source(image) {
        None => Err(format!(
            "the image's build has no source identity ({image:?})"
        )),
        Some((commit, true)) => Err(format!(
            "the image was built from a tree modified from commit {commit}"
        )),
        Some((commit, false)) if commit != launcher => Err(format!(
            "the image was built from commit {commit}, and this launcher from commit {launcher}"
        )),
        Some(_) => Ok(()),
    }
}

/// A build identity's commit, and whether its tree was modified.
fn source(identity: &str) -> Option<(&str, bool)> {
    let (commit, dirty) = match identity.strip_suffix("-dirty") {
        Some(commit) => (commit, true),
        None => (identity, false),
    };
    let hex = commit.len() == 40
        && commit
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    hex.then_some((commit, dirty))
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
/// image repository, and the hosts which any egress it declares, unless
/// empty, permits it.
#[derive(Debug)]
pub(crate) struct Eligible {
    pub task_type: ops::TaskType,
    pub repository: &'static str,
    pub egress_hosts: &'static [&'static str],
}

/// Python's connectors install their projects' dependencies from the package
/// index as they start.
const PYPI: &[&str] = &["pypi.org", "files.pythonhosted.org"];

const ELIGIBLE: &[Eligible] = &[
    Eligible {
        task_type: ops::TaskType::Capture,
        repository: "ghcr.io/estuary/capture-python",
        egress_hosts: PYPI,
    },
    Eligible {
        task_type: ops::TaskType::Derivation,
        repository: "ghcr.io/estuary/derive-python",
        egress_hosts: PYPI,
    },
    Eligible {
        task_type: ops::TaskType::Materialization,
        repository: "ghcr.io/estuary/materialize-python",
        egress_hosts: PYPI,
    },
];

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
        egress: TaskEgress,
    },
}

/// The egress a task declares, validated and normalized.
#[derive(Debug)]
pub(crate) struct TaskEgress {
    pub hosts: Vec<egress::AllowedName>,
    pub cidrs: Vec<ipnetwork::Ipv4Network>,
}

/// Decide whether a connector start may proceed, and how, given the
/// `execution` it requests, and the VMM capability `vmm` and data plane
/// `plane` of this service. `image` is the connector's image, or None if its
/// endpoint isn't an image. A request which embeds the task's built spec
/// passes that spec's execution as `spec_execution`, which must agree with
/// `execution`, so that a caller cannot drop the task's settings.
///
/// Every connector start is decided here, before any image is pulled or any
/// connector is started. A request for VMM execution is either launched in a
/// VMM or fails: it never falls back to an ordinary container. A VMM request
/// must declare egress, and is refused by a service whose memory limit cannot
/// size a VMM. Declared egress is refused by any execution which
/// cannot enforce it, its hosts and CIDRs are validated here, and CIDRs are
/// refused on a public plane, so that a direct caller cannot skip any of
/// these checks.
pub(crate) fn vmm_for<'a>(
    vmm: Option<&'a Vmm>,
    plane: crate::Plane,
    task_type: ops::TaskType,
    image: Option<&str>,
    execution: &flow::ConnectorExecution,
    spec_execution: Option<&flow::ConnectorExecution>,
) -> anyhow::Result<Execution<'a>> {
    check_spec_execution(execution, spec_execution)?;

    if execution.egress.is_some() && !execution.vmm {
        return Err(refuse(tonic::Status::invalid_argument(
            "the task declares egress, which ordinary execution cannot enforce; \
             egress is enforced only by VMM execution",
        )));
    }
    if !execution.vmm {
        return Ok(Execution::Ordinary);
    }
    let Some(image) = image else {
        return Err(refuse(tonic::Status::invalid_argument(
            "VMM execution requires an image connector",
        )));
    };
    let Some(eligible) = eligible(task_type, image) else {
        return Err(refuse(tonic::Status::invalid_argument(format!(
            "connector image '{image}' is not eligible for VMM execution as a {}",
            task_type.as_str_name()
        ))));
    };
    let Some(flow::connector_execution::Egress { hosts, cidrs }) = &execution.egress else {
        return Err(refuse(tonic::Status::invalid_argument(
            "the task requests VMM execution but declares no egress; a VMM task must \
             declare `egress`, and `egress: {}` permits no external destination",
        )));
    };
    let Some(vmm) = vmm else {
        return Err(refuse(tonic::Status::failed_precondition(
            "this data plane does not support VMM execution",
        )));
    };
    if let Err(err) = &vmm.memory {
        return Err(refuse(tonic::Status::failed_precondition(format!(
            "this data plane cannot size a VMM: {err}"
        ))));
    }
    let invalid = |err: anyhow::Error| refuse(tonic::Status::invalid_argument(format!("{err:#}")));
    let egress = TaskEgress {
        hosts: egress::hosts("task egress.hosts", hosts).map_err(invalid)?,
        cidrs: crate::policy::egress_cidrs(plane, cidrs).map_err(invalid)?,
    };

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
        return Err(refuse(tonic::Status::invalid_argument(format!(
            "Start.execution {execution:?} differs from the execution {spec_execution:?} of the task's built spec"
        ))));
    }
    Ok(())
}

/// Refuse the execution a request asks for, so that a client can tell the
/// refusal from a failure of its connector.
fn refuse(status: tonic::Status) -> anyhow::Error {
    proto_grpc::status_to_anyhow(proto_grpc::connector::refuse_execution(status))
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
                "an overhead consuming the whole limit, which refuses VMM starts only",
                &[BASE[0], BASE[1], "CONNECTOR_VMM_MEMORY_OVERHEAD_MIB=1024"],
            ),
            (
                "a fractional memory limit",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=1.5g"],
            ),
            (
                "a memory limit with an upper-case unit",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=1536MiB"],
            ),
            (
                "a memory limit of zero, which engines read as none",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=0"],
            ),
            (
                "a memory limit beyond 32 bits of guest MiB",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=4294967552m"],
            ),
            (
                "a memory limit with an exponent",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=1.5e3m"],
            ),
            (
                "a memory limit the engines wrap to none",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=8192p"],
            ),
            (
                "a hexadecimal memory limit, which the engines read",
                &[BASE[0], BASE[1], "CONNECTOR_MEMORY_LIMIT=0x1p30"],
            ),
            (
                "a malformed CPU limit",
                &[BASE[0], BASE[1], "CONNECTOR_CPU_LIMIT=two"],
            ),
            (
                "unpaired builds allowed",
                &[BASE[0], BASE[1], "CONNECTOR_VMM_ALLOW_UNPAIRED_BUILD=true"],
            ),
            (
                "unpaired builds refused",
                &[BASE[0], BASE[1], "CONNECTOR_VMM_ALLOW_UNPAIRED_BUILD=false"],
            ),
            (
                "a malformed allowance of unpaired builds",
                &[BASE[0], BASE[1], "CONNECTOR_VMM_ALLOW_UNPAIRED_BUILD=1"],
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

    #[test]
    fn resources() {
        fn show<T: std::fmt::Display>(result: anyhow::Result<T>) -> String {
            match result {
                Ok(value) => value.to_string(),
                Err(error) => format!("error: {error:#}"),
            }
        }
        let mut table = String::new();

        // Each accepted value is what go-units v0.5.0 and its master return;
        // rootful podman 4.9.3, which links v0.4.0, stores the common ones.
        // Each refused spelling is refused by them, or wraps past 2^63.
        table.push_str("## memory bytes\n");
        for limit in [
            "1073741824",
            "1g",
            "1G",
            "1gb",
            "1GB",
            "1gib",
            "1GiB",
            "1536m",
            "1536M",
            "1536mb",
            "1536MiB",
            "1.5g",
            "1.5GiB",
            "0.5m",
            "1.05g",
            "0.000000001g",
            "0.0000000001m",
            "1.999999999m",
            "0.3m",
            "1.1g",
            "1k",
            "1KiB",
            "1kb",
            "1t",
            "1p",
            "1b",
            "1B",
            "1.5b",
            "1.0b",
            "1.9",
            "1073741823.7",
            "00001g",
            "1.000000000000000000001g",
            "0",
            "0g",
            "0b",
            "512 m",
            "1.5 g",
            "1 kib",
            "1 b",
            "1 ",
            "1.5 ",
            "8191p",
            "8388607t",
            "4194304.25g",
            // Numbers which ParseFloat reads, and v0.4.0 did not.
            ".5g",
            "+.5g",
            "+1g",
            "1.g",
            "5.g",
            "1.",
            "0.",
            "00.5g",
            "1e3m",
            "1E+3m",
            "1.e3m",
            "5e-1g",
            "1e3",
            "1e3 m",
            "1e-400",
            "1_000m",
            "1.5_5g",
            "1e1_0",
            "+1_0",
            "-0",
            "-0g",
            // The engines wrap these to no limit.
            "8192p",
            "8388608t",
            "9223372036854775807",
            "9223372036854775807b",
            "17179869183.999999999g",
            "18446744073709551616",
            "inf b",
            "infinity b",
            "nan b",
            // ParseFloat's hexadecimal floats, which only this refuses.
            "0x1p30",
            "0x1.8p1g",
            // Refused by v0.5.0, though v0.4.0 parsed them.
            "1Gi",
            "1ki",
            "1ib",
            "1i",
            "1 i",
            // Refused by both.
            "-1g",
            "-.5g",
            "-inf b",
            "1e400",
            "512  m",
            "1g ",
            " 1g",
            "1 2",
            "1__000m",
            "_1m",
            "1_m",
            "1_.5g",
            "1._5g",
            "1_e10",
            "1e",
            "1e+",
            ".e1",
            "b5",
            "0x40000000",
            "1.5.5g",
            "1.5gg",
            "1gbb",
            "1kg",
            "1x",
            "1é",
            "g",
            "b",
            ".",
            "inf",
            "NaN",
            "",
        ] {
            table.push_str(&format!(
                "{limit:?} => {}\n",
                show(super::memory_bytes(limit))
            ));
        }

        table.push_str("\n## guest memory MiB, less 256 MiB of overhead\n");
        for limit in [
            "1g",
            "1.5g",
            "1280m",
            "1073741823",
            "0.1g",
            "257m",
            "256m",
            "0",
            "4294967551m",
            "4294967552m",
            "18446744073709551615",
        ] {
            let guest =
                super::memory_bytes(limit).and_then(|bytes| super::guest_memory_mib(bytes, 256));
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
                cidrs: Vec::new(),
            }),
        }
    }

    const CAPTURE: &str = "ghcr.io/estuary/capture-python:stable";
    const DERIVE: &str = "ghcr.io/estuary/derive-python:stable";
    const MATERIALIZE: &str = "ghcr.io/estuary/materialize-python:stable";

    /// Which images may request VMM execution as which task type: an exact
    /// repository, whatever its tag or digest, as its own type only.
    #[test]
    fn vmm_for_matrix() {
        let capable = super::fixture();
        let ordinary = execution(false, None);
        let vmm = execution(true, Some(&[]));

        let mut images: Vec<Option<String>> = Vec::new();
        for repository in [
            "ghcr.io/estuary/capture-python",
            "ghcr.io/estuary/derive-python",
            "ghcr.io/estuary/materialize-python",
        ] {
            for suffix in [
                ":stable",
                ":local",
                ":dev",
                "@sha256:0123abcd",
                ":stable@sha256:0123abcd",
                "",
            ] {
                images.push(Some(format!("{repository}{suffix}")));
            }
        }
        images.extend(
            [
                "ghcr.io/estuary/derive-typescript:stable",
                "ghcr.io/estuary/derive-python-extra:stable",
                "ghcr.io/estuary/capture-python/extra:stable",
                "GHCR.IO/estuary/derive-python:stable",
                "example.com/estuary/materialize-python:stable",
                "ghcr.io/estuary/source-hello-world:dev",
            ]
            .map(|image| Some(image.to_string())),
        );
        images.push(None);

        let task_types = [
            ops::TaskType::Capture,
            ops::TaskType::Derivation,
            ops::TaskType::Materialization,
        ];

        let mut rows = Vec::new();
        for task_type in task_types {
            for image in &images {
                for execution in [&ordinary, &vmm] {
                    let outcome = outcome(vmm_for(
                        Some(&capable),
                        crate::Plane::Private,
                        task_type,
                        image.as_deref(),
                        execution,
                        None,
                    ));
                    if !execution.vmm {
                        assert_eq!(outcome, "ordinary", "{task_type:?} {image:?}");
                        continue;
                    }
                    rows.push(format!(
                        "{task_type:<15} {image:<54} => {outcome}",
                        task_type = task_type.as_str_name(),
                        image = image.as_deref().unwrap_or("<not an image>"),
                    ));
                }
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }

    /// Every task type's own eligible image, on every plane, with and without
    /// VMM capability, as ordinary or VMM execution, with egress missing,
    /// empty, or declared. Public-plane refusal of ordinary Python is the
    /// image policy's, which `policy::Image::check` decides after this.
    #[test]
    fn vmm_for_egress() {
        let capable = super::fixture();
        let executions = [
            ("ordinary", execution(false, None)),
            ("ordinary, egress {}", execution(false, Some(&[]))),
            (
                "ordinary, egress [api]",
                execution(false, Some(&["api.acmeco.example"])),
            ),
            ("vmm, egress omitted", execution(true, None)),
            ("vmm, egress {}", execution(true, Some(&[]))),
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
        for plane in [
            crate::Plane::Public,
            crate::Plane::Private,
            crate::Plane::Local,
        ] {
            for capability in [None, Some(&capable)] {
                for (task_type, image) in [
                    (ops::TaskType::Capture, CAPTURE),
                    (ops::TaskType::Derivation, DERIVE),
                    (ops::TaskType::Materialization, MATERIALIZE),
                ] {
                    for (name, execution) in &executions {
                        let outcome = match vmm_for(
                            capability,
                            plane,
                            task_type,
                            Some(image),
                            execution,
                            None,
                        ) {
                            Ok(super::Execution::Vmm { egress, .. }) => {
                                let hosts: Vec<String> =
                                    egress.hosts.iter().map(ToString::to_string).collect();
                                format!("vmm, task hosts {hosts:?}")
                            }
                            result => outcome(result),
                        };
                        rows.push(format!(
                            "{plane:<7} {capable:<7} {task_type:<15} {name:<29} => {outcome}",
                            plane = plane.as_str_name(),
                            capable = if capability.is_some() { "capable" } else { "-" },
                            task_type = task_type.as_str_name(),
                        ));
                    }
                }
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }

    /// Egress is refused, whatever the task's connector, by an execution
    /// which cannot enforce it, and VMM execution is refused to whatever is
    /// not an image.
    #[test]
    fn vmm_for_ineligible_endpoints() {
        let capable = super::fixture();
        let mut rows = Vec::new();
        for image in [Some("ghcr.io/estuary/source-hello-world:dev"), None] {
            for (name, execution) in [
                ("ordinary, egress {}", execution(false, Some(&[]))),
                ("vmm, egress omitted", execution(true, None)),
                ("vmm, egress {}", execution(true, Some(&[]))),
            ] {
                rows.push(format!(
                    "{image:<39} {name:<19} => {}",
                    outcome(vmm_for(
                        Some(&capable),
                        crate::Plane::Private,
                        ops::TaskType::Capture,
                        image,
                        &execution,
                        None,
                    )),
                    image = image.unwrap_or("<not an image>"),
                ));
            }
        }
        insta::assert_snapshot!(rows.join("\n"), @r"
        ghcr.io/estuary/source-hello-world:dev  ordinary, egress {} => InvalidArgument: the task declares egress, which ordinary execution cannot enforce; egress is enforced only by VMM execution
        ghcr.io/estuary/source-hello-world:dev  vmm, egress omitted => InvalidArgument: connector image 'ghcr.io/estuary/source-hello-world:dev' is not eligible for VMM execution as a capture
        ghcr.io/estuary/source-hello-world:dev  vmm, egress {}      => InvalidArgument: connector image 'ghcr.io/estuary/source-hello-world:dev' is not eligible for VMM execution as a capture
        <not an image>                          ordinary, egress {} => InvalidArgument: the task declares egress, which ordinary execution cannot enforce; egress is enforced only by VMM execution
        <not an image>                          vmm, egress omitted => InvalidArgument: VMM execution requires an image connector
        <not an image>                          vmm, egress {}      => InvalidArgument: VMM execution requires an image connector
        ");
    }

    /// Only a private or local plane accepts CIDRs, and every plane accepts
    /// an empty list. CIDRs are validated and normalized whatever the plane.
    #[test]
    fn vmm_for_cidrs_by_plane() {
        let capable = super::fixture();
        let image = Some("ghcr.io/estuary/derive-python:stable");
        let declarations: [(&str, &[&str]); 6] = [
            ("[]", &[]),
            (
                "[10/8, 10.2/16, 192.168.4.7/32]",
                &["10.0.0.0/8", "10.2.0.0/16", "192.168.4.7/32"],
            ),
            ("[0/0]", &["0.0.0.0/0"]),
            ("[93.184.216/24] (public)", &["93.184.216.0/24"]),
            ("[fd00::/8]", &["fd00::/8"]),
            ("[10.1.2.3/8]", &["10.1.2.3/8"]),
        ];

        let mut rows = Vec::new();
        for plane in [
            crate::Plane::Public,
            crate::Plane::Private,
            crate::Plane::Local,
        ] {
            for (name, cidrs) in declarations {
                let execution = ConnectorExecution {
                    vmm: true,
                    egress: Some(Egress {
                        hosts: vec!["api.acmeco.example".to_string()],
                        cidrs: cidrs.iter().map(ToString::to_string).collect(),
                    }),
                };
                let outcome = match vmm_for(
                    Some(&capable),
                    plane,
                    ops::TaskType::Derivation,
                    image,
                    &execution,
                    None,
                ) {
                    Ok(super::Execution::Vmm { egress, .. }) => {
                        let cidrs: Vec<String> =
                            egress.cidrs.iter().map(ToString::to_string).collect();
                        format!("vmm, task cidrs {cidrs:?}")
                    }
                    result => outcome(result),
                };
                rows.push(format!(
                    "{plane:<7} {name:<34} => {outcome}",
                    plane = plane.as_str_name(),
                ));
            }
        }
        insta::assert_snapshot!(rows.join("\n"), @r#"
        PUBLIC  []                                 => vmm, task cidrs []
        PUBLIC  [10/8, 10.2/16, 192.168.4.7/32]    => InvalidArgument: the task declares egress.cidrs, which only private data planes accept
        PUBLIC  [0/0]                              => InvalidArgument: the task declares egress.cidrs, which only private data planes accept
        PUBLIC  [93.184.216/24] (public)           => InvalidArgument: the task declares egress.cidrs, which only private data planes accept
        PUBLIC  [fd00::/8]                         => InvalidArgument: the task declares egress.cidrs, which only private data planes accept
        PUBLIC  [10.1.2.3/8]                       => InvalidArgument: the task declares egress.cidrs, which only private data planes accept
        PRIVATE []                                 => vmm, task cidrs []
        PRIVATE [10/8, 10.2/16, 192.168.4.7/32]    => vmm, task cidrs ["10.0.0.0/8", "192.168.4.7/32"]
        PRIVATE [0/0]                              => vmm, task cidrs ["0.0.0.0/0"]
        PRIVATE [93.184.216/24] (public)           => vmm, task cidrs ["93.184.216.0/24"]
        PRIVATE [fd00::/8]                         => InvalidArgument: task egress.cidrs "fd00::/8" is IPv6; only IPv4 ranges are supported
        PRIVATE [10.1.2.3/8]                       => InvalidArgument: task egress.cidrs "10.1.2.3/8" sets bits below its prefix length; write 10.0.0.0/8, or 10.1.2.3/32 for one address
        LOCAL   []                                 => vmm, task cidrs []
        LOCAL   [10/8, 10.2/16, 192.168.4.7/32]    => vmm, task cidrs ["10.0.0.0/8", "192.168.4.7/32"]
        LOCAL   [0/0]                              => vmm, task cidrs ["0.0.0.0/0"]
        LOCAL   [93.184.216/24] (public)           => vmm, task cidrs ["93.184.216.0/24"]
        LOCAL   [fd00::/8]                         => InvalidArgument: task egress.cidrs "fd00::/8" is IPv6; only IPv4 ranges are supported
        LOCAL   [10.1.2.3/8]                       => InvalidArgument: task egress.cidrs "10.1.2.3/8" sets bits below its prefix length; write 10.0.0.0/8, or 10.1.2.3/32 for one address
        "#);
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
                let outcome = outcome(vmm_for(
                    None,
                    crate::Plane::Private,
                    derivation,
                    image,
                    start,
                    Some(spec),
                ));
                rows.push(format!("start {start_name} spec {spec_name} => {outcome}"));
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }

    /// A memory limit which cannot size a VMM refuses VMM starts at admission.
    /// Ordinary starts of the same service run as ever.
    #[test]
    fn vmm_for_refuses_an_unsizable_memory_limit() {
        let mut rows = Vec::new();
        for limit in ["0", "256m", "4294967552m", "8192p", "0x1p30", "1e3m", "1g"] {
            let vars = [
                ("CONNECTOR_VMM_IMAGE", "ghcr.io/estuary/connector-vmm:dev"),
                ("CONNECTOR_VMM_STATE_DIR", "/var/lib/flow/connector-vmm"),
                ("CONNECTOR_MEMORY_LIMIT", limit),
            ];
            let vmm = Vmm::from_vars(|name| {
                Ok(vars
                    .iter()
                    .find(|(var, _)| *var == name)
                    .map(|(_, value)| value.to_string()))
            })
            .expect("the service is constructed")
            .expect("capable");

            for execution in [execution(false, None), execution(true, Some(&[]))] {
                rows.push(format!(
                    "{limit:<12} vmm {:<5} => {}",
                    execution.vmm,
                    outcome(vmm_for(
                        Some(&vmm),
                        crate::Plane::Private,
                        ops::TaskType::Derivation,
                        Some(DERIVE),
                        &execution,
                        None,
                    ))
                ));
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }

    /// Whether a launcher and the build its VMM image's verify reports are
    /// paired: the same commit, from unmodified trees.
    #[test]
    fn pairing() {
        const A: &str = "0123456789abcdef0123456789abcdef01234567";
        const B: &str = "89abcdef0123456789abcdef0123456789abcdef";
        let a_dirty = format!("{A}-dirty");
        let reported = |identity: &str| format!("flow-connector-vmm build {identity}\n");

        let launchers: [(&str, Option<&str>); 6] = [
            ("clean A", Some(A)),
            ("modified A", Some(&a_dirty)),
            ("unidentified", None),
            ("empty", Some("")),
            ("malformed", Some("v0.6.3-45-gbfc779aa1ce")),
            (
                "upper-case A",
                Some("0123456789ABCDEF0123456789ABCDEF01234567"),
            ),
        ];
        let images: [(&str, String); 10] = [
            ("clean A", reported(A)),
            (
                "clean A, after other output",
                format!("listed\n{}", reported(A)),
            ),
            ("modified A", reported(&a_dirty)),
            ("clean B", reported(B)),
            ("unidentified", reported("unidentified")),
            ("empty", reported("")),
            ("a tag", reported("v0.6.3")),
            ("nothing, as older images", String::new()),
            ("A twice", format!("{}{}", reported(A), reported(A))),
            ("A without its newline", reported(A).trim_end().to_string()),
        ];

        let mut rows = Vec::new();
        for (launcher_name, launcher) in launchers {
            for (image_name, verified) in &images {
                let outcome = match super::check_pairing(launcher, verified.as_bytes()) {
                    Ok(()) => "paired".to_string(),
                    Err(reason) => format!("unpaired: {reason}"),
                };
                rows.push(format!(
                    "launcher {launcher_name:<12} image {image_name:<27} => {outcome}"
                ));
            }
        }
        insta::assert_snapshot!(rows.join("\n"));
    }
}
