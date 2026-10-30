//! Releasing what a launch's record owns, by its live owner at teardown or by
//! whoever recovers it once its owner is dead: the same steps, under the
//! record's lock.
//!
//! The container goes first, because nothing else is safe to remove while it
//! may run: its mount and state are bound into it, and its network holds it.
//! An unknown container, from a query or removal which failed, stops the
//! release there. Host ingress permissions go next; a failed revoke keeps the
//! network and record for retry. Then the network, rootfs volume, mount and state
//! are each attempted whatever became of the others. The record goes last, once
//! everything it names is gone; otherwise a later release can finish the job.
//!
//! A podman service may still create a resource after its release has removed
//! the record. So `recover` also lists what carries its state directory's
//! label, and removes each resource whose record is gone.

use super::record::{self, Record};
use anyhow::Context;
use std::time::Duration;

/// Bounds each command, so that a wedged engine can't hold a release forever.
pub(crate) const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// How far a release has got with one kind of resource.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Progress {
    /// Not yet looked for (podman's) or not yet removed (directories).
    Pending,
    /// Found: container IDs, or network names, still to remove.
    Found(Vec<String>),
    /// Proven absent, or removed.
    Done,
    /// Finding or removing it failed, with this error.
    Failed(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Release {
    pub containers: Progress,
    pub ingress: Progress,
    pub networks: Progress,
    pub volumes: Progress,
    pub mount: Progress,
    pub state: Progress,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Action {
    FindContainers,
    RemoveContainers(Vec<String>),
    RemoveIngress,
    FindNetworks,
    RemoveNetworks(Vec<String>),
    FindVolumes,
    RemoveVolumes(Vec<String>),
    RemoveMount,
    RemoveState,
    RemoveRecord,
    /// Stop, keeping the record for a later release.
    Keep,
}

impl Release {
    /// A release of `record`, which owns only the directories it marks.
    pub(crate) fn new(record: &Record) -> Self {
        let dir = |created| {
            if created {
                Progress::Pending
            } else {
                Progress::Done
            }
        };
        Self {
            containers: Progress::Pending,
            ingress: if record.ingress_image.is_some() {
                Progress::Pending
            } else {
                Progress::Done
            },
            networks: Progress::Pending,
            volumes: Progress::Pending,
            mount: dir(record.created_mount),
            state: dir(record.created_state),
        }
    }

    pub(crate) fn next(&self) -> Action {
        match &self.containers {
            Progress::Pending => return Action::FindContainers,
            Progress::Found(ids) => return Action::RemoveContainers(ids.clone()),
            Progress::Failed(_) => return Action::Keep,
            Progress::Done => (),
        }
        match &self.ingress {
            Progress::Pending => return Action::RemoveIngress,
            Progress::Done => (),
            Progress::Failed(_) => return Action::Keep,
            Progress::Found(_) => unreachable!("ingress is removed by owner token"),
        }
        match &self.networks {
            Progress::Pending => return Action::FindNetworks,
            Progress::Found(names) => return Action::RemoveNetworks(names.clone()),
            Progress::Failed(_) | Progress::Done => (),
        }
        match &self.volumes {
            Progress::Pending => return Action::FindVolumes,
            Progress::Found(names) => return Action::RemoveVolumes(names.clone()),
            Progress::Failed(_) | Progress::Done => (),
        }
        if self.mount == Progress::Pending {
            return Action::RemoveMount;
        }
        if self.state == Progress::Pending {
            return Action::RemoveState;
        }
        if [&self.networks, &self.volumes, &self.mount, &self.state]
            .iter()
            .all(|progress| **progress == Progress::Done)
        {
            Action::RemoveRecord
        } else {
            Action::Keep
        }
    }

    /// What remains, as resource and error, for a release which kept its
    /// record.
    pub(crate) fn failures(&self, record: &Record) -> Vec<(String, String)> {
        [
            (format!("container {}", record.name), &self.containers),
            (format!("ingress {}", record.token), &self.ingress),
            (format!("network {}", record.name), &self.networks),
            (format!("rootfs volume {}", record.name), &self.volumes),
            (record.mount.clone(), &self.mount),
            (record.state.clone(), &self.state),
        ]
        .into_iter()
        .filter_map(|(resource, progress)| match progress {
            Progress::Failed(error) => Some((resource, error.clone())),
            _ => None,
        })
        .collect()
    }
}

/// Release what `record`, at `path` in `state_dir` and held by the caller,
/// owns with `podman`. Ok once the record itself is removed; otherwise the
/// failures of what remains.
pub(crate) async fn release(
    podman: &str,
    state_dir: &str,
    path: &str,
    record: &Record,
) -> Result<(), Vec<(String, String)>> {
    let label = format!("label={}={}", record::OWNER_LABEL, record.token);
    let mut release = Release::new(record);

    loop {
        match release.next() {
            Action::FindContainers => {
                release.containers = found(
                    command(
                        podman,
                        &[
                            "ps",
                            "--all",
                            "--no-trunc",
                            "--filter",
                            &label,
                            "--format",
                            "{{.ID}}",
                        ],
                    )
                    .await,
                );
            }
            Action::RemoveContainers(ids) => {
                let mut args = vec!["rm", "--force", "--time=0", "--ignore"];
                args.extend(ids.iter().map(String::as_str));
                release.containers = removed(command(podman, &args).await.map(|_| ()));
            }
            Action::RemoveIngress => {
                let image = record
                    .ingress_image
                    .as_deref()
                    .expect("ingress has a VMM image");
                let mut args = super::plan::host_boundary(image, "revoke", &[]);
                args.push(format!("--owner={}", record.token));
                release.ingress = removed(
                    command(podman, &args.iter().map(String::as_str).collect::<Vec<_>>())
                        .await
                        .map(|_| ()),
                );
            }
            Action::FindNetworks => {
                release.networks = found(
                    command(
                        podman,
                        &["network", "ls", "--filter", &label, "--format", "{{.Name}}"],
                    )
                    .await,
                );
            }
            Action::RemoveNetworks(names) => {
                // By name: podman 4.9 skips its in-use check when given a
                // network's ID, and removes it from under its containers.
                let mut result = Ok(());
                for name in &names {
                    if let Err(err) = command(podman, &["network", "rm", name]).await {
                        result = Err(err);
                        break;
                    }
                }
                release.networks = removed(result);
            }
            Action::FindVolumes => {
                release.volumes = found(
                    command(
                        podman,
                        &["volume", "ls", "--filter", &label, "--format", "{{.Name}}"],
                    )
                    .await,
                );
            }
            Action::RemoveVolumes(names) => {
                // No force: a volume still used by any container must stay.
                let mut args = vec!["volume", "rm"];
                args.extend(names.iter().map(String::as_str));
                release.volumes = removed(command(podman, &args).await.map(|_| ()));
            }
            Action::RemoveMount => release.mount = removed(remove_dir(&record.mount)),
            Action::RemoveState => release.state = removed(remove_dir(&record.state)),
            Action::RemoveRecord => {
                return record::remove(state_dir, path)
                    .map_err(|err| vec![(path.to_string(), format!("removing it: {err}"))]);
            }
            Action::Keep => return Err(release.failures(record)),
        }
    }
}

/// Release every record in `state_dir` whose owner is dead, then remove what
/// carries `state_dir`'s label but has no record. What this fails to release
/// is reported to this process's log alone: it isn't the launching session's,
/// and may not be its task's. If `state_dir` can't be read, nothing is removed.
pub(crate) async fn recover(podman: &str, state_dir: &str) {
    let entries = match std::fs::read_dir(state_dir) {
        Ok(entries) => entries,
        Err(err) => {
            tracing::error!(%state_dir, error = %err, "failed to read VMM ownership records");
            return;
        }
    };

    for entry in entries {
        let file_name = match entry {
            Ok(entry) => entry.file_name(),
            Err(err) => {
                tracing::error!(%state_dir, error = %err, "failed to read VMM ownership records");
                return;
            }
        };
        let Some(name) = file_name.to_str().and_then(record::launch_name) else {
            continue;
        };
        let path = record::path(state_dir, name);

        let (_held, content) = match record::take(&path) {
            Ok(Some(taken)) => taken,
            Ok(None) => continue,
            Err(err) => {
                tracing::error!(record = %path, error = %err, "failed to open a VMM ownership record");
                continue;
            }
        };
        let record = match record::parse(state_dir, name, &content) {
            Ok(record) => record,
            Err(err) => {
                tracing::error!(
                    record = %path,
                    error = format!("{err:#}"),
                    "a VMM ownership record is malformed, so nothing it names is released",
                );
                continue;
            }
        };

        match release(podman, state_dir, &path, &record).await {
            Ok(()) => tracing::info!(record = %path, "released the resources of a dead VMM launch"),
            Err(failures) => {
                for (resource, error) in failures {
                    tracing::error!(
                        record = %path,
                        %resource,
                        %error,
                        "failed to release a resource of a dead VMM launch; its record is kept",
                    );
                }
            }
        }
    }

    remove_unrecorded(podman, state_dir).await;
}

/// Remove each container, network and volume carrying `state_dir`'s label
/// whose record is gone. A launch claims its record before its first create.
/// So each kind is listed before its records are looked for, and a live
/// launch's resource is then always found with its record. Containers go
/// first, because their network and volume can't be removed while they exist.
/// A failure is logged, and the next launch's recovery retries it.
async fn remove_unrecorded(podman: &str, state_dir: &str) {
    let label = format!("label={}={state_dir}", record::STATE_DIR_LABEL);
    let kinds: [(&str, &[&str], &str, &[&str]); 3] = [
        (
            "container",
            &["ps", "--all"],
            "{{.Names}}",
            &["rm", "--force", "--time=0", "--ignore"],
        ),
        (
            "network",
            &["network", "ls"],
            "{{.Name}}",
            &["network", "rm"],
        ),
        ("volume", &["volume", "ls"], "{{.Name}}", &["volume", "rm"]),
    ];

    for (kind, list, format, remove) in kinds {
        let mut args = list.to_vec();
        args.extend(["--filter", &label, "--format", format]);
        let listed = match command(podman, &args).await {
            Ok(stdout) => lines(&stdout),
            Err(err) => {
                tracing::error!(
                    %state_dir,
                    kind,
                    error = format!("{err:#}"),
                    "failed to list VMM resources by their state directory",
                );
                continue;
            }
        };

        let (unrecorded, kept) = unrecorded(state_dir, listed);
        for (resource, why) in kept {
            tracing::error!(
                %state_dir,
                kind,
                %resource,
                %why,
                "a VMM resource has no record that can be looked for, so it is kept",
            );
        }
        for resource in unrecorded {
            let mut args = remove.to_vec();
            args.push(&resource);
            let removed = match command(podman, &args).await {
                Ok(_stdout) => Ok(()),
                // A concurrent release may have removed it first.
                Err(err) => match exists(podman, kind, &resource).await {
                    Ok(false) => Ok(()),
                    Ok(true) | Err(_) => Err(err),
                },
            };
            match removed {
                Ok(()) => tracing::info!(
                    %state_dir,
                    kind,
                    %resource,
                    "removed a VMM resource whose record is gone",
                ),
                Err(err) => tracing::error!(
                    %state_dir,
                    kind,
                    %resource,
                    error = format!("{err:#}"),
                    "failed to remove a VMM resource whose record is gone",
                ),
            }
        }
    }
}

/// Of `listed`, which carry `state_dir`'s label, those whose launch has no
/// record there, and those kept with why: any not named for a launch, and any
/// whose record can't be looked for. A record proves the launch is not
/// released, whatever it holds, so it isn't read.
fn unrecorded(state_dir: &str, listed: Vec<String>) -> (Vec<String>, Vec<(String, String)>) {
    let (mut unrecorded, mut kept) = (Vec::new(), Vec::new());
    for resource in listed {
        let Some(name) = record::resource_launch(&resource) else {
            kept.push((resource, "is not named for a VMM launch".to_string()));
            continue;
        };
        let path = record::path(state_dir, name);
        match std::fs::symlink_metadata(&path) {
            Ok(_) => (),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => unrecorded.push(resource),
            Err(err) => kept.push((resource, format!("looking for {path}: {err}"))),
        }
    }
    (unrecorded, kept)
}

/// Whether podman's `<kind> exists` finds `name`, where `kind` is container,
/// network or volume: its status is 0 if so, and 1 if not.
async fn exists(podman: &str, kind: &str, name: &str) -> anyhow::Result<bool> {
    let args = [kind, "exists", name];
    let output = tokio::time::timeout(
        COMMAND_TIMEOUT,
        async_process::output(crate::container::engine_command(podman).args(args)),
    )
    .await
    .map_err(|_elapsed| {
        anyhow::anyhow!("{podman} command {args:?} did not finish within {COMMAND_TIMEOUT:?}")
    })?
    .with_context(|| format!("failed to run {podman} command {args:?}"))?;

    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => anyhow::bail!(
            "{podman} command {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

fn found(result: anyhow::Result<Vec<u8>>) -> Progress {
    match result {
        Ok(stdout) => {
            let found = lines(&stdout);
            if found.is_empty() {
                Progress::Done
            } else {
                Progress::Found(found)
            }
        }
        Err(err) => Progress::Failed(format!("{err:#}")),
    }
}

fn lines(stdout: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn removed(result: anyhow::Result<()>) -> Progress {
    match result {
        Ok(()) => Progress::Done,
        Err(err) => Progress::Failed(format!("{err:#}")),
    }
}

fn remove_dir(path: &str) -> anyhow::Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow::Error::new(err).context(format!("removing {path}"))),
    }
}

pub(crate) async fn command(podman: &str, args: &[&str]) -> anyhow::Result<Vec<u8>> {
    match tokio::time::timeout(COMMAND_TIMEOUT, crate::container::engine_cmd(podman, args)).await {
        Ok(result) => result,
        Err(_elapsed) => Err(anyhow::anyhow!(
            "{podman} command {args:?} did not finish within {COMMAND_TIMEOUT:?}"
        )),
    }
}

#[cfg(test)]
mod test {
    use super::{Action, Progress, Release};

    #[test]
    fn ingress_cleanup_precedes_network_release() {
        let record = super::record::parse(
            "/state",
            "fv_0123456789abcdef",
            &super::record::claim_line(
                "fv_0123456789abcdef",
                "00112233445566778899aabbccddeeff",
                "/tmp/connector-mounts-1000/mount-fv_0123456789abcdef",
                Some("vmm:local"),
            ),
        )
        .unwrap();
        let mut release = Release::new(&record);
        assert_eq!(release.next(), Action::FindContainers);
        release.containers = Progress::Done;
        assert_eq!(release.next(), Action::RemoveIngress);
        release.ingress = Progress::Failed("revoke failed".into());
        assert_eq!(release.next(), Action::Keep);
        assert_eq!(release.failures(&record).len(), 1);
        release.ingress = Progress::Done;
        assert_eq!(release.next(), Action::FindNetworks);
    }

    #[test]
    fn releases() {
        use Outcome::*;

        let cases: &[(&str, (bool, bool), &[Outcome])] = &[
            (
                "nothing was made",
                (false, false),
                &[FindsNothing, FindsNothing, FindsNothing],
            ),
            (
                "only the mount was made",
                (true, false),
                &[FindsNothing, FindsNothing, FindsNothing, Removes],
            ),
            (
                "everything was made",
                (true, true),
                &[
                    Finds, Removes, Finds, Removes, Finds, Removes, Removes, Removes,
                ],
            ),
            (
                "everything, already gone",
                (true, true),
                &[FindsNothing, FindsNothing, FindsNothing, Removes, Removes],
            ),
            ("the container query fails", (true, true), &[Fails]),
            ("the container removal fails", (true, true), &[Finds, Fails]),
            (
                "the network query fails",
                (true, true),
                &[FindsNothing, Fails, FindsNothing, Removes, Removes],
            ),
            (
                "the network is in use",
                (true, true),
                &[FindsNothing, Finds, Fails, FindsNothing, Removes, Removes],
            ),
            (
                "the rootfs query fails",
                (true, true),
                &[FindsNothing, FindsNothing, Fails, Removes, Removes],
            ),
            (
                "the rootfs is in use",
                (true, true),
                &[FindsNothing, FindsNothing, Finds, Fails, Removes, Removes],
            ),
            (
                "the mount removal fails",
                (true, true),
                &[FindsNothing, FindsNothing, FindsNothing, Fails, Removes],
            ),
            (
                "the state removal fails",
                (true, true),
                &[FindsNothing, FindsNothing, FindsNothing, Removes, Fails],
            ),
            (
                "every removal fails",
                (true, true),
                &[Finds, Removes, Finds, Fails, Finds, Fails, Fails, Fails],
            ),
        ];

        let mut table = String::new();
        for (label, (created_mount, created_state), outcomes) in cases {
            let record = super::record::Record {
                name: "fv_0123456789abcdef".to_string(),
                token: "00112233445566778899aabbccddeeff".to_string(),
                mount: "/tmp/connector-mounts-1000/mount-fv_0123456789abcdef".to_string(),
                state: "/var/lib/flow/connector-vmm/fv_0123456789abcdef".to_string(),
                created_mount: *created_mount,
                created_state: *created_state,
                ingress_image: None,
            };
            let mut release = Release::new(&record);
            let mut outcomes = outcomes.iter();
            let mut steps = Vec::new();

            let last = loop {
                let action = release.next();
                if matches!(action, Action::RemoveRecord | Action::Keep) {
                    break action;
                }
                let outcome = outcomes.next().expect("an outcome for each step");
                steps.push(format!("{action:?} -> {outcome:?}"));

                let progress = match (outcome, &action) {
                    (Finds, Action::FindContainers) => {
                        Progress::Found(vec!["0123abcd".to_string()])
                    }
                    (Finds, Action::FindNetworks | Action::FindVolumes) => {
                        Progress::Found(vec![record.name.clone()])
                    }
                    (FindsNothing | Removes, _) => Progress::Done,
                    (Fails, _) => Progress::Failed("refused".to_string()),
                    (Finds, action) => panic!("{action:?} finds nothing"),
                };
                match action {
                    Action::FindContainers | Action::RemoveContainers(_) => {
                        release.containers = progress
                    }
                    Action::FindNetworks | Action::RemoveNetworks(_) => release.networks = progress,
                    Action::FindVolumes | Action::RemoveVolumes(_) => release.volumes = progress,
                    Action::RemoveIngress => release.ingress = progress,
                    Action::RemoveMount => release.mount = progress,
                    Action::RemoveState => release.state = progress,
                    Action::RemoveRecord | Action::Keep => unreachable!(),
                }
            };
            assert!(outcomes.next().is_none(), "{label}: unused outcomes");

            table.push_str(&format!("# {label}\n"));
            for step in steps {
                table.push_str(&format!("{step}\n"));
            }
            table.push_str(&format!("{last:?}\n"));
            for (resource, error) in release.failures(&record) {
                table.push_str(&format!("  left {resource}: {error}\n"));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table);
    }

    /// Of what carries a state directory's label, only what has no record
    /// there is removed. A record of any content keeps its launch's resources.
    /// A name not of a launch, or a record which can't be looked for, keeps
    /// the resource and says why.
    #[test]
    fn unrecorded_resources() {
        let dir = tempfile::tempdir().unwrap();
        let state_dir = dir.path().to_str().unwrap();
        let record = super::record::path(state_dir, "fv_0123456789abcdef");
        std::fs::write(&record, "").unwrap();

        let listed: Vec<String> = [
            "fv_0123456789abcdef",
            "fv_0123456789abcdef-ingress",
            "fv_fedcba9876543210",
            "fv_fedcba9876543210-ingress",
            "fv_fedcba9876543210-grant",
            "fv_0123456789ABCDEF",
            "fv_0123456789abcde",
            "quirky_turing",
        ]
        .map(String::from)
        .to_vec();

        let mut table = String::new();
        for (label, at) in [
            ("a state directory", state_dir),
            ("a file in place of one", record.as_str()),
        ] {
            let (unrecorded, kept) = super::unrecorded(at, listed.clone());
            table.push_str(&format!("# {label}\n"));
            for resource in &listed {
                let outcome = if unrecorded.contains(resource) {
                    "remove".to_string()
                } else if let Some((_, why)) = kept.iter().find(|(kept, _)| kept == resource) {
                    format!("keep: {why}")
                } else {
                    "keep: its record exists".to_string()
                };
                table.push_str(&format!("{resource} => {outcome}\n"));
            }
            table.push('\n');
        }
        insta::assert_snapshot!(table.replace(state_dir, "<state>"));
    }

    /// Advance Tokio's clock explicitly so each command's deadline is checked
    /// without relying on the scheduler to observe a wall-clock boundary.
    #[tokio::test(start_paused = true)]
    async fn engine_commands_each_have_a_timeout() {
        let mut elapsed = Vec::new();
        for _ in 0..3 {
            let began = tokio::time::Instant::now();
            let command = super::command("sleep", &["86400"]);
            tokio::pin!(command);
            assert!(futures::poll!(&mut command).is_pending());
            tokio::time::advance(super::COMMAND_TIMEOUT - std::time::Duration::from_secs(1)).await;
            assert!(futures::poll!(&mut command).is_pending());
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            let error = command.await.expect_err("the engine command timed out");
            elapsed.push((began.elapsed().as_secs(), error.to_string()));
        }
        insta::assert_debug_snapshot!(elapsed);
    }

    #[derive(Debug)]
    enum Outcome {
        Finds,
        FindsNothing,
        Removes,
        Fails,
    }
}
