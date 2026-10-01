//! Releasing what a launch's record owns, by its live owner at teardown or by
//! whoever recovers it once its owner is dead: the same steps, under the
//! record's lock.
//!
//! The container goes first, because nothing else is safe to remove while it
//! may run: its mount and state are bound into it, and its network holds it.
//! An unknown container, from a query or removal which failed, stops the
//! release there. Once it's gone, the network, mount and state are each
//! attempted whatever became of the others. The record goes last, and only
//! once everything it names is gone; otherwise it stays, unchanged, so that a
//! later release, which repeats every check, can finish the job.

use super::record::{self, Record};
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
    pub networks: Progress,
    pub mount: Progress,
    pub state: Progress,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Action {
    FindContainers,
    RemoveContainers(Vec<String>),
    FindNetworks,
    RemoveNetworks(Vec<String>),
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
            networks: Progress::Pending,
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
        match &self.networks {
            Progress::Pending => return Action::FindNetworks,
            Progress::Found(names) => return Action::RemoveNetworks(names.clone()),
            Progress::Failed(_) | Progress::Done => (),
        }
        if self.mount == Progress::Pending {
            return Action::RemoveMount;
        }
        if self.state == Progress::Pending {
            return Action::RemoveState;
        }
        if [&self.networks, &self.mount, &self.state]
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
            (format!("network {}", record.name), &self.networks),
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

/// Release every record in `state_dir` whose owner is dead. What this fails
/// to release is reported to this process's log alone: it isn't the launching
/// session's, and may not be its task's.
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
}

fn found(result: anyhow::Result<Vec<u8>>) -> Progress {
    match result {
        Ok(stdout) => {
            let found: Vec<String> = String::from_utf8_lossy(&stdout)
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(ToString::to_string)
                .collect();
            if found.is_empty() {
                Progress::Done
            } else {
                Progress::Found(found)
            }
        }
        Err(err) => Progress::Failed(format!("{err:#}")),
    }
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
    fn releases() {
        use Outcome::*;

        let cases: &[(&str, (bool, bool), &[Outcome])] = &[
            (
                "nothing was made",
                (false, false),
                &[FindsNothing, FindsNothing],
            ),
            (
                "only the mount was made",
                (true, false),
                &[FindsNothing, FindsNothing, Removes],
            ),
            (
                "everything was made",
                (true, true),
                &[Finds, Removes, Finds, Removes, Removes, Removes],
            ),
            (
                "everything, already gone",
                (true, true),
                &[FindsNothing, FindsNothing, Removes, Removes],
            ),
            ("the container query fails", (true, true), &[Fails]),
            ("the container removal fails", (true, true), &[Finds, Fails]),
            (
                "the network query fails",
                (true, true),
                &[FindsNothing, Fails, Removes, Removes],
            ),
            (
                "the network is in use",
                (true, true),
                &[FindsNothing, Finds, Fails, Removes, Removes],
            ),
            (
                "the mount removal fails",
                (true, true),
                &[FindsNothing, FindsNothing, Fails, Removes],
            ),
            (
                "the state removal fails",
                (true, true),
                &[FindsNothing, FindsNothing, Removes, Fails],
            ),
            (
                "every removal fails",
                (true, true),
                &[Finds, Removes, Finds, Fails, Fails, Fails],
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
                    (Finds, Action::FindNetworks) => Progress::Found(vec![record.name.clone()]),
                    (FindsNothing | Removes, _) => Progress::Done,
                    (Fails, _) => Progress::Failed("refused".to_string()),
                    (Finds, action) => panic!("{action:?} finds nothing"),
                };
                match action {
                    Action::FindContainers | Action::RemoveContainers(_) => {
                        release.containers = progress
                    }
                    Action::FindNetworks | Action::RemoveNetworks(_) => release.networks = progress,
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

    #[derive(Debug)]
    enum Outcome {
        Finds,
        FindsNothing,
        Removes,
        Fails,
    }
}
