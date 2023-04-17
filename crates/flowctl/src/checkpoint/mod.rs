use std::{process::Command, io::BufRead};
use tempfile::tempdir;

use serde::Deserialize;
use serde_json::value::RawValue;
use anyhow::{anyhow, Context};

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct PullCheckpoint {
    /// Name of the task to pull checkpoint for
    #[clap(long)]
    task: String,
}

#[derive(Deserialize, Clone)]
pub struct ShardSpec {
    id: String
}

#[derive(Deserialize)]
pub struct ShardResponse {
    spec: ShardSpec
}

#[derive(Deserialize)]
pub struct ShardsListResponse {
    shards: Vec<ShardResponse>
}

#[derive(Deserialize)]
pub struct Checkpoint {
    #[serde(rename="DriverCheckpoint")]
    driver_checkpoint: Box<RawValue>
}

pub fn shard_for_task(task: &str) -> anyhow::Result<Option<ShardSpec>> {
    let output = Command::new("flowctl-go")
        .args([
            "shards",
            "list",
            "-l", &format!("estuary.dev/task-name={task}"),
            "-o", "json"
        ])
        .output().context("running flowctl shards list")?;

    let shards_list_response: ShardsListResponse = serde_json::from_slice(&output.stdout).context("parsing shards list response")?;

    Ok(shards_list_response.shards.first().map(|shard| shard.spec.clone()))
}

// Go over lines of gazette state file, and find the last DriverCheckpoint and return it
pub fn find_checkpoint_in_state<T: BufRead>(state: T) -> anyhow::Result<Option<Box<RawValue>>> {
    state.lines().try_fold(None, |acc, line| {
        Ok(match serde_json::from_str::<Checkpoint>(&line?) {
            Ok(Checkpoint { driver_checkpoint }) => Some(driver_checkpoint),
            Err(_) => None
        }.or(acc))
    })
}

// Play the recovery log of a shard until the last log, which will include multiple state messages
// In order to find the driver checkpoint, you can use `find_checkpoint_in_state` to find the
// checkpoint message among all the messages in the last log
pub async fn pull_checkpoint_inner(shard: String) -> anyhow::Result<std::fs::File> {
    let dir = tempdir().context("creating temp directory")?.into_path();
    let dir_str = dir.clone().into_os_string().into_string().unwrap();
    let mut handle = Command::new("flowctl-go")
        .args([
            "shards",
            "recover",
            "--id", &shard,
            "--dir", &dir_str,
        ])
        .spawn().context("spawning gazctl shards recover")?;
    handle.wait().context("waiting for gazctl shards recover")?;

    let mut state_file_path = dir;
    state_file_path.push("state.json");
    std::fs::File::open(&state_file_path).context(format!("opening {:?} file", state_file_path))
}

pub async fn pull_checkpoint(shard: String) -> anyhow::Result<()> {
    let mut state_file = pull_checkpoint_inner(shard).await?;
    std::io::copy(&mut state_file, &mut std::io::stdout()).context("copying checkpoint to stdout")?;
    
    Ok(())
}

impl PullCheckpoint {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            task,
        } = self;

        let shard = shard_for_task(&task)?.ok_or(anyhow!("Could not find shard for task {}", task))?;

        pull_checkpoint(shard.id).await?;

        tracing::error!("all done");
        Ok(())
    }
}
