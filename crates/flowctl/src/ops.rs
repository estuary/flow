use crate::collection::{
    read::{read_collection, ReadArgs, ReadBounds},
    CollectionJournalSelector, Partition,
};
use crate::config::Config;

#[derive(clap::Args, Debug)]
pub struct Logs {
    #[clap(flatten)]
    pub task: TaskSelector,

    #[clap(flatten)]
    pub bounds: ReadBounds,
}

impl Logs {
    pub async fn run(&self, config: &Config) -> anyhow::Result<()> {
        let read_args = read_args(&self.task.task, "logs", &self.bounds);
        read_collection(config, &read_args).await?;
        Ok(())
    }
}

#[derive(clap::Args, Debug)]
pub struct Stats {
    #[clap(flatten)]
    pub task: TaskSelector,

    #[clap(flatten)]
    pub bounds: ReadBounds,
}

impl Stats {
    pub async fn run(&self, config: &Config) -> anyhow::Result<()> {
        let read_args = read_args(&self.task.task, "stats", &self.bounds);
        read_collection(config, &read_args).await?;
        Ok(())
    }
}

fn read_args(task_name: &str, logs_or_stats: &'static str, bounds: &ReadBounds) -> ReadArgs {
    let tenant = tenant(task_name);
    let collection = format!("ops/{tenant}/{logs_or_stats}");
    let selector = CollectionJournalSelector {
        collection,
        include_partitions: vec![Partition {
            name: "name".to_string(),
            value: task_name.to_string(),
        }],
        exclude_partitions: Vec::new(),
    };
    ReadArgs {
        selector,
        uncommitted: true,
        bounds: bounds.clone(),
    }
}

/// Selects one or more Flow tasks within a single tenant.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct TaskSelector {
    /// Read the logs of the task with the given name
    #[clap(long)]
    pub task: String,
    // Read the logs of all tasks with the given type
    //
    // Requires the `--tenant <tenant>` argument
    //#[clap(long, arg_enum, requires("tenant"))]
    //pub task_type: Option<TaskType>,

    // Read the logs of tasks within the given tenant
    //
    // The `--task-type` may also be specified to limit the selection to only tasks of the given
    // type. Without a `--task-type`, it will return all logs from all tasks in the tenant.
    //#[clap(long)]
    //pub tenant: Option<String>,
}

/*
#[derive(Debug, clap::ArgEnum, PartialEq, Eq, Clone, Copy)]
pub enum TaskType {
    Capture,
    Derivation,
    Materialization,
}

impl TaskType {
    fn label_value(&self) -> &'static str {
        match self {
            TaskType::Capture => "capture",
            TaskType::Derivation => "derivation",
            TaskType::Materialization => "materialization",
        }
    }
}

impl TaskSelector {
    fn tenant_name(&self) -> Result<&str, anyhow::Error> {
        self.tenant
            .as_deref()
            .or_else(|| self.task.as_deref().map(tenant))
            .ok_or_else(|| anyhow::anyhow!("missing required task selector argument"))
    }
}

*/

fn tenant(task_name: &str) -> &str {
    match task_name.split_once('/') {
        Some((first, _)) => first,
        None => task_name,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /*
    #[test]
    fn logs_translates_into_journals_read_commands() {
        assert_logs_command(
            TaskSelector {
                task: Some(String::from("acmeCo/test/capture")),
                ..Default::default()
            },
            "estuary.dev/collection=ops/acmeCo/logs,estuary.dev/field/name=acmeCo%2Ftest%2Fcapture",
        );
        assert_logs_command(
            TaskSelector {
                task_type: Some(TaskType::Capture),
                tenant: Some("acmeCo".to_owned()),
                task: None,
            },
            "estuary.dev/collection=ops/acmeCo/logs,estuary.dev/field/kind=capture",
        );
        assert_logs_command(
            TaskSelector {
                task_type: Some(TaskType::Derivation),
                tenant: Some("acmeCo".to_owned()),
                task: None,
            },
            "estuary.dev/collection=ops/acmeCo/logs,estuary.dev/field/kind=derivation",
        );
        assert_logs_command(
            TaskSelector {
                task_type: Some(TaskType::Materialization),
                tenant: Some("acmeCo".to_owned()),
                task: None,
            },
            "estuary.dev/collection=ops/acmeCo/logs,estuary.dev/field/kind=materialization",
        );
        assert_logs_command(
            TaskSelector {
                tenant: Some(String::from("acmeCo")),
                ..Default::default()
            },
            "estuary.dev/collection=ops/acmeCo/logs",
        );
    }

    fn assert_logs_command(selector: TaskSelector, expected_label_selector: &str) {
        let args = Args {
            task: selector.clone(),
            // Any extra arguments should be appended to whatever is generated
            other: vec![String::from("an extra arg")],
        };
        let cmd = args
            .try_into_exec_external()
            .expect("failed to convert args");
        let expected = ExecExternal::from((
            GO_FLOWCTL,
            vec![
                "journals",
                "read",
                "--selector",
                expected_label_selector,
                "an extra arg",
            ],
        ));
        assert_eq!(
            expected, cmd,
            "expected selector: {:?} to return journal selector: '{}', but got: {:?}",
            selector, expected_label_selector, cmd
        );
    }
    */
}
