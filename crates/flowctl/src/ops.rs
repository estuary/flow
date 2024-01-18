use serde_json::Value;

use crate::collection::{
    read::{read_collection, ReadArgs, ReadBounds},
    CollectionJournalSelector,
};

#[derive(clap::Args, Debug)]
pub struct Logs {
    #[clap(flatten)]
    pub task: TaskSelector,

    #[clap(flatten)]
    pub bounds: ReadBounds,
}

impl Logs {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let uncommitted = true; // logs reads are always 'uncommitted' because logs aren't written inside transactions.
        let read_args = read_args(
            &self.task.task,
            OpsCollection::Logs,
            &self.bounds,
            uncommitted,
        );
        read_collection(ctx, &read_args).await?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OpsCollection {
    Logs,
}

pub fn read_args(
    task_name: &str,
    collection: OpsCollection,
    bounds: &ReadBounds,
    uncommitted: bool,
) -> ReadArgs {
    let logs_or_stats = match collection {
        OpsCollection::Logs => "logs",
    };
    // Once we implement federated data planes, we'll need to update this to
    // fetch the name of the data plane based on the tenant.
    let collection = format!("ops.us-central1.v1/{logs_or_stats}");

    let mut include = std::collections::BTreeMap::new();
    include.insert(
        "name".to_string(),
        vec![Value::String(task_name.to_string())],
    );
    let selector = CollectionJournalSelector {
        collection,
        partitions: Some(models::PartitionSelector {
            include,
            exclude: Default::default(),
        }),
        ..Default::default()
    };
    ReadArgs {
        selector,
        uncommitted,
        bounds: bounds.clone(),
        auth_prefixes: vec![task_name.to_string()],
    }
}

/// Selects one or more Flow tasks within a single tenant.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct TaskSelector {
    /// The name of the task
    #[clap(long)]
    pub task: String,
    // Selects all tasks with the given type
    //
    // Requires the `--tenant <tenant>` argument
    //#[clap(long, arg_enum, requires("tenant"))]
    //pub task_type: Option<TaskType>,

    // Selects all tasks within the given tenant
    //
    // The `--task-type` may also be specified to further limit the selection to only tasks of the given
    // type.
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

#[cfg(test)]
mod test {
    // use super::*;

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
