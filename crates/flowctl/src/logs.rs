use crate::go_flowctl::GO_FLOWCTL;
use flow_cli_common::ExecExternal;
use models::build::encode_resource_path;

#[derive(clap::Args, Debug)]
#[clap(trailing_var_arg = true)]
pub struct Args {
    #[clap(flatten)]
    pub task: TaskSelector,
    /// All other arguments are forwarded to `flowctl journals read`
    ///
    /// See `flowctl journal read --help` for a list of additional arguments.
    #[clap(allow_hyphen_values = true, value_name = "flowctl journals read args")]
    pub other: Vec<String>,
}

impl Args {
    pub fn try_into_exec_external(self) -> anyhow::Result<ExecExternal> {
        use std::fmt::Write;
        let Args { task, other } = self;

        let tenant = task.tenant_name()?;

        let mut selector = String::new();

        write!(&mut selector, "{}=ops/{}/logs", labels::COLLECTION, tenant).unwrap();

        // Select the proper partition for a specific task, if given
        if let Some(task_name) = task.task.as_deref() {
            let encoded_name = encode_resource_path(&[task_name]);
            write!(
                &mut selector,
                ",{}{}={}",
                labels::FIELD_PREFIX,
                "name",
                encoded_name
            )
            .unwrap();
        }

        // Select the proper partition for a specific type of task, if given
        if let Some(task_type) = task.task_type {
            write!(
                &mut selector,
                ",{}{}={}",
                labels::FIELD_PREFIX,
                "kind",
                task_type.label_value(),
            )
            .unwrap();
        }

        let mut args = vec![
            "journals".to_owned(),
            "read".to_owned(),
            "--selector".to_owned(),
            selector,
        ];
        args.extend(other);
        Ok(ExecExternal::from((GO_FLOWCTL, args)))
    }
}

/// Selects one or more Flow tasks within a single tenant.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct TaskSelector {
    /// Read the logs of the task with the given name
    #[clap(long, conflicts_with_all(&["task_type", "tenant"]), required_unless_present("tenant"))]
    pub task: Option<String>,

    /// Read the logs of all tasks with the given type
    ///
    /// Requires the `--tenant <tenant>` argument
    #[clap(long, arg_enum, requires("tenant"))]
    pub task_type: Option<TaskType>,

    /// Read the logs of tasks within the given tenant
    ///
    /// The `--task-type` may also be specified to limit the selection to only tasks of the given
    /// type. Without a `--task-type`, it will return all logs from all tasks in the tenant.
    #[clap(long)]
    pub tenant: Option<String>,
}

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

fn tenant(task_name: &str) -> &str {
    match task_name.split_once('/') {
        Some((first, _)) => first,
        None => task_name,
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}
