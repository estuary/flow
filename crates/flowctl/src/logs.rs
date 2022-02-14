use crate::go_flowctl::GO_FLOWCTL;
use flow_cli_common::ExecExternal;

#[derive(clap::Args, Debug)]
#[clap(global_setting(clap::AppSettings::TrailingVarArg))]
pub struct LogsArgs {
    #[clap(flatten)]
    task: TaskSelector,
    /// All other arguments are forwarded to `flowctl journals read`
    #[clap(allow_hyphen_values = true, value_name = "flowctl journals read args")]
    other: Vec<String>,
}

impl LogsArgs {
    pub fn try_into_exec_external(self) -> anyhow::Result<ExecExternal> {
        use models::build::encode_resource_path;
        use std::fmt::Write;

        let LogsArgs { task, other } = self;
        let tenant = task.tenant_name()?;
        let mut prefix = format!("prefix=ops/{}/logs/", tenant);

        // This long sequence of conditionals should be better expressed as a match against enum
        // variants once https://github.com/clap-rs/clap/issues/2621 is resolved.
        if let Some(c) = task.capture.as_ref() {
            write!(
                &mut prefix,
                "kind=capture/name={}/",
                encode_resource_path(&[c])
            )
            .unwrap();
        } else if let Some(d) = task.derivation.as_ref() {
            write!(
                &mut prefix,
                "kind=derivation/name={}/",
                encode_resource_path(&[d])
            )
            .unwrap();
        } else if let Some(m) = task.materialization.as_ref() {
            write!(
                &mut prefix,
                "kind=materialization/name={}/",
                encode_resource_path(&[m])
            )
            .unwrap();
        } else if task.all_captures.is_some() {
            prefix.push_str("kind=capture/");
        } else if task.all_derivations.is_some() {
            prefix.push_str("kind=derivation/");
        } else if task.all_materializations.is_some() {
            prefix.push_str("kind=materialization/");
        } else if task.all.is_some() {
            // nothing to do here
        } else {
            unreachable!();
        }

        let mut args = vec![
            "journals".to_owned(),
            "read".to_owned(),
            "-l".to_owned(),
            prefix,
        ];
        args.extend(other);
        Ok(ExecExternal::from((GO_FLOWCTL, args)))
    }
}

/// Selects one or more Flow tasks within a single tenant.
#[derive(clap::Args, Debug, Default, Clone)]
#[clap(group = clap::ArgGroup::new("task-selector").multiple(false).required(true))]
pub struct TaskSelector {
    /// Read the logs of the given capture
    #[clap(long, group = "task-selector")]
    capture: Option<String>,
    /// Read the logs of the given derivation
    #[clap(long, group = "task-selector")]
    derivation: Option<String>,
    /// Read the logs of the given materialization
    #[clap(long, group = "task-selector")]
    materialization: Option<String>,

    /// Read the logs of all captures within the given tenant
    #[clap(long, group = "task-selector")]
    all_captures: Option<String>,
    /// Read the logs of all derivations within the given tenant
    #[clap(long, group = "task-selector")]
    all_derivations: Option<String>,
    /// Read the logs of all materializations within the given tenant
    #[clap(long, group = "task-selector")]
    all_materializations: Option<String>,

    /// Read the logs of all tasks within the given tenant
    #[clap(long, group = "task-selector")]
    all: Option<String>,
}

impl TaskSelector {
    pub fn tenant_name(&self) -> Result<&str, anyhow::Error> {
        for opt in &[
            &self.capture,
            &self.derivation,
            &self.materialization,
            &self.all_captures,
            &self.all_derivations,
            &self.all_materializations,
            &self.all,
        ] {
            if let Some(n) = opt.as_ref() {
                return Ok(tenant(n.as_str()));
            }
        }
        Err(anyhow::anyhow!("missing required task selector argument"))
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
                capture: Some(String::from("acmeCo/test/capture")),
                ..Default::default()
            },
            "prefix=ops/acmeCo/logs/kind=capture/name=acmeCo%2Ftest%2Fcapture/",
        );
        assert_logs_command(
            TaskSelector {
                derivation: Some(String::from("acmeCo/test/derivation")),
                ..Default::default()
            },
            "prefix=ops/acmeCo/logs/kind=derivation/name=acmeCo%2Ftest%2Fderivation/",
        );
        assert_logs_command(
            TaskSelector {
                materialization: Some(String::from("acmeCo/test/materialization")),
                ..Default::default()
            },
            "prefix=ops/acmeCo/logs/kind=materialization/name=acmeCo%2Ftest%2Fmaterialization/",
        );
        assert_logs_command(
            TaskSelector {
                all_captures: Some(String::from("acmeCo")),
                ..Default::default()
            },
            "prefix=ops/acmeCo/logs/kind=capture/",
        );
        assert_logs_command(
            TaskSelector {
                all_derivations: Some(String::from("acmeCo")),
                ..Default::default()
            },
            "prefix=ops/acmeCo/logs/kind=derivation/",
        );
        assert_logs_command(
            TaskSelector {
                all_materializations: Some(String::from("acmeCo")),
                ..Default::default()
            },
            "prefix=ops/acmeCo/logs/kind=materialization/",
        );
        assert_logs_command(
            TaskSelector {
                all: Some(String::from("acmeCo")),
                ..Default::default()
            },
            "prefix=ops/acmeCo/logs/",
        );
    }

    fn assert_logs_command(selector: TaskSelector, expected_label_selector: &str) {
        let args = LogsArgs {
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
                "-l",
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
