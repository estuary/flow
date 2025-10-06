use super::logs;
use anyhow::Context;
use tokio::io::AsyncReadExt;

/// Enumerate the arguments of a command, starting with the program name.
pub fn args<'c>(cmd: &'c async_process::Command) -> impl Iterator<Item = &'c std::ffi::OsStr> + 'c {
    std::iter::once(cmd.get_program()).chain(cmd.get_args())
}

/// Check if a command arguments (including program name) start with the given prefix.
pub fn starts_with(cmd: &async_process::Command, prefix: &[&str]) -> bool {
    args(cmd).zip(prefix.iter()).all(|(a, b)| a == *b)
}

#[derive(Debug)]
pub struct NonZeroExit {
    pub status: String,
    pub cmd: String,
    pub logs_token: sqlx::types::Uuid,
}

impl std::fmt::Display for NonZeroExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "command {} exited with status {:?} (logs token {})",
            self.cmd, self.status, self.logs_token
        )
    }
}

/// run a command, optionally capturing its stdout, and otherwise logging
/// to the `logs_tx` sink under `stream` and `logs_token`.
pub async fn run(
    mut cmd: async_process::Command,
    capture_stdout: bool,
    stream: &'static str,
    logs_tx: logs::Tx,
    logs_token: sqlx::types::Uuid,
) -> anyhow::Result<Vec<u8>> {
    cmd.stdin(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());

    let args: Vec<_> = args(&cmd).map(|s| s.to_os_string()).collect();
    tracing::info!(?args, cwd=?cmd.get_current_dir(), "starting command");

    logs_tx
        .send(logs::Line {
            token: logs_token,
            stream: "controller".to_string(),
            line: format!("Starting {stream}: {args:?}"),
        })
        .await
        .context("failed to send to logs sink")?;

    let mut child: async_process::Child = cmd.spawn()?.into();
    let mut captured = Vec::new();

    let mut stdout = child.stdout.take().unwrap();
    let stdout = async {
        if capture_stdout {
            stdout.read_to_end(&mut captured).await.map(|_n| ())
        } else {
            logs::capture_lines(&logs_tx, format!("{stream}:0"), logs_token, stdout).await
        }
    };

    let stderr = child.stderr.take().unwrap();
    let stderr = logs::capture_lines(&logs_tx, format!("{stream}:1"), logs_token, stderr);

    let ((), (), status) = futures::try_join!(stdout, stderr, child.wait())?;
    tracing::info!(?args, %status, "command completed");

    logs_tx
        .send(logs::Line {
            token: logs_token,
            stream: "controller".to_string(),
            line: format!("Completed {stream} ({status}): {args:?}"),
        })
        .await
        .context("failed to send to logs sink")?;

    if !status.success() {
        Err(anyhow::anyhow!(NonZeroExit {
            status: format!("{status}"),
            cmd: format!("{cmd:?}"),
            logs_token,
        }))
    } else {
        Ok(captured)
    }
}

pub async fn dry_run(
    cmd: async_process::Command,
    capture_stdout: bool,
    stream: &'static str,
    logs_tx: logs::Tx,
    logs_token: sqlx::types::Uuid,
) -> anyhow::Result<Vec<u8>> {
    // Commands which are safe to actually run in dry-run mode.
    for prefix in [
        ["git"].as_slice(),
        &["python3.12", "-m", "venv"],
        &["poetry", "install"],
        &["./venv/bin/ansible-galaxy", "install"],
    ] {
        if starts_with(&cmd, prefix) {
            return run(cmd, capture_stdout, stream, logs_tx, logs_token).await;
        }
    }

    let args: Vec<_> = args(&cmd).map(|s| s.to_os_string()).collect();
    tracing::info!(?args, cwd=?cmd.get_current_dir(), "dry-run of command");

    logs_tx
        .send(logs::Line {
            token: logs_token,
            stream: "controller".to_string(),
            line: format!("Skipping command (dry-run) {stream}: {args:?}"),
        })
        .await
        .context("failed to send to logs sink")?;

    // If we're initializing a stack, the controller expects the encrypted key
    // of the stack YAML to be updated by the Pulumi command.
    if starts_with(&cmd, &["pulumi", "stack", "init"]) {
        write_stack_init_fixture(&cmd, "dry-run-fixture")?;
        return Ok(Vec::new());
    }

    // Return a fixture which models a typical Pulumi stack output.
    if starts_with(&cmd, &["pulumi", "stack", "output"]) {
        return Ok(include_bytes!("dry_run_fixture.json").to_vec());
    }

    // Return a fixture which models a typical Pulumi stack history output with a change.
    if starts_with(&cmd, &["pulumi", "stack", "history"]) {
        return Ok(serde_json::to_vec(&[crate::stack::PulumiStackHistory {
            resource_changes: crate::stack::PulumiStackResourceChanges {
                create: 1,
                delete: 0,
                same: 0,
                update: 0,
            },
        }])
        .unwrap());
    }

    // All other commands "succeed" with no output.
    Ok(Vec::new())
}

pub fn write_stack_init_fixture(
    cmd: &async_process::Command,
    encrypted_key: &str,
) -> anyhow::Result<()> {
    let stack_path = cmd.get_current_dir().unwrap().join(format!(
        "Pulumi.{}.yaml",
        args(cmd).nth(3).unwrap().to_str().unwrap()
    ));
    let mut stack: crate::stack::PulumiStack =
        serde_yaml::from_slice(&std::fs::read(&stack_path).context("failed to read stack YAML")?)
            .context("failed to parse stack from YAML")?;

    stack.encrypted_key = encrypted_key.to_string();

    std::fs::write(&stack_path, serde_yaml::to_string(&stack).unwrap())
        .context("failed to write stack YAML")?;

    Ok(())
}
