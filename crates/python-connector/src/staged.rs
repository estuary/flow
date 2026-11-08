use crate::{GENERATED_PREFIX, Project};
use anyhow::Context;
use std::path::{Path, PathBuf};

/// A Python project staged into a temporary directory.
pub struct Staged {
    dir: tempfile::TempDir,
}

/// Error of a user program which exited unsuccessfully. Its stderr is
/// inherited, so its own logs report why, and a connector exits with its
/// status rather than logging an error of its own.
#[derive(Debug)]
pub struct Exited(pub std::process::ExitStatus);

impl std::fmt::Display for Exited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "user program exited with {}", self.0)
    }
}

impl std::error::Error for Exited {}

/// How a project's dependencies are installed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Install {
    /// Resolve a fresh lock of the project's dependencies under its own
    /// configuration, and install it, with the `dev` dependency group if `dev`.
    /// As every request does for a project which doesn't list a lock.
    Resolve { dev: bool },
    /// Check that the project's own lock is current with its configuration,
    /// and install it with the `dev` dependency group.
    /// As Validate does for a project which lists a lock.
    Check,
    /// Install exactly the project's lock, without the `dev` group,
    /// as a running task does with its listed lock.
    Frozen,
}

impl Install {
    /// Install mode of a request: Validate checks a listed lock, and runs the
    /// project's `dev` tooling. Other requests install a listed lock as-is,
    /// without the `dev` group. A project which doesn't list a lock resolves
    /// one for each request.
    pub fn of_request(is_validate: bool, has_lock: bool) -> Self {
        match (is_validate, has_lock) {
            (is_validate, false) => Self::Resolve { dev: is_validate },
            (true, true) => Self::Check,
            (false, true) => Self::Frozen,
        }
    }
}

impl Staged {
    /// Stage `files` at their paths within a new temporary project.
    /// Each path must be relative to the project, which validation enforces.
    fn stage<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> anyhow::Result<Self> {
        let dir = tempfile::TempDir::new().context("creating temporary project directory")?;
        let project = Self { dir };

        for (path, content) in files {
            project.write(path, content)?;
        }
        Ok(project)
    }

    /// Stage the files of `project` which exist, with its `generated` files,
    /// and `install` its dependencies.
    pub fn of_project(
        project: &Project,
        generated: &[(String, String)],
        install: Install,
    ) -> anyhow::Result<Self> {
        let mut staged = Self::stage(
            project
                .files
                .iter()
                .filter_map(|(path, content)| Some((path.as_str(), content.as_deref()?)))
                .chain(
                    generated
                        .iter()
                        .map(|(path, content)| (path.as_str(), content.as_str())),
                ),
        )?;
        staged.install(install)?;

        tracing::debug!(temp_dir = ?staged.root(), ?install, "staged Python project");
        Ok(staged)
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Absolute path of the project's generated modules.
    fn generated_dir(&self) -> PathBuf {
        self.root().join(GENERATED_PREFIX)
    }

    /// Write `content` at `path`, relative to the project root.
    fn write(&self, path: &str, content: &str) -> anyhow::Result<()> {
        let target = self.root().join(path);

        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory of file {path}"))?;
        }
        std::fs::write(&target, content).with_context(|| format!("failed to write file {path}"))
    }

    /// Install the project's dependencies into its virtual environment.
    /// `Check` and `Frozen` require that the project has a lock.
    ///
    /// No flags override the project's own `[tool.uv]` configuration (such
    /// as its `exclude-newer` cooldown), which the lock is resolved and
    /// checked against. A `sync` is always `--frozen`, because a lock is by
    /// then either fresh or checked, and `--locked` would re-resolve it.
    pub fn install(&mut self, install: Install) -> anyhow::Result<()> {
        match install {
            Install::Resolve { dev } => {
                self.uv(&["lock"])
                    .context("failed to resolve the project's dependencies")?;
                let sync: &[&str] = if dev {
                    &["sync", "--frozen"]
                } else {
                    &["sync", "--frozen", "--no-dev"]
                };
                self.uv(sync)
                    .context("failed to install the project's dependencies")?;
            }
            Install::Check => {
                self.uv(&["lock", "--check"]).context(
                    "the project's uv.lock is out of date with its pyproject.toml: run `uv lock` to update it",
                )?;
                self.uv(&["sync", "--frozen"])
                    .context("failed to install the project's locked dependencies")?;
            }
            Install::Frozen => {
                self.uv(&["sync", "--frozen", "--no-dev"])
                    .context("failed to install the project's locked dependencies")?;
            }
        }
        Ok(())
    }

    /// Check the project with its own `dev` tooling, under its own
    /// configuration: pyright type-checks it, and then pytest runs its tests.
    /// Its installed `dev` dependency group must provide both. A project
    /// having no tests passes.
    pub fn check(&self) -> anyhow::Result<()> {
        for tool in ["pyright", "pytest"] {
            if !self.root().join(".venv/bin").join(tool).exists() {
                anyhow::bail!(
                    "the project's `dev` dependency group must include `{tool}`, which checks it on each Validate"
                );
            }
        }

        let output = self
            .command(&["pyright"])
            .output()
            .context("failed to run pyright")?;

        if !output.status.success() {
            anyhow::bail!("Python type check failed:\n{}", self.output(&output));
        }

        // The header names the temporary project directory, which is noise.
        let output = self
            .command(&["pytest", "--no-header"])
            .stdin(std::process::Stdio::null())
            .output()
            .context("failed to run pytest")?;

        // pytest exits with status 5 if it collected no tests.
        if !output.status.success() && output.status.code() != Some(5) {
            anyhow::bail!("Python tests failed:\n{}", self.output(&output));
        }
        Ok(())
    }

    /// Build a Command which runs `args` within the project's environment,
    /// without re-syncing it. Its PYTHONPATH is the project root, and then its
    /// generated modules. That's the order in which pyright resolves imports:
    /// it searches its execution root, and then its `extraPaths`. A project
    /// isn't installed as a package, so the root is otherwise not importable.
    pub fn command(&self, args: &[&str]) -> std::process::Command {
        let python_path = std::env::join_paths([self.root().to_path_buf(), self.generated_dir()])
            .expect("a temporary directory has no path separator");

        let mut command = std::process::Command::new("uv");
        command
            .current_dir(self.root())
            .env("PYTHONPATH", python_path)
            .args(["run", "--no-sync"])
            .args(args);
        command
    }

    /// Run the user program `args` within the project's environment with
    /// `stdin`, returning its stdout. Its stderr is inherited.
    pub fn run(&self, args: &[&str], stdin: &[u8]) -> anyhow::Result<Vec<u8>> {
        use std::io::Write;

        let mut child = self
            .command(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to start {args:?}"))?;

        let mut child_stdin = child.stdin.take().unwrap();
        child_stdin.write_all(stdin)?;
        std::mem::drop(child_stdin);

        let output = child.wait_with_output()?;

        if !output.status.success() {
            return Err(Exited(output.status).into());
        }
        Ok(output.stdout)
    }

    /// Hand off a session to the user program `args`, which is given `open`
    /// and then the remainder of `requests`. Its stdout and stderr are inherited.
    pub fn hand_off(
        &self,
        args: &[&str],
        open: &impl serde::Serialize,
        requests: std::io::BufReader<std::io::Stdin>,
    ) -> anyhow::Result<()> {
        use std::io::Write;

        let mut open = serde_json::to_vec(open).unwrap();
        open.push(b'\n');

        let mut child = self
            .command(args)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to start {args:?}"))?;

        // The program may exit before consuming all of its requests,
        // in which case its exit status is the error to report.
        let mut child_stdin = child.stdin.take().unwrap();
        let _ = std::thread::spawn(move || {
            let _ = child_stdin.write_all(&open);
            let _ = std::io::copy(&mut requests.buffer(), &mut child_stdin);
            let _ = std::io::copy(&mut requests.into_inner(), &mut child_stdin);
        });

        let status = child.wait()?;
        if !status.success() {
            return Err(Exited(status).into());
        }
        Ok(())
    }

    /// Stdout and then stderr of a tool's `output`, relative to the project.
    fn output(&self, output: &std::process::Output) -> String {
        self.relative(&format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ))
    }

    /// Rewrite absolute paths of the project to be relative to it, which
    /// mirrors the layout of the user's own project.
    fn relative(&self, text: &str) -> String {
        relative_to(text, self.root())
    }

    fn uv(&self, args: &[&str]) -> anyhow::Result<()> {
        let output = std::process::Command::new("uv")
            .current_dir(self.root())
            .args(args)
            .output()
            .context("failed to run uv")?;

        if !output.status.success() {
            anyhow::bail!(
                "{}",
                self.relative(&String::from_utf8_lossy(&output.stderr))
            );
        }
        tracing::debug!(?args, stderr = %String::from_utf8_lossy(&output.stderr), "ran uv");

        Ok(())
    }
}

/// Rewrite absolute paths (or URLs) of a staged project's `root` within
/// `text` to be relative to it.
fn relative_to(text: &str, root: &Path) -> String {
    let url = url::Url::from_directory_path(root).expect("project root is absolute");
    let path = format!("{}/", root.display());

    // The URL goes first, because it contains the path.
    text.replace(url.as_str(), "").replace(&path, "")
}

#[cfg(test)]
mod test {
    #[test]
    fn output_paths_are_relative_to_the_project() {
        let output = concat!(
            "/tmp/.tmpABC/lib/geo.py:4:5 - error: \"x\" is not defined\n",
            "  File \"file:///tmp/.tmpABC/module.py\", line 12\n",
            "/tmp/.tmpABC/flow_generated/python/acmeCo/orders/__init__.py:9:1 - error\n",
            "/tmp/.tmpABCD/other.py:1:1 - error\n",
        );
        insta::assert_snapshot!(super::relative_to(output, std::path::Path::new("/tmp/.tmpABC")), @r#"
        lib/geo.py:4:5 - error: "x" is not defined
          File "module.py", line 12
        flow_generated/python/acmeCo/orders/__init__.py:9:1 - error
        /tmp/.tmpABCD/other.py:1:1 - error
        "#);
    }

    #[test]
    fn install_modes_of_requests() {
        use super::Install;
        assert_eq!(
            [
                Install::of_request(true, false),
                Install::of_request(true, true),
                Install::of_request(false, true),
                // Any other request of a project which doesn't list a lock.
                Install::of_request(false, false),
            ],
            [
                Install::Resolve { dev: true },
                Install::Check,
                Install::Frozen,
                Install::Resolve { dev: false },
            ]
        );
    }
}
