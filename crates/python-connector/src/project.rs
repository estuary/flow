use anyhow::Context;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Lockfile of a project. A project which doesn't list its own has one
/// resolved by Validate, which is then baked into the built specification.
pub const LOCK_FILE: &str = "uv.lock";
/// Manifest of a project's dependencies.
pub const PYPROJECT: &str = "pyproject.toml";
/// Directory of generated Python modules, which is placed on the PYTHONPATH.
pub const GENERATED_PREFIX: &str = "flow_generated/python";

/// A Python project staged into a temporary directory.
pub struct Project {
    dir: tempfile::TempDir,
    // Lock of the project's dependencies, once they're installed.
    lock: Option<String>,
}

/// How a project's dependencies are installed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Install {
    /// Resolve a fresh lock of the project's dependencies under its own
    /// configuration, and install it, with the `dev` dependency group if `dev`.
    /// As Validate does (with `dev`) for a project which doesn't list a lock.
    Resolve { dev: bool },
    /// Check that the project's own lock is current with its configuration,
    /// and install it with the `dev` dependency group.
    /// As Validate does for a project which lists a lock.
    Check,
    /// Install exactly the project's lock, without the `dev` group,
    /// as a running task does with its baked lock.
    Frozen,
}

impl Install {
    /// Install mode of a request: Validate resolves or checks the project's
    /// lock and runs its `dev` tooling, while other requests install their
    /// lock as-is, without the `dev` group. A request other than Validate
    /// lacks a lock if it's not of a built task (as with a Discover), or is
    /// of a legacy derivation whose built spec predates baked locks, and then
    /// resolves one.
    pub fn of_request(is_validate: bool, has_lock: bool) -> Self {
        match (is_validate, has_lock) {
            (is_validate, false) => Self::Resolve { dev: is_validate },
            (true, true) => Self::Check,
            (false, true) => Self::Frozen,
        }
    }
}

impl Project {
    /// Stage `files` at their paths within a new temporary project.
    /// Each path must be relative to the project, which validation enforces.
    pub fn stage<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> anyhow::Result<Self> {
        let dir = tempfile::TempDir::new().context("creating temporary project directory")?;
        let project = Self { dir, lock: None };

        for (path, content) in files {
            project.write(path, content)?;
        }
        Ok(project)
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Absolute path of the project's generated modules.
    pub fn generated_dir(&self) -> PathBuf {
        self.root().join(GENERATED_PREFIX)
    }

    /// Write `content` at `path`, relative to the project root.
    pub fn write(&self, path: &str, content: &str) -> anyhow::Result<()> {
        let target = self.root().join(path);

        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory of file {path}"))?;
        }
        std::fs::write(&target, content).with_context(|| format!("failed to write file {path}"))
    }

    pub fn exists(&self, path: &str) -> bool {
        self.root().join(path).exists()
    }

    /// Lock of the project's installed dependencies, if they've been installed.
    pub fn lock(&self) -> Option<&str> {
        self.lock.as_deref()
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
        let lock = std::fs::read_to_string(self.root().join(LOCK_FILE))
            .context("reading the project's lock")?;
        self.lock = Some(lock);

        Ok(())
    }

    /// Is `tool` installed within the project's environment,
    /// such as by the project's `dev` dependency group?
    pub fn has_tool(&self, tool: &str) -> bool {
        self.root().join(".venv/bin").join(tool).exists()
    }

    /// Build a Command which runs `args` within the project's environment,
    /// without re-syncing it, and with its generated modules on the PYTHONPATH.
    pub fn command(&self, args: &[&str]) -> std::process::Command {
        let mut command = std::process::Command::new("uv");
        command
            .current_dir(self.root())
            .env("PYTHONPATH", self.generated_dir())
            .args(["run", "--no-sync"])
            .args(args);
        command
    }

    /// Run `args` within the project's environment, returning its stdout,
    /// or an error having its output with paths relative to the project.
    pub fn run(&self, args: &[&str], stdin: Option<&[u8]>) -> anyhow::Result<Vec<u8>> {
        use std::io::Write;

        let mut child = self
            .command(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to start {args:?}"))?;

        let mut child_stdin = child.stdin.take().unwrap();
        if let Some(stdin) = stdin {
            child_stdin.write_all(stdin)?;
        }
        std::mem::drop(child_stdin);

        let output = child.wait_with_output()?;

        if !output.status.success() {
            anyhow::bail!(
                "{}{}",
                self.relative(&String::from_utf8_lossy(&output.stdout)),
                self.relative(&String::from_utf8_lossy(&output.stderr)),
            );
        }
        Ok(output.stdout)
    }

    /// Rewrite absolute paths of the project to be relative to it, which
    /// mirrors the layout of the user's own project.
    pub fn relative(&self, text: &str) -> String {
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

/// Rewrite absolute paths (or URLs) of `root` within `text` to be relative to it.
pub fn relative_to(text: &str, root: &Path) -> String {
    let url = url::Url::from_directory_path(root).expect("project root is absolute");
    let path = format!("{}/", root.display());

    // The URL goes first, because it contains the path.
    text.replace(url.as_str(), "").replace(&path, "")
}

/// Default `pyproject.toml` of a project which doesn't have its own.
/// `dependencies` are added to the default dependency on pydantic.
///
/// Its `exclude-newer` is a supply-chain safeguard of the garden path:
/// packages uploaded within the past week are not candidates for resolution,
/// as malicious uploads are typically yanked within days. A project with its
/// own `pyproject.toml` decides its own cooldown.
pub fn default_pyproject(name: &str, dependencies: &BTreeMap<String, String>) -> String {
    let mut dependencies = dependencies.clone();
    dependencies
        .entry("pydantic".to_string())
        .or_insert_with(|| ">=2".to_string());

    let dependencies: String = dependencies
        .iter()
        .map(|(package, version)| format!("    \"{package}{version}\",\n"))
        .collect();

    format!(
        r#"[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.14,<4"
dependencies = [
{dependencies}]

[dependency-groups]
dev = ["pyright>=1.1"]

[tool.uv]
package = false
exclude-newer = "7 days"

[tool.pyright]
typeCheckingMode = "strict"
extraPaths = ["{GENERATED_PREFIX}"]
"#
    )
}

#[cfg(test)]
mod test {
    use super::{default_pyproject, relative_to};
    use std::collections::BTreeMap;

    #[test]
    fn stderr_paths_are_relative_to_the_project() {
        let stderr = concat!(
            "/tmp/.tmpABC/lib/geo.py:4:5 - error: \"x\" is not defined\n",
            "  File \"file:///tmp/.tmpABC/module.py\", line 12\n",
            "/tmp/.tmpABC/flow_generated/python/acmeCo/orders/__init__.py:9:1 - error\n",
        );
        insta::assert_snapshot!(relative_to(stderr, std::path::Path::new("/tmp/.tmpABC")));
    }

    #[test]
    fn install_modes_of_requests() {
        use super::Install;
        assert_eq!(
            [
                Install::of_request(true, false),
                Install::of_request(true, true),
                Install::of_request(false, true),
                // A Discover, or the Open of a legacy derivation.
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

    #[test]
    fn default_pyproject_includes_dependencies() {
        let dependencies = BTreeMap::from([("httpx".to_string(), ">=0.27".to_string())]);
        insta::assert_snapshot!(default_pyproject("acmeCo-orders", &dependencies));
    }
}
