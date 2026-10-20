use anyhow::Context;
use models::custom_connector::Language;
use std::io::Write;
use std::path::Path;

/// Generated entry point of a project, which its connector runs. It's within
/// the project's generated files, so that the user's own paths are their own.
pub const ENTRY: &str = "flow_generated/main.ts";

/// Directory of generated TypeScript modules, which a project's `deno.json`
/// maps as `flow/`.
pub const GENERATED_PREFIX: &str = "flow_generated/typescript";

pub const DENO_MISSING: &str = "The Deno runtime is a prerequisite for TypeScript but could not be found. Please install Deno from https://deno.com";

// Use the project's lock as-is, failing if it doesn't pin every import.
const FROZEN: &str = "--frozen";

/// A Deno project staged into a temporary directory.
pub struct Project {
    dir: tempfile::TempDir,
    // Does every `deno` invocation use the project's lock as-is?
    frozen: bool,
}

impl Project {
    /// Stage `files` at their paths within a new temporary project.
    /// Each path must be relative to the project, which validation enforces.
    ///
    /// A project which has a lock (listed or baked) is `frozen`, and is run
    /// under exactly that lock. Otherwise Deno resolves a lock as it runs,
    /// unless the project has no remote imports.
    pub fn stage<'a>(
        files: impl IntoIterator<Item = (&'a str, &'a str)>,
        frozen: bool,
    ) -> anyhow::Result<Self> {
        let dir = tempfile::TempDir::new().context("creating temporary project directory")?;
        let project = Self { dir, frozen };

        for (path, content) in files {
            let target = project.root().join(path);

            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create directory of file {path}"))?;
            }
            std::fs::write(&target, content)
                .with_context(|| format!("failed to write file {path}"))?;
        }
        Ok(project)
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Check the project as Validate does. A frozen project's lock must pin
    /// every import. Its errors name `entry`, the task's own module, which
    /// the user's own `deno install` resolves from. Then, the project must
    /// type-check from the generated entry point.
    pub fn check(&self, entry: &str) -> anyhow::Result<()> {
        if self.frozen {
            let output = self
                .deno()
                .args(["install", FROZEN, "--entrypoint", ENTRY])
                .output()
                .context(DENO_MISSING)?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    self.relative(&String::from_utf8_lossy(&output.stderr))
                ))
                .context(format!(
                    "the project's {} is out of date with its imports: run `deno install --entrypoint {entry}` to update it",
                    Language::Typescript.lock(),
                ));
            }
        }

        let output = self
            .deno()
            .arg("check")
            .args(self.frozen.then_some(FROZEN))
            .arg(ENTRY)
            .output()
            .context(DENO_MISSING)?;

        if !output.status.success() {
            anyhow::bail!(self.relative(&String::from_utf8_lossy(&output.stderr)));
        }
        Ok(())
    }

    /// Build a Command which runs the project's entry point with `args`,
    /// under Deno `permissions`. A connector gives every run the same
    /// permissions, so that its modules load alike for each request.
    pub fn command(&self, permissions: &[&str], args: &[&str]) -> std::process::Command {
        let mut command = self.deno();
        command
            .arg("run")
            .args(permissions)
            .args(self.frozen.then_some(FROZEN))
            .arg(ENTRY)
            .args(args);
        command
    }

    /// Run the project's entry point (as `command`) with `stdin`, returning
    /// its stdout, or an error having its stderr with paths relative to the
    /// project.
    pub fn run(
        &self,
        permissions: &[&str],
        args: &[&str],
        stdin: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        let mut child = self
            .command(permissions, args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context(DENO_MISSING)?;

        let mut child_stdin = child.stdin.take().unwrap();
        child_stdin.write_all(stdin)?;
        std::mem::drop(child_stdin);

        let output = child.wait_with_output()?;

        if !output.status.success() {
            anyhow::bail!(self.relative(&String::from_utf8_lossy(&output.stderr)));
        }
        Ok(output.stdout)
    }

    /// The project's lock, which Deno resolved as it ran if the project
    /// doesn't list one. There's none if the project has no remote imports.
    pub fn lock(&self) -> anyhow::Result<Option<String>> {
        match std::fs::read_to_string(self.root().join(Language::Typescript.lock())) {
            Ok(lock) => Ok(Some(lock)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).context("failed to read the project's lock"),
        }
    }

    /// Rewrite absolute paths of the project to be relative to it, which
    /// mirrors the layout of the user's own project.
    pub fn relative(&self, text: &str) -> String {
        models::custom_connector::relative_to(text, self.root())
    }

    // A `deno` command run within the project, under its `deno.json`.
    fn deno(&self) -> std::process::Command {
        let mut command = std::process::Command::new("deno");
        command.current_dir(self.root());
        command
    }
}
