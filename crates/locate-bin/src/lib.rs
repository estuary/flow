use anyhow::Context;
use std::path::{Path, PathBuf};

/// Locate the absolute path of `binary`.
///
/// The binary is first sought alongside the currently-running program,
/// and otherwise is resolved from the `$PATH`.
pub fn locate(binary: &str) -> anyhow::Result<PathBuf> {
    // Look for the binary alongside this program.
    let this_program = std::env::args().next().unwrap();

    locate_inner(Path::new(&this_program), binary, |binary| {
        Ok(which::which(binary)?)
    })
}

// `locate_inner` factors the resolution policy out of the ambient process
// environment (the running program's path and the `$PATH` lookup) so that it
// can be exercised deterministically by unit tests.
fn locate_inner(
    this_program: &Path,
    binary: &str,
    on_path: impl FnOnce(&str) -> anyhow::Result<PathBuf>,
) -> anyhow::Result<PathBuf> {
    tracing::debug!(this_program = %this_program.display(), "attempting to find '{binary}'");
    let mut bin = this_program.parent().unwrap().join(binary);

    // If `bin` doesn't resolve to a file, then fall back to the $PATH.
    // Note a sibling *directory* named like `binary` must not shadow the lookup.
    if !bin.is_file() {
        bin = on_path(binary).with_context(|| {
            format!(
                "failed to locate '{binary}' alongside '{}' or on the $PATH",
                this_program.display()
            )
        })?;
    } else {
        bin = bin.canonicalize().unwrap();
    }
    tracing::debug!(executable = %bin.display(), "resolved {binary}");
    Ok(bin)
}

#[cfg(test)]
mod test {
    use super::locate_inner;
    use std::path::PathBuf;

    // Stands in for an empty `$PATH`.
    fn not_on_path(binary: &str) -> anyhow::Result<PathBuf> {
        anyhow::bail!("'{binary}' is not on the $PATH")
    }

    #[test]
    fn resolves_a_sibling_file() {
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl-go");
        let sibling = dir.path().join("sops");
        std::fs::write(&sibling, b"#!/bin/sh\ntrue\n").unwrap();

        // The sibling file is resolved (and canonicalized) without ever
        // consulting the $PATH.
        let located = locate_inner(&this_program, "sops", not_on_path).unwrap();
        assert_eq!(located, sibling.canonicalize().unwrap());
    }

    #[test]
    fn sibling_directory_does_not_shadow_the_path() {
        // Regression: a sibling *directory* sharing the binary's name (e.g. a
        // `go/` source tree next to the program) must not be returned in place
        // of the actual `go` executable found on the $PATH.
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl-go");
        std::fs::create_dir(dir.path().join("go")).unwrap();

        let from_path = PathBuf::from("/usr/local/bin/go");
        let located = locate_inner(&this_program, "go", |_| Ok(from_path.clone())).unwrap();
        assert_eq!(located, from_path);
    }

    #[test]
    fn missing_everywhere_is_a_contextual_error() {
        let dir = tempfile::tempdir().unwrap();
        let this_program = dir.path().join("flowctl-go");

        let err = locate_inner(&this_program, "nonesuch", not_on_path).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failed to locate 'nonesuch'") && msg.contains("$PATH"),
            "unexpected error: {msg}",
        );
    }
}
