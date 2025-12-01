use anyhow::Context;
use std::path::PathBuf;
use which::which;

pub fn locate(binary: &str) -> anyhow::Result<PathBuf> {
    // Look for binary alongside this program.
    let this_program = std::env::args().next().unwrap();

    tracing::debug!(%this_program, "attempting to find '{binary}'");
    let mut bin = std::path::Path::new(&this_program)
        .parent()
        .unwrap()
        .join(binary);

    // Fall back to the $PATH.
    if !bin.exists() {
        bin = which(binary).with_context(|| {
            format!("failed to locate '{binary}' alongside '{this_program}' or on the $PATH")
        })?;
    } else {
        bin = bin.canonicalize().unwrap();
    }
    tracing::debug!(executable = %bin.display(), "resolved {binary}");
    Ok(bin)
}
