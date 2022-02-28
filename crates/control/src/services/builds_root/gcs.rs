use super::{BuildsRootError, BuildsRootService};
use crate::services::subprocess::Subprocess;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use tokio::process::Command;

#[derive(Debug)]
pub struct GCSBuildsRoot {
    root: url::Url,
    temp_dir: TempDir,
}

impl GCSBuildsRoot {
    pub fn new(uri: url::Url) -> Result<GCSBuildsRoot, anyhow::Error> {
        anyhow::ensure!(!uri.cannot_be_a_base(), "uri cannot be a base");
        anyhow::ensure!(uri.path().ends_with('/'), "uri must end with a '/'");

        // TODO: either assert that the temp_dir is empty, or scan it an pre-populate local_builds
        // based on its contents.

        let temp_dir = TempDir::new()?;
        Ok(GCSBuildsRoot {
            root: uri,
            temp_dir,
        })
    }
}

#[async_trait]
impl BuildsRootService for GCSBuildsRoot {
    async fn put_build(&self, build_id: &str, build: &Path) -> Result<(), BuildsRootError> {
        let dest_url = self.root.join(build_id)?;
        Command::new("gsutil")
            .arg("cp")
            .arg("-n") // -n causes gsutil to fail if the file already exists
            .arg(build.display().to_string())
            .arg(dest_url.to_string())
            .execute()
            .await?;
        Ok(())
    }

    async fn retrieve_build(&self, build_id: &str) -> Result<PathBuf, BuildsRootError> {
        let dest_file = self.temp_dir.path().join(build_id);
        let src_key = self.root.join(build_id)?;

        // If we've previously attempted to retrieve this build and failed part way through, then
        // a file with this name could already exist. We don't use the `-n` flag here, and rely on
        // gsutil to overwrite the destination file in that case.
        Command::new("gsutil")
            .arg("cp")
            .arg(src_key.to_string())
            .arg(dest_file.display().to_string())
            .execute()
            .await?;

        Ok(dest_file)
    }
}
