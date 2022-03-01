use super::{BuildsRootError, BuildsRootService};
use async_trait::async_trait;
use std::path::{Path, PathBuf};

// A `BuildsRootService` that stores all builds in a local directory.
#[derive(Debug)]
pub struct LocalBuildsRoot {
    dir: PathBuf,
}

impl LocalBuildsRoot {
    pub fn new(dir: impl AsRef<Path>) -> LocalBuildsRoot {
        LocalBuildsRoot {
            dir: dir.as_ref().to_owned(),
        }
    }
}

#[async_trait]
impl BuildsRootService for LocalBuildsRoot {
    async fn put_build(&self, build_id: &str, build: &Path) -> Result<(), BuildsRootError> {
        use std::io;

        let dest_path = self.dir.join(build_id);
        if dest_path.exists() {
            Err(BuildsRootError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!("the build file: '{}' already exists", dest_path.display()),
            )))
        } else {
            tokio::fs::copy(build, &dest_path).await?;
            Ok(())
        }
    }

    async fn retrieve_build(&self, build_id: &str) -> Result<PathBuf, BuildsRootError> {
        use std::io;

        let dest_path = self.dir.join(build_id);
        if dest_path.exists() {
            Ok(dest_path)
        } else {
            Err(BuildsRootError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "no such build exists within the root",
            )))
        }
    }
}
