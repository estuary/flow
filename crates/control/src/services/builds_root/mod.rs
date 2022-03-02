use crate::config;
use crate::error::SubprocessError;
use crate::models::Id;
use async_trait::async_trait;
use rusqlite::{Connection, OpenFlags};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

mod gcs;
mod local;

/// Allows uploading builds to the builds root.
#[derive(Debug, Clone)]
pub struct PutBuilds(Arc<dyn BuildsRootService>);

impl PutBuilds {
    /// Uploads the given `build_db` to the builds root so that it is accessible to the data plane.
    async fn put_build(&self, build_id: Id, build_db: &Path) -> Result<(), BuildsRootError> {
        self.0.put_build(build_id, build_db).await
    }
}

/// A sqlite connection to a build database that has been fetched from the builds root.
/// This type implements `Deref<Target=rusqlite::Connection>`, so you can use it just like you
/// would a normal sqlite connection. This is an inert wrapper type currently, and exists to allow
/// for caching in the future.
#[derive(Debug)]
pub struct BuildDBRef {
    connection: Connection,
}

impl std::ops::Deref for BuildDBRef {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

/// Allows querying build databases by build id. Builds will be fetched automatically from the
/// builds root, if required.
#[derive(Debug, Clone)]
pub struct FetchBuilds {
    root: Arc<dyn BuildsRootService>,
    local_builds: Arc<Mutex<BTreeMap<Id, LocalBuildState>>>,
}

impl FetchBuilds {
    fn new(root: Arc<dyn BuildsRootService>) -> FetchBuilds {
        FetchBuilds {
            root,
            local_builds: Default::default(),
        }
    }

    #[tracing::instrument(level = "debug")]
    pub async fn get_build(&self, build_id: Id) -> Result<BuildDBRef, BuildsRootError> {
        let state_lock = self.get_state(build_id).await;
        let mut build_state = state_lock.0.lock().await;

        // The initial state will be a `LastError` with 0 attempts.
        // Every time we actually attempt to fetch the build, the `attempts` counter will be
        // incremented, which is used to apply an incremental backoff to attempts to dowload a
        // build from cloud storage. The assumption being that requests for a given `build_id` may
        // be pretty "bursty", and so repeated requests for a build that doesn't exist could
        // otherwise run afoul of rate limits.
        if let Some(prior_attempts) = build_state
            .as_ref()
            .err()
            .filter(|e| e.should_fetch())
            .map(|e| e.attempts)
        {
            let s = match self.root.retrieve_build(build_id).await {
                Ok(build_path) => Ok(LocalBuildDB::new(build_path)),
                Err(err) => {
                    tracing::error!(error = ?err, build_id = %build_id, prior_attempts, "failed to fetch build");
                    Err(LastError {
                        attempts: prior_attempts + 1,
                        last_attempt: Instant::now(),
                        error: err.to_string(),
                    })
                }
            };
            let _ = std::mem::replace(&mut *build_state, s);
        }

        match *build_state {
            Ok(LocalBuildDB { ref path }) => {
                let connection =
                    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
                Ok(BuildDBRef { connection })
            }
            Err(LastError { ref error, .. }) => Err(BuildsRootError::PrevError(error.clone())),
        }
    }

    /// Retrieves the `LocalBuildState` for the given build, initializing it if necessary.
    async fn get_state(&self, build_id: Id) -> LocalBuildState {
        let mut locals = self.local_builds.lock().await;
        if let Some(ours) = locals.get(&build_id) {
            ours.clone()
        } else {
            let build_state = LocalBuildState::default();
            locals.insert(build_id, build_state.clone());
            build_state
        }
    }
}

/// Creates builds root services from the `BuildsRootSettings`. Returns an error if the
/// configuration is structurally invalid, but does not attempt to connect to any external
/// services.
pub fn init_builds_root(
    conf: &config::BuildsRootSettings,
) -> anyhow::Result<(PutBuilds, FetchBuilds)> {
    if !conf.uri.path().ends_with('/') {
        anyhow::bail!("invalid uri: '{}', must end with a '/'", conf.uri);
    }

    let root: Arc<dyn BuildsRootService> = match conf.uri.scheme() {
        "gs" => Arc::new(gcs::GCSBuildsRoot::new(conf.uri.clone())?),
        "file" => Arc::new(local::LocalBuildsRoot::new(conf.uri.path())),
        other => anyhow::bail!("invalid uri: unsupported scheme: '{}'", other),
    };
    Ok((PutBuilds(root.clone()), FetchBuilds::new(root)))
}

#[derive(thiserror::Error, Debug)]
pub enum BuildsRootError {
    #[error("build database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("cloud storage error: {0}")]
    GsUtil(#[from] SubprocessError),

    #[error("cannot create build URI: {0}")]
    Url(#[from] url::ParseError),
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    PrevError(String),
}

#[async_trait]
trait BuildsRootService: Debug + Send + Sync {
    async fn put_build(&self, build_id: Id, build: &Path) -> Result<(), BuildsRootError>;
    async fn retrieve_build(&self, build_id: Id) -> Result<PathBuf, BuildsRootError>;
}

#[derive(Debug, Clone)]
struct LastError {
    attempts: u32,
    last_attempt: Instant,
    error: String,
}

impl Default for LastError {
    fn default() -> Self {
        Self {
            attempts: 0,
            last_attempt: Instant::now(),
            error: String::new(),
        }
    }
}

impl LastError {
    fn should_fetch(&self) -> bool {
        let wait = match self.attempts {
            0 => return true,
            1 => Duration::from_millis(50),
            2..=5 => Duration::from_millis(250),
            _ => Duration::from_secs(5),
        };
        self.last_attempt.elapsed() > wait
    }
}

/// A local copy of a build database, which was fetched from the builds root.
/// Note that for the local builds root, this will refer to the same file within the builds root,
/// so it should never be modified.
#[derive(Debug, Clone)]
struct LocalBuildDB {
    path: PathBuf,
}

impl LocalBuildDB {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[derive(Debug, Clone)]
struct LocalBuildState(Arc<Mutex<Result<LocalBuildDB, LastError>>>);

impl std::default::Default for LocalBuildState {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Err(LastError::default()))))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tempfile::TempDir;
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn test_concurrent_fetches_when_first_fetch_is_successful() {
        let dir = TempDir::new().unwrap();
        const BUILD_IDS: &[Id] = &[Id::new(1), Id::new(2), Id::new(3)];
        for id in BUILD_IDS {
            make_test_build(dir.path(), *id);
        }

        let root = Arc::new(MockSuccess(AtomicU32::new(0), dir.path().to_owned()));
        let fetch_svc = FetchBuilds::new(root.clone());

        // Spawn a bunch of tasks that will all try to fetch the same builds.
        let handles: Vec<JoinHandle<Result<BuildDBRef, String>>> = (0..15)
            .into_iter()
            .map(|i| {
                let fetch_svc = fetch_svc.clone();
                tokio::spawn(async move {
                    let id = BUILD_IDS[i % BUILD_IDS.len()];
                    fetch_svc
                        .get_build(id)
                        .await
                        .map_err(|err| format!("fetch {} failed with error: {:?}", i, err))
                })
            })
            .collect();

        // All of them should return successfully.
        for handle in handles {
            let db_ref = handle.await.unwrap().expect("fetch task failed");
            assert_build_ok(&db_ref);
        }

        // There should have been exactly 1 call to the builds root service for each unique build.
        let actual_calls = root.0.load(Ordering::SeqCst);
        assert_eq!(BUILD_IDS.len(), actual_calls as usize);
    }

    #[tokio::test]
    async fn local_builds_root_can_get_and_put_a_build_successfully() {
        let dir = TempDir::new().unwrap();
        let build_id = Id::new(7);
        let test_build = make_test_build(dir.path(), build_id);

        let root_dir = TempDir::new().unwrap();

        let root_uri =
            url::Url::parse(format!("file://{}/", root_dir.path().display()).as_str()).unwrap();
        let (put_svc, fetch_svc) = init_builds_root(&config::BuildsRootSettings { uri: root_uri })
            .expect("init_builds_root failed");

        put_svc
            .put_build(build_id, &test_build)
            .await
            .expect("failed to put build");

        let build = fetch_svc
            .get_build(build_id)
            .await
            .expect("failed to get build");
        assert_build_ok(&build);

        // Fetching a build that doesn't exist returns an error
        fetch_svc
            .get_build(Id::new(9999))
            .await
            .expect_err("should fail");
    }

    #[tokio::test]
    async fn fetch_errors_are_not_retried_immediately() {
        let root = Arc::new(MockFailures(AtomicU32::new(0)));
        let fetch_svc = FetchBuilds::new(root.clone());

        // Make a bunch of calls in a hot loop, where we know that they'll return errors. Then
        // we'll assert that not all the calls were actually dispatched through to the
        // MockBuildsRoot. There isn't really a good way to make a precise assertion on what the
        // number of calls _should_ be because time is involved, but we can at least be certain
        // that the actual number should be less than 10.
        for _ in 0..10 {
            let err = fetch_svc
                .get_build(Id::new(9))
                .await
                .expect_err("should fail");
            assert!(matches!(err, BuildsRootError::PrevError(_)));
        }

        let num_calls = root.0.load(std::sync::atomic::Ordering::SeqCst);
        assert!(num_calls < 10);
    }

    #[derive(Debug)]
    struct MockSuccess(AtomicU32, PathBuf);
    #[async_trait]
    impl BuildsRootService for MockSuccess {
        async fn put_build(&self, _: Id, _: &Path) -> Result<(), BuildsRootError> {
            unimplemented!()
        }

        async fn retrieve_build(&self, build_id: Id) -> Result<PathBuf, BuildsRootError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Sleep for a short time, so that we can exercise some of the synchronization code.
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            Ok(self.1.join(build_id.to_string()))
        }
    }

    #[derive(Debug)]
    struct MockFailures(AtomicU32);

    #[async_trait]
    impl BuildsRootService for MockFailures {
        async fn put_build(&self, _: Id, _: &Path) -> Result<(), BuildsRootError> {
            unimplemented!()
        }

        async fn retrieve_build(&self, build_id: Id) -> Result<PathBuf, BuildsRootError> {
            let call_num = self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Sleep for a short time, so that we can exercise some of the synchronization code.
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            Err(BuildsRootError::PrevError(format!(
                "test error: {}, build: {}",
                call_num, build_id
            )))
        }
    }

    fn make_test_build(dir: &Path, id: Id) -> PathBuf {
        let path = dir.join(id.to_string());
        let conn = Connection::open(&path).expect("failed to create db");
        conn.execute_batch(
            r#"CREATE TABLE coal_mine (canary);
            INSERT INTO coal_mine (canary) VALUES ('tweety');"#,
        )
        .expect("failed to exec sql");
        conn.close().expect("failed to close sqlite connection");
        assert!(path.exists());
        path
    }

    fn assert_build_ok(build: &BuildDBRef) {
        let canary: String = build
            .query_row(r#"SELECT canary FROM coal_mine LIMIT 1;"#, [], |row| {
                row.get(0)
            })
            .expect("failed to query row");
        assert_eq!("tweety", canary);
    }
}
