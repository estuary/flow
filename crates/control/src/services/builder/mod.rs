use crate::context::AppContext;
use crate::models::JsonValue;
use crate::models::{
    builds::{Build, State},
    id::Id,
};
use crate::repo::builds::{dequeue_build, update_build_state};

use futures::{pin_mut, select, FutureExt};
use std::{
    io::Write,
    path::{Path, PathBuf},
};
use tokio::io::AsyncBufReadExt;
use tracing::{debug, error, info};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create temporary build directory")]
    CreateDir(#[source] std::io::Error),
    #[error("failed to create source catalog file")]
    CreateSource(#[source] std::io::Error),
    #[error("invalid catalog: {0}")]
    InvalidCatalog(String),
    #[error("processing job {job:?} failed")]
    Job {
        job: String,
        #[source]
        err: std::io::Error,
    },
    #[error("failed to persist build {id}")]
    PersistBuild {
        id: Id<Build>,
        #[source]
        src: crate::services::builds_root::BuildsRootError,
    },
    #[error("sqlite database error")]
    Sqlite(#[from] rusqlite::Error),
    #[error("control plane database error")]
    SqlxError(#[from] sqlx::Error),
}

pub async fn serve_builds<F>(ctx: AppContext, shutdown: F) -> Result<(), Error>
where
    F: std::future::Future<Output = ()>,
{
    let shutdown = shutdown.fuse();
    pin_mut!(shutdown);

    let mut backoff = std::time::Duration::ZERO;

    loop {
        let sleep = tokio::time::sleep(backoff).fuse();
        pin_mut!(sleep);

        select! {
            _ = shutdown => {
                return Ok(())
            }
            _ = &mut sleep => (),
        };

        // Begin a |txn| which will scope a held advisory lock on `Build.id`.
        let mut txn = ctx.db().begin().await?;

        if let Some(build) = dequeue_build(&mut txn).await? {
            let Build {
                id,
                account_id,
                catalog,
                created_at,
                state: _,
                updated_at,
            } = build;

            let tmpdir = tempfile::TempDir::new().map_err(|e| Error::CreateDir(e))?;
            info!(%id, %account_id, tmpdir=?tmpdir.path(), %created_at, %updated_at, "processing dequeued build");

            let catalog = process_catalog(catalog).await?;
            debug!(
                processed_catalog = %serde_json::to_string_pretty(&catalog).unwrap(),
                "finished processing catalog"
            );

            let (state, db_path) = process_build(id, catalog, tmpdir.path()).await?;
            info!(?state, "processed build");

            ctx.put_builds()
                .put_build(id, &db_path)
                .await
                .map_err(|src| Error::PersistBuild { id, src })?;
            info!(%id, "put build");

            update_build_state(&mut txn, id, state).await?;
            txn.commit().await?;

            backoff = std::time::Duration::ZERO;
        } else {
            debug!("serve_builds found no build to dequeue. Sleeping...");
            backoff = std::time::Duration::from_secs(5);
        }
    }
}

async fn process_catalog(
    catalog: Option<sqlx::types::Json<JsonValue>>,
) -> Result<JsonValue, Error> {
    let mut catalog = catalog
        .ok_or_else(|| Error::InvalidCatalog("Missing catalog definition".to_owned()))?
        .0;

    inject_storage_mapping(&mut catalog)?;
    encode_resources(&mut catalog)?;

    Ok(catalog.into())
}

/// Injects valid StorageMappings into the Catalog. We're setting these up upon
/// signup and this avoids the need for users to include these in every Build's
/// catalog json individually.
fn inject_storage_mapping(catalog: &mut JsonValue) -> Result<(), Error> {
    let c = catalog
        .as_object_mut()
        .ok_or_else(|| Error::InvalidCatalog("Catalog must be an object".to_owned()))?;

    // TODO: Once we start to collect Storage Mapping information during signup,
    // we can inject their real storage mappings here. Until then, this allows
    // catalogs created by the UI to actually build successfully.
    let store = serde_json::json!({"provider": "GCS", "bucket": "flow-example"});

    // TODO: How should we setup global resolution of ops/ storage mappings?
    // Using an Account-specific prefix here fails because it does not match the
    // ops/ collections. It seems like we should be able to omit ops/ mappings
    // from individual builds? Or inject them all here?
    let prefix = "";

    c.insert(
        "storageMappings".to_owned(),
        serde_json::json!({ prefix: { "stores": [store] } }),
    );
    Ok(())
}

/// We expose the Catalog format over the control plane api as json. To this
/// end, we allow submitting json resources directly, without base64 encoding
/// them. However, this isn't expected by flowctl, so we base64 encode them for
/// the purposes of the build process. Any resources which are already encoded
/// are left as-is.
fn encode_resources(catalog: &mut serde_json::Value) -> Result<(), Error> {
    let resources = &mut catalog["resources"];

    if let Some(res) = resources.as_object_mut() {
        for (_res_url, resource) in res.iter_mut() {
            if let Some(content) = resource["content"].as_object() {
                let serialized = serde_json::to_string(&content).map_err(|_e| {
                    Error::InvalidCatalog(
                        "Catalog json-content could not be re-serialized".to_owned(),
                    )
                })?;
                let encoded = base64::encode(serialized);
                resource["content"] = serde_json::Value::String(encoded);
            }
        }

        // We've base64 encoded all the json resources.
        Ok(())
    } else {
        // A catalog without embedded resources is good to go.
        Ok(())
    }
}

async fn process_build(
    id: Id<Build>,
    catalog: JsonValue,
    tmp_dir: &Path,
) -> Result<(State, PathBuf), Error> {
    // We perform the build under a ./builds/ subdirectory, which is a
    // specific sub-path expected by temp-data-plane underneath its
    // working temporary directory. This lets temp-data-plane use the
    // build database in-place.
    let builds_dir = tmp_dir.join("builds");
    std::fs::create_dir(&builds_dir).map_err(|err| Error::CreateDir(err))?;

    // Write our catalog source file within the build directory.
    std::fs::File::create(&builds_dir.join(&format!("{}.flow.yaml", id)))
        .and_then(|mut f| {
            f.write_all(
                serde_json::to_string_pretty(&catalog)
                    .expect("to always serialize a models::Catalog")
                    .as_bytes(),
            )
        })
        .map_err(|e| Error::CreateSource(e))?;

    let db_name = format!("{}", id);
    let db_path = builds_dir.join(&db_name);
    let db = rusqlite::Connection::open(&db_path)?;

    enable_wal_mode(&db)?;
    create_job_logs_table(&db)?;

    let build_job = run_job(
        tokio::process::Command::new("flowctl")
            .arg("api")
            .arg("build")
            .arg("--build-id")
            .arg(&db_name)
            .arg("--directory")
            .arg(&builds_dir)
            .arg("--fs-root")
            .arg(&builds_dir)
            .arg("--network")
            .arg(&crate::config::settings().application.connector_network)
            .arg("--source")
            .arg(format!("file:///{}.flow.yaml", id))
            .arg("--source-type")
            .arg("catalog")
            .arg("--ts-package")
            .arg("--log.level=info")
            .arg("--log.format=color")
            .current_dir(tmp_dir),
        &db,
        "build",
    )
    .await
    .map_err(|err| Error::Job {
        job: "build".to_string(),
        err,
    })?;

    if !build_job.success() {
        return Ok((
            State::BuildFailed {
                code: build_job.code(),
            },
            db_path,
        ));
    }

    // Start a data-plane. It will use ${tmp_dir}/builds as its builds-root,
    // which we also used as the build directory, meaning the build database
    // is already in-place.
    let mut data_plane_job = tokio::process::Command::new("flowctl");
    let data_plane_job = run_job(
        data_plane_job
            .arg("temp-data-plane")
            .arg("--network")
            .arg(&crate::config::settings().application.connector_network)
            .arg("--tempdir")
            .arg(tmp_dir)
            .arg("--unix-sockets")
            .arg("--log.level=info")
            .arg("--log.format=color")
            .current_dir(tmp_dir),
        &db,
        "temp-data-plane",
    )
    .fuse();

    // Start the test runner.
    let mut test_job = tokio::process::Command::new("flowctl");
    let test_job = run_job(
        test_job
            .arg("api")
            .arg("test")
            .arg("--build-id")
            .arg(&db_name)
            .arg("--broker.address")
            .arg(&format!(
                "unix://localhost/{}/gazette.sock",
                tmp_dir.as_os_str().to_string_lossy()
            ))
            .arg("--consumer.address")
            .arg(&format!(
                "unix://localhost/{}/consumer.sock",
                tmp_dir.as_os_str().to_string_lossy()
            ))
            .arg("--log.level=info")
            .arg("--log.format=color")
            .current_dir(tmp_dir),
        &db,
        "test",
    )
    .fuse();

    // Drive the data-plane and test jobs, until tests complete.
    pin_mut!(data_plane_job, test_job);
    let test_job = select! {
        r = data_plane_job => {
            tracing::error!(?r, "test data-plane exited unexpectedly");
            test_job.await // Wait for the test job to finish.
        }
        r = test_job => r,
    }
    .map_err(|err| Error::Job {
        job: "test".to_string(),
        err,
    })?;

    if !test_job.success() {
        return Ok((
            State::TestFailed {
                code: test_job.code(),
            },
            db_path,
        ));
    }

    Ok((State::Success, db_path))
}

// run_job spawns the provided Command, capturing its stdout and stderr
// into the provided logs database identified by |job|.
async fn run_job(
    cmd: &mut tokio::process::Command,
    logs_db: &rusqlite::Connection,
    job: &str,
) -> Result<std::process::ExitStatus, std::io::Error> {
    cmd
        // Pass through PATH, but remove all other environment variables.
        .env_clear()
        .envs(std::env::vars().filter(|&(ref k, _)| k == "PATH"))
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;

    let stdout = capture_job_logs(logs_db, job, 1, child.stdout.take().unwrap());
    let stderr = capture_job_logs(logs_db, job, 2, child.stderr.take().unwrap());

    let (_, _, exit) = futures::try_join!(stdout, stderr, child.wait())?;
    Ok(exit)
}

fn enable_wal_mode(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    let mode: String = db.pragma_update_and_check(None, "journal_mode", "wal", |row| row.get(0))?;
    if mode != "wal" {
        Err(rusqlite::Error::UserFunctionError(
            format!("expected journal_mode to be wal, not {}", mode).into(),
        ))
    } else {
        Ok(())
    }
}

fn create_job_logs_table(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        r#"
    CREATE TABLE IF NOT EXISTS job_logs (
        job TEXT NOT NULL,
        source INTEGER NOT NULL, -- 0 is stdout; 1 is stderr.
        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        line TEXT NOT NULL
    );
    "#,
    )?;

    Ok(())
}

// capture_job_logs consumes lines from the AsyncRead and adds each as a log
// entry to a well-known `job_logs` table within the given SQLite database.
// Each entry is identified by its job name and source.
// By convention, stdout is source=1 and stderr is source=2.
// TODO(johnny): Consider locking down `source` with an enum.
async fn capture_job_logs<R>(
    db: &rusqlite::Connection,
    job: &str,
    source: i32,
    r: R,
) -> Result<(), std::io::Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut splits = tokio::io::BufReader::new(r).split(b'\n');
    while let Some(split) = splits.next_segment().await? {
        db.execute(
            "INSERT INTO job_logs (job, source, line) VALUES (?, ?, ?);",
            rusqlite::params![job, source, split],
        )
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
    }

    Ok(())
}
