use super::{jobs, logs, Handler, Id};

use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Write;
use tracing::{error, info};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create temporary build directory")]
    CreateDir(#[source] std::io::Error),
    #[error("failed to create source catalog file")]
    CreateSource(#[source] std::io::Error),
    #[error("failed to resolve build URL relative to builds root")]
    URLError(#[from] url::ParseError),
    #[error("database error")]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    JobError(#[from] jobs::Error),
}

/// JobStatus is the possible outcomes of a handled draft submission.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    BuildFailed,
    TestFailed,
    PersistFailed,
    Success,
}

/// A DraftHandler is a Handler which builds drafts.
pub struct DraftHandler {
    connector_network: String,
    flowctl: String,
    logs_tx: logs::Tx,
    root: url::Url,
}

impl DraftHandler {
    pub fn new(
        connector_network: &str,
        flowctl: &str,
        logs_tx: &logs::Tx,
        root: &url::Url,
    ) -> Self {
        DraftHandler {
            connector_network: connector_network.to_string(),
            flowctl: flowctl.to_string(),
            logs_tx: logs_tx.clone(),
            root: root.clone(),
        }
    }
}

// Row is the dequeued task shape of a draft build & test operation.
#[derive(Debug)]
struct Row {
    catalog_spec: serde_json::Value,
    created_at: DateTime<Utc>,
    id: Id,
    logs_token: uuid::Uuid,
    updated_at: DateTime<Utc>,
    user_id: uuid::Uuid,
}

#[async_trait::async_trait]
impl Handler for DraftHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match sqlx::query_as!(
            Row,
            r#"select
                catalog_spec,
                created_at,
                id as "id: Id",
                logs_token,
                updated_at,
                user_id
            from drafts where job_status->>'type' = 'queued'
            order by id asc
            limit 1
            for update of drafts skip locked;
            "#
        )
        .fetch_optional(&mut txn)
        .await?
        {
            None => return Ok(std::time::Duration::from_secs(5)),
            Some(row) => row,
        };

        let (id, status) = self.process(row).await?;
        info!(%id, ?status, "finished");

        let r = sqlx::query_unchecked!(
            r#"update drafts set
                    job_status = $2,
                    updated_at = clock_timestamp()
                where id = $1;
                "#,
            id,
            sqlx::types::Json(status),
        )
        .execute(&mut txn)
        .await?;

        if r.rows_affected() != 1 {
            anyhow::bail!("rows_affected is {}, not one", r.rows_affected())
        }
        txn.commit().await?;

        Ok(std::time::Duration::ZERO)
    }
}

impl DraftHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.id))]
    async fn process(&mut self, mut row: Row) -> anyhow::Result<(Id, JobStatus)> {
        let tmpdir = tempfile::TempDir::new().map_err(|e| Error::CreateDir(e))?;
        let tmpdir = tmpdir.path();

        info!(
            %row.created_at,
            %row.logs_token,
            %row.updated_at,
            %row.user_id,
            tmpdir=?tmpdir,
            "processing draft",
        );

        inject_storage_mapping(&mut row.catalog_spec);
        encode_resources(&mut row.catalog_spec);

        // We perform the build under a ./builds/ subdirectory, which is a
        // specific sub-path expected by temp-data-plane underneath its
        // working temporary directory. This lets temp-data-plane use the
        // build database in-place.
        let builds_dir = tmpdir.join("builds");
        std::fs::create_dir(&builds_dir).map_err(|e| Error::CreateDir(e))?;

        // Write our catalog source file within the build directory.
        std::fs::File::create(&builds_dir.join(&format!("{}.flow.yaml", row.id)))
            .and_then(|mut f| {
                f.write_all(
                    serde_json::to_string_pretty(&row.catalog_spec)
                        .unwrap()
                        .as_bytes(),
                )
            })
            .map_err(|e| Error::CreateSource(e))?;

        let db_name = format!("{}", row.id);
        let db_path = builds_dir.join(&db_name);

        let build_job = jobs::run(
            "build",
            &self.logs_tx,
            row.logs_token,
            tokio::process::Command::new(&self.flowctl)
                .arg("api")
                .arg("build")
                .arg("--build-id")
                .arg(&db_name)
                .arg("--directory")
                .arg(&builds_dir)
                .arg("--fs-root")
                .arg(&builds_dir)
                .arg("--network")
                .arg(&self.connector_network)
                .arg("--source")
                .arg(format!("file:///{}.flow.yaml", row.id))
                .arg("--source-type")
                .arg("catalog")
                .arg("--ts-package")
                .arg("--log.level=info")
                .arg("--log.format=color")
                .current_dir(tmpdir),
        )
        .await?;

        if !build_job.success() {
            return Ok((row.id, JobStatus::BuildFailed));
        }

        // Start a data-plane. It will use ${tmp_dir}/builds as its builds-root,
        // which we also used as the build directory, meaning the build database
        // is already in-place.
        let mut data_plane_job = tokio::process::Command::new(&self.flowctl);
        let data_plane_job = jobs::run(
            "temp-data-plane",
            &self.logs_tx,
            row.logs_token,
            data_plane_job
                .arg("temp-data-plane")
                .arg("--network")
                .arg(&self.connector_network)
                .arg("--tempdir")
                .arg(tmpdir)
                .arg("--unix-sockets")
                .arg("--log.level=info")
                .arg("--log.format=color")
                .current_dir(tmpdir),
        );

        // Start the test runner.
        let mut test_job = tokio::process::Command::new(&self.flowctl);
        let test_job = jobs::run(
            "test",
            &self.logs_tx,
            row.logs_token,
            test_job
                .arg("api")
                .arg("test")
                .arg("--build-id")
                .arg(&db_name)
                .arg("--broker.address")
                .arg(&format!(
                    "unix://localhost/{}/gazette.sock",
                    tmpdir.as_os_str().to_string_lossy()
                ))
                .arg("--consumer.address")
                .arg(&format!(
                    "unix://localhost/{}/consumer.sock",
                    tmpdir.as_os_str().to_string_lossy()
                ))
                .arg("--log.level=info")
                .arg("--log.format=color")
                .current_dir(tmpdir),
        );

        // Drive the data-plane and test jobs, until tests complete.
        tokio::pin!(test_job);
        let test_job = tokio::select! {
            r = data_plane_job => {
                tracing::error!(?r, "test data-plane exited unexpectedly");
                test_job.await // Wait for the test job to finish.
            }
            r = &mut test_job => r,
        }?;

        if !test_job.success() {
            return Ok((row.id, JobStatus::TestFailed));
        }

        // Persist the build.
        let dest_url = self.root.join(&row.id.to_string())?;

        let persist_job = jobs::run(
            "persist",
            &self.logs_tx,
            row.logs_token,
            tokio::process::Command::new("gsutil")
                .arg("cp")
                .arg(&db_path)
                .arg(dest_url.to_string()),
        )
        .await?;

        if !persist_job.success() {
            return Ok((row.id, JobStatus::PersistFailed));
        }

        Ok((row.id, JobStatus::Success))
    }
}

/// Injects valid StorageMappings into the Catalog. We're setting these up upon
/// signup and this avoids the need for users to include these in every Build's
/// catalog json individually.
fn inject_storage_mapping(catalog: &mut serde_json::Value) {
    let c = catalog.as_object_mut().unwrap();

    // Don't mess with an existing storage mapping.
    if c.contains_key("storageMappings") {
        return;
    }

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
}

/// We expose the Catalog format over the control plane api as json. To this
/// end, we allow submitting json resources directly, without base64 encoding
/// them. However, this isn't expected by flowctl, so we base64 encode them for
/// the purposes of the build process. Any resources which are already encoded
/// are left as-is.
fn encode_resources(catalog: &mut serde_json::Value) {
    // Don't add a resources key if there isn't one already.
    if !catalog.as_object().unwrap().contains_key("resources") {
        return;
    }

    let resources = &mut catalog["resources"];

    if let Some(res) = resources.as_object_mut() {
        for (_res_url, resource) in res.iter_mut() {
            if let Some(content) = resource["content"].as_object() {
                let serialized = serde_json::to_string(&content).unwrap();
                let encoded = base64::encode(serialized);
                resource["content"] = serde_json::Value::String(encoded);
            }
        }
        // We've base64 encoded all the json resources.
    } else {
        // A catalog without embedded resources is good to go.
    }
}
