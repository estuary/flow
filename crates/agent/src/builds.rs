use super::{jobs, logs, Handler, Id};

use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Write;
use tracing::{debug, error, info};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to create temporary build directory")]
    CreateDir(#[source] std::io::Error),
    #[error("failed to create source catalog file")]
    CreateSource(#[source] std::io::Error),
    #[error("failed to resolve build URL relative to builds root")]
    URLError(#[from] url::ParseError),
    #[error("database error")]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    JobError(#[from] jobs::Error),
}

/// State is the possible states of a build, serialized as the `builds.state` column.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum State {
    Queued,
    BuildFailed,
    TestFailed,
    PersistFailed,
    Success,
}

/// A BuilderHandler is a Handler which runs builds.
pub struct BuildHandler {
    connector_network: String,
    flowctl: String,
    logs_tx: logs::Tx,
    root: url::Url,
}

impl BuildHandler {
    pub fn new(
        connector_network: &str,
        flowctl: &str,
        logs_tx: &logs::Tx,
        root: &url::Url,
    ) -> Self {
        BuildHandler {
            connector_network: connector_network.to_string(),
            flowctl: flowctl.to_string(),
            logs_tx: logs_tx.clone(),
            root: root.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Handler for BuildHandler {
    type Error = Error;

    fn dequeue() -> &'static str {
        r#"SELECT
            account_id,
            created_at,
            id,
            logs_token,
            spec,
            updated_at
        FROM builds WHERE state->>'type' = 'queued'
        ORDER BY id ASC
        LIMIT 1
        FOR UPDATE OF builds SKIP LOCKED;
        "#
    }

    fn update() -> &'static str {
        "UPDATE builds SET state = $2::text::jsonb, updated_at = clock_timestamp() WHERE id = $1;"
    }

    #[tracing::instrument(ret, skip_all, fields(build = %row.get::<_, Id>(2)))]
    async fn on_dequeue(
        &mut self,
        txn: &mut tokio_postgres::Transaction,
        row: tokio_postgres::Row,
        update: &tokio_postgres::Statement,
    ) -> Result<u64, Error> {
        let (id, state) = self.process(row).await?;

        let state = serde_json::to_string(&state).unwrap();
        info!(%id, %state, "finished");

        Ok(txn.execute(update, &[&id, &state]).await?)
    }
}

impl BuildHandler {
    #[tracing::instrument(ret, skip_all)]
    async fn process(&mut self, row: tokio_postgres::Row) -> Result<(Id, State), Error> {
        let (account_id, created_at, id, logs_token, mut spec, updated_at) = (
            row.get::<_, Id>(0),
            row.get::<_, DateTime<Utc>>(1),
            row.get::<_, Id>(2),
            row.get::<_, uuid::Uuid>(3),
            row.get::<_, serde_json::Value>(4),
            row.get::<_, DateTime<Utc>>(5),
        );

        let tmpdir = tempfile::TempDir::new().map_err(|e| Error::CreateDir(e))?;
        let tmpdir = tmpdir.path();
        info!(%account_id, tmpdir=?tmpdir, %created_at, %updated_at, "processing build");

        inject_storage_mapping(&mut spec);
        encode_resources(&mut spec);
        debug!(
            catalog = %serde_json::to_string_pretty(&spec).unwrap(),
            "tweaked catalog spec"
        );

        // We perform the build under a ./builds/ subdirectory, which is a
        // specific sub-path expected by temp-data-plane underneath its
        // working temporary directory. This lets temp-data-plane use the
        // build database in-place.
        let builds_dir = tmpdir.join("builds");
        std::fs::create_dir(&builds_dir).map_err(|e| Error::CreateDir(e))?;

        // Write our catalog source file within the build directory.
        std::fs::File::create(&builds_dir.join(&format!("{}.flow.yaml", id)))
            .and_then(|mut f| f.write_all(serde_json::to_string_pretty(&spec).unwrap().as_bytes()))
            .map_err(|e| Error::CreateSource(e))?;

        let db_name = format!("{}", id);
        let db_path = builds_dir.join(&db_name);

        let build_job = jobs::run(
            "build",
            &self.logs_tx,
            logs_token,
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
                .arg(format!("file:///{}.flow.yaml", id))
                .arg("--source-type")
                .arg("catalog")
                .arg("--ts-package")
                .arg("--log.level=info")
                .arg("--log.format=color")
                .current_dir(tmpdir),
        )
        .await?;

        if !build_job.success() {
            return Ok((id, State::BuildFailed));
        }

        // Start a data-plane. It will use ${tmp_dir}/builds as its builds-root,
        // which we also used as the build directory, meaning the build database
        // is already in-place.
        let mut data_plane_job = tokio::process::Command::new(&self.flowctl);
        let data_plane_job = jobs::run(
            "temp-data-plane",
            &self.logs_tx,
            logs_token,
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
            logs_token,
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
            return Ok((id, State::TestFailed));
        }

        // Persist the build.
        let dest_url = self.root.join(&id.to_string())?;

        let persist_job = jobs::run(
            "persist",
            &self.logs_tx,
            logs_token,
            tokio::process::Command::new("gsutil")
                .arg("cp")
                .arg(&db_path)
                .arg(dest_url.to_string()),
        )
        .await?;

        if !persist_job.success() {
            return Ok((id, State::PersistFailed));
        }

        Ok((id, State::Success))
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
