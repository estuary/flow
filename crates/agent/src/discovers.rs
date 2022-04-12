use super::{jobs, logs, Handler, Id};

use anyhow::Context;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use std::collections::BTreeMap;
use tracing::{debug, info};

/// JobStatus is the possible outcomes of a handled discover operation.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    WrongProtocol { protocol: String },
    TagFailed,
    PullFailed,
    DiscoverFailed,
    Success,
}

/// A DiscoverHandler is a Handler which performs discovery operations.
pub struct DiscoverHandler {
    connector_network: String,
    flowctl: String,
    logs_tx: logs::Tx,
}

impl DiscoverHandler {
    pub fn new(connector_network: &str, flowctl: &str, logs_tx: &logs::Tx) -> Self {
        Self {
            connector_network: connector_network.to_string(),
            flowctl: flowctl.to_string(),
            logs_tx: logs_tx.clone(),
        }
    }
}

// Row is the dequeued task shape of a discover operation.
#[derive(Debug)]
struct Row {
    capture_name: String,
    connector_tag_id: Id,
    connector_tag_job_success: bool,
    created_at: DateTime<Utc>,
    draft_id: Id,
    endpoint_config_json: String,
    id: Id,
    image_name: String,
    image_tag: String,
    logs_token: uuid::Uuid,
    protocol: String,
    updated_at: DateTime<Utc>,
    user_id: uuid::Uuid,
}

#[async_trait::async_trait]
impl Handler for DiscoverHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match sqlx::query_as!(
            Row,
            // TODO(johnny): If we stored `docker inspect` output within connector_tags,
            // we could pull a resolved digest directly from it?
            // Better: have `flowctl api spec` run it internally and surface the digest?
            r#"select
                discovers.capture_name,
                discovers.connector_tag_id as "connector_tag_id: Id",
                connector_tags.job_status->>'type' = 'success' as "connector_tag_job_success!",
                discovers.created_at,
                discovers.draft_id as "draft_id: Id",
                discovers.endpoint_config::text as "endpoint_config_json!",
                discovers.id as "id: Id",
                connectors.image_name,
                connector_tags.image_tag,
                discovers.logs_token,
                connector_tags.protocol as "protocol!",
                discovers.updated_at,
                drafts.user_id
            from discovers
            join drafts on discovers.draft_id = drafts.id
            join connector_tags on discovers.connector_tag_id = connector_tags.id
            join connectors on connectors.id = connector_tags.connector_id
            where discovers.job_status->>'type' = 'queued' and connector_tags.job_status->>'type' != 'queued'
            order by discovers.id asc
            limit 1
            for update of discovers skip locked;
            "#
        )
        .fetch_optional(&mut txn)
        .await?
        {
            None => return Ok(std::time::Duration::from_secs(5)),
            Some(row) => row,
        };

        let (id, status) = self.process(row, &mut txn).await?;
        info!(%id, ?status, "finished");

        sqlx::query_unchecked!(
            r#"update discovers set
                    job_status = $2,
                    updated_at = clock_timestamp()
                where id = $1
                returning 1 as "must_exist";
                "#,
            id,
            sqlx::types::Json(status),
        )
        .fetch_one(&mut txn)
        .await?;

        txn.commit().await?;

        Ok(std::time::Duration::ZERO)
    }
}

impl DiscoverHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<(Id, JobStatus)> {
        info!(
            %row.capture_name,
            %row.connector_tag_id,
            %row.connector_tag_job_success,
            %row.created_at,
            %row.draft_id,
            %row.image_name,
            %row.image_tag,
            %row.logs_token,
            %row.protocol,
            %row.updated_at,
            %row.user_id,
            "processing discover",
        );
        let image_composed = format!("{}{}", row.image_name, row.image_tag);

        if !row.connector_tag_job_success {
            return Ok((row.id, JobStatus::TagFailed));
        }
        if row.protocol != "capture" {
            return Ok((
                row.id,
                JobStatus::WrongProtocol {
                    protocol: row.protocol,
                },
            ));
        }

        // Pull the image.
        let pull = jobs::run(
            "pull",
            &self.logs_tx,
            row.logs_token,
            tokio::process::Command::new("docker")
                .arg("pull")
                .arg(&image_composed),
        )
        .await?;

        if !pull.success() {
            return Ok((row.id, JobStatus::PullFailed));
        }

        // Fetch its discover output.
        let discover = jobs::run_with_input_output(
            "discover",
            &self.logs_tx,
            row.logs_token,
            row.endpoint_config_json.as_bytes(),
            tokio::process::Command::new(&self.flowctl)
                .arg("api")
                .arg("discover")
                .arg("--config=/dev/stdin")
                .arg("--image")
                .arg(&image_composed)
                .arg("--network")
                .arg(&self.connector_network)
                .arg("--output=json"),
        )
        .await?;

        if !discover.0.success() {
            return Ok((row.id, JobStatus::DiscoverFailed));
        }

        let catalog = swizzle_response_to_catalog(
            &row.capture_name,
            &row.endpoint_config_json,
            &row.image_name,
            &row.image_tag,
            &discover.1,
        )
        .context("converting discovery response into a catalog")?;

        insert_draft_specs(row.draft_id, catalog, txn)
            .await
            .context("inserting draft specs")?;

        Ok((row.id, JobStatus::Success))
    }
}

async fn insert_draft_specs(
    draft_id: Id,
    models::Catalog {
        collections,
        captures,
        ..
    }: models::Catalog,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), sqlx::Error> {
    for (capture, spec) in captures {
        sqlx::query!(
            r#"insert into draft_specs(
                draft_id,
                catalog_name,
                spec_type,
                spec_patch
            ) values ($1, $2, 'capture', $3)
            on conflict (draft_id, catalog_name) do update set
                spec_type = 'capture',
                spec_patch = $3
            returning 1 as "must_exist";
            "#,
            draft_id as Id,
            capture.as_str() as &str,
            Json(spec) as Json<models::CaptureDef>,
        )
        .fetch_one(&mut *txn)
        .await?;
    }
    for (collection, spec) in collections {
        sqlx::query!(
            r#"insert into draft_specs(
                draft_id,
                catalog_name,
                spec_type,
                spec_patch
            ) values ($1, $2, 'collection', $3)
            on conflict (draft_id, catalog_name) do update set
                spec_type = 'collection',
                spec_patch = $3
            returning 1 as "must_exist";
            "#,
            draft_id as Id,
            collection.as_str() as &str,
            Json(spec) as Json<models::CollectionDef>,
        )
        .fetch_one(&mut *txn)
        .await?;
    }

    sqlx::query_unchecked!(
        r#"update drafts set updated_at = clock_timestamp() where id = $1
            returning 1 as "must_exist";"#,
        draft_id as Id,
    )
    .fetch_one(&mut *txn)
    .await?;

    Ok(())
}

// swizzle_response_to_catalog accepts a raw discover response (as bytes),
// along with the raw endpoint configuration and connector image,
// and returns a models::Catalog.
fn swizzle_response_to_catalog(
    capture_name: &str,
    endpoint_config_json: &str,
    image_name: &str,
    image_tag: &str,
    response: &[u8],
) -> Result<models::Catalog, serde_json::Error> {
    // Split the capture name into a suffix after the final '/',
    // and a prefix of everything before that final '/'.
    // The prefix is used to namespace associated collections of the capture.
    let (capture_prefix, capture_suffix) = capture_name
        .rsplit_once("/")
        .expect("database constraints ensure catalog name has at least one '/'");

    // Extract the docker image suffix after the final '/', or the image if there is no '/'.
    // The image suffix is used to name associated resources of the capture, like configuration.
    let image_suffix = match image_name.rsplit_once("/") {
        Some((_, s)) => s,
        None => &image_name,
    };
    let image_composed = format!("{image_name}{image_tag}");

    let response: serde_json::Value = serde_json::from_slice(response)?;
    debug!(%capture_prefix, %capture_suffix, %image_composed, %image_suffix, %response, "converting response");

    // Response is the expected shape of a discover response.
    #[derive(Deserialize)]
    struct Response {
        bindings: Vec<Binding>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Binding {
        /// A recommended display name for this discovered binding.
        recommended_name: String,
        /// JSON-encoded object which specifies the endpoint resource to be captured.
        resource_spec: models::Object,
        /// JSON schema of documents produced by this binding.
        document_schema: models::Schema,
        /// Composite key of documents (if known), as JSON-Pointers.
        #[serde(default)]
        key_ptrs: Vec<models::JsonPointer>,
    }
    let response: Response = serde_json::from_value(response)?;

    // Break apart each response.binding into constituent
    // collection and capture binding models.
    let mut bindings = Vec::new();
    let mut collections = BTreeMap::new();

    for Binding {
        recommended_name,
        resource_spec: resource,
        document_schema: schema,
        key_ptrs,
    } in response.bindings
    {
        let collection = models::Collection::new(format!("{capture_prefix}/{recommended_name}"));

        bindings.push(models::CaptureBinding {
            resource,
            target: collection.clone(),
        });
        collections.insert(
            collection,
            models::CollectionDef {
                schema,
                key: models::CompositeKey::new(key_ptrs),
                projections: Default::default(),
                derivation: None,
                journals: Default::default(),
            },
        );
    }

    let mut catalog = models::Catalog::default();
    catalog.collections = collections;
    catalog.captures.insert(
        models::Capture::new(capture_name),
        models::CaptureDef {
            bindings,
            endpoint: models::CaptureEndpoint::Connector(models::ConnectorConfig {
                image: image_composed,
                // TODO(johnny): This re-orders sops and is WRONG!
                // Can we make config universally a RawValue?
                // It will probably require inverting how we do yaml deser,
                // where we first transcode an entire catalog source file into a
                // a JSON buffer and *then* parse a models::Catalog using serde_json.
                config: models::Config::Inline(serde_json::from_str(endpoint_config_json)?),
            }),
            interval: models::CaptureDef::default_interval(),
            shards: Default::default(),
        },
    );

    Ok(catalog)
}
