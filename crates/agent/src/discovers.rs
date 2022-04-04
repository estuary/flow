use super::{jobs, logs, Handler, Id};

use anyhow::Context;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, info};

/// JobStatus is the possible outcomes of a handled discover operation.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    WrongProtocol { protocol: String },
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
    created_at: DateTime<Utc>,
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
            // TODO(johnny): If we stored `docker inspect` output within connector_images,
            // we could pull a resolved digest directly from it?
            r#"select
                c.image_name,
                d.capture_name,
                d.connector_tag_id as "connector_tag_id: Id",
                d.created_at,
                d.endpoint_config::text as "endpoint_config_json!",
                d.id as "id: Id",
                d.logs_token,
                d.updated_at,
                d.user_id,
                t.image_tag,
                t.protocol as "protocol!"
            from discovers as d
            join connector_tags as t on d.connector_tag_id = t.id
            join connectors as c on c.id = t.connector_id
            where d.job_status->>'type' = 'queued' and t.job_status->>'type' = 'success'
            order by d.id asc
            limit 1
            for update of d skip locked;
            "#
        )
        .fetch_optional(&mut txn)
        .await?
        {
            None => return Ok(std::time::Duration::from_secs(5)),
            Some(row) => row,
        };

        let (id, status, catalog_spec) = self.process(row).await?;
        info!(%id, ?status, "finished");

        let r = sqlx::query_unchecked!(
            r#"update discovers set
                    job_status = $2,
                    updated_at = clock_timestamp(),
                    -- Remaining fields are null on failure:
                    catalog_spec = $3
                where id = $1;
                "#,
            id,
            sqlx::types::Json(status),
            catalog_spec,
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

impl DiscoverHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.id))]
    async fn process(
        &mut self,
        row: Row,
    ) -> anyhow::Result<(Id, JobStatus, Option<serde_json::Value>)> {
        info!(
            %row.image_name,
            %row.capture_name,
            %row.connector_tag_id,
            %row.created_at,
            %row.logs_token,
            %row.updated_at,
            %row.user_id,
            %row.image_tag,
            %row.protocol,
            "processing discover",
        );
        let image_composed = format!("{}{}", row.image_name, row.image_tag);

        if row.protocol != "capture" {
            return Ok((
                row.id,
                JobStatus::WrongProtocol {
                    protocol: row.protocol,
                },
                None,
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
            return Ok((row.id, JobStatus::PullFailed, None));
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
            return Ok((row.id, JobStatus::DiscoverFailed, None));
        }

        let spec = swizzle_response_to_bundle(
            &row.capture_name,
            &row.endpoint_config_json,
            &row.image_name,
            &row.image_tag,
            &discover.1,
        )
        .context("converting discovery response into a bundle")?;

        Ok((row.id, JobStatus::Success, Some(spec)))
    }
}

// swizzle_response_to_bundle accepts a raw discover response (as bytes),
// along with the raw endpoint configuration and connector image, and returns a
// swizzled bundle in the shape of a Flow catalog specification.
// This bundle is suitable for direct usage within the UI.
fn swizzle_response_to_bundle(
    capture_name: &str,
    endpoint_config_json: &str,
    image_name: &str,
    image_tag: &str,
    response: &[u8],
) -> Result<serde_json::Value, serde_json::Error> {
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
        resource_spec: serde_json::Value,
        /// JSON schema of documents produced by this binding.
        document_schema: serde_json::Value,
        /// Composite key of documents (if known), as JSON-Pointers.
        key_ptrs: Vec<String>,
    }
    let response: Response = serde_json::from_value(response)?;

    // Swizzle the discovered bindings into:
    //  - Separate *.schema.json resources for each captured collection.
    //  - A single *.config.json for the endpoint configuration.
    //  - A single catalog *.flow.json resource holding the capture and associated collections.
    //  - A top-level bundle with inline resources and an import of *.flow.json.
    let mut bindings = Vec::new();
    let mut collections = serde_json::Map::<String, serde_json::Value>::new();
    let mut resources = serde_json::Map::<String, serde_json::Value>::new();

    // Relative resource paths are resolved to their absolute
    let fake_root = "flow://discovered/";

    for b in response.bindings {
        let recommended = &b.recommended_name;
        let collection_name = format!("{capture_prefix}/{recommended}");
        let schema_url = format!("{recommended}.schema.json");

        bindings.push(json!({
            "resource": b.resource_spec,
            "target": collection_name,
        }));
        collections.insert(
            collection_name,
            json!({
                "schema": schema_url,
                "key": b.key_ptrs,
            }),
        );
        resources.insert(
            format!("{fake_root}{schema_url}"),
            json!({
                "contentType": "JSON_SCHEMA",
                "content": &b.document_schema,
            }),
        );
    }

    // Add endpoint configuration as a resource.
    // We MUST base64 this config, because inlining it directly may re-order
    // properties which will break a contained sops MAC signature.
    let config_url = format!("{image_suffix}.config.json");
    resources.insert(
        format!("{fake_root}{config_url}"),
        json!({
            "contentType": "CONFIG",
            "content": base64::encode(endpoint_config_json),
        }),
    );

    // Add top-level Flow catalog specification for this capture.
    let flow_url = format!("{fake_root}{image_suffix}.flow.json");
    resources.insert(
        flow_url.clone(),
        json!({
            "contentType": "CATALOG",
            "content": {
                "captures": {
                    capture_name: {
                        "endpoint": {
                            "connector": {
                                "image": image_composed,
                                "config": config_url,
                            }
                        },
                        "bindings": bindings,
                    },
                },
                "collections": collections,
            },
        }),
    );

    Ok(json!({
        "resources": resources,
        "import": [flow_url],
    }))
}
