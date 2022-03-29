use super::{jobs, logs, Handler, Id};

use anyhow::Context;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, info};

/// State is the possible states of a discover operation,
/// serialized as the `discovers.state` column.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum State {
    Queued,
    WrongType { connector_type: String },
    PullFailed,
    DiscoverFailed,
    Success { spec: serde_json::Value },
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

#[async_trait::async_trait]
impl Handler for DiscoverHandler {
    type Error = anyhow::Error;

    fn dequeue() -> &'static str {
        // TODO(johnny): If we stored `docker inspect` output within connector_images,
        // we could pull a resolved digest directly from it?
        r#"SELECT
            c.image,
            d.account_id,
            d.capture_name,
            d.created_at,
            d.endpoint_config::text,
            d.id,
            d.image_id,
            d.logs_token,
            d.updated_at,
            i.state->'spec'->>'type',
            i.tag
        FROM discovers AS d
        JOIN connector_images AS i ON d.image_id = i.id
        JOIN connectors AS c ON c.id = i.connector_id
        WHERE d.state->>'type' = 'queued' AND i.state->>'type' = 'success'
        ORDER BY d.id ASC
        LIMIT 1
        FOR UPDATE OF d SKIP LOCKED;
        "#
    }

    fn update() -> &'static str {
        "UPDATE discovers SET state = $2::text::jsonb, updated_at = clock_timestamp() WHERE id = $1;"
    }

    #[tracing::instrument(ret, skip_all, fields(discover_id = %row.get::<_, Id>(5)))]
    async fn on_dequeue(
        &mut self,
        txn: &mut tokio_postgres::Transaction,
        row: tokio_postgres::Row,
        update: &tokio_postgres::Statement,
    ) -> Result<u64, Self::Error> {
        let (id, state) = self.process(row).await?;

        let state = serde_json::to_string(&state).unwrap();
        info!(%id, %state, "finished");

        Ok(txn.execute(update, &[&id, &state]).await?)
    }
}

impl DiscoverHandler {
    #[tracing::instrument(err, skip_all)]
    async fn process(&mut self, row: tokio_postgres::Row) -> Result<(Id, State), anyhow::Error> {
        let (
            image_name,
            account_id,
            capture_name,
            created_at,
            endpoint_config_json,
            id,
            image_id,
            logs_token,
            updated_at,
            connector_type,
            image_tag,
        ) = (
            row.get::<_, String>(0),
            row.get::<_, Id>(1),
            row.get::<_, String>(2),
            row.get::<_, DateTime<Utc>>(3),
            row.get::<_, String>(4),
            row.get::<_, Id>(5),
            row.get::<_, Id>(6),
            row.get::<_, uuid::Uuid>(7),
            row.get::<_, DateTime<Utc>>(8),
            row.get::<_, String>(9),
            row.get::<_, String>(10),
        );
        info!(%image_name, %account_id, %created_at, %image_id, %logs_token, %updated_at, %image_tag,
             "processing discover");
        let image_composed = format!("{image_name}{image_tag}");

        if connector_type != "capture" {
            return Ok((id, State::WrongType { connector_type }));
        }

        // Pull the image.
        let pull = jobs::run(
            "pull",
            &self.logs_tx,
            logs_token,
            tokio::process::Command::new("docker")
                .arg("pull")
                .arg(&image_composed),
        )
        .await?;

        if !pull.success() {
            return Ok((id, State::PullFailed));
        }

        // Fetch its discover output.
        let discover = jobs::run_with_input_output(
            "discover",
            &self.logs_tx,
            logs_token,
            endpoint_config_json.as_bytes(),
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
            return Ok((id, State::DiscoverFailed));
        }

        let spec = swizzle_response_to_bundle(
            &capture_name,
            &endpoint_config_json,
            &image_name,
            &image_tag,
            &discover.1,
        )
        .context("converting discovery response into a bundle")?;

        Ok((id, State::Success { spec }))
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
