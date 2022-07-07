use super::{jobs, logs, Handler, Id};

use agent_sql::connector_tags::Row;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tracing::info;

/// JobStatus is the possible outcomes of a handled connector tag.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    PullFailed,
    SpecFailed,
    OpenGraphFailed { error: String },
    Success,
}

/// A TagHandler is a Handler which evaluates tagged connector images.
pub struct TagHandler {
    connector_network: String,
    bindir: String,
    logs_tx: logs::Tx,
}

impl TagHandler {
    pub fn new(connector_network: &str, bindir: &str, logs_tx: &logs::Tx) -> Self {
        Self {
            connector_network: connector_network.to_string(),
            bindir: bindir.to_string(),
            logs_tx: logs_tx.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Handler for TagHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match agent_sql::connector_tags::dequeue(&mut txn).await? {
            None => return Ok(std::time::Duration::from_secs(5)),
            Some(row) => row,
        };

        let (id, status) = self.process(row, &mut txn).await?;
        info!(%id, ?status, "finished");

        agent_sql::connector_tags::resolve(id, status, &mut txn).await?;
        txn.commit().await?;

        Ok(std::time::Duration::ZERO)
    }
}

/// This tag is used for local development of connectors. Any images having this tag will not be
/// pulled from a registry, so that developers can simply `docker build` and then update
/// connector_tags without having to push to a registry.
pub const LOCAL_IMAGE_TAG: &str = ":local";

impl TagHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.tag_id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<(Id, JobStatus)> {
        info!(
            %row.image_name,
            %row.created_at,
            %row.image_tag,
            %row.logs_token,
            %row.updated_at,
            "processing connector image tag",
        );
        let image_composed = format!("{}{}", row.image_name, row.image_tag);

        if row.image_tag != LOCAL_IMAGE_TAG {
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
                return Ok((row.tag_id, JobStatus::PullFailed));
            }
        }

        // Fetch its connector specification.
        let spec = jobs::run_with_output(
            "spec",
            &self.logs_tx,
            row.logs_token,
            tokio::process::Command::new(format!("{}/flowctl-go", &self.bindir))
                .arg("api")
                .arg("spec")
                .arg("--image")
                .arg(&image_composed)
                .arg("--network")
                .arg(&self.connector_network),
        )
        .await?;

        if !spec.0.success() {
            return Ok((row.tag_id, JobStatus::SpecFailed));
        }

        let fetch_open_graph =
            tokio::process::Command::new(format!("{}/fetch-open-graph", &self.bindir))
                .kill_on_drop(true)
                .arg("-url")
                .arg(&row.external_url)
                .output()
                .await
                .context("fetching open graph metadata")?;

        if !fetch_open_graph.status.success() {
            return Ok((
                row.tag_id,
                JobStatus::OpenGraphFailed {
                    error: String::from_utf8_lossy(&fetch_open_graph.stderr).into(),
                },
            ));
        }
        let open_graph_raw: Box<RawValue> = serde_json::from_slice(&fetch_open_graph.stdout)
            .context("parsing open graph response")?;

        agent_sql::connector_tags::update_open_graph_raw(row.connector_id, open_graph_raw, txn)
            .await?;

        /// Spec is the output shape of the `flowctl api spec` command.
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Spec {
            #[serde(rename = "documentationURL")]
            documentation_url: String,
            endpoint_spec_schema: Box<RawValue>,
            #[serde(rename = "type")]
            protocol: String,
            resource_spec_schema: Box<RawValue>,
            oauth2_spec: Box<RawValue>,
        }
        let Spec {
            documentation_url,
            endpoint_spec_schema,
            protocol,
            resource_spec_schema,
            oauth2_spec,
        } = serde_json::from_slice(&spec.1).context("parsing connector spec output")?;

        agent_sql::connector_tags::update_tag_fields(
            row.tag_id,
            documentation_url,
            endpoint_spec_schema,
            protocol,
            resource_spec_schema,
            txn,
        )
        .await?;

        agent_sql::connector_tags::update_oauth2_spec(
            row.connector_id,
            oauth2_spec,
            txn,
        ).await?;

        return Ok((row.tag_id, JobStatus::Success));
    }
}
