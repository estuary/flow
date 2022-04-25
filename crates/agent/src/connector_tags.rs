use super::{jobs, logs, Handler, Id};

use anyhow::Context;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sqlx::types::{Json, Uuid};
use tracing::info;

/// JobStatus is the possible outcomes of a handled connector tag.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    PullFailed,
    SpecFailed,
    Success,
}

/// A TagHandler is a Handler which evaluates tagged connector images.
pub struct TagHandler {
    connector_network: String,
    flowctl: String,
    logs_tx: logs::Tx,
}

impl TagHandler {
    pub fn new(connector_network: &str, flowctl: &str, logs_tx: &logs::Tx) -> Self {
        Self {
            connector_network: connector_network.to_string(),
            flowctl: flowctl.to_string(),
            logs_tx: logs_tx.clone(),
        }
    }
}

// Row is the dequeued task shape of a tag connector operation.
#[derive(Debug)]
struct Row {
    created_at: DateTime<Utc>,
    id: Id,
    image_name: String,
    image_tag: String,
    logs_token: Uuid,
    updated_at: DateTime<Utc>,
}

#[async_trait::async_trait]
impl Handler for TagHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match sqlx::query_as!(
            Row,
            r#"select
                c.image_name,
                t.created_at,
                t.id as "id: Id",
                t.image_tag,
                t.logs_token,
                t.updated_at
            from connector_tags as t
            join connectors as c on c.id = t.connector_id
            where t.job_status->>'type' = 'queued'
            order by t.id asc
            limit 1
            for update of t skip locked;
            "#
        )
        .fetch_optional(&mut txn)
        .await?
        {
            None => return Ok(std::time::Duration::from_secs(5)),
            Some(row) => row,
        };

        let ProcessedTag {
            id,
            status,
            doc_url,
            endpoint_spec,
            protocol,
            resource_spec,
        } = self.process(row).await?;
        info!(%id, ?status, "finished");

        let r = sqlx::query_unchecked!(
            r#"update connector_tags set
                job_status = $2,
                updated_at = clock_timestamp(),
                -- Remaining fields are null on failure:
                documentation_url = $3,
                endpoint_spec_schema = $4,
                protocol = $5,
                resource_spec_schema = $6
            where id = $1;
            "#,
            id,
            Json(status),
            doc_url,
            Json(endpoint_spec) as Json<Option<Box<RawValue>>>,
            protocol,
            Json(resource_spec) as Json<Option<Box<RawValue>>>,
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

struct ProcessedTag {
    id: Id,
    status: JobStatus,
    doc_url: Option<String>,
    endpoint_spec: Option<Box<RawValue>>,
    protocol: Option<String>,
    resource_spec: Option<Box<RawValue>>,
}

impl ProcessedTag {
    fn failure(id: Id, status: JobStatus) -> Self {
        Self {
            id,
            status,
            doc_url: None,
            endpoint_spec: None,
            protocol: None,
            resource_spec: None,
        }
    }
}

impl TagHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.id))]
    async fn process(&mut self, row: Row) -> anyhow::Result<ProcessedTag> {
        info!(
            %row.image_name,
            %row.created_at,
            %row.image_tag,
            %row.logs_token,
            %row.updated_at,
            "processing connector image tag",
        );
        let image_composed = format!("{}{}", row.image_name, row.image_tag);

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
            return Ok(ProcessedTag::failure(row.id, JobStatus::PullFailed));
        }

        // Fetch its connector specification.
        let spec = jobs::run_with_output(
            "spec",
            &self.logs_tx,
            row.logs_token,
            tokio::process::Command::new(&self.flowctl)
                .arg("api")
                .arg("spec")
                .arg("--image")
                .arg(&image_composed)
                .arg("--network")
                .arg(&self.connector_network),
        )
        .await?;

        if !spec.0.success() {
            return Ok(ProcessedTag::failure(row.id, JobStatus::SpecFailed));
        }

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
        }
        let Spec {
            documentation_url,
            endpoint_spec_schema,
            protocol,
            resource_spec_schema,
        } = serde_json::from_slice(&spec.1).context("parsing connector spec output")?;

        return Ok(ProcessedTag {
            id: row.id,
            status: JobStatus::Success,
            doc_url: Some(documentation_url),
            endpoint_spec: Some(endpoint_spec_schema),
            protocol: Some(protocol),
            resource_spec: Some(resource_spec_schema),
        });
    }
}
