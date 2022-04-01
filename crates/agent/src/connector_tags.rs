use super::{jobs, logs, Handler, Id};

use anyhow::Context;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

/// JobStatus is the possible outcomes of a handled connector tag.
#[derive(Deserialize, Serialize)]
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

#[async_trait::async_trait]
impl Handler for TagHandler {
    type Error = anyhow::Error;

    fn dequeue() -> &'static str {
        r#"select
            c.image_name,
            t.created_at,
            t.id,
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
    }

    fn update() -> &'static str {
        r#"update connector_tags set
            job_status = $2::text::jsonb,
            updated_at = clock_timestamp(),
            -- Remaining fields are null on failure:
            documentation_url = $3,
            endpoint_spec_schema = $4::json,
            protocol = $5
        where id = $1;
        "#
    }

    #[tracing::instrument(ret, skip_all, fields(connector_tag = %row.get::<_, Id>(2)))]
    async fn on_dequeue(
        &mut self,
        txn: &mut tokio_postgres::Transaction,
        row: tokio_postgres::Row,
        update: &tokio_postgres::Statement,
    ) -> Result<u64, Self::Error> {
        let (id, status, doc_url, spec, protocol) = self.process(row).await?;

        let status = serde_json::to_string(&status).unwrap();
        info!(%id, %status, "finished");

        Ok(txn
            .execute(update, &[&id, &status, &doc_url, &spec, &protocol])
            .await?)
    }
}

impl TagHandler {
    #[tracing::instrument(err, skip_all)]
    async fn process(
        &mut self,
        row: tokio_postgres::Row,
    ) -> Result<
        (
            Id,
            JobStatus,
            Option<String>,
            Option<serde_json::Value>,
            Option<String>,
        ),
        anyhow::Error,
    > {
        let (image_name, created_at, id, image_tag, logs_token, updated_at) = (
            row.get::<_, String>(0),
            row.get::<_, DateTime<Utc>>(1),
            row.get::<_, Id>(2),
            row.get::<_, String>(3),
            row.get::<_, uuid::Uuid>(4),
            row.get::<_, DateTime<Utc>>(5),
        );
        info!(
            %image_name,
            %created_at,
            %image_tag,
            %logs_token,
            %updated_at,
            "processing connector image tag"
        );
        let image_composed = format!("{}{}", image_name, image_tag);

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
            return Ok((id, JobStatus::PullFailed, None, None, None));
        }

        // Fetch its connector specification.
        let spec = jobs::run_with_output(
            "spec",
            &self.logs_tx,
            logs_token,
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
            return Ok((id, JobStatus::SpecFailed, None, None, None));
        }

        /// Spec is the output shape of the `flowctl api spec` command.
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Spec {
            #[serde(rename = "documentationURL")]
            documentation_url: String,
            endpoint_spec_schema: serde_json::Value,
            #[serde(rename = "type")]
            protocol: String,
        }
        let Spec {
            documentation_url,
            endpoint_spec_schema,
            protocol,
        } = serde_json::from_slice(&spec.1).context("parsing connector spec output")?;

        return Ok((
            id,
            JobStatus::Success,
            Some(documentation_url),
            Some(endpoint_spec_schema),
            Some(protocol),
        ));
    }
}
