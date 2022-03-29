use super::{jobs, logs, Handler, Id};

use anyhow::Context;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

/// State is the possible states of a connector image,
/// serialized as the `connector_images.state` column.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum State {
    Queued,
    PullFailed,
    SpecFailed,
    Success { spec: serde_json::Value },
}

/// A SpecHandler is a Handler which fetches connector specifications.
pub struct SpecHandler {
    connector_network: String,
    flowctl: String,
    logs_tx: logs::Tx,
}

impl SpecHandler {
    pub fn new(connector_network: &str, flowctl: &str, logs_tx: &logs::Tx) -> Self {
        Self {
            connector_network: connector_network.to_string(),
            flowctl: flowctl.to_string(),
            logs_tx: logs_tx.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Handler for SpecHandler {
    type Error = anyhow::Error;

    fn dequeue() -> &'static str {
        r#"SELECT
            c.image,
            i.created_at,
            i.id,
            i.logs_token,
            i.tag,
            i.updated_at
        FROM connector_images AS i
        JOIN connectors AS c ON c.id = i.connector_id
        WHERE i.state->>'type' = 'queued'
        ORDER BY i.id ASC
        LIMIT 1
        FOR UPDATE OF i SKIP LOCKED;
        "#
    }

    fn update() -> &'static str {
        "UPDATE connector_images SET state = $2::text::jsonb, updated_at = clock_timestamp() WHERE id = $1;"
    }

    #[tracing::instrument(ret, skip_all, fields(connector_image = %row.get::<_, Id>(2)))]
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

impl SpecHandler {
    #[tracing::instrument(err, skip_all)]
    async fn process(&mut self, row: tokio_postgres::Row) -> Result<(Id, State), anyhow::Error> {
        let (image, created_at, id, logs_token, tag, updated_at) = (
            row.get::<_, String>(0),
            row.get::<_, DateTime<Utc>>(1),
            row.get::<_, Id>(2),
            row.get::<_, uuid::Uuid>(3),
            row.get::<_, String>(4),
            row.get::<_, DateTime<Utc>>(5),
        );

        info!(%image, %created_at, %id, %logs_token, %tag, %updated_at, "processing connector image");
        let image = format!("{}{}", image, tag);

        // Pull the image.
        let pull = jobs::run(
            "pull",
            &self.logs_tx,
            logs_token,
            tokio::process::Command::new("docker")
                .arg("pull")
                .arg(&image),
        )
        .await?;

        if !pull.success() {
            return Ok((id, State::PullFailed));
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
                .arg(&image)
                .arg("--network")
                .arg(&self.connector_network),
        )
        .await?;

        if !spec.0.success() {
            return Ok((id, State::SpecFailed));
        }

        let spec = serde_json::from_slice(&spec.1).context("parsing connector spec output")?;
        return Ok((id, State::Success { spec }));
    }
}
