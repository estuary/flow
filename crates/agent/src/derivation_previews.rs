use super::{
    connector_tags::LOCAL_IMAGE_TAG, draft, jobs, logs, CatalogType, Handler, HandlerStatus, Id,
};
use agent_sql::derivation_previews::Row;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;
use tempdir::TempDir;
use journal_client::append;

/// JobStatus is the possible outcomes of a handled derivation preview operation.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum JobStatus {
    Queued,
    PreviewFailed,
    PersistFailed,
    Success {
        journal_name: String,
    },
}

/// A DerivationPreview is a Handler which performs discovery operations.
pub struct DerivationPreviewHandler {
    broker_address: String,
    bindir: String,
    logs_tx: logs::Tx,
}

impl DerivationPreviewHandler {
    pub fn new(broker_address: &str, bindir: &str, logs_tx: &logs::Tx) -> Self {
        Self {
            broker_address: broker_address.to_string(),
            bindir: bindir.to_string(),
            logs_tx: logs_tx.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Handler for DerivationPreviewHandler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<HandlerStatus> {
        let mut txn = pg_pool.begin().await?;

        let row: Row = match agent_sql::derivation_previews::dequeue(&mut txn).await? {
            None => return Ok(HandlerStatus::Idle),
            Some(row) => row,
        };

        let (id, status) = self.process(row, &mut txn).await?;
        tracing::info!(%id, ?status, "finished");

        agent_sql::derivation_previews::resolve(id, status, &mut txn).await?;
        txn.commit().await?;

        Ok(HandlerStatus::Active)
    }

    fn table_name(&self) -> &'static str {
        "discovers"
    }
}

impl DerivationPreviewHandler {
    #[tracing::instrument(err, skip_all, fields(id=?row.id))]
    async fn process(
        &mut self,
        row: Row,
        txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<(Id, JobStatus)> {
        tracing::info!(
            %row.created_at,
            %row.collection_name,
            %row.draft_id,
            %row.updated_at,
            %row.logs_token,
            %row.num_documents,
            "processing derivation preview",
        );

        // Remove draft errors from a previous attempt.
        agent_sql::drafts::delete_errors(row.draft_id, txn)
            .await
            .context("clearing old errors")?;

        // FIXME: at the time of writing this, there is only a single task processed by an agent process
        // at a time, but note that this does not work if multiple tasks are handled by the same agent process.
        jobs::run(
            "derivation_preview_draft_select",
            &self.logs_tx,
            row.logs_token,
            async_process::Command::new(format!("{}/flowctl", &self.bindir))
                .arg("draft")
                .arg("select")
                .arg("--id")
                .arg(&row.draft_id.to_string())
        ).await?;

        let tmp_dir = tempdir::TempDir::new(&format!("derivation-preview-{}", row.id))?;

        jobs::run(
            "derivation_preview_draft_develop",
            &self.logs_tx,
            row.logs_token,
            async_process::Command::new(format!("{}/flowctl", &self.bindir))
                .arg("draft")
                .arg("develop")
                .arg("--target")
                .arg(&tmp_dir.path())
        ).await?;

        let (derivation_preview, output) = jobs::run_with_input_output_lines(
            "derivation_preview",
            &self.logs_tx,
            row.logs_token,
            &[][..],
            row.num_documents as usize,
            async_process::Command::new(format!("{}/flowctl", &self.bindir))
                .arg("preview")
                .arg("--source")
                .arg(&tmp_dir.path())
                .arg("--collection")
                .arg(&row.collection_name)
        )
        .await?;

        if !derivation_preview.success() {
            let error = draft::Error {
                catalog_name: row.collection_name,
                scope: None,
                detail: output.iter()
                    .map(|v| String::from_utf8(*v).context("derivation preview error output is not UTF-8"))
                    .collect::<anyhow::Result<Vec<String>>>()?.join(","),
            };
            draft::insert_errors(row.draft_id, vec![error], txn).await?;

            return Ok((row.id, JobStatus::PreviewFailed));
        }

        let mut client = journal_client::connect_journal_client(
            self.broker_address,
            bearer_token,
        ).await?;

        let journal_name = format!("preview/{}", row.id);

        // Write derivation_preview_output to a gazette journal
        for line in output {
            append(&mut client, journal_name, line).await?;
        }

        Ok((row.id, JobStatus::Success { journal_name }))
    }
}

#[cfg(test)]
mod test {
}
