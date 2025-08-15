use std::collections::HashSet;

use anyhow::Context;
use gazette::{
    broker::{apply_request::Change, journal_spec::suspend},
    journal,
};
use itertools::Itertools;
use labels::percent_encoding;
use proto_gazette::broker;

use crate::collection::CollectionJournalSelector;

#[derive(clap::Args, Debug)]
pub struct CleanupOpsJournals {
    #[clap(long, short)]
    pub for_real: bool,
}

const EXISTING_TASKS_JSON: &str = include_str!("/Users/phil/projects/flow/existing_tasks.json");

#[derive(serde::Deserialize, Debug)]
pub struct ExistingTask {
    catalog_name: String,
}

impl CleanupOpsJournals {
    pub async fn cleanup_ops_journals(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let existing_tasks: Vec<ExistingTask> = serde_json::from_str(EXISTING_TASKS_JSON).unwrap();
        if existing_tasks.is_empty() {
            anyhow::bail!("no existing tasks");
        }
        tracing::warn!(existing_task_count = %existing_tasks.len(), "Cleaning up ops journals");

        let existing_tasks = existing_tasks
            .into_iter()
            .map(|t| t.catalog_name)
            .collect::<HashSet<_>>();

        let deleted_logs = self
            .cleanup_ops_journals_inner(ctx, "ops.us-central1.v1/logs", &existing_tasks)
            .await?;
        let deleted_stats = self
            .cleanup_ops_journals_inner(ctx, "ops.us-central1.v1/stats", &existing_tasks)
            .await?;
        tracing::warn!(deleted_logs = %deleted_logs, deleted_stats = %deleted_stats, "Finished cleaning up logs and stats journals");
        Ok(())
    }

    pub async fn cleanup_ops_journals_inner(
        &self,
        ctx: &mut crate::CliContext,
        collection: &str,
        existing_tasks: &HashSet<String>,
    ) -> anyhow::Result<i32> {
        let (journal_name_prefix, client) =
            flow_client::fetch_user_collection_authorization(&ctx.client, collection, true).await?;

        let selector = CollectionJournalSelector {
            collection: collection.to_string(),
            partitions: None,
        };
        let label_selector = selector.build_label_selector(journal_name_prefix);
        let list_reps = client
            .list(broker::ListRequest {
                selector: Some(label_selector),
                ..Default::default()
            })
            .await?;

        let task_name_label = format!("{}name", ::labels::FIELD_PREFIX);
        let mut changes_iter = list_reps
            .journals
            .into_iter()
            .filter_map(|journal| {
                let Some(spec) = journal.spec else {
                    return None;
                };
                let Some(labels) = spec.labels.as_ref() else {
                    tracing::warn!("journal missing labels");
                    return None;
                };

                let task_values = ::labels::values(labels, &task_name_label);
                if task_values.len() != 1 {
                    tracing::warn!(values = %task_values.len(), "task values != 1");
                    return None;
                }
                let task_name = task_values.first().unwrap().value.as_str();
                let task_name = ::percent_encoding::percent_decode(task_name.as_bytes())
                    .decode_utf8()
                    .expect("failed to decode task name")
                    .into_owned();
                if existing_tasks.contains(&task_name) {
                    tracing::warn!(%task_name, journal= %spec.name, "task exists, keeping journal");
                    return None;
                }

                let Some(suspend) = spec.suspend.as_ref() else {
                    tracing::warn!("journal not suspended");
                    return None;
                };
                if suspend.level != suspend::Level::Full as i32 {
                    tracing::warn!("suspend level != full");
                    return None;
                }

                Some(broker::apply_request::Change {
                    expect_mod_revision: journal.mod_revision,
                    upsert: None,
                    delete: spec.name,
                })
            })
            .peekable();

        let mut total_deleted = 0;
        let batch_size = 120;
        let mut batch = Vec::with_capacity(batch_size);
        while let Some(change) = changes_iter.next() {
            total_deleted += 1;
            tracing::warn!(expect_mod_revision = change.expect_mod_revision, journal = %change.delete, for_real = self.for_real, "deleting journal");
            if !self.for_real {
                continue;
            }
            batch.push(change);
            if batch.len() == batch_size || changes_iter.peek().is_none() {
                tracing::info!(%total_deleted, "Sending apply request for batch");
                let changes = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                let _req = broker::ApplyRequest { changes };
                /*
                //client.apply(req).await.context("apply request failed")?;
                 */
                tracing::info!(%total_deleted, "Finished batch");
            }
        }
        tracing::warn!(%total_deleted, "Finished deleting {collection} journals");

        Ok(total_deleted)
    }
}
