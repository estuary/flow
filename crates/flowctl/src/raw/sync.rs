use crate::{CliContext, ops::TaskSelector};

/// Force every shard of a task to immediately commit its open transaction
/// ("sync now"), blocking until each shard's forced transaction commits.
pub async fn do_sync_now(ctx: &mut CliContext, selector: &TaskSelector) -> anyhow::Result<()> {
    let task_name = &selector.task;

    // Sync-now forces the task to commit its pending writes, so it requires a
    // Write capability (which maps to the gazette APPEND capability enforced by
    // the reactor's SyncNow handler).
    let (shard_id_prefix, _, _, shard_client, _journal_client) =
        crate::dataplane::user_task_authorization(
            &ctx.rest,
            &ctx.user_tokens,
            &ctx.router,
            task_name,
            models::Capability::Write,
        )
        .await?;

    // Select the task's shards by their id prefix (as `raw list-shards` does).
    let req = proto_gazette::consumer::ListRequest {
        selector: Some(proto_gazette::LabelSelector {
            include: Some(proto_gazette::LabelSet {
                labels: vec![proto_gazette::Label {
                    name: "id".to_string(),
                    value: shard_id_prefix.clone(),
                    prefix: true,
                }],
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    let summary = gazette::shard::sync_task_shards(&shard_client, req).await?;

    let rows = summary
        .synced
        .into_iter()
        .map(|id| SyncRow {
            shard: id,
            result: "Synced".to_string(),
            detail: String::new(),
        })
        .chain(summary.failed.into_iter().map(|(id, reason)| SyncRow {
            shard: id,
            result: "Failed".to_string(),
            detail: reason,
        }));
    ctx.write_all(rows, ())?;

    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct SyncRow {
    shard: String,
    result: String,
    detail: String,
}

impl crate::output::CliOutput for SyncRow {
    type TableAlt = ();
    type CellValue = String;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Shard", "Result", "Detail"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        vec![self.shard, self.result, self.detail]
    }
}
