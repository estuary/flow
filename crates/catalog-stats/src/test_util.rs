use crate::{CatalogStats, Client, Grain, pack_name_prefix_range};
use anyhow::Context;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{self as bt, mutation, row_filter};

/// Seeds one row per `(grain, stats)` pair, writing into
/// `catalog_stats_<grain>`.
pub async fn seed_rows(client: &Client, rows: &[(Grain, CatalogStats)]) -> anyhow::Result<()> {
    let mut bt_client = client.client.clone();
    let set_cell = |qualifier: &str, value: &[u8]| bt::Mutation {
        mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
            family_name: crate::COLUMN_FAMILY.to_string(),
            column_qualifier: qualifier.as_bytes().to_vec(),
            timestamp_micros: -1,
            value: value.to_vec(),
        })),
    };

    for (grain, stats) in rows {
        let row_key = crate::pack_row_key(&stats.catalog_name, stats.ts);
        let ts_str = crate::format_ts(stats.ts);
        let flow_document = serde_json::to_vec(stats).context("encoding flow_document")?;

        let mutations = vec![
            set_cell("catalog_name", stats.catalog_name.as_bytes()),
            set_cell("ts", ts_str.as_bytes()),
            set_cell("flow_document", &flow_document),
        ];

        bt_client
            .mutate_row(bt::MutateRowRequest {
                table_name: client.table_name(*grain),
                row_key,
                mutations,
                ..Default::default()
            })
            .await
            .with_context(|| format!("MutateRow for {} {} {grain}", stats.catalog_name, ts_str))?;
    }

    Ok(())
}

/// Deletes every row in each `catalog_stats_<grain>` table.
pub async fn delete_all_rows(client: &Client) -> anyhow::Result<()> {
    delete_matching_rows(client, bt::RowSet::default()).await
}

/// Deletes every row in each `catalog_stats_<grain>` table whose
/// `catalog_name` starts with `prefix`. Empty `prefix` is a no-op (mirrors
/// the public API's empty-prefix guard) — callers that genuinely want a
/// full wipe should use `delete_all_rows`.
pub async fn delete_rows_with_prefix(client: &Client, prefix: &str) -> anyhow::Result<()> {
    let Some(row_range) = pack_name_prefix_range(prefix) else {
        return Ok(());
    };
    delete_matching_rows(
        client,
        bt::RowSet {
            row_keys: vec![],
            row_ranges: vec![row_range],
        },
    )
    .await
}

async fn delete_matching_rows(client: &Client, row_set: bt::RowSet) -> anyhow::Result<()> {
    let mut bt_client = client.client.clone();

    for grain in [Grain::Hourly, Grain::Daily, Grain::Monthly] {
        let table_name = client.table_name(grain);

        let mut stream = bt_client
            .read_rows(bt::ReadRowsRequest {
                table_name: table_name.clone(),
                rows: Some(row_set.clone()),
                filter: Some(bt::RowFilter {
                    filter: Some(row_filter::Filter::StripValueTransformer(true)),
                }),
                ..Default::default()
            })
            .await
            .with_context(|| format!("ReadRows for delete_matching_rows ({grain})"))?
            .into_inner();

        // Only the first chunk of each row carries `row_key`; later chunks
        // for the same row leave it empty.
        while let Some(message) = stream.message().await.context("ReadRows stream error")? {
            for chunk in message.chunks {
                if chunk.row_key.is_empty() {
                    continue;
                }
                bt_client
                    .mutate_row(bt::MutateRowRequest {
                        table_name: table_name.clone(),
                        row_key: chunk.row_key,
                        mutations: vec![bt::Mutation {
                            mutation: Some(mutation::Mutation::DeleteFromRow(
                                mutation::DeleteFromRow {},
                            )),
                        }],
                        ..Default::default()
                    })
                    .await
                    .context("MutateRow DeleteFromRow")?;
            }
        }
    }

    Ok(())
}
