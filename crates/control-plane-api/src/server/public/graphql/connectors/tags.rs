use std::collections::HashMap;

use models::Id;

use crate::server::public::graphql::{JsonObject, PgDataLoader, connectors::ConnectorProto};

/// The resolved specification for a connector at a particular image tag.
/// Includes the JSON schemas needed to configure the connector's endpoint
/// and resources.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ConnectorSpec {
    /// The OCI Image tag this spec was resolved from, including the leading `:`. For example `:v1`.
    /// This may differ from the requested tag if the request fell back to the default.
    image_tag: String,
    /// The protocol of the connector (capture or materialization)
    protocol: Option<ConnectorProto>,
    /// URL pointing to the documentation page for this connector
    documentation_url: Option<String>,
    /// Endpoint configuration JSON Schema. Returned as raw JSON because JSON Schema is a recursive format that cannot be meaningfully decomposed into GraphQL fields.
    endpoint_spec_schema: Option<JsonObject>,
    /// Resource configuration JSON Schema. Returned as raw JSON because JSON Schema is a recursive format that cannot be meaningfully decomposed into GraphQL fields.
    resource_spec_schema: Option<JsonObject>,
    /// Whether backfill should be disabled for this connector
    disable_backfill: bool,
    /// The default interval between invocations of a capture using this connector. Formatted as HH:MM:SS. Only applicable to non-streaming (polling) capture connectors.
    default_capture_interval: Option<String>,
}

/// Internal key for the DataLoader, not exposed in GraphQL.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct ConnectorSpecKey(pub Id);

impl async_graphql::dataloader::Loader<ConnectorSpecKey> for PgDataLoader {
    type Value = ConnectorSpec;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[ConnectorSpecKey],
    ) -> Result<HashMap<ConnectorSpecKey, Self::Value>, Self::Error> {
        type JsonRawValue = sqlx::types::Json<Box<serde_json::value::RawValue>>;

        let ids = keys.iter().map(|k| k.0).collect::<Vec<Id>>();
        let rows = sqlx::query!(
            r#"select
              ct.id as "id: Id",
              ct.image_tag,
              ct.protocol as "protocol: ConnectorProto",
              ct.documentation_url,
              ct.endpoint_spec_schema as "endpoint_spec_schema: JsonRawValue",
              ct.resource_spec_schema as "resource_spec_schema: JsonRawValue",
              ct.disable_backfill,
              ct.default_capture_interval::text as "default_capture_interval"
            from unnest($1::flowid[]) as input(id)
            join connector_tags ct on input.id = ct.id
            "#,
            ids as Vec<Id>,
        )
        .fetch_all(&self.0)
        .await
        .map_err(async_graphql::Error::from)?;

        let results_map = rows
            .into_iter()
            .map(|row| {
                let key = ConnectorSpecKey(row.id);

                let val = ConnectorSpec {
                    image_tag: row.image_tag,
                    protocol: row.protocol,
                    documentation_url: row.documentation_url,
                    endpoint_spec_schema: row
                        .endpoint_spec_schema
                        .map(|pg| async_graphql::Json(pg.0)),
                    resource_spec_schema: row
                        .resource_spec_schema
                        .map(|pg| async_graphql::Json(pg.0)),
                    disable_backfill: row.disable_backfill,
                    default_capture_interval: row.default_capture_interval,
                };
                (key, val)
            })
            .collect();

        Ok(results_map)
    }
}
