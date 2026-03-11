use std::collections::HashMap;

use chrono::{DateTime, Utc};
use models::Id;

use crate::server::public::graphql::{JsonObject, PgDataLoader, connectors::ConnectorProto};

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct ConnectorTag {
    /// The primary key in the connector_tags table. Not exposed in the graphql
    /// schema, only because we don't need it for anything at the moment.
    #[graphql(skip)]
    #[allow(unused)]
    id: Id,
    /// The id of the connector this tag relates to. Not exposed in the graphql
    /// schema, only because we don't need it for anything at the moment.
    #[graphql(skip)]
    #[allow(unused)]
    connector_id: Id,

    /// The OCI Image tag value, including the leading `:`. For example `:v1`
    image_tag: String,

    /// The protocol of the connector with this tag value
    protocol: Option<ConnectorProto>,
    /// Time at which the ConnectorTag was created
    created_at: DateTime<Utc>,
    /// Time at which the ConnectorTag was last updated
    updated_at: DateTime<Utc>,
    /// URL pointing to the documentation page for this connector
    documentation_url: Option<String>,
    /// Endpoint specification JSON-Schema of the tagged connector
    endpoint_spec_schema: Option<JsonObject>,
    /// Resource specification JSON-Schema of the tagged connector
    resource_spec_schema: Option<JsonObject>,
    /// Whether the UI should hide the backfill button for this connector
    disable_backfill: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectorTagKey {
    pub connector_id: models::Id,
    pub image_tag: String,
}

impl async_graphql::dataloader::Loader<ConnectorTagKey> for PgDataLoader {
    type Value = ConnectorTag;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[ConnectorTagKey],
    ) -> Result<HashMap<ConnectorTagKey, Self::Value>, Self::Error> {
        type JsonRawValue = sqlx::types::Json<Box<serde_json::value::RawValue>>;

        let ids = keys.iter().map(|k| k.connector_id).collect::<Vec<Id>>();
        let tags = keys
            .iter()
            .map(|k| k.image_tag.as_str())
            .collect::<Vec<&'_ str>>();
        let rows = sqlx::query!(
            r#"select
              ct.id as "id: Id",
              ct.image_tag,
              ct.connector_id as "connector_id: Id",
              ct.protocol as "protocol: ConnectorProto",
              ct.created_at,
              ct.updated_at,
              ct.documentation_url,
              ct.endpoint_spec_schema as "endpoint_spec_schema: JsonRawValue",
              ct.resource_spec_schema as "resource_spec_schema: JsonRawValue",
              ct.disable_backfill
            from unnest($1::flowid[], $2::text[]) as input(connector_id, image_tag)
            join connector_tags ct on input.connector_id = ct.connector_id and input.image_tag = ct.image_tag
            "#,
            ids as Vec<Id>,
            tags as Vec<&str>,
        ).fetch_all(&self.0)
        .await
        .map_err(async_graphql::Error::from)?;

        let results_map = rows
            .into_iter()
            .map(|row| {
                let key = ConnectorTagKey {
                    connector_id: row.connector_id,
                    image_tag: row.image_tag.clone(),
                };

                let val = ConnectorTag {
                    id: row.id,
                    image_tag: row.image_tag,
                    connector_id: row.connector_id,
                    protocol: row.protocol,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    documentation_url: row.documentation_url,
                    endpoint_spec_schema: row
                        .endpoint_spec_schema
                        .map(|pg| async_graphql::Json(pg.0)),
                    resource_spec_schema: row
                        .resource_spec_schema
                        .map(|pg| async_graphql::Json(pg.0)),
                    disable_backfill: row.disable_backfill,
                };
                (key, val)
            })
            .collect();

        Ok(results_map)
    }
}
