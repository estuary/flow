use crate::server::public::graphql::PgDataLoader;
use async_graphql::{Context, connection};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(async_graphql::SimpleObject, Clone, Debug)]
#[graphql(complex)]
pub struct SpecPublicationHistoryItem {
    /// The id of the publication
    pub publication_id: models::Id,
    /// Type of the published catalog specification, if recorded.
    /// This may be null for a deletion.
    pub catalog_type: Option<models::CatalogType>,
    /// Timestamp of the publication
    pub published_at: DateTime<Utc>,
    /// The id of the user who created the publication
    pub user_id: uuid::Uuid,
    /// The email of the user who created the publication, if known
    pub user_email: Option<String>,
    /// The full name of the user who created the publication, if known
    pub user_full_name: Option<String>,
    /// The URL of an avatar image for the user who created the publication, if known
    pub user_avatar_url: Option<String>,
    /// Description of the publication, including any automated model updates
    /// performed as part of the publication
    pub detail: Option<String>,
    #[graphql(skip)]
    pub model: Option<models::RawValue>,
}

#[async_graphql::ComplexObject]
impl SpecPublicationHistoryItem {
    /// Catalog specification published by this publication, or null for a deletion.
    pub async fn model<'a>(&'a self) -> Option<async_graphql::Json<&'a models::RawValue>> {
        self.model.as_ref().map(|model| async_graphql::Json(model))
    }
}

/// Key for loading a publication of a given spec.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublicationInfoKey {
    pub catalog_name: models::Name,
    /// None selects the most recent publication.
    pub publication_id: Option<models::Id>,
    pub include_model: bool,
}

impl async_graphql::dataloader::Loader<PublicationInfoKey> for PgDataLoader {
    type Value = SpecPublicationHistoryItem;

    type Error = String;

    async fn load(
        &self,
        keys: &[PublicationInfoKey],
    ) -> Result<HashMap<PublicationInfoKey, Self::Value>, Self::Error> {
        let names: Vec<&'_ str> = keys.iter().map(|n| n.catalog_name.as_str()).collect();
        let publication_ids: Vec<Option<models::Id>> =
            keys.iter().map(|k| k.publication_id).collect();
        let include_models: Vec<bool> = keys.iter().map(|k| k.include_model).collect();
        let rows = sqlx::query!(
            r#"select
                ls.catalog_name as "catalog_name!: models::Name",
                args.publication_id as "requested_publication_id: models::Id",
                args.include_model as "include_model!: bool",
                ps.pub_id as "publication_id!: models::Id",
                ps.spec_type as "catalog_type: models::CatalogType",
                ps.published_at as "published_at!: DateTime<Utc>",
                ps.user_id as "user_id!: uuid::Uuid",
                u.email as "user_email: String",
                u.raw_user_meta_data->>'picture' as "user_avatar_url: String",
                u.raw_user_meta_data->>'full_name' as "user_full_name: String",
                ps.detail as "detail: String",
                case when args.include_model then ps.spec else null end as "model: models::RawValue"
              from unnest($1::catalog_name[], $2::flowid[], $3::boolean[]) as args(name, publication_id, include_model)
              join live_specs ls on args.name = ls.catalog_name
              join publication_specs ps on ls.id = ps.live_spec_id and ps.pub_id = coalesce(args.publication_id, ls.last_pub_id)
              left outer join auth.users u on ps.user_id = u.id
            "#,
            &names as &[&str],
            &publication_ids as &[Option<models::Id>],
            &include_models as &[bool]
        )
        .fetch_all(&self.0)
        .await
        .map_err(|e| format!("failed to fetch publication info: {e}"))?;

        let results = rows
            .into_iter()
            .map(|row| {
                let key = PublicationInfoKey {
                    catalog_name: row.catalog_name,
                    publication_id: row.requested_publication_id,
                    include_model: row.include_model,
                };
                let val = SpecPublicationHistoryItem {
                    publication_id: row.publication_id,
                    catalog_type: row.catalog_type,
                    published_at: row.published_at,
                    user_id: row.user_id,
                    user_email: row.user_email,
                    user_avatar_url: row.user_avatar_url,
                    user_full_name: row.user_full_name,
                    detail: row.detail,
                    model: row.model,
                };
                (key, val)
            })
            .collect();
        Ok(results)
    }
}

use super::TimestampCursor;

pub type SpecHistoryConnection = async_graphql::connection::Connection<
    TimestampCursor,
    SpecPublicationHistoryItem,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

/// Fetches the publication history for a given live spec, **without performing
/// any authorization checks**.
pub async fn fetch_spec_history_no_authz(
    ctx: &Context<'_>,
    catalog_name: models::Name,
    include_model: bool,
    after: Option<String>,
    first: Option<i32>,
    before: Option<String>,
    last: Option<i32>,
) -> async_graphql::Result<SpecHistoryConnection> {
    const DEFAULT_PAGE_SIZE: usize = 10;

    let env = ctx.data::<crate::Envelope>()?;

    connection::query_with::<TimestampCursor, _, _, _, async_graphql::Error>(
        after,
        before,
        first,
        last,
        |after, before, first, last| async move {
            let (nodes, has_prev, has_next) = if before.is_some() || last.is_some() {
                let (rows, has_prev) = fetch_spec_history_before(
                    catalog_name.as_str(),
                    include_model,
                    before
                        .map(|c| c.0)
                        .unwrap_or(tokens::now() + chrono::Duration::minutes(5)),
                    last.unwrap_or(DEFAULT_PAGE_SIZE),
                    &env.pg_pool,
                )
                .await
                .map_err(async_graphql::Error::from)?;
                (rows, has_prev, false)
            } else {
                let (rows, has_next) = fetch_spec_history_after(
                    catalog_name.as_str(),
                    include_model,
                    after
                        .map(|c| c.0)
                        .unwrap_or_else(|| "2020-01-01T00:00:00Z".parse().unwrap()),
                    first.unwrap_or(DEFAULT_PAGE_SIZE),
                    &env.pg_pool,
                )
                .await
                .map_err(async_graphql::Error::from)?;
                (rows, false, has_next)
            };

            let edges = nodes
                .into_iter()
                .map(|node| {
                    async_graphql::connection::Edge::new(TimestampCursor(node.published_at), node)
                })
                .collect();
            let mut conn = SpecHistoryConnection::new(has_prev, has_next);
            conn.edges = edges;
            async_graphql::Result::Ok(conn)
        },
    )
    .await
}

async fn fetch_spec_history_before(
    catalog_name: &str,
    include_model: bool,
    before: DateTime<Utc>,
    last: usize,
    pool: &sqlx::PgPool,
) -> sqlx::Result<(Vec<SpecPublicationHistoryItem>, bool)> {
    let limit = last as i64 + 1;
    let mut rows = sqlx::query_as!(
        SpecPublicationHistoryItem,
        r#"select
            ps.pub_id as "publication_id: models::Id",
            ps.spec_type as "catalog_type: models::CatalogType",
            ps.published_at as "published_at: DateTime<Utc>",
            ps.user_id as "user_id: uuid::Uuid",
            u.email as "user_email: String",
            u.raw_user_meta_data->>'picture' as "user_avatar_url: String",
            u.raw_user_meta_data->>'full_name' as "user_full_name: String",
            ps.detail as "detail: String",
            case when $2::boolean then ps.spec else null end as "model: models::RawValue"
          from live_specs ls
          join publication_specs ps on ls.id = ps.live_spec_id
          left outer join auth.users u on ps.user_id = u.id
          where ls.catalog_name = $1::catalog_name
            and ps.published_at < $3::timestamptz
          order by ps.published_at desc
          limit $4
          "#,
        catalog_name as &str,
        include_model,
        before,
        limit,
    )
    .fetch_all(pool)
    .await?;

    let has_prev = rows.len() > last;
    if has_prev {
        rows.pop();
    }
    rows.reverse();
    Ok((rows, has_prev))
}

async fn fetch_spec_history_after(
    catalog_name: &str,
    include_model: bool,
    after: DateTime<Utc>,
    first: usize,
    pool: &sqlx::PgPool,
) -> sqlx::Result<(Vec<SpecPublicationHistoryItem>, bool)> {
    let limit = first as i64 + 1;
    let mut rows = sqlx::query_as!(
        SpecPublicationHistoryItem,
        r#"select
            ps.pub_id as "publication_id: models::Id",
            ps.spec_type as "catalog_type: models::CatalogType",
            ps.published_at as "published_at: DateTime<Utc>",
            ps.user_id as "user_id: uuid::Uuid",
            u.email as "user_email: String",
            u.raw_user_meta_data->>'picture' as "user_avatar_url: String",
            u.raw_user_meta_data->>'full_name' as "user_full_name: String",
            ps.detail as "detail: String",
            case when $2::boolean then ps.spec else null end as "model: models::RawValue"
          from live_specs ls
          join publication_specs ps on ls.id = ps.live_spec_id
          left outer join auth.users u on ps.user_id = u.id
          where ls.catalog_name = $1::catalog_name
            and ps.published_at > $3::timestamptz
          order by ps.published_at asc
          limit $4
          "#,
        catalog_name as &str,
        include_model,
        after,
        limit,
    )
    .fetch_all(pool)
    .await?;

    let has_next = rows.len() > first;
    if has_next {
        rows.pop();
    }
    // keep rows in ascending order
    Ok((rows, has_next))
}

#[cfg(test)]
mod test {
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn publication_for_id(pool: sqlx::PgPool) {
        let alice = uuid::Uuid::from_bytes([0x11; 16]);
        const MODEL: &str = r#"{"schema":{"type":"object","description":"old"},"key":[]}"#;

        sqlx::query(
            "INSERT INTO publication_specs (live_spec_id, pub_id, published_at, spec, spec_type, user_id)
             SELECT id, p.pub_id::flowid, p.ts::timestamptz, $1::json, spec_type, $2
             FROM live_specs CROSS JOIN (VALUES
                 ('0000000000000010', '2024-01-01'),
                 ('0000000000000011', '2024-01-01')
             ) p(pub_id, ts) WHERE catalog_name = 'aliceCo/data/foo'",
        )
        .bind(MODEL)
        .bind(alice)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO publication_specs (live_spec_id, pub_id, spec, spec_type, user_id)
             SELECT id, '0000000000000012', spec, spec_type, $1 FROM live_specs
             WHERE catalog_name = 'aliceCo/in/capture-foo'",
        )
        .bind(alice)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO publication_specs (live_spec_id, pub_id, published_at, spec, spec_type, user_id)
             SELECT id, '0000000000000013', '2024-02-01', NULL, NULL, $1 FROM live_specs
             WHERE catalog_name = 'aliceCo/data/foo'",
        )
        .bind(alice)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE live_specs SET spec = NULL, spec_type = NULL, last_pub_id = '0000000000000013'
             WHERE catalog_name = 'aliceCo/data/foo'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let server = crate::test_server::TestServer::start(
            pool.clone(),
            crate::test_server::snapshot(pool, false).await,
        )
        .await;
        let token = server.make_access_token(alice, None);
        const QUERY: &str = r#"
            query($name: Name!, $publicationId: Id!) {
                liveSpecs(by: { names: [$name] }) {
                    edges { node {
                        liveSpec { catalogType }
                        lastPublication { publicationId catalogType }
                        withModel: lastPublication { publicationId catalogType model }
                        publicationForId(id: $publicationId) { publicationId catalogType model }
                        withoutModel: publicationForId(id: $publicationId) { publicationId catalogType }
                        previous: publicationForId(id: "0000000000000010") { publicationId catalogType model }
                        publicationHistory {
                            edges { node { publicationId catalogType model } }
                            pageInfo { hasNextPage hasPreviousPage }
                        }
                        reverseHistory: publicationHistory(last: 10) {
                            edges { node { publicationId catalogType model } }
                        }
                    } }
                }
            }
        "#;

        // Aliases exercise distinct IDs and model selections in the same loader.
        // The lookup is exact even when two revisions share a timestamp.
        for (publication_id, found) in [
            ("0000000000000011", true),
            ("0000000000000013", true),
            ("0000000000000012", false),
            ("0000000000000099", false),
        ] {
            let raw: Box<serde_json::value::RawValue> = server
                .graphql(
                    &serde_json::json!({"query": QUERY, "variables": {
                        "name": "aliceCo/data/foo", "publicationId": publication_id,
                    }}),
                    Some(&token),
                )
                .await;
            let response: serde_json::Value = serde_json::from_str(raw.get()).unwrap();
            assert!(response.get("errors").is_none(), "{response}");
            let node = &response["data"]["liveSpecs"]["edges"][0]["node"];
            assert!(node["liveSpec"].is_null());
            assert_eq!(
                node["lastPublication"],
                serde_json::json!({
                    "publicationId": "0000000000000013", "catalogType": null,
                })
            );
            assert_eq!(
                node["withModel"],
                serde_json::json!({
                    "publicationId": "0000000000000013", "catalogType": null, "model": null,
                })
            );
            let mut expected = serde_json::Value::Null;
            if found {
                expected =
                    serde_json::json!({"publicationId": publication_id, "catalogType": null});
                if publication_id != "0000000000000013" {
                    expected["catalogType"] = "collection".into();
                }
            }
            assert_eq!(node["withoutModel"], expected);
            if found {
                expected["model"] = if publication_id == "0000000000000013" {
                    serde_json::Value::Null
                } else {
                    serde_json::from_str(MODEL).unwrap()
                };
            }
            assert_eq!(node["publicationForId"], expected);
            assert_eq!(
                node["previous"],
                serde_json::json!({
                    "publicationId": "0000000000000010", "catalogType": "collection",
                    "model": serde_json::from_str::<serde_json::Value>(MODEL).unwrap(),
                })
            );
            assert_eq!(
                node["publicationHistory"]["pageInfo"],
                serde_json::json!({"hasNextPage": false, "hasPreviousPage": false})
            );
            for history in ["publicationHistory", "reverseHistory"] {
                let edges = node[history]["edges"].as_array().unwrap();
                let mut actual_ids = edges
                    .iter()
                    .map(|edge| edge["node"]["publicationId"].as_str().unwrap())
                    .collect::<Vec<_>>();
                actual_ids.sort_unstable();
                assert_eq!(
                    actual_ids,
                    ["0000000000000010", "0000000000000011", "0000000000000013"]
                );
                for edge in edges {
                    if edge["node"]["publicationId"] == "0000000000000013" {
                        assert!(edge["node"]["model"].is_null());
                        assert!(edge["node"]["catalogType"].is_null());
                    } else {
                        assert_eq!(edge["node"]["catalogType"], "collection");
                        assert_eq!(
                            edge["node"]["model"],
                            serde_json::from_str::<serde_json::Value>(MODEL).unwrap()
                        );
                    }
                }
            }
            assert!(
                raw.get().contains(MODEL),
                "historical model key order changed: {raw:?}"
            );
        }

        for (name, access_token) in [
            ("ops/tasks/public/one/logs", Some(token.as_str())),
            ("aliceCo/data/foo", None),
        ] {
            let response: serde_json::Value = server.graphql(
                &serde_json::json!({"query": QUERY, "variables": {"name": name, "publicationId": "0000000000000011"}}),
                access_token,
            ).await;
            assert!(response.get("errors").is_some(), "{response}");
            assert!(response["data"].is_null(), "{response}");
        }
    }
}
