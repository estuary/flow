use crate::server::public::graphql::PgDataLoader;
use async_graphql::{Context, connection};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(async_graphql::SimpleObject, Clone, Debug)]
#[graphql(complex)]
pub struct SpecPublicationHistoryItem {
    /// The id of the publication
    pub publication_id: models::Id,
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
    /// The live spec model that was published
    pub async fn model<'a>(&'a self) -> Option<async_graphql::Json<&'a models::RawValue>> {
        self.model.as_ref().map(|model| async_graphql::Json(model))
    }
}

/// Key for loading the most recent publication info for a given spec
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LastPublicationInfoKey {
    pub catalog_name: models::Name,
    pub include_model: bool,
}

impl async_graphql::dataloader::Loader<LastPublicationInfoKey> for PgDataLoader {
    type Value = SpecPublicationHistoryItem;

    type Error = String;

    async fn load(
        &self,
        keys: &[LastPublicationInfoKey],
    ) -> Result<HashMap<LastPublicationInfoKey, Self::Value>, Self::Error> {
        let names: Vec<&'_ str> = keys.iter().map(|n| n.catalog_name.as_str()).collect();
        let include_models: Vec<bool> = keys.iter().map(|k| k.include_model).collect();
        let rows = sqlx::query!(
            r#"select
                ls.catalog_name as "catalog_name!: models::Name",
                ls.last_pub_id as "publication_id!: models::Id",
                ps.published_at as "published_at!: DateTime<Utc>",
                ps.user_id as "user_id!: uuid::Uuid",
                u.email as "user_email: String",
                u.raw_user_meta_data->>'picture' as "user_avatar_url: String",
                u.raw_user_meta_data->>'full_name' as "user_full_name: String",
                ps.detail as "detail: String",
                case when args.include_model then ps.spec else null end as "model: models::RawValue"
              from unnest($1::catalog_name[], $2::boolean[]) as args(name, include_model)
              join live_specs ls on args.name = ls.catalog_name
              join publication_specs ps on ls.id = ps.live_spec_id and ls.last_pub_id = ps.pub_id
              left outer join auth.users u on ps.user_id = u.id
            "#,
            &names as &[&str],
            &include_models as &[bool]
        )
        .fetch_all(&self.0)
        .await
        .map_err(|e| format!("failed to fetch last publication info: {e}"))?;

        let results = rows
            .into_iter()
            .map(|row| {
                let key = LastPublicationInfoKey {
                    catalog_name: models::Name::new(row.catalog_name),
                    include_model: row.model.is_some(),
                };
                let val = SpecPublicationHistoryItem {
                    publication_id: row.publication_id,
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

/// A `CursorType` that is just a RFC3339 UTC timestamp
pub struct TimestampCursor(DateTime<Utc>);
impl connection::CursorType for TimestampCursor {
    type Error = chrono::ParseError;

    fn decode_cursor(s: &str) -> Result<Self, Self::Error> {
        let dt = DateTime::parse_from_rfc3339(s)?;
        Ok(Self(dt.to_utc()))
    }

    fn encode_cursor(&self) -> String {
        self.0.to_rfc3339()
    }
}

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
