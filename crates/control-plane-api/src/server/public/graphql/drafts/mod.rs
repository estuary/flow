use async_graphql::{Context, types::connection};

const DEFAULT_PAGE_SIZE: i32 = 100;
const MAX_PAGE_SIZE: i32 = 1000;

/// An authenticated user's private workspace for staging catalog changes.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Draft {
    pub id: models::Id,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub detail: Option<String>,
    /// Number of staged specifications the caller is currently allowed to read.
    pub num_specs: i32,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct DraftSpec {
    pub catalog_name: models::Name,
    pub spec_type: Option<models::CatalogType>,
    /// The staged catalog model. Null, together with a null `specType`, stages
    /// a deletion: publishing the draft removes the live specification of this
    /// name.
    pub spec: Option<super::JsonObject>,
    pub expect_pub_id: Option<models::Id>,
    pub last_pub_id: Option<models::Id>,
    pub detail: Option<String>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Whether the staged model is textually identical to the published one,
    /// so that a no-op can be told apart from a real edit. The comparison is
    /// over the serialized model, so a reordered document reads as changed.
    pub is_unchanged: bool,
}

pub type DraftConnection = connection::Connection<
    models::Id,
    Draft,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

pub type DraftSpecConnection = connection::Connection<
    String,
    DraftSpec,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[derive(Debug)]
struct DraftSpecRow {
    catalog_name: models::Name,
    spec_type: Option<models::CatalogType>,
    spec: Option<crate::TextJson<Box<serde_json::value::RawValue>>>,
    expect_pub_id: Option<models::Id>,
    last_pub_id: Option<models::Id>,
    detail: Option<String>,
    updated_at: chrono::DateTime<chrono::Utc>,
    is_unchanged: bool,
}

impl DraftSpecRow {
    fn into_graphql(self) -> DraftSpec {
        DraftSpec {
            catalog_name: self.catalog_name,
            spec_type: self.spec_type,
            spec: self.spec.map(|spec| async_graphql::Json(spec.0)),
            expect_pub_id: self.expect_pub_id,
            last_pub_id: self.last_pub_id,
            detail: self.detail,
            updated_at: self.updated_at,
            is_unchanged: self.is_unchanged,
        }
    }
}

/// Resolves the page size, defaulting an absent `first` and rejecting one out
/// of range rather than silently clamping it, so a caller is never quietly
/// handed fewer rows than it asked for.
fn page_size(first: Option<i32>) -> async_graphql::Result<i32> {
    match first {
        None => Ok(DEFAULT_PAGE_SIZE),
        Some(first) if (1..=MAX_PAGE_SIZE).contains(&first) => Ok(first),
        Some(_) => Err(async_graphql::Error::new(format!(
            "first must be between 1 and {MAX_PAGE_SIZE}"
        ))),
    }
}

#[async_graphql::ComplexObject]
impl Draft {
    /// Specifications the caller may read, in catalog-name order.
    async fn specs(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<DraftSpecConnection> {
        let env = ctx.data::<crate::Envelope>()?;
        let subject = env.claims()?.subject();
        let user_id = subject.user_id;
        let limit = page_size(first)?;
        let snapshot = env.snapshot();
        let readable_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            &subject,
            models::authz::Capability::CatalogRead,
            None,
        );

        connection::query_with::<String, _, _, _, async_graphql::Error>(
            after,
            None,
            Some(limit),
            None,
            |after, _, first, _| async move {
                let limit = first.expect("validated first is always present");
                let rows = sqlx::query_as!(
                    DraftSpecRow,
                    r#"
                    SELECT
                        ds.catalog_name AS "catalog_name!: models::Name",
                        ds.spec_type AS "spec_type?: models::CatalogType",
                        ds.spec AS "spec?: crate::TextJson<Box<serde_json::value::RawValue>>",
                        ds.expect_pub_id AS "expect_pub_id?: models::Id",
                        ls.last_pub_id AS "last_pub_id?: models::Id",
                        ds.detail,
                        ds.updated_at,
                        (uds.catalog_name IS NOT NULL) AS "is_unchanged!"
                    FROM draft_specs ds
                    JOIN drafts d ON d.id = ds.draft_id
                    LEFT JOIN live_specs ls ON ls.catalog_name = ds.catalog_name
                    LEFT JOIN unchanged_draft_specs uds
                      ON uds.draft_id = ds.draft_id
                     AND uds.catalog_name = ds.catalog_name
                    WHERE ds.draft_id = $1
                      AND d.user_id = $2
                      AND ds.catalog_name::text ^@ ANY($3)
                      AND ($4::text IS NULL OR ds.catalog_name::text > $4)
                    ORDER BY ds.catalog_name ASC
                    LIMIT $5
                    "#,
                    self.id as models::Id,
                    user_id,
                    &readable_prefixes,
                    after.as_deref(),
                    i64::try_from(limit + 1).expect("page limit fits i64"),
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;
                let mut result = DraftSpecConnection::new(after.is_some(), has_next);
                result.edges.extend(rows.into_iter().take(limit).map(|row| {
                    let cursor = row.catalog_name.to_string();
                    connection::Edge::new(cursor, row.into_graphql())
                }));
                Ok(result)
            },
        )
        .await
    }

    /// Diagnostics from the draft's last build. A diagnostic scoped to a
    /// catalog name the caller cannot read is omitted.
    async fn errors(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<models::draft_error::Error>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let rows = sqlx::query!(
            r#"
            SELECT de.scope, de.detail
            FROM draft_errors de
            JOIN drafts d ON d.id = de.draft_id
            WHERE de.draft_id = $1 AND d.user_id = $2
            ORDER BY de.scope, de.detail
            "#,
            self.id as models::Id,
            user_id,
        )
        .fetch_all(&env.pg_pool)
        .await?;

        let mut errors = Vec::with_capacity(rows.len());
        for row in rows {
            let catalog_name = url::Url::parse(&row.scope)
                .ok()
                .as_ref()
                .and_then(tables::parse_synthetic_scope)
                .map(|(_, catalog_name)| catalog_name)
                .unwrap_or_default();

            if !catalog_name.is_empty()
                && !super::may_access(ctx, &catalog_name, models::authz::Capability::CatalogRead)?
            {
                continue;
            }
            errors.push(models::draft_error::Error {
                catalog_name,
                scope: Some(row.scope),
                detail: row.detail,
            });
        }
        Ok(errors)
    }
}

#[derive(Debug, Default)]
pub struct DraftsQuery;

#[async_graphql::Object]
impl DraftsQuery {
    async fn draft(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<Option<Draft>> {
        let env = ctx.data::<crate::Envelope>()?;
        let subject = env.claims()?.subject();
        let user_id = subject.user_id;
        let snapshot = env.snapshot();
        let readable_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            &subject,
            models::authz::Capability::CatalogRead,
            None,
        );

        let row = sqlx::query_as!(
            Draft,
            r#"
            SELECT
                d.id AS "id!: models::Id",
                d.created_at,
                d.updated_at,
                d.detail,
                (
                    SELECT count(*)::int4
                    FROM draft_specs ds
                    WHERE ds.draft_id = d.id
                      AND ds.catalog_name::text ^@ ANY($3)
                ) AS "num_specs!"
            FROM drafts d
            WHERE d.id = $1 AND d.user_id = $2
            "#,
            id as models::Id,
            user_id,
            &readable_prefixes,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row)
    }

    /// The caller's own drafts, in stable ID order.
    async fn drafts(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<DraftConnection> {
        let env = ctx.data::<crate::Envelope>()?;
        let subject = env.claims()?.subject();
        let user_id = subject.user_id;
        let limit = page_size(first)?;
        let snapshot = env.snapshot();
        let readable_prefixes = super::authorized_prefixes::authorized_prefixes(
            &snapshot.role_grants,
            &snapshot.user_grants,
            &subject,
            models::authz::Capability::CatalogRead,
            None,
        );

        connection::query_with::<models::Id, _, _, _, async_graphql::Error>(
            after,
            None,
            Some(limit),
            None,
            |after, _, first, _| async move {
                let limit = first.expect("validated first is always present");
                let rows = sqlx::query_as!(
                    Draft,
                    r#"
                    SELECT
                        d.id AS "id!: models::Id",
                        d.created_at,
                        d.updated_at,
                        d.detail,
                        (
                            SELECT count(*)::int4
                            FROM draft_specs ds
                            WHERE ds.draft_id = d.id
                              AND ds.catalog_name::text ^@ ANY($4)
                        ) AS "num_specs!"
                    FROM drafts d
                    WHERE d.user_id = $1
                      AND ($2::flowid IS NULL OR d.id > $2)
                    ORDER BY d.id ASC
                    LIMIT $3
                    "#,
                    user_id,
                    after as Option<models::Id>,
                    i64::try_from(limit + 1).expect("page limit fits i64"),
                    &readable_prefixes,
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;
                let mut result = DraftConnection::new(after.is_some(), has_next);
                result.edges.extend(rows.into_iter().take(limit).map(|row| {
                    let cursor = row.id;
                    connection::Edge::new(cursor, row)
                }));
                Ok(result)
            },
        )
        .await
    }
}

#[cfg(test)]
mod test;
