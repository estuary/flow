use async_graphql::{Context, MaybeUndefined, types::connection};

const DEFAULT_PAGE_SIZE: usize = 100;

/// A draft change-set of Flow catalog specifications.
/// Only its owner can access it; catalog edit permissions are checked when
/// publishing.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Draft {
    pub id: models::Id,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Description of this draft.
    pub detail: Option<String>,
    /// Number of staged specifications.
    pub num_specs: i32,
}

/// A proposed catalog specification of a draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct DraftSpec {
    pub catalog_name: models::Name,
    pub catalog_type: Option<models::CatalogType>,
    /// A serialized catalog specification. Null, together with a null
    /// `catalogType`, stages deletion of the specification when this draft is
    /// published.
    pub model: Option<super::JsonObject>,
    /// Publication precondition: publishing fails if the live specification's
    /// `lastPubId` differs. The zero ID requires that no live specification
    /// exists; null imposes no precondition.
    pub expect_pub_id: Option<models::Id>,
    /// Last publication ID which updated the live specification, or null if
    /// it does not exist or the caller lacks CatalogRead on its name.
    pub last_pub_id: Option<models::Id>,
    /// Description of the staged change.
    pub detail: Option<String>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Whether the staged specification is textually identical to the published
    /// one, so that a no-op can be told apart from a real edit. The comparison is
    /// over the serialized specification, so a reordered document reads as changed.
    /// False if no live specification exists or the caller lacks CatalogRead on it.
    pub is_unchanged: bool,
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct DraftSpecInput {
    pub catalog_name: models::Name,
    pub catalog_type: MaybeUndefined<models::CatalogType>,
    /// The catalog specification to stage. Null, together with a null `catalogType`,
    /// stages a deletion of the live specification instead.
    ///
    /// Both fields are required to be present. Omitting either is an error
    /// rather than a deletion, so a partial input cannot silently become one.
    pub model: MaybeUndefined<async_graphql::Json<async_graphql::Value>>,
    /// Require the live specification's `lastPubId` to match when publishing.
    /// When editing an existing specification, copy its `lastPubId` here to
    /// prevent publication from overwriting intervening changes.
    /// The zero ID requires that no live specification exists. Omitting this
    /// field or passing null clears any previously staged precondition.
    pub expect_pub_id: Option<models::Id>,
    /// Description of the staged change. Omission or null clears the previous
    /// description.
    pub detail: Option<String>,
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

#[async_graphql::ComplexObject]
impl Draft {
    /// Staged specifications, in catalog-name order.
    async fn specs(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<DraftSpecConnection> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;

        connection::query_with::<String, _, _, _, async_graphql::Error>(
            after,
            None,
            first,
            None,
            |after, _, first, _| async move {
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);
                // `is_unchanged` repeats the `unchanged_draft_specs` comparison rather
                // than joining that view: its `draft_specs_ext` and `live_specs_ext`
                // authorize by `auth.uid()`, which this pool never sets, unless the
                // connecting role has `rolbypassrls`. CatalogRead is checked below.
                let rows = sqlx::query!(
                    r#"
                    SELECT
                        ds.catalog_name AS "catalog_name!: models::Name",
                        ds.spec_type AS "spec_type?: models::CatalogType",
                        ds.spec AS "spec?: crate::TextJson<Box<serde_json::value::RawValue>>",
                        ds.expect_pub_id AS "expect_pub_id?: models::Id",
                        ls.last_pub_id AS "last_pub_id?: models::Id",
                        ds.detail,
                        ds.updated_at,
                        COALESCE(ls.md5 = md5(trim(ds.spec::text)), false) AS "is_unchanged!"
                    FROM draft_specs ds
                    JOIN drafts d ON d.id = ds.draft_id
                    LEFT JOIN live_specs ls ON ls.catalog_name = ds.catalog_name
                    WHERE ds.draft_id = $1
                      AND d.user_id = $2
                      AND ($3::text IS NULL OR ds.catalog_name::text > $3)
                    ORDER BY ds.catalog_name ASC
                    LIMIT $4
                    "#,
                    self.id as models::Id,
                    user_id,
                    after.as_deref(),
                    i64::try_from(limit + 1).expect("page limit fits i64"),
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;
                let mut result = DraftSpecConnection::new(after.is_some(), has_next);
                result.edges = rows
                    .into_iter()
                    .take(limit)
                    .map(|row| {
                        let cursor = row.catalog_name.to_string();
                        // Ownership exposes staged data, but live-spec metadata
                        // requires catalog read access, as in draft_specs_ext.
                        let may_read_live = super::may_access(
                            ctx,
                            row.catalog_name.as_str(),
                            models::authz::Capability::CatalogRead,
                        )?;
                        let spec = DraftSpec {
                            catalog_name: row.catalog_name,
                            catalog_type: row.spec_type,
                            model: row.spec.map(|spec| async_graphql::Json(spec.0)),
                            expect_pub_id: row.expect_pub_id,
                            last_pub_id: row.last_pub_id.filter(|_| may_read_live),
                            detail: row.detail,
                            updated_at: row.updated_at,
                            is_unchanged: may_read_live && row.is_unchanged,
                        };
                        Ok(connection::Edge::new(cursor, spec))
                    })
                    .collect::<async_graphql::Result<_>>()?;
                Ok(result)
            },
        )
        .await
    }

    /// Errors found while validating, testing, or publishing this draft.
    /// Staging and unstaging specifications do not clear these errors, so they
    /// may describe an earlier version of the draft.
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
        let user_id = env.claims()?.subject().user_id;

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
                ) AS "num_specs!"
            FROM drafts d
            WHERE d.id = $1 AND d.user_id = $2
            "#,
            id as models::Id,
            user_id,
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
        let user_id = env.claims()?.subject().user_id;

        connection::query_with::<models::Id, _, _, _, async_graphql::Error>(
            after,
            None,
            first,
            None,
            |after, _, first, _| async move {
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);
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

#[derive(Debug, Default)]
pub struct DraftsMutation;

#[async_graphql::Object]
impl DraftsMutation {
    /// Create an empty private draft owned by the authenticated caller.
    async fn create_draft(
        &self,
        ctx: &Context<'_>,
        detail: Option<String>,
    ) -> async_graphql::Result<Draft> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;

        let row = sqlx::query!(
            r#"
            INSERT INTO drafts (detail, user_id)
            VALUES ($1, $2)
            RETURNING
                id AS "id!: models::Id",
                created_at,
                updated_at
            "#,
            detail.as_deref(),
            user_id,
        )
        .fetch_one(&env.pg_pool)
        .await?;

        Ok(Draft {
            id: row.id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            detail,
            num_specs: 0,
        })
    }

    /// Stage specifications in a draft the caller owns, replacing all staged
    /// fields under each name. Omitted `expectPubId` and `detail` are cleared.
    /// The batch is atomic; returns the distinct staged names in catalog-name order.
    /// Catalog edit permissions are checked when publishing.
    async fn stage_draft_specs(
        &self,
        ctx: &Context<'_>,
        draft_id: models::Id,
        specs: Vec<DraftSpecInput>,
    ) -> async_graphql::Result<Vec<models::Name>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let mut txn = env.pg_pool.begin().await?;

        if !owns_draft(draft_id, user_id, &mut txn).await? {
            return Err(async_graphql::Error::new("draft not found"));
        }

        for spec in &specs {
            spec.validate()?;
        }

        for spec in &specs {
            crate::draft::replace_spec(
                draft_id,
                spec.catalog_name.as_str(),
                spec.model.value().map(|spec| &spec.0),
                spec.catalog_type.value().copied(),
                spec.expect_pub_id,
                spec.detail.as_deref(),
                &mut txn,
            )
            .await?;
        }
        if !specs.is_empty() {
            crate::draft::touch(draft_id, &mut txn).await?;
        }

        txn.commit().await?;
        let mut catalog_names: Vec<_> = specs.into_iter().map(|spec| spec.catalog_name).collect();
        catalog_names.sort();
        catalog_names.dedup();
        Ok(catalog_names)
    }

    /// Drop staged specifications from a draft the caller owns, and return the
    /// names actually dropped, in catalog-name order. This un-stages a pending
    /// change; it does not delete anything published. To stage a deletion of a
    /// live specification, use `stageDraftSpecs` with null `model` and `catalogType`.
    async fn unstage_draft_specs(
        &self,
        ctx: &Context<'_>,
        draft_id: models::Id,
        catalog_names: Vec<models::Name>,
    ) -> async_graphql::Result<Vec<models::Name>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let mut txn = env.pg_pool.begin().await?;

        if !owns_draft(draft_id, user_id, &mut txn).await? {
            return Err(async_graphql::Error::new("draft not found"));
        }

        let catalog_name_refs = catalog_names
            .iter()
            .map(models::Name::as_str)
            .collect::<Vec<_>>();

        let deleted = if catalog_name_refs.is_empty() {
            Vec::new()
        } else {
            crate::draft::delete_specs(draft_id, &catalog_name_refs, &mut txn).await?
        };
        if !deleted.is_empty() {
            crate::draft::touch(draft_id, &mut txn).await?;
        }
        txn.commit().await?;
        Ok(deleted)
    }

    /// Discard a draft the caller owns, along with everything staged in it.
    /// Returns the deleted ID. Nothing published is affected.
    async fn delete_draft(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<models::Id> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;

        let deleted = sqlx::query_scalar!(
            r#"
            DELETE FROM drafts
            WHERE id = $1 AND user_id = $2
            RETURNING id AS "id!: models::Id"
            "#,
            id as models::Id,
            user_id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;
        let Some(deleted) = deleted else {
            return Err(async_graphql::Error::new("draft not found"));
        };

        Ok(deleted)
    }
}

impl DraftSpecInput {
    fn validate(&self) -> async_graphql::Result<()> {
        if self.catalog_type.is_undefined() {
            return Err(async_graphql::Error::new(
                "catalogType must be provided (use null for a catalog deletion)",
            ));
        }
        if self.model.is_undefined() {
            return Err(async_graphql::Error::new(
                "model must be provided (use null for a catalog deletion)",
            ));
        }
        if self.model.is_null() != self.catalog_type.is_null() {
            return Err(async_graphql::Error::new(
                "model and catalogType must both be null or both be non-null",
            ));
        }
        Ok(())
    }
}

async fn owns_draft(
    draft_id: models::Id,
    user_id: uuid::Uuid,
    db: &mut sqlx::PgConnection,
) -> sqlx::Result<bool> {
    sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM drafts WHERE id = $1 AND user_id = $2
        ) AS "owns_draft!"
        "#,
        draft_id as models::Id,
        user_id,
    )
    .fetch_one(db)
    .await
}

#[cfg(test)]
mod test;
