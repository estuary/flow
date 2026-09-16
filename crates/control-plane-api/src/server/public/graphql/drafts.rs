use async_graphql::{Context, MaybeUndefined, types::connection};

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

/// A catalog specification staged within a draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct DraftSpec {
    pub catalog_name: models::Name,
    pub spec_type: Option<models::CatalogType>,
    pub spec: Option<super::JsonObject>,
    pub expect_pub_id: Option<models::Id>,
    pub last_pub_id: Option<models::Id>,
    pub detail: Option<String>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub is_unchanged: bool,
}

/// JSON input that retains object key order for encrypted catalog models.
///
/// Sops verifies its MAC by traversing a document in order. GraphQL values use
/// ordered maps, so serialize that representation directly into raw JSON
/// rather than routing through `serde_json::Value`, whose maps sort keys.
#[derive(Debug, Clone)]
pub struct DraftSpecJson(pub models::RawValue);

#[async_graphql::Scalar(name = "JSON")]
impl async_graphql::ScalarType for DraftSpecJson {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let text = serde_json::to_string(&value)?;
        Ok(Self(models::RawValue::from_string(text)?))
    }

    fn to_value(&self) -> async_graphql::Value {
        serde_json::from_str(self.0.get()).expect("draft spec is valid JSON")
    }
}

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct DraftSpecInput {
    pub catalog_name: models::Name,
    pub spec_type: MaybeUndefined<models::CatalogType>,
    pub spec: MaybeUndefined<DraftSpecJson>,
    pub expect_pub_id: Option<models::Id>,
    pub detail: Option<String>,
}

#[derive(Debug)]
struct ValidatedDraftSpecInput {
    catalog_name: models::Name,
    spec_type: Option<models::CatalogType>,
    spec: Option<DraftSpecJson>,
    expect_pub_id: Option<models::Id>,
    detail: Option<String>,
}

impl TryFrom<DraftSpecInput> for ValidatedDraftSpecInput {
    type Error = async_graphql::Error;

    fn try_from(input: DraftSpecInput) -> Result<Self, Self::Error> {
        let spec_type = match input.spec_type {
            MaybeUndefined::Undefined => {
                return Err(async_graphql::Error::new(
                    "specType must be provided (use null for a catalog deletion)",
                ));
            }
            MaybeUndefined::Null => None,
            MaybeUndefined::Value(spec_type) => Some(spec_type),
        };
        let spec = match input.spec {
            MaybeUndefined::Undefined => {
                return Err(async_graphql::Error::new(
                    "spec must be provided (use null for a catalog deletion)",
                ));
            }
            MaybeUndefined::Null => None,
            MaybeUndefined::Value(spec) => Some(spec),
        };
        if spec.is_some() != spec_type.is_some() {
            return Err(async_graphql::Error::new(
                "spec and specType must both be null or both be non-null",
            ));
        }

        Ok(Self {
            catalog_name: input.catalog_name,
            spec_type,
            spec,
            expect_pub_id: input.expect_pub_id,
            detail: input.detail,
        })
    }
}

/// The stable identity and creation time of a deleted draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct DeletedDraft {
    pub id: models::Id,
    pub created_at: chrono::DateTime<chrono::Utc>,
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
struct DraftMetadataRow {
    id: models::Id,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    detail: Option<String>,
    num_specs: i64,
}

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

async fn fetch_draft_specs_by_names(
    draft_id: models::Id,
    catalog_names: &[&str],
    db: &mut sqlx::PgConnection,
) -> sqlx::Result<Vec<DraftSpecRow>> {
    sqlx::query_as!(
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
        LEFT JOIN live_specs ls ON ls.catalog_name = ds.catalog_name
        LEFT JOIN unchanged_draft_specs uds
          ON uds.draft_id = ds.draft_id
         AND uds.catalog_name = ds.catalog_name
        WHERE ds.draft_id = $1
          AND ds.catalog_name::text = ANY($2)
        ORDER BY ds.catalog_name ASC
        "#,
        draft_id as models::Id,
        catalog_names as &[&str],
    )
    .fetch_all(db)
    .await
}

async fn lock_owned_draft(
    draft_id: models::Id,
    user_id: uuid::Uuid,
    db: &mut sqlx::PgConnection,
) -> sqlx::Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"
        SELECT true AS "locked!"
        FROM drafts
        WHERE id = $1 AND user_id = $2
        FOR UPDATE
        "#,
        draft_id as models::Id,
        user_id,
    )
    .fetch_optional(db)
    .await?
    .unwrap_or(false))
}

async fn verify_spec_edits(
    env: &crate::Envelope,
    catalog_names: &[&str],
) -> async_graphql::Result<()> {
    if catalog_names.is_empty() {
        return Ok(());
    }
    let policy_result = crate::server::evaluate_names_authorization(
        env.snapshot(),
        env.claims()?,
        models::authz::Capability::SpecEdit,
        catalog_names.iter().copied(),
    );
    let (_expiry, ()) = env.authorization_outcome(policy_result).await?;
    Ok(())
}

impl DraftMetadataRow {
    fn into_graphql(self) -> Draft {
        Draft {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            detail: self.detail,
            num_specs: i32::try_from(self.num_specs).expect("draft spec count fits GraphQL Int"),
        }
    }
}

fn page_size(first: MaybeUndefined<i32>) -> async_graphql::Result<i32> {
    match first {
        MaybeUndefined::Undefined => Ok(DEFAULT_PAGE_SIZE),
        MaybeUndefined::Null => Err(async_graphql::Error::new("first must not be null")),
        MaybeUndefined::Value(first) if (1..=MAX_PAGE_SIZE).contains(&first) => Ok(first),
        MaybeUndefined::Value(_) => Err(async_graphql::Error::new(format!(
            "first must be between 1 and {MAX_PAGE_SIZE}"
        ))),
    }
}

#[async_graphql::ComplexObject]
impl Draft {
    /// List readable specifications in this draft, in catalog-name order.
    async fn specs(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: MaybeUndefined<i32>,
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

    /// Return the draft's current diagnostics without exposing errors scoped
    /// to catalog names the caller cannot read.
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
    /// Look up a draft owned by the authenticated user.
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
            DraftMetadataRow,
            r#"
            SELECT
                d.id AS "id!: models::Id",
                d.created_at,
                d.updated_at,
                d.detail,
                (
                    SELECT count(*)
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

        Ok(row.map(DraftMetadataRow::into_graphql))
    }

    /// List drafts owned by the authenticated user in stable ID order.
    async fn drafts(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: MaybeUndefined<i32>,
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
                    DraftMetadataRow,
                    r#"
                    SELECT
                        d.id AS "id!: models::Id",
                        d.created_at,
                        d.updated_at,
                        d.detail,
                        (
                            SELECT count(*)
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
                    connection::Edge::new(cursor, row.into_graphql())
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
    /// Create an empty draft owned by the authenticated user.
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

    /// Insert or replace catalog specifications in an owned draft.
    async fn upsert_draft_specs(
        &self,
        ctx: &Context<'_>,
        draft_id: models::Id,
        specs: Vec<DraftSpecInput>,
    ) -> async_graphql::Result<Vec<DraftSpec>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let mut txn = env.pg_pool.begin().await?;

        if !lock_owned_draft(draft_id, user_id, &mut txn).await? {
            return Err(async_graphql::Error::new("draft not found"));
        }

        let specs = specs
            .into_iter()
            .map(ValidatedDraftSpecInput::try_from)
            .collect::<async_graphql::Result<Vec<_>>>()?;
        let catalog_names = specs
            .iter()
            .map(|spec| spec.catalog_name.as_str())
            .collect::<Vec<_>>();
        verify_spec_edits(env, &catalog_names).await?;

        for spec in &specs {
            crate::draft::replace_spec(
                draft_id,
                spec.catalog_name.as_str(),
                spec.spec.as_ref().map(|spec| &*spec.0),
                spec.spec_type,
                spec.expect_pub_id,
                spec.detail.as_deref(),
                &mut txn,
            )
            .await?;
        }
        if !specs.is_empty() {
            crate::draft::touch(draft_id, &mut txn).await?;
        }

        let rows = if catalog_names.is_empty() {
            Vec::new()
        } else {
            fetch_draft_specs_by_names(draft_id, &catalog_names, &mut txn).await?
        };
        txn.commit().await?;
        Ok(rows.into_iter().map(DraftSpecRow::into_graphql).collect())
    }

    /// Remove catalog specifications from an owned draft.
    async fn delete_draft_specs(
        &self,
        ctx: &Context<'_>,
        draft_id: models::Id,
        catalog_names: Vec<models::Name>,
    ) -> async_graphql::Result<Vec<models::Name>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let mut txn = env.pg_pool.begin().await?;

        if !lock_owned_draft(draft_id, user_id, &mut txn).await? {
            return Err(async_graphql::Error::new("draft not found"));
        }

        let catalog_name_refs = catalog_names
            .iter()
            .map(models::Name::as_str)
            .collect::<Vec<_>>();
        verify_spec_edits(env, &catalog_name_refs).await?;

        let deleted = if catalog_name_refs.is_empty() {
            Vec::new()
        } else {
            crate::draft::delete_specs_returning(draft_id, &catalog_name_refs, &mut txn).await?
        };
        if !deleted.is_empty() {
            crate::draft::touch(draft_id, &mut txn).await?;
        }
        txn.commit().await?;
        Ok(deleted)
    }

    /// Delete a draft owned by the authenticated user.
    async fn delete_draft(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<DeletedDraft> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;

        // Discarding a private workspace has no catalog-side effect, so draft
        // ownership — rather than current catalog grants — is sufficient. A
        // single DELETE also makes concurrent attempts atomic, while the FK
        // cascade removes all contained specs and diagnostics.
        let deleted = sqlx::query!(
            r#"
            DELETE FROM drafts
            WHERE id = $1 AND user_id = $2
            RETURNING id AS "id!: models::Id", created_at
            "#,
            id as models::Id,
            user_id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;
        let Some(deleted) = deleted else {
            return Err(async_graphql::Error::new("draft not found"));
        };

        Ok(DeletedDraft {
            id: deleted.id,
            created_at: deleted.created_at,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_server;

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
    const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);
    const EDITOR: uuid::Uuid = uuid::Uuid::from_bytes([0x33; 16]);

    fn id(value: u64) -> models::Id {
        models::Id::new(value.to_be_bytes())
    }

    async fn insert_draft(
        pool: &sqlx::PgPool,
        id: models::Id,
        user_id: uuid::Uuid,
        detail: &str,
        created_at: chrono::DateTime<chrono::Utc>,
    ) {
        sqlx::query(
            r#"
            INSERT INTO drafts (id, user_id, detail, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $4)
            "#,
        )
        .bind(id)
        .bind(user_id)
        .bind(detail)
        .bind(created_at)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_spec(
        pool: &sqlx::PgPool,
        id: models::Id,
        draft_id: models::Id,
        catalog_name: &str,
    ) {
        sqlx::query(
            r#"
            INSERT INTO draft_specs (id, draft_id, catalog_name)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(id)
        .bind(draft_id)
        .bind(catalog_name)
        .execute(pool)
        .await
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_draft_spec(
        pool: &sqlx::PgPool,
        id: models::Id,
        draft_id: models::Id,
        catalog_name: &str,
        spec_type: Option<models::CatalogType>,
        spec: Option<serde_json::Value>,
        expect_pub_id: Option<models::Id>,
        detail: Option<&str>,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) {
        sqlx::query(
            r#"
            INSERT INTO draft_specs (
                id, draft_id, catalog_name, spec_type, spec, expect_pub_id,
                detail, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
            "#,
        )
        .bind(id)
        .bind(draft_id)
        .bind(catalog_name)
        .bind(spec_type)
        .bind(spec.map(sqlx::types::Json))
        .bind(expect_pub_id)
        .bind(detail)
        .bind(updated_at)
        .execute(pool)
        .await
        .unwrap();
    }

    fn error_message(response: &serde_json::Value) -> &str {
        response["errors"][0]["message"]
            .as_str()
            .expect("GraphQL error message")
    }

    #[test]
    fn validates_page_size() {
        assert_eq!(page_size(MaybeUndefined::Undefined).unwrap(), 100);
        assert_eq!(page_size(MaybeUndefined::Value(1)).unwrap(), 1);
        assert_eq!(page_size(MaybeUndefined::Value(1000)).unwrap(), 1000);

        assert_eq!(
            page_size(MaybeUndefined::Null).unwrap_err().message,
            "first must not be null"
        );
        for invalid in [0, -1, 1001] {
            assert_eq!(
                page_size(MaybeUndefined::Value(invalid))
                    .unwrap_err()
                    .message,
                "first must be between 1 and 1000"
            );
        }
    }

    #[test]
    fn draft_spec_json_preserves_object_key_order() {
        let original = r#"{"z-last":{"b":2,"a":1},"a-first":0}"#;
        let value: async_graphql::Value = serde_json::from_str(original).unwrap();
        let parsed =
            <DraftSpecJson as async_graphql::ScalarType>::parse(value).expect("valid JSON");
        assert_eq!(parsed.0.get(), original);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_draft_lifecycle_and_authorization(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, $2)")
            .bind(BOB)
            .bind("bob@example.test")
            .execute(&pool)
            .await
            .unwrap();
        // A Viewer may own drafts and inspect readable specs, but has no
        // authority to mutate those catalog names.
        sqlx::query(
            "INSERT INTO user_grants (user_id, object_role, capability) VALUES ($1, $2, 'read')",
        )
        .bind(BOB)
        .bind("bobCo/")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, $2)")
            .bind(EDITOR)
            .bind("editor@example.test")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            r#"
            INSERT INTO user_grants (user_id, object_role, capability, bundles)
            VALUES ($1, $2, 'none', ARRAY['editor'::capability_bundle])
            "#,
        )
        .bind(EDITOR)
        .bind("editorCo/")
        .execute(&pool)
        .await
        .unwrap();

        let first_id = id(0x10);
        let foreign_id = id(0x20);
        let second_id = id(0x30);
        let third_id = id(0x40);
        let editor_id = id(0x50);
        let first_created_at = "2024-01-01T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let second_created_at = "2024-01-02T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let third_created_at = "2024-01-03T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();

        insert_draft(&pool, first_id, ALICE, "first", first_created_at).await;
        insert_draft(&pool, foreign_id, BOB, "foreign", first_created_at).await;
        insert_draft(&pool, second_id, ALICE, "second", second_created_at).await;
        insert_draft(&pool, third_id, ALICE, "third", third_created_at).await;
        insert_draft(&pool, editor_id, EDITOR, "editor", third_created_at).await;
        insert_spec(&pool, id(0x101), first_id, "aliceCo/readable").await;
        insert_spec(&pool, id(0x102), first_id, "otherCo/not-readable").await;
        insert_spec(&pool, id(0x103), third_id, "aliceCo/deletable").await;
        insert_spec(&pool, id(0x104), foreign_id, "bobCo/view-only").await;
        insert_spec(&pool, id(0x105), editor_id, "editorCo/editable").await;

        let auth_snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), auth_snapshot).await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));
        let bob_token = server.make_access_token(BOB, Some("bob@example.test"));
        let editor_token = server.make_access_token(EDITOR, Some("editor@example.test"));

        let unauthenticated: serde_json::Value = server
            .graphql(
                &serde_json::json!({ "query": "query { drafts { edges { cursor } } }" }),
                None,
            )
            .await;
        assert!(error_message(&unauthenticated).contains("missing a required Authorization"));

        let first_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query Drafts($id: Id!) {
                          draft(id: $id) {
                            id createdAt updatedAt detail numSpecs
                          }
                          drafts(first: 2) {
                            edges { cursor node { id detail numSpecs } }
                            pageInfo {
                              hasPreviousPage hasNextPage startCursor endCursor
                            }
                          }
                        }
                    "#,
                    "variables": { "id": first_id.to_string() }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(first_page["data"]["draft"]["id"], first_id.to_string());
        assert_eq!(first_page["data"]["draft"]["detail"], "first");
        assert_eq!(first_page["data"]["draft"]["numSpecs"], 1);
        assert_eq!(
            first_page["data"]["drafts"]["edges"][0]["node"]["id"],
            first_id.to_string()
        );
        assert_eq!(
            first_page["data"]["drafts"]["edges"][1]["node"]["id"],
            second_id.to_string()
        );
        assert_eq!(
            first_page["data"]["drafts"]["pageInfo"]["hasPreviousPage"],
            false
        );
        assert_eq!(
            first_page["data"]["drafts"]["pageInfo"]["hasNextPage"],
            true
        );

        let after = first_page["data"]["drafts"]["pageInfo"]["endCursor"]
            .as_str()
            .unwrap();
        assert_eq!(after, second_id.to_string());
        let second_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        query Drafts($after: String) {
                          drafts(after: $after, first: 2) {
                            edges { cursor node { id detail numSpecs } }
                            pageInfo { hasPreviousPage hasNextPage }
                          }
                        }
                    "#,
                    "variables": { "after": after }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            second_page["data"]["drafts"]["edges"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            second_page["data"]["drafts"]["edges"][0]["node"]["id"],
            third_id.to_string()
        );
        assert_eq!(
            second_page["data"]["drafts"]["pageInfo"],
            serde_json::json!({ "hasPreviousPage": true, "hasNextPage": false })
        );

        // A caller's draft identity is private even when another user has
        // catalog permissions, and unknown IDs are indistinguishable.
        for hidden_id in [foreign_id, id(0xffff)] {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": "query Draft($id: Id!) { draft(id: $id) { id } }",
                        "variables": { "id": hidden_id.to_string() }
                    }),
                    Some(&alice_token),
                )
                .await;
            assert_eq!(response["data"]["draft"], serde_json::Value::Null);
        }

        // Omitting first uses the default and still excludes Bob's draft.
        let default_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "query { drafts { edges { node { id } } } }"
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            default_page["data"]["drafts"]["edges"]
                .as_array()
                .unwrap()
                .len(),
            3
        );

        for invalid_first in [serde_json::Value::Null, 0.into(), (-1).into(), 1001.into()] {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": "query Drafts($first: Int) { drafts(first: $first) { edges { cursor } } }",
                        "variables": { "first": invalid_first }
                    }),
                    Some(&alice_token),
                )
                .await;
            assert!(error_message(&response).starts_with("first must"));
        }

        let created: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        mutation {
                          createDraft(detail: "created through GraphQL") {
                            id createdAt updatedAt detail numSpecs
                          }
                        }
                    "#
                }),
                Some(&alice_token),
            )
            .await;
        let created_id = created["data"]["createDraft"]["id"]
            .as_str()
            .expect("created draft ID")
            .parse::<models::Id>()
            .unwrap();
        assert_eq!(
            created["data"]["createDraft"]["detail"],
            "created through GraphQL"
        );
        assert_eq!(created["data"]["createDraft"]["numSpecs"], 0);
        let created_owner: uuid::Uuid =
            sqlx::query_scalar("SELECT user_id FROM drafts WHERE id = $1")
                .bind(created_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(created_owner, ALICE);

        let bob_draft_count_before: i64 =
            sqlx::query_scalar("SELECT count(*) FROM drafts WHERE user_id = $1")
                .bind(BOB)
                .fetch_one(&pool)
                .await
                .unwrap();
        let viewer_created: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation { createDraft { id numSpecs } }"
                }),
                Some(&bob_token),
            )
            .await;
        let viewer_created_id = viewer_created["data"]["createDraft"]["id"]
            .as_str()
            .expect("Viewer-created draft ID")
            .parse::<models::Id>()
            .unwrap();
        assert_eq!(viewer_created["data"]["createDraft"]["numSpecs"], 0);
        let bob_draft_count_after: i64 =
            sqlx::query_scalar("SELECT count(*) FROM drafts WHERE user_id = $1")
                .bind(BOB)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(bob_draft_count_after, bob_draft_count_before + 1);

        // Ownership is sufficient to delete the empty draft that a Viewer was
        // allowed to create.
        let viewer_deleted_empty: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": viewer_created_id.to_string() }
                }),
                Some(&bob_token),
            )
            .await;
        assert_eq!(
            viewer_deleted_empty["data"]["deleteDraft"]["id"],
            viewer_created_id.to_string()
        );

        let viewer_draft: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "query Draft($id: Id!) { draft(id: $id) { id numSpecs } }",
                    "variables": { "id": foreign_id.to_string() }
                }),
                Some(&bob_token),
            )
            .await;
        assert_eq!(viewer_draft["data"]["draft"]["numSpecs"], 1);

        let delete_missing = |delete_id: models::Id| {
            serde_json::json!({
                "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                "variables": { "id": delete_id.to_string() }
            })
        };
        let foreign_delete: serde_json::Value = server
            .graphql(&delete_missing(foreign_id), Some(&alice_token))
            .await;
        let missing_delete: serde_json::Value = server
            .graphql(&delete_missing(id(0xffff)), Some(&alice_token))
            .await;
        assert_eq!(error_message(&foreign_delete), "draft not found");
        assert_eq!(
            error_message(&foreign_delete),
            error_message(&missing_delete),
            "foreign and missing drafts must not be distinguishable"
        );

        // Catalog capabilities govern access to draft contents, but ownership
        // governs discarding the private workspace. Bob has only CatalogRead
        // and can still delete his nonempty draft and its specs.
        let viewer_deleted_nonempty: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": foreign_id.to_string() }
                }),
                Some(&bob_token),
            )
            .await;
        assert_eq!(
            viewer_deleted_nonempty["data"]["deleteDraft"]["id"],
            foreign_id.to_string()
        );
        let viewer_draft_specs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
                .bind(foreign_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(viewer_draft_specs, 0);

        // The Editor bundle, without legacy Admin, supplies the individual
        // CatalogRead bit used to count readable specs. Deletion itself is
        // owner-authorized.
        let editor_draft: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "query Draft($id: Id!) { draft(id: $id) { id numSpecs } }",
                    "variables": { "id": editor_id.to_string() }
                }),
                Some(&editor_token),
            )
            .await;
        assert_eq!(editor_draft["data"]["draft"]["numSpecs"], 1);
        let editor_deleted: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": editor_id.to_string() }
                }),
                Some(&editor_token),
            )
            .await;
        assert_eq!(
            editor_deleted["data"]["deleteDraft"]["id"],
            editor_id.to_string()
        );

        // Alice can discard her own draft even though one contained name is
        // hidden by CatalogRead filtering. The cascade removes both specs.
        let deleted_hidden: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": "mutation Delete($id: Id!) { deleteDraft(id: $id) { id } }",
                    "variables": { "id": first_id.to_string() }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            deleted_hidden["data"]["deleteDraft"]["id"],
            first_id.to_string()
        );
        let remaining_specs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
                .bind(first_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(remaining_specs, 0);

        let deleted: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                        mutation Delete($id: Id!) {
                          deleteDraft(id: $id) { id createdAt }
                        }
                    "#,
                    "variables": { "id": third_id.to_string() }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(deleted["data"]["deleteDraft"]["id"], third_id.to_string());
        let returned_created_at = deleted["data"]["deleteDraft"]["createdAt"]
            .as_str()
            .unwrap()
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        assert_eq!(returned_created_at, third_created_at);
        let deleted_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM drafts d LEFT JOIN draft_specs ds ON ds.draft_id = d.id WHERE d.id = $1",
        )
        .bind(third_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(deleted_rows, 0);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_draft_contents_and_diagnostics(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let draft_id = id(0x60);
        let created_at = "2024-02-01T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        insert_draft(&pool, draft_id, ALICE, "contents", created_at).await;

        // The current unchanged-spec comparison deliberately ignores the
        // legacy inferred-schema MD5 columns: inferred schema changes are now
        // represented in the stored catalog model itself.
        sqlx::query(
            r#"
            INSERT INTO inferred_schemas (collection_name, schema, flow_document)
            VALUES ('aliceCo/data/foo', '{"type":"object"}', '{}')
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            UPDATE live_specs
            SET inferred_schema_md5 = 'the previously published inferred schema'
            WHERE catalog_name = 'aliceCo/data/foo'
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        let unchanged_updated_at = "2024-02-02T01:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let deletion_updated_at = "2024-02-02T02:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let changed_updated_at = "2024-02-02T03:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let new_updated_at = "2024-02-02T04:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let expected_pub_id = id(0x700);

        insert_draft_spec(
            &pool,
            id(0x601),
            draft_id,
            "aliceCo/data/foo",
            Some(models::CatalogType::Collection),
            Some(serde_json::json!({})),
            Some(expected_pub_id),
            Some("same as live"),
            unchanged_updated_at,
        )
        .await;
        insert_draft_spec(
            &pool,
            id(0x602),
            draft_id,
            "aliceCo/in/capture-foo",
            None,
            None,
            Some(models::Id::zero()),
            None,
            deletion_updated_at,
        )
        .await;
        let encrypted_model = serde_json::json!({
            "endpoint": {
                "connector": {
                    "image": "example/materialize:test",
                    "config": {
                        "token": "ENC[AES256_GCM,data:invented,tag:invented,type:str]"
                    }
                }
            },
            "bindings": []
        });
        insert_draft_spec(
            &pool,
            id(0x603),
            draft_id,
            "aliceCo/out/materialize-bar",
            Some(models::CatalogType::Materialization),
            Some(encrypted_model.clone()),
            None,
            Some("changed model"),
            changed_updated_at,
        )
        .await;
        let new_model = serde_json::json!({ "schema": { "type": "object" } });
        insert_draft_spec(
            &pool,
            id(0x604),
            draft_id,
            "aliceCo/z-new",
            Some(models::CatalogType::Collection),
            Some(new_model.clone()),
            None,
            None,
            new_updated_at,
        )
        .await;
        insert_draft_spec(
            &pool,
            id(0x605),
            draft_id,
            "otherCo/hidden",
            Some(models::CatalogType::Capture),
            Some(serde_json::json!({ "secret": "must not leak" })),
            None,
            Some("must not leak"),
            new_updated_at,
        )
        .await;

        sqlx::query(
            r#"
            INSERT INTO draft_errors (draft_id, scope, detail) VALUES
              ($1, 'flow://collection/aliceCo/data/foo#/schema', 'readable diagnostic'),
              ($1, 'file:///tmp/catalog.yaml', 'global diagnostic'),
              ($1, 'flow://capture/otherCo/hidden', 'hidden diagnostic')
            "#,
        )
        .bind(draft_id)
        .execute(&pool)
        .await
        .unwrap();

        let live_pub_id: models::Id = sqlx::query_scalar(
            "SELECT last_pub_id FROM live_specs WHERE catalog_name = 'aliceCo/data/foo'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let auth_snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), auth_snapshot).await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));

        let page_query = r#"
            query DraftContents($id: Id!, $after: String, $first: Int) {
              draft(id: $id) {
                numSpecs
                specs(after: $after, first: $first) {
                  edges {
                    cursor
                    node {
                      catalogName specType spec expectPubId lastPubId detail
                      updatedAt isUnchanged
                    }
                  }
                  pageInfo {
                    hasPreviousPage hasNextPage startCursor endCursor
                  }
                }
                errors { catalogName scope detail }
              }
            }
        "#;
        let first_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": page_query,
                    "variables": {
                        "id": draft_id.to_string(),
                        "first": 2
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(first_page.get("errors").is_none(), "{first_page:#}");
        assert_eq!(first_page["data"]["draft"]["numSpecs"], 4);

        let first_edges = first_page["data"]["draft"]["specs"]["edges"]
            .as_array()
            .unwrap();
        assert_eq!(first_edges.len(), 2);
        assert_eq!(first_edges[0]["cursor"], "aliceCo/data/foo");
        assert_eq!(first_edges[0]["node"]["catalogName"], "aliceCo/data/foo");
        assert_eq!(first_edges[0]["node"]["specType"], "collection");
        assert_eq!(first_edges[0]["node"]["spec"], serde_json::json!({}));
        assert_eq!(
            first_edges[0]["node"]["expectPubId"],
            expected_pub_id.to_string()
        );
        assert_eq!(first_edges[0]["node"]["lastPubId"], live_pub_id.to_string());
        assert_eq!(first_edges[0]["node"]["detail"], "same as live");
        assert_eq!(first_edges[0]["node"]["isUnchanged"], true);
        assert_eq!(
            first_edges[0]["node"]["updatedAt"]
                .as_str()
                .unwrap()
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap(),
            unchanged_updated_at
        );

        assert_eq!(first_edges[1]["cursor"], "aliceCo/in/capture-foo");
        assert_eq!(first_edges[1]["node"]["specType"], serde_json::Value::Null);
        assert_eq!(first_edges[1]["node"]["spec"], serde_json::Value::Null);
        assert_eq!(
            first_edges[1]["node"]["expectPubId"],
            models::Id::zero().to_string()
        );
        assert_eq!(first_edges[1]["node"]["lastPubId"], live_pub_id.to_string());
        assert_eq!(first_edges[1]["node"]["detail"], serde_json::Value::Null);
        assert_eq!(first_edges[1]["node"]["isUnchanged"], false);
        assert_eq!(
            first_page["data"]["draft"]["specs"]["pageInfo"],
            serde_json::json!({
                "hasPreviousPage": false,
                "hasNextPage": true,
                "startCursor": "aliceCo/data/foo",
                "endCursor": "aliceCo/in/capture-foo"
            })
        );

        // The recognized unauthorized diagnostic is omitted, while a global
        // diagnostic remains visible with an empty derived catalog name.
        assert_eq!(
            first_page["data"]["draft"]["errors"],
            serde_json::json!([
                {
                    "catalogName": "",
                    "scope": "file:///tmp/catalog.yaml",
                    "detail": "global diagnostic"
                },
                {
                    "catalogName": "aliceCo/data/foo",
                    "scope": "flow://collection/aliceCo/data/foo#/schema",
                    "detail": "readable diagnostic"
                }
            ])
        );

        let after = first_page["data"]["draft"]["specs"]["pageInfo"]["endCursor"]
            .as_str()
            .unwrap();
        let second_page: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": page_query,
                    "variables": {
                        "id": draft_id.to_string(),
                        "after": after,
                        "first": 2
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(second_page.get("errors").is_none(), "{second_page:#}");
        let second_edges = second_page["data"]["draft"]["specs"]["edges"]
            .as_array()
            .unwrap();
        assert_eq!(second_edges.len(), 2);
        assert_eq!(
            second_edges[0]["node"]["catalogName"],
            "aliceCo/out/materialize-bar"
        );
        assert_eq!(second_edges[0]["node"]["spec"], encrypted_model);
        assert_eq!(second_edges[0]["node"]["isUnchanged"], false);
        assert_eq!(second_edges[1]["node"]["catalogName"], "aliceCo/z-new");
        assert_eq!(second_edges[1]["node"]["spec"], new_model);
        assert_eq!(
            second_edges[1]["node"]["lastPubId"],
            serde_json::Value::Null
        );
        assert_eq!(
            second_page["data"]["draft"]["specs"]["pageInfo"],
            serde_json::json!({
                "hasPreviousPage": true,
                "hasNextPage": false,
                "startCursor": "aliceCo/out/materialize-bar",
                "endCursor": "aliceCo/z-new"
            })
        );

        let null_first: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": page_query,
                    "variables": { "id": draft_id.to_string(), "first": null }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(error_message(&null_first), "first must not be null");

        let remaining_specs: i64 =
            sqlx::query_scalar("SELECT count(*) FROM draft_specs WHERE draft_id = $1")
                .bind(draft_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            remaining_specs, 5,
            "reading isUnchanged must not prune draft specs"
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_draft_edits(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, $2)")
            .bind(BOB)
            .bind("bob@example.test")
            .execute(&pool)
            .await
            .unwrap();

        let draft_id = id(0x80);
        let foreign_draft_id = id(0x81);
        let created_at = "2024-03-01T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        insert_draft(&pool, draft_id, ALICE, "edits", created_at).await;
        insert_draft(&pool, foreign_draft_id, BOB, "foreign", created_at).await;

        let original_expect_pub_id = id(0x810);
        insert_draft_spec(
            &pool,
            id(0x801),
            draft_id,
            "aliceCo/existing",
            Some(models::CatalogType::Collection),
            Some(serde_json::json!({ "schema": { "type": "string" } })),
            Some(original_expect_pub_id),
            Some("original detail"),
            created_at,
        )
        .await;
        insert_draft_spec(
            &pool,
            id(0x802),
            draft_id,
            "aliceCo/explicit-null",
            Some(models::CatalogType::Test),
            Some(serde_json::json!({ "steps": [{ "old": true }] })),
            Some(original_expect_pub_id),
            Some("must be cleared"),
            created_at,
        )
        .await;
        insert_draft_spec(
            &pool,
            id(0x803),
            draft_id,
            "otherCo/hidden",
            Some(models::CatalogType::Capture),
            Some(serde_json::json!({ "original": "hidden" })),
            None,
            None,
            created_at,
        )
        .await;

        let auth_snapshot = test_server::snapshot(pool.clone(), false).await;
        let server = test_server::TestServer::start(pool.clone(), auth_snapshot).await;
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));

        let upsert_mutation = r#"
            mutation Upsert($draftId: Id!, $specs: [DraftSpecInput!]!) {
              upsertDraftSpecs(draftId: $draftId, specs: $specs) {
                catalogName specType spec expectPubId lastPubId detail
                updatedAt isUnchanged
              }
            }
        "#;
        let delete_mutation = r#"
            mutation Delete($draftId: Id!, $catalogNames: [Name!]!) {
              deleteDraftSpecs(
                draftId: $draftId,
                catalogNames: $catalogNames
              )
            }
        "#;

        let encrypted_model = serde_json::json!({
            "endpoint": {
                "connector": {
                    "image": "example/source:test",
                    "config": {
                        "password": "ENC[AES256_GCM,data:invented,tag:invented,type:str]"
                    }
                }
            },
            "bindings": []
        });
        let inserted: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": [
                            {
                                "catalogName": "aliceCo/delete-me",
                                "specType": null,
                                "spec": null,
                                "expectPubId": models::Id::zero().to_string(),
                                "detail": "catalog deletion"
                            },
                            {
                                "catalogName": "aliceCo/existing",
                                "specType": "capture",
                                "spec": encrypted_model,
                                "expectPubId": models::Id::zero().to_string(),
                                "detail": "replacement detail"
                            },
                            {
                                "catalogName": "aliceCo/explicit-null",
                                "specType": "test",
                                "spec": { "steps": [] },
                                "expectPubId": null,
                                "detail": null
                            },
                            {
                                "catalogName": "aliceCo/new",
                                "specType": "test",
                                "spec": { "steps": [] },
                                "expectPubId": null,
                                "detail": null
                            }
                        ]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(inserted.get("errors").is_none(), "{inserted:#}");
        let inserted_specs = inserted["data"]["upsertDraftSpecs"].as_array().unwrap();
        assert_eq!(
            inserted_specs
                .iter()
                .map(|spec| spec["catalogName"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "aliceCo/delete-me",
                "aliceCo/existing",
                "aliceCo/explicit-null",
                "aliceCo/new"
            ]
        );
        assert_eq!(inserted_specs[0]["specType"], serde_json::Value::Null);
        assert_eq!(inserted_specs[0]["spec"], serde_json::Value::Null);
        assert_eq!(
            inserted_specs[0]["expectPubId"],
            models::Id::zero().to_string()
        );
        assert_eq!(inserted_specs[0]["detail"], "catalog deletion");
        assert_eq!(inserted_specs[0]["isUnchanged"], false);
        assert_eq!(inserted_specs[1]["specType"], "capture");
        assert_eq!(inserted_specs[1]["spec"], encrypted_model);
        assert_eq!(
            inserted_specs[1]["expectPubId"],
            models::Id::zero().to_string()
        );
        assert_eq!(inserted_specs[1]["detail"], "replacement detail");
        assert_eq!(inserted_specs[1]["lastPubId"], serde_json::Value::Null);
        assert_eq!(inserted_specs[2]["expectPubId"], serde_json::Value::Null);
        assert_eq!(inserted_specs[2]["detail"], serde_json::Value::Null);
        assert_eq!(inserted_specs[3]["expectPubId"], serde_json::Value::Null);
        assert_eq!(inserted_specs[3]["detail"], serde_json::Value::Null);

        let existing_created_at: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "SELECT created_at FROM draft_specs WHERE draft_id = $1 AND catalog_name = $2",
        )
        .bind(draft_id)
        .bind("aliceCo/existing")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(existing_created_at, created_at);
        let draft_updated_at: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
                .bind(draft_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(draft_updated_at > created_at);

        // Omitting nullable client-editable values replaces them with null,
        // just as explicitly passing null does.
        let omitted_nullable_values: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": [{
                            "catalogName": "aliceCo/existing",
                            "specType": "capture",
                            "spec": encrypted_model
                        }]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(
            omitted_nullable_values.get("errors").is_none(),
            "{omitted_nullable_values:#}"
        );
        assert_eq!(
            omitted_nullable_values["data"]["upsertDraftSpecs"][0]["expectPubId"],
            serde_json::Value::Null
        );
        assert_eq!(
            omitted_nullable_values["data"]["upsertDraftSpecs"][0]["detail"],
            serde_json::Value::Null
        );

        // The two nullable model fields are nevertheless required to be
        // present, and all inputs are validated before any row is written.
        let omitted_type: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": [
                            {
                                "catalogName": "aliceCo/should-not-insert",
                                "specType": "collection",
                                "spec": {}
                            },
                            {
                                "catalogName": "aliceCo/missing-type",
                                "spec": {}
                            }
                        ]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            error_message(&omitted_type),
            "specType must be provided (use null for a catalog deletion)"
        );
        let invalid_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM draft_specs WHERE draft_id = $1 AND catalog_name::text LIKE 'aliceCo/%insert%'",
        )
        .bind(draft_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(invalid_rows, 0);

        let omitted_spec: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": [{
                            "catalogName": "aliceCo/missing-spec",
                            "specType": "collection"
                        }]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            error_message(&omitted_spec),
            "spec must be provided (use null for a catalog deletion)"
        );

        let inconsistent_nulls: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": [{
                            "catalogName": "aliceCo/inconsistent",
                            "specType": null,
                            "spec": {}
                        }]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            error_message(&inconsistent_nulls),
            "spec and specType must both be null or both be non-null"
        );

        // Authorization is evaluated for the complete batch before either an
        // upsert or a removal can affect an authorized row.
        let existing_before: serde_json::Value = sqlx::query_scalar(
            "SELECT spec FROM draft_specs WHERE draft_id = $1 AND catalog_name = $2",
        )
        .bind(draft_id)
        .bind("aliceCo/existing")
        .fetch_one(&pool)
        .await
        .unwrap();
        let hidden_before: serde_json::Value = sqlx::query_scalar(
            "SELECT spec FROM draft_specs WHERE draft_id = $1 AND catalog_name = $2",
        )
        .bind(draft_id)
        .bind("otherCo/hidden")
        .fetch_one(&pool)
        .await
        .unwrap();
        let unauthorized_upsert: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": [
                            {
                                "catalogName": "aliceCo/existing",
                                "specType": "collection",
                                "spec": { "unauthorized": "batch" }
                            },
                            {
                                "catalogName": "otherCo/hidden",
                                "specType": "capture",
                                "spec": { "unauthorized": "batch" }
                            }
                        ]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(unauthorized_upsert.get("errors").is_some());
        let existing_after: serde_json::Value = sqlx::query_scalar(
            "SELECT spec FROM draft_specs WHERE draft_id = $1 AND catalog_name = $2",
        )
        .bind(draft_id)
        .bind("aliceCo/existing")
        .fetch_one(&pool)
        .await
        .unwrap();
        let hidden_after: serde_json::Value = sqlx::query_scalar(
            "SELECT spec FROM draft_specs WHERE draft_id = $1 AND catalog_name = $2",
        )
        .bind(draft_id)
        .bind("otherCo/hidden")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(existing_after, existing_before);
        assert_eq!(hidden_after, hidden_before);

        let unauthorized_delete: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": delete_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "catalogNames": ["aliceCo/existing", "otherCo/hidden"]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(unauthorized_delete.get("errors").is_some());
        let rows_after_unauthorized_delete: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM draft_specs WHERE draft_id = $1 AND catalog_name::text = ANY($2)",
        )
        .bind(draft_id)
        .bind(["aliceCo/existing", "otherCo/hidden"])
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rows_after_unauthorized_delete, 2);

        let deleted: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": delete_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "catalogNames": [
                            "aliceCo/new",
                            "aliceCo/not-present",
                            "aliceCo/delete-me"
                        ]
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert!(deleted.get("errors").is_none(), "{deleted:#}");
        assert_eq!(
            deleted["data"]["deleteDraftSpecs"],
            serde_json::json!(["aliceCo/delete-me", "aliceCo/new"])
        );

        let before_empty: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
                .bind(draft_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let empty_delete: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": delete_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "catalogNames": []
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            empty_delete["data"]["deleteDraftSpecs"],
            serde_json::json!([])
        );
        let empty_upsert: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": {
                        "draftId": draft_id.to_string(),
                        "specs": []
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(
            empty_upsert["data"]["upsertDraftSpecs"],
            serde_json::json!([])
        );
        let after_empty: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT updated_at FROM drafts WHERE id = $1")
                .bind(draft_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(after_empty, before_empty);

        let missing: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": upsert_mutation,
                    "variables": { "draftId": id(0xffff).to_string(), "specs": [] }
                }),
                Some(&alice_token),
            )
            .await;
        let foreign: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": delete_mutation,
                    "variables": {
                        "draftId": foreign_draft_id.to_string(),
                        "catalogNames": []
                    }
                }),
                Some(&alice_token),
            )
            .await;
        assert_eq!(error_message(&missing), "draft not found");
        assert_eq!(error_message(&foreign), "draft not found");

        let unauthenticated: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": delete_mutation,
                    "variables": { "draftId": draft_id.to_string(), "catalogNames": [] }
                }),
                None,
            )
            .await;
        assert!(unauthenticated.get("errors").is_some());

        let published: i64 =
            sqlx::query_scalar("SELECT count(*) FROM publications WHERE draft_id = $1")
                .bind(draft_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(published, 0);
        let created_live_specs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM live_specs WHERE catalog_name::text = ANY($1)",
        )
        .bind([
            "aliceCo/delete-me",
            "aliceCo/existing",
            "aliceCo/explicit-null",
            "aliceCo/new",
        ])
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(created_live_specs, 0);
    }
}
