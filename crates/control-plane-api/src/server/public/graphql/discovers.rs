use async_graphql::{Context, types::connection};
use sqlx::Row;

use super::TimestampCursor;

const DEFAULT_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
#[graphql(rename_items = "camelCase")]
pub enum DiscoverStatus {
    /// The discover is queued or in progress.
    Queued,
    Success,
    WrongProtocol,
    TagFailed,
    ImageForbidden,
    DiscoverFailed,
    NoDataPlane,
    NotAuthorized,
    /// This status occurs only on historical discovers.
    MergeFailed,
    /// This status occurs only on historical discovers.
    DeprecatedBackground,
    /// This status occurs only on historical discovers.
    PullFailed,
}

impl DiscoverStatus {
    fn from_db(value: &str) -> async_graphql::Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "success" => Ok(Self::Success),
            "wrongProtocol" => Ok(Self::WrongProtocol),
            "tagFailed" => Ok(Self::TagFailed),
            "imageForbidden" => Ok(Self::ImageForbidden),
            "discoverFailed" => Ok(Self::DiscoverFailed),
            "noDataPlane" => Ok(Self::NoDataPlane),
            "notAuthorized" => Ok(Self::NotAuthorized),
            "mergeFailed" => Ok(Self::MergeFailed),
            "deprecatedBackground" => Ok(Self::DeprecatedBackground),
            "pullFailed" => Ok(Self::PullFailed),
            _ => Err(async_graphql::Error::new("unknown discover status")),
        }
    }
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct LogLine {
    logged_at: chrono::DateTime<chrono::Utc>,
    stream: String,
    line: String,
}

pub type LogLineConnection = connection::Connection<
    TimestampCursor,
    LogLine,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

/// An asynchronous capture discovery that merges its results into a draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Discover {
    id: models::Id,
    draft_id: models::Id,
    capture_name: models::Name,
    data_plane_name: String,
    status: DiscoverStatus,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[async_graphql::ComplexObject]
impl Discover {
    /// Current draft errors, which may predate this discover or be replaced by a later job.
    async fn errors(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<models::draft_error::Error>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        super::drafts::errors_for_draft(self.draft_id, user_id, &env.pg_pool).await
    }

    /// Lines may arrive after discovery finishes. Pagination is by timestamp and is best effort.
    async fn logs(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<LogLineConnection> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;

        connection::query_with::<TimestampCursor, _, _, _, async_graphql::Error>(
            after,
            None,
            first,
            None,
            |after, _, first, _| async move {
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);
                let after_time = after.map(|cursor| cursor.0);
                let rows = sqlx::query(
                    r#"
                    SELECT l.logged_at, l.stream, l.log_line
                    FROM internal.log_lines l
                    JOIN discovers di ON di.logs_token = l.token
                    JOIN drafts d ON d.id = di.draft_id
                    WHERE di.id = $1 AND d.user_id = $2
                      AND ($3::timestamptz IS NULL OR l.logged_at > $3)
                    ORDER BY l.logged_at ASC
                    LIMIT $4
                    "#,
                )
                .bind(self.id)
                .bind(user_id)
                .bind(after_time)
                .bind(i64::try_from(limit + 1).expect("page limit fits i64"))
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;
                let mut result = LogLineConnection::new(after_time.is_some(), has_next);
                for row in rows.into_iter().take(limit) {
                    let logged_at = row.try_get("logged_at")?;
                    result.edges.push(connection::Edge::new(
                        TimestampCursor(logged_at),
                        LogLine {
                            logged_at,
                            stream: row.try_get("stream")?,
                            line: row.try_get("log_line")?,
                        },
                    ));
                }
                Ok(result)
            },
        )
        .await
    }
}

async fn fetch_discover(
    id: models::Id,
    user_id: uuid::Uuid,
    pool: &sqlx::PgPool,
) -> async_graphql::Result<Option<Discover>> {
    let row = sqlx::query(
        r#"
        SELECT di.id, di.draft_id, di.capture_name, di.data_plane_name,
               di.job_status->>'type' AS status, di.created_at, di.updated_at
        FROM discovers di
        JOIN drafts d ON d.id = di.draft_id
        WHERE di.id = $1 AND d.user_id = $2
        "#,
    )
    .bind(id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        Ok(Discover {
            id: row.try_get("id")?,
            draft_id: row.try_get("draft_id")?,
            capture_name: row.try_get("capture_name")?,
            data_plane_name: row.try_get("data_plane_name")?,
            status: DiscoverStatus::from_db(row.try_get::<&str, _>("status")?)?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    })
    .transpose()
}

#[derive(Debug, Default)]
pub struct DiscoversQuery;

#[async_graphql::Object]
impl DiscoversQuery {
    /// Returns null if the discover does not exist or the caller does not own its draft.
    async fn discover(
        &self,
        ctx: &Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<Option<Discover>> {
        let env = ctx.data::<crate::Envelope>()?;
        fetch_discover(id, env.claims()?.subject().user_id, &env.pg_pool).await
    }
}

#[derive(Debug, Default)]
pub struct DiscoversMutation;

#[async_graphql::Object]
impl DiscoversMutation {
    /// Discover bindings using a staged capture, or copy a readable live capture first.
    /// The resulting definitions remain in the draft until published separately.
    /// `dataPlane` selects the plane for this discovery operation, not publication.
    async fn create_discover(
        &self,
        ctx: &Context<'_>,
        draft_id: models::Id,
        capture_name: models::Name,
        data_plane: Option<String>,
    ) -> async_graphql::Result<Discover> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let mut txn = env.pg_pool.begin().await?;

        // Keep the draft alive through submission without conflicting with
        // stageDraftSpecs, which updates draft_specs before touching drafts.
        // The copy itself rejects a conflicting insert.
        let owned = sqlx::query_scalar::<_, models::Id>(
            "SELECT id FROM drafts WHERE id = $1 AND user_id = $2 FOR KEY SHARE",
        )
        .bind(draft_id)
        .bind(user_id)
        .fetch_optional(&mut *txn)
        .await?;
        if owned.is_none() {
            return Err(async_graphql::Error::new("draft not found"));
        }
        if !capture_name.as_str().contains('/') {
            return Err(async_graphql::Error::new(
                "invalid catalog name: expected a tenant/name path",
            ));
        }
        if let Err(err) = validator::Validate::validate(&capture_name) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog name: {err}"
            )));
        }

        super::verify_authorization(
            env,
            capture_name.as_str(),
            models::authz::Capability::SpecEdit,
        )
        .await?;

        let drafted = sqlx::query(
            r#"
            SELECT spec::text AS spec, spec_type::text AS spec_type
            FROM draft_specs
            WHERE draft_id = $1 AND catalog_name = $2
            FOR UPDATE
            "#,
        )
        .bind(draft_id)
        .bind(capture_name.as_str())
        .fetch_optional(&mut *txn)
        .await?;

        // Plane selection is based on the live capture even when its definition
        // cannot be disclosed to this caller or the draft contains edits.
        let live = sqlx::query(
            r#"
            SELECT ls.spec::text AS spec, ls.spec_type::text AS spec_type,
                   ls.last_pub_id, dp.data_plane_name::text AS data_plane_name
            FROM live_specs ls
            LEFT JOIN data_planes dp ON dp.id = ls.data_plane_id
            WHERE ls.catalog_name = $1
            FOR SHARE OF ls
            "#,
        )
        .bind(capture_name.as_str())
        .fetch_optional(&mut *txn)
        .await?;
        let live_capture = live.as_ref().filter(|row| {
            row.get::<Option<&str>, _>("spec_type") == Some("capture")
                && row.get::<Option<&str>, _>("spec").is_some()
        });

        let copy_live = drafted.is_none();
        let model_text: &str = if let Some(row) = drafted.as_ref() {
            if row.get::<Option<&str>, _>("spec_type") != Some("capture") {
                return Err(async_graphql::Error::new("draft entry is not a capture"));
            }
            row.get::<Option<&str>, _>("spec")
                .ok_or_else(|| async_graphql::Error::new("draft entry is a deletion"))?
        } else {
            let row = live_capture.ok_or_else(|| async_graphql::Error::new("capture not found"))?;
            if !super::may_access(
                ctx,
                capture_name.as_str(),
                models::authz::Capability::CatalogRead,
            )? {
                return Err(async_graphql::Error::new("capture not found"));
            }
            row.get::<&str, _>("spec")
        };
        let model: models::CaptureDef = serde_json::from_str(model_text)
            .map_err(|_| async_graphql::Error::new("invalid capture model"))?;
        if model.delete {
            return Err(async_graphql::Error::new("capture is staged for deletion"));
        }
        let models::CaptureEndpoint::Connector(config) = &model.endpoint else {
            return Err(async_graphql::Error::new(
                "capture requires a connector endpoint",
            ));
        };
        let (image_name, image_tag) = models::split_image_tag(&config.image);
        if image_name.is_empty() || image_tag.is_empty() {
            return Err(async_graphql::Error::new(
                "capture requires a tagged connector image",
            ));
        }
        if !serde_json::from_str::<serde_json::Value>(config.config.get())?.is_object() {
            return Err(async_graphql::Error::new(
                "endpoint configuration must be an inline JSON object",
            ));
        }

        let tag_id = sqlx::query_scalar::<_, models::Id>(
            r#"
            SELECT ct.id FROM connector_tags ct
            JOIN connectors c ON c.id = ct.connector_id
            WHERE c.image_name = $1 AND ct.image_tag = $2
              AND ct.protocol = 'capture' AND ct.job_status->>'type' = 'success'
            "#,
        )
        .bind(&image_name)
        .bind(&image_tag)
        .fetch_optional(&mut *txn)
        .await?
        .ok_or_else(|| async_graphql::Error::new("capture connector tag is not ready"))?;

        let plane_name = if let Some(live) = live_capture {
            let current = live
                .get::<Option<String>, _>("data_plane_name")
                .ok_or_else(|| async_graphql::Error::new("data plane not found or unauthorized"))?;
            if data_plane
                .as_deref()
                .is_some_and(|selected| selected != current)
            {
                return Err(async_graphql::Error::new(
                    "data plane differs from the live capture",
                ));
            }
            current
        } else {
            select_new_capture_plane(capture_name.as_str(), data_plane.as_deref(), &mut txn).await?
        };
        if !super::may_access(ctx, &plane_name, models::Capability::Read)? {
            return Err(async_graphql::Error::new(
                "data plane not found or unauthorized",
            ));
        }
        let plane = env
            .snapshot()
            .data_plane_by_catalog_name(&plane_name)
            .ok_or_else(|| async_graphql::Error::new("data plane not found or unauthorized"))?;
        if tokens::jwt::parse_base64_hmac_keys(plane.hmac_keys.iter().take(1)).is_err() {
            return Err(async_graphql::Error::new(
                "data plane not found or unauthorized",
            ));
        }

        if copy_live {
            let inserted = sqlx::query_scalar::<_, models::Id>(
                r#"
                INSERT INTO draft_specs (draft_id, catalog_name, spec, spec_type, expect_pub_id)
                SELECT $1, ls.catalog_name, ls.spec, ls.spec_type, ls.last_pub_id
                FROM live_specs ls
                WHERE ls.catalog_name = $2 AND ls.spec_type = 'capture' AND ls.spec IS NOT NULL
                ON CONFLICT (draft_id, catalog_name) DO NOTHING
                RETURNING id
                "#,
            )
            .bind(draft_id)
            .bind(capture_name.as_str())
            .fetch_optional(&mut *txn)
            .await?;
            if inserted.is_none() {
                return Err(async_graphql::Error::new(
                    "capture was staged concurrently; retry",
                ));
            }
            crate::draft::touch(draft_id, &mut txn).await?;
        }

        let update_only = model
            .auto_discover
            .as_ref()
            .is_some_and(|policy| !policy.add_new_bindings);
        let row = sqlx::query(
            r#"
            INSERT INTO discovers (
                draft_id, capture_name, connector_tag_id, endpoint_config,
                update_only, data_plane_name
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, created_at, updated_at
            "#,
        )
        .bind(draft_id)
        .bind(capture_name.as_str())
        .bind(tag_id)
        .bind(crate::TextJson(config.config.clone()))
        .bind(update_only)
        .bind(&plane_name)
        .fetch_one(&mut *txn)
        .await?;

        let discover = Discover {
            id: row.try_get("id")?,
            draft_id,
            capture_name,
            data_plane_name: plane_name,
            status: DiscoverStatus::Queued,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        };
        txn.commit().await?;
        Ok(discover)
    }
}

async fn select_new_capture_plane(
    capture_name: &str,
    selected: Option<&str>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> async_graphql::Result<String> {
    let partition = sqlx::query(
        r#"
        SELECT catalog_prefix::text AS prefix, spec::text AS spec
        FROM storage_mappings
        WHERE starts_with($1, catalog_prefix::text)
        ORDER BY length(catalog_prefix::text) DESC
        LIMIT 1
        "#,
    )
    .bind(capture_name)
    .fetch_optional(&mut **txn)
    .await?
    .ok_or_else(|| async_graphql::Error::new("no storage mapping for capture"))?;
    let prefix: String = partition.try_get("prefix")?;
    let spec: models::StorageDef = serde_json::from_str(partition.try_get("spec")?)?;

    let recovery_name = format!("recovery/{capture_name}");
    let recovery = sqlx::query(
        r#"
        SELECT catalog_prefix::text AS prefix, spec::text AS spec
        FROM storage_mappings
        WHERE starts_with($1, catalog_prefix::text)
        ORDER BY length(catalog_prefix::text) DESC
        LIMIT 1
        "#,
    )
    .bind(&recovery_name)
    .fetch_optional(&mut **txn)
    .await?
    .ok_or_else(|| async_graphql::Error::new("no recovery storage mapping for capture"))?;
    let recovery_prefix: String = recovery.try_get("prefix")?;
    let recovery_spec: models::StorageDef = serde_json::from_str(recovery.try_get("spec")?)?;
    if recovery_prefix != format!("recovery/{prefix}")
        || (!recovery_spec.data_planes.is_empty() && recovery_spec.data_planes != spec.data_planes)
    {
        return Err(async_graphql::Error::new(
            "storage mapping data planes do not match",
        ));
    }

    match selected {
        Some(name) if prefix == "ops/" || spec.data_planes.iter().any(|plane| plane == name) => {
            Ok(name.to_owned())
        }
        Some(_) => Err(async_graphql::Error::new(
            "data plane is not in the storage mapping",
        )),
        None => {
            spec.data_planes.first().cloned().ok_or_else(|| {
                async_graphql::Error::new("storage mapping has no primary data plane")
            })
        }
    }
}

#[cfg(test)]
mod test;
