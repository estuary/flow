use async_graphql::types::connection;

use super::TimestampCursor;

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 1_000;

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
    status: models::discovers::JobStatus,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[async_graphql::ComplexObject]
impl Discover {
    /// Errors currently recorded on the associated draft.
    async fn errors(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<Vec<models::draft_error::Error>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        super::drafts::errors_for_draft(self.draft_id, user_id, &env.pg_pool).await
    }

    /// Discovery logs in timestamp order. Pagination is best effort, and logs
    /// may arrive after discovery completes.
    /// `first` controls page size and cannot exceed 1000 log lines.
    async fn logs(
        &self,
        ctx: &async_graphql::Context<'_>,
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
                if limit > MAX_PAGE_SIZE {
                    return Err(async_graphql::Error::new("first cannot exceed 1000"));
                }
                let after_time = after.map(|cursor| cursor.0);
                let rows = sqlx::query!(
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
                    self.id as models::Id,
                    user_id,
                    after_time,
                    (limit + 1) as i64,
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;
                let mut result = LogLineConnection::new(after_time.is_some(), has_next);
                for row in rows.into_iter().take(limit) {
                    let logged_at = row.logged_at;
                    result.edges.push(connection::Edge::new(
                        TimestampCursor(logged_at),
                        LogLine {
                            logged_at,
                            stream: row.stream,
                            line: row.log_line,
                        },
                    ));
                }
                Ok(result)
            },
        )
        .await
    }
}

#[derive(Debug, Default)]
pub struct DiscoversQuery;

#[async_graphql::Object]
impl DiscoversQuery {
    /// Returns a discover visible to the caller, or null.
    async fn discover(
        &self,
        ctx: &async_graphql::Context<'_>,
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
    /// Queue discovery for a capture using its staged or live definition.
    /// Discovery updates the given draft with its results.
    async fn create_discover(
        &self,
        ctx: &async_graphql::Context<'_>,
        draft_id: models::Id,
        capture_name: models::Name,
        #[graphql(
            desc = "Optional data plane name for discovery. Selected automatically when omitted."
        )]
        data_plane: Option<String>,
    ) -> async_graphql::Result<Discover> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let owned = sqlx::query_scalar!(
            r#"SELECT EXISTS (
                SELECT 1 FROM drafts WHERE id = $1 AND user_id = $2
            ) AS "owned!""#,
            draft_id as models::Id,
            user_id,
        )
        .fetch_one(&env.pg_pool)
        .await?;
        if !owned {
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

        // Authorization may wait for a new snapshot, so acquire the connection
        // and draft lock only after it completes. Recheck ownership in case the
        // draft was deleted during preflight. KEY SHARE avoids lock inversion
        // with stageDraftSpecs, which touches drafts after updating draft_specs.
        let mut txn = env.pg_pool.begin().await?;
        let owned = sqlx::query_scalar!(
            r#"SELECT id AS "id!: models::Id"
               FROM drafts WHERE id = $1 AND user_id = $2 FOR KEY SHARE"#,
            draft_id as models::Id,
            user_id,
        )
        .fetch_optional(&mut *txn)
        .await?;
        if owned.is_none() {
            return Err(async_graphql::Error::new("draft not found"));
        }

        let drafted = sqlx::query!(
            r#"
            SELECT spec::text AS spec, spec_type::text AS spec_type
            FROM draft_specs
            WHERE draft_id = $1 AND catalog_name = $2
            FOR UPDATE
            "#,
            draft_id as models::Id,
            capture_name.as_str() as &str,
        )
        .fetch_optional(&mut *txn)
        .await?;

        // Plane selection is based on the live capture even when its definition
        // cannot be disclosed to this caller or the draft contains edits.
        let live = sqlx::query!(
            r#"
            SELECT ls.spec::text AS spec, ls.spec_type::text AS spec_type,
                   dp.data_plane_name::text AS "data_plane_name?"
            FROM live_specs ls
            LEFT JOIN data_planes dp ON dp.id = ls.data_plane_id
            WHERE ls.catalog_name = $1
            FOR SHARE OF ls
            "#,
            capture_name.as_str() as &str,
        )
        .fetch_optional(&mut *txn)
        .await?;
        let live_capture = live
            .as_ref()
            .filter(|row| row.spec_type.as_deref() == Some("capture") && row.spec.is_some());

        let copy_live = drafted.is_none();
        let model_text: &str = if let Some(row) = drafted.as_ref() {
            if row.spec_type.as_deref() != Some("capture") {
                return Err(async_graphql::Error::new("draft entry is not a capture"));
            }
            row.spec
                .as_deref()
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
            row.spec.as_deref().expect("live capture has a definition")
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

        let tag_id = sqlx::query_scalar!(
            r#"
            SELECT ct.id AS "id!: models::Id" FROM connector_tags ct
            JOIN connectors c ON c.id = ct.connector_id
            WHERE c.image_name = $1 AND ct.image_tag = $2
              AND ct.protocol = 'capture' AND ct.job_status->>'type' = 'success'
            "#,
            image_name,
            image_tag,
        )
        .fetch_optional(&mut *txn)
        .await?
        .ok_or_else(|| async_graphql::Error::new("capture connector tag is not ready"))?;

        let plane_name = if let Some(live) = live_capture {
            let current = live.data_plane_name.as_ref().ok_or_else(|| {
                env.snapshot().request_refresh();
                async_graphql::Error::new("data plane not found or unauthorized")
            })?;
            if data_plane
                .as_deref()
                .is_some_and(|selected| selected != current)
            {
                return Err(async_graphql::Error::new(
                    "data plane differs from the live capture",
                ));
            }
            current.clone()
        } else {
            let tenant = capture_name.as_str().split_inclusive('/').next().unwrap();
            let storage_rows =
                crate::storage_mappings::resolve_storage_mappings(vec![tenant], &mut *txn).await?;
            let (mappings, mapping_planes) =
                crate::storage_mappings::join_storage_mappings(storage_rows)?;
            let mapping = mappings
                .lookup(capture_name.as_str())
                .ok_or_else(|| async_graphql::Error::new("no storage mapping for capture"))?;
            let prefix = &mapping.catalog_prefix;
            let planes = &mapping_planes[prefix];
            match data_plane.as_deref() {
                // The `ops/` mapping lists no planes because ops catalogs are
                // created into every plane, including one being created right now.
                Some(name)
                    if prefix.as_str() == "ops/" || planes.iter().any(|plane| plane == name) =>
                {
                    name.to_owned()
                }
                Some(name) => {
                    return Err(anyhow::anyhow!(
                        "storage mapping {prefix} doesn't permit data plane {name}"
                    )
                    .into());
                }
                None => planes
                    .first()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "storage mapping {prefix} is missing associated data planes"
                        )
                    })?
                    .clone(),
            }
        };
        if !super::may_access(ctx, &plane_name, models::Capability::Read)? {
            env.snapshot().request_refresh();
            return Err(async_graphql::Error::new(
                "data plane not found or unauthorized",
            ));
        }
        let plane = env
            .snapshot()
            .data_plane_by_catalog_name(&plane_name)
            .ok_or_else(|| {
                env.snapshot().request_refresh();
                async_graphql::Error::new("data plane not found or unauthorized")
            })?;
        if plane.connector_route().is_err() {
            env.snapshot().request_refresh();
            return Err(async_graphql::Error::new(
                "data plane not found or unauthorized",
            ));
        }

        if copy_live {
            let inserted = sqlx::query_scalar!(
                r#"
                INSERT INTO draft_specs (draft_id, catalog_name, spec, spec_type, expect_pub_id)
                SELECT $1, ls.catalog_name, ls.spec, ls.spec_type, ls.last_pub_id
                FROM live_specs ls
                WHERE ls.catalog_name = $2 AND ls.spec_type = 'capture' AND ls.spec IS NOT NULL
                ON CONFLICT (draft_id, catalog_name) DO NOTHING
                RETURNING id AS "id!: models::Id"
                "#,
                draft_id as models::Id,
                capture_name.as_str() as &str,
            )
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
        let row = sqlx::query!(
            r#"
            INSERT INTO discovers (
                draft_id, capture_name, connector_tag_id, endpoint_config,
                update_only, data_plane_name
            ) VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id AS "id!: models::Id", created_at, updated_at
            "#,
            draft_id as models::Id,
            capture_name.as_str() as &str,
            tag_id as models::Id,
            crate::TextJson(config.config.clone()) as crate::TextJson<models::RawValue>,
            update_only,
            plane_name,
        )
        .fetch_one(&mut *txn)
        .await?;

        let discover = Discover {
            id: row.id,
            draft_id,
            capture_name,
            data_plane_name: plane_name,
            status: models::discovers::JobStatus::Queued,
            created_at: row.created_at,
            updated_at: row.updated_at,
        };
        txn.commit().await?;
        Ok(discover)
    }
}

async fn fetch_discover(
    id: models::Id,
    user_id: uuid::Uuid,
    pool: &sqlx::PgPool,
) -> async_graphql::Result<Option<Discover>> {
    let row = sqlx::query!(
        r#"
        SELECT di.id AS "id!: models::Id", di.draft_id AS "draft_id!: models::Id",
               di.capture_name AS "capture_name!: models::Name", di.data_plane_name,
               di.job_status AS "status!: sqlx::types::Json<models::discovers::JobStatus>",
               di.created_at, di.updated_at
        FROM discovers di
        JOIN drafts d ON d.id = di.draft_id
        WHERE di.id = $1 AND d.user_id = $2
        "#,
        id as models::Id,
        user_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| Discover {
        id: row.id,
        draft_id: row.draft_id,
        capture_name: row.capture_name,
        data_plane_name: row.data_plane_name,
        status: row.status.0,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }))
}

#[cfg(test)]
mod test;
