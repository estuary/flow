use async_graphql::types::connection;

use super::TimestampCursor;
use super::logs::{LogLine, LogLineConnection};

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 1_000;

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
        let user_id = env.claims()?.subject().user_id;
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
        .fetch_optional(&env.pg_pool)
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
        let subject = env.claims()?.subject();

        super::verify_authorization(
            env,
            capture_name.as_str(),
            models::authz::Capability::SpecEdit,
        )
        .await?;

        let row = match crate::discovers::create(
            &env.pg_pool,
            env.snapshot(),
            &subject,
            draft_id,
            capture_name.as_str(),
            data_plane.as_deref(),
        )
        .await
        {
            Ok(row) => row,
            Err(error) => match error.downcast::<tonic::Status>() {
                Ok(status) => env.authorization_outcome(Err(status)).await?.1,
                Err(error) => return Err(error.into()),
            },
        };
        Ok(Discover {
            id: row.id,
            draft_id,
            capture_name,
            data_plane_name: row.data_plane_name,
            status: models::discovers::JobStatus::Queued,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[cfg(test)]
mod test;
