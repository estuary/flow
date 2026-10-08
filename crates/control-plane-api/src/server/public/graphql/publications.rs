use async_graphql::types::connection;

use super::TimestampCursor;
use super::logs::{LogLine, LogLineConnection};

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PAGE_SIZE: usize = 1_000;

/// A user-initiated publication of a draft.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Publication {
    id: models::Id,
    draft_id: models::Id,
    dry_run: bool,
    /// Outcome so far. `type` is `queued` until the executor finishes.
    /// `lockFailures` is non-empty only for `buildIdLockFailure`.
    status: models::publications::JobStatus,
    /// Id of the publication's build. Null until success. On a committed
    /// publication it becomes `lastPubId` on every live specification it
    /// changed. A successful dry run records it too, but commits nothing,
    /// so no live specification carries it.
    pub_id: Option<models::Id>,
}

#[async_graphql::ComplexObject]
impl Publication {
    /// The draft's current errors. All jobs on a draft share them: they may
    /// predate this publication, and a later job replaces them. Empty once
    /// the draft has been deleted.
    async fn errors(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<Vec<models::draft_error::Error>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        super::drafts::errors_for_draft(self.draft_id, user_id, &env.pg_pool).await
    }

    /// Publication logs in timestamp order. Pagination is best effort, and
    /// logs may arrive after the publication completes. `first` controls
    /// page size and cannot exceed 1000 log lines.
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
                    JOIN publications p ON p.logs_token = l.token
                    WHERE p.id = $1 AND p.user_id = $2
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
pub struct PublicationsQuery;

#[async_graphql::Object]
impl PublicationsQuery {
    /// Returns a publication initiated by the caller, or null when no such
    /// publication exists or the caller did not initiate it; the two are
    /// indistinguishable.
    async fn publication(
        &self,
        ctx: &async_graphql::Context<'_>,
        id: models::Id,
    ) -> async_graphql::Result<Option<Publication>> {
        let env = ctx.data::<crate::Envelope>()?;
        let user_id = env.claims()?.subject().user_id;
        let row = sqlx::query!(
            r#"
            SELECT id AS "id!: models::Id", draft_id AS "draft_id!: models::Id", dry_run,
                   job_status AS "status!: sqlx::types::Json<models::publications::JobStatus>",
                   pub_id AS "pub_id: models::Id"
            FROM publications
            WHERE id = $1 AND user_id = $2
            "#,
            id as models::Id,
            user_id,
        )
        .fetch_optional(&env.pg_pool)
        .await?;

        Ok(row.map(|row| Publication {
            id: row.id,
            draft_id: row.draft_id,
            dry_run: row.dry_run,
            status: row.status.0,
            pub_id: row.pub_id,
        }))
    }
}

#[cfg(test)]
mod test;
