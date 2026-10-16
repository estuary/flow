//! Log line types and a pager over one job's logs, shared by the job types
//! that expose logs. A GraphQL type cannot be defined twice, so they cannot
//! each define their own.
//!
//! The pager is keyed by `logs_token` alone and does no authorization of its
//! own: a caller holds the token only for a job it has already authorized.

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

/// Returns a page of the logs written under `logs_token`, in timestamp order.
pub(super) async fn page(
    pg_pool: &sqlx::PgPool,
    logs_token: uuid::Uuid,
    after: Option<String>,
    first: Option<i32>,
) -> async_graphql::Result<LogLineConnection> {
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
                SELECT logged_at, stream, log_line
                FROM internal.log_lines
                WHERE token = $1
                  AND ($2::timestamptz IS NULL OR logged_at > $2)
                ORDER BY logged_at ASC
                LIMIT $3
                "#,
                logs_token,
                after_time,
                (limit + 1) as i64,
            )
            .fetch_all(pg_pool)
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
