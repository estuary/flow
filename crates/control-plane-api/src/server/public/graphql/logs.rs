//! GraphQL types for the logs of an asynchronous job, shared by every type
//! which exposes logs because a GraphQL type cannot be defined twice. Each
//! of those types fetches and pages its own logs.

use async_graphql::types::connection;

use super::TimestampCursor;

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct LogLine {
    pub(super) logged_at: chrono::DateTime<chrono::Utc>,
    pub(super) stream: String,
    pub(super) line: String,
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
