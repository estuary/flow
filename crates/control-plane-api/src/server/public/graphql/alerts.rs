use async_graphql::{
    Context,
    types::{Json, connection},
};
use chrono::{DateTime, Utc};
use models::status::AlertType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::server::{App, ControlClaims};

#[derive(Debug, Default)]
pub struct AlertsQuery;

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct AlertsBy {
    /// Show alerts for the given catalog namespace prefix.
    prefix: String,
    /// Optionally filter alerts by active status. If unspecified, both active
    /// and resolved alerts will be returned.
    active: Option<bool>,
}

#[async_graphql::Object]
impl AlertsQuery {
    /// Returns a list of alerts that are currently active for the given catalog
    /// prefixes.
    async fn alerts(
        &self,
        ctx: &Context<'_>,
        by: AlertsBy,
        started_at: Option<chrono::DateTime<chrono::Utc>>,
        before: Option<String>,
        last: Option<i32>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedAlerts> {
        fetch_alert_history_by_prefix(ctx, by, started_at, before, last, after, first).await
    }
}

/// An alert from the alert_history table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, async_graphql::SimpleObject)]
pub struct Alert {
    /// The type of the alert
    pub alert_type: AlertType,
    /// The catalog name that the alert pertains to.
    pub catalog_name: String,
    /// Time at which the alert became active.
    pub fired_at: DateTime<Utc>,
    /// The time at which the alert was resolved, or null if it is still active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<DateTime<Utc>>,
    /// The alert arguments contain additional details about the alert, which
    /// may be used in formatting the alert message.
    pub arguments: Json<async_graphql::Value>,
    // Note that resovled_arguments are omitted for now, because it's
    // unclear whether we really have a use case for them in the API.
    // pub resolved_arguments: Json<async_graphql::Value>,
}

/// A typed key for loading all of the currently active alerts for a given `catalog_name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActiveAlerts(pub String);

impl async_graphql::dataloader::Loader<ActiveAlerts> for super::PgDataLoader {
    type Value = Vec<Alert>;
    type Error = String;

    async fn load(
        &self,
        keys: &[ActiveAlerts],
    ) -> Result<std::collections::HashMap<ActiveAlerts, Self::Value>, Self::Error> {
        use itertools::Itertools;
        let catalog_names = keys.iter().map(|k| k.0.as_str()).collect::<Vec<_>>();
        let rows = sqlx::query!(
            r#"select
            alert_type as "alert_type: AlertType",
            catalog_name,
            fired_at,
            resolved_at,
            arguments as "arguments: crate::TextJson<async_graphql::Value>"
        from alert_history
        where catalog_name = any($1::text[])
        and resolved_at is null
        order by fired_at desc
            "#,
            &catalog_names as &[&str]
        )
        .fetch_all(&self.0)
        .await
        .map_err(|err| format!("failed to fetch alerts: {err:#}"))?;

        let result = rows
            .into_iter()
            .map(|row| {
                let key = ActiveAlerts(row.catalog_name.clone());
                let alert = Alert {
                    alert_type: row.alert_type,
                    catalog_name: row.catalog_name,
                    fired_at: row.fired_at,
                    resolved_at: row.resolved_at,
                    arguments: async_graphql::types::Json(row.arguments.0),
                };
                (key, alert)
            })
            .into_group_map();
        Ok(result)
    }
}

pub type PaginatedAlerts = connection::Connection<
    PaginatedAlertsCursor,
    Alert,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

pub struct PaginatedAlertsCursor {
    fired_at: DateTime<Utc>,
    catalog_name: String,
    alert_type: AlertType,
}
impl PaginatedAlertsCursor {
    fn into_parts(self) -> (Option<DateTime<Utc>>, Option<AlertType>, Option<String>) {
        (
            Some(self.fired_at),
            Some(self.alert_type),
            Some(self.catalog_name),
        )
    }
}

impl async_graphql::connection::CursorType for PaginatedAlertsCursor {
    type Error = anyhow::Error;

    /// Decode cursor from string.
    fn decode_cursor(s: &str) -> Result<Self, Self::Error> {
        use anyhow::Context;
        let mut splits = s.split(";");

        let Some(ts_str) = splits.next() else {
            anyhow::bail!("invalid alerts cursor, no timestamp: '{s}'");
        };
        let Some(alert_type_str) = splits.next() else {
            anyhow::bail!("invalid alerts cursor, no type: '{s}'");
        };
        let Some(catalog_name) = splits.next() else {
            anyhow::bail!("invalid alerts cursor, no name: '{s}'");
        };
        let fired_at = DateTime::parse_from_rfc3339(ts_str)
            .context("invalid alerts cursor")?
            .to_utc();
        let Some(alert_type) = AlertType::from_str(alert_type_str) else {
            anyhow::bail!("invalid alerts cursor, invalid alert type: '{s}'");
        };
        Ok(PaginatedAlertsCursor {
            fired_at,
            catalog_name: catalog_name.to_string(),
            alert_type,
        })
    }

    /// Encode cursor to string.
    fn encode_cursor(&self) -> String {
        format!(
            "{};{};{}",
            self.fired_at.to_rfc3339(),
            self.alert_type,
            self.catalog_name
        )
    }
}

const DEFAULT_PAGE_SIZE: usize = 20;

async fn fetch_alert_history_by_prefix(
    ctx: &Context<'_>,
    by: AlertsBy,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    before: Option<String>,
    last: Option<i32>,
    after: Option<String>,
    first: Option<i32>,
) -> async_graphql::Result<PaginatedAlerts> {
    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    // Verify user authorization
    let _ = app
        .verify_user_authorization_graphql(
            claims,
            started_at,
            vec![by.prefix.to_string()],
            models::Capability::Read,
        )
        .await?;

    connection::query_with::<PaginatedAlertsCursor, _, _, _, _>(
        after,
        before,
        first,
        last,
        |after, before, first, last| async move {
            let (rows, has_prev, has_next) = if let Some(after_cursor) = after {
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);
                let (rows, has_next) =
                    fetch_alerts_by_prefix_after(by, after_cursor, limit, &app.pg_pool).await?;
                // We cannot efficiently determine whether alerts exist before
                // this cursor, so return `hasPreviousPage: false`, as per the
                // spec.
                (rows, false, has_next)
            } else {
                let limit = last.unwrap_or(DEFAULT_PAGE_SIZE);
                let before_cursor = before.unwrap_or(PaginatedAlertsCursor {
                    fired_at: Utc::now() + chrono::Duration::minutes(1),
                    catalog_name: String::new(),
                    alert_type: AlertType::ShardFailed,
                });
                let (rows, has_prev) =
                    fetch_alerts_by_prefix_before(by, before_cursor, limit, &app.pg_pool).await?;
                // We cannot efficiently determine whether alerts exist after
                // this cursor, so return `hasNextPage: false`, as per the spec.
                (rows, has_prev, false)
            };

            let mut conn = connection::Connection::new(has_prev, has_next);

            for row in rows {
                let cursor = PaginatedAlertsCursor {
                    fired_at: row.fired_at,
                    alert_type: row.alert_type,
                    catalog_name: row.catalog_name.clone(),
                };
                let alert = Alert {
                    alert_type: row.alert_type,
                    catalog_name: row.catalog_name,
                    fired_at: row.fired_at,
                    resolved_at: row.resolved_at,
                    arguments: async_graphql::Json(row.arguments.0),
                };
                conn.edges.push(connection::Edge::new(cursor, alert));
            }
            async_graphql::Result::<PaginatedAlerts>::Ok(conn)
        },
    )
    .await
}

struct AlertRow {
    fired_at: DateTime<Utc>,
    alert_type: AlertType,
    catalog_name: String,
    resolved_at: Option<DateTime<Utc>>,
    arguments: crate::TextJson<async_graphql::Value>,
}

async fn fetch_alerts_by_prefix_before(
    AlertsBy { prefix, active }: AlertsBy,
    before: PaginatedAlertsCursor,
    last: usize,
    db: &sqlx::PgPool,
) -> sqlx::Result<(Vec<AlertRow>, bool)> {
    // Try fetching one more than the requested number of rows so that we can
    // determine `hasPreviousPage`.
    let limit = last + 1;
    let mut rows = sqlx::query_as!(
        AlertRow,
        r#"
        select
            alert_type as "alert_type!: AlertType",
            catalog_name as "catalog_name!: String",
            fired_at,
            resolved_at,
            arguments as "arguments!: crate::TextJson<async_graphql::Value>"
        from alert_history a
        where starts_with(a.catalog_name, $1)
            and (
                a.fired_at < $2::timestamptz
                or (a.fired_at = $2::timestamptz and a.alert_type < $3::alert_type)
                or (a.fired_at = $2::timestamptz and a.alert_type = $3::alert_type and a.catalog_name::text < $4)
            )
            and (
                $5::boolean is null
                or ($5::boolean = true and a.resolved_at is null)
                or ($5::boolean = false and a.resolved_at is not null)
            )
        order by a.fired_at desc, a.alert_type desc, a.catalog_name desc
        limit $6
        "#,
        prefix,
        before.fired_at as DateTime<Utc>,
        before.alert_type as AlertType,
        before.catalog_name,
        active,
        limit as i64,
    )
    .fetch_all(db)
    .await?;

    let has_more = rows.len() == limit;
    if has_more {
        rows.pop();
    }

    Ok((rows, has_more))
}

async fn fetch_alerts_by_prefix_after(
    AlertsBy { prefix, active }: AlertsBy,
    after: PaginatedAlertsCursor,
    first: usize,
    db: &sqlx::PgPool,
) -> sqlx::Result<(Vec<AlertRow>, bool)> {
    // Try fetching one more than the requested number of rows so that we can
    // determine `hasNextPage`.
    let limit = first + 1;
    let mut rows = sqlx::query_as!(
        AlertRow,
        r#"
        select
            alert_type as "alert_type!: AlertType",
            catalog_name as "catalog_name!: String",
            fired_at,
            resolved_at,
            arguments as "arguments!: crate::TextJson<async_graphql::Value>"
        from alert_history a
        where starts_with(a.catalog_name, $1)
            and (
                $2::boolean is null
                or ($2::boolean = true and a.resolved_at is null)
                or ($2::boolean = false and a.resolved_at is not null)
            )
            and (
                a.fired_at > $3::timestamptz
                or (a.fired_at = $3::timestamptz and a.alert_type > $4::alert_type)
                or (a.fired_at = $3::timestamptz and a.alert_type = $4::alert_type and a.catalog_name::text > $5)
            )
        order by a.fired_at asc, a.alert_type asc, a.catalog_name asc
        limit $6
        "#,
        prefix,
        active,
        after.fired_at as DateTime<Utc>,
        after.alert_type as AlertType,
        after.catalog_name,
        limit as i64,
    )
    .fetch_all(db)
    .await?;

    let has_more = rows.len() == limit;
    if has_more {
        rows.pop();
    }

    // Reverse the order of the rows so that they are ordered consistently when paginating forward and backward
    rows.reverse();
    Ok((rows, has_more))
}

/// Queries the history of alert for a single given live spec.
/// Note that this currently only returns alerts that are resolved, though
/// we could allow this to return active alerts as well if we wanted.
/// Note: Authorization checks must be performed by the caller.
/// This function does not perform any authorization checks.
pub async fn live_spec_alert_history_no_authz(
    ctx: &Context<'_>,
    catalog_name: &str,
    before_date: Option<String>,
    limit: i32,
) -> async_graphql::Result<PaginatedAlerts> {
    let app = ctx.data::<Arc<App>>()?;
    connection::query(
        None,
        before_date,
        None,
        Some(limit),
        |_: Option<PaginatedAlertsCursor>, before, _, limit| async move {
            let (before_ts, before_alert_type, before_name) = before.map(|c| c.into_parts()).unwrap_or_default();
            let effective_limit = limit.unwrap_or(20);
            let rows = sqlx::query!(
                r#"
            select
                alert_type as "alert_type!: AlertType",
                catalog_name as "catalog_name!: String",
                fired_at,
                resolved_at,
                arguments as "arguments!: crate::TextJson<async_graphql::Value>"
            from alert_history a
            where a.catalog_name = $1
                and a.resolved_at is not null
                and (
                    $2::timestamptz is null
                    or a.fired_at < $2::timestamptz
                    or (a.fired_at = $2::timestamptz and a.alert_type < $3::alert_type)
                    or (a.fired_at = $2::timestamptz and a.alert_type = $3::alert_type and a.catalog_name::text < $4)
                )
            order by a.fired_at desc, a.catalog_name desc
            limit $5
            "#,
                catalog_name,
                before_ts,
                before_alert_type as Option<AlertType>,
                before_name,
                effective_limit as i64,
            )
            .fetch_all(&app.pg_pool)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to fetch alerts: {}", e)))?;

            let has_prev_page = rows.len() == effective_limit;
            let mut conn = connection::Connection::new(has_prev_page, false);

            for row in rows {
                let cursor = PaginatedAlertsCursor {
                    fired_at: row.fired_at,
                    alert_type: row.alert_type,
                    catalog_name: row.catalog_name.clone(),
                };
                let alert = Alert {
                    alert_type: row.alert_type,
                    catalog_name: row.catalog_name,
                    fired_at: row.fired_at,
                    resolved_at: row.resolved_at,
                    arguments: async_graphql::Json(row.arguments.0),
                };
                conn.edges.push(connection::Edge::new(cursor, alert));
            }
            async_graphql::Result::<PaginatedAlerts>::Ok(conn)
        },
    )
    .await
}
