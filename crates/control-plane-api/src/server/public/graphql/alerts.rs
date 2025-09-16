use async_graphql::{
    types::{connection, Json},
    Context,
};
use chrono::{DateTime, Utc};
use models::status::AlertType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::server::{App, ControlClaims};

#[derive(Debug, Default)]
pub struct AlertsQuery;

#[async_graphql::Object]
impl AlertsQuery {
    /// Returns a list of alerts that are currently firing for the given catalog
    /// prefixes.
    async fn alerts(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Show alerts for the given catalog prefixes")] prefix: String,
        #[graphql(desc = "Optionally filter alerts by whether or not they are firing")]
        firing: Option<bool>,
        before: Option<String>,
        last: Option<i32>,
    ) -> async_graphql::Result<PaginatedAlerts> {
        prefix_alert_history(ctx, prefix.as_str(), firing, before, last).await
    }
}

/// An alert from the alert_history table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, async_graphql::SimpleObject)]
pub struct Alert {
    /// The type of the alert
    pub alert_type: AlertType,
    /// The catalog name that the alert pertains to.
    pub catalog_name: String,
    /// Time at which the alert started firing.
    pub fired_at: DateTime<Utc>,
    /// The time at which the alert was resolved, or null if it is still firing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<DateTime<Utc>>,
    /// The alert arguments contain additional details about the alert, which
    /// may be used in formatting the alert message.
    pub arguments: Json<async_graphql::Value>,
    // Note that resovled_arguments are omitted for now, because it's
    // unclear whether we really have a use case for them in the API.
    // pub resolved_arguments: Json<async_graphql::Value>,
}

/// A typed key for loading all of the currently firing alerts for a given `catalog_name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FiringAlerts(pub String);

impl async_graphql::dataloader::Loader<FiringAlerts> for super::PgDataLoader {
    type Value = Vec<Alert>;
    type Error = String;

    async fn load(
        &self,
        keys: &[FiringAlerts],
    ) -> Result<std::collections::HashMap<FiringAlerts, Self::Value>, Self::Error> {
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
                let key = FiringAlerts(row.catalog_name.clone());
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
    DateTime<Utc>,
    Alert,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

pub async fn prefix_alert_history(
    ctx: &Context<'_>,
    prefix: &str,
    filter_firing: Option<bool>,
    before_timestamp: Option<String>,
    limit: Option<i32>,
) -> async_graphql::Result<PaginatedAlerts> {
    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    // Verify user authorization
    let _ = app
        .verify_user_authorization(claims, vec![prefix.to_string()], models::Capability::Read)
        .await
        .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

    connection::query(
        None,
        before_timestamp,
        None,
        limit,
        |_after, before, _first, last| async move {
            let effective_limit = last.unwrap_or(20);

            let rows = sqlx::query!(
                r#"
        select
            alert_type as "alert_type!: AlertType",
            catalog_name as "catalog_name!: String",
            fired_at,
            resolved_at,
            arguments as "arguments!: crate::TextJson<async_graphql::Value>"
        from alert_history a
        where starts_with(a.catalog_name, $1)
            and a.fired_at < $2
            and case $3::boolean
              when true then a.resolved_at is null
              when false then a.resolved_at is not null
              else true
            end
        order by a.fired_at desc
        limit $4
        "#,
                prefix,
                before.unwrap_or(Utc::now()),
                filter_firing,
                effective_limit as i64,
            )
            .fetch_all(&app.pg_pool)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to fetch alerts: {}", e)))?;

            let has_prev_page = rows.len() == effective_limit;
            let mut conn = connection::Connection::new(has_prev_page, false);

            for row in rows {
                let fired_at = row.fired_at;
                let alert = Alert {
                    alert_type: row.alert_type,
                    catalog_name: row.catalog_name,
                    fired_at,
                    resolved_at: row.resolved_at,
                    arguments: async_graphql::Json(row.arguments.0),
                };
                conn.edges.push(connection::Edge::new(fired_at, alert));
            }
            async_graphql::Result::<PaginatedAlerts>::Ok(conn)
        },
    )
    .await
}

/// Queries the history of alert for a single given live spec.
/// Note that this currently only returns alerts that are resolved, though
/// we could allow this to return firing alerts as well if we wanted.
pub async fn live_spec_alert_history(
    ctx: &Context<'_>,
    catalog_name: &str,
    before_date: Option<String>,
    limit: i32,
) -> async_graphql::Result<PaginatedAlerts> {
    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    // Verify user authorization
    let _ = app
        .verify_user_authorization(
            claims,
            vec![catalog_name.to_string()],
            models::Capability::Read,
        )
        .await
        .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

    connection::query(
        None,
        before_date,
        None,
        Some(limit),
        |_, before, _, limit| async move {
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
                and a.fired_at < $2
                and a.resolved_at is not null
            order by a.fired_at desc
            limit $3
            "#,
                catalog_name,
                before.unwrap_or(Utc::now()),
                effective_limit as i64,
            )
            .fetch_all(&app.pg_pool)
            .await
            .map_err(|e| async_graphql::Error::new(format!("Failed to fetch alerts: {}", e)))?;

            let has_prev_page = rows.len() == effective_limit;
            let mut conn = connection::Connection::new(has_prev_page, false);

            for row in rows {
                let fired_at = row.fired_at;
                let alert = Alert {
                    alert_type: row.alert_type,
                    catalog_name: row.catalog_name,
                    fired_at,
                    resolved_at: row.resolved_at,
                    arguments: async_graphql::Json(row.arguments.0),
                };
                conn.edges.push(connection::Edge::new(fired_at, alert));
            }
            async_graphql::Result::<PaginatedAlerts>::Ok(conn)
        },
    )
    .await
}
