use async_graphql::{
    types::{connection, Json},
    Context,
};
use chrono::{DateTime, Utc};
use models::status::AlertType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::api::{App, ControlClaims};

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

pub struct AlertLoader(pub sqlx::PgPool);

/// A typed key for loading all of the currently firing alerts for a given `catalog_name`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FiringAlerts {
    pub catalog_name: String,
}

impl async_graphql::dataloader::Loader<FiringAlerts> for AlertLoader {
    type Value = Vec<Alert>;
    type Error = String;

    async fn load(
        &self,
        keys: &[FiringAlerts],
    ) -> Result<std::collections::HashMap<FiringAlerts, Self::Value>, Self::Error> {
        use itertools::Itertools;
        let catalog_names = keys
            .iter()
            .map(|k| k.catalog_name.as_str())
            .collect::<Vec<_>>();
        let rows = sqlx::query!(
            r#"select
            alert_type as "alert_type: AlertType",
            catalog_name,
            fired_at,
            resolved_at,
            arguments as "arguments: agent_sql::TextJson<async_graphql::Value>"
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
                let key = FiringAlerts {
                    catalog_name: row.catalog_name.clone(),
                };
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

/// Get alerts from alert_history by catalog_name
pub async fn list_alerts_firing(
    ctx: &Context<'_>,
    prefixes: Vec<String>,
) -> async_graphql::Result<Vec<Alert>> {
    let app = ctx.data::<Arc<App>>()?;
    let claims = ctx.data::<ControlClaims>()?;

    // Verify user authorization
    let authorized_names = app
        .verify_user_authorization(claims, prefixes.clone(), models::Capability::Read)
        .await
        .map_err(|e| async_graphql::Error::new(format!("Authorization failed: {}", e)))?;

    if authorized_names.is_empty() {
        return Ok(vec![]);
    }

    // Query alert_history table
    let rows = sqlx::query!(
        r#"
        select
            alert_type as "alert_type!: AlertType",
            catalog_name as "catalog_name!: String",
            fired_at,
            resolved_at,
            arguments as "arguments!: agent_sql::TextJson<async_graphql::Value>"
        from unnest($1::text[]) p(prefix)
        join alert_history a on a.resolved_at is null and starts_with(a.catalog_name, p.prefix)
        order by a.fired_at desc
        limit 100
        "#,
        &authorized_names as &[String],
    )
    .fetch_all(&app.pg_pool)
    .await
    .map_err(|e| async_graphql::Error::new(format!("Failed to fetch alerts: {}", e)))?;

    let results = rows
        .into_iter()
        .map(|row| Alert {
            alert_type: row.alert_type,
            catalog_name: row.catalog_name,
            fired_at: row.fired_at,
            resolved_at: row.resolved_at,
            arguments: Json(row.arguments.0),
        })
        .collect();

    Ok(results)
}

pub type PaginatedAlerts = connection::Connection<DateTime<Utc>, Alert>;

pub async fn alert_history(
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

    // JFC, really?! ... yes, really. IDK why rustc couldn't infer the error
    // type, or why pagination requires 10 generic type parameters, but here we are.
    connection::query::<_, _, _, _, _, _, _, _, _, async_graphql::Error>(
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
                arguments as "arguments!: agent_sql::TextJson<async_graphql::Value>"
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
            async_graphql::Result::Ok(conn)
        },
    )
    .await
}
