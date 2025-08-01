use async_graphql::{
    types::Json, Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject,
};
use chrono::{DateTime, Utc};
use models::status::AlertType;
use schemars::JsonSchema;
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
    pub arguments: Json<models::RawValue>,
    // Note that resovled_arguments are omitted for now, because it's
    // unclear whether we really have a use case for them in the API.
    // pub resolved_arguments: Option<models::RawValue>,
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
            arguments as "arguments!: models::RawValue"
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
            arguments: Json(row.arguments),
        })
        .collect();

    Ok(results)
}
