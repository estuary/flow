use async_graphql::{
    types::Json, Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject,
};
use chrono::{DateTime, Utc};
use models::status::AlertType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, sync::Arc};

use crate::api::{
    public::graphql::id::{GraphqlId, TypedId},
    App, ControlClaims,
};

/// An alert from the alert_history table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, async_graphql::SimpleObject)]
#[graphql(complex)]
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
    // pub resolved_arguments: Option<models::RawValue>,
}

impl Alert {
    fn get_id(&self) -> AlertId {
        AlertId::new((self.catalog_name.clone(), self.alert_type, self.fired_at))
    }
}

#[async_graphql::ComplexObject]
impl Alert {
    pub async fn id(&self) -> AlertId {
        self.get_id()
    }
}

pub struct AlertLoader(pub sqlx::PgPool);

/// Identifies a specific alert by encoding the catalog_name, alert_type, and fired_at values.
pub type AlertId = GraphqlId<(String, AlertType, DateTime<Utc>)>;
async_graphql::scalar!(AlertId);

impl async_graphql::dataloader::Loader<AlertId> for AlertLoader {
    type Value = Alert;
    type Error = String;

    async fn load(
        &self,
        keys: &[AlertId],
    ) -> Result<std::collections::HashMap<AlertId, Self::Value>, Self::Error> {
        let names: Vec<&str> = keys.iter().map(|key| key.0.as_str()).collect();
        let types: Vec<AlertType> = keys.iter().map(|k| k.1).collect();
        let fired_ats: Vec<DateTime<Utc>> = keys.iter().map(|k| k.2).collect();
        let rows = sqlx::query!(r#"
            select
            alert_history.catalog_name,
            alert_history.alert_type as "alert_type: AlertType",
            alert_history.fired_at as "fired_at: DateTime<Utc>",
            resolved_at as "resolved_at: DateTime<Utc>",
            arguments as "arguments: agent_sql::TextJson<serde_json::Value>"
            from unnest($1::text[], $2::alert_type[], $3::timestamptz[]) input(catalog_name, alert_type, fired_at)
            join alert_history on input.catalog_name = alert_history.catalog_name
                and input.alert_type = alert_history.alert_type
                and input.fired_at = alert_history.fired_at
            "#,
            &names as &[&str],
            &types as &[AlertType],
            &fired_ats as &[DateTime<Utc>],
        ).fetch_all(&self.0).await.map_err(|e| format!("failed to load alerts: {e:#}"))?;

        let results = rows
            .into_iter()
            .map(|row| {
                let args = async_graphql::Value::from_json(row.arguments.0).unwrap();
                let alert = Alert {
                    catalog_name: row.catalog_name,
                    alert_type: row.alert_type,
                    fired_at: row.fired_at,
                    resolved_at: row.resolved_at,
                    arguments: async_graphql::Json(args),
                };
                let id = alert.get_id();
                (id, alert)
            })
            .collect();
        Ok(results)
    }
}

impl async_graphql::dataloader::Loader<String> for AlertLoader {
    type Value = Vec<Alert>;
    type Error = String;

    async fn load(
        &self,
        keys: &[String],
    ) -> Result<std::collections::HashMap<String, Self::Value>, Self::Error> {
        use itertools::Itertools;
        let rows = sqlx::query!(
            r#"select
            alert_type as "alert_type: AlertType",
            catalog_name,
            fired_at,
            resolved_at,
            arguments as "arguments: sqlx::types::Json<async_graphql::Value>"
        from alert_history
        where catalog_name = any($1::text[])
            "#,
            keys
        )
        .fetch_all(&self.0)
        .await
        .map_err(|err| format!("failed to fetch alerts: {err:#}"))?;

        let result = rows
            .into_iter()
            .map(|row| {
                let key = row.catalog_name.clone();
                let alert = Alert {
                    alert_type: row.alert_type,
                    catalog_name: key.clone(),
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
            arguments as "arguments!: sqlx::types::Json<async_graphql::Value>"
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
