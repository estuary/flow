//! Low-level functions for firing and resolving alerts, and querying open alerts.
use anyhow::Context;
use chrono::{DateTime, Utc};
use models::status::AlertType;
use serde::{Deserialize, Serialize};
use sqlx::types::Json;

/// Used for alert arguments and resolved_arguments
pub type ArgsObject = std::collections::BTreeMap<String, serde_json::Value>;

/// Represents the firing of an alert, which has not happened yet.
#[derive(Debug)]
pub struct FireAlert {
    /// A new unique id for the notification task, which must not already exist in the database.
    pub id: models::Id,
    pub catalog_name: String,
    pub alert_type: AlertType,
    pub fired_at: DateTime<Utc>,
    /// The alert arguments, to which recipients will be added to form to the
    /// final `arguments` for the `alert_history` table.
    pub base_arguments: ArgsObject,
}

/// Represents the resolution of an alert, which has not happened yet.
#[derive(Debug)]
pub struct ResolveAlert {
    /// The primary key of the alert to resolve
    pub id: models::Id,
    pub catalog_name: String,
    pub alert_type: AlertType,
    pub resolved_at: DateTime<Utc>,
    /// The optional base resolved_arguments. If provided, then recipients will
    /// be added and the result used as `resolved_arguments` in `alert_history`.
    /// If not provided, then the alert resolution will use the existing
    /// `arguements` instead, and the recipients will _not_ be resolved again.
    /// In other words, if you don't provide `base_resolved_arguments`, then the
    /// resolution emails will only be sent to the exact set of recipients that
    /// were used for the fired emails.
    pub base_resolved_arguments: Option<ArgsObject>,
}

/// Represents the firing or resolution of an alert.
#[derive(Debug)]
pub enum AlertAction {
    Fire(FireAlert),
    Resolve(ResolveAlert),
}

/// An alert from the alert_history table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, async_graphql::SimpleObject)]
pub struct Alert {
    /// The primary key of this alert
    #[graphql(skip)]
    pub id: models::Id,
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
    pub arguments: async_graphql::Json<ArgsObject>,
    /// Optional arguments for the resolution. Most commonly, this will be null
    /// and the regular `arguments` would be used during resolution in that case.
    pub resolved_arguments: Option<async_graphql::Json<ArgsObject>>,
}

/// Returns all open alerts for the given catalog names.
pub async fn fetch_open_alerts_by_catalog_name(
    catalog_names: &[&str],
    pool: &sqlx::PgPool,
) -> anyhow::Result<Vec<Alert>> {
    let rows = sqlx::query!(
        r#"select
        id as "id: models::Id",
        alert_type as "alert_type: AlertType",
        catalog_name,
        fired_at,
        resolved_at,
        arguments as "arguments: Json<ArgsObject>"
    from alert_history
    where catalog_name = any($1::text[])
    and resolved_at is null
    order by fired_at desc
        "#,
        &catalog_names as &[&str]
    )
    .fetch_all(pool)
    .await?;

    let result = rows
        .into_iter()
        .map(|row| Alert {
            id: row.id,
            alert_type: row.alert_type,
            catalog_name: row.catalog_name,
            fired_at: row.fired_at,
            resolved_at: row.resolved_at,
            arguments: async_graphql::Json(row.arguments.0),
            resolved_arguments: None,
        })
        .collect();
    Ok(result)
}

pub async fn fetch_alert_by_id(id: models::Id, db: &sqlx::PgPool) -> anyhow::Result<Option<Alert>> {
    let row = sqlx::query!(
        r#"select
        id as "id: models::Id",
        catalog_name,
        alert_type as "alert_type: AlertType",
        fired_at,
        resolved_at,
        arguments as "arguments: Json<ArgsObject>",
        resolved_arguments as "resolved_arguments: Json<ArgsObject>"
        from alert_history
        where id = $1
        "#,
        id as models::Id,
    )
    .fetch_optional(db)
    .await?;

    let Some(alert) = row else {
        return Ok(None);
    };

    Ok(Some(Alert {
        id: alert.id,
        catalog_name: alert.catalog_name,
        alert_type: alert.alert_type,
        fired_at: alert.fired_at,
        resolved_at: alert.resolved_at,
        arguments: async_graphql::types::Json(alert.arguments.0),
        resolved_arguments: alert
            .resolved_arguments
            .map(|r| async_graphql::types::Json(r.0)),
    }))
}

/// Applies the given list of alert actions, inserting or updating
/// `alert_history` as necessary. This function resolves alert subscriptions,
/// and creates alert notification tasks for each fired alert.
pub async fn apply_alert_actions(
    actions: Vec<AlertAction>,
    txn: &mut sqlx::PgConnection,
) -> anyhow::Result<()> {
    for action in actions {
        match action {
            AlertAction::Fire(FireAlert {
                id,
                catalog_name,
                alert_type,
                fired_at,
                base_arguments: arguments,
            }) => {
                fire_alert(
                    id,
                    catalog_name.as_str(),
                    alert_type,
                    fired_at,
                    arguments,
                    txn,
                )
                .await
                .with_context(|| format!("firing {alert_type} alert for {catalog_name}"))?;
            }
            AlertAction::Resolve(ResolveAlert {
                id,
                catalog_name,
                alert_type,
                resolved_at,
                base_resolved_arguments: resolved_arguments,
            }) => {
                resolve_alert(
                    id,
                    catalog_name.as_str(),
                    alert_type,
                    resolved_at,
                    resolved_arguments,
                    txn,
                )
                .await
                .with_context(|| format!("resolving {alert_type} alert for {catalog_name}"))?;
            }
        }
    }
    Ok(())
}

async fn fire_alert(
    id: models::Id,
    catalog_name: &str,
    alert_type: AlertType,
    fired_at: DateTime<Utc>,
    mut arguments: ArgsObject,
    txn: &mut sqlx::PgConnection,
) -> anyhow::Result<()> {
    add_recipients(catalog_name, alert_type, &mut arguments, txn)
        .await
        .context("adding recipients")?;

    sqlx::query!(
        r#"with add_history as (
            insert into alert_history
            (id, catalog_name, alert_type, fired_at, arguments)
            values ($1, $2::catalog_name, $3, $4, $5)
            returning id
        )
        insert into internal.tasks (task_id, task_type, parent_id, wake_at, inbox)
        select
            id,
            9,
            '0000000000000000'::flowid,
            now(),
            array[json_build_array('0000000000000000', '{"event": "fired"}'::json)]
        from add_history
            "#,
        id as models::Id,
        catalog_name as &str,
        alert_type as AlertType,
        fired_at,
        Json(arguments) as Json<ArgsObject>,
    )
    .execute(txn)
    .await?;

    tracing::info!(%id, %catalog_name, %alert_type, %fired_at, "fired alert");

    Ok(())
}

async fn resolve_alert(
    id: models::Id,
    catalog_name: &str,
    alert_type: AlertType,
    resolved_at: DateTime<Utc>,
    mut resolved_arguments: Option<ArgsObject>,
    txn: &mut sqlx::PgConnection,
) -> anyhow::Result<()> {
    if let Some(args) = resolved_arguments.as_mut() {
        add_recipients(catalog_name, alert_type, args, txn)
            .await
            .context("adding recipients")?;
    }
    sqlx::query!(
        r#"with history as (
            update alert_history
            set resolved_at = $2,
            resolved_arguments = $3
            where id = $1
            returning id
        )
        select internal.send_to_task(id, '0000000000000000'::flowid, '{"event": "resolved"}')
        from history
            "#,
        id as models::Id,
        resolved_at,
        resolved_arguments.map(|a| Json(a)) as Option<Json<ArgsObject>>,
    )
    .execute(txn)
    .await?;

    tracing::info!(%id, %catalog_name, %alert_type, %resolved_at, "resolved alert");

    Ok(())
}

pub async fn fetch_open_alerts_by_type(
    alert_types: &[AlertType],
    pool: &sqlx::PgPool,
) -> sqlx::Result<Vec<Alert>> {
    let rows = sqlx::query!(
        r#"select
            id as "id: models::Id",
            catalog_name,
            alert_type as "alert_type: AlertType",
            fired_at,
            arguments as "arguments: Json<ArgsObject>"
        from alert_history
        where resolved_at is null
        and alert_type = any($1::alert_type[])
        "#,
        alert_types as &[AlertType]
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|alert| Alert {
            id: alert.id,
            catalog_name: alert.catalog_name,
            alert_type: alert.alert_type,
            fired_at: alert.fired_at,
            arguments: async_graphql::Json(alert.arguments.0),
            resolved_at: None,
            resolved_arguments: None,
        })
        .collect())
}

async fn add_recipients(
    catalog_name: &str,
    alert_type: AlertType,
    args: &mut ArgsObject,
    txn: &mut sqlx::PgConnection,
) -> anyhow::Result<()> {
    let recips = fetch_recipients(catalog_name, alert_type, txn).await?;
    let recips_json = serde_json::to_value(recips).context("serializing recipients")?;
    args.insert("recipients".to_string(), recips_json);
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AlertRecipient {
    email: String,
    full_name: Option<String>,
}

async fn fetch_recipients(
    alert_catalog_name: &str,
    alert_type: AlertType,
    txn: &mut sqlx::PgConnection,
) -> anyhow::Result<Vec<AlertRecipient>> {
    let rows = sqlx::query_as!(
        AlertRecipient,
        r#"select
            asub.email as "email!: String",
            u.raw_user_meta_data->>'full_name' as "full_name: String"
        from alert_subscriptions asub
        left join auth.users u on asub.email = u.email and u.is_sso_user is false
        where $1 ^@ asub.catalog_prefix
        and $2::alert_type = any(asub.include_alert_types)
        and asub.email is not null
        "#,
        alert_catalog_name,
        alert_type as AlertType,
    )
    .fetch_all(txn)
    .await?;
    Ok(rows)
}
