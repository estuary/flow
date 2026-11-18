use crate::alerts::{AlertViewRow, evaluate_alert_actions};
use anyhow::Context;
use control_plane_api::alerts::{AlertAction, ArgsObject, ResolveAlert};
use models::status::AlertType;
use std::collections::BTreeMap;

/// What a tenant controller step observed about one of the tenant's alerts.
pub enum Observation {
    /// An open alert fired before `period_start` resolves, and the alert fires
    /// again, so that each period's alert notifies.
    Firing {
        arguments: ArgsObject,
        period_start: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// An open alert resolves with `arguments`, or with the arguments it fired
    /// with if there are none.
    Resolved { arguments: Option<ArgsObject> },
    /// An open alert resolves without notifications.
    ResolvedSilently,
}

pub fn catalog_name(tenant: &str, alert_type: AlertType) -> String {
    format!("{tenant}alerts/{alert_type}")
}

/// Fires and resolves the tenant's alerts of each observed type to match its
/// observation. Alerts of other types are left as they are: another step
/// evaluates them, or this step couldn't.
pub async fn apply(
    pool: &sqlx::PgPool,
    tenant: &str,
    observed: BTreeMap<AlertType, Observation>,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    if observed.is_empty() {
        return Ok(());
    }
    let names = observed
        .keys()
        .map(|alert_type| catalog_name(tenant, *alert_type))
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();
    // A live spec could share one of these names, so its alerts are excluded.
    let (silent, open): (Vec<_>, Vec<_>) =
        control_plane_api::alerts::fetch_open_alerts_by_catalog_name(&names, pool)
            .await
            .context("fetching open alerts")?
            .into_iter()
            .filter(|alert| observed.contains_key(&alert.alert_type))
            .partition(|alert| {
                matches!(observed[&alert.alert_type], Observation::ResolvedSilently)
            });

    let (earlier, open): (Vec<_>, Vec<_>) = open.into_iter().partition(|alert| {
        matches!(
            observed[&alert.alert_type],
            Observation::Firing { period_start: Some(start), .. } if alert.fired_at < start
        )
    });

    if !silent.is_empty() {
        let ids = silent.iter().map(|alert| alert.id).collect::<Vec<_>>();
        let mut conn = pool.acquire().await?;
        control_plane_api::alerts::resolve_alerts_silently(ids, &mut conn)
            .await
            .context("closing alerts silently")?;
        tracing::info!(%tenant, count = silent.len(), "closed tenant alerts silently");
    }

    // An observation without a row resolves any open alert of its type.
    let rows = observed
        .into_iter()
        .filter_map(|(alert_type, observation)| {
            let (arguments, firing) = match observation {
                Observation::Firing { arguments, .. } => (arguments, true),
                Observation::Resolved {
                    arguments: Some(arguments),
                } => (arguments, false),
                Observation::Resolved { arguments: None } | Observation::ResolvedSilently => {
                    return None;
                }
            };
            Some(AlertViewRow {
                catalog_name: catalog_name(tenant, alert_type),
                alert_type,
                base_arguments: arguments,
                firing,
            })
        })
        .collect();
    let mut actions: Vec<_> = earlier
        .into_iter()
        .map(|alert| {
            AlertAction::Resolve(ResolveAlert {
                id: alert.id,
                catalog_name: alert.catalog_name,
                alert_type: alert.alert_type,
                resolved_at: now,
                base_resolved_arguments: None,
            })
        })
        .collect();
    actions.extend(evaluate_alert_actions(
        now,
        &mut crate::id_generator::with_random_shard(),
        open,
        rows,
    ));
    if actions.is_empty() {
        return Ok(());
    }

    let mut txn = pool.begin().await?;
    control_plane_api::alerts::apply_alert_actions(actions, &mut txn)
        .await
        .context("applying alert actions")?;
    txn.commit().await.context("committing alert actions")?;
    Ok(())
}
