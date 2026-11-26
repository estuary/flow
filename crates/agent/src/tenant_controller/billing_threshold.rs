use super::alerts::{self, Observation};
use super::{Outcome, TaskStatus, Tenant, retry_backoff};
use anyhow::Context;
use chrono::Datelike as _;
use control_plane_api::alerts::ArgsObject;
use models::status::AlertType;
use std::collections::BTreeMap;

/// How often a tenant with an enabled amount is checked.
const CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Fires `billing_threshold_exceeded` while the tenant's month-to-date charges
/// (`billing_types::Invoice::threshold_charges`) are at or above the amount
/// configured on its prefix. The open alert keeps later checks in the month
/// from firing it again. It resolves without a notification when its month
/// ends, when the charges fall below the amount, or when the amount is
/// disabled or removed.
///
/// Unlike the alerts in `trial_alerts`, this one applies to every plan.
pub async fn evaluate(
    status: &mut TaskStatus,
    pool: &sqlx::PgPool,
    tenant: &Tenant,
    stats: &catalog_stats::Client,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Outcome> {
    match do_evaluate(pool, tenant, stats, now).await {
        Ok(configured) => {
            *status = TaskStatus::default();
            Ok(if configured {
                Outcome::Sleep(CHECK_INTERVAL)
            } else {
                Outcome::Idle
            })
        }
        Err(err) => {
            status.failures += 1;
            let backoff = retry_backoff(status.failures);
            status.last_error = Some(format!("{err:#}"));
            tracing::warn!(
                tenant = %tenant.tenant,
                failures = status.failures,
                ?backoff,
                "billing threshold evaluation failed: {err:#}",
            );
            Ok(Outcome::Sleep(backoff))
        }
    }
}

/// Returns whether the tenant has an enabled amount.
async fn do_evaluate(
    pool: &sqlx::PgPool,
    tenant: &Tenant,
    stats: &catalog_stats::Client,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<bool> {
    let config = control_plane_api::controllers::fetch_alert_config(
        &tenant.tenant,
        pool,
        &Default::default(),
    )
    .await
    .context("fetching alert config")?;
    let usd_cents = config
        .billing_threshold_exceeded
        .filter(|config| config.enabled != Some(false))
        .and_then(|config| config.condition)
        .map(|condition| condition.usd_cents.get());

    let mut observation = Observation::Resolved { arguments: None };
    if let Some(usd_cents) = usd_cents {
        let month = chrono::Month::try_from(now.month() as u8).expect("month is in 1..=12");
        let invoice = billing_types::invoices::generate(
            pool,
            stats,
            now.year(),
            month,
            Some(&[tenant.tenant.clone()]),
        )
        .await
        .context("calculating month-to-date invoice")?
        .pop()
        .expect("generate returns an invoice for each named tenant");

        let charges = invoice.threshold_charges();
        if charges >= i64::from(usd_cents) {
            let arguments: ArgsObject = [
                ("tenant", serde_json::json!(tenant.tenant)),
                ("month", serde_json::json!(now.format("%B %Y").to_string())),
                ("threshold_usd_cents", serde_json::json!(usd_cents)),
                ("charges_usd_cents", serde_json::json!(charges)),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
            let month_start = now
                .date_naive()
                .with_day(1)
                .expect("every month has a first day")
                .and_time(chrono::NaiveTime::MIN)
                .and_utc();
            observation = Observation::Firing {
                arguments,
                period_start: Some(month_start),
            };
        }
    }

    let observed = BTreeMap::from([(AlertType::BillingThresholdExceeded, observation)]);
    alerts::apply(pool, &tenant.tenant, observed, now).await?;
    Ok(usd_cents.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    const TENANT: &str = "billingThreshold/";

    async fn set_amount(pool: &sqlx::PgPool, config: serde_json::Value) {
        sqlx::query(
            "insert into alert_configs (catalog_prefix_or_name, config) values ($1, $2) \
             on conflict (catalog_prefix_or_name) do update set config = excluded.config",
        )
        .bind(TENANT)
        .bind(sqlx::types::Json(config))
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn fires_once_per_crossing(pool: sqlx::PgPool) {
        // The emulator is shared and never truncated, so this test's rows are
        // cleared first.
        let stats = catalog_stats::test_util::connect_emulator("estuary-local").await;
        catalog_stats::test_util::delete_rows_with_prefix(&stats, TENANT)
            .await
            .unwrap();
        // 20 GB at the default $0.50 per GB is $10.00 of May usage.
        let usage = catalog_stats::CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: TENANT.to_string(),
            ts: chrono::Utc.with_ymd_and_hms(2026, 5, 10, 0, 0, 0).unwrap(),
            stats_summary: catalog_stats::StatsSummary {
                read_by_me: ops::stats::DocsAndBytes {
                    docs_total: 1,
                    bytes_total: 20_000_000_000,
                },
                ..Default::default()
            },
            task_stats: Default::default(),
        };
        catalog_stats::test_util::seed_rows(&stats, &[(catalog_stats::Grain::Daily, usage)])
            .await
            .unwrap();
        // A contract needs no payment method. With its $10.00 recurring fee,
        // May's subtotal is $20.00, and June's is $10.00 from the start.
        sqlx::query(
            "insert into tenants (tenant, plan_kind, collection_method, recurring_usd_cents, created_at) \
             values ($1, 'contract', 'send_invoice', 1000, '2026-01-01')",
        )
        .bind(TENANT)
        .execute(&pool)
        .await
        .unwrap();
        let tenant = Tenant {
            tenant: TENANT.to_string(),
            billing_emails: Vec::new(),
            billing_name: None,
            billing_address: None,
            payment_provider: Some(billing_types::PaymentProvider::Stripe),
            plan_kind: billing_types::PlanKind::Contract,
            collection_method: billing_types::CollectionMethod::SendInvoice,
            trial_start: None,
        };

        // Each evaluation is at a distinct time, since an alert's `fired_at`
        // is part of the `alert_history` primary key.
        let at = |month, day, hour| {
            chrono::Utc
                .with_ymd_and_hms(2026, month, day, hour, 0, 0)
                .unwrap()
        };
        let amount = |enabled: bool, usd_cents: u32| serde_json::json!({"billingThresholdExceeded": {"enabled": enabled, "condition": {"usdCents": usd_cents}}});
        let cases = [
            ("no amount", None, at(5, 20, 1)),
            ("below the amount", Some(amount(true, 2_001)), at(5, 20, 2)),
            (
                "reaches the amount",
                Some(amount(true, 2_000)),
                at(5, 20, 3),
            ),
            ("still over the amount", None, at(5, 20, 4)),
            ("amount raised", Some(amount(true, 2_001)), at(5, 20, 5)),
            ("amount lowered", Some(amount(true, 1_000)), at(5, 20, 6)),
            ("over in a new month", None, at(6, 1, 0)),
            ("disabled", Some(amount(false, 1_000)), at(6, 1, 1)),
        ];

        let mut status = TaskStatus::default();
        let mut results = Vec::new();
        for (case, config, now) in cases {
            if let Some(config) = config {
                set_amount(&pool, config).await;
            }
            let outcome = evaluate(&mut status, &pool, &tenant, &stats, now)
                .await
                .unwrap();

            let alerts: Vec<String> = sqlx::query_scalar(
                "select concat(arguments->>'month', case when resolved_at is null then ' open' else ' resolved' end) \
                 from alert_history where catalog_name = $1 order by fired_at",
            )
            .bind(format!("{TENANT}alerts/billing_threshold_exceeded"))
            .fetch_all(&pool)
            .await
            .unwrap();
            results.push(serde_json::json!({
                "case": case,
                "waitSeconds": outcome.wait().map(|duration| duration.as_secs()),
                "alerts": alerts,
            }));
        }
        let arguments: serde_json::Value = sqlx::query_scalar(
            "select arguments - 'recipients' from alert_history order by fired_at desc limit 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        results.push(serde_json::json!({"lastArguments": arguments}));

        insta::assert_json_snapshot!(results);
    }
}
