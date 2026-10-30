use super::{Outcome, PlanKind, TaskStatus, Tenant, retry_backoff};
use anyhow::Context;
use chrono::Datelike as _;

/// Task usage limits allow 10% headroom over two tasks running around the clock.
const MAX_DAILY_USAGE_SECONDS: u64 = 2 * 24 * 3600 * 11 / 10;
const MAX_MONTHLY_USAGE_SECONDS: u64 = 2 * 31 * 24 * 3600 * 11 / 10;
const MAX_MONTHLY_BYTES: u64 = 10_000_000_000;

/// Daily buckets examined by each check, ending with today's partial bucket.
/// The lookback catches excess daily usage after missed checks within this window.
const DAILY_LOOKBACK_DAYS: i64 = 7;

const SECONDS_PER_DAY: i64 = 86_400;

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct FreeTrialStatus {
    /// When the tenant's next daily check is due.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_check: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    pub task_status: TaskStatus,
}

/// Usage of a tenant over the windows to which free tier limits apply.
#[derive(Debug)]
struct Usage {
    /// Largest task usage of any examined day.
    max_daily_usage_seconds: u64,
    /// Task usage of the current month to date.
    monthly_usage_seconds: u64,
    /// Bytes read and written by tasks in the current month to date.
    monthly_bytes: u64,
}

impl Usage {
    fn exceeds_free_tier(&self) -> bool {
        self.max_daily_usage_seconds > MAX_DAILY_USAGE_SECONDS
            || self.monthly_usage_seconds > MAX_MONTHLY_USAGE_SECONDS
            || self.monthly_bytes > MAX_MONTHLY_BYTES
    }
}

/// Starts a self-serve tenant's free trial if its recent usage exceeds the free
/// tier, checking once a day at a time of day fixed for the tenant. A started
/// trial is recorded in `tenant`.
pub async fn check(
    status: &mut FreeTrialStatus,
    pool: &sqlx::PgPool,
    tenant: &mut Tenant,
    client: &catalog_stats::Client,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Outcome> {
    if tenant.trial_start.is_some() {
        return Ok(Outcome::Idle);
    }

    if let Some(next_retry) = status.task_status.next_retry.filter(|at| *at > now) {
        return Ok(Outcome::Sleep(
            (next_retry - now).to_std().unwrap_or_default(),
        ));
    }
    if let Some(next_check) = status.next_check.filter(|at| *at > now) {
        return Ok(Outcome::Sleep(
            (next_check - now).to_std().unwrap_or_default(),
        ));
    }

    // Other plans keep their daily check without reading usage, so a tenant
    // switched back to `self_serve` is checked at its next check time.
    let checked = if tenant.plan_kind == PlanKind::SelfServe {
        do_check(pool, &tenant.tenant, client, now).await
    } else {
        Ok(false)
    };

    match checked {
        Ok(started) => {
            if started {
                tenant.trial_start = Some(now.date_naive());
            }
            let next_check = next_check_after(&tenant.tenant, now);
            status.next_check = Some(next_check);
            status.task_status = TaskStatus::default();
            Ok(Outcome::Sleep(
                (next_check - now).to_std().unwrap_or_default(),
            ))
        }
        Err(err) => {
            status.task_status.failures += 1;
            let backoff = retry_backoff(status.task_status.failures);
            status.task_status.next_retry = Some(now + chrono::Duration::from_std(backoff)?);
            status.task_status.last_error = Some(format!("{err:#}"));
            tracing::warn!(
                tenant = %tenant.tenant,
                failures = status.task_status.failures,
                ?backoff,
                "free trial check failed: {err:#}",
            );
            Ok(Outcome::Sleep(backoff))
        }
    }
}

/// Returns whether the check started the tenant's trial.
async fn do_check(
    pool: &sqlx::PgPool,
    tenant: &str,
    client: &catalog_stats::Client,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<bool> {
    let (daily, monthly) = fetch_usage(client, tenant, now).await?;

    let usage = usage(&daily, monthly.as_ref());
    if !usage.exceeds_free_tier() {
        return Ok(false);
    }

    let started = sqlx::query!(
        r#"
        UPDATE tenants
        SET trial_start = $2
        WHERE tenant = $1 AND trial_start IS NULL
        "#,
        tenant,
        now.date_naive(),
    )
    .execute(pool)
    .await
    .context("starting free trial")?
    .rows_affected()
        > 0;

    if started {
        tracing::info!(tenant, ?usage, "started free trial");
    }
    Ok(started)
}

/// Reads the daily buckets examined by a check, and the current month's
/// bucket, of the tenant's rollup row.
async fn fetch_usage(
    client: &catalog_stats::Client,
    tenant: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<(
    Vec<catalog_stats::CatalogStats>,
    Option<catalog_stats::CatalogStats>,
)> {
    let today = now.date_naive();
    let tomorrow = (today + chrono::Days::new(1))
        .and_time(chrono::NaiveTime::MIN)
        .and_utc();
    let first_day = tomorrow - chrono::Duration::days(DAILY_LOOKBACK_DAYS);
    let month = today
        .with_day(1)
        .expect("every month has a first day")
        .and_time(chrono::NaiveTime::MIN)
        .and_utc();

    // The tenant's own name addresses its rollup row, which sums every task
    // and collection beneath it.
    let names = [tenant];
    let daily = catalog_stats::collect_stats(client.fetch_range_for_names(
        &names,
        catalog_stats::Grain::Daily,
        first_day..tomorrow,
    ))
    .await
    .context("reading daily usage")?;
    let monthly = catalog_stats::collect_stats(client.fetch_at_for_names(
        &names,
        catalog_stats::Grain::Monthly,
        month,
    ))
    .await
    .context("reading monthly usage")?;

    Ok((daily, monthly.into_iter().next()))
}

fn usage(
    daily: &[catalog_stats::CatalogStats],
    monthly: Option<&catalog_stats::CatalogStats>,
) -> Usage {
    let monthly = monthly.map(|row| &row.stats_summary);

    Usage {
        max_daily_usage_seconds: daily
            .iter()
            .map(|row| row.stats_summary.usage_seconds)
            .max()
            .unwrap_or_default(),
        monthly_usage_seconds: monthly.map_or(0, |s| s.usage_seconds),
        monthly_bytes: monthly.map_or(0, |s| {
            s.read_by_me.bytes_total + s.written_by_me.bytes_total
        }),
    }
}

/// Returns the first instant after `after` which falls on the tenant's check
/// time. Each tenant's time of day comes from a hash of its name, which spreads
/// checks evenly across the day and keeps a tenant's time stable across agent
/// restarts and wake messages.
pub(super) fn next_check_after(
    tenant: &str,
    after: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    let offset = (xxhash_rust::xxh3::xxh3_64(tenant.as_bytes()) % SECONDS_PER_DAY as u64) as i64;
    let days = (after.timestamp() - offset).div_euclid(SECONDS_PER_DAY);

    chrono::DateTime::from_timestamp((days + 1) * SECONDS_PER_DAY + offset, 0)
        .expect("check time is within the representable range")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;
    use std::sync::Arc;

    /// A daily row on the given day of May 2026.
    fn daily(
        name: &str,
        day: u32,
        usage_seconds: u64,
    ) -> (catalog_stats::Grain, catalog_stats::CatalogStats) {
        let ts = chrono::Utc.with_ymd_and_hms(2026, 5, day, 0, 0, 0).unwrap();
        (
            catalog_stats::Grain::Daily,
            stats(name, ts, usage_seconds, 0),
        )
    }

    /// A monthly row for the given month of 2026.
    fn monthly(
        name: &str,
        month: u32,
        usage_seconds: u64,
        bytes: u64,
    ) -> (catalog_stats::Grain, catalog_stats::CatalogStats) {
        let ts = chrono::Utc
            .with_ymd_and_hms(2026, month, 1, 0, 0, 0)
            .unwrap();
        (
            catalog_stats::Grain::Monthly,
            stats(name, ts, usage_seconds, bytes),
        )
    }

    /// Bytes read and written count together, so `bytes` is split between them.
    fn stats(
        name: &str,
        ts: chrono::DateTime<chrono::Utc>,
        usage_seconds: u64,
        bytes: u64,
    ) -> catalog_stats::CatalogStats {
        let docs_and_bytes = |bytes_total| ops::stats::DocsAndBytes {
            docs_total: 1,
            bytes_total,
        };
        catalog_stats::CatalogStats {
            meta: ops::Meta {
                uuid: "00000000-0000-0000-0000-000000000000".to_string(),
            },
            catalog_name: name.to_string(),
            ts,
            stats_summary: catalog_stats::StatsSummary {
                usage_seconds,
                read_by_me: docs_and_bytes(bytes / 2),
                written_by_me: docs_and_bytes(bytes - bytes / 2),
                ..Default::default()
            },
            task_stats: Default::default(),
        }
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn starts_trials_of_tenants_exceeding_the_free_tier(pool: sqlx::PgPool) {
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 5, 20, 15, 30, 0)
            .unwrap();
        // Independent policy values catch accidental threshold changes.
        let daily_limit = 190_080;
        let monthly_limit = 5_892_480;
        let monthly_bytes_limit = 10_000_000_000;

        // The emulator is shared and never truncated, so this test's rows are
        // cleared first.
        let client = Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        catalog_stats::test_util::delete_rows_with_prefix(&client, "freeTrial")
            .await
            .unwrap();

        let rows = [
            // Usage at, but not over, every limit.
            daily("freeTrialAtLimits/", 20, daily_limit),
            monthly("freeTrialAtLimits/", 5, monthly_limit, monthly_bytes_limit),
            // Only the tenant's rollup counts, even if a nested task is over.
            daily("freeTrialAtLimits/team/task", 20, daily_limit * 2),
            // The earliest examined day is over the daily limit.
            daily("freeTrialDailyHours/", 14, daily_limit + 1),
            // A day before the examined window is ignored.
            daily("freeTrialStaleDay/", 13, daily_limit + 1),
            monthly("freeTrialMonthlyHours/", 5, monthly_limit + 1, 0),
            monthly("freeTrialMonthlyBytes/", 5, 0, monthly_bytes_limit + 1),
            // Last month's usage is ignored.
            monthly("freeTrialLastMonth/", 4, monthly_limit + 1, 0),
            // Only self-serve tenants start trials.
            monthly("freeTrialContract/", 5, monthly_limit + 1, 0),
            monthly("freeTrialInternal/", 5, monthly_limit + 1, 0),
        ];
        catalog_stats::test_util::seed_rows(&client, &rows)
            .await
            .unwrap();

        let tenants = [
            "freeTrialAtLimits/",
            "freeTrialDailyHours/",
            "freeTrialStaleDay/",
            "freeTrialMonthlyHours/",
            "freeTrialMonthlyBytes/",
            "freeTrialLastMonth/",
            "freeTrialContract/",
            "freeTrialInternal/",
        ];
        sqlx::query("INSERT INTO tenants (tenant) SELECT unnest($1::text[])")
            .bind(&tenants[..])
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(
            "UPDATE tenants SET plan_kind = 'contract', payment_provider = 'stripe' WHERE tenant = 'freeTrialContract/';
             UPDATE tenants SET plan_kind = 'internal' WHERE tenant = 'freeTrialInternal/';",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut results = Vec::new();

        for tenant in tenants {
            let task_id: models::Id =
                sqlx::query_scalar("SELECT controller_task_id FROM tenants WHERE tenant = $1")
                    .bind(tenant)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let mut row = super::super::fetch_tenant_by_controller_task(&pool, task_id)
                .await
                .unwrap()
                .unwrap();

            let mut status = FreeTrialStatus::default();
            check(&mut status, &pool, &mut row, &client, now)
                .await
                .unwrap();

            let trial_start: Option<chrono::NaiveDate> =
                sqlx::query_scalar("SELECT trial_start FROM tenants WHERE tenant = $1")
                    .bind(tenant)
                    .fetch_one(&pool)
                    .await
                    .unwrap();

            assert_eq!(status.task_status.failures, 0, "{tenant}: {status:?}");
            assert!(status.next_check.is_some(), "{tenant}: {status:?}");
            assert_eq!(row.trial_start, trial_start, "{tenant}");
            results.push(serde_json::json!({
                "tenant": tenant,
                "trialStart": trial_start,
            }));
        }

        insta::assert_json_snapshot!(results);
    }

    #[test]
    fn check_times_are_fixed_per_tenant() {
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 5, 20, 15, 30, 0)
            .unwrap();
        let one_day = chrono::Duration::days(1);

        let first = next_check_after("acmeCo/", now);
        assert!(first > now && first <= now + one_day);
        // A poll before the check time keeps it, and each check schedules the
        // next for the same time the following day.
        assert_eq!(
            next_check_after("acmeCo/", first - chrono::Duration::seconds(1)),
            first
        );
        assert_eq!(next_check_after("acmeCo/", first), first + one_day);

        // Tenants spread across every hour of the day.
        let mut by_hour = [0; 24];
        for i in 0..1000 {
            let at = next_check_after(&format!("tenant{i}/"), now);
            by_hour[chrono::Timelike::hour(&at) as usize] += 1;
        }
        assert!(by_hour.iter().all(|count| *count > 20), "{by_hour:?}");
    }
}
