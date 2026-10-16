use super::{Outcome, TaskStatus, Tenant, retry_backoff};
use anyhow::Context;
use chrono::Datelike as _;
use std::sync::Arc;

/// The free tier is two tasks and 10 GB of data per month (see
/// site/docs/getting-started/pricing.md). Task usage limits allow 10% headroom
/// over two tasks running around the clock.
const MAX_DAILY_USAGE_SECONDS: u64 = 2 * 24 * 3600 * 11 / 10;
const MAX_MONTHLY_USAGE_SECONDS: u64 = 2 * 31 * 24 * 3600 * 11 / 10;
const MAX_MONTHLY_BYTES: u64 = 10_000_000_000;

/// Daily buckets examined by each check, ending with today's partial bucket.
/// Looking back past today means that missed checks (an outage, or a long
/// retry backoff) don't let a day which exceeded the free tier go unnoticed.
const DAILY_LOOKBACK_DAYS: i64 = 7;

/// Consecutive transient read failures tolerated within one check. The client
/// applies its own backoff between attempts; past that, the check fails and is
/// retried by the controller.
const MAX_READ_ATTEMPTS: usize = 3;
/// Bounds the stats reads of a check, so that a stalled read fails the check
/// rather than holding the controller indefinitely.
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
#[derive(Debug, PartialEq)]
struct Usage {
    /// Largest task usage of any examined day.
    max_daily_usage_seconds: u64,
    /// Task usage of the current month to date.
    monthly_usage_seconds: u64,
    /// Bytes read and written by tasks in the current month to date.
    monthly_bytes: u64,
}

/// Starts the tenant's free trial if its recent usage exceeds the free tier,
/// checking once a day at a time of day fixed for the tenant.
pub async fn check(
    status: &mut FreeTrialStatus,
    pool: &sqlx::PgPool,
    tenant: &Tenant,
    catalog_stats: &Option<Arc<catalog_stats::Client>>,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Outcome> {
    // There's nothing to check once a trial has started, nor without stats.
    let (None, Some(client)) = (tenant.trial_start, catalog_stats) else {
        return Ok(Outcome::Idle);
    };

    if let Some(next_retry) = status.task_status.next_retry.filter(|at| *at > now) {
        return Ok(Outcome::WaitForRetry(
            (next_retry - now).to_std().unwrap_or_default(),
        ));
    }
    if let Some(next_check) = status.next_check.filter(|at| *at > now) {
        return Ok(Outcome::Scheduled(
            (next_check - now).to_std().unwrap_or_default(),
        ));
    }

    match do_check(pool, &tenant.tenant, client, now).await {
        Ok(()) => {
            let next_check = next_check_after(&tenant.tenant, now);
            status.next_check = Some(next_check);
            status.task_status = TaskStatus::default();
            Ok(Outcome::Scheduled(
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
            Ok(Outcome::WaitForRetry(backoff))
        }
    }
}

async fn do_check(
    pool: &sqlx::PgPool,
    tenant: &str,
    client: &catalog_stats::Client,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    let (daily, monthly) = tokio::time::timeout(READ_TIMEOUT, fetch_usage(client, tenant, now))
        .await
        .context("timed out reading tenant usage")??;

    let usage = usage(&daily, monthly.as_ref());
    if !exceeds_free_tier(&usage) {
        return Ok(());
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
    Ok(())
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
    let daily = collect(client.fetch_range_for_names(
        &names,
        catalog_stats::Grain::Daily,
        first_day..tomorrow,
    ))
    .await
    .context("reading daily usage")?;
    let monthly = collect(client.fetch_at_for_names(&names, catalog_stats::Grain::Monthly, month))
        .await
        .context("reading monthly usage")?;

    Ok((daily, monthly.into_iter().next()))
}

async fn collect(
    stream: impl futures::Stream<Item = catalog_stats::RetryResult<catalog_stats::CatalogStats>>,
) -> anyhow::Result<Vec<catalog_stats::CatalogStats>> {
    let mut stream = std::pin::pin!(stream);
    let mut rows = Vec::new();

    while let Some(item) = futures::StreamExt::next(&mut stream).await {
        match item {
            Ok(row) => rows.push(row),
            // The stream stays alive after a transient failure, and polling it
            // again retries. Any other failure ends the stream, and must not be
            // mistaken for a complete read.
            Err(err) if err.inner.is_transient() && err.attempt < MAX_READ_ATTEMPTS => {
                tracing::debug!(attempt = err.attempt, error = %err.inner, "retrying stats read");
            }
            Err(err) => return Err(err.inner.into()),
        }
    }
    Ok(rows)
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

fn exceeds_free_tier(usage: &Usage) -> bool {
    usage.max_daily_usage_seconds > MAX_DAILY_USAGE_SECONDS
        || usage.monthly_usage_seconds > MAX_MONTHLY_USAGE_SECONDS
        || usage.monthly_bytes > MAX_MONTHLY_BYTES
}

/// Returns the first instant after `after` which falls on the tenant's check
/// time. Each tenant's time of day comes from a hash of its name, which spreads
/// checks evenly across the day and keeps a tenant's time stable across agent
/// restarts and wake messages.
fn next_check_after(
    tenant: &str,
    after: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    let offset = (xxhash_rust::xxh3::xxh3_64(tenant.as_bytes()) % SECONDS_PER_DAY as u64) as i64;
    let days = (after.timestamp() - offset).div_euclid(SECONDS_PER_DAY);

    chrono::DateTime::from_timestamp((days + 1) * SECONDS_PER_DAY + offset, 0)
        .expect("check time is within the representable range")
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use chrono::TimeZone as _;

    /// The emulator's port is per-stack, so the ambient mise env is required.
    pub(in super::super) async fn emulator() -> Arc<catalog_stats::Client> {
        let port = std::env::var("FLOW_PORT_BIGTABLE")
            .expect("FLOW_PORT_BIGTABLE must be set — run via 'mise run' (see local:bigtable)");

        let client = catalog_stats::Client::connect(&catalog_stats::BigtableConfig {
            project: "estuary-local".to_string(),
            instance: "estuary-local".to_string(),
            emulator_host: Some(format!("localhost:{port}")),
        })
        .await
        .expect("BigTable emulator must be running: `mise run local:bigtable`");

        Arc::new(client)
    }

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

        // The emulator is shared and never truncated, so this test's rows are
        // cleared first.
        let client = emulator().await;
        catalog_stats::test_util::delete_rows_with_prefix(&client, "freeTrial")
            .await
            .unwrap();

        let rows = [
            // Usage at, but not over, every limit.
            daily("freeTrialAtLimits/", 20, MAX_DAILY_USAGE_SECONDS),
            monthly(
                "freeTrialAtLimits/",
                5,
                MAX_MONTHLY_USAGE_SECONDS,
                MAX_MONTHLY_BYTES,
            ),
            // The earliest examined day is over the daily limit.
            daily("freeTrialDailyHours/", 14, MAX_DAILY_USAGE_SECONDS + 1),
            // A day before the examined window is ignored.
            daily("freeTrialStaleDay/", 13, MAX_DAILY_USAGE_SECONDS + 1),
            monthly(
                "freeTrialMonthlyHours/",
                5,
                MAX_MONTHLY_USAGE_SECONDS + 1,
                0,
            ),
            monthly("freeTrialMonthlyBytes/", 5, 0, MAX_MONTHLY_BYTES + 1),
            // Last month's usage is ignored.
            monthly("freeTrialLastMonth/", 4, MAX_MONTHLY_USAGE_SECONDS + 1, 0),
            // Only the tenant's rollup row counts. Its nested rows are already
            // summed into it, and must not be counted again.
            daily("freeTrialNested/", 20, MAX_DAILY_USAGE_SECONDS),
            daily("freeTrialNested/team/", 20, MAX_DAILY_USAGE_SECONDS),
            daily("freeTrialNested/team/task", 20, MAX_DAILY_USAGE_SECONDS),
            // A tenant whose trial has already started is left as-is.
            daily("freeTrialStarted/", 20, MAX_DAILY_USAGE_SECONDS + 1),
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
            "freeTrialNested/",
            "freeTrialStarted/",
            "freeTrialNoStats/",
        ];
        sqlx::query("INSERT INTO tenants (tenant) SELECT unnest($1::text[])")
            .bind(&tenants[..])
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE tenants SET trial_start = '2026-04-02' WHERE tenant = 'freeTrialStarted/'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let catalog_stats = Some(client);
        let mut results = Vec::new();

        for tenant in tenants {
            let task_id: models::Id =
                sqlx::query_scalar("SELECT controller_task_id FROM tenants WHERE tenant = $1")
                    .bind(tenant)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let row = super::super::fetch_tenant_by_controller_task(&pool, task_id)
                .await
                .unwrap()
                .unwrap();

            let mut status = FreeTrialStatus::default();
            let outcome = check(&mut status, &pool, &row, &catalog_stats, now)
                .await
                .unwrap();

            let trial_start: Option<chrono::NaiveDate> =
                sqlx::query_scalar("SELECT trial_start FROM tenants WHERE tenant = $1")
                    .bind(tenant)
                    .fetch_one(&pool)
                    .await
                    .unwrap();

            assert_eq!(status.task_status.failures, 0, "{tenant}: {status:?}");
            results.push(serde_json::json!({
                "tenant": tenant,
                "trialStart": trial_start,
                "nextCheck": status.next_check,
                "waitSeconds": outcome.wait().map(|wait| wait.as_secs()),
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
