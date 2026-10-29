use anyhow::Context;
use std::collections::{BTreeMap, BTreeSet};

// `catalog_stats::collect_stats` gives each read a fixed timeout, which one
// read of every tenant can exceed.
const STATS_READ_TENANTS: usize = 500;

/// Calculate invoices for the given tenants, or every tenant when
/// `tenants` is `None`. An empty selection returns no invoices.
/// Results are ordered by tenant.
/// Billing terms and adjustments share a PostgreSQL snapshot, taken before fetching usage.
pub async fn generate(
    pool: &sqlx::PgPool,
    stats: &catalog_stats::Client,
    year: i32,
    month: chrono::Month,
    tenants: Option<&[String]>,
) -> anyhow::Result<Vec<crate::Invoice>> {
    let period = crate::calculate::month_range(year, month)?;
    let start_ts = period.start.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let end_ts = period.end.and_hms_opt(0, 0, 0).unwrap().and_utc();

    let mut tx = pool
        .begin()
        .await
        .context("starting billing input transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let terms = sqlx::query_as!(
        crate::BillingTerms,
        r#"SELECT tenant::text AS "tenant!", data_tiers, usage_tiers, recurring_usd_cents, trial_start
           FROM public.tenants WHERE $1::text[] IS NULL OR tenant::text = ANY($1) ORDER BY tenant::text"#,
        tenants,
    )
    .fetch_all(&mut *tx)
    .await
    .context("reading tenant billing terms")?;
    if let Some(names) = tenants {
        let found: BTreeSet<&str> = terms.iter().map(|t| t.tenant.as_str()).collect();
        let missing: BTreeSet<&str> = names
            .iter()
            .map(String::as_str)
            .filter(|n| !found.contains(n))
            .collect();
        anyhow::ensure!(
            missing.is_empty(),
            "unknown tenants: {}",
            missing.into_iter().collect::<Vec<_>>().join(", ")
        );
    }
    let adjustment_rows = sqlx::query!(
        r#"SELECT tenant::text AS "tenant!", detail, usd_cents FROM internal.billing_adjustments
           WHERE billed_month = $1 AND ($2::text[] IS NULL OR tenant::text = ANY($2))
           ORDER BY tenant::text, id"#,
        start_ts,
        tenants,
    )
    .fetch_all(&mut *tx)
    .await
    .context("reading billing adjustments")?;
    let adjustments = adjustment_rows.into_iter().fold(
        BTreeMap::<String, Vec<crate::Adjustment>>::new(),
        |mut adjustments, row| {
            adjustments
                .entry(row.tenant)
                .or_default()
                .push(crate::Adjustment {
                    detail: row.detail,
                    usd_cents: row.usd_cents,
                });
            adjustments
        },
    );
    tx.rollback()
        .await
        .context("finishing billing input transaction")?;

    let names: Vec<&str> = terms.iter().map(|t| t.tenant.as_str()).collect();
    let mut daily_stats = Vec::new();
    for chunk in names.chunks(STATS_READ_TENANTS) {
        daily_stats.extend(
            catalog_stats::collect_stats(stats.fetch_range_for_names(
                chunk,
                catalog_stats::Grain::Daily,
                start_ts..end_ts,
            ))
            .await
            .context("reading daily billing statistics")?,
        );
    }
    let daily = daily_stats.into_iter().fold(
        BTreeMap::<String, Vec<crate::DailyUsage>>::new(),
        |mut daily, row| {
            daily
                .entry(row.catalog_name)
                .or_default()
                .push(crate::DailyUsage {
                    date: row.ts.date_naive(),
                    bytes_read: row.stats_summary.read_by_me.bytes_total,
                    bytes_written: row.stats_summary.written_by_me.bytes_total,
                    usage_seconds: row.stats_summary.usage_seconds,
                });
            daily
        },
    );
    terms
        .iter()
        .map(|tenant| {
            crate::calculate(
                year,
                month,
                tenant,
                daily
                    .get(&tenant.tenant)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
                adjustments
                    .get(&tenant.tenant)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            )
            .with_context(|| format!("calculating invoice for {}", tenant.tenant))
        })
        .collect()
}
