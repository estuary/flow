use anyhow::Context;
use futures::TryStreamExt;
use sqlx::Row;
use std::collections::{BTreeMap, BTreeSet};

// `catalog_stats::collect_stats` gives each read a fixed timeout, which one
// read of every tenant can exceed.
const STATS_READ_TENANTS: usize = 500;

/// Recalculate and freeze one month's invoices for every tenant. Existing frozen
/// invoices and their Stripe invoice IDs are retained.
/// Returns the number of newly frozen invoices.
/// All reads finish before a transaction inserts the new invoices atomically.
pub async fn freeze(
    pool: &sqlx::PgPool,
    stats: &catalog_stats::Client,
    year: i32,
    month: chrono::Month,
) -> anyhow::Result<u64> {
    let start = crate::calculate::month_range(year, month)?.start;
    let invoices = generate(pool, stats, year, month, None).await?;
    let tx = pool.begin().await.context("starting invoice freeze")?;
    let (tx, inserted) = futures::stream::iter(invoices.into_iter().map(Ok::<_, anyhow::Error>))
        .try_fold((tx, 0_u64), |(mut tx, inserted), invoice| async move {
            let count = sqlx::query(
                "INSERT INTO internal.billing_historicals (tenant, billed_month, report) \
                 VALUES ($1::public.catalog_tenant, $2, $3) \
                 ON CONFLICT (tenant, billed_month) DO NOTHING",
            )
            .bind(&invoice.billed_prefix)
            .bind(start.and_hms_opt(0, 0, 0).unwrap().and_utc())
            .bind(sqlx::types::Json(&invoice))
            .execute(&mut *tx)
            .await
            .with_context(|| format!("freezing invoice for {}", invoice.billed_prefix))?
            .rows_affected();
            Ok::<_, anyhow::Error>((tx, inserted + count))
        })
        .await?;
    tx.commit().await.context("committing invoice freeze")?;
    Ok(inserted)
}

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
    let selected = tenants.map(|names| names.to_vec());

    let mut tx = pool
        .begin()
        .await
        .context("starting billing input transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await?;
    let term_rows = sqlx::query(
        "SELECT tenant::text AS tenant, data_tiers, usage_tiers, recurring_usd_cents, trial_start \
         FROM public.tenants WHERE $1::text[] IS NULL OR tenant::text = ANY($1) ORDER BY tenant::text",
    )
    .bind(&selected)
    .fetch_all(&mut *tx)
    .await
    .context("reading tenant billing terms")?;
    let terms: Vec<crate::BillingTerms> = term_rows
        .into_iter()
        .map(|row| {
            Ok(crate::BillingTerms {
                tenant: row.try_get("tenant")?,
                data_tiers: row.try_get("data_tiers")?,
                usage_tiers: row.try_get("usage_tiers")?,
                recurring_usd_cents: row.try_get("recurring_usd_cents")?,
                trial_start: row.try_get("trial_start")?,
            })
        })
        .collect::<Result<_, sqlx::Error>>()?;
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
    let adjustment_rows = sqlx::query(
        "SELECT tenant::text AS tenant, detail, usd_cents FROM internal.billing_adjustments \
         WHERE billed_month = $1 AND ($2::text[] IS NULL OR tenant::text = ANY($2)) \
         ORDER BY tenant::text, id",
    )
    .bind(start_ts)
    .bind(&selected)
    .fetch_all(&mut *tx)
    .await
    .context("reading billing adjustments")?;
    let adjustments = adjustment_rows.into_iter().try_fold(
        BTreeMap::<String, Vec<crate::Adjustment>>::new(),
        |mut adjustments, row| {
            let tenant = row.try_get("tenant")?;
            let adjustment = crate::Adjustment {
                detail: row.try_get("detail")?,
                usd_cents: row.try_get("usd_cents")?,
            };
            adjustments.entry(tenant).or_default().push(adjustment);
            Ok::<_, sqlx::Error>(adjustments)
        },
    )?;
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
