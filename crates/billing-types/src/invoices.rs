use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use std::collections::{BTreeMap, BTreeSet};

// `catalog_stats::collect_stats` gives each read a fixed timeout, which one
// read of every tenant can exceed.
const STATS_READ_TENANTS: usize = 500;

/// Recalculate and freeze one month's invoices for every tenant in batches.
/// Existing frozen invoices and their Stripe invoice IDs are retained.
/// Returns the number of newly frozen invoices. All new invoices commit together;
/// a failed read, calculation, or insertion rolls back earlier batches.
pub async fn freeze(
    pool: &sqlx::PgPool,
    stats: &catalog_stats::Client,
    year: i32,
    month: chrono::Month,
) -> anyhow::Result<u64> {
    let period = crate::calculate::month_range(year, month)?;
    let billed_month = period.start.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let inputs = load_inputs(pool, billed_month, None).await?;
    let batches = invoice_batches(stats, &inputs, year, month, period);
    futures::pin_mut!(batches);

    let Some(first) = batches.try_next().await? else {
        return Ok(0);
    };
    let mut tx = pool.begin().await.context("starting invoice freeze")?;
    let mut inserted = insert_batch(&mut tx, billed_month, first).await?;
    while let Some(invoices) = batches.try_next().await? {
        inserted += insert_batch(&mut tx, billed_month, invoices).await?;
    }
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
    let billed_month = period.start.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let inputs = load_inputs(pool, billed_month, tenants).await?;
    invoice_batches(stats, &inputs, year, month, period)
        .try_concat()
        .await
}

async fn insert_batch(
    txn: &mut sqlx::PgConnection,
    billed_month: chrono::DateTime<chrono::Utc>,
    invoices: Vec<crate::Invoice>,
) -> anyhow::Result<u64> {
    let (tenants, reports): (Vec<_>, Vec<_>) = invoices
        .iter()
        .map(|invoice| (invoice.billed_prefix.as_str(), sqlx::types::Json(invoice)))
        .unzip();
    Ok(sqlx::query!(
        "INSERT INTO internal.billing_historicals (tenant, billed_month, report) \
         SELECT tenant::public.catalog_tenant, $3, report \
         FROM unnest($1::text[], $2::jsonb[]) AS invoices(tenant, report) \
         ON CONFLICT (tenant, billed_month) DO NOTHING",
        &tenants as &[&str],
        &reports as &[sqlx::types::Json<&crate::Invoice>],
        billed_month,
    )
    .execute(txn)
    .await
    .context("freezing invoice batch")?
    .rows_affected())
}

struct BillingInputs {
    terms: Vec<crate::BillingTerms>,
    adjustments: BTreeMap<String, Vec<crate::Adjustment>>,
}

async fn load_inputs(
    pool: &sqlx::PgPool,
    billed_month: chrono::DateTime<chrono::Utc>,
    tenants: Option<&[String]>,
) -> anyhow::Result<BillingInputs> {
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
        billed_month,
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

    Ok(BillingInputs { terms, adjustments })
}

fn invoice_batches<'a>(
    stats: &'a catalog_stats::Client,
    inputs: &'a BillingInputs,
    year: i32,
    month: chrono::Month,
    period: std::ops::Range<chrono::NaiveDate>,
) -> impl futures::Stream<Item = anyhow::Result<Vec<crate::Invoice>>> + 'a {
    let start_ts = period.start.and_hms_opt(0, 0, 0).unwrap().and_utc();
    let end_ts = period.end.and_hms_opt(0, 0, 0).unwrap().and_utc();
    futures::stream::iter(inputs.terms.chunks(STATS_READ_TENANTS)).then(move |terms| async move {
        let names: Vec<_> = terms.iter().map(|t| t.tenant.as_str()).collect();
        let daily = catalog_stats::collect_stats(stats.fetch_range_for_names(
            &names,
            catalog_stats::Grain::Daily,
            start_ts..end_ts,
        ))
        .await
        .context("reading daily billing statistics")?
        .into_iter()
        .fold(
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
                    inputs
                        .adjustments
                        .get(&tenant.tenant)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                )
                .with_context(|| format!("calculating invoice for {}", tenant.tenant))
            })
            .collect()
    })
}
