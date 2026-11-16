use crate::billing::InvoiceType;
use anyhow::Context as _;
use billing_types::LineItem;
use chrono::{DateTime, Datelike as _, NaiveDate, Utc};
use serde::Deserialize as _;
use std::cmp::Reverse;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvoiceCursor {
    pub date_start: NaiveDate,
    pub date_end: NaiveDate,
    pub invoice_type: InvoiceType,
}

impl InvoiceCursor {
    pub fn from_row(row: &DbInvoiceRow) -> Self {
        Self {
            date_start: row.date_start,
            date_end: row.date_end,
            invoice_type: row.invoice_type,
        }
    }

    // Newest `date_end` first, then newest `date_start`, then final, manual,
    // and preview invoices.
    fn rank(&self) -> (Reverse<NaiveDate>, Reverse<NaiveDate>, u8) {
        let type_rank = match self.invoice_type {
            InvoiceType::Final => 0,
            InvoiceType::Manual => 1,
            InvoiceType::Preview => 2,
        };
        (Reverse(self.date_end), Reverse(self.date_start), type_rank)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InvoiceQuery {
    pub date_start_gt: Option<NaiveDate>,
    pub date_start_lt: Option<NaiveDate>,
    pub date_end_gt: Option<NaiveDate>,
    pub date_end_lt: Option<NaiveDate>,
    pub invoice_type_eq: Option<InvoiceType>,
}

impl InvoiceQuery {
    fn matches(&self, invoice: &InvoiceCursor) -> bool {
        self.date_start_gt
            .is_none_or(|date| invoice.date_start > date)
            && self
                .date_start_lt
                .is_none_or(|date| invoice.date_start < date)
            && self.date_end_gt.is_none_or(|date| invoice.date_end > date)
            && self.date_end_lt.is_none_or(|date| invoice.date_end < date)
            && self
                .invoice_type_eq
                .is_none_or(|invoice_type| invoice.invoice_type == invoice_type)
    }
}

#[derive(Debug, Clone)]
pub struct DbInvoiceRow {
    pub date_start: NaiveDate,
    pub date_end: NaiveDate,
    pub line_items: Vec<LineItem>,
    pub subtotal: i32,
    pub extra: serde_json::Value,
    pub invoice_type: InvoiceType,
    pub stripe_invoice_id: Option<String>,
}

/// Fetch a page of the tenant's invoices, newest first, and whether invoices
/// precede and follow it. `after` and `before` bound the page, then `first`
/// and `last` trim it.
/// Reports and usage are read only for invoices on the page. Previews read
/// usage through `stats`, so this returns an error if the page includes a
/// preview and `stats` is `None`.
pub async fn fetch_invoice_page(
    pool: &sqlx::PgPool,
    stats: Option<&catalog_stats::Client>,
    tenant: &str,
    query: &InvoiceQuery,
    after: Option<InvoiceCursor>,
    before: Option<InvoiceCursor>,
    first: Option<usize>,
    last: Option<usize>,
) -> anyhow::Result<(Vec<DbInvoiceRow>, bool, bool)> {
    // Build the invoice list from dates and types alone, and read reports after
    // paging. Every unfrozen month since the tenant was created has a preview.
    let frozen: BTreeMap<NaiveDate, DateTime<Utc>> = sqlx::query_scalar!(
        "SELECT billed_month FROM internal.billing_historicals WHERE tenant = $1",
        tenant,
    )
    .fetch_all(pool)
    .await
    .context("listing frozen invoices")?
    .into_iter()
    .map(|billed_month| (billed_month.date_naive(), billed_month))
    .collect();
    let mut manual: BTreeMap<(NaiveDate, NaiveDate), DbInvoiceRow> = sqlx::query!(
        "SELECT date_start, date_end, usd_cents, description, stripe_invoice_id
         FROM internal.manual_bills WHERE tenant = $1",
        tenant,
    )
    .fetch_all(pool)
    .await
    .context("reading manual bills")?
    .into_iter()
    .map(|bill| {
        let row = DbInvoiceRow {
            date_start: bill.date_start,
            date_end: bill.date_end,
            line_items: vec![LineItem {
                description: Some(bill.description),
                count: 1,
                rate: bill.usd_cents.into(),
                subtotal: bill.usd_cents.into(),
            }],
            subtotal: bill.usd_cents,
            extra: serde_json::Value::Null,
            invoice_type: InvoiceType::Manual,
            stripe_invoice_id: bill.stripe_invoice_id,
        };
        ((bill.date_start, bill.date_end), row)
    })
    .collect();
    let created_at = sqlx::query_scalar!(
        "SELECT created_at FROM public.tenants WHERE tenant = $1",
        tenant,
    )
    .fetch_one(pool)
    .await
    .context("reading tenant creation date")?
    .date_naive();

    let monthly = |month: NaiveDate, invoice_type: InvoiceType| InvoiceCursor {
        date_start: month,
        date_end: month_end(month),
        invoice_type,
    };
    let previews = invoice_months(created_at, Utc::now().date_naive())
        .filter(|month| !frozen.contains_key(month))
        .map(|month| monthly(month, InvoiceType::Preview));

    let mut invoices: Vec<InvoiceCursor> = frozen
        .keys()
        .map(|month| monthly(*month, InvoiceType::Final))
        .chain(manual.values().map(InvoiceCursor::from_row))
        .chain(previews)
        .filter(|invoice| query.matches(invoice))
        .collect();
    invoices.sort_by_key(InvoiceCursor::rank);
    let (page, has_previous, has_next) = crate::paginate(
        invoices,
        InvoiceCursor::rank,
        after.as_ref().map(InvoiceCursor::rank),
        before.as_ref().map(InvoiceCursor::rank),
        first,
        last,
    );

    let frozen_on_page: Vec<DateTime<Utc>> = page
        .iter()
        .filter(|invoice| invoice.invoice_type == InvoiceType::Final)
        .map(|invoice| frozen[&invoice.date_start])
        .collect();
    let mut frozen_rows: BTreeMap<NaiveDate, DbInvoiceRow> = sqlx::query!(
        "SELECT billed_month, report, stripe_invoice_id
         FROM internal.billing_historicals
         WHERE tenant = $1 AND billed_month = ANY($2)",
        tenant,
        &frozen_on_page,
    )
    .fetch_all(pool)
    .await
    .context("reading frozen invoices")?
    .into_iter()
    .map(|frozen| -> anyhow::Result<(NaiveDate, DbInvoiceRow)> {
        let month = frozen.billed_month.date_naive();
        // Reports frozen by the SQL calculation hold null `line_items` and
        // `subtotal` for months in which the tenant had no usage.
        let line_items = Option::<Vec<LineItem>>::deserialize(&frozen.report["line_items"])
            .with_context(|| format!("reading the line items of the {month} invoice"))?
            .unwrap_or_default();
        let subtotal = Option::<i32>::deserialize(&frozen.report["subtotal"])
            .with_context(|| format!("reading the subtotal of the {month} invoice"))?
            .unwrap_or_default();
        let row = DbInvoiceRow {
            date_start: month,
            date_end: month_end(month),
            line_items,
            subtotal,
            extra: frozen.report,
            invoice_type: InvoiceType::Final,
            stripe_invoice_id: frozen.stripe_invoice_id,
        };
        Ok((month, row))
    })
    .collect::<anyhow::Result<_>>()?;

    let mut rows = Vec::with_capacity(page.len());
    for invoice in page {
        let row = match invoice.invoice_type {
            InvoiceType::Final => frozen_rows.remove(&invoice.date_start),
            InvoiceType::Manual => manual.remove(&(invoice.date_start, invoice.date_end)),
            InvoiceType::Preview => Some(preview(pool, stats, tenant, invoice.date_start).await?),
        };
        rows.push(row.context("a frozen invoice was removed while reading the page")?);
    }

    Ok((rows, has_previous, has_next))
}

/// The first day of each month from the tenant's creation through the current month.
fn invoice_months(created_at: NaiveDate, today: NaiveDate) -> impl Iterator<Item = NaiveDate> {
    std::iter::successors(created_at.with_day(1), |month| {
        month.checked_add_months(chrono::Months::new(1))
    })
    .take_while(move |month| *month <= today)
}

fn month_end(month: NaiveDate) -> NaiveDate {
    month
        .checked_add_months(chrono::Months::new(1))
        .unwrap()
        .pred_opt()
        .unwrap()
}

/// Calculate the tenant's invoice for an unfrozen `month` from its usage so far.
async fn preview(
    pool: &sqlx::PgPool,
    stats: Option<&catalog_stats::Client>,
    tenant: &str,
    month: NaiveDate,
) -> anyhow::Result<DbInvoiceRow> {
    let invoice = billing_types::invoices::generate(
        pool,
        stats.context("catalog stats are not configured")?,
        month.year(),
        chrono::Month::try_from(month.month() as u8)?,
        Some(&[tenant.to_string()]),
    )
    .await?
    .pop()
    .context("no invoice was calculated for the tenant")?;

    Ok(DbInvoiceRow {
        date_start: month,
        date_end: month_end(month),
        subtotal: i32::try_from(invoice.subtotal)
            .with_context(|| format!("reading the subtotal of the {month} invoice"))?,
        extra: serde_json::to_value(&invoice)?,
        line_items: invoice.line_items,
        invoice_type: InvoiceType::Preview,
        stripe_invoice_id: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoice_months_span_creation_through_today() {
        let months = |created_at: &str, today: &str| -> Vec<String> {
            invoice_months(created_at.parse().unwrap(), today.parse().unwrap())
                .map(|month| month.to_string())
                .collect()
        };
        assert_eq!(
            months("2024-02-15", "2024-03-01"),
            ["2024-02-01", "2024-03-01"]
        );
        assert_eq!(
            months("2023-12-31", "2024-01-01"),
            ["2023-12-01", "2024-01-01"]
        );
        assert!(months("2024-02-01", "2024-01-31").is_empty());

        assert_eq!(
            month_end("2024-02-01".parse().unwrap()).to_string(),
            "2024-02-29"
        );
    }
}
