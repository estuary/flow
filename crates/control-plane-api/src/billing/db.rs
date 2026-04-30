use crate::billing::InvoiceType;
use chrono::NaiveDate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvoiceCursorKey {
    pub date_start: NaiveDate,
    pub date_end: NaiveDate,
    pub invoice_type: InvoiceType,
}

impl InvoiceCursorKey {
    pub fn from_row(row: &DbInvoiceRow) -> Self {
        Self {
            date_start: row.date_start,
            date_end: row.date_end,
            invoice_type: row.invoice_type,
        }
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

#[derive(Debug, Clone)]
pub struct DbInvoiceRow {
    pub date_start: NaiveDate,
    pub date_end: NaiveDate,
    pub billed_prefix: String,
    pub line_items: sqlx::types::Json<serde_json::Value>,
    pub subtotal: i32,
    pub extra: sqlx::types::Json<serde_json::Value>,
    pub invoice_type: InvoiceType,
}

/// Fetch invoices older than `cursor` (or the newest invoices when `cursor`
/// is `None`). Returned rows are ordered newest-first.
pub async fn fetch_invoice_rows_forward(
    pool: &sqlx::PgPool,
    tenant: &str,
    query: &InvoiceQuery,
    cursor: Option<InvoiceCursorKey>,
    limit: Option<usize>,
) -> anyhow::Result<(Vec<DbInvoiceRow>, bool)> {
    let query_limit = limit.map(|l| l as i64 + 1).unwrap_or(i64::MAX);
    let invoice_type_eq = query.invoice_type_eq.map(|t| t.as_str());
    let cursor_date_end = cursor.map(|c| c.date_end);
    let cursor_date_start = cursor.map(|c| c.date_start);
    let cursor_invoice_type = cursor.map(|c| c.invoice_type.as_str());

    let mut invoices = sqlx::query_as!(
        DbInvoiceRow,
        r#"
        SELECT
            date_start as "date_start!",
            date_end as "date_end!",
            billed_prefix as "billed_prefix!",
            line_items as "line_items!: sqlx::types::Json<serde_json::Value>",
            subtotal as "subtotal!",
            extra as "extra!: sqlx::types::Json<serde_json::Value>",
            invoice_type as "invoice_type!: InvoiceType"
        FROM invoices_ext
        WHERE billed_prefix = $1
          AND ($2::date IS NULL OR date_start > $2)
          AND ($3::date IS NULL OR date_start < $3)
          AND ($4::date IS NULL OR date_end > $4)
          AND ($5::date IS NULL OR date_end < $5)
          AND ($6::text IS NULL OR invoice_type::text = $6)
          AND (
            $7::date IS NULL
            OR date_end < $7
            OR (date_end = $7 AND date_start < $8)
            OR (date_end = $7 AND date_start = $8 AND invoice_type::text > $9)
          )
        ORDER BY date_end DESC, date_start DESC, invoice_type ASC
        LIMIT $10
        "#,
        tenant,
        query.date_start_gt,
        query.date_start_lt,
        query.date_end_gt,
        query.date_end_lt,
        invoice_type_eq,
        cursor_date_end,
        cursor_date_start,
        cursor_invoice_type,
        query_limit,
    )
    .fetch_all(pool)
    .await?;

    // Query for one extra row so that its presence indicates more rows exist
    // past this batch; truncate it before returning.
    let has_more = limit.is_some_and(|l| invoices.len() > l);
    if let Some(l) = limit {
        invoices.truncate(l);
    }
    Ok((invoices, has_more))
}

/// Fetch invoices newer than `cursor`. Returned rows are ordered newest-first.
pub async fn fetch_invoice_rows_backward(
    pool: &sqlx::PgPool,
    tenant: &str,
    query: &InvoiceQuery,
    cursor: Option<InvoiceCursorKey>,
    limit: Option<usize>,
) -> anyhow::Result<(Vec<DbInvoiceRow>, bool)> {
    let query_limit = limit.map(|l| l as i64 + 1).unwrap_or(i64::MAX);
    let invoice_type_eq = query.invoice_type_eq.map(|t| t.as_str());
    let cursor_date_end = cursor.map(|c| c.date_end);
    let cursor_date_start = cursor.map(|c| c.date_start);
    let cursor_invoice_type = cursor.map(|c| c.invoice_type.as_str());

    let mut invoices = sqlx::query_as!(
        DbInvoiceRow,
        r#"
        SELECT
            date_start as "date_start!",
            date_end as "date_end!",
            billed_prefix as "billed_prefix!",
            line_items as "line_items!: sqlx::types::Json<serde_json::Value>",
            subtotal as "subtotal!",
            extra as "extra!: sqlx::types::Json<serde_json::Value>",
            invoice_type as "invoice_type!: InvoiceType"
        FROM invoices_ext
        WHERE billed_prefix = $1
          AND ($2::date IS NULL OR date_start > $2)
          AND ($3::date IS NULL OR date_start < $3)
          AND ($4::date IS NULL OR date_end > $4)
          AND ($5::date IS NULL OR date_end < $5)
          AND ($6::text IS NULL OR invoice_type::text = $6)
          AND (
            $7::date IS NULL
            OR date_end > $7
            OR (date_end = $7 AND date_start > $8)
            OR (date_end = $7 AND date_start = $8 AND invoice_type::text < $9)
          )
        ORDER BY date_end ASC, date_start ASC, invoice_type DESC
        LIMIT $10
        "#,
        tenant,
        query.date_start_gt,
        query.date_start_lt,
        query.date_end_gt,
        query.date_end_lt,
        invoice_type_eq,
        cursor_date_end,
        cursor_date_start,
        cursor_invoice_type,
        query_limit,
    )
    .fetch_all(pool)
    .await?;

    let has_more = limit.is_some_and(|l| invoices.len() > l);
    if let Some(l) = limit {
        invoices.truncate(l);
    }
    invoices.reverse();
    Ok((invoices, has_more))
}
