use crate::billing::InvoiceType;
use chrono::NaiveDate;

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
}

#[derive(Debug, Clone, Copy, Default)]
pub struct InvoiceQuery {
    pub date_start_gt: Option<NaiveDate>,
    pub date_start_lt: Option<NaiveDate>,
    pub date_end_gt: Option<NaiveDate>,
    pub date_end_lt: Option<NaiveDate>,
    pub invoice_type_eq: Option<InvoiceType>,
}

/// Preview payloads are placeholders until the caller calculates the selected page.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbInvoiceRow {
    pub date_start: NaiveDate,
    pub date_end: NaiveDate,
    pub billed_prefix: String,
    pub line_items: sqlx::types::Json<serde_json::Value>,
    pub subtotal: i32,
    pub extra: sqlx::types::Json<serde_json::Value>,
    pub invoice_type: InvoiceType,
    pub stripe_invoice_id: Option<String>,
}

// Preview payloads are placeholders; callers calculate them after pagination.
// Final and manual projections match internal.invoices_ext.
const INVOICE_SOURCES: &str = r#"
    ), invoices AS (
        SELECT
            date_trunc('month', (h.report ->> 'billed_month')::timestamptz)::date AS date_start,
            (date_trunc('month', (h.report ->> 'billed_month')::timestamptz)
                + interval '1 month' - interval '1 day')::date AS date_end,
            h.tenant::text AS billed_prefix,
            coalesce(nullif(h.report -> 'line_items', 'null'::jsonb), '[]'::jsonb) AS line_items,
            coalesce(nullif(h.report -> 'subtotal', 'null'::jsonb), to_jsonb(0))::integer AS subtotal,
            h.report AS extra,
            'final'::text AS invoice_type,
            h.stripe_invoice_id
        FROM internal.billing_historicals h
        WHERE h.tenant::text = (SELECT tenant FROM invoice_args)

        UNION ALL

        SELECT
            invoice_month::date AS date_start,
            (invoice_month + interval '1 month' - interval '1 day')::date AS date_end,
            t.tenant::text AS billed_prefix,
            '[]'::jsonb AS line_items,
            0::integer AS subtotal,
            'null'::jsonb AS extra,
            'preview'::text AS invoice_type,
            null::text AS stripe_invoice_id
        FROM public.tenants t
        JOIN LATERAL generate_series(
            greatest('2023-08-01'::date, date_trunc('month', t.created_at)::date)::timestamptz,
            date_trunc('month', now()::date::timestamptz),
            interval '1 month'
        ) invoice_month(invoice_month)
        ON NOT EXISTS (
            SELECT 1 FROM internal.billing_historicals h
            WHERE h.tenant::text ^@ t.tenant::text
              AND date_trunc('month', (h.report ->> 'billed_month')::timestamptz)::date = invoice_month.invoice_month
        )
        WHERE t.tenant::text = (SELECT tenant FROM invoice_args)

        UNION ALL

        SELECT
            m.date_start,
            m.date_end,
            m.tenant::text AS billed_prefix,
            jsonb_build_array(jsonb_build_object(
                'description', m.description, 'count', 1,
                'rate', m.usd_cents, 'subtotal', m.usd_cents
            )) AS line_items,
            m.usd_cents AS subtotal,
            'null'::jsonb AS extra,
            'manual'::text AS invoice_type,
            m.stripe_invoice_id
        FROM internal.manual_bills m
        WHERE m.tenant::text = (SELECT tenant FROM invoice_args)
    )
    SELECT date_start, date_end, billed_prefix, line_items, subtotal, extra,
           invoice_type, stripe_invoice_id
    FROM invoices
    WHERE true
"#;

/// Fetch invoices older than the cursor, newest first.
pub async fn fetch_invoice_rows_forward(
    pool: &sqlx::PgPool,
    tenant: &str,
    query: &InvoiceQuery,
    cursor: Option<InvoiceCursor>,
    limit: Option<usize>,
) -> anyhow::Result<(Vec<DbInvoiceRow>, bool)> {
    fetch_invoice_rows(pool, tenant, query, cursor, limit, false).await
}

/// Fetch invoices newer than the cursor, newest first.
pub async fn fetch_invoice_rows_backward(
    pool: &sqlx::PgPool,
    tenant: &str,
    query: &InvoiceQuery,
    cursor: Option<InvoiceCursor>,
    limit: Option<usize>,
) -> anyhow::Result<(Vec<DbInvoiceRow>, bool)> {
    fetch_invoice_rows(pool, tenant, query, cursor, limit, true).await
}

async fn fetch_invoice_rows(
    pool: &sqlx::PgPool,
    tenant: &str,
    query: &InvoiceQuery,
    cursor: Option<InvoiceCursor>,
    limit: Option<usize>,
    backward: bool,
) -> anyhow::Result<(Vec<DbInvoiceRow>, bool)> {
    let mut sql = sqlx::QueryBuilder::<sqlx::Postgres>::new("WITH invoice_args AS (SELECT ");
    sql.push_bind(tenant)
        .push("::text AS tenant")
        .push(INVOICE_SOURCES);
    if let Some(date) = query.date_start_gt {
        sql.push(" AND date_start > ").push_bind(date);
    }
    if let Some(date) = query.date_start_lt {
        sql.push(" AND date_start < ").push_bind(date);
    }
    if let Some(date) = query.date_end_gt {
        sql.push(" AND date_end > ").push_bind(date);
    }
    if let Some(date) = query.date_end_lt {
        sql.push(" AND date_end < ").push_bind(date);
    }
    if let Some(invoice_type) = query.invoice_type_eq {
        sql.push(" AND invoice_type = ")
            .push_bind(invoice_type.to_string());
    }
    if let Some(cursor) = cursor {
        let (date_comparison, type_comparison) = if backward { (">", "<") } else { ("<", ">") };
        sql.push(" AND (date_end ")
            .push(date_comparison)
            .push(" ")
            .push_bind(cursor.date_end)
            .push(" OR (date_end = ")
            .push_bind(cursor.date_end)
            .push(" AND date_start ")
            .push(date_comparison)
            .push(" ")
            .push_bind(cursor.date_start)
            .push(") OR (date_end = ")
            .push_bind(cursor.date_end)
            .push(" AND date_start = ")
            .push_bind(cursor.date_start)
            .push(" AND invoice_type ")
            .push(type_comparison)
            .push(" ")
            .push_bind(cursor.invoice_type.to_string())
            .push("))");
    }
    if backward {
        sql.push(" ORDER BY date_end ASC, date_start ASC, invoice_type DESC");
    } else {
        sql.push(" ORDER BY date_end DESC, date_start DESC, invoice_type ASC");
    }
    let query_limit = limit.map(|l| l as i64 + 1).unwrap_or(i64::MAX);
    sql.push(" LIMIT ").push_bind(query_limit);

    let mut invoices = sql.build_query_as::<DbInvoiceRow>().fetch_all(pool).await?;
    let has_more = limit.is_some_and(|l| invoices.len() > l);
    if let Some(l) = limit {
        invoices.truncate(l);
    }
    if backward {
        invoices.reverse();
    }
    Ok((invoices, has_more))
}
