use crate::billing::InvoiceType;
use chrono::NaiveDate;

pub async fn fetch_invoice_rows(
    pool: &sqlx::PgPool,
    tenant: &str,
    date_start_eq: Option<NaiveDate>,
    date_end_eq: Option<NaiveDate>,
    invoice_type_eq: Option<InvoiceType>,
) -> anyhow::Result<Vec<DbInvoiceRow>> {
    let mut invoices: Vec<DbInvoiceRow> = sqlx::query_as::<_, DbInvoiceRow>(
        r#"
        SELECT
            date_start,
            date_end,
            billed_prefix,
            line_items,
            subtotal,
            extra,
            invoice_type
        FROM invoices_ext
        WHERE billed_prefix = $1
        ORDER BY
            date_end DESC,
            date_start DESC,
            invoice_type ASC,
            subtotal DESC,
            line_items::text DESC,
            extra::text DESC
        "#,
    )
    .bind(tenant)
    .fetch_all(pool)
    .await?;

    if let Some(date_start) = date_start_eq {
        invoices.retain(|invoice| invoice.date_start == date_start);
    }
    if let Some(date_end) = date_end_eq {
        invoices.retain(|invoice| invoice.date_end == date_end);
    }
    if let Some(invoice_type) = invoice_type_eq {
        invoices.retain(|invoice| invoice.invoice_type == invoice_type);
    }

    Ok(invoices)
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DbInvoiceRow {
    pub date_start: NaiveDate,
    pub date_end: NaiveDate,
    pub billed_prefix: String,
    pub line_items: sqlx::types::Json<serde_json::Value>,
    pub subtotal: i32,
    pub extra: sqlx::types::Json<serde_json::Value>,
    pub invoice_type: InvoiceType,
}
