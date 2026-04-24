use serde::{Deserialize, Serialize};

mod stripe_helpers;
pub use stripe_helpers::{SearchParams, stripe_search};

pub const TENANT_METADATA_KEY: &str = "estuary.dev/tenant_name";
pub const INVOICE_TYPE_KEY: &str = "estuary.dev/invoice_type";
pub const BILLING_PERIOD_START_KEY: &str = "estuary.dev/period_start";
pub const BILLING_PERIOD_END_KEY: &str = "estuary.dev/period_end";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize, sqlx::Type)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(rename_items = "SCREAMING_SNAKE_CASE")
)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "text")]
#[sqlx(rename_all = "snake_case")]
pub enum InvoiceType {
    Final,
    Preview,
    Manual,
}

impl InvoiceType {
    pub fn as_str(self) -> &'static str {
        match self {
            InvoiceType::Final => "final",
            InvoiceType::Preview => "preview",
            InvoiceType::Manual => "manual",
        }
    }
}

/// Status clause to append to a Stripe invoice search query.
///
/// Stripe's search DSL accepts both positive (`status:"open"`) and negative
/// (`-status:"draft"`) filters; callers generally want one or the other or
/// neither. The string values correspond to `stripe::InvoiceStatus`
/// serializations (lowercase).
#[derive(Debug, Clone, Copy)]
pub enum StatusFilter {
    /// No status clause.
    Any,
    /// `status:"<name>"` — match only invoices with this status.
    Only(&'static str),
    /// `-status:"<name>"` — exclude invoices with this status.
    Exclude(&'static str),
}

impl StatusFilter {
    fn clause(self) -> Option<String> {
        match self {
            StatusFilter::Any => None,
            StatusFilter::Only(s) => Some(format!(r#"status:"{s}""#)),
            StatusFilter::Exclude(s) => Some(format!(r#"-status:"{s}""#)),
        }
    }
}

pub fn customer_search_query(tenant: &str) -> String {
    format!(r#"metadata["{TENANT_METADATA_KEY}"]:"{tenant}""#)
}

/// Build a Stripe invoice search query for a specific customer, billing period,
/// and invoice type, with an optional status clause.
pub fn invoice_search_query(
    customer_id: impl std::fmt::Display,
    date_start: &str,
    date_end: &str,
    invoice_type: InvoiceType,
    status: StatusFilter,
) -> String {
    let mut clauses = vec![
        format!(r#"customer:"{customer_id}""#),
        format!(
            r#"metadata["{INVOICE_TYPE_KEY}"]:"{}""#,
            invoice_type.as_str()
        ),
        format!(r#"metadata["{BILLING_PERIOD_START_KEY}"]:"{date_start}""#),
        format!(r#"metadata["{BILLING_PERIOD_END_KEY}"]:"{date_end}""#),
    ];
    if let Some(status) = status.clause() {
        clauses.push(status);
    }
    clauses.join(" AND ")
}

/// Build a Stripe invoice search query matching an invoice type, with an
/// optional `period_start` metadata filter and an optional status clause.
pub fn invoices_by_type_query(
    invoice_type: InvoiceType,
    date_start: Option<&str>,
    status: StatusFilter,
) -> String {
    let mut clauses = Vec::with_capacity(3);
    if let Some(status) = status.clause() {
        clauses.push(status);
    }
    clauses.push(format!(
        r#"metadata["{INVOICE_TYPE_KEY}"]:"{}""#,
        invoice_type.as_str()
    ));
    if let Some(date_start) = date_start {
        clauses.push(format!(
            r#"metadata["{BILLING_PERIOD_START_KEY}"]:"{date_start}""#
        ));
    }
    clauses.join(" AND ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn customer_query_format() {
        assert_eq!(
            customer_search_query("acme/widgets"),
            r#"metadata["estuary.dev/tenant_name"]:"acme/widgets""#
        );
    }

    #[test]
    fn invoice_query_exclude_draft() {
        let got = invoice_search_query(
            "cus_123",
            "2026-04-01",
            "2026-04-30",
            InvoiceType::Final,
            StatusFilter::Exclude("draft"),
        );
        assert_eq!(
            got,
            r#"customer:"cus_123" AND metadata["estuary.dev/invoice_type"]:"final" AND metadata["estuary.dev/period_start"]:"2026-04-01" AND metadata["estuary.dev/period_end"]:"2026-04-30" AND -status:"draft""#
        );
    }

    #[test]
    fn invoice_query_exclude_deleted() {
        let got = invoice_search_query(
            "cus_123",
            "2026-04-01",
            "2026-04-30",
            InvoiceType::Final,
            StatusFilter::Exclude("deleted"),
        );
        assert!(got.ends_with(r#"AND -status:"deleted""#));
    }

    #[test]
    fn invoices_by_type_with_month_and_status() {
        let got = invoices_by_type_query(
            InvoiceType::Final,
            Some("2026-04-01"),
            StatusFilter::Only("draft"),
        );
        assert_eq!(
            got,
            r#"status:"draft" AND metadata["estuary.dev/invoice_type"]:"final" AND metadata["estuary.dev/period_start"]:"2026-04-01""#
        );
    }

    #[test]
    fn invoices_by_type_without_month() {
        let got = invoices_by_type_query(InvoiceType::Manual, None, StatusFilter::Only("open"));
        assert_eq!(
            got,
            r#"status:"open" AND metadata["estuary.dev/invoice_type"]:"manual""#
        );
    }
}
