use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod stripe_helpers;
pub use stripe_helpers::{SearchParams, stripe_search};

pub const TENANT_METADATA_KEY: &str = "estuary.dev/tenant_name";
// The human billing-contact name is stored under this customer-metadata key
// rather than Stripe's `Customer.name`, because `Customer.name` is the
// tenant-slug join key used by the `internal.tenant_alerts` and
// `internal.free_trial_alerts` views.
pub const CUSTOMER_NAME_METADATA_KEY: &str = "estuary.dev/customer_name";
const INVOICE_TYPE_KEY: &str = "estuary.dev/invoice_type";
const BILLING_PERIOD_START_KEY: &str = "estuary.dev/period_start";
const BILLING_PERIOD_END_KEY: &str = "estuary.dev/period_end";

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
    strum::Display,
    strum::EnumString,
)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Enum),
    graphql(rename_items = "SCREAMING_SNAKE_CASE")
)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "text")]
#[sqlx(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum InvoiceType {
    Final,
    Preview,
    Manual,
}

/// Status clause to append to a Stripe invoice search query.
///
/// Stripe's search DSL accepts both positive (`status:"open"`) and negative
/// (`-status:"draft"`) filters
#[derive(Debug, Clone, Copy, Default)]
pub enum StatusFilter {
    /// No status clause.
    #[default]
    Any,
    /// `status:"<name>"`: match only invoices with this status.
    Only(stripe::InvoiceStatus),
    /// `-status:"<name>"`: exclude invoices with this status.
    Exclude(stripe::InvoiceStatus),
}

impl StatusFilter {
    fn clause(self) -> Option<String> {
        match self {
            StatusFilter::Any => None,
            StatusFilter::Only(s) => Some(format!(r#"status:"{}""#, s.as_str())),
            StatusFilter::Exclude(s) => Some(format!(r#"-status:"{}""#, s.as_str())),
        }
    }
}

pub fn customer_search_query(tenant: &str) -> String {
    format!(r#"metadata["{TENANT_METADATA_KEY}"]:"{tenant}""#)
}

/// Build the Stripe metadata map that stamps a tenant onto a tenant-scoped
/// object — the SetupIntent created for `setBillingPaymentMethod`, and the base
/// map for a tenant's Customer. The `setup_intent.succeeded` webhook recovers
/// the tenant from exactly this map. Centralizing construction here keeps
/// `TENANT_METADATA_KEY` in one place and gives us a single spot to add further
/// tenant-scoped metadata fields later; callers needing extra keys start from
/// this map and `insert` onto it.
pub fn tenant_metadata(tenant: &str) -> HashMap<String, String> {
    HashMap::from([(TENANT_METADATA_KEY.to_string(), tenant.to_string())])
}

/// Deterministic Stripe Idempotency-Key for `Customer::create` calls. Using the
/// tenant name collapses concurrent or retried creations across processes within
/// Stripe's 24-hour idempotency window, so a search-index lag race can't produce
/// duplicate customer rows for the same tenant.
pub fn customer_create_idempotency_key(tenant: &str) -> String {
    format!("flow-customer-create:{tenant}")
}

/// These 4 pieces of metadata link an invoice in Stripe to a row in `invoices_ext`. This is
/// an area that could be improved in the future if needed, but presently `invoices_ext` does not
/// model a single "primary key", which is why we need to use this compound identity. It composes:
/// * "Final" invoices, which come from `internal.billing_historicals`, and use the natural key of
///   `(tenant, billed_month)`. `billing_historicals` does not contain a primary key
/// * "Manual" invoices, which come from `internal.manual_bills` which uses the natural key
///   `(tenant, date_start, date_end)`, again not modelling a primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvoiceMetadata {
    pub tenant: String,
    pub invoice_type: InvoiceType,
    pub period_start: String,
    pub period_end: String,
}

impl InvoiceMetadata {
    pub fn to_metadata_map(&self) -> HashMap<String, String> {
        HashMap::from([
            (TENANT_METADATA_KEY.to_string(), self.tenant.clone()),
            (INVOICE_TYPE_KEY.to_string(), self.invoice_type.to_string()),
            (
                BILLING_PERIOD_START_KEY.to_string(),
                self.period_start.clone(),
            ),
            (BILLING_PERIOD_END_KEY.to_string(), self.period_end.clone()),
        ])
    }

    /// Parse an `InvoiceMetadata` from a Stripe invoice's metadata map.
    /// Returns `Some` only if all four expected fields are present and the
    /// invoice type parses; otherwise returns `None`.
    pub fn from_metadata_map(map: &HashMap<String, String>) -> Option<Self> {
        Some(Self {
            tenant: map.get(TENANT_METADATA_KEY)?.clone(),
            invoice_type: map.get(INVOICE_TYPE_KEY)?.parse().ok()?,
            period_start: map.get(BILLING_PERIOD_START_KEY)?.clone(),
            period_end: map.get(BILLING_PERIOD_END_KEY)?.clone(),
        })
    }
}

/// Filter for a Stripe invoice search. Each `Some` field becomes an AND-joined
/// clause in the resulting query; `None` fields are omitted.
#[derive(Debug, Default, Clone, Copy)]
pub struct InvoiceSearch<'a> {
    pub customer_id: Option<&'a str>,
    pub invoice_type: Option<InvoiceType>,
    pub period_start: Option<&'a str>,
    pub period_end: Option<&'a str>,
    pub status: StatusFilter,
}

impl InvoiceSearch<'_> {
    pub fn to_query(&self) -> String {
        let mut clauses = Vec::with_capacity(5);
        if let Some(id) = self.customer_id {
            clauses.push(format!(r#"customer:"{id}""#));
        }
        if let Some(invoice_type) = self.invoice_type {
            clauses.push(format!(
                r#"metadata["{INVOICE_TYPE_KEY}"]:"{invoice_type}""#
            ));
        }
        if let Some(period_start) = self.period_start {
            clauses.push(format!(
                r#"metadata["{BILLING_PERIOD_START_KEY}"]:"{period_start}""#
            ));
        }
        if let Some(period_end) = self.period_end {
            clauses.push(format!(
                r#"metadata["{BILLING_PERIOD_END_KEY}"]:"{period_end}""#
            ));
        }
        if let Some(status) = self.status.clause() {
            clauses.push(status);
        }
        clauses.join(" AND ")
    }
}

/// This represents the payment type that's used within our tenants table.
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Hash,
    Copy,
    sqlx::Type,
)]
#[sqlx(type_name = "payment_provider_type", rename_all = "lowercase")]
pub enum PaymentProvider {
    Stripe,
    External,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_metadata_carries_tenant() {
        assert_eq!(
            tenant_metadata("acme/widgets"),
            HashMap::from([(
                "estuary.dev/tenant_name".to_string(),
                "acme/widgets".to_string()
            )])
        );
    }

    #[test]
    fn customer_query_format() {
        assert_eq!(
            customer_search_query("acme/widgets"),
            r#"metadata["estuary.dev/tenant_name"]:"acme/widgets""#
        );
    }

    #[test]
    fn customer_create_idempotency_key_format() {
        // Same input must produce the same key across processes for cross-call
        // idempotency to work; the prefix namespaces it from future deterministic
        // keys for other Stripe writes.
        assert_eq!(
            customer_create_idempotency_key("acme/widgets"),
            "flow-customer-create:acme/widgets"
        );
        assert_eq!(
            customer_create_idempotency_key("acme/widgets"),
            customer_create_idempotency_key("acme/widgets"),
        );
    }

    #[test]
    fn invoice_metadata_round_trip() {
        let original = InvoiceMetadata {
            tenant: "acme/widgets".to_string(),
            invoice_type: InvoiceType::Final,
            period_start: "2026-04-01".to_string(),
            period_end: "2026-04-30".to_string(),
        };
        let parsed = InvoiceMetadata::from_metadata_map(&original.to_metadata_map());
        assert_eq!(parsed, Some(original));
    }

    #[test]
    fn invoice_metadata_missing_field_returns_none() {
        let mut map = InvoiceMetadata {
            tenant: "acme/widgets".to_string(),
            invoice_type: InvoiceType::Final,
            period_start: "2026-04-01".to_string(),
            period_end: "2026-04-30".to_string(),
        }
        .to_metadata_map();
        map.remove(BILLING_PERIOD_END_KEY);
        assert_eq!(InvoiceMetadata::from_metadata_map(&map), None);
    }

    #[test]
    fn search_full_exclude_draft() {
        let got = InvoiceSearch {
            customer_id: Some("cus_123"),
            invoice_type: Some(InvoiceType::Final),
            period_start: Some("2026-04-01"),
            period_end: Some("2026-04-30"),
            status: StatusFilter::Exclude(stripe::InvoiceStatus::Draft),
        }
        .to_query();
        assert_eq!(
            got,
            r#"customer:"cus_123" AND metadata["estuary.dev/invoice_type"]:"final" AND metadata["estuary.dev/period_start"]:"2026-04-01" AND metadata["estuary.dev/period_end"]:"2026-04-30" AND -status:"draft""#
        );
    }

    #[test]
    fn search_full_exclude_void() {
        let got = InvoiceSearch {
            customer_id: Some("cus_123"),
            invoice_type: Some(InvoiceType::Final),
            period_start: Some("2026-04-01"),
            period_end: Some("2026-04-30"),
            status: StatusFilter::Exclude(stripe::InvoiceStatus::Void),
        }
        .to_query();
        assert!(got.ends_with(r#"AND -status:"void""#));
    }

    #[test]
    fn search_type_and_period_start() {
        let got = InvoiceSearch {
            invoice_type: Some(InvoiceType::Final),
            period_start: Some("2026-04-01"),
            status: StatusFilter::Only(stripe::InvoiceStatus::Draft),
            ..Default::default()
        }
        .to_query();
        assert_eq!(
            got,
            r#"metadata["estuary.dev/invoice_type"]:"final" AND metadata["estuary.dev/period_start"]:"2026-04-01" AND status:"draft""#
        );
    }

    #[test]
    fn search_type_only_status_any() {
        let got = InvoiceSearch {
            invoice_type: Some(InvoiceType::Manual),
            ..Default::default()
        }
        .to_query();
        assert_eq!(got, r#"metadata["estuary.dev/invoice_type"]:"manual""#);
    }
}
