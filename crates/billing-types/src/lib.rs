use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod db;
mod provider;
mod stripe_impl;
pub use db::record_customer_id;
pub use provider::{BillingProvider, default_payment_method_id};
pub use stripe_impl::StripeBillingProvider;

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

/// Build the Stripe metadata map that stamps a tenant onto a tenant-scoped
/// object — the SetupIntent created for `setBillingPaymentMethod`. The
/// `setup_intent.succeeded` webhook recovers
/// the tenant from exactly this map. Centralizing construction here keeps
/// `TENANT_METADATA_KEY` in one place and gives us a single spot to add further
/// tenant-scoped metadata fields later; callers needing extra keys start from
/// this map and `insert` onto it.
pub fn tenant_metadata(tenant: &str) -> HashMap<String, String> {
    HashMap::from([(TENANT_METADATA_KEY.to_string(), tenant.to_string())])
}

/// Deterministic Stripe Idempotency-Key for `Customer::create` calls. Using the
/// tenant name collapses concurrent or retried creations across processes within
/// Stripe's 24-hour idempotency window.
pub fn customer_create_idempotency_key(tenant: &str) -> String {
    format!("flow-customer-create:{tenant}")
}

/// Tenant, bill type, and billing period attached to a Flow invoice in Stripe.
/// These fields support historical backfill matching.
/// The bill's `stripe_invoice_id` records its invoice identity.
/// Manual bill dates can be edited; publishing refreshes a draft's metadata.
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
}
