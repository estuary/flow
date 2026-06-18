use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_graphql::{Result, dataloader::Loader};
use billing_types::InvoiceMetadata;

use crate::billing::{BillingProvider, InvoiceType};

/// Metadata identity that links a Stripe invoice back to a catalog invoice row:
/// `(invoice_type, period_start, period_end)`, scoped to a single customer.
type InvoiceIdentity = (InvoiceType, String, String);

/// Compound key that identifies the Stripe invoice for one catalog invoice row.
/// Batched keys are grouped by `customer_id`, so the loader resolves them with a
/// single `list_invoices` call per customer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct StripeInvoiceKey {
    pub(super) customer_id: stripe::CustomerId,
    pub(super) date_start: String,
    pub(super) date_end: String,
    pub(super) invoice_type: InvoiceType,
}

/// Request-scoped loader that resolves a Stripe customer by tenant prefix.
pub(crate) struct CustomerDataLoader(pub Arc<dyn BillingProvider>);

impl Loader<String> for CustomerDataLoader {
    type Value = stripe::Customer;
    type Error = async_graphql::Error;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>> {
        fan_out_optional(keys, |tenant| {
            let provider = self.0.clone();
            async move {
                provider
                    .find_customer(&tenant)
                    .await
                    .map_err(|err| async_graphql::Error::new(err.to_string()))
            }
        })
        .await
    }
}

/// Request-scoped loader that resolves Stripe invoices for catalog invoice rows.
///
/// Every row in an `invoices` connection belongs to one tenant, hence one Stripe
/// customer. The loader groups the batched keys by customer, issues a single
/// `list_invoices` per customer, and matches each row locally by its metadata
/// identity. Searching Stripe once per row instead would fan out one Search API
/// call per invoice and burst past Stripe's rate limit on large pages.
pub(crate) struct StripeInvoiceLoader(pub Arc<dyn BillingProvider>);

impl Loader<StripeInvoiceKey> for StripeInvoiceLoader {
    type Value = stripe::Invoice;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[StripeInvoiceKey],
    ) -> Result<HashMap<StripeInvoiceKey, Self::Value>> {
        let mut keys_by_customer: HashMap<stripe::CustomerId, Vec<&StripeInvoiceKey>> =
            HashMap::new();
        for key in keys {
            keys_by_customer
                .entry(key.customer_id.clone())
                .or_default()
                .push(key);
        }

        let mut resolved = HashMap::new();
        for (customer_id, keys) in keys_by_customer {
            let invoices = self
                .0
                .list_invoices(&customer_id)
                .await
                .map_err(|err| async_graphql::Error::new(err.to_string()))?;

            // Index the customer's invoices by metadata identity so each
            // requested key resolves with no further Stripe calls. Drafts are
            // skipped: they're in-progress and shouldn't surface amounts or
            // PDFs. Invoices without our metadata (e.g. created outside the
            // billing pipeline) can't be matched and are ignored. `or_insert`
            // keeps the first match, which is the newest since Stripe lists
            // invoices newest-first.
            let mut by_identity: HashMap<InvoiceIdentity, stripe::Invoice> = HashMap::new();
            for invoice in invoices {
                if matches!(invoice.status, Some(stripe::InvoiceStatus::Draft)) {
                    continue;
                }
                let Some(metadata) = invoice
                    .metadata
                    .as_ref()
                    .and_then(InvoiceMetadata::from_metadata_map)
                else {
                    continue;
                };
                by_identity
                    .entry((
                        metadata.invoice_type,
                        metadata.period_start,
                        metadata.period_end,
                    ))
                    .or_insert(invoice);
            }

            for key in keys {
                let identity = (
                    key.invoice_type,
                    key.date_start.clone(),
                    key.date_end.clone(),
                );
                if let Some(invoice) = by_identity.get(&identity) {
                    resolved.insert(key.clone(), invoice.clone());
                }
            }
        }
        Ok(resolved)
    }
}

/// Request-scoped loader that resolves a Stripe charge by payment intent ID.
pub(crate) struct ChargeDataLoader(pub Arc<dyn BillingProvider>);

impl Loader<stripe::PaymentIntentId> for ChargeDataLoader {
    type Value = stripe::Charge;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[stripe::PaymentIntentId],
    ) -> Result<HashMap<stripe::PaymentIntentId, Self::Value>> {
        fan_out_optional(keys, |pi_id| {
            let provider = self.0.clone();
            async move {
                let pi = provider
                    .retrieve_payment_intent(&pi_id)
                    .await
                    .map_err(|err| async_graphql::Error::new(err.to_string()))?;
                let charge = match pi.latest_charge {
                    None => None,
                    Some(stripe::Expandable::Object(charge)) => Some(*charge),
                    Some(stripe::Expandable::Id(id)) => {
                        tracing::error!(
                            charge_id=?id,
                            payment_intent_id=?pi_id,
                            "Stripe returned non-expanded charge for payment intent");
                        return Err(async_graphql::Error::new("Something went wrong"));
                    }
                };
                Ok(charge)
            }
        })
        .await
    }
}

/// Fans the per-key lookups out in parallel, drops keys whose lookup returned
/// `None`, and collects the rest into a `HashMap`. Short-circuits on the first
/// error.
async fn fan_out_optional<K, V, F, Fut>(keys: &[K], f: F) -> Result<HashMap<K, V>>
where
    K: Clone + Eq + std::hash::Hash,
    F: Fn(K) -> Fut,
    Fut: Future<Output = Result<Option<V>>>,
{
    let pairs = futures::future::try_join_all(keys.iter().cloned().map(|k| {
        let fut = f(k.clone());
        async move { fut.await.map(|v| v.map(|v| (k, v))) }
    }))
    .await?;
    Ok(pairs.into_iter().flatten().collect())
}
