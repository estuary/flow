use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_graphql::{Result, dataloader::Loader};
use billing_types::{InvoiceSearch, StatusFilter};

use crate::billing::{BillingProvider, InvoiceType};

/// Compound key used by `StripeInvoiceLoader` to dedup search calls within a
/// request: one Stripe search per (customer, period, type).
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

/// Request-scoped loader that resolves a Stripe invoice by its metadata key.
pub(crate) struct StripeInvoiceLoader(pub Arc<dyn BillingProvider>);

impl Loader<StripeInvoiceKey> for StripeInvoiceLoader {
    type Value = stripe::Invoice;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[StripeInvoiceKey],
    ) -> Result<HashMap<StripeInvoiceKey, Self::Value>> {
        fan_out_optional(keys, |key| {
            let provider = self.0.clone();
            let query = InvoiceSearch {
                customer_id: Some(key.customer_id.as_str()),
                invoice_type: Some(key.invoice_type),
                period_start: Some(&key.date_start),
                period_end: Some(&key.date_end),
                status: StatusFilter::Exclude(stripe::InvoiceStatus::Draft),
            }
            .to_query();
            async move {
                provider
                    .search_invoices(&query)
                    .await
                    .map(|invoices| invoices.into_iter().next())
                    .map_err(|err| async_graphql::Error::new(err.to_string()))
            }
        })
        .await
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
                    Some(stripe::Expandable::Object(charge)) => Some(*charge),
                    _ => None,
                };
                Ok(charge)
            }
        })
        .await
    }
}

/// Fans the per-key lookups out in parallel, drops keys whose lookup returned
/// `None`, and collects the rest into a `HashMap`. Stops on the first error.
async fn fan_out_optional<K, V, F, Fut>(keys: &[K], f: F) -> Result<HashMap<K, V>>
where
    K: Clone + Eq + std::hash::Hash,
    F: Fn(K) -> Fut,
    Fut: Future<Output = Result<Option<V>>>,
{
    let lookups = keys.iter().cloned().map(|k| {
        let fut = f(k.clone());
        async move { fut.await.map(|v| v.map(|v| (k, v))) }
    });
    futures::future::join_all(lookups)
        .await
        .into_iter()
        .filter_map(Result::transpose)
        .collect()
}
