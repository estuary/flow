use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_graphql::{Result, dataloader::Loader};

use crate::billing::{self, BillingProvider};

/// Request-scoped loader that resolves a Stripe customer by tenant prefix.
pub(crate) struct CustomerDataLoader {
    pub pg_pool: sqlx::PgPool,
    pub provider: Arc<dyn BillingProvider>,
}

impl Loader<String> for CustomerDataLoader {
    type Value = stripe::Customer;
    type Error = async_graphql::Error;

    async fn load(&self, keys: &[String]) -> Result<HashMap<String, Self::Value>> {
        fan_out_optional(keys, |tenant| {
            let pg_pool = self.pg_pool.clone();
            let provider = self.provider.clone();
            async move {
                billing::find_customer(&pg_pool, provider.as_ref(), &tenant)
                    .await
                    .map_err(|err| async_graphql::Error::new(err.to_string()))
            }
        })
        .await
    }
}

/// Request-scoped loader that resolves a finalized Stripe invoice by its ID.
/// Drafts resolve to `None`: they're still being prepared by billing
/// automation and aren't shown to the tenant.
pub(crate) struct StripeInvoiceLoader(pub Arc<dyn BillingProvider>);

impl Loader<stripe::InvoiceId> for StripeInvoiceLoader {
    type Value = stripe::Invoice;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[stripe::InvoiceId],
    ) -> Result<HashMap<stripe::InvoiceId, Self::Value>> {
        fan_out_optional(keys, |invoice_id| {
            let provider = self.0.clone();
            async move {
                let invoice = provider
                    .retrieve_invoice(&invoice_id)
                    .await
                    .map_err(|err| async_graphql::Error::new(err.to_string()))?;
                Ok(invoice.filter(|i| i.status != Some(stripe::InvoiceStatus::Draft)))
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
