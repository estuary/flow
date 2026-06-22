use super::BillingProvider;
use billing_types::{
    SearchParams, TENANT_METADATA_KEY, customer_create_idempotency_key, stripe_search,
};
use std::collections::HashMap;

/// Production `BillingProvider` backed by the Stripe API.
#[derive(Clone)]
pub struct StripeBillingProvider {
    client: stripe::Client,
}

// Manual impl: `stripe::Client` doesn't derive `Debug`, and we wouldn't want
// it formatted anyway since it holds the API key. `BillingProvider` requires
// `Debug` so this stub satisfies the bound without leaking the secret.
impl std::fmt::Debug for StripeBillingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StripeBillingProvider")
            .finish_non_exhaustive()
    }
}

impl StripeBillingProvider {
    pub fn new(api_key: String) -> Self {
        Self {
            client: stripe::Client::new(api_key)
                .with_strategy(stripe::RequestStrategy::ExponentialBackoff(4)),
        }
    }
}

#[async_trait::async_trait]
impl BillingProvider for StripeBillingProvider {
    async fn search_customers(&self, query: &str) -> anyhow::Result<Vec<stripe::Customer>> {
        stripe_search(
            &self.client,
            "customers",
            SearchParams {
                query: query.to_string(),
                ..Default::default()
            },
        )
        .await
    }

    async fn create_customer(
        &self,
        tenant: &str,
        user_email: &str,
        user_name: Option<&str>,
        address: Option<stripe::Address>,
    ) -> anyhow::Result<stripe::Customer> {
        let mut metadata = HashMap::from([
            (TENANT_METADATA_KEY.to_string(), tenant.to_string()),
            ("created_by_user_email".to_string(), user_email.to_string()),
        ]);
        if let Some(name) = user_name {
            metadata.insert("created_by_user_name".to_string(), name.to_string());
        }

        let description = format!("Represents the billing entity for Flow tenant '{tenant}'");
        // Stripe's customer-search index lags writes by seconds, so two near-
        // simultaneous `find_or_create_customer` calls can both miss in search
        // and both create. Pinning a deterministic Idempotency-Key per tenant
        // collapses retries inside Stripe's 24h idempotency window.
        let client = self
            .client
            .clone()
            .with_strategy(stripe::RequestStrategy::Idempotent(
                customer_create_idempotency_key(tenant),
            ));
        let customer = stripe::Customer::create(
            &client,
            stripe::CreateCustomer {
                email: Some(user_email),
                name: Some(tenant),
                address,
                description: Some(&description),
                metadata: Some(metadata),
                ..Default::default()
            },
        )
        .await?;
        Ok(customer)
    }

    async fn update_customer_default_payment_method(
        &self,
        customer_id: &stripe::CustomerId,
        payment_method_id: Option<&str>,
    ) -> anyhow::Result<stripe::Customer> {
        let customer = stripe::Customer::update(
            &self.client,
            customer_id,
            stripe::UpdateCustomer {
                invoice_settings: Some(stripe::CustomerInvoiceSettings {
                    default_payment_method: payment_method_id.map(str::to_string),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await?;
        Ok(customer)
    }

    async fn list_payment_methods(
        &self,
        customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<Vec<stripe::PaymentMethod>> {
        let list = stripe::Customer::retrieve_payment_methods(
            &self.client,
            customer_id,
            stripe::CustomerPaymentMethodRetrieval::default(),
        )
        .await?;
        Ok(list.data)
    }

    async fn create_setup_intent(
        &self,
        customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<stripe::SetupIntent> {
        let si = stripe::SetupIntent::create(
            &self.client,
            stripe::CreateSetupIntent {
                customer: Some(customer_id.clone()),
                description: Some("Store your payment details"),
                automatic_payment_methods: Some(stripe::CreateSetupIntentAutomaticPaymentMethods {
                    enabled: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await?;
        Ok(si)
    }

    async fn get_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod> {
        let pm = stripe::PaymentMethod::retrieve(&self.client, payment_method_id, &[]).await?;
        Ok(pm)
    }

    async fn detach_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod> {
        let pm = stripe::PaymentMethod::detach(&self.client, payment_method_id).await?;
        Ok(pm)
    }

    async fn search_invoices(&self, query: &str) -> anyhow::Result<Vec<stripe::Invoice>> {
        stripe_search(
            &self.client,
            "invoices",
            SearchParams {
                query: query.to_string(),
                ..Default::default()
            },
        )
        .await
    }

    async fn retrieve_payment_intent(
        &self,
        id: &stripe::PaymentIntentId,
    ) -> anyhow::Result<stripe::PaymentIntent> {
        let pi = stripe::PaymentIntent::retrieve(&self.client, id, &["latest_charge"]).await?;
        Ok(pi)
    }

    async fn update_customer_billing_profile(
        &self,
        customer_id: &stripe::CustomerId,
        email: Option<&str>,
        name: Option<&str>,
        address: Option<stripe::Address>,
    ) -> anyhow::Result<stripe::Customer> {
        // The human billing name is written to customer metadata, not Stripe's
        // `Customer.name`. `Customer.name` is the tenant slug that the
        // `internal.tenant_alerts` and `internal.free_trial_alerts` views join
        // `stripe.customers` on, so overwriting it would drop the tenant out of
        // those views once the change syncs back through the customer CDC mirror.
        // TODO(billing): migrate those views to join on the
        // `estuary.dev/tenant_name` metadata key, then move the name back onto
        // `Customer.name` and retire this metadata field.
        let metadata = name.map(|name| {
            std::collections::HashMap::from([(
                billing_types::CUSTOMER_NAME_METADATA_KEY.to_string(),
                name.to_string(),
            )])
        });
        let customer = stripe::Customer::update(
            &self.client,
            customer_id,
            stripe::UpdateCustomer {
                email,
                address,
                metadata,
                ..Default::default()
            },
        )
        .await?;
        Ok(customer)
    }
}
