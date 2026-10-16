use std::collections::HashMap;

/// `BillingProvider` is intentionally a narrow seam around outbound Stripe API
/// calls while keeping Stripe SDK wiring out of API and publisher code.
#[async_trait::async_trait]
pub trait BillingProvider: Send + Sync + std::fmt::Debug {
    /// Retrieve a customer by ID. Stripe returns a deleted customer with
    /// `deleted` set rather than failing.
    async fn retrieve_customer(
        &self,
        customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<stripe::Customer>;

    /// Create a tenant-named customer, retaining caller metadata except the
    /// tenant entry, which the implementation must set authoritatively.
    async fn create_customer(
        &self,
        tenant: &str,
        email: Option<&str>,
        address: Option<stripe::Address>,
        metadata: HashMap<String, String>,
    ) -> anyhow::Result<stripe::Customer>;

    async fn update_customer_default_payment_method(
        &self,
        customer_id: &stripe::CustomerId,
        payment_method_id: Option<&str>,
    ) -> anyhow::Result<stripe::Customer>;

    async fn list_payment_methods(
        &self,
        customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<Vec<stripe::PaymentMethod>>;

    /// Create a SetupIntent for `customer_id`, stamping `tenant` into the
    /// SetupIntent's metadata. The `setup_intent.succeeded` webhook echoes that
    /// metadata back, which is how the webhook handler recovers the tenant
    /// without a second Stripe lookup (see `server::public::stripe_webhooks`).
    async fn create_setup_intent(
        &self,
        customer_id: &stripe::CustomerId,
        tenant: &str,
    ) -> anyhow::Result<stripe::SetupIntent>;

    async fn get_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod>;

    async fn detach_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod>;

    /// Retrieve an invoice by ID, or `None` if it no longer exists in Stripe.
    async fn retrieve_invoice(
        &self,
        invoice_id: &stripe::InvoiceId,
    ) -> anyhow::Result<Option<stripe::Invoice>>;

    async fn retrieve_payment_intent(
        &self,
        id: &stripe::PaymentIntentId,
    ) -> anyhow::Result<stripe::PaymentIntent>;

    async fn update_customer_billing_profile(
        &self,
        customer_id: &stripe::CustomerId,
        email: Option<&str>,
        name: Option<&str>,
        address: Option<stripe::Address>,
    ) -> anyhow::Result<stripe::Customer>;
}

pub fn default_payment_method_id(customer: &stripe::Customer) -> Option<String> {
    customer
        .invoice_settings
        .as_ref()
        .and_then(|s| s.default_payment_method.as_ref())
        .map(|e| e.id().to_string())
}
