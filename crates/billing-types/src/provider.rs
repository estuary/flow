/// `BillingProvider` is intentionally a narrow seam around outbound Stripe API
/// calls. It is not meant to be a complete billing service boundary, which is
/// why the interface still uses Stripe-native types. Database-backed billing
/// reads live separately under `control_plane_api::billing::db`, and
/// resolution of a tenant's recorded customer ID, which combines both, lives
/// under `control_plane_api::billing::customer`.
///
/// This trait exists for two reasons:
/// 1. Keep the Stripe SDK wiring in one place.
/// 2. Make resolver tests deterministic without calling live Stripe.
#[async_trait::async_trait]
pub trait BillingProvider: Send + Sync + std::fmt::Debug {
    /// Retrieve a customer by ID. Stripe returns a deleted customer with
    /// `deleted` set rather than failing.
    async fn retrieve_customer(
        &self,
        customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<stripe::Customer>;

    async fn create_customer(
        &self,
        tenant: &str,
        user_email: &str,
        user_name: Option<&str>,
        billing_name: Option<&str>,
        address: Option<stripe::Address>,
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
    /// without a second Stripe lookup (see `control_plane_api::server::public::stripe_webhooks`).
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
