/// `BillingProvider` is intentionally a narrow seam around outbound Stripe API
/// calls. It is not meant to be a complete billing service boundary, which is
/// why the interface still uses Stripe-native types. Database-backed billing
/// reads live separately under `billing::db`.
///
/// This trait exists for two reasons:
/// 1. Keep the Stripe SDK wiring in one place.
/// 2. Make resolver tests deterministic without calling live Stripe.
#[async_trait::async_trait]
pub trait BillingProvider: Send + Sync + std::fmt::Debug {
    async fn search_customers(&self, query: &str) -> anyhow::Result<Vec<stripe::Customer>>;

    async fn create_customer(
        &self,
        tenant: &str,
        user_email: &str,
        user_name: Option<&str>,
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

    async fn create_setup_intent(
        &self,
        customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<stripe::SetupIntent>;

    async fn get_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod>;

    async fn detach_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod>;

    async fn search_invoices(&self, query: &str) -> anyhow::Result<Vec<stripe::Invoice>>;

    async fn find_customer(&self, tenant: &str) -> anyhow::Result<Option<stripe::Customer>> {
        let query = billing_types::customer_search_query(tenant);
        let mut customers = self.search_customers(&query).await?;
        Ok(customers.drain(..).next())
    }

    async fn require_customer(&self, tenant: &str) -> anyhow::Result<stripe::Customer> {
        self.find_customer(tenant)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no Stripe customer exists for tenant '{tenant}'"))
    }

    async fn find_or_create_customer(
        &self,
        tenant: &str,
        email: &str,
        full_name: Option<&str>,
    ) -> anyhow::Result<stripe::Customer> {
        if let Some(existing) = self.find_customer(tenant).await? {
            return Ok(existing);
        }

        self.create_customer(tenant, email, full_name).await
    }

    async fn fetch_invoice(
        &self,
        tenant: &str,
        date_start: &str,
        date_end: &str,
        invoice_type: billing_types::InvoiceType,
    ) -> anyhow::Result<Option<stripe::Invoice>> {
        let Some(customer) = self.find_customer(tenant).await? else {
            return Ok(None);
        };

        let query = billing_types::InvoiceSearch {
            customer_id: Some(customer.id.as_str()),
            invoice_type: Some(invoice_type),
            period_start: Some(date_start),
            period_end: Some(date_end),
            status: billing_types::StatusFilter::Exclude(stripe::InvoiceStatus::Draft),
        }
        .to_query();
        let mut invoices = self.search_invoices(&query).await?;
        Ok(invoices.drain(..).next())
    }
}

pub fn default_payment_method_id(customer: &stripe::Customer) -> Option<String> {
    customer
        .invoice_settings
        .as_ref()
        .and_then(|s| s.default_payment_method.as_ref())
        .map(|e| e.id().to_string())
}
