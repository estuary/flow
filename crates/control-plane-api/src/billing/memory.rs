use super::BillingProvider;
use billing_types::TENANT_METADATA_KEY;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Default)]
struct State {
    customers: Vec<stripe::Customer>,
    payment_methods: Vec<(stripe::CustomerId, stripe::PaymentMethod)>,
    invoices: Vec<(stripe::CustomerId, stripe::Invoice)>,
    setup_intent_counter: u64,
}

/// In-memory `BillingProvider` used by tests and local development.
#[derive(Debug, Default)]
pub struct InMemoryBillingProvider {
    state: Mutex<State>,
}

impl InMemoryBillingProvider {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_customer(&self, tenant: &str, id: &str, default_pm: Option<&str>) {
        let mut state = self.state.lock().unwrap();
        state.customers.push(stripe::Customer {
            id: id.parse().unwrap(),
            invoice_settings: Some(stripe::InvoiceSettingCustomerSetting {
                default_payment_method: default_pm
                    .map(|pm| stripe::Expandable::Id(pm.parse().unwrap())),
                ..Default::default()
            }),
            metadata: Some(HashMap::from([(
                TENANT_METADATA_KEY.to_string(),
                tenant.to_string(),
            )])),
            ..Default::default()
        });
    }

    pub fn add_payment_method(
        &self,
        customer_id: &str,
        id: &str,
        type_: stripe::PaymentMethodType,
        billing_details: stripe::BillingDetails,
        card: Option<stripe::CardDetails>,
        us_bank_account: Option<stripe::PaymentMethodUsBankAccount>,
    ) {
        let pm = stripe::PaymentMethod {
            id: id.parse().unwrap(),
            type_,
            billing_details,
            card,
            us_bank_account,
            ..Default::default()
        };
        self.state
            .lock()
            .unwrap()
            .payment_methods
            .push((customer_id.parse().unwrap(), pm));
    }

    pub fn add_invoice(&self, customer_id: &str, invoice: stripe::Invoice) {
        self.state
            .lock()
            .unwrap()
            .invoices
            .push((customer_id.parse().unwrap(), invoice));
    }

    fn customer_search_tenant(query: &str) -> Option<&str> {
        let prefix = format!(r#"metadata["{}"]:""#, TENANT_METADATA_KEY);
        query
            .strip_prefix(&prefix)
            .and_then(|rest| rest.strip_suffix('"'))
    }
}

#[async_trait::async_trait]
impl BillingProvider for InMemoryBillingProvider {
    async fn search_customers(&self, query: &str) -> anyhow::Result<Vec<stripe::Customer>> {
        let state = self.state.lock().unwrap();
        let Some(tenant) = Self::customer_search_tenant(query) else {
            return Ok(state.customers.clone());
        };

        Ok(state
            .customers
            .iter()
            .filter(|customer| {
                customer
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get(TENANT_METADATA_KEY))
                    .is_some_and(|value| value == tenant)
            })
            .cloned()
            .collect())
    }

    async fn create_customer(
        &self,
        tenant: &str,
        _user_email: &str,
        _user_name: Option<&str>,
    ) -> anyhow::Result<stripe::Customer> {
        let mut state = self.state.lock().unwrap();
        let id = format!("cus_mock_{}", tenant.replace('/', ""));
        let customer = stripe::Customer {
            id: id.parse().unwrap(),
            metadata: Some(HashMap::from([(
                TENANT_METADATA_KEY.to_string(),
                tenant.to_string(),
            )])),
            ..Default::default()
        };
        state.customers.push(customer.clone());
        Ok(customer)
    }

    async fn update_customer_default_payment_method(
        &self,
        customer_id: &stripe::CustomerId,
        payment_method_id: Option<&str>,
    ) -> anyhow::Result<stripe::Customer> {
        let mut state = self.state.lock().unwrap();
        let customer = state
            .customers
            .iter_mut()
            .find(|c| &c.id == customer_id)
            .ok_or_else(|| anyhow::anyhow!("customer not found: {customer_id}"))?;
        let settings = customer
            .invoice_settings
            .get_or_insert_with(Default::default);
        settings.default_payment_method =
            payment_method_id.map(|id| stripe::Expandable::Id(id.parse().unwrap()));
        Ok(customer.clone())
    }

    async fn list_payment_methods(
        &self,
        customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<Vec<stripe::PaymentMethod>> {
        let state = self.state.lock().unwrap();
        Ok(state
            .payment_methods
            .iter()
            .filter(|(cid, _)| cid == customer_id)
            .map(|(_, method)| method.clone())
            .collect())
    }

    async fn create_setup_intent(
        &self,
        _customer_id: &stripe::CustomerId,
    ) -> anyhow::Result<stripe::SetupIntent> {
        let mut state = self.state.lock().unwrap();
        state.setup_intent_counter += 1;
        Ok(stripe::SetupIntent {
            client_secret: Some(format!(
                "seti_mock_{}_secret_test",
                state.setup_intent_counter
            )),
            ..Default::default()
        })
    }

    async fn get_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod> {
        let state = self.state.lock().unwrap();
        state
            .payment_methods
            .iter()
            .find(|(_, method)| &method.id == payment_method_id)
            .map(|(_, method)| method.clone())
            .ok_or_else(|| anyhow::anyhow!("payment method not found: {payment_method_id}"))
    }

    async fn detach_payment_method(
        &self,
        payment_method_id: &stripe::PaymentMethodId,
    ) -> anyhow::Result<stripe::PaymentMethod> {
        let mut state = self.state.lock().unwrap();
        let idx = state
            .payment_methods
            .iter()
            .position(|(_, method)| &method.id == payment_method_id)
            .ok_or_else(|| anyhow::anyhow!("payment method not found: {payment_method_id}"))?;
        let (_, method) = state.payment_methods.remove(idx);
        Ok(method)
    }

    async fn search_invoices(&self, query: &str) -> anyhow::Result<Vec<stripe::Invoice>> {
        let state = self.state.lock().unwrap();
        Ok(state
            .invoices
            .iter()
            .filter(|(customer_id, _)| query.contains(customer_id.as_str()))
            .map(|(_, invoice)| invoice.clone())
            .collect())
    }
}
