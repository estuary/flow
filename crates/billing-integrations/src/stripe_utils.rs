use billing_types::{InvoiceMetadata, InvoiceType};
use num_format::{Locale, ToFormattedString};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug)]
pub struct Invoice(stripe::Invoice, InvoiceMetadata);

impl Invoice {
    /// Keep the database bill identity alongside live Stripe state.
    pub fn with_metadata(invoice: stripe::Invoice, metadata: InvoiceMetadata) -> Self {
        Self(invoice, metadata)
    }

    pub fn metadata(&self) -> &InvoiceMetadata {
        &self.1
    }
}

impl Deref for Invoice {
    type Target = stripe::Invoice;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Invoice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Invoice {
    pub fn tenant(&self) -> String {
        self.1.tenant.clone()
    }
    pub fn amount(&self) -> f64 {
        self.0.amount_due.unwrap_or(0) as f64 / 100.0
    }
    pub fn id(&self) -> &stripe::InvoiceId {
        &self.0.id
    }
    pub fn has_cc(&self) -> bool {
        self.customer()
            .and_then(|c| c.invoice_settings.as_ref())
            .and_then(|s| s.default_payment_method.as_ref())
            .is_some()
    }
    pub fn collection_method(&self) -> anyhow::Result<stripe::CollectionMethod> {
        self.0.collection_method.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "Invoice {} (customer {}) is missing collection_method",
                self.0.id,
                self.customer().map(|c| c.id.clone()).unwrap_or_default()
            )
        })
    }
    pub fn customer(&self) -> Option<&stripe::Customer> {
        self.0.customer.as_ref()?.as_object()
    }
    pub fn status(&self) -> Option<stripe::InvoiceStatus> {
        self.0.status.clone()
    }

    pub fn is_manual(&self) -> bool {
        self.1.invoice_type == InvoiceType::Manual
    }

    pub fn to_table_row(&self) -> Vec<comfy_table::Cell> {
        let formatted_amount = format!(
            "${}",
            (self.amount() as i64).to_formatted_string(&Locale::en)
        );
        let cents = (self.amount().fract() * 100.0).round() as u8;
        let formatted_amount = format!("{}.{:02}", &formatted_amount, cents);
        vec![
            comfy_table::Cell::new(self.tenant()),
            comfy_table::Cell::new(formatted_amount),
            comfy_table::Cell::new(self.id()),
            comfy_table::Cell::new(if self.has_cc() { "yes" } else { "no" }),
            comfy_table::Cell::new(
                self.collection_method()
                    .map_or("<missing>".to_string(), |cm| format!("{}", cm)),
            ),
            comfy_table::Cell::new(
                self.status()
                    .map_or("<missing>".to_string(), |cm| format!("{}", cm)),
            ),
        ]
    }

    pub fn table_header() -> Vec<&'static str> {
        vec![
            "Tenant",
            "Amount",
            "Invoice ID",
            "Has Default Payment Method",
            "Collection Method",
            "Status",
        ]
    }
}
