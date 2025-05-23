use crate::publish::TENANT_METADATA_KEY;
use num_format::{Locale, ToFormattedString};
use serde::{de::DeserializeOwned, Serialize};
use std::ops::{Deref, DerefMut};
use stripe::SearchList;

#[derive(Serialize, Default, Debug)]
pub struct SearchParams {
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expand: Option<Vec<String>>,
}

pub async fn stripe_search<R: DeserializeOwned + 'static + Send>(
    client: &stripe::Client,
    resource: &str,
    mut params: SearchParams,
) -> Result<Vec<R>, stripe::StripeError> {
    let mut all_data = Vec::new();
    let mut page = None;
    loop {
        if let Some(p) = page {
            params.page = Some(p);
        }
        let resp: SearchList<R> = client
            .get_query(&format!("/{}/search", resource), &params)
            .await?;
        let count = resp.data.len();
        all_data.extend(resp.data);
        if count == 0 || !resp.has_more {
            break;
        }
        page = resp.next_page;
    }
    Ok(all_data)
}

pub async fn fetch_invoices(
    stripe_client: &stripe::Client,
    query: &str,
) -> anyhow::Result<Vec<Invoice>> {
    stripe_search(
        stripe_client,
        "invoices",
        SearchParams {
            query: query.to_string(),
            expand: Some(vec!["data.customer".to_string()]),
            ..Default::default()
        },
    )
    .await
    .map(|invoices| {
        invoices
            .into_iter()
            .map(|inv: stripe::Invoice| Invoice::from(inv))
            .collect()
    })
    .map_err(|e| e.into())
}

#[derive(Clone, Debug)]
pub struct Invoice(stripe::Invoice);

impl From<stripe::Invoice> for Invoice {
    fn from(invoice: stripe::Invoice) -> Self {
        Invoice(invoice)
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
        self.0
            .metadata
            .as_ref()
            .and_then(|m| m.get(TENANT_METADATA_KEY))
            .cloned()
            .unwrap_or_default()
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
