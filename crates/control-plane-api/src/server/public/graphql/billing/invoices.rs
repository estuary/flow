use std::collections::HashMap;
use std::sync::Arc;

use super::super::filters;
use crate::billing::{self, BillingProvider, InvoiceCursorKey, InvoiceQuery, InvoiceType};
use anyhow::Context as _;
use async_graphql::{
    ComplexObject, Context, InputObject, Result, SimpleObject,
    connection::{self},
    dataloader::{DataLoader, Loader},
};
use chrono::NaiveDate;

pub(super) type InvoiceCursor = InvoiceCursorKey;

impl connection::CursorType for InvoiceCursorKey {
    type Error = anyhow::Error;

    fn decode_cursor(s: &str) -> std::result::Result<Self, Self::Error> {
        let mut splits = s.split(';');
        let Some(date_end) = splits.next() else {
            anyhow::bail!("invalid invoice cursor, no date_end: '{s}'");
        };
        let Some(date_start) = splits.next() else {
            anyhow::bail!("invalid invoice cursor, no date_start: '{s}'");
        };
        let Some(invoice_type) = splits.next() else {
            anyhow::bail!("invalid invoice cursor, no invoice_type: '{s}'");
        };

        let date_end =
            NaiveDate::parse_from_str(date_end, "%Y-%m-%d").context("invalid invoice cursor")?;
        let date_start =
            NaiveDate::parse_from_str(date_start, "%Y-%m-%d").context("invalid invoice cursor")?;
        let invoice_type = InvoiceType::from_str(invoice_type).ok_or_else(|| {
            anyhow::anyhow!("invalid invoice cursor, unknown invoice type: '{invoice_type}'")
        })?;

        Ok(Self {
            date_start,
            date_end,
            invoice_type,
        })
    }

    fn encode_cursor(&self) -> String {
        format!(
            "{};{};{}",
            self.date_end,
            self.date_start,
            self.invoice_type.as_str()
        )
    }
}

#[derive(Debug, Clone, Default, InputObject)]
pub struct InvoiceTypeFilter {
    pub eq: Option<InvoiceType>,
}

#[derive(Debug, Clone, Default, InputObject)]
pub struct InvoiceFilter {
    pub date_start: Option<filters::DateFilter>,
    pub date_end: Option<filters::DateFilter>,
    pub invoice_type: Option<InvoiceTypeFilter>,
}

impl InvoiceFilter {
    pub(super) fn into_query(self) -> InvoiceQuery {
        let date_start = self.date_start.unwrap_or_default();
        let date_end = self.date_end.unwrap_or_default();

        InvoiceQuery {
            date_start_gt: date_start.gt,
            date_start_lt: date_start.lt,
            date_end_gt: date_end.gt,
            date_end_lt: date_end.lt,
            invoice_type_eq: self.invoice_type.and_then(|f| f.eq),
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
#[graphql(complex)]
pub struct Invoice {
    pub date_start: String,
    pub date_end: String,
    pub invoice_type: InvoiceType,
    pub subtotal: i32,
    pub line_items: async_graphql::Json<serde_json::Value>,
    pub extra: async_graphql::Json<serde_json::Value>,
    #[graphql(skip)]
    tenant: String,
}

#[ComplexObject]
impl Invoice {
    async fn amount_due(&self, ctx: &Context<'_>) -> Result<Option<i64>> {
        Ok(self
            .stripe_invoice(ctx)
            .await?
            .and_then(|invoice| invoice.amount_due))
    }

    async fn status(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        Ok(self.stripe_invoice(ctx).await?.and_then(|invoice| {
            invoice
                .status
                .as_ref()
                .and_then(|s| serde_json::to_value(s).ok())
                .and_then(|v| v.as_str().map(str::to_string))
        }))
    }

    async fn invoice_pdf(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        Ok(self
            .stripe_invoice(ctx)
            .await?
            .and_then(|invoice| invoice.invoice_pdf.clone()))
    }

    async fn hosted_invoice_url(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        Ok(self
            .stripe_invoice(ctx)
            .await?
            .and_then(|invoice| invoice.hosted_invoice_url.clone()))
    }
}

impl Invoice {
    pub(super) fn from_row(row: billing::DbInvoiceRow) -> Self {
        Self {
            date_start: row.date_start.to_string(),
            date_end: row.date_end.to_string(),
            invoice_type: row.invoice_type,
            subtotal: row.subtotal,
            line_items: async_graphql::Json(row.line_items.0),
            extra: async_graphql::Json(row.extra.0),
            tenant: row.billed_prefix,
        }
    }

    async fn stripe_invoice(&self, ctx: &Context<'_>) -> Result<Option<stripe::Invoice>> {
        let loader = ctx.data::<DataLoader<StripeDataLoader>>()?;
        loader
            .load_one(StripeInvoiceKey {
                tenant: self.tenant.clone(),
                date_start: self.date_start.clone(),
                date_end: self.date_end.clone(),
                invoice_type: self.invoice_type,
            })
            .await
    }
}

/// DataLoader key for fetching a `stripe::Invoice` identified by the tenant,
/// billing period, and invoice type that locate it in Stripe's metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::server::public::graphql) struct StripeInvoiceKey {
    tenant: String,
    date_start: String,
    date_end: String,
    invoice_type: InvoiceType,
}

/// Request-scoped loader that resolves Stripe-backed records through the
/// shared `BillingProvider`.
pub(in crate::server::public::graphql) struct StripeDataLoader(pub Arc<dyn BillingProvider>);

impl Loader<StripeInvoiceKey> for StripeDataLoader {
    type Value = stripe::Invoice;
    type Error = async_graphql::Error;

    async fn load(
        &self,
        keys: &[StripeInvoiceKey],
    ) -> Result<HashMap<StripeInvoiceKey, Self::Value>> {
        let mut out = HashMap::with_capacity(keys.len());
        let mut customer_ids: HashMap<String, Option<stripe::CustomerId>> = HashMap::new();
        for key in keys {
            let customer_id = if let Some(customer_id) = customer_ids.get(&key.tenant) {
                customer_id.clone()
            } else {
                let customer_id = self
                    .0
                    .find_customer(&key.tenant)
                    .await
                    .map_err(|err| async_graphql::Error::new(err.to_string()))?
                    .map(|customer| customer.id);
                customer_ids.insert(key.tenant.clone(), customer_id.clone());
                customer_id
            };

            let Some(customer_id) = customer_id else {
                continue;
            };
            let query = billing_types::InvoiceSearch {
                customer_id: Some(customer_id.as_str()),
                invoice_type: Some(key.invoice_type),
                period_start: Some(&key.date_start),
                period_end: Some(&key.date_end),
                status: billing_types::StatusFilter::Exclude(stripe::InvoiceStatus::Draft),
            }
            .to_query();
            let fetched = self
                .0
                .search_invoices(&query)
                .await
                .map_err(|err| async_graphql::Error::new(err.to_string()))?;
            if let Some(invoice) = fetched.into_iter().next() {
                out.insert(key.clone(), invoice);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use crate::billing;
    use crate::test_server;
    use serde_json::json;
    use std::sync::Arc;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_billing_invoice_filter(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "aliceco";
        let user_id = provision_test_tenant(&pool, tenant).await;

        insert_billing_historical(&pool, tenant, "2024-01-01", 1234, "Usage").await;
        insert_billing_historical(&pool, tenant, "2024-02-01", 900, "Usage").await;

        let (server, token) = start_server_and_token(&pool, user_id, tenant, mock_provider()).await;

        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query {
                          tenant(name: "aliceco/") {
                            name
                            billing {
                              invoices(
                                first: 10
                                filter: {
                                  invoiceType: { eq: FINAL }
                                  dateStart: { gt: "2023-12-31", lt: "2024-02-01" }
                                }
                              ) {
                                edges {
                                  node {
                                    dateStart
                                    dateEnd
                                    invoiceType
                                    subtotal
                                    lineItems
                                    extra
                                  }
                                }
                              }
                            }
                          }
                        }
                    "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!("invoice_filter_by_date_start", response);

        let by_end: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query {
                          tenant(name: "aliceco/") {
                            billing {
                              invoices(
                                first: 10
                                filter: {
                                  invoiceType: { eq: FINAL }
                                  dateEnd: { gt: "2024-01-31", lt: "2024-03-01" }
                                }
                              ) {
                                edges { node { dateStart dateEnd invoiceType } }
                              }
                            }
                          }
                        }
                    "#
                }),
                Some(&token),
            )
            .await;
        let edges = &by_end["data"]["tenant"]["billing"]["invoices"]["edges"];
        assert_eq!(edges.as_array().map(Vec::len), Some(1));
        assert_eq!(edges[0]["node"]["dateStart"].as_str(), Some("2024-02-01"));
        assert_eq!(edges[0]["node"]["dateEnd"].as_str(), Some("2024-02-29"));
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_billing_invoice_stripe_fields(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "invoicefields";
        let user_id = provision_test_tenant(&pool, tenant).await;

        insert_billing_historical(&pool, tenant, "2024-02-01", 2500, "Manual").await;

        let mock = billing::InMemoryBillingProvider::new();
        mock.add_customer("invoicefields/", "cus_invoice", None);
        mock.add_invoice(
            "cus_invoice",
            stripe::Invoice {
                amount_due: Some(2600),
                status: Some(stripe::InvoiceStatus::Open),
                invoice_pdf: Some("https://example.test/invoice.pdf".to_string()),
                hosted_invoice_url: Some("https://example.test/hosted".to_string()),
                ..Default::default()
            },
        );

        let (server, token) = start_server_and_token(&pool, user_id, tenant, Arc::new(mock)).await;

        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query {
                          tenant(name: "invoicefields/") {
                            billing {
                              invoices(
                                first: 1
                                filter: {
                                  invoiceType: { eq: FINAL }
                                  dateStart: { gt: "2024-01-31", lt: "2024-02-02" }
                                }
                              ) {
                                edges {
                                  node {
                                    amountDue
                                    status
                                    invoicePdf
                                    hostedInvoiceUrl
                                  }
                                }
                              }
                            }
                          }
                        }
                    "#
                }),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!("invoice_stripe_fields", response);
    }

    fn invoices_page(response: &serde_json::Value) -> &serde_json::Value {
        &response["data"]["tenant"]["billing"]["invoices"]
    }

    fn cursor(page: &serde_json::Value, field: &str) -> String {
        page["pageInfo"][field]
            .as_str()
            .unwrap_or_else(|| panic!("page is missing {field}: {page:#?}"))
            .to_string()
    }

    async fn fetch_page(
        server: &test_server::TestServer,
        token: &str,
        tenant: &str,
        filter: serde_json::Value,
        page_args: serde_json::Value,
    ) -> serde_json::Value {
        let mut variables = serde_json::Map::from_iter([
            ("tenant".to_string(), json!(tenant)),
            ("filter".to_string(), filter),
        ]);
        variables.extend(page_args.as_object().unwrap().clone());
        server
            .graphql(
                &json!({ "query": INVOICES_PAGE_QUERY, "variables": variables }),
                Some(token),
            )
            .await
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_billing_invoice_pagination(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "invoicepages";
        let user_id = provision_test_tenant(&pool, tenant).await;

        for month in ["2024-01-01", "2024-02-01", "2024-03-01"] {
            insert_billing_historical(&pool, tenant, month, 500, "Usage").await;
        }

        let (server, token) = start_server_and_token(&pool, user_id, tenant, mock_provider()).await;
        let filter = json!({
            "invoiceType": { "eq": "FINAL" },
            "dateStart": { "gt": "2023-12-31", "lt": "2024-04-01" },
        });

        let first_page = fetch_page(
            &server,
            &token,
            "invoicepages/",
            filter.clone(),
            json!({"first": 1}),
        )
        .await;
        insta::assert_json_snapshot!("pagination_first_page", invoices_page(&first_page));

        let after = cursor(invoices_page(&first_page), "endCursor");
        let second_page = fetch_page(
            &server,
            &token,
            "invoicepages/",
            filter.clone(),
            json!({"after": after, "first": 1}),
        )
        .await;
        insta::assert_json_snapshot!("pagination_second_page", invoices_page(&second_page));

        let before = cursor(invoices_page(&second_page), "startCursor");
        let previous_page = fetch_page(
            &server,
            &token,
            "invoicepages/",
            filter,
            json!({"before": before, "last": 1}),
        )
        .await;
        insta::assert_json_snapshot!("pagination_previous_page", invoices_page(&previous_page));
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_billing_invoice_tie_break_pagination(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "invoicetie";
        let user_id = provision_test_tenant(&pool, tenant).await;

        for month in ["2024-02-01", "2024-03-01"] {
            insert_billing_historical(&pool, tenant, month, 500, "Usage").await;
        }

        sqlx::query(
            r#"
            insert into internal.manual_bills (tenant, usd_cents, description, date_start, date_end)
            values ($1, 700, 'Manual adjustment', '2024-03-01', '2024-03-31')
            "#,
        )
        .bind(format!("{tenant}/"))
        .execute(&pool)
        .await
        .expect("insert manual bill");

        let (server, token) = start_server_and_token(&pool, user_id, tenant, mock_provider()).await;
        let filter = json!({ "dateStart": { "gt": "2024-01-31", "lt": "2024-04-01" } });

        let first_page = fetch_page(
            &server,
            &token,
            "invoicetie/",
            filter.clone(),
            json!({"first": 1}),
        )
        .await;
        insta::assert_json_snapshot!("tie_break_first_page", invoices_page(&first_page));

        let after = cursor(invoices_page(&first_page), "endCursor");
        let second_page = fetch_page(
            &server,
            &token,
            "invoicetie/",
            filter.clone(),
            json!({"after": after, "first": 1}),
        )
        .await;
        insta::assert_json_snapshot!("tie_break_second_page", invoices_page(&second_page));

        let before = cursor(invoices_page(&second_page), "startCursor");
        let previous_page = fetch_page(
            &server,
            &token,
            "invoicetie/",
            filter,
            json!({"before": before, "last": 1}),
        )
        .await;
        insta::assert_json_snapshot!("tie_break_previous_page", invoices_page(&previous_page));
    }
}
