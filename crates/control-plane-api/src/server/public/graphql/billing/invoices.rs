use super::super::filters;
use super::loaders::{ChargeDataLoader, StripeInvoiceLoader};
use super::payment_methods::{CardPaymentMethodDetails, UsBankAccountPaymentMethodDetails};
use crate::billing::{DbInvoiceRow, InvoiceCursor, InvoiceQuery, InvoiceType};
use anyhow::Context as _;
use async_graphql::{
    ComplexObject, Context, InputObject, Result, SimpleObject,
    connection::{self},
    dataloader::DataLoader,
};
use chrono::NaiveDate;

impl connection::CursorType for InvoiceCursor {
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
        let invoice_type = invoice_type.parse::<InvoiceType>().map_err(|_| {
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
            self.date_end, self.date_start, self.invoice_type
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
    stripe_invoice_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum ChargeStatus {
    Failed,
    Pending,
    Succeeded,
}

impl From<&stripe::ChargeStatus> for ChargeStatus {
    fn from(s: &stripe::ChargeStatus) -> Self {
        match s {
            stripe::ChargeStatus::Failed => Self::Failed,
            stripe::ChargeStatus::Pending => Self::Pending,
            stripe::ChargeStatus::Succeeded => Self::Succeeded,
        }
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct InvoicePaymentDetails {
    pub status: ChargeStatus,
    pub receipt_url: Option<String>,
    pub card: Option<CardPaymentMethodDetails>,
    pub us_bank_account: Option<UsBankAccountPaymentMethodDetails>,
}

impl InvoicePaymentDetails {
    fn from_charge(charge: &stripe::Charge) -> Self {
        let (card, us_bank_account) = match charge.payment_method_details {
            Some(ref details) => (
                details.card.as_ref().map(CardPaymentMethodDetails::from),
                details
                    .us_bank_account
                    .as_ref()
                    .map(UsBankAccountPaymentMethodDetails::from),
            ),
            None => (None, None),
        };
        Self {
            status: ChargeStatus::from(&charge.status),
            receipt_url: charge.receipt_url.clone(),
            card,
            us_bank_account,
        }
    }
}

#[ComplexObject]
impl Invoice {
    async fn amount_due(&self, ctx: &Context<'_>) -> Result<Option<i64>> {
        Ok(self
            .stripe_invoice(ctx)
            .await?
            .and_then(|inv| inv.amount_due))
    }

    async fn status(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        Ok(self
            .stripe_invoice(ctx)
            .await?
            .and_then(|inv| inv.status.as_ref().map(|s| s.as_str().to_string())))
    }

    async fn invoice_pdf(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        Ok(self
            .stripe_invoice(ctx)
            .await?
            .and_then(|inv| inv.invoice_pdf.clone()))
    }

    async fn hosted_invoice_url(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        Ok(self
            .stripe_invoice(ctx)
            .await?
            .and_then(|inv| inv.hosted_invoice_url.clone()))
    }

    async fn payment_details(&self, ctx: &Context<'_>) -> Result<Option<InvoicePaymentDetails>> {
        let Some(invoice) = self.stripe_invoice(ctx).await? else {
            return Ok(None);
        };
        let Some(ref pi) = invoice.payment_intent else {
            return Ok(None);
        };
        let loader = ctx.data::<DataLoader<ChargeDataLoader>>()?;
        let charge = loader.load_one(pi.id()).await?;
        Ok(charge.map(|ref c| InvoicePaymentDetails::from_charge(c)))
    }
}

impl Invoice {
    pub(super) fn from_row(row: DbInvoiceRow) -> Self {
        Self {
            date_start: row.date_start.to_string(),
            date_end: row.date_end.to_string(),
            invoice_type: row.invoice_type,
            subtotal: row.subtotal,
            line_items: async_graphql::Json(row.line_items.0),
            extra: async_graphql::Json(row.extra.0),
            stripe_invoice_id: row.stripe_invoice_id,
        }
    }

    /// The finalized Stripe invoice for this bill, if one is available. Draft
    /// invoices and bills without a recorded invoice return `null`.
    async fn stripe_invoice(&self, ctx: &Context<'_>) -> Result<Option<stripe::Invoice>> {
        let Some(invoice_id) = &self.stripe_invoice_id else {
            return Ok(None);
        };
        let invoice_id: stripe::InvoiceId = invoice_id
            .parse()
            .map_err(|_| async_graphql::Error::new("invalid recorded Stripe invoice ID"))?;
        let loader = ctx.data::<DataLoader<StripeInvoiceLoader>>()?;
        loader.load_one(invoice_id).await
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

        // February's invoice is paid and March's is still a draft, which is
        // hidden from the tenant. April's bill has no recorded invoice.
        for month in ["2024-02-01", "2024-03-01", "2024-04-01"] {
            insert_billing_historical(&pool, tenant, month, 2500, "Usage").await;
        }
        for (month, invoice_id) in [("2024-02-01", "in_paid"), ("2024-03-01", "in_draft")] {
            sqlx::query(
                "update internal.billing_historicals set stripe_invoice_id = $1 where billed_month = $2::timestamptz",
            )
            .bind(invoice_id)
            .bind(month)
            .execute(&pool)
            .await
            .expect("record invoice ID");
        }

        let mock = billing::InMemoryBillingProvider::new();
        mock.add_invoice(stripe::Invoice {
            id: "in_paid".parse().unwrap(),
            amount_due: Some(2600),
            status: Some(stripe::InvoiceStatus::Paid),
            invoice_pdf: Some("https://example.test/invoice.pdf".to_string()),
            hosted_invoice_url: Some("https://example.test/hosted".to_string()),
            payment_intent: Some(stripe::Expandable::Id("pi_test_123".parse().unwrap())),
            ..Default::default()
        });
        mock.add_invoice(stripe::Invoice {
            id: "in_draft".parse().unwrap(),
            amount_due: Some(2500),
            status: Some(stripe::InvoiceStatus::Draft),
            ..Default::default()
        });
        mock.add_payment_intent(stripe::PaymentIntent {
            id: "pi_test_123".parse().unwrap(),
            latest_charge: Some(stripe::Expandable::Object(Box::new(stripe::Charge {
                status: stripe::ChargeStatus::Succeeded,
                receipt_url: Some("https://example.test/receipt".to_string()),
                payment_method_details: Some(stripe::PaymentMethodDetails {
                    card: Some(stripe::PaymentMethodDetailsCard {
                        brand: Some("visa".to_string()),
                        last4: Some("4242".to_string()),
                        exp_month: 12,
                        exp_year: 2025,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }))),
            ..Default::default()
        });

        let (server, token) = start_server_and_token(&pool, user_id, tenant, Arc::new(mock)).await;

        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query {
                          tenant(name: "invoicefields/") {
                            billing {
                              invoices(
                                first: 10
                                filter: {
                                  invoiceType: { eq: FINAL }
                                  dateStart: { gt: "2024-01-31", lt: "2024-05-01" }
                                }
                              ) {
                                edges {
                                  node {
                                    dateStart
                                    amountDue
                                    status
                                    invoicePdf
                                    hostedInvoiceUrl
                                    paymentDetails {
                                      status
                                      receiptUrl
                                      card { brand last4 expMonth expYear }
                                      usBankAccount { bankName last4 accountHolderType }
                                    }
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

    async fn reject_legacy_calculator(pool: &sqlx::PgPool) {
        sqlx::query(
            r#"CREATE OR REPLACE FUNCTION internal.billing_report_202308(
                billed_prefix public.catalog_prefix, billed_month timestamptz
            ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER AS $$
            BEGIN RAISE EXCEPTION 'legacy calculator called'; END $$"#,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn preview_server(
        pool: &sqlx::PgPool,
        user_id: uuid::Uuid,
        tenant: &str,
        client: Option<Arc<catalog_stats::Client>>,
        provider: Arc<dyn billing::BillingProvider>,
    ) -> (test_server::TestServer, String) {
        let server = test_server::TestServer::start_with_config(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
            Some(provider),
            client,
            models::AlertConfig::default(),
        )
        .await;
        let token = server.make_access_token(user_id, Some(&format!("{tenant}@example.test")));
        (server, token)
    }

    const INVOICE_DETAILS: &str = r#"
        query($tenant: String!, $filter: InvoiceFilter, $first: Int) {
          tenant(name: $tenant) { billing {
            invoices(first: $first, filter: $filter) {
              pageInfo { hasNextPage hasPreviousPage }
              edges { node { dateStart dateEnd invoiceType subtotal lineItems extra amountDue } }
            }
          } }
        }
    "#;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_previews_use_bigtable_and_keep_frozen_and_manual_invoices(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "previewusage";
        let prefix = "previewusage/";
        let user_id = provision_test_tenant(&pool, tenant).await;
        sqlx::query("UPDATE tenants SET created_at = '2024-02-01', data_tiers = ARRAY[50], usage_tiers = ARRAY[14], recurring_usd_cents = 1000, trial_start = '2020-01-01' WHERE tenant = $1")
            .bind(prefix).execute(&pool).await.unwrap();
        insert_billing_historical(&pool, tenant, "2024-02-01", 2500, "Frozen charge").await;
        sqlx::query("UPDATE internal.billing_historicals SET stripe_invoice_id = 'in_preview_test' WHERE tenant = $1")
            .bind(prefix).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO internal.manual_bills (tenant, usd_cents, description, date_start, date_end) VALUES ($1, 700, 'Manual charge', '2024-03-01', '2024-03-31')")
            .bind(prefix).execute(&pool).await.unwrap();
        sqlx::query("UPDATE internal.manual_bills SET stripe_invoice_id = 'in_manual_preview_test' WHERE tenant = $1")
            .bind(prefix).execute(&pool).await.unwrap();
        reject_legacy_calculator(&pool).await;
        let client = Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        catalog_stats::test_util::delete_rows_with_prefix(&client, prefix)
            .await
            .unwrap();
        let row: catalog_stats::CatalogStats = serde_json::from_value(json!({
            "_meta": {"uuid": "00000000-0000-0000-0000-000000000000"},
            "catalogName": prefix, "ts": "2024-03-08T00:00:00Z",
            "statsSummary": {"readByMe": {"bytesTotal": 500000000, "docsTotal": 0},
                "writtenByMe": {"bytesTotal": 1000000000, "docsTotal": 0}, "usageSeconds": 5400}
        }))
        .unwrap();
        let current_month: chrono::NaiveDate =
            sqlx::query_scalar("SELECT date_trunc('month', now())::date")
                .fetch_one(&pool)
                .await
                .unwrap();
        let current_end = current_month
            .checked_add_months(chrono::Months::new(1))
            .unwrap()
            .pred_opt()
            .unwrap();
        let mut current_row = row.clone();
        current_row.ts = current_month.and_hms_opt(0, 0, 0).unwrap().and_utc();
        catalog_stats::test_util::seed_rows(
            &client,
            &[
                (catalog_stats::Grain::Daily, row),
                (catalog_stats::Grain::Daily, current_row),
            ],
        )
        .await
        .unwrap();
        let provider = billing::InMemoryBillingProvider::new();
        provider.add_invoice(stripe::Invoice {
            id: "in_preview_test".parse().unwrap(),
            amount_due: Some(2600),
            status: Some(stripe::InvoiceStatus::Open),
            ..Default::default()
        });
        provider.add_invoice(stripe::Invoice {
            id: "in_manual_preview_test".parse().unwrap(),
            amount_due: Some(750),
            status: Some(stripe::InvoiceStatus::Open),
            ..Default::default()
        });
        let (server, token) =
            preview_server(&pool, user_id, tenant, Some(client), Arc::new(provider)).await;
        let response: serde_json::Value = server.graphql(&json!({
            "query": INVOICE_DETAILS,
            "variables": {"tenant": prefix, "first": 10, "filter": {"dateStart": {"gt": "2024-01-31", "lt": "2024-04-01"}}}
        }), Some(&token)).await;
        assert!(response.get("errors").is_none(), "{response:#}");
        let edges = invoices_page(&response)["edges"].as_array().unwrap();
        assert_eq!(edges.len(), 3, "{response:#}");
        insta::assert_json_snapshot!("preview_final_and_manual_nodes", edges);
        let manual = &edges[0]["node"];
        let preview = &edges[1]["node"];
        let frozen = &edges[2]["node"];
        assert_eq!(manual["invoiceType"], "MANUAL");
        assert_eq!(manual["subtotal"], 700);
        assert_eq!(manual["amountDue"], 750);
        assert_eq!(manual["lineItems"][0]["description"], "Manual charge");
        assert_eq!(preview["invoiceType"], "PREVIEW");
        assert_eq!(preview["dateStart"], "2024-03-01");
        assert_eq!(preview["subtotal"], 1128);
        assert_eq!(preview["extra"]["processed_data_gb"], 1.5);
        assert_eq!(preview["extra"]["task_usage_hours"], 1.5);
        assert_eq!(preview["extra"]["daily_usage"][0]["data_subtotal"], 100);
        assert_eq!(preview["extra"]["daily_usage"][0]["task_subtotal"], 28);
        assert_eq!(
            preview["lineItems"][0]["description"],
            "Recurring service charge"
        );
        assert_eq!(frozen["invoiceType"], "FINAL");
        assert_eq!(frozen["subtotal"], 2500);
        assert_eq!(frozen["lineItems"][0]["description"], "Frozen charge");
        assert_eq!(frozen["amountDue"], 2600);

        let current: serde_json::Value = server.graphql(&json!({
            "query": INVOICE_DETAILS,
            "variables": {"tenant": prefix, "first": 1, "filter": {"invoiceType": {"eq": "PREVIEW"}}}
        }), Some(&token)).await;
        assert!(current.get("errors").is_none(), "{current:#}");
        let current_invoice = &invoices_page(&current)["edges"][0]["node"];
        assert_eq!(current_invoice["dateStart"], current_month.to_string());
        assert_eq!(current_invoice["dateEnd"], current_end.to_string());
        assert_eq!(current_invoice["invoiceType"], "PREVIEW");
        assert_eq!(current_invoice["subtotal"], 1128);
        assert_eq!(current_invoice["extra"]["processed_data_gb"], 1.5);
        assert_eq!(current_invoice["extra"]["task_usage_hours"], 1.5);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_preview_page_only_requires_stats_for_selected_rows(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "previewpage";
        let prefix = "previewpage/";
        let user_id = provision_test_tenant(&pool, tenant).await;
        sqlx::query("UPDATE tenants SET created_at = '2024-03-01' WHERE tenant = $1")
            .bind(prefix)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO internal.manual_bills (tenant, usd_cents, description, date_start, date_end) VALUES ($1, 700, 'Manual charge', '2024-03-01', '2024-03-31')")
            .bind(prefix).execute(&pool).await.unwrap();
        reject_legacy_calculator(&pool).await;
        let (server, token) = preview_server(&pool, user_id, tenant, None, mock_provider()).await;
        let filter = json!({"dateStart": {"gt": "2024-02-29", "lt": "2024-04-01"}});
        let first = fetch_page(&server, &token, prefix, filter.clone(), json!({"first": 1})).await;
        assert!(first.get("errors").is_none(), "{first:#}");
        let page = invoices_page(&first);
        assert_eq!(page["edges"][0]["node"]["invoiceType"], "MANUAL");
        assert_eq!(page["pageInfo"]["hasNextPage"], true);
        let next = fetch_page(
            &server,
            &token,
            prefix,
            filter.clone(),
            json!({"first": 1, "after": cursor(page, "endCursor")}),
        )
        .await;
        assert!(
            next["errors"]
                .to_string()
                .contains("catalog stats are not configured"),
            "{next:#}"
        );
        let empty = fetch_page(
            &server,
            &token,
            prefix,
            json!({"invoiceType": {"eq": "FINAL"}}),
            json!({"first": 1}),
        )
        .await;
        assert!(empty.get("errors").is_none(), "{empty:#}");
        assert_eq!(invoices_page(&empty)["edges"].as_array().unwrap().len(), 0);
        let client = Arc::new(catalog_stats::test_util::connect_emulator("estuary-local").await);
        catalog_stats::test_util::delete_rows_with_prefix(&client, prefix)
            .await
            .unwrap();
        let (configured, configured_token) =
            preview_server(&pool, user_id, tenant, Some(client), mock_provider()).await;
        let second = fetch_page(
            &configured,
            &configured_token,
            prefix,
            filter.clone(),
            json!({"first": 1, "after": cursor(page, "endCursor")}),
        )
        .await;
        assert_eq!(
            invoices_page(&second)["edges"][0]["node"]["invoiceType"],
            "PREVIEW",
            "{second:#}"
        );
        let back = fetch_page(
            &configured,
            &configured_token,
            prefix,
            filter,
            json!({"last": 1, "before": cursor(invoices_page(&second), "startCursor")}),
        )
        .await;
        assert_eq!(
            invoices_page(&back)["edges"][0]["node"]["invoiceType"],
            "MANUAL",
            "{back:#}"
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_preview_read_failure_and_authorization(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let owner = "previewowner";
        let target = "previewtarget";
        let owner_id = provision_test_tenant(&pool, owner).await;
        let target_id = provision_test_tenant(&pool, target).await;
        reject_legacy_calculator(&pool).await;
        let client =
            Arc::new(catalog_stats::test_util::connect_emulator("nonexistent-instance").await);
        let (server, target_token) =
            preview_server(&pool, target_id, target, Some(client), mock_provider()).await;
        let response: serde_json::Value = server.graphql(&json!({
            "query": INVOICE_DETAILS,
            "variables": {"tenant": "previewtarget/", "first": 1, "filter": {"invoiceType": {"eq": "PREVIEW"}}}
        }), Some(&target_token)).await;
        assert!(
            response["errors"]
                .to_string()
                .contains("reading daily billing statistics"),
            "{response:#}"
        );
        assert!(
            response["data"]["tenant"]["billing"]["invoices"].is_null(),
            "{response:#}"
        );
        assert!(
            !response["errors"]
                .to_string()
                .contains("legacy calculator called")
        );
        let viewer_token = server.make_restricted_access_token(
            target_id,
            Some("previewtarget@example.test"),
            Some(vec!["viewer".to_string()]),
            None,
        );
        let viewer_denied: serde_json::Value = server.graphql(&json!({
            "query": INVOICE_DETAILS,
            "variables": {"tenant": "previewtarget/", "first": 1, "filter": {"invoiceType": {"eq": "PREVIEW"}}}
        }), Some(&viewer_token)).await;
        assert!(
            viewer_denied["errors"]
                .to_string()
                .contains("billing or admin"),
            "{viewer_denied:#}"
        );
        assert!(
            viewer_denied["errors"].to_string().contains("FORBIDDEN"),
            "{viewer_denied:#}"
        );
        assert!(
            !viewer_denied["errors"]
                .to_string()
                .contains("reading daily billing statistics")
        );

        let owner_token = server.make_access_token(owner_id, Some("previewowner@example.test"));
        let denied: serde_json::Value = server.graphql(&json!({
            "query": INVOICE_DETAILS,
            "variables": {"tenant": "previewtarget/", "first": 1, "filter": {"invoiceType": {"eq": "PREVIEW"}}}
        }), Some(&owner_token)).await;
        assert_eq!(
            denied["data"]["tenant"],
            serde_json::Value::Null,
            "{denied:#}"
        );
        assert!(denied.get("errors").is_some());
        assert!(
            !denied["errors"]
                .to_string()
                .contains("reading daily billing statistics")
        );
    }
}
