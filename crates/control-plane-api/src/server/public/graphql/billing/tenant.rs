use std::sync::Arc;

use super::super::tenant::Tenant;
use super::super::verify_authorization;
use super::billing_provider;
use super::contact::{self, BillingContact};
use super::invoices::{Invoice, InvoiceFilter};
use super::loaders::CustomerDataLoader;
use super::payment_methods::PaymentMethod;
use crate::billing::{self, BillingProvider, InvoiceCursor};
use async_graphql::{
    ComplexObject, Context, Result,
    connection::{self, Connection},
    dataloader::DataLoader,
};

#[ComplexObject]
impl Tenant {
    async fn billing(&self, ctx: &Context<'_>) -> Result<TenantBilling> {
        let env = ctx.data::<crate::Envelope>()?;
        verify_authorization(env, &self.name, models::authz::Capability::ViewBilling).await?;
        let provider = billing_provider(ctx)?;
        let (trial_start, trial_end, payment_provider, is_gcp_marketplace) = sqlx::query_as::<
            _,
            (Option<chrono::NaiveDate>, Option<chrono::NaiveDate>, Option<billing_types::PaymentProvider>, bool),
        >(
            "SELECT trial_start, (trial_start + INTERVAL '1 month')::date, payment_provider, gcm_account_id IS NOT NULL FROM tenants WHERE tenant = $1",
        )
        .bind(&self.name)
        .fetch_one(&env.pg_pool)
        .await?;

        Ok(TenantBilling {
            tenant: self.name.clone(),
            provider,
            trial_start,
            trial_end,
            payment_provider,
            is_gcp_marketplace,
        })
    }
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct TenantBilling {
    #[graphql(skip)]
    tenant: String,
    #[graphql(skip)]
    provider: Arc<dyn BillingProvider>,
    /// Date the tenant's free trial started, or null if it has not started.
    trial_start: Option<chrono::NaiveDate>,
    /// Exclusive end date of the free trial, one calendar month after trialStart.
    trial_end: Option<chrono::NaiveDate>,
    payment_provider: Option<billing_types::PaymentProvider>,
    /// Whether the tenant is linked to a Google Cloud Marketplace account.
    is_gcp_marketplace: bool,
}

#[ComplexObject]
impl TenantBilling {
    async fn contact(&self, ctx: &Context<'_>) -> Result<BillingContact> {
        let env = ctx.data::<crate::Envelope>()?;
        contact::fetch_billing_contact(&env.pg_pool, &self.tenant)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))
    }

    async fn payment_methods(&self, ctx: &Context<'_>) -> Result<Vec<PaymentMethod>> {
        let loader = ctx.data::<DataLoader<CustomerDataLoader>>()?;
        let Some(customer) = loader.load_one(self.tenant.clone()).await? else {
            return Ok(Vec::new());
        };
        let methods = self
            .provider
            .list_payment_methods(&customer.id)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        Ok(methods.iter().map(PaymentMethod::from).collect())
    }

    async fn primary_payment_method(&self, ctx: &Context<'_>) -> Result<Option<PaymentMethod>> {
        let loader = ctx.data::<DataLoader<CustomerDataLoader>>()?;
        let Some(customer) = loader.load_one(self.tenant.clone()).await? else {
            return Ok(None);
        };
        let Some(primary_id) = billing::default_payment_method_id(&customer) else {
            return Ok(None);
        };
        let pm = self
            .provider
            .get_payment_method(&primary_id.parse().map_err(|_| {
                async_graphql::Error::new("invalid payment method ID in customer default")
            })?)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        Ok(Some(PaymentMethod::from(&pm)))
    }

    async fn invoices(
        &self,
        ctx: &Context<'_>,
        filter: Option<InvoiceFilter>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<Connection<InvoiceCursor, Invoice>> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = self.tenant.clone();
        let query = filter.unwrap_or_default().into_query();

        connection::query_with::<InvoiceCursor, _, _, _, async_graphql::Error>(
            after,
            before,
            first,
            last,
            |after, before, first, last| async move {
                let (rows, has_prev, has_next) = if before.is_some() || last.is_some() {
                    let (rows, has_prev) = billing::fetch_invoice_rows_backward(
                        &env.pg_pool,
                        &tenant,
                        &query,
                        before,
                        last,
                    )
                    .await
                    .map_err(async_graphql::Error::from)?;
                    (rows, has_prev, before.is_some())
                } else {
                    let (rows, has_next) = billing::fetch_invoice_rows_forward(
                        &env.pg_pool,
                        &tenant,
                        &query,
                        after,
                        first,
                    )
                    .await
                    .map_err(async_graphql::Error::from)?;
                    (rows, after.is_some(), has_next)
                };

                let mut connection = Connection::new(has_prev, has_next);
                connection.edges.extend(rows.into_iter().map(|row| {
                    let cursor = InvoiceCursor::from_row(&row);
                    let invoice = Invoice::from_row(row);
                    connection::Edge::new(cursor, invoice)
                }));
                Ok(connection)
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use crate::test_server;
    use serde_json::json;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes"))
    )]
    async fn graphql_billing_metadata(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "billingmetadata";
        let user_id = provision_test_tenant(&pool, tenant).await;
        let (server, token) = start_server_and_token(&pool, user_id, tenant, mock_provider()).await;
        let query = json!({
            "query": r#"{
                tenant(name: "billingmetadata/") {
                    billing { trialStart trialEnd paymentProvider isGcpMarketplace }
                }
            }"#
        });

        let initial: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(initial, @r#"
        {
          "data": {
            "tenant": {
              "billing": {
                "isGcpMarketplace": false,
                "paymentProvider": "STRIPE",
                "trialEnd": null,
                "trialStart": null
              }
            }
          }
        }
        "#);

        let account_id = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO internal.gcm_accounts (id) VALUES ($1)")
            .bind(account_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE tenants SET trial_start = '2026-10-01', payment_provider = 'external', gcm_account_id = $1 WHERE tenant = 'billingmetadata/'",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .unwrap();

        let marketplace: serde_json::Value = server.graphql(&query, Some(&token)).await;
        insta::assert_json_snapshot!(marketplace, @r#"
        {
          "data": {
            "tenant": {
              "billing": {
                "isGcpMarketplace": true,
                "paymentProvider": "EXTERNAL",
                "trialEnd": "2026-11-01",
                "trialStart": "2026-10-01"
              }
            }
          }
        }
        "#);

        sqlx::query("UPDATE tenants SET payment_provider = NULL WHERE tenant = 'billingmetadata/'")
            .execute(&pool)
            .await
            .unwrap();
        let nullable: serde_json::Value = server.graphql(&query, Some(&token)).await;
        assert_eq!(
            nullable["data"]["tenant"]["billing"]["paymentProvider"],
            json!(null)
        );
        assert_eq!(
            nullable["data"]["tenant"]["billing"]["isGcpMarketplace"],
            json!(true)
        );
        assert!(nullable.get("errors").is_none());

        for (start, end) in [
            ("2026-01-31", "2026-02-28"),
            ("2024-01-31", "2024-02-29"),
            ("2024-02-29", "2024-03-29"),
        ] {
            sqlx::query(
                "UPDATE tenants SET trial_start = $1::text::date WHERE tenant = 'billingmetadata/'",
            )
            .bind(start)
            .execute(&pool)
            .await
            .unwrap();
            let response: serde_json::Value = server.graphql(&query, Some(&token)).await;
            assert_eq!(
                response["data"]["tenant"]["billing"]["trialStart"],
                json!(start)
            );
            assert_eq!(
                response["data"]["tenant"]["billing"]["trialEnd"],
                json!(end)
            );
        }

        let viewer_token = server.make_restricted_access_token(
            user_id,
            None,
            Some(vec!["viewer".to_string()]),
            None,
        );
        let denied: serde_json::Value = server.graphql(&query, Some(&viewer_token)).await;
        assert_eq!(denied["data"]["tenant"], json!(null));
        assert_eq!(denied["errors"].as_array().map(Vec::len), Some(1));
        assert_eq!(denied["errors"][0]["path"], json!(["tenant", "billing"]));
    }

    /// `Query.tenant` errors when the caller lacks Read on the requested
    /// prefix: the response is null + a single error.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_tenant_query_authorization(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let owner_tenant = "tenantowner";
        let target_tenant = "tenanttarget";
        let owner_user_id = provision_test_tenant(&pool, owner_tenant).await;
        let _target_user_id = provision_test_tenant(&pool, target_tenant).await;

        let (server, token) =
            start_server_and_token(&pool, owner_user_id, owner_tenant, mock_provider()).await;

        let unauthorized: serde_json::Value = server
            .graphql(
                &json!({
                    "query": format!(r#"
                        query {{
                          tenant(name: "{target_tenant}/") {{
                            name
                          }}
                        }}
                    "#)
                }),
                Some(&token),
            )
            .await;
        assert_eq!(unauthorized["data"]["tenant"], serde_json::Value::Null);
        assert_eq!(unauthorized["errors"].as_array().map(Vec::len), Some(1));
    }
}
