use std::sync::Arc;

use super::super::tenant::{Tenant, verify_tenant};
use super::billing_provider;
use super::contact::{self, BillingContact};
use super::invoices::{Invoice, InvoiceCursor, InvoiceFilter};
use super::payment_methods::PaymentMethod;
use crate::billing::{self, BillingProvider};
use async_graphql::{
    ComplexObject, Context, Result,
    connection::{self, Connection},
};

#[ComplexObject]
impl Tenant {
    async fn billing(&self, ctx: &Context<'_>) -> Result<TenantBilling> {
        let env = ctx.data::<crate::Envelope>()?;
        verify_tenant(env, &self.name, models::Capability::Admin).await?;
        let provider = billing_provider(ctx)?;
        Ok(TenantBilling::new(self.name.clone(), provider))
    }
}

#[derive(Debug, Clone)]
pub struct TenantBilling {
    tenant: String,
    provider: Arc<dyn BillingProvider>,
}

impl TenantBilling {
    fn new(tenant: String, provider: Arc<dyn BillingProvider>) -> Self {
        Self { tenant, provider }
    }
}

#[async_graphql::Object]
impl TenantBilling {
    async fn contact(&self, ctx: &Context<'_>) -> Result<BillingContact> {
        let env = ctx.data::<crate::Envelope>()?;
        contact::fetch_billing_contact(&env.pg_pool, &self.tenant)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))
    }

    async fn payment_methods(&self) -> Result<Vec<PaymentMethod>> {
        let Some(customer) = self
            .provider
            .as_ref()
            .find_customer(&self.tenant)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?
        else {
            return Ok(Vec::new());
        };
        let methods = self
            .provider
            .list_payment_methods(&customer.id)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        Ok(methods.iter().map(PaymentMethod::from).collect())
    }

    async fn primary_payment_method(&self) -> Result<Option<PaymentMethod>> {
        let Some(customer) = self
            .provider
            .as_ref()
            .find_customer(&self.tenant)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?
        else {
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

        // verify_tenant runs before tenant_exists, so querying another tenant
        // (or a nonexistent one) fails identically; one assertion is enough.
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
