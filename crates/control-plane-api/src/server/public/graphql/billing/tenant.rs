use super::super::TimestampCursor;
use super::super::tenant::Tenant;
use super::super::verify_authorization;
use super::adjustments::{
    BillingAdjustment, fetch_adjustments_backward, fetch_adjustments_forward,
};
use super::billing_provider;
use super::contact::{self, BillingContact};
use super::invoices::{Invoice, InvoiceFilter};
use super::loaders::CustomerDataLoader;
use super::payment_methods::PaymentMethod;
use crate::billing::{self, InvoiceCursor};
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
        Ok(TenantBilling {
            tenant: self.name.clone(),
        })
    }
}

/// The billing provider is resolved lazily by the fields which actually
/// require it (payment methods), so that database-backed fields like
/// `contact`, `invoices`, and `adjustments` work on deployments where
/// billing is not configured.
#[derive(Debug, Clone)]
pub struct TenantBilling {
    tenant: String,
}

#[async_graphql::Object]
impl TenantBilling {
    async fn contact(&self, ctx: &Context<'_>) -> Result<BillingContact> {
        let env = ctx.data::<crate::Envelope>()?;
        contact::fetch_billing_contact(&env.pg_pool, &self.tenant)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))
    }

    async fn payment_methods(&self, ctx: &Context<'_>) -> Result<Vec<PaymentMethod>> {
        let provider = billing_provider(ctx)?;
        let loader = ctx.data::<DataLoader<CustomerDataLoader>>()?;
        let Some(customer) = loader.load_one(self.tenant.clone()).await? else {
            return Ok(Vec::new());
        };
        let methods = provider
            .list_payment_methods(&customer.id)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        Ok(methods.iter().map(PaymentMethod::from).collect())
    }

    async fn primary_payment_method(&self, ctx: &Context<'_>) -> Result<Option<PaymentMethod>> {
        let provider = billing_provider(ctx)?;
        let loader = ctx.data::<DataLoader<CustomerDataLoader>>()?;
        let Some(customer) = loader.load_one(self.tenant.clone()).await? else {
            return Ok(None);
        };
        let Some(primary_id) = billing::default_payment_method_id(&customer) else {
            return Ok(None);
        };
        let pm = provider
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

    /// Billing adjustments (credits and fees) applied to this tenant's
    /// invoices, ordered newest-first.
    async fn adjustments(
        &self,
        ctx: &Context<'_>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> Result<Connection<TimestampCursor, BillingAdjustment>> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = self.tenant.clone();

        connection::query_with::<TimestampCursor, _, _, _, async_graphql::Error>(
            after,
            before,
            first,
            last,
            |after, before, first, last| async move {
                let has_before = before.is_some();
                let has_after = after.is_some();
                let (rows, has_prev, has_next) = if has_before || last.is_some() {
                    let (rows, has_prev) = fetch_adjustments_backward(
                        &env.pg_pool,
                        &tenant,
                        before.map(|c| c.0),
                        last,
                    )
                    .await
                    .map_err(async_graphql::Error::from)?;
                    (rows, has_prev, has_before)
                } else {
                    let (rows, has_next) =
                        fetch_adjustments_forward(&env.pg_pool, &tenant, after.map(|c| c.0), first)
                            .await
                            .map_err(async_graphql::Error::from)?;
                    (rows, has_after, has_next)
                };

                let mut connection = Connection::new(has_prev, has_next);
                connection.edges.extend(rows.into_iter().map(|row| {
                    let cursor = TimestampCursor(row.created_at);
                    connection::Edge::new(cursor, row)
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
