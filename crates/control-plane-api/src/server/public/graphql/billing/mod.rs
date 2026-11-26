use std::sync::Arc;

use crate::billing::BillingProvider;
use async_graphql::Context;

pub(super) mod contact;
mod invoices;
mod loaders;
mod mutations;
mod payment_methods;
mod plan;
mod tenant;

pub(super) use loaders::{ChargeDataLoader, CustomerDataLoader, StripeInvoiceLoader};
pub use mutations::BillingMutation;

fn billing_provider(ctx: &Context<'_>) -> async_graphql::Result<Arc<dyn BillingProvider>> {
    ctx.data::<Arc<dyn BillingProvider>>()
        .cloned()
        .map_err(|_| async_graphql::Error::new("Billing is not configured"))
}

/// Errors unless the tenant of `catalog_name` is paid. Contract and internal
/// tenants are paid. A self-serve tenant is paid when it is sent invoices, or
/// has a default payment method.
pub(super) async fn require_paid_tenant(
    ctx: &Context<'_>,
    catalog_name: &str,
) -> async_graphql::Result<()> {
    // Without a billing provider, no tenant can add a payment method.
    let Ok(provider) = billing_provider(ctx) else {
        return Ok(());
    };
    let env = ctx.data::<crate::Envelope>()?;
    let tenant = models::tenant_from(catalog_name);

    let needs_payment_method = sqlx::query_scalar!(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM tenants
            WHERE tenant = $1
              AND plan_kind = 'self_serve'
              AND collection_method = 'charge_default_payment_method'
        ) AS "exists!"
        "#,
        tenant,
    )
    .fetch_one(&env.pg_pool)
    .await?;

    if !needs_payment_method {
        return Ok(());
    }

    let customer = crate::billing::find_customer(&env.pg_pool, provider.as_ref(), tenant).await?;
    if customer
        .as_ref()
        .and_then(crate::billing::default_payment_method_id)
        .is_some()
    {
        return Ok(());
    }

    Err(async_graphql::Error::new(format!(
        "{tenant} must add a payment method to use invite links and service accounts"
    )))
}

#[cfg(test)]
pub(crate) mod test_util {
    use crate::{billing, test_server};
    use serde_json::json;
    use std::sync::Arc;

    pub async fn provision_test_tenant(pool: &sqlx::PgPool, tenant: &str) -> uuid::Uuid {
        crate::tenants::provision_test_tenant(
            pool,
            tenant,
            &format!("{tenant}@example.test"),
            json!({"full_name": format!("{tenant} admin")}),
        )
        .await
    }

    pub fn mock_provider() -> Arc<dyn billing::BillingProvider> {
        Arc::new(billing::InMemoryBillingProvider::new())
    }

    pub async fn insert_billing_historical(
        pool: &sqlx::PgPool,
        tenant: &str,
        month: &str,
        subtotal: i32,
        description: &str,
    ) {
        let report = billing_types::Invoice {
            billed_month: format!("{month}T00:00:00+00:00"),
            billed_prefix: format!("{tenant}/"),
            daily_usage: Vec::new(),
            line_items: vec![billing_types::LineItem {
                description: Some(description.to_string()),
                count: 1,
                rate: subtotal.into(),
                subtotal: subtotal.into(),
            }],
            processed_data_gb: 0.0,
            recurring_fee: 0,
            subtotal: subtotal.into(),
            task_usage_hours: 0.0,
            trial_credit: 0,
            free_tier_credit: 0,
            trial_start: None,
        };
        sqlx::query(
            "insert into internal.billing_historicals (tenant, billed_month, report) values ($1, $2::timestamptz, $3)",
        )
        .bind(&report.billed_prefix)
        .bind(&report.billed_month)
        .bind(sqlx::types::Json(&report))
        .execute(pool)
        .await
        .expect("insert billing historical");
    }

    pub const INVOICES_PAGE_QUERY: &str = r#"
        query InvoicesPage(
            $tenant: String!
            $filter: InvoiceFilter
            $after: String
            $before: String
            $first: Int
            $last: Int
        ) {
            tenant(name: $tenant) {
                billing {
                    invoices(
                        after: $after
                        before: $before
                        first: $first
                        last: $last
                        filter: $filter
                    ) {
                        pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
                        edges { cursor node { dateStart dateEnd invoiceType } }
                    }
                }
            }
        }
    "#;

    pub async fn start_server_and_token(
        pool: &sqlx::PgPool,
        user_id: uuid::Uuid,
        tenant: &str,
        provider: Arc<dyn billing::BillingProvider>,
    ) -> (test_server::TestServer, String) {
        let server = test_server::TestServer::start_with_config(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
            Some(provider),
            None,
            models::AlertConfig::default(),
        )
        .await;
        let token = server.make_access_token(user_id, Some(&format!("{tenant}@example.test")));
        (server, token)
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::provision_test_tenant;
    use crate::{billing, test_server};
    use serde_json::json;
    use std::sync::Arc;

    async fn attach_test_card(
        client: &stripe::Client,
        customer_id: &stripe::CustomerId,
        test_pm_token: &str,
    ) -> stripe::PaymentMethod {
        let pm_id: stripe::PaymentMethodId = test_pm_token.parse().unwrap();
        stripe::PaymentMethod::attach(
            client,
            &pm_id,
            stripe::AttachPaymentMethod {
                customer: customer_id.clone(),
            },
        )
        .await
        .expect("attach test payment method")
    }

    /// Exercises every Stripe API call made by the billing GraphQL mutations:
    ///   - Customer retrieve, create, update
    ///   - SetupIntent create
    ///   - PaymentMethod list, detach
    #[ignore = "requires STRIPE_API_KEY set to a Stripe testmode key"]
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn graphql_billing_live_stripe(pool: sqlx::PgPool) {
        use crate::billing::StripeBillingProvider;

        let _guard = test_server::init();
        let stripe_key =
            std::env::var("STRIPE_API_KEY").expect("STRIPE_API_KEY must be set to run this test");
        let stripe_client = stripe::Client::new(stripe_key.clone());

        let tenant = format!("stripeit{}", uuid::Uuid::new_v4().simple());
        let user_id = provision_test_tenant(&pool, &tenant).await;
        let provider: Arc<dyn billing::BillingProvider> =
            Arc::new(StripeBillingProvider::new(stripe_key));
        let server = test_server::TestServer::start_with_config(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
            Some(provider.clone()),
            None,
            models::AlertConfig::default(),
        )
        .await;
        let token = server.make_access_token(user_id, Some(&format!("{tenant}@example.test")));

        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": format!(r#"
                        mutation {{
                          createBillingSetupIntent(tenant: "{tenant}/") {{
                            clientSecret
                          }}
                        }}
                    "#)
                }),
                Some(&token),
            )
            .await;
        assert!(
            response["data"]["createBillingSetupIntent"]["clientSecret"]
                .as_str()
                .is_some(),
            "setup intent should return a client secret: {response:?}"
        );

        // The setup intent recorded the customer it created, so the mutations
        // below retrieve it by ID rather than racing Stripe's search index.
        let customer = billing::require_customer(&pool, provider.as_ref(), &format!("{tenant}/"))
            .await
            .expect("setup intent recorded the tenant's customer");

        // Attach payment methods directly via the Stripe API (what Stripe.js
        // would do client-side after the SetupIntent completes).
        let card_a = attach_test_card(&stripe_client, &customer.id, "pm_card_visa").await;
        let card_b = attach_test_card(&stripe_client, &customer.id, "pm_card_mastercard").await;

        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": format!(r#"
                        mutation {{
                          setBillingPaymentMethod(tenant: "{tenant}/", paymentMethodId: "{}") {{
                            primaryPaymentMethod {{ id }}
                            paymentMethods {{ id }}
                          }}
                        }}
                    "#, card_a.id)
                }),
                Some(&token),
            )
            .await;
        assert_eq!(
            response["data"]["setBillingPaymentMethod"]["primaryPaymentMethod"]["id"],
            json!(card_a.id.to_string()),
            "card_a should be set as primary: {response:?}"
        );
        let pm_ids: Vec<&str> = response["data"]["setBillingPaymentMethod"]["paymentMethods"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|pm| pm["id"].as_str())
            .collect();
        assert!(
            pm_ids.contains(&card_a.id.as_str()),
            "card_a should be listed"
        );
        assert!(
            pm_ids.contains(&card_b.id.as_str()),
            "card_b should be listed"
        );

        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": format!(r#"
                        mutation {{
                          deleteBillingPaymentMethod(tenant: "{tenant}/", paymentMethodId: "{}") {{
                            primaryPaymentMethod {{ id }}
                            paymentMethods {{ id }}
                          }}
                        }}
                    "#, card_a.id)
                }),
                Some(&token),
            )
            .await;
        assert_eq!(
            response["data"]["deleteBillingPaymentMethod"]["primaryPaymentMethod"]["id"],
            json!(card_b.id.to_string()),
            "card_b should become primary after deleting card_a: {response:?}"
        );
        let pm_ids: Vec<&str> = response["data"]["deleteBillingPaymentMethod"]["paymentMethods"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|pm| pm["id"].as_str())
            .collect();
        assert!(
            !pm_ids.contains(&card_a.id.as_str()),
            "card_a should be gone"
        );
        assert!(pm_ids.contains(&card_b.id.as_str()), "card_b should remain");

        // A second setup-intent on the same tenant: exercises the "find"
        // branch of find_or_create_customer.
        let response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": format!(r#"
                        mutation {{
                          createBillingSetupIntent(tenant: "{tenant}/") {{
                            clientSecret
                          }}
                        }}
                    "#)
                }),
                Some(&token),
            )
            .await;
        assert!(
            response["data"]["createBillingSetupIntent"]["clientSecret"]
                .as_str()
                .is_some(),
            "setup intent for existing customer should return a client secret: {response:?}"
        );
    }
}
