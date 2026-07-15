use std::sync::Arc;

use crate::billing::BillingProvider;
use async_graphql::Context;

pub(super) mod contact;
mod invoices;
mod loaders;
mod mutations;
mod payment_methods;
mod tenant;

pub(super) use loaders::{ChargeDataLoader, CustomerDataLoader, StripeInvoiceLoader};
pub use mutations::BillingMutation;

fn billing_provider(ctx: &Context<'_>) -> async_graphql::Result<Arc<dyn BillingProvider>> {
    ctx.data::<Arc<dyn BillingProvider>>()
        .cloned()
        .map_err(|_| async_graphql::Error::new("Billing is not configured"))
}

#[cfg(test)]
pub(crate) mod test_util {
    use crate::{billing, test_server};
    use serde_json::json;
    use std::sync::Arc;

    pub async fn provision_test_tenant(pool: &sqlx::PgPool, tenant: &str) -> uuid::Uuid {
        crate::directives::beta_onboard::provision_test_tenant(
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
        let billed_at = format!("{month}T00:00:00Z");
        sqlx::query(
            r#"
            insert into internal.billing_historicals (tenant, billed_month, report)
            values (
                $1,
                $2::timestamptz,
                jsonb_build_object(
                    'billed_month', $2,
                    'subtotal', $3::int,
                    'line_items', jsonb_build_array(jsonb_build_object('description', $4, 'subtotal', $3::int))
                )
            )
            "#,
        )
        .bind(format!("{tenant}/"))
        .bind(&billed_at)
        .bind(subtotal)
        .bind(description)
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

    async fn wait_for_customer_searchable(
        provider: &dyn billing::BillingProvider,
        tenant: &str,
    ) -> stripe::Customer {
        for _ in 0..30 {
            if let Ok(Some(customer)) = provider.find_customer(tenant).await {
                return customer;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        panic!("customer for tenant '{tenant}' never became searchable after 60s");
    }

    /// Exercises every Stripe API call made by the billing GraphQL mutations:
    ///   - Customer search, create, update
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
            test_server::snapshot(pool, true).await,
            Some(provider.clone()),
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

        // Stripe's customer-search index lags writes by seconds; the mutations
        // below all depend on search to find the customer.
        let customer = wait_for_customer_searchable(provider.as_ref(), &format!("{tenant}/")).await;

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
