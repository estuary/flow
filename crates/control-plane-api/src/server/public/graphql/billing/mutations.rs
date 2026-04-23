use super::super::tenant::{validate_tenant_name, verify_tenant};
use super::billing_provider;
use super::payment_methods::PaymentMethod;
use crate::billing::{self, BillingProvider};
use anyhow::Context as _;
use async_graphql::{Context, Result, SimpleObject};

async fn require_customer_payment_methods(
    provider: &dyn BillingProvider,
    customer_id: &stripe::CustomerId,
    payment_method_id: &str,
) -> Result<Vec<stripe::PaymentMethod>> {
    let methods = provider
        .list_payment_methods(customer_id)
        .await
        .map_err(|err| async_graphql::Error::new(err.to_string()))?;

    if methods
        .iter()
        .all(|method| method.id.as_str() != payment_method_id)
    {
        return Err(async_graphql::Error::new(
            "payment method is not attached to the tenant's Stripe customer",
        ));
    }

    Ok(methods)
}

#[derive(Debug, Default)]
pub struct BillingMutation;

#[async_graphql::Object]
impl BillingMutation {
    async fn create_billing_setup_intent(
        &self,
        ctx: &Context<'_>,
        tenant: String,
    ) -> Result<CreateBillingSetupIntentPayload> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = validate_tenant_name(&tenant)?;
        verify_tenant(env, tenant.as_str(), models::Capability::Admin).await?;

        let claims = env.claims()?;
        let email = claims
            .email
            .as_deref()
            .context("authenticated user is missing an email claim")?;
        let full_name: Option<String> = sqlx::query_scalar(
            "SELECT raw_user_meta_data->>'full_name' FROM auth.users WHERE id = $1",
        )
        .bind(claims.sub)
        .fetch_one(&env.pg_pool)
        .await
        .map_err(|err| async_graphql::Error::new(err.to_string()))?;

        let provider = billing_provider(ctx)?;
        let customer = provider
            .as_ref()
            .find_or_create_customer(tenant.as_str(), email, full_name.as_deref())
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        let setup_intent = provider
            .create_setup_intent(&customer.id)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        let client_secret = setup_intent
            .client_secret
            .context("stripe setup intent response was missing client_secret")?;

        Ok(CreateBillingSetupIntentPayload { client_secret })
    }

    async fn set_billing_payment_method(
        &self,
        ctx: &Context<'_>,
        tenant: String,
        payment_method_id: String,
    ) -> Result<UpdateBillingPaymentMethodPayload> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = validate_tenant_name(&tenant)?;
        verify_tenant(env, tenant.as_str(), models::Capability::Admin).await?;

        let provider = billing_provider(ctx)?;
        let customer = provider
            .as_ref()
            .require_customer(tenant.as_str())
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        let methods =
            require_customer_payment_methods(provider.as_ref(), &customer.id, &payment_method_id)
                .await?;
        let updated_customer = provider
            .update_customer_default_payment_method(&customer.id, Some(payment_method_id.as_str()))
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;

        let primary_payment_method = billing::default_payment_method_id(&updated_customer)
            .and_then(|id| methods.iter().find(|m| m.id.as_str() == id))
            .map(PaymentMethod::from);
        Ok(UpdateBillingPaymentMethodPayload {
            payment_methods: methods.iter().map(PaymentMethod::from).collect(),
            primary_payment_method,
        })
    }

    async fn delete_billing_payment_method(
        &self,
        ctx: &Context<'_>,
        tenant: String,
        payment_method_id: String,
    ) -> Result<UpdateBillingPaymentMethodPayload> {
        let env = ctx.data::<crate::Envelope>()?;
        let tenant = validate_tenant_name(&tenant)?;
        verify_tenant(env, tenant.as_str(), models::Capability::Admin).await?;

        let provider = billing_provider(ctx)?;
        let customer = provider
            .as_ref()
            .require_customer(tenant.as_str())
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        let methods =
            require_customer_payment_methods(provider.as_ref(), &customer.id, &payment_method_id)
                .await?;
        let deleted_payment_method_id: stripe::PaymentMethodId = payment_method_id
            .parse()
            .map_err(|_| async_graphql::Error::new("invalid payment method ID"))?;
        let deleted_default_payment_method = billing::default_payment_method_id(&customer)
            .as_deref()
            == Some(payment_method_id.as_str());

        provider
            .detach_payment_method(&deleted_payment_method_id)
            .await
            .map_err(|err| async_graphql::Error::new(err.to_string()))?;
        let remaining_methods: Vec<stripe::PaymentMethod> = methods
            .into_iter()
            .filter(|method| method.id.as_str() != payment_method_id)
            .collect();

        let primary_id = if deleted_default_payment_method {
            let fallback = remaining_methods
                .first()
                .map(|method| method.id.to_string());
            let updated_customer = provider
                .update_customer_default_payment_method(&customer.id, fallback.as_deref())
                .await
                .map_err(|err| async_graphql::Error::new(err.to_string()))?;
            billing::default_payment_method_id(&updated_customer)
        } else {
            billing::default_payment_method_id(&customer)
        };
        let primary_payment_method = primary_id
            .and_then(|id| remaining_methods.iter().find(|m| m.id.as_str() == id))
            .map(PaymentMethod::from);

        Ok(UpdateBillingPaymentMethodPayload {
            payment_methods: remaining_methods.iter().map(PaymentMethod::from).collect(),
            primary_payment_method,
        })
    }
}

#[derive(Debug, Clone, SimpleObject)]
pub struct CreateBillingSetupIntentPayload {
    client_secret: String,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct UpdateBillingPaymentMethodPayload {
    payment_methods: Vec<PaymentMethod>,
    primary_payment_method: Option<PaymentMethod>,
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
    async fn graphql_billing_payment_methods_and_mutations(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let tenant = "billingmock";
        let user_id = provision_test_tenant(&pool, tenant).await;
        let victim_tenant = "billingvictim";
        let victim_user_id = provision_test_tenant(&pool, victim_tenant).await;

        let mock = billing::InMemoryBillingProvider::new();
        mock.add_customer("billingmock/", "cus_123", Some("pm_1"));
        mock.add_payment_method(
            "cus_123",
            "pm_1",
            stripe::PaymentMethodType::Card,
            stripe::BillingDetails {
                name: Some("Alice".to_string()),
                ..Default::default()
            },
            Some(stripe::CardDetails {
                brand: "visa".to_string(),
                last4: "4242".to_string(),
                ..Default::default()
            }),
            None,
        );
        mock.add_payment_method(
            "cus_123",
            "pm_2",
            stripe::PaymentMethodType::UsBankAccount,
            stripe::BillingDetails {
                name: Some("Alice".to_string()),
                ..Default::default()
            },
            None,
            Some(stripe::PaymentMethodUsBankAccount {
                bank_name: Some("STRIPE TEST BANK".to_string()),
                last4: Some("6789".to_string()),
                ..Default::default()
            }),
        );
        mock.add_customer("billingvictim/", "cus_victim", Some("pm_v"));
        mock.add_payment_method(
            "cus_victim",
            "pm_v",
            stripe::PaymentMethodType::Card,
            stripe::BillingDetails {
                name: Some("Victim".to_string()),
                ..Default::default()
            },
            Some(stripe::CardDetails {
                brand: "visa".to_string(),
                last4: "4444".to_string(),
                exp_month: 12,
                exp_year: 2030,
                ..Default::default()
            }),
            None,
        );

        let (server, token) = start_server_and_token(&pool, user_id, tenant, Arc::new(mock)).await;
        let victim_token = server.make_access_token(
            victim_user_id,
            Some(&format!("{victim_tenant}@example.test")),
        );

        let query_response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query {
                          tenant(name: "billingmock/") {
                            billing {
                              primaryPaymentMethod { id }
                              paymentMethods {
                                id
                                type
                                billingDetails {
                                  name
                                }
                                card {
                                  brand
                                  last4
                                  expMonth
                                  expYear
                                }
                                usBankAccount {
                                  bankName
                                  last4
                                  accountHolderType
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
        insta::assert_json_snapshot!("payment_methods_query", query_response);

        let mutation_response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        mutation {
                          setBillingPaymentMethod(tenant: "billingmock/", paymentMethodId: "pm_2") {
                            primaryPaymentMethod { id }
                            paymentMethods { id }
                          }
                        }
                    "#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("set_payment_method", mutation_response);

        // Delete the current default (pm_2); expect fallback to promote pm_1.
        let delete_default_response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        mutation {
                          deleteBillingPaymentMethod(tenant: "billingmock/", paymentMethodId: "pm_2") {
                            primaryPaymentMethod { id }
                            paymentMethods { id }
                          }
                        }
                    "#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("delete_default_payment_method", delete_default_response);

        let cross_tenant_delete_response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        mutation {
                          deleteBillingPaymentMethod(tenant: "billingmock/", paymentMethodId: "pm_v") {
                            primaryPaymentMethod { id }
                          }
                        }
                    "#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!("cross_tenant_delete_denied", cross_tenant_delete_response);

        let victim_query_response: serde_json::Value = server
            .graphql(
                &json!({
                    "query": r#"
                        query {
                          tenant(name: "billingvictim/") {
                            billing {
                              primaryPaymentMethod { id }
                              paymentMethods { id }
                            }
                          }
                        }
                    "#
                }),
                Some(&victim_token),
            )
            .await;
        insta::assert_json_snapshot!("victim_tenant_query", victim_query_response);
    }
}
