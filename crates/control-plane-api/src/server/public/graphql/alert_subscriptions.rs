use anyhow::Context as _;
use async_graphql::Context;
use models::status::AlertType;

use crate::alert_subscriptions::{
    AlertSubscription, create_alert_subscription, delete_alert_subscription,
    fetch_alert_subscriptions_prefixed_by, fetch_alert_subsription_for_update,
    update_alert_subscription,
};

#[derive(Debug, Default)]
pub struct AlertSubscriptionsQuery;

#[derive(Debug, Clone, async_graphql::InputObject)]
pub struct AlertSubscriptionsBy {
    /// Show alert subscriptions for the given catalog namespace prefix. This
    /// will return all subscriptions having a catalog prefix that starts with
    /// the given prefix. For example, `prefix: "acmeCo/"` would return
    /// subscriptions for both `acmeCo/` and `acmeCo/nested/`.
    prefix: models::Prefix,
}

#[async_graphql::Object]
impl AlertSubscriptionsQuery {
    /// Returns a complete list of alert subscriptions.
    async fn alert_subscriptions(
        &self,
        ctx: &Context<'_>,
        by: AlertSubscriptionsBy,
    ) -> async_graphql::Result<Vec<AlertSubscription>> {
        let env = ctx.data::<crate::Envelope>()?;

        let _ = verify_authorization(&env, &by.prefix).await?;

        let mut conn = env.pg_pool.acquire().await?;
        let alerts = fetch_alert_subscriptions_prefixed_by(&by.prefix, &mut conn).await?;
        Ok(alerts)
    }
}

#[derive(Debug, Default)]
pub struct AlertSubscriptionsMutation;

#[async_graphql::Object]
impl AlertSubscriptionsMutation {
    /// Creates a new alert subscription. Returns an error if there is already
    /// an existing subscription for the same prefix and email address.
    pub async fn create_alert_subscription(
        &self,
        ctx: &Context<'_>,
        prefix: models::Prefix,
        email: String,
        alert_types: Option<Vec<AlertType>>,
        detail: Option<String>,
    ) -> async_graphql::Result<AlertSubscription> {
        let env = ctx.data::<crate::Envelope>()?;

        let _ = verify_authorization(&env, &prefix).await?;

        // Validate the email address. Note that we _don't_ support mailbox
        // address syntax like `Foo <foo@bar.test>`. We just want the plain
        // email address.
        if !validator::ValidateEmail::validate_email(&email) {
            return Err(async_graphql::Error::new(
                "email address is invalid, expected a plain email address like 'foo@bar.test', see: https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address",
            ));
        }

        let mut txn = env.pg_pool.begin().await?;

        let duplicate = fetch_alert_subsription_for_update(&prefix, &email, &mut *txn).await?;
        if duplicate.is_some() {
            return Err(async_graphql::Error::new(format!(
                "an alert subscription already exists for email '{}' and prefix '{}'",
                email, prefix,
            )));
        }
        let updated = create_alert_subscription(
            prefix.as_str(),
            email.as_str(),
            alert_types.as_deref().unwrap_or(DEFAULT_ALERT_TYPES),
            detail.as_deref(),
            &mut *txn,
        )
        .await?;

        let _ = txn
            .commit()
            .await
            .context("committing alert subscription update txn")?;

        tracing::info!(%prefix, email = %email, "created alert subscription");
        Ok(updated)
    }

    /// Updates the alert subscription for the given prefix and email, returning
    /// the updated subscription.
    pub async fn update_alert_subscription(
        &self,
        ctx: &Context<'_>,
        prefix: models::Prefix,
        email: String,
        alert_types: Option<Vec<AlertType>>,
        detail: Option<String>,
    ) -> async_graphql::Result<AlertSubscription> {
        let env = ctx.data::<crate::Envelope>()?;

        let _ = verify_authorization(&env, &prefix).await?;
        if alert_types.is_none() && detail.is_none() {
            return Err(async_graphql::Error::new(
                "must provide at least one of: alertTypes, detail",
            ));
        }

        let mut txn = env.pg_pool.begin().await?;
        let Some(existing) =
            fetch_alert_subsription_for_update(prefix.as_str(), email.as_str(), &mut *txn).await?
        else {
            return Err(async_graphql::Error::new(format!(
                "no alert subscription exists for prefix '{prefix}' and email '{email}'"
            )));
        };

        let new_detail = detail.as_deref().or(existing.detail.as_deref());

        let updated = update_alert_subscription(
            prefix.as_str(),
            email.as_str(),
            alert_types.as_deref().unwrap_or(&existing.alert_types),
            new_detail,
            &mut *txn,
        )
        .await?;
        txn.commit()
            .await
            .context("committing alert subscription update transaction")?;

        tracing::info!(%prefix, %email, "updated alert subscription");
        Ok(updated)
    }

    /// Delete an alert subscription that exactly matches the given prefix and email.
    pub async fn delete_alert_subscription(
        &self,
        ctx: &Context<'_>,
        prefix: models::Prefix,
        email: String,
    ) -> async_graphql::Result<AlertSubscription> {
        let env = ctx.data::<crate::Envelope>()?;

        let _ = verify_authorization(&env, &prefix).await?;

        let Some(existing) =
            delete_alert_subscription(prefix.as_str(), email.as_str(), &env.pg_pool).await?
        else {
            return Err(async_graphql::Error::new(format!(
                "no alert subscription exists for prefix '{prefix}' and email '{email}'"
            )));
        };
        tracing::info!(%prefix, %email, "deleted alert subscription");
        Ok(existing)
    }
}

/// Ensures that the user has admin capability to the prefix, which is required
/// for both viewing and modifying alert subscriptions.
async fn verify_authorization(
    envelope: &crate::Envelope,
    catalog_prefix: &str,
) -> async_graphql::Result<()> {
    let policy_result = crate::server::evaluate_names_authorization(
        envelope.snapshot(),
        envelope.claims()?,
        models::Capability::Admin,
        [catalog_prefix],
    );
    let (_expiry, ()) = envelope.authorization_outcome(policy_result).await?;
    Ok(())
}

const DEFAULT_ALERT_TYPES: &'static [AlertType] = &[
    AlertType::DataMovementStalled,
    AlertType::ShardFailed,
    AlertType::FreeTrial,
    AlertType::FreeTrialEnding,
    AlertType::FreeTrialStalled,
    AlertType::MissingPaymentMethod,
];

#[cfg(test)]
mod test {
    use crate::test_server;
    //use flow_client_next as flow_client;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_alert_subscription_crud(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.test"),
        );

        // Start by testing an empty list response
        let list_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query {
                        alertSubscriptions(by: {prefix: "aliceCo/"}) {
                        catalogPrefix
                        email
                        destination
                        detail
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(list_response, @r#"
        {
          "data": {
            "alertSubscriptions": []
          }
        }
        "#);

        // Don't specify alert types on creation. Expect response to show default alert types.
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
             mutation {
               createAlertSubscription(
                 prefix: "aliceCo/"
                 email: "alice@example.test"
                 detail: "test detail"
               ) {
               alertTypes
               }
            }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(create_response, @r#"
        {
          "data": {
            "createAlertSubscription": {
              "alertTypes": [
                "data_movement_stalled",
                "shard_failed",
                "free_trial",
                "free_trial_ending",
                "free_trial_stalled",
                "missing_payment_method"
              ]
            }
          }
        }
        "#);
        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
             mutation {
               createAlertSubscription(
                 prefix: "aliceCo/nested/"
                 email: "different@example.test"
                 alertTypes: ["shard_failed","auto_discover_failed"]
               ) {
               catalogPrefix
               email
               destination
               alertTypes
               }
            }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(create_response, @r#"
        {
          "data": {
            "createAlertSubscription": {
              "alertTypes": [
                "shard_failed",
                "auto_discover_failed"
              ],
              "catalogPrefix": "aliceCo/nested/",
              "destination": "mailto:different@example.test",
              "email": "different@example.test"
            }
          }
        }
        "#);

        let list_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query {
                        alertSubscriptions(by: {prefix: "aliceCo/"}) {
                        catalogPrefix
                        email
                        destination
                        detail
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(list_response, @r#"
        {
          "data": {
            "alertSubscriptions": [
              {
                "catalogPrefix": "aliceCo/",
                "destination": "mailto:alice@example.test",
                "detail": "test detail",
                "email": "alice@example.test"
              },
              {
                "catalogPrefix": "aliceCo/nested/",
                "destination": "mailto:different@example.test",
                "detail": null,
                "email": "different@example.test"
              }
            ]
          }
        }
        "#);

        let update_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
             mutation {
               updateAlertSubscription(
                 prefix: "aliceCo/nested/"
                 email: "different@example.test"
                 alertTypes: ["auto_discover_failed"]
                 detail: "new detail"
               ) {
               catalogPrefix
               email
               destination
               alertTypes
               detail
               }
            }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(update_response, @r#"
        {
          "data": {
            "updateAlertSubscription": {
              "alertTypes": [
                "auto_discover_failed"
              ],
              "catalogPrefix": "aliceCo/nested/",
              "destination": "mailto:different@example.test",
              "detail": "new detail",
              "email": "different@example.test"
            }
          }
        }
        "#);

        let delete_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                     mutation {
                       deleteAlertSubscription(
                         prefix: "aliceCo/"
                         email: "alice@example.test"
                       ) {
                       catalogPrefix
                       email
                       destination
                       alertTypes
                       detail
                       }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(delete_response, @r#"
        {
          "data": {
            "deleteAlertSubscription": {
              "alertTypes": [
                "data_movement_stalled",
                "shard_failed",
                "free_trial",
                "free_trial_ending",
                "free_trial_stalled",
                "missing_payment_method"
              ],
              "catalogPrefix": "aliceCo/",
              "destination": "mailto:alice@example.test",
              "detail": "test detail",
              "email": "alice@example.test"
            }
          }
        }
        "#);

        let list_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query {
                        alertSubscriptions(by: {prefix: "aliceCo/"}) {
                        catalogPrefix
                        email
                        destination
                        detail
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(list_response, @r#"
        {
          "data": {
            "alertSubscriptions": [
              {
                "catalogPrefix": "aliceCo/nested/",
                "destination": "mailto:different@example.test",
                "detail": "new detail",
                "email": "different@example.test"
              }
            ]
          }
        }
        "#);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_alert_subscription_errors(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.test"),
        );

        // Unauthorized prefixes
        let list_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"query {
                        alertSubscriptions(by: {prefix: "notAliceAtAll/"}) {
                            catalogPrefix
                            email
                            destination
                            detail
                        }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(list_response, @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 25,
                  "line": 2
                }
              ],
              "message": "PermissionDenied: alice@example.test is not authorized to access prefix or name 'notAliceAtAll/' with required capability admin",
              "path": [
                "alertSubscriptions"
              ]
            }
          ]
        }
        "#);

        let create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                mutation {
                    createAlertSubscription(
                        prefix: "notAliceAtAll/"
                        email: "alice@example.test"
                    ) {
                        alertTypes
                    }
                }
            "#}),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(create_response, @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 21,
                  "line": 3
                }
              ],
              "message": "PermissionDenied: alice@example.test is not authorized to access prefix or name 'notAliceAtAll/' with required capability admin",
              "path": [
                "createAlertSubscription"
              ]
            }
          ]
        }
        "#);

        // Create a subscription so that we can test updating it
        let _create_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
             mutation {
               createAlertSubscription(
                 prefix: "aliceCo/"
                 email: "alice@example.test"
                 alertTypes: ["shard_failed","auto_discover_failed"]
               ) {
               alertTypes
               }
            }"#
                }),
                Some(&token),
            )
            .await;

        // Send an update that omits both alertTypes and detail, and expect an error
        let update_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
             mutation {
               updateAlertSubscription(
                 prefix: "aliceCo/"
                 email: "alice@example.test"
               ) {
               alertTypes
               }
            }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(update_response, @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 16,
                  "line": 3
                }
              ],
              "message": "must provide at least one of: alertTypes, detail",
              "path": [
                "updateAlertSubscription"
              ]
            }
          ]
        }
        "#);

        // Update when prefix does not exist
        let update_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
             mutation {
               updateAlertSubscription(
                 prefix: "aliceCo/does/not/exist/"
                 email: "alice@example.test"
                 alertTypes: ["shard_failed"]
               ) {
               alertTypes
               }
            }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(update_response, @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 16,
                  "line": 3
                }
              ],
              "message": "no alert subscription exists for prefix 'aliceCo/does/not/exist/' and email 'alice@example.test'",
              "path": [
                "updateAlertSubscription"
              ]
            }
          ]
        }
        "#);

        // Update when email does not exist
        let update_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
             mutation {
               updateAlertSubscription(
                 prefix: "aliceCo/"
                 email: "not-alice-at-all@example.test"
                 alertTypes: ["shard_failed"]
               ) {
               alertTypes
               }
            }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(update_response, @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 16,
                  "line": 3
                }
              ],
              "message": "no alert subscription exists for prefix 'aliceCo/' and email 'not-alice-at-all@example.test'",
              "path": [
                "updateAlertSubscription"
              ]
            }
          ]
        }
        "#);

        // Delete when the prefix doesn't exist
        let delete_response: serde_json::Value = server
            .graphql(
                &serde_json::json!({
                    "query": r#"
                     mutation {
                       deleteAlertSubscription(
                         prefix: "aliceCo/does/not/exist/"
                         email: "alice@example.test"
                       ) {
                       catalogPrefix
                       email
                       }
                    }"#
                }),
                Some(&token),
            )
            .await;
        insta::assert_json_snapshot!(delete_response, @r#"
        {
          "data": null,
          "errors": [
            {
              "locations": [
                {
                  "column": 24,
                  "line": 3
                }
              ],
              "message": "no alert subscription exists for prefix 'aliceCo/does/not/exist/' and email 'alice@example.test'",
              "path": [
                "deleteAlertSubscription"
              ]
            }
          ]
        }
        "#);
    }
}
