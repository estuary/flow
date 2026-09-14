type Request = models::authorizations::UserCollectionAuthorizationRequest;
type Response = models::authorizations::UserCollectionAuthorization;

#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_user_collection(
    mut env: crate::Envelope,
    super::Request(Request {
        collection,
        capability,
        started_unix,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::ApiError> {
    // Legacy: if `started_unix` is set, then use as the logical request start
    // rounded up to the next second (as it was round down when encoded).
    if started_unix != 0 {
        env.started =
            tokens::DateTime::from_timestamp_secs(1 + started_unix as i64).unwrap_or_default();
    }

    let policy_result = evaluate_authorization(&env, &collection, capability);

    // Legacy: if `started_unix` was set then use a custom 200 response for client-side retries.
    let (expiry, (encoding_key, mut claims, broker_address, journal_name_prefix)) =
        match env.authorization_outcome(policy_result).await {
            Ok(ok) => ok,
            Err(crate::ApiError::AuthZRetry(retry)) if started_unix != 0 => {
                return Ok(axum::Json(Response {
                    retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                    ..Default::default()
                }));
            }
            Err(err) => return Err(err),
        };

    claims.iat = env.started.timestamp() as u64;
    claims.exp = expiry.timestamp() as u64;

    let broker_token = tokens::jwt::sign(&claims, &encoding_key)?;

    Ok(axum::Json(Response {
        broker_address,
        broker_token,
        journal_name_prefix,
        retry_millis: 0,
    }))
}

fn evaluate_authorization(
    env: &crate::Envelope,
    collection_name: &models::Collection,
    capability: models::Capability,
) -> crate::AuthZResult<(
    tokens::jwt::EncodingKey,
    proto_gazette::Claims,
    String,
    String,
)> {
    let (is_authorized, user_id, user_email) =
        env.user_is_authorized_for(collection_name.as_str(), capability)?;
    if !is_authorized {
        return Err(tonic::Status::permission_denied(format!(
            "{user_email} is not authorized to {collection_name} for {capability:?}",
        ))
        .into());
    }

    if !env.verify_estuary_support(user_id, capability) {
        return Err(tonic::Status::permission_denied(format!(
            "{user_email} is not authorized to {collection_name} for Admin capability (requires estuary_support/ grant)",
        )).into());
    }

    let snapshot = env.snapshot();
    let Some(collection) = snapshot.collection_by_catalog_name(collection_name) else {
        return Err(
            tonic::Status::not_found(format!("collection {collection_name} is not known")).into(),
        );
    };
    let Some(data_plane) = snapshot.data_planes.get_by_key(&collection.data_plane_id) else {
        return Err(tonic::Status::internal(format!(
            "collection data-plane {} not found",
            collection.data_plane_id
        ))
        .into());
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        return Err(tonic::Status::internal(format!(
            "collection data-plane {} has no configured HMAC keys",
            data_plane.data_plane_name
        ))
        .into());
    };
    let encoding_key =
        tokens::jwt::EncodingKey::from_secret(&tokens::jwt::parse_base64(encoding_key)?);

    let claims = proto_gazette::Claims {
        cap: super::map_capability_to_gazette(capability),
        exp: 0, // Filled later.
        iat: 0, // Filled later.
        iss: data_plane.data_plane_fqdn.clone(),
        sub: user_id.to_string(),
        sel: proto_gazette::broker::LabelSelector {
            include: Some(labels::build_set([
                ("name:prefix", collection.journal_template_name.as_str()),
                (labels::COLLECTION, collection_name.as_str()),
            ])),
            exclude: None,
        },
    };

    Ok((
        snapshot.cordon_at(&collection.collection_name, data_plane),
        (
            encoding_key,
            claims,
            data_plane.broker_address.clone(),
            collection.journal_template_name.clone(),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_success() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/anvils/peaches"),
            models::Capability::Write,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            "bobCo/anvils/peaches/1122334455667788/",
            {
              "cap": 26,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "estuary.dev/collection",
                      "value": "bobCo/anvils/peaches"
                    },
                    {
                      "name": "name",
                      "value": "bobCo/anvils/peaches/1122334455667788/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            }
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_authorized() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("acmeCo/other/thing"),
            models::Capability::Read,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to acmeCo/other/thing for Read"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_capability_too_high() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/anvils/peaches"),
            models::Capability::Admin,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/anvils/peaches for Admin"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_found() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/widgets/not/found"),
            models::Capability::Read,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "collection bobCo/widgets/not/found is not known"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_cordon() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/widgets/squashes"),
            models::Capability::Read,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok_Cordoned": [
            "broker.2",
            "bobCo/widgets/squashes/1122334455667788/",
            {
              "cap": 10,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "estuary.dev/collection",
                      "value": "bobCo/widgets/squashes"
                    },
                    {
                      "name": "name",
                      "value": "bobCo/widgets/squashes/1122334455667788/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            }
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn test_bob_cannot_get_admin_even_with_admin_grant() {
        // bob@bob has admin capability on bobCo/tires/ but lacks estuary_support/
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/tires/collection"),
            models::Capability::Admin,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/tires/collection for Admin capability (requires estuary_support/ grant)"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_admin_with_estuary_support_grant() {
        // alice@alice has estuary_support/ grant in the fixture, so admin should succeed
        let outcome = run(
            uuid::Uuid::from_bytes([64; 16]),
            Some("alice@alice".to_string()),
            models::Collection::new("aliceCo/wonderland/data"),
            models::Capability::Admin,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            "aliceCo/wonderland/data/1122334455667788/",
            {
              "cap": 30,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "estuary.dev/collection",
                      "value": "aliceCo/wonderland/data"
                    },
                    {
                      "name": "name",
                      "value": "aliceCo/wonderland/data/1122334455667788/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "40404040-4040-4040-4040-404040404040"
            }
          ]
        }
        "###);
    }

    // Serialization wrapper that distinguishes cordoned vs non-cordoned success.
    #[derive(serde::Serialize)]
    enum Outcome {
        Ok((String, String, proto_gazette::Claims)),
        #[serde(rename = "Ok_Cordoned")]
        OkCordoned((String, String, proto_gazette::Claims)),
        Err {
            status: u16,
            error: String,
        },
    }

    async fn run(
        user_id: uuid::Uuid,
        email: Option<String>,
        collection: models::Collection,
        capability: models::Capability,
    ) -> Outcome {
        // The policy is pure over the Snapshot and claims, so `started` is
        // irrelevant here: no outcome handling runs against it.
        let env = crate::test_server::envelope(
            crate::Snapshot::build_fixture(None),
            crate::test_server::verified_control_claims(user_id, email),
            tokens::DateTime::UNIX_EPOCH,
        )
        .await;

        match evaluate_authorization(&env, &collection, capability) {
            Ok((cordon_at, (_key, mut data_claims, broker_address, journal_name_prefix))) => {
                // Zero out timestamps for stable snapshots.
                data_claims.iat = 0;
                data_claims.exp = 0;

                if cordon_at.is_some() {
                    Outcome::OkCordoned((broker_address, journal_name_prefix, data_claims))
                } else {
                    Outcome::Ok((broker_address, journal_name_prefix, data_claims))
                }
            }
            Err(err) => {
                let (status, error) = err.into_status_message();
                Outcome::Err { status, error }
            }
        }
    }

    // Integration tests below use sqlx::test with actual database
    use crate::test_server;
    use flow_client_next as flow_client;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_user_collection_auth_success(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let user_tokens = server.make_fixed_user_tokens(uuid::Uuid::from_bytes([0x11; 16]), None);
        tokio::time::pause();

        let source = flow_client::workflows::UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: user_tokens.clone(),
            collection: models::Collection::new("aliceCo/data/foo"),
            capability: models::Capability::Write,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_json_snapshot!(
            refresh.token().result().unwrap(),
            {".brokerToken" => "<redacted>"},
            @r###"
            {
              "brokerAddress": "broker.dp.one",
              "brokerToken": "<redacted>",
              "journalNamePrefix": "aliceCo/data/foo/gen1234/",
              "retryMillis": 0
            }
            "###,
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_user_collection_auth_failure(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let user_tokens = server.make_fixed_user_tokens(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.com"),
        );
        tokio::time::pause();

        let source = flow_client::workflows::UserCollectionAuth {
            client: server.rest_client(),
            user_tokens: user_tokens.clone(),
            collection: models::Collection::new("Some/Other/Collection"),
            capability: models::Capability::Write,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_debug_snapshot!(
            refresh.token().result().unwrap_err(),
            @r#"
        Status {
            code: PermissionDenied,
            message: "alice@example.com is not authorized to Some/Other/Collection for Write",
            source: None,
        }
        "#,
        );
    }
}
