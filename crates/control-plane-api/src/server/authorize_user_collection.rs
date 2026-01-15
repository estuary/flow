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

    let policy_result =
        evaluate_authorization(env.snapshot(), env.claims()?, &collection, capability);

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
    snapshot: &crate::Snapshot,
    claims: &crate::ControlClaims,
    collection_name: &models::Collection,
    capability: models::Capability,
) -> crate::AuthZResult<(
    tokens::jwt::EncodingKey,
    proto_gazette::Claims,
    String,
    String,
)> {
    let models::authorizations::ControlClaims {
        sub: user_id,
        email: user_email,
        ..
    } = claims;
    let user_email = user_email.as_ref().map(String::as_str).unwrap_or("user");

    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        *user_id,
        collection_name,
        capability,
    ) {
        return Err(tonic::Status::permission_denied(format!(
            "{user_email} is not authorized to {collection_name} for {capability:?}",
        )));
    }

    // For admin capability, require that the user has a transitive role grant to estuary_support/
    if capability == models::Capability::Admin {
        let has_support_access = tables::UserGrant::is_authorized(
            &snapshot.role_grants,
            &snapshot.user_grants,
            *user_id,
            "estuary_support/",
            models::Capability::Admin,
        );

        if !has_support_access {
            return Err(tonic::Status::permission_denied(format!(
                "{user_email} is not authorized to {collection_name} for Admin capability (requires estuary_support/ grant)",
            )));
        }
    }

    let Some(collection) = snapshot.collection_by_catalog_name(collection_name) else {
        return Err(tonic::Status::not_found(format!(
            "collection {collection_name} is not known"
        )));
    };
    let Some(data_plane) = snapshot.data_planes.get_by_key(&collection.data_plane_id) else {
        return Err(tonic::Status::internal(format!(
            "collection data-plane {} not found",
            collection.data_plane_id
        )));
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        return Err(tonic::Status::internal(format!(
            "collection data-plane {} has no configured HMAC keys",
            data_plane.data_plane_name
        )));
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

    #[test]
    fn test_success() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/anvils/peaches"),
            models::Capability::Write,
        );

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

    #[test]
    fn test_not_authorized() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("acmeCo/other/thing"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to acmeCo/other/thing for Read"
          }
        }
        "###);
    }

    #[test]
    fn test_capability_too_high() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/anvils/peaches"),
            models::Capability::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/anvils/peaches for Admin"
          }
        }
        "###);
    }

    #[test]
    fn test_not_found() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/widgets/not/found"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "collection bobCo/widgets/not/found is not known"
          }
        }
        "###);
    }

    #[test]
    fn test_cordon() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/widgets/squashes"),
            models::Capability::Read,
        );

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

    #[test]
    fn test_bob_cannot_get_admin_even_with_admin_grant() {
        // bob@bob has admin capability on bobCo/tires/ but lacks estuary_support/
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Collection::new("bobCo/tires/collection"),
            models::Capability::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/tires/collection for Admin capability (requires estuary_support/ grant)"
          }
        }
        "###);
    }

    #[test]
    fn test_admin_with_estuary_support_grant() {
        // alice@alice has estuary_support/ grant in the fixture, so admin should succeed
        let outcome = run(
            uuid::Uuid::from_bytes([64; 16]),
            Some("alice@alice".to_string()),
            models::Collection::new("aliceCo/wonderland/data"),
            models::Capability::Admin,
        );

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

    fn run(
        user_id: uuid::Uuid,
        email: Option<String>,
        collection: models::Collection,
        capability: models::Capability,
    ) -> Outcome {
        let snapshot = crate::Snapshot::build_fixture(None);
        let claims = models::authorizations::ControlClaims {
            aud: "authenticated".to_string(),
            iat: 0,
            exp: 0,
            sub: user_id,
            role: "authenticated".to_string(),
            email,
        };

        match evaluate_authorization(&snapshot, &claims, &collection, capability) {
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
            Err(status) => Outcome::Err {
                status: tokens::rest::grpc_status_code_to_http(status.code()),
                error: status.message().to_string(),
            },
        }
    }
}
