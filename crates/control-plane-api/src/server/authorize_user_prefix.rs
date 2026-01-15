type Request = models::authorizations::UserPrefixAuthorizationRequest;
type Response = models::authorizations::UserPrefixAuthorization;

#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_user_prefix(
    mut env: crate::Envelope,
    super::Request(Request {
        prefix,
        data_plane,
        capability,
        started_unix,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    // Legacy: if `started_unix` is set, then use as the logical request start
    // rounded up to the next second (as it was round down when encoded).
    if started_unix != 0 {
        env.started =
            tokens::DateTime::from_timestamp_secs(1 + started_unix as i64).unwrap_or_default();
    }

    let policy_result = evaluate_authorization(
        env.snapshot(),
        env.claims()?,
        &prefix,
        &data_plane,
        capability,
    );

    // Legacy: if `started_unix` was set then use a custom 200 response for client-side retries.
    let (
        expiry,
        (encoding_key, mut broker_claims, broker_address, mut reactor_claims, reactor_address),
    ) = match env.authorization_outcome(policy_result).await {
        Ok(ok) => ok,
        Err(crate::ApiError::AuthZRetry(retry)) if started_unix != 0 => {
            return Ok(axum::Json(Response {
                retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                ..Default::default()
            }));
        }
        Err(err) => return Err(err),
    };

    broker_claims.exp = expiry.timestamp() as u64;
    broker_claims.iat = env.started.timestamp() as u64;
    reactor_claims.exp = expiry.timestamp() as u64;
    reactor_claims.iat = env.started.timestamp() as u64;

    let broker_token = tokens::jwt::sign(&broker_claims, &encoding_key)?;
    let reactor_token = tokens::jwt::sign(&reactor_claims, &encoding_key)?;

    Ok(axum::Json(Response {
        broker_token,
        broker_address,
        reactor_token,
        reactor_address,
        retry_millis: 0,
    }))
}

fn evaluate_authorization(
    snapshot: &crate::Snapshot,
    claims: &crate::ControlClaims,
    prefix: &models::Prefix,
    data_plane_name: &models::Name,
    capability: models::Capability,
) -> tonic::Result<(
    Option<chrono::DateTime<chrono::Utc>>,
    (
        tokens::jwt::EncodingKey,
        proto_gazette::Claims, // Broker claims.
        String,                // Broker address.
        proto_gazette::Claims, // Reactor claims.
        String,                // Reactor address.
    ),
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
        prefix,
        capability,
    ) {
        return Err(tonic::Status::permission_denied(format!(
            "{user_email} is not authorized to {prefix} for {capability:?}",
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
                "{user_email} is not authorized to {prefix} for Admin capability (requires estuary_support/ grant)",
            )));
        }
    }

    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        *user_id,
        data_plane_name,
        models::Capability::Read,
    ) {
        return Err(tonic::Status::permission_denied(format!(
            "{user_email} is not authorized to {data_plane_name}",
        )));
    }

    let Some(data_plane) = snapshot.data_plane_by_catalog_name(data_plane_name) else {
        return Err(tonic::Status::not_found(format!(
            "data-plane {data_plane_name} not found"
        )));
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        return Err(tonic::Status::internal(format!(
            "data-plane {data_plane_name} has no configured HMAC keys"
        )));
    };
    let encoding_key =
        tokens::jwt::EncodingKey::from_secret(&tokens::jwt::parse_base64(encoding_key)?);

    let broker_claims = proto_gazette::Claims {
        cap: super::map_capability_to_gazette(capability),
        exp: 0, // Filled later.
        iat: 0, // Filled later.
        iss: data_plane.data_plane_fqdn.clone(),
        sub: user_id.to_string(),
        sel: proto_gazette::broker::LabelSelector {
            include: Some(labels::build_set([
                ("name:prefix", prefix.as_str()),
                ("name:prefix", &format!("recovery/capture/{prefix}")),
                ("name:prefix", &format!("recovery/derivation/{prefix}")),
                ("name:prefix", &format!("recovery/materialize/{prefix}")),
            ])),
            exclude: None,
        },
    };

    let reactor_claims = proto_gazette::Claims {
        cap: super::map_capability_to_gazette(capability) | proto_flow::capability::NETWORK_PROXY,
        exp: 0, // Filled later.
        iat: 0, // Filled later.
        iss: data_plane.data_plane_fqdn.clone(),
        sub: user_id.to_string(),
        sel: proto_gazette::broker::LabelSelector {
            include: Some(labels::build_set([
                ("id:prefix", format!("capture/{prefix}").as_str()),
                ("id:prefix", &format!("derivation/{prefix}")),
                ("id:prefix", &format!("materialize/{prefix}")),
            ])),
            exclude: None,
        },
    };

    Ok((
        None, // This API does not enforce cordons.
        (
            encoding_key,
            broker_claims,
            data_plane.broker_address.clone(),
            reactor_claims,
            data_plane.reactor_address.clone(),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success_one() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            {
              "cap": 10,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "bobCo/tires/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/capture/bobCo/tires/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/derivation/bobCo/tires/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/materialize/bobCo/tires/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            },
            "reactor.2",
            {
              "cap": 262154,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "id",
                      "value": "capture/bobCo/tires/",
                      "prefix": true
                    },
                    {
                      "name": "id",
                      "value": "derivation/bobCo/tires/",
                      "prefix": true
                    },
                    {
                      "name": "id",
                      "value": "materialize/bobCo/tires/",
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
    fn test_success_two() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("acmeCo/shared/stuff/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            {
              "cap": 10,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "acmeCo/shared/stuff/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/capture/acmeCo/shared/stuff/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/derivation/acmeCo/shared/stuff/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/materialize/acmeCo/shared/stuff/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            },
            "reactor.2",
            {
              "cap": 262154,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "id",
                      "value": "capture/acmeCo/shared/stuff/",
                      "prefix": true
                    },
                    {
                      "name": "id",
                      "value": "derivation/acmeCo/shared/stuff/",
                      "prefix": true
                    },
                    {
                      "name": "id",
                      "value": "materialize/acmeCo/shared/stuff/",
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
    fn test_not_authorized_to_prefix() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("acmeCo/whoosh/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Write,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to acmeCo/whoosh/ for Write"
          }
        }
        "###);
    }

    #[test]
    fn test_not_authorized_to_data_plane() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/private/something"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to ops/dp/private/something"
          }
        }
        "###);
    }

    #[test]
    fn test_data_plane_not_found() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/public/plane-missing"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "data-plane ops/dp/public/plane-missing not found"
          }
        }
        "###);
    }

    #[test]
    fn test_capability_too_high() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("acmeCo/shared/stuff/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Write,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to acmeCo/shared/stuff/ for Write"
          }
        }
        "###);
    }

    #[test]
    fn test_bob_cannot_get_admin_even_with_admin_grant() {
        // bob@bob has admin capability on bobCo/tires/ but lacks estuary_support/
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/tires/ for Admin capability (requires estuary_support/ grant)"
          }
        }
        "###);
    }

    #[test]
    fn test_admin_with_estuary_support_grant() {
        // alice@alice has estuary_support/ grant and can get admin capability
        let outcome = run(
            uuid::Uuid::from_bytes([64; 16]),
            Some("alice@alice".to_string()),
            models::Prefix::new("aliceCo/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            {
              "cap": 30,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "aliceCo/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/capture/aliceCo/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/derivation/aliceCo/",
                      "prefix": true
                    },
                    {
                      "name": "name",
                      "value": "recovery/materialize/aliceCo/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "40404040-4040-4040-4040-404040404040"
            },
            "reactor.2",
            {
              "cap": 262174,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "id",
                      "value": "capture/aliceCo/",
                      "prefix": true
                    },
                    {
                      "name": "id",
                      "value": "derivation/aliceCo/",
                      "prefix": true
                    },
                    {
                      "name": "id",
                      "value": "materialize/aliceCo/",
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

    #[derive(serde::Serialize)]
    enum Outcome {
        Ok((String, proto_gazette::Claims, String, proto_gazette::Claims)),
        Err { status: u16, error: String },
    }

    fn run(
        user_id: uuid::Uuid,
        email: Option<String>,
        prefix: models::Prefix,
        data_plane: models::Name,
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

        match evaluate_authorization(&snapshot, &claims, &prefix, &data_plane, capability) {
            Ok((
                _cordon_at,
                (_key, mut broker_claims, broker_address, mut reactor_claims, reactor_address),
            )) => {
                // Zero out timestamps for stable snapshots.
                broker_claims.iat = 0;
                broker_claims.exp = 0;
                reactor_claims.iat = 0;
                reactor_claims.exp = 0;

                Outcome::Ok((
                    broker_address,
                    broker_claims,
                    reactor_address,
                    reactor_claims,
                ))
            }
            Err(status) => Outcome::Err {
                status: tokens::rest::grpc_status_code_to_http(status.code()),
                error: status.message().to_string(),
            },
        }
    }
}
