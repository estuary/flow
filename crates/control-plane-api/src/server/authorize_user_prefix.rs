use super::{App, Snapshot};
use crate::server::error::ApiErrorExt;
use anyhow::Context;
use axum::http::StatusCode;
use std::sync::Arc;

type Request = models::authorizations::UserPrefixAuthorizationRequest;
type Response = models::authorizations::UserPrefixAuthorization;

#[axum::debug_handler]
#[tracing::instrument(
    skip(app),
    err(level = tracing::Level::WARN),
)]
pub async fn authorize_user_prefix(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(super::ControlClaims {
        sub: user_id,
        email,
        ..
    }): axum::Extension<super::ControlClaims>,
    super::Request(Request {
        prefix,
        data_plane,
        capability,
        started_unix,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    do_authorize_user_prefix(
        &app.snapshot,
        user_id,
        email,
        prefix,
        data_plane,
        capability,
        started_unix,
    )
    .await
}

pub async fn do_authorize_user_prefix(
    snapshot: &std::sync::RwLock<Snapshot>,
    user_id: uuid::Uuid,
    email: Option<String>,
    prefix: models::Prefix,
    data_plane: models::Name,
    capability: models::Capability,
    started_unix: u64,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    let started = chrono::DateTime::from_timestamp(started_unix as i64, 0).unwrap_or_default();

    match Snapshot::evaluate(snapshot, started, |snapshot: &Snapshot| {
        evaluate_authorization(
            snapshot,
            user_id,
            email.as_ref(),
            &prefix,
            &data_plane,
            capability,
        )
    }) {
        Ok((
            exp,
            (encoding_key, mut broker_claims, broker_address, mut reactor_claims, reactor_address),
        )) => {
            broker_claims.inner.exp = exp.timestamp() as u64;
            broker_claims.inner.iat = started.timestamp() as u64;
            reactor_claims.inner.exp = exp.timestamp() as u64;
            reactor_claims.inner.iat = started.timestamp() as u64;

            let header = jsonwebtoken::Header::default();
            let broker_token = jsonwebtoken::encode(&header, &broker_claims, &encoding_key)
                .context("failed to encode authorized JWT")?;
            let reactor_token = jsonwebtoken::encode(&header, &reactor_claims, &encoding_key)
                .context("failed to encode authorized JWT")?;

            Ok(axum::Json(Response {
                broker_token,
                broker_address,
                reactor_token,
                reactor_address,
                retry_millis: 0,
            }))
        }
        Err(Ok(backoff)) => Ok(axum::Json(Response {
            retry_millis: backoff.as_millis() as u64,
            ..Default::default()
        })),
        Err(Err(err)) => Err(err),
    }
}

fn evaluate_authorization(
    snapshot: &Snapshot,
    user_id: uuid::Uuid,
    user_email: Option<&String>,
    prefix: &models::Prefix,
    data_plane_name: &models::Name,
    capability: models::Capability,
) -> Result<
    (
        Option<chrono::DateTime<chrono::Utc>>,
        (
            jsonwebtoken::EncodingKey,
            super::DataClaims, // Broker claims.
            String,            // Broker address.
            super::DataClaims, // Reactor claims.
            String,            // Reactor address.
        ),
    ),
    crate::server::error::ApiError,
> {
    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        user_id,
        prefix,
        capability,
    ) {
        return Err(anyhow::anyhow!(
            "{} is not authorized to {prefix} for {capability:?}",
            user_email.map(String::as_str).unwrap_or("user")
        )
        .with_status(StatusCode::FORBIDDEN));
    }

    // For admin capability, require that the user has a transitive role grant to estuary_support/
    if capability == models::Capability::Admin {
        let has_support_access = tables::UserGrant::is_authorized(
            &snapshot.role_grants,
            &snapshot.user_grants,
            user_id,
            "estuary_support/",
            models::Capability::Admin,
        );

        if !has_support_access {
            return Err(anyhow::anyhow!(
                "{} is not authorized to {prefix} for Admin capability (requires estuary_support/ grant)",
                user_email.map(String::as_str).unwrap_or("user")
            )
            .with_status(StatusCode::FORBIDDEN));
        }
    }

    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        user_id,
        &data_plane_name,
        models::Capability::Read,
    ) {
        return Err(anyhow::anyhow!(
            "{} is not authorized to {data_plane_name}",
            user_email.map(String::as_str).unwrap_or("user")
        )
        .with_status(StatusCode::FORBIDDEN));
    }

    let Some(data_plane) = snapshot.data_plane_by_catalog_name(&data_plane_name) else {
        return Err(anyhow::anyhow!("data-plane {data_plane_name} not found")
            .with_status(StatusCode::NOT_FOUND));
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        return Err(
            anyhow::anyhow!("data-plane {data_plane_name} has no configured HMAC keys")
                .with_status(StatusCode::INTERNAL_SERVER_ERROR),
        );
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)
        .context("invalid data-plane hmac key")?;

    let broker_claims = super::DataClaims {
        inner: proto_gazette::Claims {
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
        },
        prefixes: Vec::new(), // TODO(johnny): remove.
    };

    let reactor_claims = super::DataClaims {
        inner: proto_gazette::Claims {
            cap: super::map_capability_to_gazette(capability)
                | proto_flow::capability::NETWORK_PROXY,
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
        },
        prefixes: Vec::new(), // TODO(johnny): remove.
    };

    Ok((
        None, // This API does not enforce cordons.
        (
            encoding_key,
            broker_claims,
            super::maybe_rewrite_address(true, &data_plane.broker_address),
            reactor_claims,
            super::maybe_rewrite_address(true, &data_plane.reactor_address),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_success_one() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Read,
        )
        .await;

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

    #[tokio::test]
    async fn test_success_two() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("acmeCo/shared/stuff/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Read,
        )
        .await;

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

    #[tokio::test]
    async fn test_not_authorized_to_prefix() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("acmeCo/whoosh/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Write,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to acmeCo/whoosh/ for Write"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_authorized_to_data_plane() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/private/something"),
            models::Capability::Read,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to ops/dp/private/something"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_data_plane_not_found() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/public/plane-missing"),
            models::Capability::Read,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "data-plane ops/dp/public/plane-missing not found"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_capability_to_high() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("acmeCo/shared/stuff/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Write,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to acmeCo/shared/stuff/ for Write"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_bob_cannot_get_admin_even_with_admin_grant() {
        // bob@bob has admin capability on bobCo/tires/ but lacks estuary_support/
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Prefix::new("bobCo/tires/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Admin,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/tires/ for Admin capability (requires estuary_support/ grant)"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_admin_with_estuary_support_grant() {
        // alice@alice has estuary_support/ grant and can get admin capability
        let outcome = run(
            uuid::Uuid::from_bytes([64; 16]),
            Some("alice@alice".to_string()),
            models::Prefix::new("aliceCo/"),
            models::Name::new("ops/dp/public/plane-two"),
            models::Capability::Admin,
        )
        .await;

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

    async fn run(
        user_id: uuid::Uuid,
        email: Option<String>,
        prefix: models::Prefix,
        data_plane: models::Name,
        capability: models::Capability,
    ) -> Result<(String, proto_gazette::Claims, String, proto_gazette::Claims), crate::server::error::ApiError>
    {
        let taken = chrono::Utc::now();
        let snapshot = Snapshot::build_fixture(Some(taken));
        let snapshot = std::sync::RwLock::new(snapshot);

        let Response {
            broker_address,
            broker_token,
            reactor_address,
            reactor_token,
            retry_millis,
        } = do_authorize_user_prefix(
            &snapshot,
            user_id,
            email,
            prefix,
            data_plane,
            capability,
            taken.timestamp() as u64 - 1,
        )
        .await?
        .0;

        if retry_millis != 0 {
            return Err(anyhow::anyhow!("retry").into());
        }

        let mut broker_claims = jsonwebtoken::decode::<super::super::DataClaims>(
            &broker_token,
            &jsonwebtoken::DecodingKey::from_secret("key3".as_bytes()),
            &jsonwebtoken::Validation::default(),
        )
        .expect("failed to decode response token")
        .claims
        .inner;

        let mut reactor_claims = jsonwebtoken::decode::<super::super::DataClaims>(
            &reactor_token,
            &jsonwebtoken::DecodingKey::from_secret("key3".as_bytes()),
            &jsonwebtoken::Validation::default(),
        )
        .expect("failed to decode response token")
        .claims
        .inner;

        (broker_claims.iat, broker_claims.exp) = (0, 0);
        (reactor_claims.iat, reactor_claims.exp) = (0, 0);

        Ok((
            broker_address,
            broker_claims,
            reactor_address,
            reactor_claims,
        ))
    }
}
