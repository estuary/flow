use super::{App, Snapshot};
use crate::server::error::ApiErrorExt;
use anyhow::Context;
use axum::http::StatusCode;
use std::sync::Arc;

type Request = models::authorizations::UserCollectionAuthorizationRequest;
type Response = models::authorizations::UserCollectionAuthorization;

#[axum::debug_handler]
#[tracing::instrument(
    skip(app),
    err(level = tracing::Level::WARN),
)]
pub async fn authorize_user_collection(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(super::ControlClaims {
        sub: user_id,
        email,
        ..
    }): axum::Extension<super::ControlClaims>,
    super::Request(Request {
        collection,
        capability,
        started_unix,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    do_authorize_user_collection(
        &app.snapshot,
        user_id,
        email,
        collection,
        capability,
        started_unix,
    )
    .await
}

pub async fn do_authorize_user_collection(
    snapshot: &std::sync::RwLock<Snapshot>,
    user_id: uuid::Uuid,
    email: Option<String>,
    collection: models::Collection,
    capability: models::Capability,
    started_unix: u64,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    let (has_started, started) = if started_unix == 0 {
        (false, chrono::Utc::now())
    } else {
        (
            true,
            chrono::DateTime::from_timestamp(started_unix as i64, 0).unwrap_or_default(),
        )
    };

    loop {
        match Snapshot::evaluate(snapshot, started, |snapshot: &Snapshot| {
            evaluate_authorization(snapshot, user_id, email.as_ref(), &collection, capability)
        }) {
            Ok((exp, (encoding_key, mut claims, broker_address, journal_name_prefix))) => {
                claims.inner.iat = started.timestamp() as u64;
                claims.inner.exp = exp.timestamp() as u64;

                let broker_token =
                    jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, &encoding_key)
                        .context("failed to encode authorized JWT")?;

                return Ok(axum::Json(Response {
                    broker_address,
                    broker_token,
                    journal_name_prefix,
                    retry_millis: 0,
                }));
            }
            Err(Ok(backoff)) if has_started => {
                return Ok(axum::Json(Response {
                    retry_millis: backoff.as_millis() as u64,
                    ..Default::default()
                }))
            }
            Err(Ok(backoff)) => {
                () = tokio::time::sleep(backoff).await;
            }
            Err(Err(err)) => return Err(err),
        }
    }
}

fn evaluate_authorization(
    snapshot: &Snapshot,
    user_id: uuid::Uuid,
    user_email: Option<&String>,
    collection_name: &models::Collection,
    capability: models::Capability,
) -> Result<
    (
        Option<chrono::DateTime<chrono::Utc>>,
        (jsonwebtoken::EncodingKey, super::DataClaims, String, String),
    ),
    crate::server::error::ApiError,
> {
    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        user_id,
        collection_name,
        capability,
    ) {
        return Err(anyhow::anyhow!(
            "{} is not authorized to {collection_name} for {capability:?}",
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
                "{} is not authorized to {collection_name} for Admin capability (requires estuary_support/ grant)",
                user_email.map(String::as_str).unwrap_or("user")
            )
            .with_status(StatusCode::FORBIDDEN));
        }
    }

    let Some(collection) = snapshot.collection_by_catalog_name(collection_name) else {
        return Err(anyhow::anyhow!("collection {collection_name} is not known")
            .with_status(StatusCode::NOT_FOUND));
    };
    let Some(data_plane) = snapshot.data_planes.get_by_key(&collection.data_plane_id) else {
        return Err(anyhow::anyhow!(
            "collection data-plane {} not found",
            collection.data_plane_id
        )
        .with_status(StatusCode::INTERNAL_SERVER_ERROR));
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        return Err(anyhow::anyhow!(
            "collection data-plane {} has no configured HMAC keys",
            data_plane.data_plane_name
        )
        .with_status(StatusCode::INTERNAL_SERVER_ERROR));
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)
        .context("invalid data-plane hmac key")?;

    let claims = super::DataClaims {
        inner: proto_gazette::Claims {
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
        },
        // TODO(johnny): Temporary support for data-plane-gateway.
        prefixes: vec![
            collection_name.to_string(),
            collection.journal_template_name.clone(),
        ],
    };

    Ok((
        snapshot.cordon_at(&collection.collection_name, data_plane),
        (
            encoding_key,
            claims,
            super::maybe_rewrite_address(true, &data_plane.broker_address),
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
    async fn test_capability_to_high() {
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
          "Err": {
            "status": 500,
            "error": "retry"
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

    async fn run(
        user_id: uuid::Uuid,
        email: Option<String>,
        collection: models::Collection,
        capability: models::Capability,
    ) -> Result<(String, String, proto_gazette::Claims), crate::server::error::ApiError> {
        let taken = chrono::Utc::now();
        let snapshot = Snapshot::build_fixture(Some(taken));
        let snapshot = std::sync::RwLock::new(snapshot);

        let Response {
            broker_address,
            broker_token,
            journal_name_prefix,
            retry_millis,
        } = do_authorize_user_collection(
            &snapshot,
            user_id,
            email,
            collection,
            capability,
            taken.timestamp() as u64 - 1,
        )
        .await?
        .0;

        if retry_millis != 0 {
            return Err(anyhow::anyhow!("retry").into());
        }

        // Decode and verify the response token.
        let mut decoded = jsonwebtoken::decode::<super::super::DataClaims>(
            &broker_token,
            &jsonwebtoken::DecodingKey::from_secret("key3".as_bytes()),
            &jsonwebtoken::Validation::default(),
        )
        .expect("failed to decode response token")
        .claims
        .inner;

        (decoded.iat, decoded.exp) = (0, 0);

        Ok((broker_address, journal_name_prefix, decoded))
    }
}
