use super::{App, Snapshot};
use crate::api::error::ApiErrorExt;
use anyhow::Context;
use axum::http::StatusCode;
use std::sync::Arc;

type Request = models::authorizations::UserTaskAuthorizationRequest;
type Response = models::authorizations::UserTaskAuthorization;

#[axum::debug_handler]
#[tracing::instrument(
    skip(app),
    err(level = tracing::Level::WARN),
)]
pub async fn authorize_user_task(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(super::ControlClaims {
        sub: user_id,
        email,
        ..
    }): axum::Extension<super::ControlClaims>,
    super::Request(Request {
        task,
        capability,
        started_unix,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::api::ApiError> {
    do_authorize_user_task(
        &app.snapshot,
        user_id,
        email,
        task,
        capability,
        started_unix,
    )
    .await
}

pub async fn do_authorize_user_task(
    snapshot: &std::sync::RwLock<Snapshot>,
    user_id: uuid::Uuid,
    email: Option<String>,
    task: models::Name,
    capability: models::Capability,
    started_unix: u64,
) -> Result<axum::Json<Response>, crate::api::ApiError> {
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
            evaluate_authorization(snapshot, user_id, email.as_ref(), &task, capability)
        }) {
            Ok((
                exp,
                (
                    encoding_key,
                    mut broker_claims,
                    broker_address,
                    ops_logs_journal,
                    ops_stats_journal,
                    mut reactor_claims,
                    reactor_address,
                    shard_id_prefix,
                ),
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

                return Ok(axum::Json(Response {
                    broker_token,
                    broker_address,
                    reactor_token,
                    ops_logs_journal,
                    ops_stats_journal,
                    reactor_address,
                    shard_id_prefix,
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
    task_name: &models::Name,
    capability: models::Capability,
) -> Result<
    (
        Option<chrono::DateTime<chrono::Utc>>,
        (
            jsonwebtoken::EncodingKey,
            super::DataClaims, // Broker claims.
            String,            // Broker address.
            String,            // ops logs journal
            String,            // opts stats journal
            super::DataClaims, // Reactor claims.
            String,            // Reactor address.
            String,            // Shard ID prefix.
        ),
    ),
    crate::api::ApiError,
> {
    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        user_id,
        task_name,
        capability,
    ) {
        return Err(anyhow::anyhow!(
            "{} is not authorized to {task_name} for {capability:?}",
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
                "{} is not authorized to {task_name} for Admin capability (requires estuary_support/ grant)",
                user_email.map(String::as_str).unwrap_or("user")
            )
            .with_status(StatusCode::FORBIDDEN));
        }
    }

    let Some(task) = snapshot.task_by_catalog_name(task_name) else {
        return Err(
            anyhow::anyhow!("task {task_name} is not known").with_status(StatusCode::NOT_FOUND)
        );
    };
    let Some(data_plane) = snapshot.data_planes.get_by_key(&task.data_plane_id) else {
        return Err(
            anyhow::anyhow!("task data-plane {} not found", task.data_plane_id)
                .with_status(StatusCode::INTERNAL_SERVER_ERROR),
        );
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        return Err(anyhow::anyhow!(
            "task data-plane {} has no configured HMAC keys",
            data_plane.data_plane_name
        )
        .with_status(StatusCode::INTERNAL_SERVER_ERROR));
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)
        .context("invalid data-plane hmac key")?;

    let (Some(ops_logs), Some(ops_stats)) = (
        snapshot.collection_by_catalog_name(&data_plane.ops_logs_name),
        snapshot.collection_by_catalog_name(&data_plane.ops_stats_name),
    ) else {
        return Err(anyhow::anyhow!(
            "couldn't resolve data-plane {} ops collections",
            task.data_plane_id
        )
        .with_status(StatusCode::INTERNAL_SERVER_ERROR));
    };

    let ops_suffix = super::ops_suffix(task);
    let ops_logs_journal = format!("{}{}", ops_logs.journal_template_name, &ops_suffix[1..]);
    let ops_stats_journal = format!("{}{}", ops_stats.journal_template_name, &ops_suffix[1..]);

    let broker_claims = super::DataClaims {
        inner: proto_gazette::Claims {
            cap: proto_gazette::capability::LIST | proto_gazette::capability::READ,
            exp: 0, // Filled later.
            iat: 0, // Filled later.
            iss: data_plane.data_plane_fqdn.clone(),
            sub: user_id.to_string(),
            sel: proto_gazette::broker::LabelSelector {
                include: Some(labels::build_set([
                    ("name", ops_logs_journal.as_str()),
                    ("name", ops_stats_journal.as_str()),
                ])),
                exclude: None,
            },
        },
        // TODO(johnny): Temporary support for data-plane-gateway.
        prefixes: vec![ops_logs_journal.clone(), ops_stats_journal.clone()],
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
                include: Some(labels::build_set([(
                    "id:prefix",
                    task.shard_template_id.as_str(),
                )])),
                exclude: None,
            },
        },
        prefixes: vec![task.task_name.to_string(), task.shard_template_id.clone()],
    };

    Ok((
        snapshot.cordon_at(&task.task_name, data_plane),
        (
            encoding_key,
            broker_claims,
            super::maybe_rewrite_address(true, &data_plane.broker_address),
            ops_logs_journal,
            ops_stats_journal,
            reactor_claims,
            super::maybe_rewrite_address(true, &data_plane.reactor_address),
            task.shard_template_id.clone(),
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
            models::Name::new("bobCo/anvils/materialize-orange"),
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
                      "value": "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00"
                    },
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00"
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
                      "value": "materialization/bobCo/anvils/materialize-orange/0011223344556677/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            },
            "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            "materialization/bobCo/anvils/materialize-orange/0011223344556677/"
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_authorized() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("acmeCo/other/thing"),
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
            models::Name::new("bobCo/anvils/materialize-orange"),
            models::Capability::Admin,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/anvils/materialize-orange for Admin"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_found() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/widgets/not/found"),
            models::Capability::Read,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "task bobCo/widgets/not/found is not known"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_cordon() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/widgets/materialize-mango"),
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
        // We need to add a task under bobCo/tires/ to the fixture for this test
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/tires/materialize-wheels"),
            models::Capability::Admin,
        )
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/tires/materialize-wheels for Admin capability (requires estuary_support/ grant)"
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
            models::Name::new("aliceCo/wonderland/materialize-tea"),
            models::Capability::Admin,
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
                      "value": "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00"
                    },
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00"
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
                      "value": "materialization/aliceCo/wonderland/materialize-tea/0011223344556677/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "40404040-4040-4040-4040-404040404040"
            },
            "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00",
            "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00",
            "materialization/aliceCo/wonderland/materialize-tea/0011223344556677/"
          ]
        }
        "###);
    }

    async fn run(
        user_id: uuid::Uuid,
        email: Option<String>,
        task: models::Name,
        capability: models::Capability,
    ) -> Result<
        (
            String,
            proto_gazette::Claims,
            String,
            proto_gazette::Claims,
            String,
            String,
            String,
        ),
        crate::api::ApiError,
    > {
        let taken = chrono::Utc::now();
        let snapshot = Snapshot::build_fixture(Some(taken));
        let snapshot = std::sync::RwLock::new(snapshot);

        let Response {
            broker_address,
            broker_token,
            reactor_address,
            reactor_token,
            ops_logs_journal,
            ops_stats_journal,
            shard_id_prefix,
            retry_millis,
        } = do_authorize_user_task(
            &snapshot,
            user_id,
            email,
            task,
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
            ops_logs_journal,
            ops_stats_journal,
            shard_id_prefix,
        ))
    }
}
