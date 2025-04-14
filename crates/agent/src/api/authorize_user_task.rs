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
        task: task_name,
        started_unix,
    }): super::Request<Request>,
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
        match Snapshot::evaluate(&app.snapshot, started, |snapshot: &Snapshot| {
            evaluate_authorization(snapshot, user_id, email.as_ref(), &task_name)
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
        models::Capability::Read,
    ) {
        return Err(anyhow::anyhow!(
            "{} is not authorized to {task_name}",
            user_email.map(String::as_str).unwrap_or("user")
        )
        .with_status(StatusCode::FORBIDDEN));
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
            cap: proto_gazette::capability::LIST
                | proto_gazette::capability::READ
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
