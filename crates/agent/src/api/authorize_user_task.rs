use super::{App, Snapshot};
use anyhow::Context;
use std::sync::Arc;

type Request = models::authorizations::UserTaskAuthorizationRequest;
type Response = models::authorizations::UserTaskAuthorization;

#[tracing::instrument(
    skip(snapshot),
    err(level = tracing::Level::WARN),
)]
async fn do_authorize_user_task(
    App { snapshot, .. }: &App,
    super::ControlClaims {
        sub: user_id,
        email,
        ..
    }: super::ControlClaims,
    Request {
        task: task_name,
        started_unix,
    }: Request,
) -> anyhow::Result<Response> {
    let (has_started, started_unix) = if started_unix == 0 {
        (false, jsonwebtoken::get_current_timestamp())
    } else {
        (true, started_unix)
    };

    loop {
        match Snapshot::evaluate(snapshot, started_unix, |snapshot: &Snapshot| {
            evaluate_authorization(snapshot, user_id, email.as_ref(), &task_name)
        }) {
            Ok(response) => return Ok(response),
            Err(Ok(retry_millis)) if has_started => {
                return Ok(Response {
                    retry_millis,
                    ..Default::default()
                })
            }
            Err(Ok(retry_millis)) => {
                () = tokio::time::sleep(std::time::Duration::from_millis(retry_millis)).await;
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
) -> anyhow::Result<Response> {
    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        user_id,
        task_name,
        models::Capability::Read,
    ) {
        anyhow::bail!(
            "{} is not authorized to {task_name}",
            user_email.map(String::as_str).unwrap_or("user")
        );
    }

    let Some(task) = snapshot.task_by_catalog_name(task_name) else {
        anyhow::bail!("task {task_name} is not known")
    };
    let Some(data_plane) = snapshot.data_planes.get_by_key(&task.data_plane_id) else {
        anyhow::bail!("couldn't resolve task {task_name} data-plane")
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        anyhow::bail!(
            "task data-plane {} has no configured HMAC keys",
            data_plane.data_plane_name
        );
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)?;

    let (Some(ops_logs), Some(ops_stats)) = (
        snapshot.collection_by_catalog_name(&data_plane.ops_logs_name),
        snapshot.collection_by_catalog_name(&data_plane.ops_stats_name),
    ) else {
        anyhow::bail!(
            "couldn't resolve data-plane {} ops collections",
            task.data_plane_id
        )
    };

    let ops_suffix = super::ops_suffix(task);
    let ops_logs_journal = format!("{}{}", ops_logs.journal_template_name, &ops_suffix[1..]);
    let ops_stats_journal = format!("{}{}", ops_stats.journal_template_name, &ops_suffix[1..]);

    let iat = jsonwebtoken::get_current_timestamp();
    let exp = iat + super::exp_seconds();
    let header = jsonwebtoken::Header::default();

    let claims = super::DataClaims {
        inner: proto_gazette::Claims {
            cap: proto_gazette::capability::LIST | proto_gazette::capability::READ,
            exp,
            iat,
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
    let broker_token = jsonwebtoken::encode(&header, &claims, &encoding_key)
        .context("failed to encode authorized JWT")?;

    let claims = super::DataClaims {
        inner: proto_gazette::Claims {
            cap: proto_gazette::capability::LIST
                | proto_gazette::capability::READ
                | proto_flow::capability::NETWORK_PROXY,
            exp,
            iat,
            iss: claims.inner.iss,
            sub: claims.inner.sub,
            sel: proto_gazette::broker::LabelSelector {
                include: Some(labels::build_set([(
                    "id:prefix",
                    task.shard_template_id.as_str(),
                )])),
                exclude: None,
            },
        },
        prefixes: vec![task.task_name.to_string()],
    };
    let reactor_token = jsonwebtoken::encode(&header, &claims, &encoding_key)
        .context("failed to encode authorized JWT")?;

    Ok(Response {
        broker_address: super::maybe_rewrite_address(true, &data_plane.broker_address),
        broker_token,
        ops_logs_journal,
        ops_stats_journal,
        reactor_address: super::maybe_rewrite_address(true, &data_plane.reactor_address),
        reactor_token,
        retry_millis: 0,
        shard_id_prefix: task.shard_template_id.clone(),
    })
}

#[axum::debug_handler]
pub async fn authorize_user_task(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Extension(claims): axum::Extension<super::ControlClaims>,
    super::Request(request): super::Request<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_authorize_user_task(&app, claims, request).await }).await
}
