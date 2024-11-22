use super::App;
use crate::api::snapshot::Snapshot;
use anyhow::Context;
use std::sync::Arc;

type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::RoleAuthorization;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AccessTokenClaims {
    exp: u64,
    iat: u64,
    role: String,
}

#[axum::debug_handler]
pub async fn authorize_role(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Path(role): axum::extract::Path<models::authorizations::AllowedRole>,
    axum::Json(request): axum::Json<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_authorize_role(&app, role, &request).await }).await
}

/// Provides a way for data-plane components to get access to the control-plane.
///
/// Specifically, this checks that:
///     * Your request is coming from an authorized actor in a data-plane,
///     * That actor is acting on behalf of a task running in that same data-plane.
/// It shares most of its logic with `/authorize/task`, except that instead of
/// authorizing access to a particular collection via a task, it simply validates
/// that the specified task is running in the same data-plane as the requesting actor,
/// and then returns a control-plane access token authorized to act as the requested role.
///
/// NOTE: Instead of authorizing access to a role which then has access to particular
/// tables in the control plane, ideally this endpoint would return a signed token
/// that includes the specific catalog name it's authorized for. This would require control-plane
/// support for catalog-prefix-scoped tokens which we don't have at the moment, so instead we use roles.
#[tracing::instrument(skip(app), err(level = tracing::Level::WARN))]
async fn do_authorize_role(
    app: &App,
    role: models::authorizations::AllowedRole,
    Request { token }: &Request,
) -> anyhow::Result<Response> {
    let jsonwebtoken::TokenData { header, claims }: jsonwebtoken::TokenData<proto_gazette::Claims> =
        {
            // In this pass we do not validate the signature,
            // because we don't yet know which data-plane the JWT is signed by.
            let empty_key = jsonwebtoken::DecodingKey::from_secret(&[]);
            let mut validation = jsonwebtoken::Validation::default();
            validation.insecure_disable_signature_validation();
            jsonwebtoken::decode(token, &empty_key, &validation)
        }?;
    tracing::debug!(?claims, ?header, "decoded authorization request");

    let shard_id = claims.sub.as_str();
    if shard_id.is_empty() {
        anyhow::bail!("missing required shard ID (`sub` claim)");
    }

    let shard_data_plane_fqdn = claims.iss.as_str();
    if shard_data_plane_fqdn.is_empty() {
        anyhow::bail!("missing required shard data-plane FQDN (`iss` claim)");
    }

    if claims.cap & proto_flow::capability::AUTHORIZE == 0 {
        anyhow::bail!("missing required AUTHORIZE capability: {}", claims.cap);
    }

    match Snapshot::evaluate(&app.snapshot, claims.iat, |snapshot: &Snapshot| {
        evaluate_authorization(snapshot, shard_id, shard_data_plane_fqdn, token)
    }) {
        Ok(()) => {
            let unix_ts = jsonwebtoken::get_current_timestamp();
            let claims = AccessTokenClaims {
                iat: unix_ts,
                exp: unix_ts + (60 * 60),
                role: role.to_string(),
            };

            let signed = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &claims,
                &app.control_plane_jwt_signer,
            )?;

            Ok(Response {
                token: signed,
                retry_millis: 0,
            })
        }
        Err(Ok(retry_millis)) => Ok(Response {
            retry_millis,
            ..Default::default()
        }),
        Err(Err(err)) => Err(err),
    }
}

fn evaluate_authorization(
    snapshot: &Snapshot,
    shard_id: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
) -> anyhow::Result<()> {
    let Snapshot {
        data_planes, tasks, ..
    } = snapshot;
    // Map `claims.sub`, a Shard ID, into its task.
    let task = tasks
        .binary_search_by(|task| {
            if shard_id.starts_with(&task.shard_template_id) {
                std::cmp::Ordering::Equal
            } else {
                task.shard_template_id.as_str().cmp(shard_id)
            }
        })
        .ok()
        .map(|index| &tasks[index]);

    // Map `claims.iss`, a data-plane FQDN, into its task-matched data-plane.
    let task_data_plane = task.and_then(|task| {
        data_planes
            .get_by_key(&task.data_plane_id)
            .filter(|data_plane| data_plane.data_plane_fqdn == shard_data_plane_fqdn)
    });

    let (Some(_), Some(task_data_plane)) = (task, task_data_plane) else {
        tracing::debug!(
            ?task,
            ?task_data_plane,
            ?tasks,
            "failed to find matching task in data plane"
        );
        anyhow::bail!(
            "task shard {shard_id} within data-plane {shard_data_plane_fqdn} is not known"
        )
    };

    // Attempt to find an HMAC key of this data-plane which validates against the request token.
    let validation = jsonwebtoken::Validation::default();
    let mut verified = false;

    for hmac_key in &task_data_plane.hmac_keys {
        let key = jsonwebtoken::DecodingKey::from_base64_secret(hmac_key)
            .context("invalid data-plane hmac key")?;

        if jsonwebtoken::decode::<proto_gazette::Claims>(token, &key, &validation).is_ok() {
            verified = true;
            break;
        }
    }
    if !verified {
        anyhow::bail!("no data-plane keys validated against the token signature");
    }

    Ok(())
}
