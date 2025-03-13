use super::{App, Snapshot};
use crate::api::error::ApiErrorExt;
use anyhow::Context;
use axum::http::StatusCode;
use std::sync::Arc;

type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::TaskAuthorization;

/// Authorizes some set of actions to be performed on a particular collection by way of a task.
/// This checks that:
///     * The request is `iss`ued by an actor in a particular data plane
///         * Validated by checking the request signature against the HMACs for the `iss`uer data-plane
///     * The request is on behalf of a `sub`ject task running in that data plane
///         * The subject task is identified by its `shard_template_id`, not just its name.
///     * The request is to perform some `cap`abilities on a particular collection
///         * The collection is identified by its `journal_template_name`, not just its name.
///         * The target collection is specified as a label selector for the label `name`
///     * The request's subject is granted those capabilities on that collection by
///       the control-plane
///     * The requested collection may be in a different data plane than the issuer.
#[axum::debug_handler]
#[tracing::instrument(skip(app), err(level = tracing::Level::WARN))]
pub async fn authorize_task(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Json(Request { token }): axum::Json<Request>,
) -> Result<axum::Json<Response>, crate::api::ApiError> {
    let jsonwebtoken::TokenData { header, mut claims }: jsonwebtoken::TokenData<
        proto_gazette::Claims,
    > = {
        // In this pass we do not validate the signature,
        // because we don't yet know which data-plane the JWT is signed by.
        let empty_key = jsonwebtoken::DecodingKey::from_secret(&[]);
        let mut validation = jsonwebtoken::Validation::default();
        validation.insecure_disable_signature_validation();
        jsonwebtoken::decode(&token, &empty_key, &validation)
            .map_err(|err| anyhow::anyhow!(err).with_status(StatusCode::BAD_REQUEST))?
    };
    tracing::debug!(?claims, ?header, "decoded authorization request");

    let shard_id = claims.sub.as_str();
    if shard_id.is_empty() {
        return Err(anyhow::anyhow!("missing required shard ID (`sub` claim)")
            .with_status(StatusCode::BAD_REQUEST));
    }

    let shard_data_plane_fqdn = claims.iss.as_str();
    if shard_data_plane_fqdn.is_empty() {
        return Err(
            anyhow::anyhow!("missing required shard data-plane FQDN (`iss` claim)")
                .with_status(StatusCode::BAD_REQUEST),
        );
    }

    let journal_name_or_prefix = labels::expect_one(claims.sel.include(), "name")
        .map_err(|err| anyhow::anyhow!(err).with_status(StatusCode::BAD_REQUEST))?
        .to_owned();

    // Require the request was signed with the AUTHORIZE capability,
    // and then strip this capability before issuing a response token.
    if claims.cap & proto_flow::capability::AUTHORIZE == 0 {
        return Err(
            anyhow::anyhow!("missing required AUTHORIZE capability: {}", claims.cap)
                .with_status(StatusCode::FORBIDDEN),
        );
    }
    claims.cap &= !proto_flow::capability::AUTHORIZE;

    // Validate and match the requested capabilities to a corresponding role.
    // NOTE: Because we pass through the claims after validating them here,
    // we need to explicitly enumerate and exactly match every case, as just
    // checking that the requested capability contains a particular grant isn't enough.
    // For example, we wouldn't want to allow a request for `REPLICATE` just
    // because it also requests `READ`.
    let required_role = match claims.cap {
        cap if (cap == proto_gazette::capability::LIST)
            || (cap == proto_gazette::capability::READ)
            || (cap == (proto_gazette::capability::LIST | proto_gazette::capability::READ)) =>
        {
            models::Capability::Read
        }
        // We're intentionally rejecting requests for both APPLY and APPEND, as those two
        // grants authorize wildly different capabilities, and no sane logic should
        // need both at the same time. So as a sanity check/defense-in-depth measure
        // we won't grant you a token that has both, even if we technically could.
        cap if (cap == proto_gazette::capability::APPLY)
            || (cap == proto_gazette::capability::APPEND) =>
        {
            models::Capability::Write
        }
        cap => {
            return Err(
                anyhow::anyhow!("capability {cap} cannot be authorized by this service")
                    .with_status(StatusCode::FORBIDDEN),
            )
        }
    };

    match Snapshot::evaluate(&app.snapshot, claims.iat, |snapshot: &Snapshot| {
        evaluate_authorization(
            snapshot,
            shard_id,
            shard_data_plane_fqdn,
            &token,
            &journal_name_or_prefix,
            required_role,
        )
    }) {
        Ok((encoding_key, data_plane_fqdn, broker_address)) => {
            claims.iss = data_plane_fqdn;
            claims.exp = claims.iat + super::exp_seconds();

            let token = jsonwebtoken::encode(&header, &claims, &encoding_key)
                .context("failed to encode authorized JWT")?;

            Ok(axum::Json(Response {
                broker_address,
                token,
                ..Default::default()
            }))
        }
        Err(Err(err)) if err.error.downcast_ref::<BlackHole>().is_some() => {
            let BlackHole {
                encoding_key,
                broker_address,
            } = err.error.downcast::<BlackHole>().unwrap();

            // claims.iss is left unchanged.
            claims.sel.include = Some(labels::add_value(
                claims.sel.include.unwrap_or_default(),
                "estuary.dev/match-nothing",
                "1",
            ));
            claims.exp = claims.iat + super::exp_seconds();

            let token = jsonwebtoken::encode(&header, &claims, &encoding_key)
                .context("failed to encode authorized JWT")?;

            Ok(axum::Json(Response {
                broker_address,
                token,
                ..Default::default()
            }))
        }
        Err(Ok(retry_millis)) => Ok(axum::Json(Response {
            retry_millis,
            ..Default::default()
        })),
        Err(Err(err)) => Err(err),
    }
}

// A BlackHole error is raised when a task request is authorized,
// but its `journal_name_or_prefix` doesn't map to a collection
// (either because the collection is deleted, or exists under a
// different generation ID).
//
// A "black hole" response extends the request claims with a
// label selector that never matches anything, which:
// - Causes List RPCs to succeed, but return nothing:
//   This avoids failing a sourcing task, on the presumption
//   that it's just awaiting an update from the control plane
//   and should be allowed to gracefully finish its other work.
// - Causes Append RPCs to error with status JOURNAL_NOT_FOUND.
//   This allows for correct handling of recovered ACK intents
//   destined for a journal which has since been deleted.
// - Causes an Apply RPC, used to create partitions, to fail.
//
// Note that we're directing this token to the task's data-plane,
// since we have no idea what data-plane the collection might have
// lived in, as we couldn't find it.
#[derive(thiserror::Error)]
#[error("black hole")]
struct BlackHole {
    encoding_key: jsonwebtoken::EncodingKey,
    broker_address: String,
}

impl std::fmt::Debug for BlackHole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

fn evaluate_authorization(
    snapshot: &Snapshot,
    shard_id: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
    journal_name_or_prefix: &str,
    required_role: models::Capability,
) -> Result<(jsonwebtoken::EncodingKey, String, String), crate::api::ApiError> {
    let Snapshot {
        collections,
        data_planes,
        role_grants,
        tasks,
        ..
    } = snapshot;

    // Map `claims.iss`, a data-plane FQDN, into its token-verified data-plane.
    let Some(task_data_plane) = snapshot
        .verify_data_plane_token(shard_data_plane_fqdn, token)
        .context("invalid data-plane hmac key")?
    else {
        return Err(
            anyhow::anyhow!("no data-plane keys validated against the token signature")
                .with_status(StatusCode::FORBIDDEN),
        );
    };

    // Map `claims.sub`, a Shard ID, into a task running in `task_data_plane`.
    let Some(task) = tasks
        .binary_search_by(|task| {
            if shard_id.starts_with(&task.shard_template_id) {
                std::cmp::Ordering::Equal
            } else {
                task.shard_template_id.as_str().cmp(shard_id)
            }
        })
        .ok()
        .map(|index| &tasks[index])
        .filter(|task| task.data_plane_id == task_data_plane.control_id)
    else {
        return Err(anyhow::anyhow!(
            "task shard {shard_id} within data-plane {shard_data_plane_fqdn} is not known"
        )
        .with_status(StatusCode::PRECONDITION_FAILED));
    };

    // Map a required `name` journal label selector into its collection.
    let collection = collections
        .binary_search_by(|collection| {
            if journal_name_or_prefix.starts_with(&collection.journal_template_name) {
                std::cmp::Ordering::Equal
            } else {
                collection
                    .journal_template_name
                    .as_str()
                    .cmp(journal_name_or_prefix)
            }
        })
        .ok()
        .map(|index| &collections[index]);

    let (found, collection_data_plane, is_ops) = if let Some(collection) = collection {
        let Some(collection_data_plane) = data_planes.get_by_key(&collection.data_plane_id) else {
            return Err(anyhow::anyhow!(
                "collection data-plane {} not found",
                collection.data_plane_id
            )
            .with_status(StatusCode::INTERNAL_SERVER_ERROR));
        };

        // As a special case outside of the RBAC system, allow a task to write
        // to its designated partition within its ops collections.
        let is_ops = required_role == models::Capability::Write
            && (collection.collection_name == task_data_plane.ops_logs_name
                || collection.collection_name == task_data_plane.ops_stats_name)
            && journal_name_or_prefix.ends_with(&super::ops_suffix(task));

        (true, collection_data_plane, is_ops)
    } else {
        // This collection doesn't exist, or exists under a different generation ID.
        (false, task_data_plane, false)
    };

    if !is_ops
        && !tables::RoleGrant::is_authorized(
            role_grants,
            &task.task_name,
            &journal_name_or_prefix,
            required_role,
        )
    {
        tracing::warn!(
            %task.spec_type,
            %shard_id,
            %journal_name_or_prefix,
            ?required_role,
            ops_logs=%task_data_plane.ops_logs_name,
            ops_stats=%task_data_plane.ops_stats_name,
            ops_suffix=%super::ops_suffix(task),
            "task authorization rejection context"
        );
        return Err(anyhow::anyhow!(
            "task shard {shard_id} is not authorized to {journal_name_or_prefix} for {required_role:?}"
        ).with_status(StatusCode::FORBIDDEN));
    }

    let Some(encoding_key) = collection_data_plane.hmac_keys.first() else {
        return Err(anyhow::anyhow!(
            "collection data-plane {} has no configured HMAC keys",
            collection_data_plane.data_plane_name
        )
        .with_status(StatusCode::INTERNAL_SERVER_ERROR));
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)
        .context("invalid data-plane hmac key")?;

    if found {
        Ok((
            encoding_key,
            collection_data_plane.data_plane_fqdn.clone(),
            super::maybe_rewrite_address(
                task.data_plane_id != collection_data_plane.control_id,
                &collection_data_plane.broker_address,
            ),
        ))
    } else {
        Err(super::ApiError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: anyhow::anyhow!(BlackHole {
                broker_address: collection_data_plane.broker_address.to_string(),
                encoding_key,
            }),
        })
    }
}
