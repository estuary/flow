use super::{App, Snapshot};
use anyhow::Context;
use std::sync::Arc;

type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::TaskAuthorization;

#[axum::debug_handler]
pub async fn authorize_task(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Json(request): axum::Json<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_authorize_task(&app, &request).await }).await
}

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
#[tracing::instrument(skip(app), err(level = tracing::Level::WARN))]
async fn do_authorize_task(app: &App, Request { token }: &Request) -> anyhow::Result<Response> {
    let jsonwebtoken::TokenData { header, mut claims }: jsonwebtoken::TokenData<
        proto_gazette::Claims,
    > = {
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

    let journal_name_or_prefix = labels::expect_one(claims.sel.include(), "name")?.to_owned();

    // Require the request was signed with the AUTHORIZE capability,
    // and then strip this capability before issuing a response token.
    if claims.cap & proto_flow::capability::AUTHORIZE == 0 {
        anyhow::bail!("missing required AUTHORIZE capability: {}", claims.cap);
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
        cap => anyhow::bail!("capability {cap} cannot be authorized by this service"),
    };

    match Snapshot::evaluate(&app.snapshot, claims.iat, |snapshot: &Snapshot| {
        evaluate_authorization(
            snapshot,
            shard_id,
            shard_data_plane_fqdn,
            token,
            &journal_name_or_prefix,
            required_role,
        )
    }) {
        Ok((encoding_key, data_plane_fqdn, broker_address)) => {
            claims.iss = data_plane_fqdn;
            claims.exp = claims.iat + super::exp_seconds();

            let token = jsonwebtoken::encode(&header, &claims, &encoding_key)
                .context("failed to encode authorized JWT")?;

            Ok(Response {
                broker_address,
                token,
                ..Default::default()
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
    journal_name_or_prefix: &str,
    required_role: models::Capability,
) -> anyhow::Result<(jsonwebtoken::EncodingKey, String, String)> {
    let Snapshot {
        collections,
        data_planes,
        role_grants,
        tasks,
        ..
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

    let (Some(task), Some(task_data_plane)) = (task, task_data_plane) else {
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

    // Map a required `name` journal label selector into its collection.
    let Some(collection) = collections
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
        .map(|index| &collections[index])
    else {
        anyhow::bail!("journal name or prefix {journal_name_or_prefix} is not known");
    };

    let Some(collection_data_plane) = data_planes.get_by_key(&collection.data_plane_id) else {
        anyhow::bail!(
            "collection data-plane {} not found",
            collection.data_plane_id
        );
    };

    // As a special case outside of the RBAC system, allow a task to write
    // to its designated partition within its ops collections.
    if required_role == models::Capability::Write
        && (collection.collection_name == task_data_plane.ops_logs_name
            || collection.collection_name == task_data_plane.ops_stats_name)
        && journal_name_or_prefix.ends_with(&super::ops_suffix(task))
    {
        // Authorized write into designated ops partition.
    } else if tables::RoleGrant::is_authorized(
        role_grants,
        &task.task_name,
        &collection.collection_name,
        required_role,
    ) {
        // Authorized access through RBAC.
    } else {
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
        anyhow::bail!(
            "task shard {shard_id} is not authorized to {journal_name_or_prefix} for {required_role:?}"
        );
    }

    let Some(encoding_key) = collection_data_plane.hmac_keys.first() else {
        anyhow::bail!(
            "collection data-plane {} has no configured HMAC keys",
            collection_data_plane.data_plane_name
        );
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)?;

    Ok((
        encoding_key,
        collection_data_plane.data_plane_fqdn.clone(),
        super::maybe_rewrite_address(
            task.data_plane_id != collection.data_plane_id,
            &collection_data_plane.broker_address,
        ),
    ))
}
