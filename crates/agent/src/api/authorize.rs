use super::App;
use anyhow::Context;
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    // # JWT token to be authorized and signed.
    token: String,
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    // # JWT token which has been authorized for use.
    token: String,
    // # Address of Gazette brokers for the issued token.
    broker_address: String,
}

pub async fn authorize_task(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Json(request): axum::Json<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_authorize_task(&app, &request).await }).await
}

#[tracing::instrument(skip_all, err(level = tracing::Level::WARN))]
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

    // Split off the leading 'capture', 'derivation', or 'materialization'
    // prefix of the Shard ID conveyed in `claims.subject`.
    // The remainder of `task_shard` is a catalog task plus a shard suffix.
    let Some((task_type, task_shard)) = claims.sub.split_once('/') else {
        anyhow::bail!("invalid claims subject {}", claims.sub);
    };
    // Map task-type from shard prefix naming, to ops log naming.
    let task_type = match task_type {
        "capture" => "capture",
        "derivation" => "derivation",
        "materialize" => "materialization",
        _ => anyhow::bail!("invalid shard task type {task_type}"),
    };

    let journal_name_or_prefix = labels::expect_one(claims.sel.include(), "name")?;

    // Require the request was signed with the AUTHORIZE capability,
    // and then strip this capability before issuing a response token.
    if claims.cap & proto_flow::capability::AUTHORIZE == 0 {
        anyhow::bail!("missing required AUTHORIZE capability: {}", claims.cap);
    }
    claims.cap &= !proto_flow::capability::AUTHORIZE;

    // Validate and match the requested capabilities to a corresponding role.
    let required_role = match claims.cap {
        proto_gazette::capability::LIST | proto_gazette::capability::READ => "read",
        proto_gazette::capability::APPLY | proto_gazette::capability::APPEND => "write",
        _ => {
            anyhow::bail!(
                "capability {} cannot be authorized by this service",
                claims.cap
            );
        }
    };

    // Resolve the identified data-plane through its task assignment (which is verified) and FQDN.
    let Some(task_data_plane) = agent_sql::data_plane::fetch_data_plane_by_task_and_fqdn(
        &app.pg_pool,
        task_shard,
        &claims.iss,
    )
    .await?
    else {
        anyhow::bail!(
            "task {task_shard} within data-plane {} is not known",
            claims.iss
        );
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

    // Query for a task => collection pair and their RBAC authorization.
    let (task_name, collection_name, collection_data_plane_id, mut authorized) =
        agent_sql::data_plane::verify_task_authorization(
            &app.pg_pool,
            task_shard,
            journal_name_or_prefix,
            required_role,
        )
        .await?
        .unwrap_or((
            String::new(),
            models::Collection::default(),
            models::Id::zero(),
            false,
        ));

    // As a special case outside of the RBAC system, allow a task to write
    // to its designated partition within its ops collections.
    if !authorized
        && required_role == "write"
        && (collection_name == task_data_plane.ops_logs_name
            || collection_name == task_data_plane.ops_stats_name)
        && journal_name_or_prefix.ends_with(&format!(
            "/kind={task_type}/name={}/pivot=00",
            labels::percent_encoding(&task_name).to_string(),
        ))
    {
        authorized = true;
    }

    if !authorized {
        let ops_suffix = format!(
            "/kind={task_type}/name={}/pivot=00",
            labels::percent_encoding(&task_name).to_string(),
        );
        tracing::warn!(
            %task_type,
            %task_shard,
            %journal_name_or_prefix,
            required_role,
            ops_logs=%task_data_plane.ops_logs_name,
            ops_stats=%task_data_plane.ops_stats_name,
            %ops_suffix,
            "task authorization rejection context"
        );
        anyhow::bail!("task shard {task_shard} is not authorized to {journal_name_or_prefix} for {required_role}");
    }
    // We've now completed AuthN and AuthZ checks and can proceed.

    // TODO(johnny): We can avoid a DB query in the common case,
    // where a task and collection data-plane are the same.
    // I'm not doing this yet to keep the code path simpler while we're testing.

    let collection_data_plane = agent_sql::data_plane::fetch_data_planes(
        &app.pg_pool,
        vec![collection_data_plane_id],
        "", // No default name to retrieve.
        uuid::Uuid::nil(),
    )
    .await?
    .pop()
    .context("collection data-plane does not exist")?;

    claims.iss = collection_data_plane.data_plane_fqdn;
    claims.iat = jsonwebtoken::get_current_timestamp();
    claims.exp = claims.iat + 3_600; // One hour.

    let Some(encoding_key) = collection_data_plane.hmac_keys.first() else {
        anyhow::bail!(
            "collection data-plane {collection_data_plane_id} has no configured HMAC keys"
        );
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_base64_secret(&encoding_key)?;
    let token = jsonwebtoken::encode(&header, &claims, &encoding_key)?;

    Ok(Response {
        broker_address: collection_data_plane.broker_address,
        token,
    })
}
