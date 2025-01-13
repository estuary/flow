use super::App;
use crate::api::snapshot::Snapshot;
use anyhow::Context;
use models::CatalogType;
use std::sync::Arc;

type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::DekafAuthResponse;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AccessTokenClaims {
    exp: u64,
    iat: u64,
    role: String,
}

#[axum::debug_handler]
pub async fn authorize_dekaf(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Json(request): axum::Json<Request>,
) -> axum::response::Response {
    super::wrap(async move { do_authorize_dekaf(&app, &request).await }).await
}

const DEKAF_ROLE: &str = "dekaf";

/// Dekaf straddles the control-plane and data-plane:
///    * It needs full control over the `registered_avro_schemas` table in the control-plane
///      in order to serve its schema registry functionality
///    * It needs access to the full specs of materializations both in order to authenticate
///      sessions, as well as figure out which bindings to expose under what names, etc.
///    * It needs to read from journals in order to serve topic data to consumers
///    * It needs to write to ops logs and stats for observability and billing concerns
///
/// This endpoint provides it a way to do all of these things, while also staying within
/// the authorization framework used by other data-plane actors.
///
/// Specifically, this checks that:
///     * Your request is coming from an authorized actor in a data-plane,
///     * That actor is acting on behalf of a task running in that same data-plane.
///
/// Once we've authenticated and authorized the request as best we can, we put together
/// a package of all the information Dekaf needs in one place:
///    * A short-lived control-plane access token to authorize requests under the `dekaf` role
///      which has grants to the `public.registered_avro_schemas` table
///    * The `models::MaterializationDef` for the materialization being requested
///      as identified by the `sub` JWT claim
///    * The ops logs and stats journal names for the materialization. This will allow Dekaf to
///      write ops logs and stats.
#[tracing::instrument(skip(app), err(level = tracing::Level::WARN))]
async fn do_authorize_dekaf(app: &App, Request { token }: &Request) -> anyhow::Result<Response> {
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

    let task_name = claims.sub.as_str();
    if task_name.is_empty() {
        anyhow::bail!("missing required materialization name (`sub` claim)");
    }

    let shard_data_plane_fqdn = claims.iss.as_str();
    if shard_data_plane_fqdn.is_empty() {
        anyhow::bail!("missing required task data-plane FQDN (`iss` claim)");
    }

    if claims.cap != proto_flow::capability::AUTHORIZE {
        anyhow::bail!("invalid capability, must be AUTHORIZE only: {}", claims.cap);
    }

    match Snapshot::evaluate(&app.snapshot, claims.iat, |snapshot: &Snapshot| {
        evaluate_authorization(snapshot, task_name, shard_data_plane_fqdn, token)
    }) {
        Ok((ops_logs_journal, ops_stats_journal)) => {
            let materialization_spec = sqlx::query!(
                r#"
                    select
                        built_spec as "spec: sqlx::types::Json<models::RawValue>",
                        spec_type as "spec_type: models::CatalogType"
                    from live_specs
                    where live_specs.catalog_name = $1
                    "#,
                task_name
            )
            .fetch_one(&app.pg_pool)
            .await
            .context("failed to fetch task spec")?;

            let (Some(materialization_spec), Some(spec_type)) =
                (materialization_spec.spec, materialization_spec.spec_type)
            else {
                anyhow::bail!("`live_specs` row for {task_name} is missing spec or spec_type");
            };

            if !matches!(spec_type, models::CatalogType::Materialization) {
                anyhow::bail!("Unexpected spec type {:?}", spec_type);
            }

            let unix_ts = jsonwebtoken::get_current_timestamp();
            let claims = AccessTokenClaims {
                iat: unix_ts,
                exp: unix_ts + (60 * 60),
                role: DEKAF_ROLE.to_string(),
            };

            let signed = jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &claims,
                &app.control_plane_jwt_signer,
            )?;

            Ok(Response {
                token: signed,
                ops_logs_journal,
                ops_stats_journal,
                task_spec: Some(materialization_spec.0),
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
    task_name: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
) -> anyhow::Result<(String, String)> {
    tracing::debug!(?task_name, "Task name");
    // Map `claims.sub`, a task name, into its task.
    let task = snapshot.task_by_catalog_name(&models::Name::new(task_name.to_string()));

    // Map `claims.iss`, a data-plane FQDN, into its task-matched data-plane.
    let task_data_plane = task.and_then(|task| {
        snapshot
            .data_planes
            .get_by_key(&task.data_plane_id)
            .filter(|data_plane| data_plane.data_plane_fqdn == shard_data_plane_fqdn)
    });

    let (Some(task), Some(task_data_plane)) = (task, task_data_plane) else {
        anyhow::bail!("task {task_name} within data-plane {shard_data_plane_fqdn} is not known")
    };

    if task.spec_type != CatalogType::Materialization {
        anyhow::bail!(
            "task {task_name} must be a materialization, but is {:?} instead",
            task.spec_type
        )
    }

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

    let (Some(ops_logs), Some(ops_stats)) = (
        snapshot.collection_by_catalog_name(&task_data_plane.ops_logs_name),
        snapshot.collection_by_catalog_name(&task_data_plane.ops_stats_name),
    ) else {
        anyhow::bail!(
            "couldn't resolve data-plane {} ops collections",
            task.data_plane_id
        )
    };

    let ops_suffix = super::ops_suffix(task);
    let ops_logs_journal = format!("{}{}", ops_logs.journal_template_name, &ops_suffix[1..]);
    let ops_stats_journal = format!("{}{}", ops_stats.journal_template_name, &ops_suffix[1..]);

    Ok((ops_logs_journal, ops_stats_journal))
}
