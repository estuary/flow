use super::App;
use crate::api::error::ApiErrorExt;
use crate::api::snapshot::Snapshot;
use anyhow::Context;
use axum::http::StatusCode;
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
#[axum::debug_handler]
#[tracing::instrument(skip(app), err(level = tracing::Level::WARN))]
pub async fn authorize_dekaf(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::Json(Request { token }): axum::Json<Request>,
) -> Result<axum::Json<Response>, crate::api::ApiError> {
    let jsonwebtoken::TokenData { header, claims }: jsonwebtoken::TokenData<proto_gazette::Claims> = {
        // In this pass we do not validate the signature,
        // because we don't yet know which data-plane the JWT is signed by.
        let empty_key = jsonwebtoken::DecodingKey::from_secret(&[]);
        let mut validation = jsonwebtoken::Validation::default();
        validation.insecure_disable_signature_validation();
        jsonwebtoken::decode(&token, &empty_key, &validation)
            .map_err(|err| anyhow::anyhow!(err).with_status(StatusCode::BAD_REQUEST))?
    };
    tracing::debug!(?claims, ?header, "decoded authorization request");

    let task_name = claims.sub.as_str();
    if task_name.is_empty() {
        return Err(
            anyhow::anyhow!("missing required materialization name (`sub` claim)")
                .with_status(StatusCode::BAD_REQUEST),
        );
    }

    let shard_data_plane_fqdn = claims.iss.as_str();
    if shard_data_plane_fqdn.is_empty() {
        return Err(
            anyhow::anyhow!("missing required task data-plane FQDN (`iss` claim)")
                .with_status(StatusCode::BAD_REQUEST),
        );
    }

    if claims.cap != proto_flow::capability::AUTHORIZE {
        return Err(
            anyhow::anyhow!("invalid capability, must be AUTHORIZE only: {}", claims.cap)
                .with_status(StatusCode::FORBIDDEN),
        );
    }

    match Snapshot::evaluate(&app.snapshot, claims.iat, |snapshot: &Snapshot| {
        evaluate_authorization(snapshot, task_name, shard_data_plane_fqdn, &token)
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
                return Err(anyhow::anyhow!(
                    "`live_specs` row for {task_name} is missing spec or spec_type"
                )
                .with_status(StatusCode::INTERNAL_SERVER_ERROR));
            };

            if !matches!(spec_type, models::CatalogType::Materialization) {
                return Err(anyhow::anyhow!("Unexpected spec type {:?}", spec_type)
                    .with_status(StatusCode::INTERNAL_SERVER_ERROR));
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
            )
            .context("failed to encode authorized JWT")?;

            Ok(axum::Json(Response {
                token: signed,
                ops_logs_journal,
                ops_stats_journal,
                task_spec: Some(materialization_spec.0),
                retry_millis: 0,
            }))
        }
        Err(Ok(retry_millis)) => Ok(axum::Json(Response {
            retry_millis,
            ..Default::default()
        })),
        Err(Err(err)) => Err(err),
    }
}

fn evaluate_authorization(
    snapshot: &Snapshot,
    task_name: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
) -> Result<(String, String), crate::api::ApiError> {
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

    // Map `claims.sub`, a task name, into a task running in `task_data_plane`.
    let Some(task) = snapshot
        .task_by_catalog_name(&models::Name::new(task_name.to_string()))
        .filter(|task| task.data_plane_id == task_data_plane.control_id)
    else {
        return Err(anyhow::anyhow!(
            "task {task_name} within data-plane {shard_data_plane_fqdn} is not known"
        )
        .with_status(StatusCode::PRECONDITION_FAILED));
    };

    if task.spec_type != CatalogType::Materialization {
        return Err(anyhow::anyhow!(
            "task {task_name} must be a materialization, but is {:?} instead",
            task.spec_type
        )
        .with_status(StatusCode::PRECONDITION_FAILED));
    }

    let (Some(ops_logs), Some(ops_stats)) = (
        snapshot.collection_by_catalog_name(&task_data_plane.ops_logs_name),
        snapshot.collection_by_catalog_name(&task_data_plane.ops_stats_name),
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

    Ok((ops_logs_journal, ops_stats_journal))
}

const DEKAF_ROLE: &str = "dekaf";
