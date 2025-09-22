use super::App;
use crate::server::error::ApiErrorExt;
use crate::server::snapshot::Snapshot;
use anyhow::Context;
use axum::http::StatusCode;
use futures::{FutureExt, TryFutureExt};
use models::CatalogType;
use std::sync::Arc;

type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::DekafAuthResponse;

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
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    let fetch_spec = |task: String| {
        sqlx::query!(
            r#"
            SELECT
                spec_type AS "spec_type!: models::CatalogType",
                built_spec AS "built_spec!: sqlx::types::Json<models::RawValue>"
            FROM live_specs
            WHERE live_specs.catalog_name = $1 AND built_spec IS NOT NULL
            "#,
            task
        )
        .fetch_one(&app.pg_pool)
        .map_ok(|r| (r.spec_type, r.built_spec.0))
        .boxed()
    };

    do_authorize_dekaf(
        &app.snapshot,
        token,
        &app.control_plane_jwt_signer,
        fetch_spec,
    )
    .await
}

pub async fn do_authorize_dekaf<'a>(
    snapshot: &'a std::sync::RwLock<Snapshot>,
    token: String,
    control_key: &'a jsonwebtoken::EncodingKey,
    fetch_spec: impl FnOnce(
        String,
    ) -> futures::future::BoxFuture<
        'a,
        sqlx::Result<(models::CatalogType, models::RawValue)>,
    >,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    let (_header, claims) = super::parse_untrusted_data_plane_claims(&token)?;

    let task_name = claims.sub.as_str();
    let shard_data_plane_fqdn = claims.iss.as_str();

    if claims.cap != proto_flow::capability::AUTHORIZE {
        return Err(
            anyhow::anyhow!("invalid capability, must be AUTHORIZE only: {}", claims.cap)
                .with_status(StatusCode::FORBIDDEN),
        );
    }

    match Snapshot::evaluate(
        snapshot,
        chrono::DateTime::from_timestamp(claims.iat as i64, 0).unwrap_or_default(),
        |snapshot: &Snapshot| {
            evaluate_authorization(snapshot, task_name, shard_data_plane_fqdn, &token)
        },
    ) {
        Ok((exp, (ops_logs_journal, ops_stats_journal, redirect_fqdn))) => {
            let (spec_type, built_spec) = fetch_spec(task_name.to_string())
                .await
                .context("failed to fetch task spec")?;

            if !matches!(spec_type, models::CatalogType::Materialization) {
                return Err(anyhow::anyhow!("Unexpected spec type {:?}", spec_type)
                    .with_status(StatusCode::INTERNAL_SERVER_ERROR));
            }

            let claims = models::authorizations::ControlClaims {
                iat: claims.iat,
                exp: exp.timestamp() as u64,
                sub: uuid::Uuid::nil(),
                role: DEKAF_ROLE.to_string(),
                email: None,
            };

            // Only return a token if we are not redirecting
            let token = if redirect_fqdn.is_none() {
                jsonwebtoken::encode(&jsonwebtoken::Header::default(), &claims, control_key)
                    .context("failed to encode authorized JWT")?
            } else {
                "".to_string()
            };

            Ok(axum::Json(Response {
                token,
                ops_logs_journal,
                ops_stats_journal,
                task_spec: Some(built_spec),
                retry_millis: 0,
                redirect_dataplane_fqdn: redirect_fqdn,
            }))
        }
        Err(Ok(backoff)) => Ok(axum::Json(Response {
            retry_millis: backoff.as_millis() as u64,
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
) -> Result<
    (
        Option<chrono::DateTime<chrono::Utc>>,
        (String, String, Option<String>),
    ),
    crate::server::error::ApiError,
> {
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

    // First, try to find task in the requesting dataplane by mapping
    // `claims.sub`, a task name, into a task running in `task_data_plane`.
    if let Some(task) = snapshot
        .task_by_catalog_name(&task_name)
        .filter(|task| task.data_plane_id == task_data_plane.control_id)
    {
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

        return Ok((
            snapshot.cordon_at(&task.task_name, task_data_plane),
            (ops_logs_journal, ops_stats_journal, None), // No redirect needed
        ));
    }

    // Task not found in requesting dataplane, check if it exists elsewhere to redirect
    if let Some(task) = snapshot.task_by_catalog_name(&task_name) {
        if task.spec_type != CatalogType::Materialization {
            return Err(anyhow::anyhow!(
                "task {task_name} must be a materialization, but is {:?} instead",
                task.spec_type
            )
            .with_status(StatusCode::PRECONDITION_FAILED));
        }

        let target_dataplane = snapshot
            .data_planes
            .iter()
            .find(|dp| dp.control_id == task.data_plane_id)
            .ok_or_else(|| {
                anyhow::anyhow!("target dataplane for task {task_name} not found")
                    .with_status(StatusCode::INTERNAL_SERVER_ERROR)
            })?;

        return Ok((
            snapshot.cordon_at(&task.task_name, task_data_plane),
            (
                String::new(),
                String::new(),
                Some(target_dataplane.data_plane_fqdn.clone()),
            ),
        ));
    }

    // Task not found anywhere
    Err(
        anyhow::anyhow!("task {task_name} not found in any dataplane")
            .with_status(StatusCode::PRECONDITION_FAILED),
    )
}

const DEKAF_ROLE: &str = "dekaf";

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_success() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE,
            iss: "fqdn2".to_string(),
            sel: proto_gazette::LabelSelector::default(),
            sub: "bobCo/anvils/materialize-orange".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            {
              "iat": 0,
              "exp": 0,
              "sub": "00000000-0000-0000-0000-000000000000",
              "role": "dekaf"
            },
            "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            {
              "$serde_json::private::RawValue": "{\"spec\":\"fixture\"}"
            }
          ]
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_materialization() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE,
            iss: "fqdn2".to_string(),
            sel: proto_gazette::LabelSelector::default(),
            sub: "bobCo/widgets/source-squash".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 412,
            "error": "task bobCo/widgets/source-squash must be a materialization, but is Capture instead"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_not_found() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE,
            iss: "fqdn2".to_string(),
            sel: proto_gazette::LabelSelector::default(),
            sub: "bobCo/bananas".to_string(),
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 412,
            "error": "task bobCo/bananas not found in any dataplane"
          }
        }
        "###);
    }

    #[tokio::test]
    async fn test_cordon_task() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE,
            iss: "fqdn2".to_string(),
            sel: proto_gazette::LabelSelector::default(),
            sub: "bobCo/widgets/materialize-mango".to_string(),
        })
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
    async fn test_redirect() {
        let outcome = run(proto_gazette::Claims {
            iat: 0,
            exp: 0,
            cap: proto_flow::capability::AUTHORIZE,
            iss: "fqdn2".to_string(), // Request from dataplane 2
            sel: proto_gazette::LabelSelector::default(),
            sub: "acmeCo/materialize-pear".to_string(), // Task is in dataplane 1
        })
        .await;

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 500,
            "error": "redirect to: fqdn1"
          }
        }
        "###);
    }

    async fn run(
        mut claims: proto_gazette::Claims,
    ) -> Result<
        (
            models::authorizations::ControlClaims,
            String,
            String,
            Option<models::RawValue>,
        ),
        crate::server::error::ApiError,
    > {
        let taken = chrono::Utc::now();
        let snapshot = Snapshot::build_fixture(Some(taken));
        let snapshot = std::sync::RwLock::new(snapshot);

        claims.iat = taken.timestamp() as u64;
        claims.exp = taken.timestamp() as u64 + 100;

        let request_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("key3".as_bytes()),
        )
        .unwrap();

        let Response {
            token: response_token,
            ops_logs_journal,
            ops_stats_journal,
            task_spec,
            retry_millis,
            redirect_dataplane_fqdn,
        } = do_authorize_dekaf(
            &snapshot,
            request_token,
            &jsonwebtoken::EncodingKey::from_secret("control-key".as_bytes()),
            |_task| {
                async {
                    Ok((
                        models::CatalogType::Materialization,
                        models::RawValue::from_value(&serde_json::json!({"spec": "fixture"})),
                    ))
                }
                .boxed()
            },
        )
        .await?
        .0;

        if retry_millis != 0 {
            return Err(anyhow::anyhow!("retry").into());
        }

        if let Some(redirect) = redirect_dataplane_fqdn {
            return Err(anyhow::anyhow!(format!("redirect to: {}", redirect)).into());
        }

        // Decode and verify the response token.
        let mut decoded = jsonwebtoken::decode::<models::authorizations::ControlClaims>(
            &response_token,
            &jsonwebtoken::DecodingKey::from_secret("control-key".as_bytes()),
            &jsonwebtoken::Validation::default(),
        )
        .expect("failed to decode response token")
        .claims;

        (decoded.iat, decoded.exp) = (0, 0);

        Ok((decoded, ops_logs_journal, ops_stats_journal, task_spec))
    }
}
