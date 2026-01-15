use futures::TryFutureExt;

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
#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(app, env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_dekaf(
    axum::extract::State(app): axum::extract::State<std::sync::Arc<crate::App>>,
    mut env: crate::Envelope,
    super::Request(Request { token }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::ApiError> {
    let unverified = super::parse_untrusted_data_plane_claims(&token)?;

    // Use the `iat` claim to establish the logical start of the request,
    // rounded up to the next second (as it was round down when encoded).
    env.started = tokens::DateTime::from_timestamp_secs(1 + unverified.claims().iat as i64)
        .unwrap_or_default();

    let policy_result = evaluate_authorization(
        env.snapshot(),
        &unverified.claims().sub,
        &unverified.claims().iss,
        &token,
    );

    // Legacy: return a custom 200 response for client-side retries.
    let (expiry, (ops_logs_journal, ops_stats_journal, redirect_fqdn)) =
        match env.authorization_outcome(policy_result).await {
            Ok(ok) => ok,
            Err(crate::ApiError::AuthZRetry(retry)) => {
                return Ok(axum::Json(Response {
                    retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                    ..Default::default()
                }));
            }
            Err(err @ crate::ApiError::Status(_)) => return Err(err),
        };

    let (spec_type, built_spec) = sqlx::query!(
        r#"
        SELECT
            spec_type AS "spec_type!: models::CatalogType",
            built_spec AS "built_spec!: sqlx::types::Json<models::RawValue>"
        FROM live_specs
        WHERE live_specs.catalog_name = $1 AND built_spec IS NOT NULL
        "#,
        &unverified.claims().sub
    )
    .fetch_one(&env.pg_pool)
    .map_ok(|r| (r.spec_type, r.built_spec.0))
    .await
    .map_err(|err| tonic::Status::internal(format!("failed to fetch task spec: {err}")))?;

    if !matches!(spec_type, models::CatalogType::Materialization) {
        return Err(tonic::Status::internal(format!("unexpected spec type {spec_type:?}")).into());
    }

    let response_claims = models::authorizations::ControlClaims {
        aud: "authenticated".to_string(),
        iat: unverified.claims().iat,
        exp: expiry.timestamp() as u64,
        sub: uuid::Uuid::nil(),
        role: DEKAF_ROLE.to_string(),
        email: None,
    };

    // Only return a token if we are not redirecting
    let token = if redirect_fqdn.is_none() {
        tokens::jwt::sign(&response_claims, &app.control_plane_jwt_encode_key)?
    } else {
        String::new()
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

fn evaluate_authorization(
    snapshot: &crate::Snapshot,
    task_name: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
) -> crate::AuthZResult<(String, String, Option<String>)> {
    // Map `claims.iss`, a data-plane FQDN, into its token-verified data-plane.
    let Some(task_data_plane) = snapshot.verify_data_plane_token(shard_data_plane_fqdn, token)?
    else {
        return Err(tonic::Status::unauthenticated(
            "no data-plane keys validated against the token signature",
        ));
    };

    // First, try to find task in the requesting dataplane by mapping
    // `task name` (the `sub` claim) into a task running in `task_data_plane`.
    if let Some(task) = snapshot
        .task_by_catalog_name(task_name)
        .filter(|task| task.data_plane_id == task_data_plane.control_id)
    {
        if task.spec_type != models::CatalogType::Materialization {
            return Err(tonic::Status::failed_precondition(format!(
                "task {task_name} must be a materialization, but is {:?} instead",
                task.spec_type
            )));
        }

        let (Some(ops_logs), Some(ops_stats)) = (
            snapshot.collection_by_catalog_name(&task_data_plane.ops_logs_name),
            snapshot.collection_by_catalog_name(&task_data_plane.ops_stats_name),
        ) else {
            return Err(tonic::Status::internal(format!(
                "couldn't resolve data-plane {} ops collections",
                task.data_plane_id
            )));
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
    if let Some(task) = snapshot.task_by_catalog_name(task_name) {
        if task.spec_type != models::CatalogType::Materialization {
            return Err(tonic::Status::failed_precondition(format!(
                "task {task_name} must be a materialization, but is {:?} instead",
                task.spec_type
            )));
        }

        let Some(target_dataplane) = snapshot
            .data_planes
            .iter()
            .find(|dp| dp.control_id == task.data_plane_id)
        else {
            return Err(tonic::Status::internal(format!(
                "target dataplane for task {task_name} not found"
            )));
        };

        return Ok((
            snapshot.cordon_at(&task.task_name, task_data_plane),
            (
                String::new(),
                String::new(),
                Some(target_dataplane.data_plane_fqdn.clone()),
            ),
        ));
    }

    Err(tonic::Status::not_found(format!(
        "task {task_name} not found"
    )))
}

const DEKAF_ROLE: &str = "dekaf";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success() {
        let outcome = run(
            "bobCo/anvils/materialize-orange",
            "fqdn2", // Request from data-plane 2 where this task lives
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": {
            "ops_logs_journal": "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            "ops_stats_journal": "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            "redirect_fqdn": null
          }
        }
        "###);
    }

    #[test]
    fn test_not_materialization() {
        let outcome = run("bobCo/widgets/source-squash", "fqdn2");

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 412,
            "error": "task bobCo/widgets/source-squash must be a materialization, but is Capture instead"
          }
        }
        "###);
    }

    #[test]
    fn test_not_found() {
        let outcome = run("bobCo/bananas", "fqdn2");

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "task bobCo/bananas not found"
          }
        }
        "###);
    }

    #[test]
    fn test_cordon_task() {
        let outcome = run("bobCo/widgets/materialize-mango", "fqdn2");

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok_Cordoned": {
            "ops_logs_journal": "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fwidgets%2Fmaterialize-mango/pivot=00",
            "ops_stats_journal": "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fwidgets%2Fmaterialize-mango/pivot=00",
            "redirect_fqdn": null
          }
        }
        "###);
    }

    #[test]
    fn test_redirect() {
        let outcome = run(
            "acmeCo/materialize-pear", // Task is in dataplane 1
            "fqdn2",                   // Request from dataplane 2
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": {
            "ops_logs_journal": "",
            "ops_stats_journal": "",
            "redirect_fqdn": "fqdn1"
          }
        }
        "###);
    }

    #[derive(serde::Serialize)]
    struct SuccessOutput {
        ops_logs_journal: String,
        ops_stats_journal: String,
        redirect_fqdn: Option<String>,
    }

    #[derive(serde::Serialize)]
    enum Outcome {
        Ok(SuccessOutput),
        #[serde(rename = "Ok_Cordoned")]
        OkCordoned(SuccessOutput),
        Err {
            status: u16,
            error: String,
        },
    }

    fn run(task_name: &str, shard_data_plane_fqdn: &str) -> Outcome {
        let taken = tokens::now();
        let snapshot = crate::Snapshot::build_fixture(Some(taken));

        // Build and sign a proper JWT token for the request.
        let claims = proto_gazette::Claims {
            iat: taken.timestamp() as u64 - 10, // Before snapshot
            exp: taken.timestamp() as u64 + 100,
            cap: proto_flow::capability::AUTHORIZE,
            iss: shard_data_plane_fqdn.to_string(),
            sel: proto_gazette::LabelSelector::default(),
            sub: task_name.to_string(),
        };

        // Use key3 for fqdn2
        let key = if shard_data_plane_fqdn == "fqdn2" {
            "key3"
        } else {
            "key1"
        };
        let token = tokens::jwt::sign(
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(key.as_bytes()),
        )
        .unwrap();

        match evaluate_authorization(&snapshot, task_name, shard_data_plane_fqdn, &token) {
            Ok((cordon_at, (ops_logs_journal, ops_stats_journal, redirect_fqdn))) => {
                let output = SuccessOutput {
                    ops_logs_journal,
                    ops_stats_journal,
                    redirect_fqdn,
                };

                if cordon_at.is_some() {
                    Outcome::OkCordoned(output)
                } else {
                    Outcome::Ok(output)
                }
            }
            Err(status) => Outcome::Err {
                status: tokens::rest::grpc_status_code_to_http(status.code()),
                error: status.message().to_string(),
            },
        }
    }
}
