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
#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_task(
    mut env: crate::Envelope,
    super::Request(Request { token }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::ApiError> {
    let unverified = super::parse_untrusted_data_plane_claims(&token)?;

    // Use the `iat` claim to establish the logical start of the request,
    // rounded up to the next second (as it was round down when encoded).
    env.started = tokens::DateTime::from_timestamp_secs(1 + unverified.claims().iat as i64)
        .unwrap_or_default();

    let journal_name_or_prefix = labels::expect_one(unverified.claims().sel.include(), "name")
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?
        .to_owned();

    // Strip the request's AUTHORIZE capability from response claims.
    let cap = unverified.claims().cap & !proto_flow::capability::AUTHORIZE;

    // Validate and match the requested capabilities to a corresponding role.
    // NOTE: Because we pass through the claims after validating them here,
    // we need to explicitly enumerate and exactly match every case, as just
    // checking that the requested capability contains a particular grant isn't enough.
    // For example, we wouldn't want to allow a request for `REPLICATE` just
    // because it also requests `READ`.
    let required_role = match cap {
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
            return Err(tonic::Status::invalid_argument(format!(
                "capability {cap} cannot be authorized by this service"
            ))
            .into());
        }
    };

    let policy_result = evaluate_authorization(
        env.snapshot(),
        env.started,
        &unverified.claims().sub,
        &unverified.claims().iss,
        &token,
        &journal_name_or_prefix,
        required_role,
    );

    // Legacy: return a custom 200 response for client-side retries.
    let (expiry, (encoding_key, data_plane_fqdn, broker_address, found)) =
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

    // Build and sign response claims.
    let mut response_claims = proto_gazette::Claims {
        cap,
        exp: expiry.timestamp() as u64,
        iat: env.started.timestamp() as u64,
        iss: data_plane_fqdn,
        sel: unverified.claims().sel.clone(),
        sub: unverified.claims().sub.clone(),
    };

    if !found {
        // Return a "black hole" response by extending the request claims
        // with a label selector that never matches anything. This:
        // - Causes List RPCs to succeed, but return nothing:
        //   This avoids failing a sourcing task, on the presumption
        //   that it's just awaiting an update from the control plane
        //   and should be allowed to gracefully finish its other work.
        // - Causes Append RPCs to error with status JOURNAL_NOT_FOUND.
        //   This allows for correct handling of recovered ACK intents
        //   destined for a journal which has since been deleted.
        // - Causes an Apply RPC, used to create partitions, to fail.
        //
        // Note that we're directing the task back to it's own data-plane,
        // since we have no idea what data-plane the collection might have
        // once lived in, as we couldn't find it.
        response_claims.sel.include = Some(labels::add_value(
            response_claims.sel.include.unwrap_or_default(),
            "estuary.dev/match-nothing",
            "1",
        ));
    }
    let token = tokens::jwt::sign(&response_claims, &encoding_key)?;

    Ok(axum::Json(Response {
        broker_address,
        token,
        ..Default::default()
    }))
}

fn evaluate_authorization(
    snapshot: &crate::Snapshot,
    started: tokens::DateTime,
    shard_id: &str,
    shard_data_plane_fqdn: &str,
    token: &str,
    journal_name_or_prefix: &str,
    required_role: models::Capability,
) -> crate::AuthZResult<(tokens::jwt::EncodingKey, String, String, bool)> {
    // Map `claims.iss`, a data-plane FQDN, into its token-verified data-plane.
    let Some(task_data_plane) = snapshot.verify_data_plane_token(shard_data_plane_fqdn, token)?
    else {
        return Err(tonic::Status::unauthenticated(
            "no data-plane keys validated against the token signature",
        ));
    };

    // Map `claims.sub`, a Shard ID, into a task running in `task_data_plane`.
    let Some(task) = snapshot
        .task_by_shard_id(shard_id)
        .filter(|task| task.data_plane_id == task_data_plane.control_id)
    else {
        return Err(tonic::Status::failed_precondition(format!(
            "task shard {shard_id} within data-plane {shard_data_plane_fqdn} is not known"
        )));
    };

    // Map a required `name` journal label selector into its collection.
    let collection = snapshot.collection_by_journal_name(journal_name_or_prefix);

    let (found, collection_data_plane, is_ops) = if let Some(collection) = collection {
        let Some(collection_data_plane) =
            snapshot.data_planes.get_by_key(&collection.data_plane_id)
        else {
            return Err(tonic::Status::internal(format!(
                "collection {} data-plane {} not found",
                collection.collection_name, collection.data_plane_id,
            )));
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
            &snapshot.role_grants,
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
        return Err(tonic::Status::permission_denied(format!(
            "task shard {shard_id} is not authorized to {journal_name_or_prefix} for {required_role:?}"
        )));
    }

    let Some(encoding_key) = collection_data_plane.hmac_keys.first() else {
        return Err(tonic::Status::internal(format!(
            "collection data-plane {} has no configured HMAC keys",
            collection_data_plane.data_plane_name
        )));
    };
    let encoding_key =
        tokens::jwt::EncodingKey::from_secret(&tokens::jwt::parse_base64(encoding_key)?);

    let cordon_at = match (
        snapshot.cordon_at(&task.task_name, task_data_plane),
        collection.and_then(|collection| {
            snapshot.cordon_at(&collection.collection_name, collection_data_plane)
        }),
    ) {
        (Some(l), Some(r)) => Some(l.min(r)),
        (Some(i), None) | (None, Some(i)) => Some(i),
        (None, None) => None,
    };

    // If !found, then the task request is authorized but its
    // `journal_name_or_prefix` doesn't map to a known collection because:
    //  1) The collection has been reset or deleted and the client is out of date.
    //  3) The collection has been reset and *we* are out of date.
    //
    // For case (1), we return a "black hole" response which is technically valid
    // but matches no journals. This response avoids breaking the task while it
    // restarts due to a presumed forthcoming build update. However, we can only
    // return this response once we've ruled out case (2). Ergo, return an error
    // to trigger a snapshot refresh and retry, and we'll re-evaluate then.
    if !found && !snapshot.taken_after(started) {
        return Err(tonic::Status::unavailable(format!(
            "{journal_name_or_prefix} does not map to a known collection"
        )));
    }

    Ok((
        cordon_at,
        (
            encoding_key,
            collection_data_plane.data_plane_fqdn.clone(),
            collection_data_plane.broker_address.clone(),
            found,
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collection_success() {
        let outcome = run(
            "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000",
            "fqdn1",
            "acmeCo/pineapples/1122334455667788/pivot=00",
            models::Capability::Write,
            0, // iat offset - before snapshot
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": {
            "broker_address": "broker.1",
            "data_plane_fqdn": "fqdn1",
            "found": true
          }
        }
        "###);
    }

    #[test]
    fn test_ops_success() {
        let outcome = run(
            "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000",
            "fqdn1",
            "ops/tasks/public/plane-one/logs/1122334455667788/kind=capture/name=acmeCo%2Fsource-pineapple/pivot=00",
            models::Capability::Write,
            0, // iat offset - before snapshot
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": {
            "broker_address": "broker.1",
            "data_plane_fqdn": "fqdn1",
            "found": true
          }
        }
        "###);
    }

    #[test]
    fn test_not_authorized() {
        let outcome = run(
            "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000",
            "fqdn1",
            "bobCo/bananas/1122334455667788/pivot=00",
            models::Capability::Write,
            0,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "task shard capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000 is not authorized to bobCo/bananas/1122334455667788/pivot=00 for Write"
          }
        }
        "###);
    }

    #[test]
    fn test_cordon_collection() {
        let outcome = run(
            "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000",
            "fqdn1",
            "acmeCo/bananas/1122334455667788/pivot=00",
            models::Capability::Write,
            0,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok_Cordoned": {
            "broker_address": "broker.1",
            "data_plane_fqdn": "fqdn1",
            "found": true
          }
        }
        "###);
    }

    #[test]
    fn test_cordon_task() {
        let outcome = run(
            "capture/acmeCo/source-banana/0011223344556677/00000000-00000000",
            "fqdn1",
            "acmeCo/pineapples/1122334455667788/pivot=00",
            models::Capability::Write,
            0,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok_Cordoned": {
            "broker_address": "broker.1",
            "data_plane_fqdn": "fqdn1",
            "found": true
          }
        }
        "###);
    }

    #[test]
    fn test_black_hole_after_snapshot() {
        // Request iat is after current snapshot - trigger retry for possible stale snapshot.
        let outcome = run(
            "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000",
            "fqdn1",
            "acmeCo/pineapples/88667755330099/pivot=00", // Not found - wrong generation ID.
            models::Capability::Write,
            2, // iat offset = 2 seconds after taken, which is > snapshot.taken (taken + 1)
        );

        insta::assert_json_snapshot!(outcome, @r#"
        {
          "Err": {
            "status": 503,
            "error": "acmeCo/pineapples/88667755330099/pivot=00 does not map to a known collection"
          }
        }
        "#);
    }

    #[test]
    fn test_black_hole_before_snapshot() {
        // Request iat is before current snapshot - we can confirm collection doesn't exist.
        let outcome = run(
            "capture/acmeCo/source-pineapple/0011223344556677/00000000-00000000",
            "fqdn1",
            "acmeCo/pineapples/88667755330099/pivot=00", // Not found - wrong generation ID.
            models::Capability::Write,
            0, // iat offset = before snapshot
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": {
            "broker_address": "broker.1",
            "data_plane_fqdn": "fqdn1",
            "found": false
          }
        }
        "###);
    }

    #[derive(serde::Serialize)]
    struct SuccessOutput {
        broker_address: String,
        data_plane_fqdn: String,
        found: bool,
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

    fn run(
        shard_id: &str,
        shard_data_plane_fqdn: &str,
        journal_name_or_prefix: &str,
        required_role: models::Capability,
        iat_offset_seconds: i64,
    ) -> Outcome {
        let taken = tokens::now();
        // Build snapshot with taken time 1 second in the future so that
        // iat_offset=0 is "before snapshot" and iat_offset=1+ is "same or after snapshot"
        let snapshot = crate::Snapshot::build_fixture(Some(taken + chrono::Duration::seconds(1)));

        // Build and sign a proper JWT token for the request.
        let claims = proto_gazette::Claims {
            iat: (taken.timestamp() + iat_offset_seconds) as u64,
            exp: (taken.timestamp() + 100) as u64,
            cap: proto_flow::capability::AUTHORIZE | proto_gazette::capability::APPEND,
            iss: shard_data_plane_fqdn.to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([("name", journal_name_or_prefix)])),
                exclude: None,
            },
            sub: shard_id.to_string(),
        };

        let token = tokens::jwt::sign(
            &claims,
            &jsonwebtoken::EncodingKey::from_secret("key1".as_bytes()),
        )
        .unwrap();

        let started =
            chrono::DateTime::from_timestamp(taken.timestamp() + iat_offset_seconds, 0).unwrap();

        match evaluate_authorization(
            &snapshot,
            started,
            shard_id,
            shard_data_plane_fqdn,
            &token,
            journal_name_or_prefix,
            required_role,
        ) {
            Ok((cordon_at, (_key, data_plane_fqdn, broker_address, found))) => {
                let output = SuccessOutput {
                    broker_address,
                    data_plane_fqdn,
                    found,
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

    // Integration tests below use sqlx::test with actual database
    use crate::test_server;
    use flow_client_next as flow_client;

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_task_collection_auth_success(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        tokio::time::pause();

        // Create signed source with data-plane HMAC key.
        // The capture task aliceCo/in/capture-foo has write access to aliceCo/data/
        // via role_grant: aliceCo/in/ -> aliceCo/data/ (write)
        let signed_source = flow_client::workflows::task_collection_auth::new_signed_source(
            "aliceCo/data/foo/gen1234/pivot=00".to_string(), // journal name
            "capture/aliceCo/in/capture-foo/gen5678/00000000-00000000".to_string(), // shard id
            proto_gazette::capability::APPEND,
            "dp.one".to_string(), // data_plane_fqdn from fixture
            tokens::jwt::EncodingKey::from_secret(b"secret"), // HMAC key (c2VjcmV0 decoded)
        );

        let source = flow_client::workflows::TaskCollectionAuth {
            client: server.rest_client(),
            signed_source,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_json_snapshot!(
            refresh.token().result().unwrap(),
            {".token" => "<redacted>"},
            @r###"
            {
              "token": "<redacted>",
              "brokerAddress": "broker.dp.one",
              "retryMillis": 0
            }
            "###,
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_task_collection_auth_failure(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        tokio::time::pause();

        // The capture task aliceCo/in/capture-foo does NOT have access to some/other/collection
        let signed_source = flow_client::workflows::task_collection_auth::new_signed_source(
            "some/other/collection/gen9999/pivot=00".to_string(), // unauthorized journal
            "capture/aliceCo/in/capture-foo/gen5678/00000000-00000000".to_string(), // shard id
            proto_gazette::capability::APPEND,
            "dp.one".to_string(),
            tokens::jwt::EncodingKey::from_secret(b"secret"),
        );

        let source = flow_client::workflows::TaskCollectionAuth {
            client: server.rest_client(),
            signed_source,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_debug_snapshot!(
            refresh.token().result().unwrap_err(),
            @r#"
        Status {
            code: PermissionDenied,
            message: "task shard capture/aliceCo/in/capture-foo/gen5678/00000000-00000000 is not authorized to some/other/collection/gen9999/pivot=00 for Write",
            source: None,
        }
        "#,
        );
    }
}
