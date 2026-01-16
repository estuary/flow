type Request = models::authorizations::UserTaskAuthorizationRequest;
type Response = models::authorizations::UserTaskAuthorization;

#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_user_task(
    mut env: crate::Envelope,
    super::Request(Request {
        task,
        capability,
        started_unix,
    }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::server::error::ApiError> {
    // Legacy: if `started_unix` is set, then use as the logical request start
    // rounded up to the next second (as it was round down when encoded).
    if started_unix != 0 {
        env.started =
            tokens::DateTime::from_timestamp_secs(1 + started_unix as i64).unwrap_or_default();
    }

    let policy_result = evaluate_authorization(env.snapshot(), env.claims()?, &task, capability);

    // Legacy: if `started_unix` was set then use a custom 200 response for client-side retries.
    let (
        expiry,
        (
            encoding_key,
            mut broker_claims,
            broker_address,
            ops_logs_journal,
            ops_stats_journal,
            mut reactor_claims,
            reactor_address,
            shard_id_prefix,
        ),
    ) = match env.authorization_outcome(policy_result).await {
        Ok(ok) => ok,
        Err(crate::ApiError::AuthZRetry(retry)) if started_unix != 0 => {
            return Ok(axum::Json(Response {
                retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                ..Default::default()
            }));
        }
        Err(err) => return Err(err),
    };

    broker_claims.exp = expiry.timestamp() as u64;
    broker_claims.iat = env.started.timestamp() as u64;
    reactor_claims.exp = expiry.timestamp() as u64;
    reactor_claims.iat = env.started.timestamp() as u64;

    let broker_token = tokens::jwt::sign(&broker_claims, &encoding_key)?;
    let reactor_token = tokens::jwt::sign(&reactor_claims, &encoding_key)?;

    Ok(axum::Json(Response {
        broker_token,
        broker_address,
        reactor_token,
        ops_logs_journal,
        ops_stats_journal,
        reactor_address,
        shard_id_prefix,
        retry_millis: 0,
    }))
}

fn evaluate_authorization(
    snapshot: &crate::Snapshot,
    claims: &crate::ControlClaims,
    task_name: &models::Name,
    capability: models::Capability,
) -> tonic::Result<(
    Option<chrono::DateTime<chrono::Utc>>,
    (
        tokens::jwt::EncodingKey,
        proto_gazette::Claims, // Broker claims.
        String,                // Broker address.
        String,                // ops logs journal
        String,                // opts stats journal
        proto_gazette::Claims, // Reactor claims.
        String,                // Reactor address.
        String,                // Shard ID prefix.
    ),
)> {
    let models::authorizations::ControlClaims {
        sub: user_id,
        email: user_email,
        ..
    } = claims;
    let user_email = user_email.as_ref().map(String::as_str).unwrap_or("user");

    if !tables::UserGrant::is_authorized(
        &snapshot.role_grants,
        &snapshot.user_grants,
        *user_id,
        task_name,
        capability,
    ) {
        return Err(tonic::Status::permission_denied(format!(
            "{user_email} is not authorized to {task_name} for {capability:?}",
        )));
    }

    // For admin capability, require that the user has a transitive role grant to estuary_support/
    if capability == models::Capability::Admin {
        let has_support_access = tables::UserGrant::is_authorized(
            &snapshot.role_grants,
            &snapshot.user_grants,
            *user_id,
            "estuary_support/",
            models::Capability::Admin,
        );

        if !has_support_access {
            return Err(tonic::Status::permission_denied(format!(
                "{user_email} is not authorized to {task_name} for Admin capability (requires estuary_support/ grant)",
            )));
        }
    }

    let Some(task) = snapshot.task_by_catalog_name(task_name) else {
        return Err(tonic::Status::not_found(format!(
            "task {task_name} is not known"
        )));
    };
    let Some(data_plane) = snapshot.data_planes.get_by_key(&task.data_plane_id) else {
        return Err(tonic::Status::internal(format!(
            "task data-plane {} not found",
            task.data_plane_id
        )));
    };
    let Some(encoding_key) = data_plane.hmac_keys.first() else {
        return Err(tonic::Status::internal(format!(
            "task data-plane {} has no configured HMAC keys",
            data_plane.data_plane_name
        )));
    };
    let encoding_key =
        tokens::jwt::EncodingKey::from_secret(&tokens::jwt::parse_base64(encoding_key)?);

    let (Some(ops_logs), Some(ops_stats)) = (
        snapshot.collection_by_catalog_name(&data_plane.ops_logs_name),
        snapshot.collection_by_catalog_name(&data_plane.ops_stats_name),
    ) else {
        return Err(tonic::Status::internal(format!(
            "couldn't resolve data-plane {} ops collections",
            task.data_plane_id
        )));
    };

    let ops_suffix = super::ops_suffix(task);
    let ops_logs_journal = format!("{}{}", ops_logs.journal_template_name, &ops_suffix[1..]);
    let ops_stats_journal = format!("{}{}", ops_stats.journal_template_name, &ops_suffix[1..]);

    let broker_claims = proto_gazette::Claims {
        cap: proto_gazette::capability::LIST | proto_gazette::capability::READ,
        exp: 0, // Filled later.
        iat: 0, // Filled later.
        iss: data_plane.data_plane_fqdn.clone(),
        sub: user_id.to_string(),
        sel: proto_gazette::broker::LabelSelector {
            include: Some(labels::build_set([
                ("name", ops_logs_journal.as_str()),
                ("name", ops_stats_journal.as_str()),
            ])),
            exclude: None,
        },
    };

    let reactor_claims = proto_gazette::Claims {
        cap: super::map_capability_to_gazette(capability) | proto_flow::capability::NETWORK_PROXY,
        exp: 0, // Filled later.
        iat: 0, // Filled later.
        iss: data_plane.data_plane_fqdn.clone(),
        sub: user_id.to_string(),
        sel: proto_gazette::broker::LabelSelector {
            include: Some(labels::build_set([(
                "id:prefix",
                task.shard_template_id.as_str(),
            )])),
            exclude: None,
        },
    };

    Ok((
        snapshot.cordon_at(&task.task_name, data_plane),
        (
            encoding_key,
            broker_claims,
            data_plane.broker_address.clone(),
            ops_logs_journal,
            ops_stats_journal,
            reactor_claims,
            data_plane.reactor_address.clone(),
            task.shard_template_id.clone(),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/anvils/materialize-orange"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            {
              "cap": 10,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00"
                    },
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00"
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            },
            "reactor.2",
            {
              "cap": 262154,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "id",
                      "value": "materialization/bobCo/anvils/materialize-orange/0011223344556677/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            },
            "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fanvils%2Fmaterialize-orange/pivot=00",
            "materialization/bobCo/anvils/materialize-orange/0011223344556677/"
          ]
        }
        "###);
    }

    #[test]
    fn test_not_authorized() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("acmeCo/other/thing"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to acmeCo/other/thing for Read"
          }
        }
        "###);
    }

    #[test]
    fn test_capability_too_high() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/anvils/materialize-orange"),
            models::Capability::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/anvils/materialize-orange for Admin"
          }
        }
        "###);
    }

    #[test]
    fn test_not_found() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/widgets/not/found"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 404,
            "error": "task bobCo/widgets/not/found is not known"
          }
        }
        "###);
    }

    #[test]
    fn test_cordon() {
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/widgets/materialize-mango"),
            models::Capability::Read,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok_Cordoned": [
            "broker.2",
            {
              "cap": 10,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fwidgets%2Fmaterialize-mango/pivot=00"
                    },
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fwidgets%2Fmaterialize-mango/pivot=00"
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            },
            "reactor.2",
            {
              "cap": 262154,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "id",
                      "value": "materialization/bobCo/widgets/materialize-mango/0011223344556677/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "20202020-2020-2020-2020-202020202020"
            },
            "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=bobCo%2Fwidgets%2Fmaterialize-mango/pivot=00",
            "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=bobCo%2Fwidgets%2Fmaterialize-mango/pivot=00",
            "materialization/bobCo/widgets/materialize-mango/0011223344556677/"
          ]
        }
        "###);
    }

    #[test]
    fn test_bob_cannot_get_admin_even_with_admin_grant() {
        // bob@bob has admin capability on bobCo/tires/ but lacks estuary_support/
        // We need to add a task under bobCo/tires/ to the fixture for this test
        let outcome = run(
            uuid::Uuid::from_bytes([32; 16]),
            Some("bob@bob".to_string()),
            models::Name::new("bobCo/tires/materialize-wheels"),
            models::Capability::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Err": {
            "status": 403,
            "error": "bob@bob is not authorized to bobCo/tires/materialize-wheels for Admin capability (requires estuary_support/ grant)"
          }
        }
        "###);
    }

    #[test]
    fn test_admin_with_estuary_support_grant() {
        // alice@alice has estuary_support/ grant in the fixture, so admin should succeed
        let outcome = run(
            uuid::Uuid::from_bytes([64; 16]),
            Some("alice@alice".to_string()),
            models::Name::new("aliceCo/wonderland/materialize-tea"),
            models::Capability::Admin,
        );

        insta::assert_json_snapshot!(outcome, @r###"
        {
          "Ok": [
            "broker.2",
            {
              "cap": 10,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00"
                    },
                    {
                      "name": "name",
                      "value": "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00"
                    }
                  ]
                }
              },
              "sub": "40404040-4040-4040-4040-404040404040"
            },
            "reactor.2",
            {
              "cap": 262174,
              "exp": 0,
              "iat": 0,
              "iss": "fqdn2",
              "sel": {
                "include": {
                  "labels": [
                    {
                      "name": "id",
                      "value": "materialization/aliceCo/wonderland/materialize-tea/0011223344556677/",
                      "prefix": true
                    }
                  ]
                }
              },
              "sub": "40404040-4040-4040-4040-404040404040"
            },
            "ops/tasks/public/plane-two/logs/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00",
            "ops/tasks/public/plane-two/stats/1122334455667788/kind=materialization/name=aliceCo%2Fwonderland%2Fmaterialize-tea/pivot=00",
            "materialization/aliceCo/wonderland/materialize-tea/0011223344556677/"
          ]
        }
        "###);
    }

    type SuccessOutput = (
        String,                // broker_address
        proto_gazette::Claims, // broker_claims
        String,                // reactor_address
        proto_gazette::Claims, // reactor_claims
        String,                // ops_logs_journal
        String,                // ops_stats_journal
        String,                // shard_id_prefix
    );

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
        user_id: uuid::Uuid,
        email: Option<String>,
        task: models::Name,
        capability: models::Capability,
    ) -> Outcome {
        let snapshot = crate::Snapshot::build_fixture(None);
        let claims = models::authorizations::ControlClaims {
            aud: "authenticated".to_string(),
            iat: 0,
            exp: 0,
            sub: user_id,
            role: "authenticated".to_string(),
            email,
        };

        match evaluate_authorization(&snapshot, &claims, &task, capability) {
            Ok((
                cordon_at,
                (
                    _key,
                    mut broker_claims,
                    broker_address,
                    ops_logs_journal,
                    ops_stats_journal,
                    mut reactor_claims,
                    reactor_address,
                    shard_id_prefix,
                ),
            )) => {
                // Zero out timestamps for stable snapshots.
                broker_claims.iat = 0;
                broker_claims.exp = 0;
                reactor_claims.iat = 0;
                reactor_claims.exp = 0;

                let output = (
                    broker_address,
                    broker_claims,
                    reactor_address,
                    reactor_claims,
                    ops_logs_journal,
                    ops_stats_journal,
                    shard_id_prefix,
                );

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
    async fn test_user_task_auth_success(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let user_tokens = server.make_fixed_user_tokens(uuid::Uuid::from_bytes([0x11; 16]), None);
        tokio::time::pause();

        let source = flow_client::workflows::UserTaskAuth {
            client: server.rest_client(),
            user_tokens: user_tokens.clone(),
            task: models::Name::new("aliceCo/in/capture-foo"),
            capability: models::Capability::Write,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_json_snapshot!(
            refresh.token().result().unwrap(),
            {".brokerToken" => "<redacted>", ".reactorToken" => "<redacted>"},
            @r###"
            {
              "brokerAddress": "broker.dp.one",
              "brokerToken": "<redacted>",
              "opsLogsJournal": "ops/tasks/public/one/logs/gen1234/kind=capture/name=aliceCo%2Fin%2Fcapture-foo/pivot=00",
              "opsStatsJournal": "ops/tasks/public/one/stats/gen1234/kind=capture/name=aliceCo%2Fin%2Fcapture-foo/pivot=00",
              "reactorAddress": "reactor.dp.one",
              "reactorToken": "<redacted>",
              "retryMillis": 0,
              "shardIdPrefix": "capture/aliceCo/in/capture-foo/gen5678/"
            }
            "###,
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_user_task_auth_failure(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let user_tokens = server.make_fixed_user_tokens(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.com"),
        );
        tokio::time::pause();

        let source = flow_client::workflows::UserTaskAuth {
            client: server.rest_client(),
            user_tokens: user_tokens.clone(),
            task: models::Name::new("Some/Other/Task"),
            capability: models::Capability::Write,
        };
        let refresh = tokens::watch(source).ready_owned().await;

        insta::assert_debug_snapshot!(
            refresh.token().result().unwrap_err(),
            @r#"
        Status {
            code: PermissionDenied,
            message: "alice@example.com is not authorized to Some/Other/Task for Write",
            source: None,
        }
        "#,
        );
    }
}
