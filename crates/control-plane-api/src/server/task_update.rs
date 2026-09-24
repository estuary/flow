//! Data-plane routes by which *tasks* update their configuration or secrets.
//!
//! - `/task/set-secret` stores a wrapped secret document, so a connector can
//!   rotate a credential it manages without a human in the loop.
//! - `/task/update-config` records a new endpoint configuration and `secrets`
//!   stanza.
//!
//! Both are authorized by a data-plane-signed token bearing `TASK_UPDATE`,
//! which the reactor provides to the connector.
//!
//! Residency is decided from the request's Snapshot. A cordon present in that
//! Snapshot is handled through the usual authorization retry path, but the
//! route does not consult current migration state again before committing its
//! write. Cordons quiesce writes; they are not a security boundary that requires
//! this route to linearize with migration. A request admitted by a stale
//! Snapshot may therefore finish after its cordon begins. Once the task moves,
//! a refreshed Snapshot admits its new data-plane and denies its old one.

/// Claims common to both routes, read from the token's selector.
struct TaskClaims<'a> {
    task_name: models::Name,
    task_type: models::CatalogType,
    /// Build the connector was started under, from `estuary.dev/build`.
    /// Required by `update-config`.
    build: Option<&'a str>,
    data_plane_fqdn: &'a str,
}

/// Parse and shape-check the selector of a `TASK_UPDATE` token. The signature
/// is verified later, against the Snapshot's keys for the issuing plane.
fn parse_task_claims(
    unverified: &tokens::jwt::Unverified<proto_gazette::Claims>,
) -> tonic::Result<TaskClaims<'_>> {
    let claims = unverified.claims();

    let task_name = labels::expect_one(claims.sel.include(), labels::TASK_NAME)
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
    let task_type = labels::expect_one(claims.sel.include(), labels::TASK_TYPE)
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;

    let build = match labels::values(claims.sel.include(), labels::BUILD) {
        [] => None,
        [build] => Some(build.value.as_str()),
        many => {
            return Err(tonic::Status::invalid_argument(format!(
                "expected at most one {} label (got {many:?})",
                labels::BUILD
            )));
        }
    };

    let task_name = models::Name::new(task_name);
    // `err` renders as ": {name} doesn't match pattern ...", restating the name.
    if let Err(err) = validator::Validate::validate(&task_name) {
        return Err(tonic::Status::invalid_argument(format!(
            "invalid task name{err}"
        )));
    }

    let task_type = match task_type {
        labels::TASK_TYPE_CAPTURE => models::CatalogType::Capture,
        labels::TASK_TYPE_DERIVATION => models::CatalogType::Collection,
        labels::TASK_TYPE_MATERIALIZATION => models::CatalogType::Materialization,
        other => {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid task type '{other}'"
            )));
        }
    };

    Ok(TaskClaims {
        task_name,
        task_type,
        build,
        data_plane_fqdn: &claims.iss,
    })
}

/// Store a wrapped secret document on behalf of a task.
///
/// The sibling rule alone applies: a connector rotates only the secrets its
/// task owns, never one it reaches under the image rule. Those belong to the
/// image's publisher, are shared by every task of that connector, and are
/// updated by hand.
///
/// A novel task is allowed to set sibling secrets, so long as it's admitted by
/// its storage mapping. This is necessary to support OAuth token rotations
/// that occur during Discover or Validate RPCs (that run as part of task
/// creation workflows).
///
/// This operation runs under a task's authority, not a user's. A task must be
/// able to rotate its own credentials (OAuth). Conceptually, its authority is
/// delegated from the user's original publication authority, which (by
/// construction under the sibling rule) also implied a user capability to set
/// its secrets.
#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env, request), err(Debug, level = tracing::Level::WARN))]
pub async fn task_set_secret(
    mut env: crate::Envelope,
    super::Request(request): super::Request<models::authorizations::TaskSetSecretRequest>,
) -> Result<axum::Json<models::authorizations::TaskSetSecret>, crate::ApiError> {
    let models::authorizations::TaskSetSecretRequest {
        name,
        document,
        token,
    } = request;

    let unverified =
        super::parse_untrusted_data_plane_claims(&token, proto_flow::capability::TASK_UPDATE)?;
    env.started = tokens::DateTime::from_timestamp_secs(1 + unverified.claims().iat as i64)
        .unwrap_or_default();

    let claims = parse_task_claims(&unverified)?;
    let task_name = &claims.task_name;

    crate::secrets::validate_task_access(task_name, &name, crate::secrets::TaskSecretAccess::Set)
        .map_err(|err| tonic::Status::permission_denied(err.to_string()))?;

    let policy_result = super::task_residency::evaluate_task_residency(
        env.snapshot(),
        task_name,
        claims.task_type,
        claims.data_plane_fqdn,
        &token,
        super::task_residency::UnknownTaskPolicy::AllowStorageMapping,
    );

    let residency = match env.authorization_outcome(policy_result).await {
        Ok((_expiry, residency)) => residency,
        // Retries are a 200 bearing `retryMillis`, as with the authorize
        // routes: the caller is a connector, not a browser to redirect.
        Err(crate::ApiError::AuthZRetry(retry)) => {
            return Ok(axum::Json(models::authorizations::TaskSetSecret {
                retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                ..Default::default()
            }));
        }
        Err(err @ crate::ApiError::Status(_)) => return Err(err),
    };

    super::task_residency::enforce_storage_mapping(&env.pg_pool, task_name, residency).await?;

    let last_modified =
        crate::secrets::validate_document(name.as_str(), &document, chrono::Utc::now())
            .map_err(tonic::Status::invalid_argument)?;

    let (secret_id, changed) =
        match crate::secrets::set(&env.pg_pool, name.as_str(), &document, last_modified).await? {
            crate::secrets::SetOutcome::Written(secret_id) => (secret_id, true),
            crate::secrets::SetOutcome::Unchanged(secret_id) => (secret_id, false),
            crate::secrets::SetOutcome::Stale => {
                return Err(tonic::Status::failed_precondition(format!(
                    "the stored secret '{name}' is newer than the provided document; \
                     re-encrypt the value you intend to set"
                ))
                .into());
            }
            crate::secrets::SetOutcome::Conflict => {
                return Err(tonic::Status::aborted(format!(
                    "secret '{name}' was concurrently set by another request; retry"
                ))
                .into());
            }
        };

    tracing::info!(%name, %secret_id, changed, %task_name, "task set secret");

    Ok(axum::Json(models::authorizations::TaskSetSecret {
        secret_id: Some(secret_id),
        changed,
        retry_millis: 0,
    }))
}

/// Record a new endpoint configuration and `secrets` stanza for a task.
///
/// This does *not* publish. It writes one `public.config_updates` row, shaped
/// exactly as a connector-emitted `configUpdate` log would be, and the existing
/// `on_config_update` trigger wakes the task's controller -- which publishes
/// the change and reports the outcome in its status and alerts.
///
/// The token must name the build the connector was started under, and it must
/// still be the task's `last_build_id`. A connector which has fallen a
/// publication behind is proposing a configuration derived from a model that
/// has since changed, so its update is refused rather than allowed to clobber.
#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env, request), err(Debug, level = tracing::Level::WARN))]
pub async fn task_update_config(
    mut env: crate::Envelope,
    super::Request(request): super::Request<models::authorizations::TaskUpdateConfigRequest>,
) -> Result<axum::Json<models::authorizations::TaskUpdateConfig>, crate::ApiError> {
    let models::authorizations::TaskUpdateConfigRequest {
        config,
        secrets,
        message,
        token,
    } = request;

    let unverified =
        super::parse_untrusted_data_plane_claims(&token, proto_flow::capability::TASK_UPDATE)?;
    env.started = tokens::DateTime::from_timestamp_secs(1 + unverified.claims().iat as i64)
        .unwrap_or_default();

    let claims = parse_task_claims(&unverified)?;
    let task_name = &claims.task_name;

    // A derivation's configuration is its module and environment, which the
    // connector does not author. Only captures and materializations rotate.
    if matches!(claims.task_type, models::CatalogType::Collection) {
        return Err(tonic::Status::failed_precondition(format!(
            "derivation '{task_name}' cannot update its configuration"
        ))
        .into());
    }

    // `estuary.dev/build` rides on Open sessions only.
    let Some(build) = claims.build else {
        return Err(tonic::Status::invalid_argument(format!(
            "token is missing the {} label, which a configuration update requires",
            labels::BUILD
        ))
        .into());
    };
    let build: models::Id = build.parse().map_err(|err| {
        tonic::Status::invalid_argument(format!("invalid {} label: {err}", labels::BUILD))
    })?;

    let policy_result = super::task_residency::evaluate_task_residency(
        env.snapshot(),
        task_name,
        claims.task_type,
        claims.data_plane_fqdn,
        &token,
        super::task_residency::UnknownTaskPolicy::RequireKnown,
    );

    match env.authorization_outcome(policy_result).await {
        Ok((_expiry, super::task_residency::TaskResidency::Resident)) => (),
        Ok((_, super::task_residency::TaskResidency::StorageMappingRequired { .. })) => {
            unreachable!("RequireKnown cannot require a storage-mapping check")
        }
        Err(crate::ApiError::AuthZRetry(retry)) => {
            return Ok(axum::Json(models::authorizations::TaskUpdateConfig {
                retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
            }));
        }
        Err(err @ crate::ApiError::Status(_)) => return Err(err),
    }

    // Shaped exactly as a connector-emitted `configUpdate` log, so that the
    // controller and its tests see one kind of row and not two.
    let ts = chrono::Utc::now();
    let event = models::status::connector::ConfigUpdate {
        shard: models::status::ShardRef {
            name: task_name.to_string(),
            key_begin: "00000000".to_string(),
            r_clock_begin: "00000000".to_string(),
            build,
        },
        ts,
        message: message
            .unwrap_or_else(|| "endpoint configuration updated by the connector".to_string()),
        fields: [
            ("eventType".to_string(), serde_json::json!("configUpdate")),
            (
                "eventTarget".to_string(),
                serde_json::json!(task_name.as_str()),
            ),
            ("config".to_string(), config.to_value()),
            ("secrets".to_string(), serde_json::json!(secrets)),
        ]
        .into_iter()
        .collect(),
    };
    let event = serde_json::to_value(&event).expect("ConfigUpdate serializes");

    // Lock the live-spec row for the duration of this statement. Publication's
    // upsert of that row takes the conflicting lock, so whichever operation
    // arrives second observes the first one's build rather than acting on a
    // statement snapshot taken while it waits.
    let row = sqlx::query!(
        r#"
        WITH spec AS MATERIALIZED (
            SELECT last_build_id FROM live_specs
            WHERE catalog_name = $1::text::catalog_name
            FOR UPDATE
        ),
        upserted AS (
            INSERT INTO config_updates (catalog_name, build, ts, flow_document)
            SELECT $1::text::catalog_name, $2::flowid, $3, $4
            WHERE EXISTS (SELECT 1 FROM spec WHERE last_build_id = $2::flowid)
            ON CONFLICT (catalog_name) DO UPDATE SET
                build = EXCLUDED.build,
                ts = EXCLUDED.ts,
                flow_document = EXCLUDED.flow_document
            RETURNING catalog_name
        )
        SELECT
            (SELECT last_build_id FROM spec) AS "live_build: models::Id",
            EXISTS (SELECT 1 FROM upserted) AS "written!: bool"
        "#,
        task_name.as_str(),
        build as models::Id,
        ts,
        event,
    )
    .fetch_one(&env.pg_pool)
    .await?;

    if !row.written {
        return Err(tonic::Status::failed_precondition(match row.live_build {
            Some(live_build) => format!(
                "task '{task_name}' is now running build {live_build}, and this connector \
                 started under build {build}: restart to update its configuration"
            ),
            // The Snapshot knew the task, so the live spec was deleted between
            // that read and this write.
            None => format!("task '{task_name}' is not known to the control-plane"),
        })
        .into());
    }

    tracing::info!(%task_name, %build, secrets = secrets.len(), "task updated its configuration");

    Ok(axum::Json(models::authorizations::TaskUpdateConfig {
        retry_millis: 0,
    }))
}

#[cfg(test)]
mod tests {
    use crate::test_server;
    use test_server::wrapped;

    /// The capability a reactor mints for these routes.
    const CAP: u32 = proto_flow::capability::TASK_UPDATE;

    /// A vendor secret, reachable by the image rule but never writable: it
    /// belongs to the image's publisher and is shared by every task of that
    /// connector.
    const IMAGE_SECRET: &str =
        "acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client";

    /// Mint the token a reactor signs for a connector's rotation calls.
    /// `builds` is plural only so that a token bearing no `estuary.dev/build`
    /// -- the unary request kinds, which have no session -- or two, or one
    /// which doesn't parse, is expressible.
    fn token(iss: &str, task_type: &str, task_name: &str, builds: &[&str], cap: u32) -> String {
        test_server::data_plane_token(
            iss,
            test_server::FIXTURE_HMAC_KEY,
            cap,
            builds.iter().map(|build| (labels::BUILD, *build)).chain([
                (labels::TASK_NAME, task_name),
                (labels::TASK_TYPE, task_type),
            ]),
            task_name,
        )
    }

    async fn post(server: &test_server::TestServer, path: &str, body: serde_json::Value) -> String {
        let response = reqwest::Client::new()
            .post(server.base_url().join(path).unwrap())
            .json(&body)
            .send()
            .await
            .unwrap();

        let status = response.status().as_u16();
        let body = response.text().await.unwrap();

        if status != 200 {
            return format!("{status} {body}");
        }
        format!("200 {body}")
    }

    /// Drive `/task/set-secret`, rendering a success as `changed` plus the
    /// minted `secretId`. Ids are time-generated, so the caller interns each
    /// into `ids` and the snapshot shows its index -- which is what the
    /// interesting property is about: whether two sets minted the same id.
    async fn set_secret(
        server: &test_server::TestServer,
        ids: &mut Vec<models::Id>,
        iss: &str,
        task_type: &str,
        task_name: &str,
        secret_name: &str,
        document: serde_json::Value,
    ) -> String {
        let outcome = post(
            server,
            "/task/set-secret",
            serde_json::json!({
                "name": secret_name,
                "document": document,
                "token": token(iss, task_type, task_name, &[], CAP),
            }),
        )
        .await;

        let Some(body) = outcome.strip_prefix("200 ") else {
            return outcome;
        };
        let body: models::authorizations::TaskSetSecret = serde_json::from_str(body).unwrap();
        let secret_id = body.secret_id.expect("a 200 always mints a secret id");

        let index = match ids.iter().position(|id| *id == secret_id) {
            Some(index) => index,
            None => {
                ids.push(secret_id);
                ids.len() - 1
            }
        };
        format!("200 changed={} secretId=#{index}", body.changed)
    }

    /// The current `last_build_id` of a task, which is what a running
    /// connector's `estuary.dev/build` label carries.
    async fn live_build(pool: &sqlx::PgPool, task_name: &str) -> models::Id {
        sqlx::query_scalar!(
            r#"SELECT last_build_id AS "id: models::Id" FROM live_specs
               WHERE catalog_name = $1::text::catalog_name"#,
            task_name,
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../fixtures",
            scripts("data_planes", "alice", "secrets", "storage_mappings")
        )
    )]
    async fn test_task_set_secret(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;

        // Interned `secretId`s, in first-seen order. See `set_secret`.
        let mut ids = Vec::new();

        let outcomes = [
            // A sibling secret the task already has, rotated to a new value.
            (
                "sibling/rotated",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/token",
                    wrapped("aliceCo/in/token", "cm90YXRlZA==", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            // Re-applying what was just stored is the same entity, and mints
            // no new id.
            (
                "sibling/unchanged",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/token",
                    wrapped("aliceCo/in/token", "cm90YXRlZA==", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            // A document older than the stored one is a stale re-apply.
            (
                "sibling/stale",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/token",
                    wrapped("aliceCo/in/token", "b2xk", "2026-01-01T00:00:00Z"),
                )
                .await,
            ),
            // A secret the task doesn't have yet is created.
            (
                "sibling/created",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/oauth-tokens",
                    wrapped(
                        "aliceCo/in/oauth-tokens",
                        "Z2VuLTE=",
                        "2026-02-01T00:00:00Z",
                    ),
                )
                .await,
            ),
            // The document guard is `secrets::test_validate_document`; this row
            // proves the route applies it and renders it as a 400.
            (
                "wrong-name",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/other",
                    wrapped("aliceCo/in/token", "Y2xvbmU=", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            // Only siblings. An image-rule secret is readable by this task but
            // never writable by it.
            (
                "image-rule-refused",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    IMAGE_SECRET,
                    wrapped(IMAGE_SECRET, "c3RvbGVu", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            (
                "not-sibling",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "aliceCo/out/token",
                    wrapped("aliceCo/out/token", "Y3Jvc3M=", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            // Residency: this task runs in dp.one.
            (
                "wrong-plane",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.two",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/token",
                    wrapped("aliceCo/in/token", "cm90YXRlZA==", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            (
                "type-mismatch",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_MATERIALIZATION,
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/token",
                    wrapped("aliceCo/in/token", "cm90YXRlZA==", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            // A task the Snapshot doesn't know -- a Discover or Validate which
            // hasn't published -- may still create its secrets, because an
            // admitted plane already holds full read authority under the
            // prefix. `bobCo/` is covered by a mapping which admits dp.two only.
            // The mapping rules themselves are `task_residency`'s.
            (
                "unknown/admitted",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-new",
                    "aliceCo/in/discovered",
                    wrapped("aliceCo/in/discovered", "bmV3", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            (
                "unknown/other-plane",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "bobCo/capture-new",
                    "bobCo/token",
                    wrapped("bobCo/token", "Ym9i", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            // Selector shape, read by `parse_task_claims` before anything is
            // authorized. Neither name reaches a policy which could allow it.
            (
                "malformed-task",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/bad name",
                    "aliceCo/in/token",
                    wrapped("aliceCo/in/token", "bm9wZQ==", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            (
                "bad-task-type",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    "dekaf",
                    "aliceCo/in/capture-foo",
                    "aliceCo/in/token",
                    wrapped("aliceCo/in/token", "bm9wZQ==", "2026-02-01T00:00:00Z"),
                )
                .await,
            ),
            // The capability is what separates these routes from a token which
            // may only read: an AUTHORIZE-only bearer cannot write.
            (
                "missing-capability",
                post(
                    &server,
                    "/task/set-secret",
                    serde_json::json!({
                        "name": "aliceCo/in/token",
                        "document": wrapped(
                            "aliceCo/in/token", "bm9wZQ==", "2026-02-01T00:00:00Z"),
                        "token": token(
                            "dp.one",
                            labels::TASK_TYPE_CAPTURE,
                            "aliceCo/in/capture-foo",
                            &[],
                            proto_flow::capability::AUTHORIZE,
                        ),
                    }),
                )
                .await,
            ),
        ];

        insta::assert_debug_snapshot!(outcomes);

        // The rotation actually landed, and the created secret exists.
        let stored = sqlx::query!(
            r#"SELECT catalog_name AS "name!: String", document::text AS "document!: String"
               FROM internal.secrets
               WHERE catalog_name IN ('aliceCo/in/token', 'aliceCo/in/oauth-tokens',
                                      'aliceCo/in/discovered')
               ORDER BY catalog_name"#,
        )
        .fetch_all(&pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            let document: serde_json::Value = serde_json::from_str(&row.document).unwrap();
            (row.name, document["value"].as_str().unwrap().to_string())
        })
        .collect::<Vec<_>>();

        insta::assert_debug_snapshot!(stored, @r###"
        [
            (
                "aliceCo/in/discovered",
                "ENC[AES256_GCM,data:bmV3,type:str]",
            ),
            (
                "aliceCo/in/oauth-tokens",
                "ENC[AES256_GCM,data:Z2VuLTE=,type:str]",
            ),
            (
                "aliceCo/in/token",
                "ENC[AES256_GCM,data:cm90YXRlZA==,type:str]",
            ),
        ]
        "###);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../fixtures",
            scripts("data_planes", "alice", "secrets", "storage_mappings")
        )
    )]
    async fn test_task_update_config(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;

        let build = live_build(&pool, "aliceCo/in/capture-foo").await;
        let build = build.to_string();
        let stale = models::Id::new([9, 9, 9, 9, 9, 9, 9, 9]).to_string();

        // A success is rendered as the `message` which landed, so that both the
        // default and an override are visible in the transcript rather than
        // only in whichever row wrote the surviving row below.
        let update = |iss: &'static str,
                      task_type: &'static str,
                      task_name: &'static str,
                      builds: Vec<String>,
                      body: serde_json::Value| {
            let server = &server;
            let pool = &pool;
            async move {
                let builds: Vec<&str> = builds.iter().map(String::as_str).collect();
                let mut body = body;
                body["token"] = serde_json::json!(token(iss, task_type, task_name, &builds, CAP));

                let outcome = post(server, "/task/update-config", body).await;
                if !outcome.starts_with("200 ") {
                    return outcome;
                }
                let message: String = sqlx::query_scalar!(
                    r#"SELECT flow_document->>'message' AS "message!: String"
                       FROM config_updates WHERE catalog_name = $1::text::catalog_name"#,
                    task_name,
                )
                .fetch_one(pool)
                .await
                .unwrap();
                format!("200 message={message:?}")
            }
        };

        let config = serde_json::json!({
            "address": "db.example.com",
            "credentials": {},
        });
        // The stanza is recorded verbatim, including a vendor secret this task
        // could never *set*: publication's `walk_plaintext` and secret
        // resolution are the enforcement points, not this route.
        let secrets = serde_json::json!({
            "aliceCo/in/oauth-tokens": "/credentials",
            IMAGE_SECRET: "/credentials",
        });

        let outcomes = [
            (
                "matching-build",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    vec![build.clone()],
                    serde_json::json!({"config": config, "secrets": secrets}),
                )
                .await,
            ),
            // `message` is the connector's own description of what it did, and
            // rides through into the `configUpdate` the controller sees.
            (
                "message-override",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    vec![build.clone()],
                    serde_json::json!({
                        "config": config,
                        "secrets": secrets,
                        "message": "refreshed an expiring OAuth token",
                    }),
                )
                .await,
            ),
            // A connector a publication behind is proposing a configuration
            // derived from a model which has since changed.
            (
                "stale-build",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    vec![stale.clone()],
                    serde_json::json!({"config": config, "secrets": {}}),
                )
                .await,
            ),
            // Unary request kinds carry no build, and have no session whose
            // configuration could be updated.
            (
                "missing-build",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    vec![],
                    serde_json::json!({"config": config, "secrets": {}}),
                )
                .await,
            ),
            (
                "two-builds",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    // Two *distinct* ids: a label set dedupes an identical
                    // value, so a repeated one is a single label.
                    vec![build.clone(), stale.clone()],
                    serde_json::json!({"config": config, "secrets": {}}),
                )
                .await,
            ),
            (
                "unparseable-build",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    vec!["not-an-id".to_string()],
                    serde_json::json!({"config": config, "secrets": {}}),
                )
                .await,
            ),
            // A derivation's configuration is its module and environment,
            // which the connector does not author.
            (
                "derivation",
                update(
                    "dp.one",
                    labels::TASK_TYPE_DERIVATION,
                    "aliceCo/data/foo",
                    vec![live_build(&pool, "aliceCo/data/foo").await.to_string()],
                    serde_json::json!({"config": config, "secrets": {}}),
                )
                .await,
            ),
            (
                "wrong-plane",
                update(
                    "dp.two",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    vec![build.clone()],
                    serde_json::json!({"config": config, "secrets": {}}),
                )
                .await,
            ),
            // Unlike `set-secret`, there is nothing to update for a task which
            // was never published: the storage-mapping fallback doesn't apply.
            (
                "unknown-task",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-new",
                    vec![build.clone()],
                    serde_json::json!({"config": config, "secrets": {}}),
                )
                .await,
            ),
            (
                "missing-capability",
                post(
                    &server,
                    "/task/update-config",
                    serde_json::json!({
                        "config": config,
                        "token": token(
                            "dp.one",
                            labels::TASK_TYPE_CAPTURE,
                            "aliceCo/in/capture-foo",
                            &[build.as_str()],
                            proto_flow::capability::AUTHORIZE,
                        ),
                    }),
                )
                .await,
            ),
        ];

        // The live build id is time-generated, so it's elided where an error
        // quotes it back.
        let outcomes: Vec<_> = outcomes
            .into_iter()
            .map(|(label, outcome)| (label, outcome.replace(&build, "<live-build>")))
            .collect();

        insta::assert_debug_snapshot!(outcomes);

        // Exactly one row, shaped as a connector-emitted `configUpdate` is, and
        // pinned to the build which proposed it -- the last successful update,
        // since the route upserts on the task name. `ts` and `build` vary per
        // run, so they're elided rather than snapshotted.
        let row = sqlx::query!(
            r#"SELECT
                 catalog_name AS "name!: String",
                 build AS "build: models::Id",
                 flow_document::text AS "document!: String"
               FROM config_updates"#,
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert_eq!(row.len(), 1);
        assert_eq!(row[0].name, "aliceCo/in/capture-foo");
        assert_eq!(row[0].build.map(|id| id.to_string()), Some(build));

        let mut document: serde_json::Value = serde_json::from_str(&row[0].document).unwrap();
        document["ts"] = serde_json::json!("<elided>");
        document["shard"]["build"] = serde_json::json!("<elided>");

        insta::assert_json_snapshot!(document, @r###"
        {
          "fields": {
            "config": {
              "address": "db.example.com",
              "credentials": {}
            },
            "eventTarget": "aliceCo/in/capture-foo",
            "eventType": "configUpdate",
            "secrets": {
              "acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client": "/credentials",
              "aliceCo/in/oauth-tokens": "/credentials"
            }
          },
          "message": "refreshed an expiring OAuth token",
          "shard": {
            "build": "<elided>",
            "keyBegin": "00000000",
            "name": "aliceCo/in/capture-foo",
            "rClockBegin": "00000000"
          },
          "ts": "<elided>"
        }
        "###);
    }

    /// Publication updates `live_specs` with an upsert, which locks the row.
    /// Hold that lock with its new build still uncommitted, then begin an update
    /// from the old connector. The route must wait and, after publication
    /// commits, observe the new build rather than write from its stale snapshot.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../fixtures",
            scripts("data_planes", "alice", "secrets", "storage_mappings")
        )
    )]
    async fn test_task_update_serializes_with_publication(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        const TASK: &str = "aliceCo/in/capture-foo";

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let old_build = live_build(&pool, TASK).await.to_string();
        let new_build = models::Id::new([0xff; 8]);

        let mut publication = pool.begin().await.unwrap();
        sqlx::query(
            "UPDATE live_specs SET last_build_id = $2::flowid \
             WHERE catalog_name = $1::text::catalog_name",
        )
        .bind(TASK)
        .bind(new_build)
        .execute(&mut *publication)
        .await
        .unwrap();

        let outcome = post(
            &server,
            "/task/update-config",
            serde_json::json!({
                "config": {"address": "db.example.com"},
                "secrets": {},
                "token": token(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    TASK,
                    &[old_build.as_str()],
                    CAP,
                ),
            }),
        );
        tokio::pin!(outcome);

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), outcome.as_mut())
                .await
                .is_err(),
            "task update did not wait for publication's live-spec lock"
        );
        publication.commit().await.unwrap();

        assert_eq!(
            outcome
                .await
                .replace(&old_build, "<old-build>")
                .replace(&new_build.to_string(), "<new-build>"),
            "412 task 'aliceCo/in/capture-foo' is now running build <new-build>, and this connector started under build <old-build>: restart to update its configuration"
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM config_updates \
             WHERE catalog_name = $1::text::catalog_name",
        )
        .bind(TASK)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "the stale request must not write an update");
    }

    /// Both routes answer a Snapshot which hasn't caught up with a 200 bearing
    /// `retryMillis`, rather than the 307 a browser could follow or a denial
    /// the connector would treat as terminal.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../fixtures",
            scripts("data_planes", "alice", "secrets", "storage_mappings")
        )
    )]
    async fn test_task_update_retries_stale_snapshot(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let build = live_build(&pool, "aliceCo/in/capture-foo")
            .await
            .to_string();

        // A gated snapshot serves an empty Snapshot first, which knows neither
        // the issuing data-plane nor the task. The provisional failure is the
        // data-plane token verification, since that's the first check an empty
        // Snapshot fails.
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let outcome = post(
            &server,
            "/task/set-secret",
            serde_json::json!({
                "name": "aliceCo/in/token",
                "document": wrapped("aliceCo/in/token", "cmV0cnk=", "2026-02-01T00:00:00Z"),
                "token": token(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &[],
                    CAP,
                ),
            }),
        )
        .await;

        let body: models::authorizations::TaskSetSecret =
            serde_json::from_str(outcome.strip_prefix("200 ").expect("a retry is a 200")).unwrap();
        assert!(body.secret_id.is_none(), "a retry discloses nothing");
        assert!(
            body.retry_millis > 0,
            "expected a non-zero retry, got {body:?}"
        );

        // A second server, because the gate is single-shot: the first request
        // consumed it, and this route must be told to retry in its own right.
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let outcome = post(
            &server,
            "/task/update-config",
            serde_json::json!({
                "config": {"address": "db.example.com"},
                "secrets": {},
                "token": token(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &[build.as_str()],
                    CAP,
                ),
            }),
        )
        .await;

        let body: models::authorizations::TaskUpdateConfig =
            serde_json::from_str(outcome.strip_prefix("200 ").expect("a retry is a 200")).unwrap();
        assert!(
            body.retry_millis > 0,
            "expected a non-zero retry, got {body:?}"
        );
    }
}
