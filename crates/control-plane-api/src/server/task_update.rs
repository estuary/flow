//! Data-plane routes by which a *task* updates its own stored state: the two
//! halves of connector-driven credential rotation.
//!
//! - `/task/set-secret` stores a wrapped secret document, so a connector can
//!   rotate a credential it manages without a human in the loop.
//! - `/task/update-config` records a new endpoint configuration and `secrets`
//!   stanza, which is how a connector migrates a legacy `sops` configuration
//!   onto first-class secrets, or repoints one afterwards.
//!
//! Both are authorized by a data-plane-signed token bearing
//! `AUTHORIZE | TASK_UPDATE`, which the reactor mints for the connector and
//! injects as `FLOW_ROTATION_TOKEN`. That capability reaches these two routes
//! and no others: `/authorize/task` enumerates the capabilities it serves
//! exactly, and `/authorize/task/decrypt-secret` requires a `secret-name`
//! label which these selectors do not carry.

/// Claims common to both routes, read from the token's selector.
struct TaskClaims<'a> {
    task_name: models::Name,
    task_type: models::CatalogType,
    /// Build the connector was started under, from `estuary.dev/build`.
    /// Present on Open sessions only, and required by `update-config`.
    build: Option<&'a str>,
    data_plane_fqdn: &'a str,
}

/// Parse and shape-check the selector of a `TASK_UPDATE` token. The signature
/// is verified later, against the Snapshot's keys for the issuing plane.
fn parse_task_claims(
    unverified: &tokens::jwt::Unverified<proto_gazette::Claims>,
) -> tonic::Result<TaskClaims<'_>> {
    let claims = unverified.claims();

    if claims.cap & proto_flow::capability::TASK_UPDATE == 0 {
        return Err(tonic::Status::unauthenticated(
            "missing required TASK_UPDATE capability",
        ));
    }

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

/// Decide whether a task may update its own state, returning the data-plane
/// name to verify during the storage-mapping fallback check, or `None` if
/// residency is settled outright.
///
/// `require_known` is set by `update-config`, which has nothing to write for a
/// task that does not exist: the fallback exists for Discover and Validate of
/// *novel* tasks, which have secrets to read but no live spec to update.
fn evaluate_authorization<'s>(
    snapshot: &'s crate::Snapshot,
    claims: &TaskClaims<'_>,
    token: &str,
    require_known: bool,
) -> crate::AuthZResult<Option<&'s str>> {
    let Some(task_data_plane) = snapshot.verify_data_plane_token(claims.data_plane_fqdn, token)?
    else {
        return Err(tonic::Status::unauthenticated(
            "no data-plane keys validated against the token signature",
        ));
    };
    let (task_name, fqdn) = (&claims.task_name, claims.data_plane_fqdn);

    let mapping_fallback = if let Some(task) = snapshot.task_by_catalog_name(task_name) {
        // Residency: a task we know of must run in the issuing plane, or bust.
        if task.data_plane_id != task_data_plane.control_id {
            return Err(tonic::Status::permission_denied(format!(
                "task '{task_name}' does not run in data-plane {fqdn}"
            )));
        }
        if task.spec_type != claims.task_type {
            return Err(tonic::Status::permission_denied(format!(
                "task '{task_name}' is a {}, not a {}",
                task.spec_type, claims.task_type,
            )));
        }
        None
    } else if require_known {
        return Err(tonic::Status::failed_precondition(format!(
            "task '{task_name}' is not known to the control-plane"
        )));
    } else {
        // Unknown tasks (Validate / Discover) check the storage mapping.
        Some(task_data_plane.data_plane_name.as_str())
    };

    Ok((
        snapshot.cordon_at(task_name, task_data_plane),
        mapping_fallback,
    ))
}

/// Store a wrapped secret document on behalf of a task.
///
/// The sibling rule alone applies: a connector rotates only the secrets its
/// task owns, never one it reaches under the image rule. Those belong to the
/// image's publisher, are shared by every task of that connector, and are
/// updated by hand.
///
/// What a "set" means -- the embedded name must match, a document older than
/// the stored one is refused, and an identical one mints no new id -- is
/// [`crate::secrets::set`], shared with the GraphQL `setSecret` mutation.
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

    let unverified = super::parse_untrusted_data_plane_claims(&token)?;
    env.started = tokens::DateTime::from_timestamp_secs(1 + unverified.claims().iat as i64)
        .unwrap_or_default();

    let claims = parse_task_claims(&unverified)?;
    let task_name = &claims.task_name;

    if super::parent_prefix(name.as_str()) != super::parent_prefix(task_name.as_str()) {
        return Err(tonic::Status::permission_denied(format!(
            "task '{task_name}' may only set secrets which are its siblings, and '{name}' is not one"
        ))
        .into());
    }

    // A novel task -- a Discover or Validate which has not published -- may
    // still create its secrets: an admitted plane already holds full read
    // authority under that prefix, so the write adds no disclosure.
    let policy_result = evaluate_authorization(env.snapshot(), &claims, &token, false);

    let fallback_check_data_plane = match env.authorization_outcome(policy_result).await {
        Ok((_expiry, admit_data_plane)) => admit_data_plane,
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

    if let Some(data_plane_name) = fallback_check_data_plane
        && !super::storage_mapping_admits(&env.pg_pool, task_name.as_str(), data_plane_name).await?
    {
        return Err(tonic::Status::permission_denied(format!(
            "task '{task_name}' is not known, and its storage mapping does not admit data-plane {data_plane_name}",
        ))
        .into());
    }

    let last_modified = crate::secrets::validate_document(name.as_str(), &document)
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
///
/// There is deliberately no plaintext-secret guard here: publication's
/// `walk_plaintext` is the enforcement point, and the window during which a
/// mistaken plaintext token sits in `config_updates` is accepted.
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

    let unverified = super::parse_untrusted_data_plane_claims(&token)?;
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

    // `estuary.dev/build` rides on Open sessions only, and it's what ties the
    // proposed configuration to a model. Without it there is nothing to check.
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

    // Unlike `set-secret`, there's nothing to write for a task which doesn't
    // exist: the storage-mapping fallback has no live spec to update.
    let policy_result = evaluate_authorization(env.snapshot(), &claims, &token, true);

    match env.authorization_outcome(policy_result).await {
        Ok((_expiry, _fallback)) => (),
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

    // One statement, so that a publication racing this request cannot slip
    // between the build check and the write.
    let row = sqlx::query!(
        r#"
        WITH spec AS (
            SELECT last_build_id FROM live_specs
            WHERE catalog_name = $1::text::catalog_name
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

    /// A stand-in for what config-encryption's `/secret/encrypt` returns. Only
    /// the fields the control plane reads are real, and `value` is ciphertext
    /// nothing in this crate can decrypt.
    fn wrapped(name: &str, ciphertext: &str, last_modified: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "value": format!("ENC[AES256_GCM,data:{ciphertext},type:str]"),
            "sops": {
                "lastmodified": last_modified,
                "mac": "ENC[AES256_GCM,data:bWFj]",
                "encrypted_regex": "^value$",
                "version": "3.11.0",
            },
        })
    }

    /// Mint the token a reactor signs for a connector's rotation calls.
    /// `build` is absent on the unary request kinds, which have no session.
    fn token(
        iss: &str,
        task_type: &str,
        task_name: &str,
        build: Option<models::Id>,
        cap: u32,
    ) -> String {
        let now = tokens::now().timestamp() as u64;

        let claims = proto_gazette::Claims {
            iat: now,
            exp: now + 100,
            cap,
            iss: iss.to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some({
                    let set = labels::build_set([
                        (labels::TASK_NAME, task_name),
                        (labels::TASK_TYPE, task_type),
                    ]);
                    match build {
                        Some(build) => labels::add_value(set, labels::BUILD, &format!("{build}")),
                        None => set,
                    }
                }),
                exclude: None,
            },
            sub: task_name.to_string(),
        };
        // "c2VjcmV0" of the data_planes fixture, base64-decoded.
        tokens::jwt::sign(&claims, &tokens::jwt::EncodingKey::from_secret(b"secret")).unwrap()
    }

    /// The capability a reactor mints for these routes.
    const CAP: u32 = proto_flow::capability::AUTHORIZE | proto_flow::capability::TASK_UPDATE;

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
                "token": token(iss, task_type, task_name, None, CAP),
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
            // A wrapped document is bound to its name and cannot be stored
            // under another, which is what keeps one from being cloned.
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
            // Only siblings. An image-rule secret belongs to the image's
            // publisher and is shared by every task of that connector: a
            // connector may read it, but never write it.
            (
                "image-rule-refused",
                set_secret(
                    &server,
                    &mut ids,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    "acmeVendor/oauth/ghcr.io/acmeVendor/source-widgets/oauth-client",
                    wrapped(
                        "acmeVendor/oauth/ghcr.io/acmeVendor/source-widgets/oauth-client",
                        "c3RvbGVu",
                        "2026-02-01T00:00:00Z",
                    ),
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
                            None,
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
        let stale = models::Id::new([9, 9, 9, 9, 9, 9, 9, 9]);

        let update = |iss: &'static str,
                      task_type: &'static str,
                      task_name: &'static str,
                      build: Option<models::Id>,
                      body: serde_json::Value| {
            let server = &server;
            async move {
                let mut body = body;
                body["token"] = serde_json::json!(token(iss, task_type, task_name, build, CAP));
                post(server, "/task/update-config", body).await
            }
        };

        let config = serde_json::json!({
            "address": "db.example.com",
            "credentials": {},
        });
        let secrets = serde_json::json!({
            "aliceCo/in/oauth-tokens": "/credentials",
            "acmeVendor/oauth/ghcr.io/acmeVendor/source-widgets/oauth-client": "/credentials",
        });

        let outcomes = [
            (
                "matching-build",
                update(
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    Some(build),
                    serde_json::json!({"config": config, "secrets": secrets}),
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
                    Some(stale),
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
                    None,
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
                    Some(live_build(&pool, "aliceCo/data/foo").await),
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
                    Some(build),
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
                    Some(build),
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
                            Some(build),
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
            .map(|(label, outcome)| (label, outcome.replace(&build.to_string(), "<live-build>")))
            .collect();

        insta::assert_debug_snapshot!(outcomes);

        // Exactly one row, shaped as a connector-emitted `configUpdate` is,
        // and pinned to the build which proposed it. `ts` and `build` vary per
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
        assert_eq!(row[0].build, Some(build));

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
              "acmeVendor/oauth/ghcr.io/acmeVendor/source-widgets/oauth-client": "/credentials",
              "aliceCo/in/oauth-tokens": "/credentials"
            }
          },
          "message": "endpoint configuration updated by the connector",
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
}
