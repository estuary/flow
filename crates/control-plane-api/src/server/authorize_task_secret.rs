type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::DecryptAuthorization;

/// Authorizes a task to decrypt a secret, returning its wrapped document.
///
/// The direct caller is config-encryption, which forwards a request body
/// token from a data-plane reactor caller, and holds the KMS grant that can
/// decrypt our successful result.
///
/// The request token is signed by the issuing data-plane. Its `sel` names the
/// task type, task name, and requested secret under `estuary.dev/task-type`,
/// `estuary.dev/task-name`, and `estuary.dev/secret-name`, and optionally the
/// connector image repository under `estuary.dev/image-repo`. Its `sub` is
/// advisory and is not inspected as part of authorization.
///
/// Three things are verified:
///
///  * The secret is admitted by the sibling rule -- a task may use the secrets
///    which sit beside it, `dirname(secret) == dirname(task)` -- or by the
///    image rule, where a reserved component delimits the exact attested
///    repository: `<prefix>/connectors/<repo>/<leaf>`.
///  * Residency: a task the Snapshot knows of must live in the issuing
///    data-plane, so that a plane asks only for the secrets of tasks it runs.
///  * Type: a task the Snapshot knows of must have the claimed task type.
///
/// If a task is not in the Snapshot, then the requesting data-plane must be
/// admitted by the longest-prefix storage mapping covering the task name -- the
/// same mapping which decides where the task could be created. A task which
/// hasn't been published still needs its secrets: a Discover or Validate is
/// where an OAuth flow runs, and it cannot authenticate without the connector's
/// client secret.
///
/// SAFETY:
///
/// Both of the names this route decides over are the issuing plane's
/// assertions: the task it claims to run, and the image repository it claims to
/// be running. Neither is re-derived here. What bounds that trust differs
/// between the two rules:
///
/// The sibling rule is bounded by the task name. Residency holds a plane to the
/// tasks it actually runs, and a task the Snapshot doesn't know falls back to
/// the storage mapping covering its name, so a plane reaches sibling secrets
/// only under prefixes whose tenants opted into it. The set of reachable names
/// is tied to a tenant's own configuration, which is an argument for least-
/// privilege when configuring storage mappings.
///
/// The image rule is deliberately not bounded that way, because the secret it
/// admits belongs to the image vendor: a vendor image's OAuth client secret
/// is named for the repository, is shared by every task of that connector,
/// and is a sibling of none of them. A plane running the image reaches the
/// image's secrets, whichever tenant asked.
///
/// Were a data-plane to be compromised, the exposure risk is that it can read
/// any image-owned secret by naming its repository. The mitigation is that such
/// secrets are limited (by policy) to vendor OAuth client credentials, meaning
/// that they do not yield refresh or access tokens that other tasks of that
/// connector hold. What it does widen is the phishing surface against that
/// connector's users (accepted, and unavoidable).
#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_task_secret(
    mut env: crate::Envelope,
    super::Request(Request { token }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::ApiError> {
    let unverified =
        super::parse_untrusted_data_plane_claims(&token, proto_flow::capability::AUTHORIZE)?;

    // Use the `iat` claim to establish the logical start of the request,
    // rounded up to the next second (as it was round down when encoded).
    env.started = tokens::DateTime::from_timestamp_secs(1 + unverified.claims().iat as i64)
        .unwrap_or_default();

    let secret_name = labels::expect_one(unverified.claims().sel.include(), labels::SECRET_NAME)
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
    let task_name = labels::expect_one(unverified.claims().sel.include(), labels::TASK_NAME)
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
    let task_type = labels::expect_one(unverified.claims().sel.include(), labels::TASK_TYPE)
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;

    let image_repo = match labels::values(unverified.claims().sel.include(), labels::IMAGE_REPO) {
        [] => None,
        [image_repo] => Some(image_repo.value.as_str()),
        many => {
            return Err(tonic::Status::invalid_argument(format!(
                "expected at most one {} label (got {many:?})",
                labels::IMAGE_REPO
            ))
            .into());
        }
    };
    let secret_name = models::Name::new(secret_name);
    let task_name = models::Name::new(task_name);

    // `err` renders as ": {name} doesn't match pattern ...", restating the name.
    if let Err(err) = validator::Validate::validate(&secret_name) {
        return Err(tonic::Status::invalid_argument(format!("invalid secret name{err}")).into());
    }
    if let Err(err) = validator::Validate::validate(&task_name) {
        return Err(tonic::Status::invalid_argument(format!("invalid task name{err}")).into());
    }

    let task_type = match task_type {
        labels::TASK_TYPE_CAPTURE => models::CatalogType::Capture,
        labels::TASK_TYPE_DERIVATION => models::CatalogType::Collection,
        labels::TASK_TYPE_MATERIALIZATION => models::CatalogType::Materialization,
        other => {
            return Err(
                tonic::Status::invalid_argument(format!("invalid task type '{other}'")).into(),
            );
        }
    };

    crate::secrets::validate_task_access(
        &task_name,
        &secret_name,
        crate::secrets::TaskSecretAccess::Read { image_repo },
    )
    .map_err(|err| tonic::Status::permission_denied(err.to_string()))?;

    let policy_result = super::task_residency::evaluate_task_residency(
        env.snapshot(),
        &task_name,
        task_type,
        &unverified.claims().iss,
        &token,
        super::task_residency::UnknownTaskPolicy::AllowStorageMapping,
    );

    let residency = match env.authorization_outcome(policy_result).await {
        Ok((_expiry, residency)) => residency,
        // Retries are a 200 bearing `retryMillis`, as with the sibling
        // authorize routes: the client is config-encryption, not a browser
        // that would follow a 307.
        Err(crate::ApiError::AuthZRetry(retry)) => {
            return Ok(axum::Json(Response {
                retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                ..Default::default()
            }));
        }
        Err(err @ crate::ApiError::Status(_)) => return Err(err),
    };

    super::task_residency::enforce_storage_mapping(&env.pg_pool, &task_name, residency).await?;

    // Absence is terminal: unlike a grant, a secret is read at its current
    // value, so a later read cannot turn this answer around.
    let Some(secret) = crate::secrets::fetch(&env.pg_pool, &secret_name).await? else {
        return Err(
            tonic::Status::not_found(format!("secret '{secret_name}' does not exist")).into(),
        );
    };

    Ok(axum::Json(Response {
        document: Some(secret.document),
        secret_id: Some(secret.secret_id),
        retry_millis: 0,
    }))
}

#[cfg(test)]
mod tests {
    use crate::test_server;

    /// An image a connector may attest, and a secret named for it under the
    /// image rule. `aliceCo` runs the tasks; `acmeVendor` publishes the
    /// connector and owns the OAuth client secret every task of it uses.
    const IMAGE_REPO: &str = "ghcr.io/acmeVendor/source-widgets";
    const IMAGE_SECRET: &str =
        "acmeVendor/oauth/connectors/ghcr.io/acmeVendor/source-widgets/oauth-client";

    /// Drive the route as config-encryption would, asking for `secret_names` on
    /// behalf of a task of `task_type` and `task_name`, issued by the data-plane
    /// of FQDN `iss`. Secrets and images are plural only so that a selector
    /// bearing none, or two, is expressible -- no reactor mints one, and the
    /// route must refuse it.
    async fn post(
        server: &test_server::TestServer,
        iss: &str,
        task_type: &str,
        task_name: &str,
        secret_names: &[&str],
        image_repos: &[&str],
    ) -> String {
        let token = test_server::data_plane_token(
            iss,
            test_server::FIXTURE_HMAC_KEY,
            proto_flow::capability::AUTHORIZE,
            secret_names
                .iter()
                .map(|name| (labels::SECRET_NAME, *name))
                .chain(image_repos.iter().map(|name| (labels::IMAGE_REPO, *name)))
                .chain([
                    (labels::TASK_NAME, task_name),
                    (labels::TASK_TYPE, task_type),
                ]),
            task_name,
        );

        let response = reqwest::Client::new()
            .post(
                server
                    .base_url()
                    .join("/authorize/task/decrypt-secret")
                    .unwrap(),
            )
            .json(&serde_json::json!({ "token": token }))
            .send()
            .await
            .unwrap();

        test_server::summarize_secret(response).await
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../fixtures",
            scripts("data_planes", "alice", "secrets", "storage_mappings")
        )
    )]
    async fn test_task_decrypt_secret(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;

        let outcomes = [
            // The sibling rule, over both of the fixture's secrets, so that a
            // 200 is pinned to the secret actually asked for and not merely to
            // the one secret a passing case could ever return. The rule itself
            // is `secrets::test_task_access`; these rows prove the route
            // applies it, and that a success carries the right document.
            (
                "sibling/in",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &["aliceCo/in/token"],
                    &[],
                )
                .await,
            ),
            (
                "sibling/out",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_MATERIALIZATION,
                    "aliceCo/out/materialize-bar",
                    &["aliceCo/out/token"],
                    &[],
                )
                .await,
            ),
            (
                "not-sibling",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_MATERIALIZATION,
                    "aliceCo/out/materialize-bar",
                    &["aliceCo/in/token"],
                    &[],
                )
                .await,
            ),
            (
                "absent",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &["aliceCo/in/nonexistent"],
                    &[],
                )
                .await,
            ),
            // Selector shape, which only this route can refuse: the names it
            // reads must parse, and there must be exactly one secret and at
            // most one image.
            (
                "malformed-secret",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &["aliceCo/bad name"],
                    &[],
                )
                .await,
            ),
            (
                "malformed-task",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/bad name",
                    &["aliceCo/in/token"],
                    &[],
                )
                .await,
            ),
            (
                "bad-task-type",
                post(
                    &server,
                    "dp.one",
                    "dekaf",
                    "aliceCo/in/capture-foo",
                    &["aliceCo/in/token"],
                    &[],
                )
                .await,
            ),
            (
                "no-selector",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &[],
                    &[],
                )
                .await,
            ),
            (
                "two-selectors",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &["aliceCo/in/token", "aliceCo/out/token"],
                    &[],
                )
                .await,
            ),
            (
                "two-images",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &[IMAGE_SECRET],
                    &[IMAGE_REPO, "ghcr.io/acmeVendor/source-gadgets"],
                )
                .await,
            ),
            // Residency: this task runs in dp.one, and an attested image is not
            // a way around that.
            (
                "wrong-plane",
                post(
                    &server,
                    "dp.two",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &[IMAGE_SECRET],
                    &[IMAGE_REPO],
                )
                .await,
            ),
            // Tasks absent from the DB defer to storage mappings, which
            // `task_residency` covers in full. These rows prove the route wires
            // that check in, and that an admitted request still 404s on a
            // secret which isn't there rather than disclosing more.
            (
                "unknown/admitted",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-new",
                    &["aliceCo/in/token"],
                    &[],
                )
                .await,
            ),
            (
                "unknown/admitted-absent",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-new",
                    &["aliceCo/in/nonexistent"],
                    &[],
                )
                .await,
            ),
            (
                "unknown/other-plane",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "bobCo/capture-new",
                    &["bobCo/token"],
                    &[],
                )
                .await,
            ),
            // The image rule end to end: the vendor's secret belongs to no
            // prefix aliceCo holds, and reaches the task only because the
            // reactor attested the repository which names it.
            (
                "image-rule",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &[IMAGE_SECRET],
                    &[IMAGE_REPO],
                )
                .await,
            ),
            (
                "image-rule/unattested",
                post(
                    &server,
                    "dp.one",
                    labels::TASK_TYPE_CAPTURE,
                    "aliceCo/in/capture-foo",
                    &[IMAGE_SECRET],
                    &[],
                )
                .await,
            ),
        ];

        insta::assert_debug_snapshot!(outcomes);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(
            path = "../fixtures",
            scripts("data_planes", "alice", "secrets", "storage_mappings")
        )
    )]
    async fn test_task_decrypt_secret_retries_stale_snapshot(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // A gated snapshot serves an empty Snapshot first, which knows neither
        // the issuing data-plane nor the task -- the shape of a control plane
        // that hasn't caught up. It must surface as a retry, not a denial.
        // (The provisional failure is the data-plane token verification, since
        // that's the first check an empty Snapshot fails.)
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        let outcome = post(
            &server,
            "dp.one",
            labels::TASK_TYPE_CAPTURE,
            "aliceCo/in/capture-foo",
            &["aliceCo/in/token"],
            &[],
        )
        .await;

        assert!(
            outcome.starts_with("200 retryMillis=") && !outcome.ends_with("=0"),
            "expected a non-zero retry, got {outcome}"
        );
    }
}
