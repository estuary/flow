type Request = models::authorizations::UserDecryptAuthorizationRequest;
type Response = models::authorizations::DecryptAuthorization;

/// Authorizes a user to decrypt a secret, returning its wrapped document.
///
/// The direct caller is config-encryption, which forwards a request and bearer
/// token from the actual caller, and holds the KMS grant that can decrypt our
/// successful result.
///
/// As this route is proxied, we cannot return a 307 redirect as other routes do,
/// and we require that the caller provide the `started` parameter to anchor the
/// operation start time across retries.
#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_user_secret(
    env: crate::Envelope,
    axum::extract::Query(Request { name }): axum::extract::Query<Request>,
) -> Result<axum::Json<Response>, crate::ApiError> {
    // `err` renders as ": {name} doesn't match pattern ...", restating the name.
    if let Err(err) = validator::Validate::validate(&name) {
        return Err(tonic::Status::invalid_argument(format!("invalid secret name{err}")).into());
    }
    if !env.started_set {
        return Err(tonic::Status::invalid_argument("`started` is a required parameter").into());
    }

    let policy_result = super::evaluate_names_authorization(
        env.snapshot(),
        env.claims()?,
        models::authz::Capability::DecryptSecret,
        [name.as_str()],
    );

    match env.authorization_outcome(policy_result).await {
        Ok((_expiry, ())) => (),
        Err(crate::ApiError::AuthZRetry(retry)) => {
            return Ok(axum::Json(Response {
                retry_millis: (retry.retry_after - retry.failed).num_milliseconds() as u64,
                ..Default::default()
            }));
        }
        Err(err) => return Err(err),
    }

    Ok(axum::Json(super::fetch_secret(&env.pg_pool, &name).await?))
}

#[cfg(test)]
mod tests {
    use crate::test_server;

    /// Callers are `Option` so that `ANON` -- a request with no bearer token at
    /// all -- is just another one of them.
    const ALICE: Option<uuid::Uuid> = Some(uuid::Uuid::from_u128(
        0x11111111_1111_1111_1111_111111111111,
    ));
    const BOB: Option<uuid::Uuid> = Some(uuid::Uuid::from_u128(
        0x22222222_2222_2222_2222_222222222222,
    ));
    const ANON: Option<uuid::Uuid> = None;

    /// Issue a request bearing `query`, as `user`. Redirects are not followed,
    /// so a 307 is observable rather than being transparently retried -- which
    /// is what a default reqwest client (config-encryption's, presumably) would
    /// do with one.
    async fn get(
        server: &test_server::TestServer,
        user: Option<uuid::Uuid>,
        query: &str,
    ) -> reqwest::Response {
        let request = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
            .get(
                server
                    .base_url()
                    .join(&format!("/authorize/user/decrypt-secret?{query}"))
                    .unwrap(),
            );

        match user {
            Some(user) => {
                request.bearer_auth(server.make_access_token(user, Some("user@example.com")))
            }
            None => request,
        }
        .send()
        .await
        .unwrap()
    }

    /// Drive the route for `name`, threading the `started` the route requires
    /// because it answers a retry with a body rather than a 307 that would
    /// otherwise mint one.
    async fn run(server: &test_server::TestServer, user: Option<uuid::Uuid>, name: &str) -> String {
        let started = tokens::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        summarize(get(server, user, &format!("name={name}&started={started}")).await).await
    }

    /// Reduce a response to a snapshot-able summary.
    async fn summarize(response: reqwest::Response) -> String {
        let status = response.status().as_u16();
        let body = response.text().await.unwrap();

        // Errors are a bare status message rather than JSON, so only a success
        // is parsed. The wrapped document itself is elided: its content is
        // opaque ciphertext, and only its presence is what a route decides.
        if status != 200 {
            return format!("{status} {body}");
        }
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();

        match body.get("document") {
            Some(_) => format!(
                "200 secretId={}",
                body["secretId"].as_str().unwrap_or("<missing>")
            ),
            None => format!("200 retryMillis={}", body["retryMillis"]),
        }
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice", "secrets"))
    )]
    async fn test_user_decrypt_secret(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;

        // Alice is an admin of aliceCo/, and so holds `DecryptSecret` there.
        // Bob holds nothing. The `id` of aliceCo/in/token is fixed by the
        // fixture, so it can be asserted verbatim.
        let outcomes = [
            ("alice/found", run(&server, ALICE, "aliceCo/in/token").await),
            (
                "alice/absent",
                run(&server, ALICE, "aliceCo/in/nonexistent").await,
            ),
            (
                "alice/other-tenant",
                run(&server, ALICE, "bobCo/token").await,
            ),
            ("bob/denied", run(&server, BOB, "aliceCo/in/token").await),
            (
                "alice/malformed",
                run(&server, ALICE, "aliceCo/bad name").await,
            ),
            // An anonymous caller is turned away before any secret is named,
            // on a route whose whole job is disclosing one.
            ("anon/denied", run(&server, ANON, "aliceCo/in/token").await),
            // Without `started`, a retry would be answered by telling the client
            // to repeat a request whose start time it re-mints each time.
            (
                "alice/no-started",
                summarize(get(&server, ALICE, "name=aliceCo/in/token").await).await,
            ),
        ];

        insta::assert_debug_snapshot!(outcomes, @r#"
        [
            (
                "alice/found",
                "200 secretId=1111111111111111",
            ),
            (
                "alice/absent",
                "404 secret 'aliceCo/in/nonexistent' does not exist",
            ),
            (
                "alice/other-tenant",
                "403 user@example.com is not authorized to access prefix or name 'bobCo/token' with required capability DecryptSecret",
            ),
            (
                "bob/denied",
                "403 user@example.com is not authorized to access prefix or name 'aliceCo/in/token' with required capability DecryptSecret",
            ),
            (
                "alice/malformed",
                "400 invalid secret name: aliceCo/bad name doesn't match pattern [\\p{Letter}\\p{Number}\\-_\\.]+(/[\\p{Letter}\\p{Number}\\-_\\.]+)* (unmatched portion is:  name)",
            ),
            (
                "anon/denied",
                "401 This is an authenticated API but the request is missing a required Authorization: Bearer token",
            ),
            (
                "alice/no-started",
                "400 `started` is a required parameter",
            ),
        ]
        "#);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice", "secrets"))
    )]
    async fn test_user_decrypt_secret_retries_stale_snapshot(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // A gated snapshot serves an empty Snapshot first, so Alice's grants
        // are not yet visible and the denial is provisional rather than
        // terminal.
        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;

        // The retry is a 200 bearing `retryMillis`, which config-encryption
        // passes back to its own caller, rather than the 307 that other user
        // routes answer with and a proxied caller could not act on. The client
        // is expected to come back with the same `started` it sent here, so
        // that its next denial can be terminal.
        let started = tokens::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let response = get(
            &server,
            ALICE,
            &format!("name=aliceCo/in/token&started={started}"),
        )
        .await;

        assert_eq!(response.status().as_u16(), 200);
        let body: serde_json::Value = response.json().await.unwrap();
        assert!(body.get("document").is_none());
        assert!(
            body["retryMillis"].as_u64().unwrap() > 0,
            "expected a non-zero retry, got {body}"
        );
    }
}
