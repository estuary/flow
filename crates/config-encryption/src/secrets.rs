use super::{Error, Keychain, invoke_sops};
use std::sync::Arc;

/// Maximum size of a secret value accepted by `/secret/encrypt`.
/// A secret is a credential -- a password, a token, a service-account document
/// -- and not a place to stash bulk data.
pub const MAX_SECRET_BYTES: usize = 64 * 1024;

/// App is the state of the secret encryption and decryption routes.
pub struct App {
    /// Keychain which wraps secret documents. Its decrypt grant is held by this
    /// service alone: unlike the legacy keychain, no data-plane can unwrap it.
    keychain: Keychain,
    /// Base URL of the control-plane API, which authorizes decryptions.
    control_plane_url: url::Url,
    client: reqwest::Client,
}

impl App {
    pub fn new(keychain: Keychain, mut control_plane_url: url::Url) -> Self {
        // `Url::join` replaces the final path segment unless the base is
        // slash-terminated, which would silently drop a base path prefix.
        if !control_plane_url.path().ends_with('/') {
            let path = format!("{}/", control_plane_url.path());
            control_plane_url.set_path(&path);
        }

        Self {
            keychain,
            control_plane_url,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("failed to build reqwest client"),
        }
    }
}

/// Router of the secret encryption and decryption routes.
pub fn router(app: Arc<App>) -> axum::Router {
    axum::Router::new()
        .route(
            "/secret/encrypt",
            axum::routing::post(encrypt_secret)
                .layer(axum::extract::DefaultBodyLimit::max(MAX_SECRET_BYTES)),
        )
        .route("/secret/decrypt", axum::routing::post(decrypt_secret))
        .with_state(app)
}

/// Query parameters of `/secret/encrypt`.
#[derive(Debug, serde::Deserialize)]
pub struct EncryptQuery {
    /// Catalog name of the secret being wrapped.
    pub name: models::Name,
}

/// Query parameters of `/secret/decrypt`.
#[derive(Debug, serde::Deserialize)]
pub struct DecryptQuery {
    /// Catalog name of the secret being unwrapped.
    pub name: models::Name,
    /// Logical start of the decrypt operation, held constant across its
    /// retries. It's forwarded verbatim to the user authorize route, which
    /// owns both its format and the fact that it's required there.
    #[serde(default)]
    pub started: Option<String>,
}

/// Wrap a secret value into its sops document.
///
/// This route is unauthenticated. Wrapping discloses nothing -- it only
/// consumes a value the caller already holds -- and it is deliberately
/// separable from setting the secret, which is where authority is enforced.
#[axum::debug_handler(state=Arc<App>)]
#[tracing::instrument(skip(app, body), err(Debug, level = tracing::Level::WARN))]
pub async fn encrypt_secret(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Query(EncryptQuery { name }): axum::extract::Query<EncryptQuery>,
    body: axum::body::Bytes,
) -> Result<axum::Json<Box<serde_json::value::RawValue>>, Error> {
    validate_name(&name)?;

    let value: &serde_json::value::RawValue =
        serde_json::from_slice(&body).map_err(Error::SecretValue)?;

    Ok(axum::Json(wrap(&app.keychain, &name, value).await?))
}

/// Unwrap the sops document of a secret, for a caller the control-plane
/// authorizes to have it.
///
/// The bearer token is the caller's, and is not ours to interpret beyond
/// choosing which authorize route can verify it. We hold the KMS decrypt grant
/// which that route's answer is addressed to.
///
/// `name` is the secret the caller asked for, and is checked against the
/// document actually returned. On the user path it's also the subject of the
/// authorization; on the task path it is not -- the task route reads the name
/// from the token's own `sel` -- so a `name` disagreeing with the token is not
/// ignored, but fails closed as a `NameMismatch`.
#[axum::debug_handler(state=Arc<App>)]
#[tracing::instrument(skip(app, headers), err(Debug, level = tracing::Level::WARN))]
pub async fn decrypt_secret(
    axum::extract::State(app): axum::extract::State<Arc<App>>,
    axum::extract::Query(DecryptQuery { name, started }): axum::extract::Query<DecryptQuery>,
    headers: axum::http::HeaderMap,
) -> Result<axum::response::Response, Error> {
    validate_name(&name)?;
    let token = bearer_token(&headers)?;

    let authorized = authorize(&app, &name, started.as_deref(), token).await?;

    // A server-directed retry passes through as our own response body. This
    // service never sleeps and never retries: the client owns retry policy,
    // and only it knows the deadline of the overall operation.
    let response = match authorized.document {
        None => models::authorizations::SecretDecryption {
            retry_millis: authorized.retry_millis,
            ..Default::default()
        },
        Some(document) => models::authorizations::SecretDecryption {
            value: Some(unwrap(&name, document.get()).await?.into()),
            secret_id: authorized.secret_id,
            retry_millis: 0,
        },
    };

    use axum::response::IntoResponse;
    Ok((
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        axum::Json(response),
    )
        .into_response())
}

/// Ask the control-plane whether the bearer of `token` may decrypt `name`,
/// and if so obtain its wrapped document.
///
/// The two authorize routes take their subject differently -- a data-plane
/// token is POSTed as a body, while a user's is forwarded as the Bearer it
/// already is -- so we route on the shape of the token's unverified claims.
/// Misclassification is harmless: the route we pick verifies the token it's
/// given, and rejects one it cannot.
async fn authorize(
    app: &App,
    name: &models::Name,
    started: Option<&str>,
    token: &str,
) -> Result<models::authorizations::DecryptAuthorization, Error> {
    /// Claims skimmed only to route the token. A data-plane token carries the
    /// gazette `cap` bitmask and `sel` label selector, and a control-plane user
    /// token carries neither.
    #[derive(serde::Deserialize)]
    struct ClaimsShape {
        #[serde(default)]
        cap: Option<u32>,
        #[serde(default)]
        sel: Option<serde::de::IgnoredAny>,
    }
    let claims = tokens::jwt::parse_unverified::<ClaimsShape>(token.as_bytes())
        .map_err(Error::BearerClaims)?;

    let request = if claims.claims().cap.is_some() || claims.claims().sel.is_some() {
        // The task route reads the secret name from the token's own `sel`, and
        // its `started` from the token's `iat`, so neither travels alongside.
        let url = join(&app.control_plane_url, "authorize/task/decrypt-secret");

        app.client
            .post(url)
            .json(&models::authorizations::TaskAuthorizationRequest {
                token: token.to_string(),
            })
    } else {
        let mut url = join(&app.control_plane_url, "authorize/user/decrypt-secret");
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("name", name.as_str());

            if let Some(started) = started {
                query.append_pair("started", started);
            }
        }
        app.client.get(url).bearer_auth(token)
    };

    let response = request
        .send()
        .await
        .map_err(Error::ControlPlaneUnreachable)?;

    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(Error::ControlPlaneUnreachable)?;

    if !status.is_success() {
        return Err(Error::ControlPlaneStatus {
            status: status.as_u16(),
            message: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    serde_json::from_slice(&body).map_err(Error::ControlPlaneResponse)
}

/// Build the sops document of secret `name` holding `value`.
///
/// Only `value` is encrypted. `name` is plaintext, but sops MACs unencrypted
/// values too, which is what lets `unwrap` refuse a document that was wrapped
/// for a different secret and then stored under this one.
async fn wrap(
    keychain: &Keychain,
    name: &models::Name,
    value: &serde_json::value::RawValue,
) -> Result<Box<serde_json::value::RawValue>, Error> {
    #[derive(serde::Serialize)]
    struct Document<'a> {
        name: &'a str,
        // Serialized verbatim: sops MACs its values in traversal order, so the
        // author's key order must survive into the document we hand it.
        value: &'a serde_json::value::RawValue,
    }
    let input = serde_json::to_vec(&Document {
        name: name.as_str(),
        value,
    })
    .expect("serialization of a RawValue is infallible");

    let stdout = invoke_sops(
        Some(keychain),
        &["--encrypt", "--encrypted-regex", "^value$"],
        &input,
    )
    .await?;

    serde_json::from_slice(&stdout).map_err(Error::SopsOutput)
}

/// Unwrap the sops `document` of secret `name`, returning its value.
///
/// A successful decryption proves the document is intact, and its embedded
/// name proves *which* secret it was wrapped for. Both are needed: without the
/// name check, a document legitimately wrapped for one secret could be stored
/// under another name and disclosed to a task authorized only for that name.
async fn unwrap(
    name: &models::Name,
    document: &str,
) -> Result<Box<serde_json::value::RawValue>, Error> {
    let stdout = invoke_sops(None, &["--decrypt"], document.as_bytes()).await?;

    #[derive(serde::Deserialize)]
    struct Document<'a> {
        // `Cow`, because a borrowed `&str` cannot represent an escaped JSON
        // string: a document whose name happened to carry one would fail to
        // parse at all, rather than being reported as the mismatch it is.
        #[serde(borrow)]
        name: std::borrow::Cow<'a, str>,
        #[serde(borrow)]
        value: &'a serde_json::value::RawValue,
    }
    let Document {
        name: embedded,
        value,
    } = serde_json::from_slice(&stdout).map_err(Error::SopsOutput)?;

    if embedded != name.as_str() {
        return Err(Error::NameMismatch {
            requested: name.to_string(),
            embedded: embedded.into_owned(),
        });
    }
    Ok(value.to_owned())
}

fn validate_name(name: &models::Name) -> Result<(), Error> {
    validator::Validate::validate(name).map_err(Error::SecretName)
}

fn bearer_token(headers: &axum::http::HeaderMap) -> Result<&str, Error> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(Error::BearerMissing)
}

fn join(base: &url::Url, path: &str) -> url::Url {
    base.join(path).expect("path is a valid relative URL")
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    // Age keypair generated for testing only. DO NOT use in production.
    const TEST_AGE_PUBLIC: &str = "age1q7f46kejaudnyksha4cmxazymmuln4crx50ly6k05ztrxep4yshsp2y5tz";
    const TEST_AGE_SECRET: &str =
        "AGE-SECRET-KEY-1LR027C0DR4JY4T06TQ5FAJ2KRWXXW9GDH79H4X46968U4P6CVL0QK6ZLJM";

    /// sops finds its age identity through the process environment, exactly as
    /// it does in production. `cargo nextest` runs each test in its own
    /// process, so this is only ever set once, before anything else reads it.
    fn keychain() -> Keychain {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| unsafe { std::env::set_var("SOPS_AGE_KEY", TEST_AGE_SECRET) });

        Keychain::Age(TEST_AGE_PUBLIC.to_string())
    }

    fn name(name: &str) -> models::Name {
        models::Name::new(name)
    }

    fn raw(value: serde_json::Value) -> Box<serde_json::value::RawValue> {
        serde_json::value::RawValue::from_string(value.to_string()).unwrap()
    }

    #[tokio::test]
    async fn test_round_trip_of_various_values() {
        let cases = [
            json!("hunter2"),
            // sops leaves a null in place rather than encrypting it. Harmless:
            // a null discloses nothing, and the MAC still covers it.
            json!(null),
            json!(42),
            json!(["a", "b"]),
            // An object value is encrypted as a subtree: `--encrypted-regex`
            // matches the `value` key, and everything beneath it is covered.
            json!({"user": "admin", "password": "s3cret", "port": 5432, "opts": {"tls": true}}),
        ];
        let name = name("acmeCo/db/creds");
        let mut outcomes = Vec::new();

        for value in cases {
            let value = raw(value);
            let document = wrap(&keychain(), &name, &value).await.unwrap();

            // Every leaf under `value` is ciphertext, and nothing else is.
            let parsed: serde_json::Value = serde_json::from_str(document.get()).unwrap();
            let mut leaves = Vec::new();
            collect_leaves(&parsed["value"], &mut leaves);

            let unwrapped = unwrap(&name, document.get()).await.unwrap();

            outcomes.push((
                value.get().to_string(),
                parsed["name"].as_str().unwrap().to_string(),
                leaves.iter().all(|leaf| leaf.starts_with("ENC[")),
                // sops pretty-prints its output, so the round trip is over
                // values and not over the bytes which carry them.
                serde_json::from_str::<serde_json::Value>(unwrapped.get()).unwrap()
                    == serde_json::from_str::<serde_json::Value>(value.get()).unwrap(),
            ));
        }

        insta::assert_debug_snapshot!(outcomes, @r#"
        [
            (
                "\"hunter2\"",
                "acmeCo/db/creds",
                true,
                true,
            ),
            (
                "null",
                "acmeCo/db/creds",
                false,
                true,
            ),
            (
                "42",
                "acmeCo/db/creds",
                true,
                true,
            ),
            (
                "[\"a\",\"b\"]",
                "acmeCo/db/creds",
                true,
                true,
            ),
            (
                "{\"opts\":{\"tls\":true},\"password\":\"s3cret\",\"port\":5432,\"user\":\"admin\"}",
                "acmeCo/db/creds",
                true,
                true,
            ),
        ]
        "#);
    }

    /// Collect the JSON text of every leaf of `node`, so that a test can assert
    /// an entire subtree was encrypted.
    fn collect_leaves(node: &serde_json::Value, out: &mut Vec<String>) {
        match node {
            serde_json::Value::Object(fields) => {
                fields.values().for_each(|child| collect_leaves(child, out))
            }
            serde_json::Value::Array(items) => {
                items.iter().for_each(|item| collect_leaves(item, out))
            }
            // Ciphertext is always a string, so a leaf is compared as its
            // content rather than as the JSON which quotes it.
            serde_json::Value::String(text) => out.push(text.clone()),
            scalar => out.push(scalar.to_string()),
        }
    }

    #[tokio::test]
    async fn test_name_binding_and_tampering() {
        let document = wrap(
            &keychain(),
            &name("acmeCo/db/creds"),
            &raw(json!("hunter2")),
        )
        .await
        .unwrap();
        let document = document.get();

        // A document is bound to its name two ways over. Renaming it in place
        // breaks the MAC, because sops covers plaintext values too. Storing it
        // verbatim under another name leaves the MAC intact, and is caught by
        // comparing the name we asked for against the one we decrypted.
        let renamed = document.replace("acmeCo/db/creds", "acmeCo/db/other");
        let restamped = document.replace(
            &lastmodified(document),
            "2099-01-01T00:00:00Z", // Whatever the stored timestamp was, this isn't it.
        );

        let outcomes = [
            (
                "cloned-under-another-name",
                unwrap(&name("acmeCo/db/other"), document)
                    .await
                    .unwrap_err(),
            ),
            (
                "renamed-in-place",
                unwrap(&name("acmeCo/db/other"), &renamed)
                    .await
                    .unwrap_err(),
            ),
            (
                "restamped-lastmodified",
                unwrap(&name("acmeCo/db/creds"), &restamped)
                    .await
                    .unwrap_err(),
            ),
        ];

        // sops reports a MAC failure with the computed digests, which vary per
        // encryption, so only the class of each failure is snapshot-able.
        let outcomes = outcomes.map(|(case, err)| match err {
            Error::NameMismatch {
                requested,
                embedded,
            } => (case, format!("NameMismatch {requested} != {embedded}")),
            Error::SopsFailed(_) => (case, "SopsFailed".to_string()),
            err => (case, format!("unexpected {err:?}")),
        });

        insta::assert_debug_snapshot!(outcomes, @r#"
        [
            (
                "cloned-under-another-name",
                "NameMismatch acmeCo/db/other != acmeCo/db/creds",
            ),
            (
                "renamed-in-place",
                "SopsFailed",
            ),
            (
                "restamped-lastmodified",
                "SopsFailed",
            ),
        ]
        "#);
    }

    fn lastmodified(document: &str) -> String {
        let parsed: serde_json::Value = serde_json::from_str(document).unwrap();
        parsed["sops"]["lastmodified"].as_str().unwrap().to_string()
    }

    /// A stand-in control-plane which records what it was asked and answers
    /// with a canned reply. It's how the routing of a token to the endpoint
    /// which can verify it, and the pass-through of that endpoint's answer,
    /// are observable without a control-plane and its database.
    struct Fake {
        calls: std::sync::Mutex<Vec<String>>,
        status: u16,
        body: String,
    }

    impl Fake {
        fn new(status: u16, body: &str) -> Arc<Self> {
            Arc::new(Self {
                calls: Default::default(),
                status,
                body: body.to_string(),
            })
        }

        fn record(&self, call: String) -> (axum::http::StatusCode, String) {
            self.calls.lock().unwrap().push(call);

            (
                axum::http::StatusCode::from_u16(self.status).unwrap(),
                self.body.clone(),
            )
        }
    }

    /// Name the token which a call carried. Which token reaches which route is
    /// the property under test; the tokens are long and uninteresting.
    fn token_label(token: &str) -> &'static str {
        match token {
            USER_TOKEN => "<user token>",
            TASK_TOKEN => "<task token>",
            _ => "<other>",
        }
    }

    /// Bind an ephemeral port and serve `router` on it, returning its base URL.
    async fn serve(router: axum::Router) -> url::Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        url::Url::parse(&format!("http://{address}/")).unwrap()
    }

    /// Serve `fake` as a control-plane, and our own routes in front of it.
    async fn serve_with_fake(fake: Arc<Fake>) -> url::Url {
        let control_plane = axum::Router::new()
            .route(
                "/authorize/task/decrypt-secret",
                axum::routing::post(
                    async |axum::extract::State(fake): axum::extract::State<Arc<Fake>>,
                           axum::Json(request): axum::Json<
                        models::authorizations::TaskAuthorizationRequest,
                    >| {
                        fake.record(format!("POST body-token={}", token_label(&request.token)))
                    },
                ),
            )
            .route(
                "/authorize/user/decrypt-secret",
                axum::routing::get(
                    async |axum::extract::State(fake): axum::extract::State<Arc<Fake>>,
                           uri: axum::http::Uri,
                           headers: axum::http::HeaderMap| {
                        fake.record(format!(
                            "GET {} bearer={}",
                            uri,
                            token_label(bearer_token(&headers).unwrap_or_default()),
                        ))
                    },
                ),
            )
            .with_state(fake);

        let control_plane_url = serve(control_plane).await;
        serve(router(Arc::new(App::new(keychain(), control_plane_url)))).await
    }

    // Well-formed but unsigned tokens. Neither route verifies a signature --
    // that's the control-plane API's job -- so only the shape of the claims matters.
    const USER_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhdXRoZW50aWNhdGVkIiwiaWF0IjoxLCJleHAiOjQxMDI0NDQ4MDAsInN1YiI6IjExMTExMTExLTExMTEtMTExMS0xMTExLTExMTExMTExMTExMSIsInJvbGUiOiJhdXRoZW50aWNhdGVkIn0.signature";
    const TASK_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJsb2NhbC5kcC5lc3R1YXJ5LWRhdGEuY29tIiwiaWF0IjoxLCJleHAiOjQxMDI0NDQ4MDAsInN1YiI6ImNhcHR1cmUvYWNtZUNvL3NvdXJjZS1mb28vMDAwMDAwMDAwMDAwMDAwMC8wMDAwMDAwMC0wMDAwMDAwMCIsImNhcCI6MzIsInNlbCI6eyJpbmNsdWRlIjp7ImxhYmVscyI6W3sibmFtZSI6ImVzdHVhcnkuZGV2L3NlY3JldC1uYW1lIiwidmFsdWUiOiJhY21lQ28vZGIvcGFzc3dvcmQifV19fX0.signature";

    /// Drive `/secret/decrypt` and reduce its outcome, and the control-plane
    /// call it made, to a snapshot-able summary.
    async fn decrypt(reply: (u16, &str), query: &str, token: &str) -> (String, String, String) {
        let fake = Fake::new(reply.0, reply.1);
        let base = serve_with_fake(fake.clone()).await;

        let response = reqwest::Client::new()
            .post(base.join(&format!("/secret/decrypt?{query}")).unwrap())
            .bearer_auth(token)
            .send()
            .await
            .unwrap();

        let status = response.status().as_u16();
        let cache_control = response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .map(|value| value.to_str().unwrap().to_string())
            .unwrap_or_default();
        let body = response.text().await.unwrap();

        (
            fake.calls.lock().unwrap().join("; "),
            format!("{status} cache-control={cache_control}"),
            body.lines().next().unwrap_or_default().to_string(),
        )
    }

    #[tokio::test]
    async fn test_decrypt_routes_to_the_control_plane() {
        let document = wrap(
            &keychain(),
            &name("acmeCo/db/password"),
            &raw(json!("hunter2")),
        )
        .await
        .unwrap();

        let authorized = serde_json::json!({
            "document": serde_json::from_str::<serde_json::Value>(document.get()).unwrap(),
            "secretId": "1122334455667788",
            "retryMillis": 0,
        })
        .to_string();

        // A user token carries neither `cap` nor `sel`, and is forwarded as the
        // Bearer it already is, alongside the `name` and `started` which the
        // user route needs and cannot recover from the token itself.
        let user = decrypt(
            (200, &authorized),
            "name=acmeCo%2Fdb%2Fpassword&started=2026-08-21T00:00:00.000Z",
            USER_TOKEN,
        )
        .await;

        // A data-plane token is POSTed as a body instead, and travels alone:
        // the task route reads the secret name from its `sel`, and `started`
        // from its `iat`.
        let task = decrypt(
            (200, &authorized),
            "name=acmeCo%2Fdb%2Fpassword",
            TASK_TOKEN,
        )
        .await;

        let retry = decrypt(
            (200, r#"{"retryMillis":7500}"#),
            "name=acmeCo%2Fdb%2Fpassword",
            TASK_TOKEN,
        )
        .await;
        let denied = decrypt(
            (403, "not authorized to decrypt 'acmeCo/db/password'"),
            "name=acmeCo%2Fdb%2Fpassword",
            TASK_TOKEN,
        )
        .await;

        // Only a successful disclosure is `no-store`; an error names no secret
        // material and passes through with the control-plane's own status.
        insta::assert_debug_snapshot!([
            ("user", user),
            ("task", task),
            ("retry", retry),
            ("denied", denied),
        ], @r#"
        [
            (
                "user",
                (
                    "GET /authorize/user/decrypt-secret?name=acmeCo%2Fdb%2Fpassword&started=2026-08-21T00%3A00%3A00.000Z bearer=<user token>",
                    "200 cache-control=no-store",
                    "{\"value\":\"hunter2\",\"secretId\":\"1122334455667788\",\"retryMillis\":0}",
                ),
            ),
            (
                "task",
                (
                    "POST body-token=<task token>",
                    "200 cache-control=no-store",
                    "{\"value\":\"hunter2\",\"secretId\":\"1122334455667788\",\"retryMillis\":0}",
                ),
            ),
            (
                "retry",
                (
                    "POST body-token=<task token>",
                    "200 cache-control=no-store",
                    "{\"retryMillis\":7500}",
                ),
            ),
            (
                "denied",
                (
                    "POST body-token=<task token>",
                    "403 cache-control=",
                    "control-plane API responded 403: not authorized to decrypt 'acmeCo/db/password'",
                ),
            ),
        ]
        "#);
    }

    /// A browser preflights `/secret/decrypt`, because it carries an
    /// `Authorization` header. Whether that preflight is answered is invisible
    /// from anywhere but a browser, so it's pinned here.
    #[tokio::test]
    async fn test_cors_preflight_of_decrypt() {
        let base = serve(
            router(Arc::new(App::new(
                keychain(),
                url::Url::parse("http://localhost:8675/").unwrap(),
            )))
            .layer(crate::cors_layer()),
        )
        .await;

        let response = reqwest::Client::new()
            .request(
                reqwest::Method::OPTIONS,
                base.join("/secret/decrypt").unwrap(),
            )
            .header("Origin", "https://dashboard.estuary.dev")
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", "authorization")
            .send()
            .await
            .unwrap();

        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .map(|value| value.to_str().unwrap().to_string())
                .unwrap_or_default()
        };

        insta::assert_debug_snapshot!(
            (
                response.status().as_u16(),
                header("access-control-allow-origin"),
                header("access-control-allow-methods"),
                header("access-control-allow-headers"),
            ),
            @r#"
        (
            200,
            "*",
            "POST",
            "authorization,content-type,accept",
        )
        "#
        );
    }

    #[test]
    fn test_control_plane_url_is_slash_terminated() {
        let app = App::new(
            Keychain::Age(TEST_AGE_PUBLIC.to_string()),
            url::Url::parse("http://localhost:8675/api").unwrap(),
        );

        assert_eq!(
            join(&app.control_plane_url, "authorize/task/decrypt-secret").as_str(),
            "http://localhost:8675/api/authorize/task/decrypt-secret"
        );
    }
}
