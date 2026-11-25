//! `POST /api/v1/sandboxes/exec` runs one command in a sandbox. The request
//! body is an [`ExecStart`] JSON line, followed by the command's stdin. The
//! response body streams the command's output as `sandbox_exec::Output` frames.
//!
//! The route rewrites [`ExecStart`] into the wrapper's Header, with a minted
//! `FLOW_AUTH_TOKEN`. The end of the request body is the command's stdin EOF.
//! The client abandons the command by dropping the response.
//!
//! Over HTTP/2 (h2c, as Cloud Run forwards it), stdin streams in while output
//! streams out. An HTTP/1 client sends its whole body before it reads the
//! response, which buffers stdin but otherwise works the same.
//!
//! The command lives only as long as the response. The response body owns the
//! provider's `Exec` (through `sandboxes::exec::relay`). When the client goes
//! away, the server drops the body, which closes the wrapper's input, and the
//! wrapper cancels the command.

use crate::sandboxes::{self, exec::Relayed, provider};
use futures::StreamExt;
use std::sync::Arc;

/// The first line of a request body: the command to run in a sandbox.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecStart {
    /// Catalog name of the sandbox.
    pub catalog_name: String,
    /// Workspace of the command: a directory of the sandbox which is its
    /// working directory and HOME. It's created if missing.
    pub workspace: String,
    /// Command line, run as `bash -lc <command>`.
    pub command: String,
    /// Name of a credential approved for the workspace by the caller.
    /// The command's `FLOW_AUTH_TOKEN` has the credential's masks. Without
    /// one, it identifies the caller and grants nothing.
    #[serde(default)]
    pub credential: Option<String>,
}

/// Bytes of the [`ExecStart`] line which the route will read before it
/// refuses a request.
const MAX_START_LINE: usize = 1 << 20;

pub async fn handle_post_sandbox_exec(
    axum::extract::State(app): axum::extract::State<Arc<crate::App>>,
    env: crate::Envelope,
    body: axum::body::Body,
) -> Result<axum::response::Response, crate::ApiError> {
    let claims = env.claims()?;
    let (start, stdin) = read_start(body).await?;
    let ExecStart {
        catalog_name,
        workspace,
        command,
        credential,
    } = start;

    let sandbox = sandboxes::fetch_owned(&env.pg_pool, claims, &catalog_name)
        .await
        .map_err(internal)?
        .ok_or_else(|| tonic::Status::not_found(format!("sandbox {catalog_name:?} not found")))?;

    match sandbox.state() {
        sandboxes::State::Ready => {}
        sandboxes::State::Provisioning => {
            return Err(tonic::Status::unavailable(format!(
                "sandbox {catalog_name:?} is still being provisioned; retry once it's ready"
            ))
            .into());
        }
        sandboxes::State::Gone => return Err(gone(&catalog_name).into()),
    }
    if !sandbox_exec::is_valid_workspace(&workspace) {
        return Err(tonic::Status::invalid_argument(format!(
            "invalid workspace {workspace:?}: must match {}",
            sandbox_exec::WORKSPACE_PATTERN
        ))
        .into());
    }

    let token =
        mint_command_token(&app, claims, sandbox.id, &workspace, credential.as_deref()).await?;

    let header = sandbox_exec::Header {
        version: sandbox_exec::PROTOCOL_VERSION,
        workspace: workspace.clone(),
        command,
        env: [("FLOW_AUTH_TOKEN".to_string(), token)].into(),
    };
    let header = sandboxes::exec::header_frame(header)
        .map_err(|err| tonic::Status::invalid_argument(format!("{err:#}")))?;

    let provider = app.sandbox_providers.serving(&sandbox).map_err(internal)?;
    let provider_state = sandbox
        .provider_state
        .as_ref()
        .expect("a ready sandbox has provider state");

    let exec = match provider
        .exec(&sandbox.handle, provider_state, &provider::WRAPPER_ARGV)
        .await
    {
        Ok(exec) => exec,
        Err(provider::Error::Gone) => {
            if let Err(err) = sandboxes::mark_gone(&env.pg_pool, &sandbox).await {
                tracing::error!(?err, %sandbox.id, "failed to mark sandbox gone");
            }
            return Err(gone(&catalog_name).into());
        }
        Err(provider::Error::Other(err)) => {
            tracing::error!(?err, %sandbox.id, "failed to start sandbox command");
            return Err(tonic::Status::internal(format!(
                "failed to start the sandbox command: {err:#}"
            ))
            .into());
        }
    };
    let credential = credential.is_some();
    tracing::info!(%sandbox.id, %workspace, credential, "started sandbox command");

    let pg_pool = env.pg_pool.clone();
    let body = async_stream::stream! {
        let mut unended = Unended {
            sandbox_id: sandbox.id,
            workspace: workspace.clone(),
            credential,
            ended: false,
        };
        let relayed = sandboxes::exec::relay(exec, header, stdin);
        let mut relayed = std::pin::pin!(relayed);

        while let Some(item) = relayed.next().await {
            match item {
                Relayed::Frames(frames) => yield Ok::<_, std::convert::Infallible>(frames),
                Relayed::Finished(terminal) => {
                    unended.ended = true;
                    tracing::info!(%sandbox.id, %workspace, credential, ?terminal, "sandbox command finished");
                }
                Relayed::Failed { message, gone } => {
                    unended.ended = true;
                    tracing::warn!(%sandbox.id, %workspace, credential, %message, "sandbox command failed");

                    // Marked before the Error frame is sent, as a client may
                    // hang up once it has the frame.
                    if gone && let Err(err) = sandboxes::mark_gone(&pg_pool, &sandbox).await {
                        tracing::error!(?err, %sandbox.id, "failed to mark sandbox gone");
                    }
                    let mut frame = bytes::BytesMut::new();
                    sandbox_exec::Output::Error { message }
                        .encode(&mut frame)
                        .expect("Error frame is within MAX_PAYLOAD");
                    yield Ok(frame.freeze());
                }
            }
        }
    };

    Ok(axum::response::Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .body(axum::body::Body::from_stream(body))
        .expect("response is valid"))
}

/// Reads the [`ExecStart`] line which begins `body`, and returns it with the
/// rest of `body`: the command's stdin. A body without a newline is only an
/// [`ExecStart`], and the command's stdin is empty.
async fn read_start(
    body: axum::body::Body,
) -> Result<(ExecStart, sandboxes::exec::Stdin), crate::ApiError> {
    let invalid = |message: String| crate::ApiError::from(tonic::Status::invalid_argument(message));
    let mut data = body.into_data_stream();
    let mut buf = bytes::BytesMut::new();
    let mut scanned = 0;

    let line = loop {
        if let Some(newline) = buf[scanned..].iter().position(|byte| *byte == b'\n') {
            break buf.split_to(scanned + newline + 1);
        }
        scanned = buf.len();

        if buf.len() > MAX_START_LINE {
            return Err(invalid(format!(
                "the request's first line exceeds {MAX_START_LINE} bytes"
            )));
        }
        match data.next().await {
            Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
            Some(Err(err)) => return Err(invalid(format!("failed to read the request: {err}"))),
            None => break buf.split(),
        }
    };
    let start: ExecStart = serde_json::from_slice(&line)
        .map_err(|err| invalid(format!("invalid first line of the request: {err}")))?;

    let rest = buf.freeze();
    let stdin = futures::stream::iter((!rest.is_empty()).then_some(Ok(rest)))
        .chain(data.map(|chunk| chunk.map_err(anyhow::Error::from)))
        .boxed();

    Ok((start, stdin))
}

/// Mints the `FLOW_AUTH_TOKEN` of a command of `workspace`, from the caller's
/// claims. It expires within `RESTRICTED_TOKEN_DURATION`.
///
/// Without a `credential`, the token's capability and prefix masks are empty:
/// it identifies the caller and grants nothing. With one, the token has the
/// approval's masks, and doesn't outlive the approval.
async fn mint_command_token(
    app: &crate::App,
    claims: &crate::ControlClaims,
    sandbox_id: models::Id,
    workspace: &str,
    credential: Option<&str>,
) -> Result<String, crate::ApiError> {
    let now = tokens::now();
    let mut expires_at = now + super::token_exchange::RESTRICTED_TOKEN_DURATION;

    let (capability_mask, prefix_mask) = match credential {
        None => (Vec::new(), Vec::new()),
        Some(name) => {
            let approval = fetch_approval(app, claims, sandbox_id, workspace, name, now).await?;
            expires_at = std::cmp::min(expires_at, approval.expires_at);
            (approval.capability_mask, approval.prefix_mask)
        }
    };

    let minted = super::token_exchange::mint_restricted_token(
        claims,
        Some(capability_mask),
        Some(prefix_mask),
        now,
        expires_at,
        app,
    )
    .await
    .map_err(|err| match err {
        // A refusal of the caller as a minter refuses every command.
        crate::ApiError::Status(status) if status.code() == tonic::Code::PermissionDenied => {
            tonic::Status::permission_denied(format!(
                "sandbox commands run with a token minted from yours: {}",
                status.message()
            ))
            .into()
        }
        err => err,
    })?;

    Ok(minted.access_token)
}

/// Fetches the approval of credential `name`, which must be unexpired at
/// `now` and approved by the caller.
async fn fetch_approval(
    app: &crate::App,
    claims: &crate::ControlClaims,
    sandbox_id: models::Id,
    workspace: &str,
    name: &str,
    now: tokens::DateTime,
) -> Result<sandboxes::credentials::Credential, crate::ApiError> {
    let Some(approval) = sandboxes::credentials::fetch(&app.pg_pool, sandbox_id, workspace, name)
        .await
        .map_err(internal)?
    else {
        return Err(tonic::Status::not_found(format!(
            "credential {name:?} is not approved for workspace {workspace:?} of this sandbox"
        ))
        .into());
    };

    if approval.expires_at <= now {
        return Err(tonic::Status::permission_denied(format!(
            "credential {name:?} of workspace {workspace:?} expired at {}; approve it again",
            approval
                .expires_at
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        ))
        .into());
    }
    if approval.approved_by != claims.sub {
        return Err(tonic::Status::permission_denied(format!(
            "credential {name:?} of workspace {workspace:?} was approved by another user"
        ))
        .into());
    }
    Ok(approval)
}

fn internal(err: anyhow::Error) -> tonic::Status {
    tracing::error!(?err, "sandbox command request failed");
    tonic::Status::internal(format!("{err:#}"))
}

/// The error of a sandbox whose machine no longer exists. No other error of
/// this route has its status code, so clients can react to it by deleting the
/// sandbox and creating another.
fn gone(catalog_name: &str) -> tonic::Status {
    tonic::Status::failed_precondition(format!(
        "sandbox {catalog_name:?} no longer exists; delete it and create another"
    ))
}

/// Logs a command whose response was dropped before its relay ended: the
/// client went away, and the command was cancelled.
struct Unended {
    sandbox_id: models::Id,
    workspace: String,
    credential: bool,
    ended: bool,
}

impl Drop for Unended {
    fn drop(&mut self) {
        if !self.ended {
            tracing::info!(
                sandbox.id = %self.sandbox_id,
                workspace = %self.workspace,
                credential = self.credential,
                "sandbox command cancelled: the client went away"
            );
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sandboxes::fake::{Fake, Process};
    use crate::sandboxes::provider::Output;
    use crate::test_server::TestServer;
    use std::sync::Arc;

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);
    const BOB: uuid::Uuid = uuid::Uuid::from_bytes([0x22; 16]);

    /// Starts a server whose sandboxes are served by a fake provider, and
    /// creates alice's sandboxes of `names`.
    async fn setup(
        pool: &sqlx::PgPool,
        names: &[&str],
    ) -> (TestServer, Arc<Fake>, Vec<crate::sandboxes::Sandbox>) {
        sqlx::query(
            "INSERT INTO auth.users (id, email)
             VALUES ($1, 'alice@example.test'), ($2, 'bob@example.test')",
        )
        .bind(ALICE)
        .bind(BOB)
        .execute(pool)
        .await
        .unwrap();

        let fake = Arc::new(Fake::new("fake"));
        let providers = crate::sandboxes::Providers::new(
            vec![fake.clone() as Arc<dyn crate::sandboxes::provider::Provider>],
            Some("fake"),
        )
        .unwrap();
        let server = TestServer::start_with_sandbox_providers(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
            providers,
        )
        .await;

        let mut sandboxes = Vec::new();
        for name in names {
            sandboxes.push(
                crate::sandboxes::create(&server.app.sandbox_providers, pool, ALICE, name)
                    .await
                    .unwrap(),
            );
        }
        (server, fake, sandboxes)
    }

    async fn approve(
        pool: &sqlx::PgPool,
        sandbox: &crate::sandboxes::Sandbox,
        name: &str,
        approved_by: uuid::Uuid,
        minutes: i32,
    ) {
        crate::sandboxes::credentials::approve(
            pool,
            sandbox.id,
            "ws",
            name,
            &["viewer".to_string()],
            &["acmeCo/".to_string()],
            approved_by,
            minutes,
        )
        .await
        .unwrap();
    }

    async fn exec(
        server: &TestServer,
        token: Option<&str>,
        request: serde_json::Value,
    ) -> reqwest::Response {
        exec_body(server, token, body(request)).await
    }

    /// Sends a request over HTTP/1, so its whole body is sent before the
    /// response is read.
    async fn exec_body(
        server: &TestServer,
        token: Option<&str>,
        body: Vec<u8>,
    ) -> reqwest::Response {
        let request = reqwest::Client::new()
            .post(server.base_url().join("/api/v1/sandboxes/exec").unwrap())
            .body(body);
        let request = match token {
            Some(token) => request.bearer_auth(token),
            None => request,
        };
        request.send().await.unwrap()
    }

    /// The body of a request: the `ExecStart` line of `request`, without its
    /// `stdin` field, and then that field's value as the command's stdin.
    pub(super) fn body(mut request: serde_json::Value) -> Vec<u8> {
        let stdin = request.as_object_mut().unwrap().remove("stdin");
        let mut body = serde_json::to_vec(&request).unwrap();
        body.push(b'\n');
        if let Some(stdin) = stdin {
            body.extend_from_slice(stdin.as_str().unwrap().as_bytes());
        }
        body
    }

    fn request(catalog_name: &str, credential: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "catalog_name": catalog_name,
            "workspace": "ws",
            "command": "cat",
            "credential": credential,
            "stdin": "hello",
        })
    }

    fn encode(frames: &[sandbox_exec::Output]) -> bytes::Bytes {
        let mut buf = bytes::BytesMut::new();
        for frame in frames {
            frame.encode(&mut buf).unwrap();
        }
        buf.freeze()
    }

    /// Reads input frames written to the process until it has StdinEof.
    async fn read_inputs(process: &mut Process) -> Vec<sandbox_exec::Input> {
        let mut inputs = Vec::new();
        let mut buf = bytes::BytesMut::new();
        while !matches!(inputs.last(), Some(sandbox_exec::Input::StdinEof)) {
            buf.extend_from_slice(&process.stdin.recv().await.unwrap());
            while let Some(input) = sandbox_exec::Input::decode(&mut buf).unwrap() {
                inputs.push(input);
            }
        }
        inputs
    }

    /// Reads the response body to its end, and decodes its frames.
    async fn read_frames(response: reqwest::Response) -> Vec<sandbox_exec::Output> {
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/octet-stream"
        );
        let mut body = bytes::BytesMut::from(&response.bytes().await.unwrap()[..]);
        let mut frames = Vec::new();
        while let Some(frame) = sandbox_exec::Output::decode(&mut body).unwrap() {
            frames.push(frame);
        }
        assert!(body.is_empty(), "the body ends with a whole frame");
        frames
    }

    async fn summarize(response: reqwest::Response) -> String {
        let status = response.status().as_u16();
        format!("{status} {}", response.text().await.unwrap())
    }

    async fn state(pool: &sqlx::PgPool, catalog_name: &str) -> crate::sandboxes::State {
        crate::sandboxes::fetch(pool, &[catalog_name.to_string()], &[])
            .await
            .unwrap()
            .pop()
            .unwrap()
            .state()
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn exec_refusals(pool: sqlx::PgPool) {
        let (server, fake, sandboxes) = setup(
            &pool,
            &[
                "acmeCo/sandbox",
                "acmeCo/provisioning",
                "acmeCo/gone",
                "acmeCo/vanished",
            ],
        )
        .await;
        let [sandbox, provisioning, gone, vanished] = &sandboxes[..] else {
            unreachable!()
        };
        let _processes = fake.processes();

        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));
        let bob_token = server.make_access_token(BOB, Some("bob@example.test"));
        let restricted_token = server.make_restricted_access_token(
            ALICE,
            Some("alice@example.test"),
            None,
            Some(vec!["acmeCo/".to_string()]),
        );

        approve(&pool, sandbox, "expired", ALICE, 60).await;
        sqlx::query(
            "UPDATE internal.sandbox_credentials
             SET expires_at = '2020-01-01T00:00:00Z' WHERE name = 'expired'",
        )
        .execute(&pool)
        .await
        .unwrap();
        approve(&pool, sandbox, "bobs", BOB, 60).await;
        approve(&pool, sandbox, "good", ALICE, 60).await;

        sqlx::query(
            "UPDATE internal.sandboxes SET provider_state = NULL WHERE catalog_name = 'acmeCo/provisioning'",
        )
        .execute(&pool)
        .await
        .unwrap();
        crate::sandboxes::mark_gone(&pool, gone).await.unwrap();
        fake.inner.lock().unwrap().machines.remove(&vanished.handle);
        let _ = provisioning;

        let mut invalid_workspace = request("acmeCo/sandbox", None);
        invalid_workspace["workspace"] = "../ws".into();
        let mut unknown_field = request("acmeCo/sandbox", None);
        unknown_field["env"] = serde_json::json!({});

        let cases = [
            ("unauthenticated", None, request("acmeCo/sandbox", None)),
            (
                "notOwner",
                Some(&bob_token),
                request("acmeCo/sandbox", None),
            ),
            (
                "missing",
                Some(&alice_token),
                request("acmeCo/missing", None),
            ),
            ("unknownField", Some(&alice_token), unknown_field),
            ("invalidWorkspace", Some(&alice_token), invalid_workspace),
            (
                "provisioning",
                Some(&alice_token),
                request("acmeCo/provisioning", None),
            ),
            ("gone", Some(&alice_token), request("acmeCo/gone", None)),
            (
                "missingCredential",
                Some(&alice_token),
                request("acmeCo/sandbox", Some("nope")),
            ),
            (
                "expiredCredential",
                Some(&alice_token),
                request("acmeCo/sandbox", Some("expired")),
            ),
            (
                "nonApprover",
                Some(&alice_token),
                request("acmeCo/sandbox", Some("bobs")),
            ),
            (
                "restrictedCaller",
                Some(&restricted_token),
                request("acmeCo/sandbox", Some("good")),
            ),
            (
                "restrictedCallerWithoutCredential",
                Some(&restricted_token),
                request("acmeCo/sandbox", None),
            ),
            (
                "goneAtExec",
                Some(&alice_token),
                request("acmeCo/vanished", None),
            ),
        ];
        let unparsed = [
            ("emptyBody", Some(&alice_token), Vec::new()),
            (
                "invalidStartLine",
                Some(&alice_token),
                b"catalog_name: acmeCo/sandbox\nstdin".to_vec(),
            ),
        ];

        let mut outcomes = Vec::new();
        for (step, token, body) in cases
            .into_iter()
            .map(|(step, token, request)| (step, token, body(request)))
            .chain(unparsed)
        {
            let response = exec_body(&server, token.map(String::as_str), body).await;
            outcomes.push(format!("{step}: {}", summarize(response).await));
        }

        // Only `goneAtExec` reached the provider.
        let execs: Vec<_> = fake
            .calls()
            .into_iter()
            .filter(|(op, _)| *op == "exec")
            .map(|(_, handle)| handle == vanished.handle)
            .collect();
        assert_eq!(execs, vec![true]);

        insta::assert_debug_snapshot!((outcomes, state(&pool, "acmeCo/vanished").await));
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn exec_relays_command(pool: sqlx::PgPool) {
        let (server, fake, sandboxes) = setup(&pool, &["acmeCo/sandbox"]).await;
        let mut processes = fake.processes();
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));

        approve(&pool, &sandboxes[0], "long", ALICE, 480).await;
        approve(&pool, &sandboxes[0], "short", ALICE, 30).await;
        let short_expires_at =
            crate::sandboxes::credentials::fetch(&pool, sandboxes[0].id, "ws", "short")
                .await
                .unwrap()
                .unwrap()
                .expires_at;

        let mut outcomes = Vec::new();
        for credential in [Some("long"), Some("short"), None] {
            let response = exec(
                &server,
                Some(&alice_token),
                request("acmeCo/sandbox", credential),
            )
            .await;
            let mut process = processes.recv().await.unwrap();
            assert_eq!(process.argv, crate::sandboxes::provider::WRAPPER_ARGV);

            let mut inputs = read_inputs(&mut process).await;
            let sandbox_exec::Input::Header(header) = inputs.remove(0) else {
                panic!("the first frame is a Header");
            };
            let claims = server.verify_access_token(&header.env["FLOW_AUTH_TOKEN"]);
            match credential {
                Some("short") => assert_eq!(claims.exp as i64, short_expires_at.timestamp()),
                _ => assert_eq!(claims.exp - claims.iat, 3600),
            }
            let claims = serde_json::json!({
                "sub": claims.sub,
                "role": claims.role,
                "capability_mask": claims.capability_mask,
                "prefix_mask": claims.prefix_mask,
            });

            // The wrapper's output, split within frames.
            let output = encode(&[
                sandbox_exec::Output::Stdout(bytes::Bytes::from_static(b"hello")),
                sandbox_exec::Output::Stderr(bytes::Bytes::from_static(b"note")),
                sandbox_exec::Output::Exit {
                    code: Some(0),
                    signal: None,
                },
            ]);
            for piece in [output.slice(..7), output.slice(7..)] {
                process.output.send(Ok(Output::Stdout(piece))).unwrap();
            }

            outcomes.push(serde_json::json!({
                "credential": credential,
                "header": format!("{header:?}"),
                "token": claims,
                "inputs": format!("{inputs:?}"),
                "frames": format!("{:?}", read_frames(response).await),
            }));

            // The response ended, and dropped the Exec.
            assert!(process.stdin.recv().await.is_none());
            assert!(process.output.is_closed());
        }

        insta::assert_json_snapshot!(outcomes);
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn exec_appends_error_frames(pool: sqlx::PgPool) {
        let (server, fake, _sandboxes) = setup(&pool, &["acmeCo/sandbox"]).await;
        let mut processes = fake.processes();
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));

        let stdout = encode(&[sandbox_exec::Output::Stdout(bytes::Bytes::from_static(
            b"some output",
        ))]);
        let mut truncated = stdout.to_vec();
        truncated.extend_from_slice(&stdout[..9]);

        let mut outcomes = Vec::new();
        for (case, items) in [
            (
                "wrapperCrashed",
                vec![
                    Ok(Output::Stdout(truncated.into())),
                    Ok(Output::Stderr(bytes::Bytes::from_static(
                        b"thread 'main' panicked\n",
                    ))),
                    Ok(Output::Exit(101)),
                ],
            ),
            ("connectionLost", vec![Ok(Output::Stdout(stdout.clone()))]),
            (
                "gone",
                vec![
                    Ok(Output::Stdout(stdout.clone())),
                    Err(crate::sandboxes::provider::Error::Gone),
                ],
            ),
        ] {
            let response = exec(&server, Some(&alice_token), request("acmeCo/sandbox", None)).await;
            let mut process = processes.recv().await.unwrap();
            read_inputs(&mut process).await;

            for item in items {
                process.output.send(item).unwrap();
            }
            drop(process.output);

            outcomes.push(format!("{case}: {:?}", read_frames(response).await));
        }

        insta::assert_debug_snapshot!((outcomes, state(&pool, "acmeCo/sandbox").await));
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn exec_cancels_when_client_disconnects(pool: sqlx::PgPool) {
        let (server, fake, _sandboxes) = setup(&pool, &["acmeCo/sandbox"]).await;
        let mut processes = fake.processes();
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));

        let mut response = exec(&server, Some(&alice_token), request("acmeCo/sandbox", None)).await;
        let mut process = processes.recv().await.unwrap();
        read_inputs(&mut process).await;

        let started = encode(&[sandbox_exec::Output::Stdout(bytes::Bytes::from_static(
            b"started\n",
        ))]);
        process
            .output
            .send(Ok(Output::Stdout(started.clone())))
            .unwrap();
        assert_eq!(response.chunk().await.unwrap(), Some(started));

        // The command is running, and its output stream is open.
        assert!(!process.output.is_closed());

        // The client goes away. The server drops the response body, which
        // owns the Exec: the provider's stdin and output are dropped.
        drop(response);
        let dropped = async {
            process.output.closed().await;
            while process.stdin.recv().await.is_some() {}
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), dropped)
            .await
            .expect("the Exec is dropped after the client disconnects");
    }

    async fn next_input(process: &mut Process, buf: &mut bytes::BytesMut) -> sandbox_exec::Input {
        loop {
            if let Some(input) = sandbox_exec::Input::decode(buf).unwrap() {
                return input;
            }
            buf.extend_from_slice(&process.stdin.recv().await.unwrap());
        }
    }

    async fn next_frame(
        response: &mut reqwest::Response,
        buf: &mut bytes::BytesMut,
    ) -> Option<sandbox_exec::Output> {
        loop {
            if let Some(frame) = sandbox_exec::Output::decode(buf).unwrap() {
                return Some(frame);
            }
            let chunk = response.chunk().await.unwrap()?;
            buf.extend_from_slice(&chunk);
        }
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn exec_streams_stdin_over_http2(pool: sqlx::PgPool) {
        let (server, fake, _sandboxes) = setup(&pool, &["acmeCo/sandbox"]).await;
        let mut processes = fake.processes();
        let alice_token = server.make_access_token(ALICE, Some("alice@example.test"));

        // The client sends stdin as it has it, while it reads output.
        let (body_tx, body_rx) =
            futures::channel::mpsc::unbounded::<Result<bytes::Bytes, std::io::Error>>();
        let mut first = body(serde_json::json!({
            "catalog_name": "acmeCo/sandbox",
            "workspace": "ws",
            "command": "cat",
        }));
        first.extend_from_slice(b"first");
        body_tx.unbounded_send(Ok(first.into())).unwrap();

        let mut response = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .unwrap()
            .post(server.base_url().join("/api/v1/sandboxes/exec").unwrap())
            .bearer_auth(&alice_token)
            .body(reqwest::Body::wrap_stream(body_rx))
            .send()
            .await
            .unwrap();
        assert_eq!(response.version(), reqwest::Version::HTTP_2);
        assert_eq!(response.status(), reqwest::StatusCode::OK);

        let mut process = processes.recv().await.unwrap();
        let (mut in_buf, mut out_buf) = (bytes::BytesMut::new(), bytes::BytesMut::new());
        let mut events = Vec::new();

        let sandbox_exec::Input::Header(_) = next_input(&mut process, &mut in_buf).await else {
            panic!("the first frame is a Header");
        };
        events.push(format!(
            "wrapper read {:?}",
            next_input(&mut process, &mut in_buf).await
        ));

        // The command's output reaches the client before it sends more stdin.
        let echo = encode(&[sandbox_exec::Output::Stdout(bytes::Bytes::from_static(
            b"first",
        ))]);
        process.output.send(Ok(Output::Stdout(echo))).unwrap();
        events.push(format!(
            "client read {:?}",
            next_frame(&mut response, &mut out_buf).await
        ));

        body_tx
            .unbounded_send(Ok(bytes::Bytes::from_static(b"second")))
            .unwrap();
        events.push(format!(
            "wrapper read {:?}",
            next_input(&mut process, &mut in_buf).await
        ));

        // The end of the body is the command's stdin EOF, which doesn't end
        // the command.
        drop(body_tx);
        events.push(format!(
            "wrapper read {:?}",
            next_input(&mut process, &mut in_buf).await
        ));

        let exit = encode(&[sandbox_exec::Output::Exit {
            code: Some(0),
            signal: None,
        }]);
        process.output.send(Ok(Output::Stdout(exit))).unwrap();
        events.push(format!(
            "client read {:?}",
            next_frame(&mut response, &mut out_buf).await
        ));
        events.push(format!(
            "client read {:?}",
            next_frame(&mut response, &mut out_buf).await
        ));

        insta::assert_debug_snapshot!(events, @r#"
        [
            "wrapper read Stdin(b\"first\")",
            "client read Some(Stdout(b\"first\"))",
            "wrapper read Stdin(b\"second\")",
            "wrapper read StdinEof",
            "client read Some(Exit { code: Some(0), signal: None })",
            "client read None",
        ]
        "#);
    }
}

/// End-to-end tests through the Docker provider and the real command wrapper.
/// They need a database, Docker, the `flow-sandbox:local` image
/// (`mise run local:sandbox-image`), and a debug flowctl build
/// (`cargo build -p flowctl`), so they're ignored by default. Run them with:
///
///   mise exec -- cargo nextest run -p control-plane-api --lib docker_test --run-ignored only
#[cfg(test)]
mod docker_test {
    use crate::sandboxes::docker::{Docker, SCOPE_LABEL};
    use std::sync::Arc;

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);

    /// Removes the containers of `scope` when dropped, including on a panic.
    struct Cleanup {
        scope: String,
    }

    impl Drop for Cleanup {
        fn drop(&mut self) {
            let output = std::process::Command::new("docker")
                .args(["ps", "--all", "--quiet", "--filter"])
                .arg(format!("label={SCOPE_LABEL}={}", self.scope))
                .output()
                .expect("docker ps");
            let ids = String::from_utf8(output.stdout).unwrap();
            let ids: Vec<&str> = ids.lines().collect();
            if ids.is_empty() {
                return;
            }
            let status = std::process::Command::new("docker")
                .args(["rm", "--force"])
                .args(&ids)
                .stdout(std::process::Stdio::null())
                .status()
                .expect("docker rm");
            // Not a panic: this may run while unwinding from one.
            if !status.success() {
                eprintln!("failed to remove test containers {ids:?}");
            }
        }
    }

    fn docker() -> (Docker, Cleanup) {
        let scope = format!("test-{}", uuid::Uuid::new_v4().simple());

        // Test binaries are built into `<target>/debug/deps/`.
        let exe = std::env::current_exe().unwrap();
        let flowctl = exe.parent().unwrap().parent().unwrap().join("flowctl");
        assert!(
            flowctl.is_file(),
            "{} is missing: run `cargo build -p flowctl`",
            flowctl.display()
        );

        let docker = Docker {
            image: "flow-sandbox:local".to_string(),
            flowctl,
            mounts: Vec::new(),
            scope: scope.clone(),
        };
        (docker, Cleanup { scope })
    }

    async fn exec(
        server: &crate::test_server::TestServer,
        token: &str,
        body: serde_json::Value,
    ) -> reqwest::Response {
        let response = reqwest::Client::new()
            .post(server.base_url().join("/api/v1/sandboxes/exec").unwrap())
            .bearer_auth(token)
            .body(super::test::body(body))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        response
    }

    /// Reads a command's response to its end, returning its stdout, stderr,
    /// and terminal frame.
    async fn read_to_end(
        response: reqwest::Response,
    ) -> (String, String, Option<sandbox_exec::Output>) {
        let mut body = bytes::BytesMut::from(&response.bytes().await.unwrap()[..]);
        let (mut stdout, mut stderr, mut last) = (String::new(), String::new(), None);
        while let Some(frame) = sandbox_exec::Output::decode(&mut body).unwrap() {
            match frame {
                sandbox_exec::Output::Stdout(data) => {
                    stdout.push_str(&String::from_utf8_lossy(&data))
                }
                sandbox_exec::Output::Stderr(data) => {
                    stderr.push_str(&String::from_utf8_lossy(&data))
                }
                terminal => last = Some(terminal),
            }
        }
        assert!(body.is_empty());
        (stdout, stderr, last)
    }

    /// Runs a bash `script` in the container directly, returning its stdout.
    async fn container_output(handle: &str, script: &str) -> String {
        let output = async_process::output(
            async_process::Command::new("docker").args(["exec", handle, "bash", "-c", script]),
        )
        .await
        .unwrap();
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    #[ignore = "requires Docker, the flow-sandbox:local image, and a debug flowctl build"]
    async fn docker_end_to_end(pool: sqlx::PgPool) {
        sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1, 'alice@example.test')")
            .bind(ALICE)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenants (tenant) VALUES ('acmeCo/')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO user_grants (user_id, object_role, capability, bundles)
             VALUES ($1, 'acmeCo/', 'read', ARRAY['create_sandbox']::capability_bundle[])",
        )
        .bind(ALICE)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO internal.tenant_consent
                 (user_id, user_email, terms_id, tenant_name, tenant_id)
             SELECT $1, 'alice@example.test', l.id, t.tenant, t.id
             FROM internal.legal_terms l CROSS JOIN tenants t
             WHERE l.type = 'sandbox' AND l.version = 1",
        )
        .bind(ALICE)
        .execute(&pool)
        .await
        .unwrap();

        let (docker, _cleanup) = docker();
        let providers = crate::sandboxes::Providers::new(
            vec![Arc::new(docker) as Arc<dyn crate::sandboxes::provider::Provider>],
            Some("docker"),
        )
        .unwrap();
        let server = crate::test_server::TestServer::start_with_sandbox_providers(
            pool.clone(),
            crate::test_server::snapshot(pool.clone(), false).await,
            providers,
        )
        .await;
        let token = server.make_access_token(ALICE, Some("alice@example.test"));

        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({"query": r#"mutation {
                    created: sandboxCreate(catalogName: "acmeCo/sandbox") { state }
                }"#}),
                Some(&token),
            )
            .await;
        assert_eq!(response["data"]["created"]["state"], "READY", "{response}");
        let response: serde_json::Value = server
            .graphql(
                &serde_json::json!({"query": r#"mutation {
                    approved: sandboxCredentialApprove(
                        catalogName: "acmeCo/sandbox", workspace: "ws", name: "reader",
                        capabilityMask: ["viewer"], prefixMask: ["acmeCo/"], durationMinutes: 60
                    ) { name }
                }"#}),
                Some(&token),
            )
            .await;
        assert_eq!(response["data"]["approved"]["name"], "reader", "{response}");

        let handle = crate::sandboxes::fetch(&pool, &["acmeCo/sandbox".to_string()], &[])
            .await
            .unwrap()
            .pop()
            .unwrap()
            .handle;

        // The command decodes its token's claims, and reads its stdin.
        let command = r#"
            payload=$(printf '%s' "$FLOW_AUTH_TOKEN" | cut -d. -f2 | tr '_-' '/+')
            while [ $(( ${#payload} % 4 )) -ne 0 ]; do payload="$payload="; done
            claims=$(printf '%s' "$payload" | base64 -d)
            printf '%s' "$claims" | grep -o '"sub":"[^"]*"'
            printf '%s' "$claims" | grep -o '"capability_mask":\[[^]]*\]'
            printf '%s' "$claims" | grep -o '"prefix_mask":\[[^]]*\]'
            echo "home: $HOME"
            echo "stdin: $(cat)"
            echo "to stderr" >&2
            exit 7
        "#;
        let response = exec(
            &server,
            &token,
            serde_json::json!({
                "catalog_name": "acmeCo/sandbox",
                "workspace": "ws",
                "command": command,
                "credential": "reader",
                "stdin": "some input",
            }),
        )
        .await;

        let with_credential = read_to_end(response).await;

        // Without a credential, the token identifies alice and grants nothing.
        let response = exec(
            &server,
            &token,
            serde_json::json!({
                "catalog_name": "acmeCo/sandbox",
                "workspace": "ws",
                "command": command,
                "stdin": "more input",
            }),
        )
        .await;
        let without_credential = read_to_end(response).await;

        insta::assert_debug_snapshot!((with_credential, without_credential));

        // A command which the client abandons mid-stream.
        let mut response = exec(
            &server,
            &token,
            serde_json::json!({
                "catalog_name": "acmeCo/sandbox",
                "workspace": "hang",
                "command": "echo $$ > pid; echo started; exec sleep 600",
            }),
        )
        .await;
        let mut buf = bytes::BytesMut::new();
        let started = loop {
            buf.extend_from_slice(&response.chunk().await.unwrap().unwrap());
            if let Some(frame) = sandbox_exec::Output::decode(&mut buf).unwrap() {
                break frame;
            }
        };
        assert_eq!(
            started,
            sandbox_exec::Output::Stdout(bytes::Bytes::from_static(b"started\n"))
        );
        let pid = container_output(&handle, "cat /work/hang/pid").await;
        let pid = pid.trim();

        // Count wrapper processes, and whether the command's process exists.
        let script = format!(
            r#"
            wrappers=0
            for cmdline in /proc/[0-9]*/cmdline; do
                case "$(tr '\0' ' ' < "$cmdline" 2>/dev/null)" in
                    "/opt/estuary/bin/flowctl raw sandbox-exec "*) wrappers=$((wrappers + 1)) ;;
                esac
            done
            if [ -e "/proc/{pid}" ]; then command=1; else command=0; fi
            echo "$wrappers $command"
            "#
        );
        assert_eq!(container_output(&handle, &script).await.trim(), "1 1");

        let dropped = std::time::Instant::now();
        drop(response);

        loop {
            let counts = container_output(&handle, &script).await;
            if counts.trim() == "0 0" {
                break;
            }
            assert!(
                dropped.elapsed() < std::time::Duration::from_secs(10),
                "processes (wrappers, command) still running: {counts}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        eprintln!(
            "command and wrapper exited {:?} after the client disconnected",
            dropped.elapsed()
        );
    }
}
