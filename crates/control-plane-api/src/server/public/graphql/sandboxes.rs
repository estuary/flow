//! Sandboxes: per-user Linux VMs that run shell commands, backed by Fly.io
//! Sprites. See `crate::sprites` for the API client and `crate::sandboxes` for
//! the records and lifecycle these operations drive.

use async_graphql::Context;
use std::sync::Arc;

/// A user's sandbox: a persistent Linux VM that runs their shell commands.
///
/// The provider's own name for the VM stays internal: clients address a
/// sandbox by its `catalogName`, scoped to the authenticated user. Creation
/// is authorized on that catalog name.
#[derive(Debug, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct Sandbox {
    /// Catalog name used to address this sandbox in queries and mutations.
    pub catalog_name: models::Name,
    /// When the sandbox was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[async_graphql::ComplexObject]
impl Sandbox {
    /// Commands run in this sandbox, newest first. Each is the record of a
    /// command that started, with its observed exit result. Deletion
    /// discards them along with their output.
    async fn execs(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<ExecEvent>> {
        let env = ctx.data::<crate::Envelope>()?;

        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, self.catalog_name.as_str()).await?;
        let execs = crate::sandboxes::list_execs(&client, &sandbox)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to list execs");
                async_graphql::Error::new("failed to list execs")
            })?;

        Ok(execs.into_iter().map(ExecEvent::from).collect())
    }
}

impl From<crate::sandboxes::Sandbox> for Sandbox {
    fn from(sandbox: crate::sandboxes::Sandbox) -> Self {
        Self {
            catalog_name: models::Name::new(sandbox.catalog_name),
            created_at: sandbox.created_at,
        }
    }
}

/// A command run in a sandbox: what ran, when it was requested, and where the
/// sandbox holds its output. Read output with `sandboxFileRead` and poll
/// `sandbox.execs` for the exit result.
#[derive(Debug, async_graphql::SimpleObject)]
pub struct ExecEvent {
    /// Server-generated identifier of this command execution.
    pub exec_id: models::Id,
    /// The bash command that ran.
    pub command: String,
    /// When the command was requested. It started shortly after.
    pub requested_at: chrono::DateTime<chrono::Utc>,
    /// Path of the file the command's stdout is written to, to pass as `path`
    /// to `sandboxFileRead`.
    pub stdout_path: String,
    /// Path of the file the command's stderr is written to, to pass as `path`
    /// to `sandboxFileRead`.
    pub stderr_path: String,
    /// Exit code observed when listing execs, or null if completion has not
    /// been observed. Zero means success. The initial `sandboxExec` response
    /// returns null because it acknowledges startup without waiting for exit.
    pub exit_result: Option<i32>,
}

impl From<crate::sandboxes::ExecEvent> for ExecEvent {
    fn from(event: crate::sandboxes::ExecEvent) -> Self {
        use crate::sandboxes::ExecFile;

        Self {
            exec_id: event.id,
            command: event.command,
            requested_at: event.requested_at,
            stdout_path: ExecFile::Stdout.path(event.id),
            stderr_path: ExecFile::Stderr.path(event.id),
            exit_result: event.exit_result,
        }
    }
}

/// A read of a sandbox file.
#[derive(Debug, async_graphql::SimpleObject)]
#[graphql(complex)]
pub struct FileRead {
    #[graphql(skip)]
    pub data: Vec<u8>,
    /// Byte offset into the file after this chunk. Pass it as `offset` on the next
    /// read. It is the requested offset when the file does not exist.
    pub offset: i32,
    /// Whether the file was there. A file the sandbox writes later, such as a
    /// running command's output, reports `false` until it appears, so this
    /// is an answer rather than a failure.
    pub exists: bool,
}

#[async_graphql::ComplexObject]
impl FileRead {
    /// Raw file bytes encoded as standard padded base64. Empty when the file
    /// does not exist. Offset and limit count raw bytes, not encoded characters.
    async fn data(&self) -> String {
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &self.data)
    }

    /// File bytes decoded as UTF-8, replacing invalid sequences with U+FFFD.
    /// Byte offsets and limits may split a multi-byte character, so replacements
    /// can appear at chunk boundaries even for valid UTF-8 files. For lossless
    /// text across reads, use `data` with a streaming UTF-8 decoder.
    /// Empty when the file does not exist.
    async fn text(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

#[derive(Debug, Default)]
pub struct SandboxesQuery;

#[async_graphql::Object]
impl SandboxesQuery {
    /// Look up one of the authenticated user's sandboxes by `catalogName`.
    ///
    /// A sandbox that is not theirs, and one since deleted, are both null: the
    /// answer says nothing about whether the catalog name names anything at all.
    async fn sandbox(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<Option<Sandbox>> {
        let env = ctx.data::<crate::Envelope>()?;

        Ok(fetch_sandbox(env, catalog_name.as_str())
            .await?
            .map(Sandbox::from))
    }

    /// List the authenticated user's sandboxes, newest first.
    async fn sandboxes(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<Sandbox>> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let sandboxes = crate::sandboxes::list(&env.pg_pool, claims.sub)
            .await
            .map_err(|err| {
                tracing::error!(?err, %claims.sub, "failed to list sandboxes");
                async_graphql::Error::new("failed to list sandboxes")
            })?;

        Ok(sandboxes.into_iter().map(Sandbox::from).collect())
    }

    /// Read `path` in the authenticated user's sandbox `catalogName`, resolved
    /// against the sandbox user's home directory, from byte `offset` onwards.
    /// `path` must be relative and must not contain `..` components. A read
    /// returns at most `limit` bytes, and never more than 1 MiB, so continue a
    /// longer file from the `offset` you are handed.
    /// Offset and limit count raw file bytes for both `data` and `text`, before
    /// base64 encoding or UTF-8 decoding.
    ///
    /// A path that does not exist returns `exists: false` rather than an
    /// error, because a sandbox writes files while a client watches for them.
    ///
    /// This reads a command's result too. An `ExecEvent` carries
    /// `stdoutPath` and `stderrPath`; read them while the command runs or
    /// afterwards, and poll `sandbox.execs` for a non-null `exitResult`.
    /// The wrapper records that result after the command's last output, so once
    /// you see it, read output through EOF from your current offset to get the
    /// remainder.
    async fn sandbox_file_read(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        path: String,
        #[graphql(desc = "Byte offset to read from", default = 0)] offset: i32,
        #[graphql(
            desc = "Most bytes to return. Omitted, or larger than the 1 MiB ceiling, reads up to that ceiling."
        )]
        limit: Option<i32>,
    ) -> async_graphql::Result<FileRead> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        let offset = u64::try_from(offset)
            .map_err(|_| async_graphql::Error::new("offset must not be negative"))?;
        let limit = limit
            .map(u64::try_from)
            .transpose()
            .map_err(|_| async_graphql::Error::new("limit must not be negative"))?;

        let chunk = crate::sandboxes::read_file(&client, &sandbox, &path, offset, limit)
            .await
            .map_err(|err| match err {
                crate::sandboxes::FileReadError::Other(err) => {
                    tracing::error!(?err, %sandbox.id, %path, "failed to read sandbox file");
                    async_graphql::Error::new(format!("failed to read sandbox file: {err:#}"))
                }
                refused => async_graphql::Error::new(refused.to_string()),
            })?;

        Ok(FileRead {
            data: chunk.bytes,
            // GraphQL `Int` is 32 bits, so a file beyond two gigabytes saturates.
            offset: i32::try_from(chunk.offset).unwrap_or(i32::MAX),
            exists: chunk.exists,
        })
    }
}

#[derive(Debug, Default)]
pub struct SandboxesMutation;

#[async_graphql::Object]
impl SandboxesMutation {
    /// Create a sandbox for the authenticated user at `catalogName`.
    /// Requires CreateSandbox on that catalog name, which must differ from the
    /// caller's other live sandboxes. A deleted sandbox frees its catalog name.
    ///
    /// When this returns the sandbox is ready to use: it accepts commands,
    /// and flowctl is installed.
    ///
    /// Users hold a limited number of sandboxes, one today, so this fails when
    /// the caller is at that limit.
    async fn sandbox_create(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<Sandbox> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;
        validator::Validate::validate(&catalog_name)
            .map_err(|err| async_graphql::Error::new(format!("invalid catalog name: {err}")))?;
        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::CreateSandbox,
        )
        .await?;
        let client = sprites_client(ctx)?;

        // A refused name or limit is the caller's to fix and says so in its
        // message; only a failure of the control plane or provider is logged.
        let sandbox =
            crate::sandboxes::create(&client, &env.pg_pool, claims.sub, catalog_name.as_str())
                .await
                .map_err(|err| match err {
                    crate::sandboxes::CreateError::Other(err) => {
                        tracing::error!(?err, %claims.sub, "failed to create sandbox");
                        async_graphql::Error::new(format!("failed to create sandbox: {err:#}"))
                    }
                    refused => async_graphql::Error::new(refused.to_string()),
                })?;

        tracing::info!(%claims.sub, %sandbox.id, "created sandbox");
        Ok(sandbox.into())
    }

    /// Run a bash command in the authenticated user's sandbox `catalogName`.
    /// This returns once the command is running, without waiting for it to
    /// finish, and the command then runs for as long as it takes. The returned
    /// `ExecEvent` carries the paths its result lands at: read `stdoutPath`
    /// and `stderrPath` with `sandboxFileRead` while the command runs or
    /// afterwards, and poll `sandbox.execs` for `exitResult`. A command that
    /// fails to start is not recorded, and this returns the failure instead.
    ///
    /// The command is passed to `bash -lc`, so it may use shell syntax such as
    /// pipes and redirection. The server generates a new `execId` for each call.
    async fn sandbox_exec(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        command: String,
        #[graphql(
            desc = "Complete UTF-8 stdin (at most 1 MiB), followed by EOF. Omitted or empty input gives immediate EOF. Sent in the request body, not stored in the exec record."
        )]
        stdin: Option<String>,
    ) -> async_graphql::Result<ExecEvent> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        let exec_id = ctx
            .data::<Arc<crate::App>>()?
            .id_generator
            .lock()
            .unwrap()
            .next();

        // The command may carry credentials, so it is recorded in
        // exec metadata and logged nowhere.
        let event = crate::sandboxes::exec(&client, exec_id, &sandbox, &command, stdin.as_deref())
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to start sandbox command");
                async_graphql::Error::new(format!("failed to start command: {err:#}"))
            })?;

        tracing::info!(%sandbox.id, %event.id, "started sandbox command");
        Ok(event.into())
    }

    /// Delete the authenticated user's sandbox `catalogName`, including its
    /// filesystem and the record of commands run in it. The deleted sandbox no
    /// longer counts against the user's limit.
    async fn sandbox_delete(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
    ) -> async_graphql::Result<bool> {
        let env = ctx.data::<crate::Envelope>()?;
        let client = sprites_client(ctx)?;
        let sandbox = resolve_sandbox(env, catalog_name.as_str()).await?;

        crate::sandboxes::delete(&client, &env.pg_pool, &sandbox)
            .await
            .map_err(|err| {
                tracing::error!(?err, %sandbox.id, "failed to delete sandbox");
                async_graphql::Error::new(format!("failed to delete sandbox: {err:#}"))
            })?;

        Ok(true)
    }
}

/// Fetches sandbox `catalog_name` if it is the caller's. A sandbox that is not theirs,
/// and one that no longer exists, are both absent, so an answer built from
/// this cannot tell them apart.
async fn fetch_sandbox(
    env: &crate::Envelope,
    catalog_name: &str,
) -> async_graphql::Result<Option<crate::sandboxes::Sandbox>> {
    let claims = env.claims()?;

    crate::sandboxes::fetch_by_catalog_name(&env.pg_pool, catalog_name, claims.sub)
        .await
        .map_err(|err| {
            tracing::error!(?err, %catalog_name, %claims.sub, "failed to look up sandbox");
            async_graphql::Error::new("failed to look up sandbox")
        })
}

/// Resolves sandbox `catalog_name` for an operation that must act on one, turning the
/// absence [`fetch_sandbox`] reports into the error the caller sees.
async fn resolve_sandbox(
    env: &crate::Envelope,
    catalog_name: &str,
) -> async_graphql::Result<crate::sandboxes::Sandbox> {
    fetch_sandbox(env, catalog_name)
        .await?
        .ok_or_else(|| async_graphql::Error::new("sandbox not found"))
}

fn sprites_client(ctx: &Context<'_>) -> async_graphql::Result<Arc<crate::sprites::Client>> {
    ctx.data::<Arc<crate::sprites::Client>>()
        .cloned()
        .map_err(|_| async_graphql::Error::new("Sandboxes are not configured"))
}

#[cfg(test)]
mod test {
    use crate::test_server;

    #[tokio::test]
    async fn file_read_representations() {
        let mut results = std::collections::BTreeMap::new();
        for (name, data, offset, exists) in [
            ("utf8", "a€z".as_bytes(), 5, true),
            ("binary", &b"a\xff\0z"[..], 4, true),
            ("split_start", &b"\x82\xacz"[..], 5, true),
            ("split_end", &b"a\xe2"[..], 2, true),
            ("missing", &b""[..], 7, false),
            ("eof", &b""[..], 7, true),
        ] {
            let schema = async_graphql::Schema::build(
                super::FileRead {
                    data: data.to_vec(),
                    offset,
                    exists,
                },
                async_graphql::EmptyMutation,
                async_graphql::EmptySubscription,
            )
            .finish();
            let response = schema.execute("{ data text offset exists }").await;
            assert!(response.errors.is_empty(), "{:?}", response.errors);
            results.insert(name, response.data.into_json().unwrap());
        }
        insta::assert_json_snapshot!(results, @r###"
        {
          "binary": {
            "data": "Yf8Aeg==",
            "exists": true,
            "offset": 4,
            "text": "a�\u0000z"
          },
          "eof": {
            "data": "",
            "exists": true,
            "offset": 7,
            "text": ""
          },
          "missing": {
            "data": "",
            "exists": false,
            "offset": 7,
            "text": ""
          },
          "split_end": {
            "data": "YeI=",
            "exists": true,
            "offset": 2,
            "text": "a�"
          },
          "split_start": {
            "data": "gqx6",
            "exists": true,
            "offset": 5,
            "text": "��z"
          },
          "utf8": {
            "data": "YeKCrHo=",
            "exists": true,
            "offset": 5,
            "text": "a€z"
          }
        }
        "###);
    }

    const ALICE: uuid::Uuid = uuid::Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
    const BOB: uuid::Uuid = uuid::Uuid::from_u128(0x2222_2222_2222_2222_2222_2222_2222_2222);

    /// A stand-in for the Sprites API that answers every request with an
    /// error and counts the requests it received. A provider is reached at
    /// most once per sandbox operation, so the count says whether an
    /// operation got past its authorization.
    async fn counting_provider() -> (std::sync::Arc<std::sync::atomic::AtomicUsize>, url::Url) {
        let requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = requests.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new().fallback(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        });
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

        (
            requests,
            url::Url::parse(&format!("http://{addr}")).unwrap(),
        )
    }

    /// Posts `sandboxFileRead` against `catalog_name` as `token`'s user, with
    /// `args` the remaining arguments as GraphQL source, so a case can pass
    /// one the schema's own types would not carry in a variable.
    async fn file_read(
        server: &test_server::TestServer,
        token: &str,
        catalog_name: &str,
        args: &str,
    ) -> serde_json::Value {
        server
            .graphql(
                &serde_json::json!({
                    "query": format!(
                        r#"query Read($catalogName: Name!) {{
                            sandboxFileRead(catalogName: $catalogName, {args}) {{
                                data text offset exists
                            }}
                        }}"#
                    ),
                    "variables": { "catalogName": catalog_name },
                }),
                Some(token),
            )
            .await
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_create_requires_capability_on_catalog_name(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let (requests, base_url) = counting_provider().await;
        let sprites = std::sync::Arc::new(crate::sprites::Client::with_base_url(
            "token".to_string(),
            base_url,
        ));

        for (bundle, catalog_name, expected_error, expected_requests) in [
            ("editor", "aliceCo/dev", "PermissionDenied", 0),
            ("admin", "otherCo/dev", "PermissionDenied", 0),
            ("admin", "invalid name", "invalid catalog name", 0),
            ("admin", "aliceCo/dev", "failed to create sandbox", 1),
        ] {
            sqlx::query(
                "UPDATE public.user_grants SET capability = 'none',
                 bundles = ARRAY[$1::capability_bundle]
                 WHERE user_id = $2 AND object_role = 'aliceCo/'",
            )
            .bind(bundle)
            .bind(ALICE)
            .execute(&pool)
            .await
            .unwrap();
            let server = test_server::TestServer::start_with_sprites(
                pool.clone(),
                test_server::snapshot(pool.clone(), false).await,
                sprites.clone(),
            )
            .await;
            let token = server.make_access_token(ALICE, None);
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": "mutation Create($name: Name!) { sandboxCreate(catalogName: $name) { catalogName createdAt } }",
                        "variables": { "name": catalog_name },
                    }),
                    Some(&token),
                )
                .await;
            assert!(
                response["errors"][0]["message"]
                    .as_str()
                    .unwrap()
                    .contains(expected_error),
                "{response}",
            );
            assert_eq!(
                requests.load(std::sync::atomic::Ordering::SeqCst),
                expected_requests
            );
            // Authorization must fail before consuming a slot. Only the allowed
            // create leaves a record when this provider refuses provisioning.
            assert_eq!(
                crate::sandboxes::list(&pool, ALICE).await.unwrap().len(),
                expected_requests,
            );
        }
        assert_eq!(
            crate::sandboxes::list(&pool, ALICE).await.unwrap()[0].catalog_name,
            "aliceCo/dev",
        );
    }

    /// A foreign catalog name must not disclose sandbox files or reach the provider.
    /// The owner's call is the control proving authorization gates the read.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_file_read_refuses_a_foreign_sandbox_before_reaching_the_provider(
        pool: sqlx::PgPool,
    ) {
        let _guard = test_server::init();

        let alices = crate::sandboxes::insert_record(&pool, ALICE, "dev")
            .await
            .unwrap();

        let (requests, base_url) = counting_provider().await;
        let sprites = std::sync::Arc::new(crate::sprites::Client::with_base_url(
            "token".to_string(),
            base_url,
        ));
        let server = test_server::TestServer::start_with_sprites(
            pool.clone(),
            test_server::snapshot(pool, false).await,
            sprites,
        )
        .await;

        let bobs_token = server.make_access_token(BOB, Some("bob@example.com"));
        let response = file_read(
            &server,
            &bobs_token,
            &alices.catalog_name,
            r#"path: "dest.txt""#,
        )
        .await;

        assert_eq!(response["errors"][0]["message"], "sandbox not found");
        assert!(response["data"]["sandboxFileRead"].is_null());
        assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 0);

        let alices_token = server.make_access_token(ALICE, Some("alice@example.com"));
        let response = file_read(
            &server,
            &alices_token,
            &alices.catalog_name,
            r#"path: "dest.txt""#,
        )
        .await;

        assert!(
            response["errors"][0]["message"]
                .as_str()
                .unwrap()
                .starts_with("failed to read sandbox file"),
            "{response}"
        );
        assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// Execs are authorized by their sandbox, even though their metadata is
    /// now in the VM. Foreign requests must never reach the provider.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_operations_authorize_the_sandbox(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let sandbox = crate::sandboxes::insert_record(&pool, ALICE, "dev")
            .await
            .unwrap();
        let (requests, base_url) = counting_provider().await;
        let server = test_server::TestServer::start_with_sprites(
            pool.clone(),
            test_server::snapshot(pool, false).await,
            std::sync::Arc::new(crate::sprites::Client::with_base_url(
                "token".to_string(),
                base_url,
            )),
        )
        .await;
        let queries = [
            "query Execs($name: Name!) { sandbox(catalogName: $name) { execs { execId command requestedAt } } }",
            "mutation Exec($name: Name!) { sandboxExec(catalogName: $name, command: \"true\") { execId } }",
            "mutation Delete($name: Name!) { sandboxDelete(catalogName: $name) }",
        ];
        for (user, expected_requests) in [(BOB, 0), (ALICE, 3)] {
            let token = server.make_access_token(user, None);
            for query in queries {
                let response: serde_json::Value = server
                    .graphql(
                        &serde_json::json!({
                            "query": query, "variables": { "name": sandbox.catalog_name },
                        }),
                        Some(&token),
                    )
                    .await;
                if user == BOB && query.starts_with("query") {
                    assert!(response["errors"].is_null(), "{response}");
                    assert!(response["data"]["sandbox"].is_null(), "{response}");
                } else {
                    let message = response["errors"][0]["message"].as_str().unwrap();
                    if user == BOB {
                        assert_eq!(message, "sandbox not found");
                    } else {
                        assert!(message.starts_with("failed to "), "{response}");
                    }
                }
            }
            assert_eq!(
                requests.load(std::sync::atomic::Ordering::SeqCst),
                expected_requests
            );
        }
    }

    /// Each call gets a fresh ID that agrees with the wrapper's metadata and
    /// the returned output paths.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_exec_generates_ids(pool: sqlx::PgPool) {
        let _guard = test_server::init();
        let sandbox = crate::sandboxes::insert_record(&pool, ALICE, "dev")
            .await
            .unwrap();
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = requests.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new().fallback(
            move |axum::extract::Query(args): axum::extract::Query<Vec<(String, String)>>| {
                captured.lock().unwrap().push(args);
                async { b"\x01started\n".to_vec() }
            },
        );
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let server = test_server::TestServer::start_with_sprites(
            pool.clone(),
            test_server::snapshot(pool, false).await,
            std::sync::Arc::new(crate::sprites::Client::with_base_url(
                "token".to_string(),
                url::Url::parse(&format!("http://{addr}")).unwrap(),
            )),
        )
        .await;
        let token = server.make_access_token(ALICE, None);
        let mut exec_ids = std::collections::BTreeSet::new();
        for index in 0..2 {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"mutation Exec($sandbox: Name!) {
                            sandboxExec(catalogName: $sandbox, command: "true") {
                                execId stdoutPath stderrPath exitResult
                            }
                        }"#,
                        "variables": { "sandbox": sandbox.catalog_name },
                    }),
                    Some(&token),
                )
                .await;
            assert!(response["errors"].is_null(), "{response}");
            let event = &response["data"]["sandboxExec"];
            let exec_id = event["execId"].as_str().unwrap();
            let id = models::Id::from_hex(exec_id).unwrap();
            assert!(
                exec_ids.insert(id),
                "each mutation must generate a fresh ID"
            );
            assert_eq!(
                event["stdoutPath"],
                crate::sandboxes::ExecFile::Stdout.path(id)
            );
            assert_eq!(
                event["stderrPath"],
                crate::sandboxes::ExecFile::Stderr.path(id)
            );
            insta::assert_json_snapshot!(response, {
                ".data.sandboxExec.execId" => "<exec-id>",
                ".data.sandboxExec.stdoutPath" => ".estuary/exec/<exec-id>/stdout",
                ".data.sandboxExec.stderrPath" => ".estuary/exec/<exec-id>/stderr",
            }, @r###"
            {
              "data": {
                "sandboxExec": {
                  "execId": "<exec-id>",
                  "exitResult": null,
                  "stderrPath": ".estuary/exec/<exec-id>/stderr",
                  "stdoutPath": ".estuary/exec/<exec-id>/stdout"
                }
              }
            }
            "###);
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), index + 1);
            let argv: Vec<&str> = requests[index]
                .iter()
                .filter(|(key, _)| key == "cmd")
                .map(|(_, value)| value.as_str())
                .collect();
            assert_eq!(argv[4], exec_id);
            let metadata: serde_json::Value = serde_json::from_str(argv[7]).unwrap();
            assert_eq!(metadata["id"], exec_id);
        }
    }

    /// The `sandbox` query answers for its owner and nobody else. A foreign
    /// catalog name reads as null, as does a name with no live sandbox, so
    /// the answer cannot be used to learn that a sandbox exists.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_sandbox_query_resolves_for_its_owner_alone(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let alices = crate::sandboxes::insert_record(&pool, ALICE, "dev")
            .await
            .unwrap();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;

        let fetch = async |token: &str, catalog_name: &str| -> serde_json::Value {
            server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        query Fetch($name: Name!) {
                            sandbox(catalogName: $name) { catalogName createdAt }
                        }"#,
                        "variables": { "name": catalog_name },
                    }),
                    Some(token),
                )
                .await
        };

        let alices_token = server.make_access_token(ALICE, Some("alice@example.com"));
        let response = fetch(&alices_token, &alices.catalog_name).await;

        assert_eq!(
            serde_json::from_value::<chrono::DateTime<chrono::Utc>>(
                response["data"]["sandbox"]["createdAt"].clone()
            )
            .unwrap(),
            alices.created_at
        );
        assert_eq!(response["data"]["sandbox"]["catalogName"], "dev");

        // The name of somebody else's sandbox, and one that names nothing, are
        // the same answer.
        let bobs_token = server.make_access_token(BOB, Some("bob@example.com"));
        let response = fetch(&bobs_token, &alices.catalog_name).await;
        assert!(response["data"]["sandbox"].is_null(), "{response}");
        assert!(response["errors"].is_null(), "{response}");

        let unknown = "acmeCo/missing";
        let response = fetch(&alices_token, unknown).await;
        assert!(response["data"]["sandbox"].is_null(), "{response}");
        assert!(response["errors"].is_null(), "{response}");

        // Identical catalog names identify separate per-user sandboxes.
        let bobs = crate::sandboxes::insert_record(&pool, BOB, &alices.catalog_name)
            .await
            .unwrap();
        let response = fetch(&bobs_token, &bobs.catalog_name).await;
        assert_eq!(
            serde_json::from_value::<chrono::DateTime<chrono::Utc>>(
                response["data"]["sandbox"]["createdAt"].clone()
            )
            .unwrap(),
            bobs.created_at
        );
        let response = fetch(&alices_token, &alices.catalog_name).await;
        assert_eq!(
            serde_json::from_value::<chrono::DateTime<chrono::Utc>>(
                response["data"]["sandbox"]["createdAt"].clone()
            )
            .unwrap(),
            alices.created_at
        );

        sqlx::query("UPDATE internal.sandboxes SET deleted_at = now() WHERE id = $1")
            .bind(alices.id)
            .execute(&pool)
            .await
            .unwrap();
        let response = fetch(&alices_token, &alices.catalog_name).await;
        assert!(response["data"]["sandbox"].is_null(), "{response}");
        assert!(response["errors"].is_null(), "{response}");

        let replacement = crate::sandboxes::insert_record(&pool, ALICE, &alices.catalog_name)
            .await
            .unwrap();
        let response = fetch(&alices_token, &alices.catalog_name).await;
        assert_eq!(
            serde_json::from_value::<chrono::DateTime<chrono::Utc>>(
                response["data"]["sandbox"]["createdAt"].clone()
            )
            .unwrap(),
            replacement.created_at
        );
    }

    /// Arguments the sandbox could never serve are refused in the control
    /// plane, so a path that escapes the home directory, or a window that runs
    /// backwards, costs no provider request at all.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_file_read_refuses_bad_arguments_before_reaching_the_provider(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let alices = crate::sandboxes::insert_record(&pool, ALICE, "dev")
            .await
            .unwrap();

        let (requests, base_url) = counting_provider().await;
        let sprites = std::sync::Arc::new(crate::sprites::Client::with_base_url(
            "token".to_string(),
            base_url,
        ));
        let server = test_server::TestServer::start_with_sprites(
            pool.clone(),
            test_server::snapshot(pool, false).await,
            sprites,
        )
        .await;

        let alices_token = server.make_access_token(ALICE, Some("alice@example.com"));

        for (args, message) in [
            (r#"path: "/etc/passwd""#, "path must not be absolute"),
            (
                r#"path: "../../etc/passwd""#,
                "path must not contain '..' components",
            ),
            (r#"path: """#, "path must not be empty"),
            (r#"path: "out", offset: -1"#, "offset must not be negative"),
            (r#"path: "out", limit: -1"#, "limit must not be negative"),
        ] {
            let response = file_read(&server, &alices_token, &alices.catalog_name, args).await;
            assert_eq!(response["errors"][0]["message"], message, "{args}");
        }
        assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}
