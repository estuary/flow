use super::filters;
use async_graphql::{Context, types::connection};

/// A secret, as named in a task's `secrets` stanza.
///
/// This is the *reference* to a secret, never its decrypted content.
/// Listing a secret therefore requires `ViewSecret`, while decrypting
/// one requires `DecryptSecret`.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct Secret {
    pub catalog_name: models::Name,
    /// Lifecycle identity of the secret's current document. Every change to the
    /// document mints a new `secretId`, and ids are time-ordered, so comparing
    /// two observations of a secret also tells you which is newer.
    pub secret_id: models::Id,
}

/// Outcome of `setSecret`.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct SetSecretResult {
    /// The secret in its post-set state.
    pub secret: Secret,
    /// Whether the document changed. False when the provided document was
    /// structurally identical to the stored one, in which case `secret.secretId`
    /// is the id the secret already had.
    pub changed: bool,
}

pub type PaginatedSecrets = connection::Connection<
    String,
    Secret,
    connection::EmptyFields,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

const DEFAULT_PAGE_SIZE: usize = 100;
const MAX_PREFIXES: usize = 20;

/// Optional filter for the `secrets` query. When omitted, every secret the
/// caller may view is returned. A filter only narrows those results.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct SecretsFilter {
    /// Filter on the secret's catalog name.
    pub catalog_name: Option<filters::PrefixFilter>,
}

#[derive(Debug, Default)]
pub struct SecretsQuery;

#[async_graphql::Object]
impl SecretsQuery {
    /// List secrets the caller may view, in catalog-name order.
    ///
    /// Requires `ViewSecret` on a prefix covering each returned name.
    async fn secrets(
        &self,
        ctx: &Context<'_>,
        filter: Option<SecretsFilter>,
        after: Option<String>,
        first: Option<i32>,
    ) -> async_graphql::Result<PaginatedSecrets> {
        let env = ctx.data::<crate::Envelope>()?;

        let snapshot = env.snapshot();
        let (view_prefixes, name_starts_with, name_in) =
            super::authorized_prefixes::filtered_authorized_prefixes(
                &snapshot.role_grants,
                &snapshot.user_grants,
                &env.claims()?.subject(),
                models::authz::Capability::ViewSecret,
                filter.and_then(|f| f.catalog_name),
                "filter.catalogName",
            )?;

        if view_prefixes.is_empty() {
            return Ok(PaginatedSecrets::new(false, false));
        }
        if view_prefixes.len() > MAX_PREFIXES {
            return Err(async_graphql::Error::new(
                "Too many accessible prefixes; narrow results with a filter",
            ));
        }

        connection::query_with::<String, _, _, _, async_graphql::Error>(
            after,
            None,
            first,
            None,
            |after, _, first, _| async move {
                let limit = first.unwrap_or(DEFAULT_PAGE_SIZE);

                let rows = sqlx::query!(
                    r#"
                    SELECT
                        catalog_name AS "catalog_name!: models::Name",
                        id AS "secret_id!: models::Id"
                    FROM internal.secrets
                    WHERE catalog_name::text ^@ ANY($1)
                      AND ($2::text IS NULL OR catalog_name::text > $2)
                      AND ($3::text IS NULL OR catalog_name::text ^@ $3)
                      AND ($5::text[] IS NULL OR catalog_name::text = ANY($5))
                    ORDER BY catalog_name
                    LIMIT $4 + 1
                    "#,
                    &view_prefixes,
                    after.as_deref(),
                    name_starts_with.as_deref(),
                    limit as i64,
                    name_in.as_deref(),
                )
                .fetch_all(&env.pg_pool)
                .await?;

                let has_next = rows.len() > limit;

                let edges: Vec<_> = rows
                    .into_iter()
                    .take(limit)
                    .map(|row| {
                        connection::Edge::new(
                            row.catalog_name.to_string(),
                            Secret {
                                catalog_name: row.catalog_name,
                                secret_id: row.secret_id,
                            },
                        )
                    })
                    .collect();

                let mut conn = connection::Connection::new(after.is_some(), has_next);
                conn.edges = edges;
                Ok(conn)
            },
        )
        .await
    }
}

#[derive(Debug, Default)]
pub struct SecretsMutation;

#[async_graphql::Object]
impl SecretsMutation {
    /// Set a secret to a pre-wrapped document.
    ///
    /// Wrapping and setting are separable steps: `document` is the output of
    /// config-encryption's `/secret/encrypt?name=…` route, which the caller
    /// invokes first.
    ///
    /// Requires `EditSecret` on a prefix covering `catalogName`. The document
    /// must be an object whose `name` equals `catalogName`.
    ///
    /// Setting is idempotent on the document's value: re-applying a
    /// document leaves `secretId` alone and reports `changed: false`. Any other
    /// change mints a new `secretId`. A document whose embedded `sops.lastmodified`
    /// predates the stored one is rejected rather than applied, guarding
    /// against a stale re-apply.
    async fn set_secret(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        // We use Value so serializations are sorted, which lets PostgreSQL
        // check document equality via string equality.
        document: serde_json::Value,
    ) -> async_graphql::Result<SetSecretResult> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        if let Err(err) = validator::Validate::validate(&catalog_name) {
            return Err(async_graphql::Error::new(format!(
                "invalid catalog name: {err}"
            )));
        }
        super::verify_authorization(
            env,
            catalog_name.as_str(),
            models::authz::Capability::EditSecret,
        )
        .await?;

        let last_modified = crate::secrets::validate_document(catalog_name.as_str(), &document)
            .map_err(async_graphql::Error::new)?;

        let (secret_id, changed) = match crate::secrets::set(
            &env.pg_pool,
            catalog_name.as_str(),
            &document,
            last_modified,
        )
        .await?
        {
            crate::secrets::SetOutcome::Written(secret_id) => (secret_id, true),
            crate::secrets::SetOutcome::Unchanged(secret_id) => (secret_id, false),
            crate::secrets::SetOutcome::Stale => {
                return Err(async_graphql::Error::new(format!(
                    "the stored secret '{catalog_name}' is newer than the provided document; \
                     re-encrypt the value you intend to set, or fetch the current document"
                )));
            }
            crate::secrets::SetOutcome::Conflict => {
                return Err(async_graphql::Error::new(format!(
                    "secret '{catalog_name}' was concurrently set by another request; retry"
                )));
            }
        };

        tracing::info!(%catalog_name, %secret_id, changed, %claims.sub, "set secret");

        Ok(SetSecretResult {
            secret: Secret {
                catalog_name,
                secret_id,
            },
            changed,
        })
    }

    /// Delete a secret by name, or every secret under a prefix.
    ///
    /// Exactly one of `catalogName` or `prefix` must be given; the prefix form
    /// is how a recursive delete is expressed. Requires `EditSecret` covering
    /// the name or prefix.
    ///
    /// Returns the names actually deleted, in catalog-name order. Deleting a
    /// secret that does not exist is an idempotent no-op that returns an empty
    /// list.
    ///
    /// Tasks referencing a deleted secret keep running: resolution happens when
    /// a connector starts, so the dangling reference surfaces at the next start
    /// or publication rather than here.
    async fn delete_secret(
        &self,
        ctx: &Context<'_>,
        catalog_name: Option<models::Name>,
        prefix: Option<models::Prefix>,
    ) -> async_graphql::Result<Vec<models::Name>> {
        let env = ctx.data::<crate::Envelope>()?;
        let claims = env.claims()?;

        let (target, valid) = match (&catalog_name, &prefix) {
            (Some(catalog_name), None) => (
                catalog_name.as_str(),
                validator::Validate::validate(catalog_name),
            ),
            (None, Some(prefix)) => (prefix.as_str(), validator::Validate::validate(prefix)),
            _ => {
                return Err(async_graphql::Error::new(
                    "provide exactly one of `catalogName` or `prefix`",
                ));
            }
        };
        if let Err(err) = valid {
            return Err(async_graphql::Error::new(format!(
                "invalid target '{target}': {err}"
            )));
        }
        // The empty string is a valid catalog prefix that would sweep every
        // secret on the platform. No grant can cover it — the `catalog_prefix`
        // DB domain requires at least one token — but a delete this broad
        // shouldn't hinge on that invariant. Reject it outright.
        if target.is_empty() {
            return Err(async_graphql::Error::new(
                "an empty prefix would delete every secret; provide a specific prefix",
            ));
        }

        super::verify_authorization(env, target, models::authz::Capability::EditSecret).await?;

        // Two statements rather than one branching on a parameter, so each is a
        // shape the planner can index: the primary key for a name, and
        // secrets_catalog_name_spgist for a prefix.
        let mut deleted: Vec<models::Name> = match &prefix {
            Some(prefix) => {
                sqlx::query_scalar!(
                    r#"
                    DELETE FROM internal.secrets
                    WHERE catalog_name::text ^@ $1
                    RETURNING catalog_name AS "catalog_name!: models::Name"
                    "#,
                    prefix.as_str(),
                )
                .fetch_all(&env.pg_pool)
                .await?
            }
            None => {
                sqlx::query_scalar!(
                    r#"
                    DELETE FROM internal.secrets
                    WHERE catalog_name = $1::text::catalog_name
                    RETURNING catalog_name AS "catalog_name!: models::Name"
                    "#,
                    target,
                )
                .fetch_all(&env.pg_pool)
                .await?
            }
        };

        // DELETE ... RETURNING yields rows in whatever order it deleted them.
        deleted.sort();

        tracing::info!(
            %target,
            recursive = prefix.is_some(),
            deleted = deleted.len(),
            %claims.sub,
            "deleted secrets"
        );

        Ok(deleted)
    }
}

#[cfg(test)]
mod test {
    use crate::test_server;
    use serde_json::json;

    use test_server::wrapped;

    const SET_SECRET: &str = r#"
        mutation($catalogName: Name!, $document: JSON!) {
            setSecret(catalogName: $catalogName, document: $document) {
                changed
                secret { catalogName secretId }
            }
        }"#;

    const LIST_SECRETS: &str = r#"
        query($filter: SecretsFilter) {
            secrets(filter: $filter) {
                edges { cursor node { catalogName secretId } }
            }
        }"#;

    const DELETE_SECRET: &str = r#"
        mutation($catalogName: Name, $prefix: Prefix) {
            deleteSecret(catalogName: $catalogName, prefix: $prefix)
        }"#;

    /// One request the transcript can run. Variables are held verbatim as JSON
    /// so that malformed inputs — a `deleteSecret` naming both a name and a
    /// prefix, a filter setting both `startsWith` and `in` — are ordinary steps
    /// rather than bespoke assertions.
    enum Op {
        Set(&'static str, serde_json::Value),
        List(serde_json::Value),
        Delete(serde_json::Value),
    }

    /// A `setSecret` of a document wrapped for the name being set — the ordinary
    /// case. Steps that deliberately mismatch the two build `Op::Set` directly.
    fn set(name: &'static str, ciphertext: &str, last_modified: &str) -> Op {
        Op::Set(name, wrapped(name, ciphertext, last_modified))
    }

    /// The first error message of a GraphQL response, or None if it succeeded.
    fn error_of(response: &serde_json::Value) -> Option<&str> {
        response["errors"][0]["message"].as_str()
    }

    /// Renders a result list. An explicit `(none)` keeps an empty outcome from
    /// reading as a missing line in the transcript.
    fn render(items: impl Iterator<Item = String>) -> String {
        let items: Vec<String> = items.collect();
        if items.is_empty() {
            "(none)".to_string()
        } else {
            items.join(", ")
        }
    }

    /// Runs `steps` in order as the named actors, rendering each outcome into a
    /// transcript for snapshotting. The step's label is its own documentation:
    /// it lands in the snapshot beside what it produced.
    ///
    /// `secretId`s are symbolized by order of first appearance, so the
    /// transcript records entity *identity* — `id-1` recurring is proof that a
    /// set was a no-op on the same entity — without pinning values that differ
    /// on every run.
    async fn transcribe(
        server: &test_server::TestServer,
        tokens: &[(&str, &str)],
        steps: Vec<(&str, &str, Op)>,
    ) -> String {
        let mut ids: Vec<String> = Vec::new();
        let mut symbol = |value: &serde_json::Value| -> String {
            let id = value.as_str().expect("secretId").to_string();
            let index = ids.iter().position(|held| *held == id).unwrap_or_else(|| {
                ids.push(id);
                ids.len() - 1
            });
            format!("id-{}", index + 1)
        };

        let mut transcript = String::new();
        for (actor, label, op) in steps {
            let token = tokens
                .iter()
                .find(|(name, _)| *name == actor)
                .expect("actor has a token")
                .1;

            let outcome = match op {
                Op::Set(catalog_name, document) => {
                    let response: serde_json::Value = server
                        .graphql(
                            &serde_json::json!({
                                "query": SET_SECRET,
                                "variables": {
                                    "catalogName": catalog_name,
                                    "document": document,
                                },
                            }),
                            Some(token),
                        )
                        .await;

                    match error_of(&response) {
                        Some(message) => format!("error: {message}"),
                        None => {
                            let result = &response["data"]["setSecret"];
                            format!(
                                "changed={} {}",
                                result["changed"],
                                symbol(&result["secret"]["secretId"]),
                            )
                        }
                    }
                }
                Op::List(filter) => {
                    let response: serde_json::Value = server
                        .graphql(
                            &serde_json::json!({
                                "query": LIST_SECRETS,
                                "variables": { "filter": filter },
                            }),
                            Some(token),
                        )
                        .await;

                    match error_of(&response) {
                        Some(message) => format!("error: {message}"),
                        None => render(
                            response["data"]["secrets"]["edges"]
                                .as_array()
                                .expect("edges")
                                .iter()
                                .map(|edge| {
                                    format!(
                                        "{}={}",
                                        edge["node"]["catalogName"].as_str().expect("catalogName"),
                                        symbol(&edge["node"]["secretId"]),
                                    )
                                }),
                        ),
                    }
                }
                Op::Delete(variables) => {
                    let response: serde_json::Value = server
                        .graphql(
                            &serde_json::json!({
                                "query": DELETE_SECRET,
                                "variables": variables,
                            }),
                            Some(token),
                        )
                        .await;

                    match error_of(&response) {
                        Some(message) => format!("error: {message}"),
                        None => render(
                            response["data"]["deleteSecret"]
                                .as_array()
                                .expect("deleted names")
                                .iter()
                                .map(|name| name.as_str().expect("name").to_string()),
                        ),
                    }
                }
            };
            transcript.push_str(&format!("{actor}: {label}\n  {outcome}\n"));
        }
        transcript
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_secret_lifecycle(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // Alice is admin on aliceCo/ from the fixture. Bob reads it but does
        // not edit it: the Viewer bundle carries none of the three secret
        // capabilities, which live in Editor. Carol holds the Editor bundle and
        // nothing else — her grant carries no legacy capability, so her bits
        // come solely from the bundle — which makes her the caller class that
        // proves the three bits ride in Editor rather than only in Admin.
        for (id, email, capability, bundles) in [
            (
                "22222222-2222-2222-2222-222222222222",
                "bob@example.test",
                "read",
                Vec::new(),
            ),
            (
                "33333333-3333-3333-3333-333333333333",
                "carol@example.test",
                "none",
                vec!["editor".to_string()],
            ),
        ] {
            sqlx::query("INSERT INTO auth.users (id, email) VALUES ($1::uuid, $2)")
                .bind(id)
                .bind(email)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO public.user_grants (user_id, object_role, capability, bundles) \
                 VALUES ($1::uuid, 'aliceCo/', $2::text::public.grant_capability, \
                         $3::text[]::public.capability_bundle[])",
            )
            .bind(id)
            .bind(capability)
            .bind(&bundles)
            .execute(&pool)
            .await
            .unwrap();
        }

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.test"),
        );
        let bob =
            server.make_access_token(uuid::Uuid::from_bytes([0x22; 16]), Some("bob@example.test"));
        let carol = server.make_access_token(
            uuid::Uuid::from_bytes([0x33; 16]),
            Some("carol@example.test"),
        );
        let tokens = [
            ("alice", alice.as_str()),
            ("bob", bob.as_str()),
            ("carol", carol.as_str()),
        ];

        const PASSWORD: &str = "aliceCo/db/password";
        const OTHER: &str = "aliceCo/db/other";
        const EDITOR_SET: &str = "aliceCo/db/editor-set";
        const USERNAME: &str = "aliceCo/db/username";
        const TOKEN: &str = "aliceCo/api/token";
        const T10: &str = "2026-08-18T10:00:00Z";
        const T11: &str = "2026-08-18T11:00:00Z";
        const T12: &str = "2026-08-18T12:00:00Z";

        // Steps are aligned data; rustfmt's call-width budget would otherwise
        // break each of them across five lines.
        #[rustfmt::skip]
        let steps: Vec<(&str, &str, Op)> = vec![
            // Identity: `secretId` names one encryption, not one write. That is
            // what makes a GitOps re-apply of a stored document idempotent, and
            // what the stale guard protects against a rotation — best-effort,
            // since only config-encryption can verify `lastmodified` untampered.
            ("alice", "a first set creates the secret", set(PASSWORD, "aaa", T10)),
            ("alice", "an identical document is a no-op", set(PASSWORD, "aaa", T10)),
            ("alice", "a newer document mints a new id", set(PASSWORD, "bbb", T11)),
            ("alice", "a stale document is rejected, not applied", set(PASSWORD, "aaa", T10)),
            // Ties are allowed: `lastmodified` has second granularity, so two
            // encryptions within one second must not deadlock rotation.
            ("alice", "a tie is allowed", set(PASSWORD, "ccc", T11)),

            // The document guard itself is `secrets::test_validate_document`,
            // a pure function shared with `/task/set-secret`. This step is the
            // one which proves the mutation applies it and renders its message.
            ("alice", "a document is bound to the name it was wrapped for",
                Op::Set(OTHER, wrapped(PASSWORD, "aaa", T10))),

            // Capability gating. Bob's Viewer bundle carries none of the three
            // secret bits; Carol's Editor carries all three, which is what
            // proves they ride in Editor rather than only in Admin.
            ("bob", "a viewer must not set a secret", set(PASSWORD, "ddd", T12)),
            ("carol", "an Editor may set", set(EDITOR_SET, "eds", T12)),
            ("carol", "an Editor may list",
                Op::List(json!({ "catalogName": { "startsWith": "aliceCo/db/" } }))),
            ("carol", "an Editor may delete", Op::Delete(json!({ "catalogName": EDITOR_SET }))),
            ("alice", "an editor must not reach outside their grants",
                set("bobCo/db/password", "eee", T12)),

            // Listing, and the filter's narrow-only contract.
            ("alice", "two more secrets, to list and to sweep", set(USERNAME, "fff", T10)),
            ("alice", "and the second", set(TOKEN, "ggg", T10)),
            ("alice", "listing yields names and ids, never documents", Op::List(json!({}))),
            ("alice", "startsWith narrows to a subtree",
                Op::List(json!({ "catalogName": { "startsWith": "aliceCo/db/" } }))),
            ("alice", "in selects an exact set",
                Op::List(json!({ "catalogName": { "in": [TOKEN] } }))),
            ("alice", "startsWith and in are mutually exclusive",
                Op::List(json!({ "catalogName": { "startsWith": "aliceCo/", "in": ["aliceCo/"] } }))),
            ("alice", "a filter can never widen scope",
                Op::List(json!({ "catalogName": { "startsWith": "bobCo/" } }))),
            // The visibility gate fails closed to an empty page, not an error.
            ("bob", "listing requires ViewSecret", Op::List(json!({}))),

            // Deletion. The empty prefix is valid per the catalog-prefix grammar
            // and would sweep every secret on the platform, so it is rejected
            // outright rather than left to depend on no grant covering it.
            ("alice", "deleteSecret takes exactly one of catalogName or prefix",
                Op::Delete(json!({}))),
            ("alice", "and rejects both",
                Op::Delete(json!({ "catalogName": PASSWORD, "prefix": "aliceCo/" }))),
            ("alice", "an empty prefix is rejected", Op::Delete(json!({ "prefix": "" }))),
            ("alice", "deleting by name removes just that secret",
                Op::Delete(json!({ "catalogName": PASSWORD }))),
            ("alice", "deleting a missing secret is a no-op",
                Op::Delete(json!({ "catalogName": PASSWORD }))),
            ("bob", "deleting requires EditSecret", Op::Delete(json!({ "catalogName": TOKEN }))),
            ("alice", "the prefix form is the recursive delete",
                Op::Delete(json!({ "prefix": "aliceCo/" }))),
            ("alice", "nothing remains", Op::List(json!({}))),
        ];

        insta::assert_snapshot!(
            "secret_lifecycle",
            transcribe(&server, &tokens, steps).await
        );
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_secret_document_is_canonicalized(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        async fn raw_set(
            server: &test_server::TestServer,
            token: &str,
            document: &str,
        ) -> serde_json::Value {
            const QUERY: &str = "mutation($catalogName: Name!, $document: JSON!) \
                { setSecret(catalogName: $catalogName, document: $document) \
                { changed secret { secretId } } }";

            let body = serde_json::value::RawValue::from_string(format!(
                r#"{{"query":"{QUERY}","variables":{{"catalogName":"aliceCo/ordered","document":{document}}}}}"#
            ))
            .unwrap();
            server.graphql(&body, Some(token)).await
        }

        // `value` before `name`, and `version` and `mac` before `lastmodified`,
        // at both levels of the document.
        const UNSORTED: &str = r#"{"value":"ENC[AES256_GCM,data:aaa,type:str]","name":"aliceCo/ordered","sops":{"version":"3.9.0","mac":"ENC[AES256_GCM,data:mac-aaa]","lastmodified":"2026-08-18T10:00:00Z"}}"#;
        const SORTED: &str = r#"{"name":"aliceCo/ordered","sops":{"lastmodified":"2026-08-18T10:00:00Z","mac":"ENC[AES256_GCM,data:mac-aaa]","version":"3.9.0"},"value":"ENC[AES256_GCM,data:aaa,type:str]"}"#;

        let created = raw_set(&server, &alice, UNSORTED).await;
        assert!(created["errors"].is_null(), "set should succeed: {created}");
        assert_eq!(created["data"]["setSecret"]["changed"], true);
        let id = created["data"]["setSecret"]["secret"]["secretId"]
            .as_str()
            .expect("secretId")
            .to_string();

        let stored: String = sqlx::query_scalar(
            "SELECT document::text FROM internal.secrets WHERE catalog_name = 'aliceCo/ordered'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, SORTED, "the stored text must be canonical");

        // Key order is therefore *not* part of a secret's identity: the same
        // document sorted is a no-op re-apply of the same entity.
        let reapplied = raw_set(&server, &alice, SORTED).await;
        assert!(
            reapplied["errors"].is_null(),
            "re-apply should succeed: {reapplied}"
        );
        assert_eq!(reapplied["data"]["setSecret"]["changed"], false);
        assert_eq!(
            reapplied["data"]["setSecret"]["secret"]["secretId"], id,
            "a re-ordered document is the same entity: {reapplied}"
        );
    }

    // Pagination cursors on catalog_name, the table's primary key, so a page
    // boundary is stable across concurrent sets and deletes.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_secrets_pagination(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        for name in ["aliceCo/a", "aliceCo/b", "aliceCo/c"] {
            sqlx::query(
                "INSERT INTO internal.secrets (catalog_name, document) VALUES ($1, $2::text::json)",
            )
            .bind(name)
            .bind(wrapped(name, "aaa", "2026-08-18T10:00:00Z").to_string())
            .execute(&pool)
            .await
            .unwrap();
        }

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), false).await,
        )
        .await;
        let alice = server.make_access_token(uuid::Uuid::from_bytes([0x11; 16]), None);

        // Walking to exhaustion, which proves the cursor terminates.
        let mut transcript = String::new();
        let mut after: Option<String> = None;
        loop {
            let response: serde_json::Value = server
                .graphql(
                    &serde_json::json!({
                        "query": r#"
                        query($after: String) {
                            secrets(first: 2, after: $after) {
                                pageInfo { hasNextPage endCursor }
                                edges { node { catalogName } }
                            }
                        }"#,
                        "variables": { "after": after },
                    }),
                    Some(&alice),
                )
                .await;
            assert!(response["errors"].is_null(), "{response}");

            let page = &response["data"]["secrets"];
            transcript.push_str(&format!(
                "page: {}\n  hasNextPage={}\n",
                render(page["edges"].as_array().expect("edges").iter().map(|edge| {
                    edge["node"]["catalogName"]
                        .as_str()
                        .expect("catalogName")
                        .to_string()
                })),
                page["pageInfo"]["hasNextPage"],
            ));

            if page["pageInfo"]["hasNextPage"] != true {
                break;
            }
            after = Some(
                page["pageInfo"]["endCursor"]
                    .as_str()
                    .expect("endCursor")
                    .to_string(),
            );
        }
        insta::assert_snapshot!("secrets_pagination", transcript);
    }
}
