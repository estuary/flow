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

/// The document must survive transport with its *key order* intact: sops
/// verifies its MAC by traversing the document in order, and the workspace
/// builds serde_json without `preserve_order`, so a round trip through
/// `serde_json::Value` would alphabetize keys and break verification. This
/// scalar routes through `async_graphql::Value` instead, whose objects are
/// `IndexMap`s, and holds the result as text from then on.
///
/// `parse` does normalize the document's *formatting* — insignificant
/// whitespace and JSON escape choices don't survive re-serialization — so the
/// identity that `setSecret` judges is normalized-text equality with key order
/// significant, not literal byte equality of what the client sent. sops is
/// indifferent to formatting; only values and traversal order feed its MAC.
#[derive(Debug, Clone)]
pub struct SecretDocument(pub models::RawValue);

/// The sops-wrapped document of a secret, as returned by config-encryption's
/// `/secret/encrypt` route. It is opaque to the control plane, which holds no
/// grant on the KMS key that wraps it and so can neither decrypt the document
/// nor verify its MAC. Provide it verbatim, exactly as config-encryption
/// returned it.
#[async_graphql::Scalar(name = "SecretDocument")]
impl async_graphql::ScalarType for SecretDocument {
    fn parse(value: async_graphql::Value) -> async_graphql::InputValueResult<Self> {
        let text = serde_json::to_string(&value)?;
        Ok(Self(models::RawValue::from_string(text)?))
    }

    fn to_value(&self) -> async_graphql::Value {
        // A held document is valid JSON by construction.
        serde_json::from_str(self.0.get()).expect("secret document is valid JSON")
    }
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
                env.claims()?.sub,
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
    /// must be an object whose `name` equals `catalogName` — the cryptographic
    /// binding that keeps a wrapped document from being cloned under another
    /// name, since sops MACs `name` even though it is stored in the clear.
    ///
    /// Setting is idempotent on the document's identity: re-applying a stored
    /// document leaves `secretId` alone and reports `changed: false`. Any other
    /// change mints a new `secretId`. A document whose embedded `sops.lastmodified`
    /// predates the stored one is rejected rather than applied, guarding
    /// against a stale re-apply; ties are allowed, because the timestamp has
    /// second granularity.
    async fn set_secret(
        &self,
        ctx: &Context<'_>,
        catalog_name: models::Name,
        document: SecretDocument,
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

        let SecretDocument(document) = document;
        let last_modified = validate_document(catalog_name.as_str(), &document)?;

        let row = sqlx::query!(
            r#"
            WITH locked AS (
                -- Lock the current row so that concurrent sets of this secret
                -- serialize, and so the outcome classified below is of the same
                -- row version that the conditional write acts upon.
                SELECT
                    id,
                    document::text AS document_text,
                    (document->'sops'->>'lastmodified')::timestamptz AS last_modified
                FROM internal.secrets
                WHERE catalog_name = $1::text::catalog_name
                FOR UPDATE
            ),
            updated AS (
                -- An identical document is the same entity, so it must not
                -- mint an id; a document older than the stored one must not be
                -- applied at all. Either way this UPDATE matches no row, and
                -- the two cases are told apart from `locked` below.
                UPDATE internal.secrets SET
                    id = internal.id_generator(),
                    document = $2::text::json
                WHERE catalog_name = $1::text::catalog_name
                  AND EXISTS (
                    SELECT 1 FROM locked
                    WHERE locked.document_text <> $2::text
                      AND locked.last_modified <= $3::timestamptz
                  )
                RETURNING id
            ),
            inserted AS (
                INSERT INTO internal.secrets (catalog_name, document)
                SELECT $1::text::catalog_name, $2::text::json
                WHERE NOT EXISTS (SELECT 1 FROM locked)
                -- `locked` takes no lock when there is no row to lock, so two
                -- concurrent first-sets can reach this INSERT. The loser writes
                -- nothing and is reported as a conflict to retry.
                ON CONFLICT (catalog_name) DO NOTHING
                RETURNING id
            )
            SELECT
                (SELECT id FROM locked) AS "prior_id: models::Id",
                (SELECT document_text FROM locked) AS "prior_document: String",
                coalesce(
                    (SELECT id FROM updated),
                    (SELECT id FROM inserted)
                ) AS "written_id: models::Id"
            "#,
            catalog_name.as_str(),
            document.get(),
            last_modified,
        )
        .fetch_one(&env.pg_pool)
        .await?;

        let (secret_id, changed) = match (row.written_id, row.prior_id, row.prior_document) {
            (Some(written_id), _, _) => (written_id, true),
            (None, Some(prior_id), Some(prior_document)) if prior_document == document.get() => {
                (prior_id, false)
            }
            (None, Some(_), _) => {
                return Err(async_graphql::Error::new(format!(
                    "the stored secret '{catalog_name}' is newer than the provided document; \
                     re-encrypt the value you intend to set, or fetch the current document"
                )));
            }
            (None, None, _) => {
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

/// Structurally validate a wrapped secret document against the name it is being
/// set at, returning its embedded `sops.lastmodified`.
///
/// The parse is into a throwaway side copy: `document` itself stays opaque text,
/// because re-serializing it would reorder keys and break the sops MAC.
fn validate_document(
    catalog_name: &str,
    document: &models::RawValue,
) -> async_graphql::Result<chrono::DateTime<chrono::Utc>> {
    // Only the fields the control plane must agree with sops about. `value` is
    // checked for presence alone — its content is ciphertext we cannot read.
    #[derive(serde::Deserialize)]
    struct Wrapped {
        name: String,
        // Deserialized to require its presence, then discarded.
        #[allow(dead_code)]
        value: serde::de::IgnoredAny,
        sops: Sops,
    }
    // Parsed with its offset retained, because Postgres re-parses the literal
    // `lastmodified` text below and the offset is part of what it must accept.
    #[derive(serde::Deserialize)]
    struct Sops {
        lastmodified: chrono::DateTime<chrono::FixedOffset>,
    }

    let wrapped: Wrapped = serde_json::from_str(document.get()).map_err(|err| {
        async_graphql::Error::new(format!(
            "document is not a wrapped secret produced by /secret/encrypt: {err}"
        ))
    })?;

    if wrapped.name != catalog_name {
        return Err(async_graphql::Error::new(format!(
            "document is wrapped for secret '{}', not '{catalog_name}'; a wrapped document is \
             bound to its name and cannot be set under another",
            wrapped.name,
        )));
    }

    // Postgres re-parses the *stored* `lastmodified` text on every later set of
    // this secret, so a value chrono accepts here but Postgres cannot parse
    // would wedge the row: every future set fails until the secret is deleted.
    // The parsers diverge on year zero and on UTC offsets beyond Postgres's
    // ±15:59 — sops never emits either (it writes UTC 'Z' timestamps), so
    // reject them up front rather than store a poison value.
    let last_modified = wrapped.sops.lastmodified;
    if chrono::Datelike::year(&last_modified) < 1
        || last_modified.offset().local_minus_utc().abs() > 15 * 3600 + 59 * 60
    {
        return Err(async_graphql::Error::new(format!(
            "document's sops.lastmodified '{last_modified}' is outside the range \
             this API can store"
        )));
    }

    Ok(last_modified.to_utc())
}

#[cfg(test)]
mod test {
    use crate::test_server;
    use serde_json::json;

    /// A stand-in for what config-encryption's `/secret/encrypt` returns: only
    /// the fields the control plane reads are real, and `value` is ciphertext
    /// that nothing in this crate can decrypt.
    fn wrapped(name: &str, ciphertext: &str, last_modified: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "value": format!("ENC[AES256_GCM,data:{ciphertext},type:str]"),
            "sops": {
                "age": [{ "recipient": "age1exampleexample", "enc": "-----BEGIN AGE ENCRYPTED FILE-----" }],
                "lastmodified": last_modified,
                "mac": format!("ENC[AES256_GCM,data:mac-{ciphertext}]"),
                "encrypted_regex": "^value$",
                "version": "3.9.0",
            },
        })
    }

    const SET_SECRET: &str = r#"
        mutation($catalogName: Name!, $document: SecretDocument!) {
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

            // sops MACs the plaintext `name`, so a document is bound to it and
            // cannot be cloned under another; the mismatch is caught up front
            // rather than surfacing later as a decryption failure.
            ("alice", "a document is bound to the name it was wrapped for",
                Op::Set(OTHER, wrapped(PASSWORD, "aaa", T10))),
            // A `lastmodified` that chrono accepts but Postgres cannot re-parse
            // would wedge the row: every future set fails until it is deleted.
            ("alice", "a year-zero lastmodified is rejected",
                set(OTHER, "zzz", "0000-12-31T23:59:59Z")),
            ("alice", "as is an offset beyond Postgres's ±15:59",
                set(OTHER, "zzz", "2026-08-18T10:00:00+16:00")),
            // A document that isn't a wrapped secret at all is rejected on shape.
            ("alice", "a document without sops is not a wrapped secret",
                Op::Set(OTHER, json!({ "name": OTHER, "value": "x" }))),
            ("alice", "nor is one without value",
                Op::Set(OTHER, json!({ "name": OTHER, "sops": { "lastmodified": T10 } }))),
            ("alice", "nor one whose lastmodified doesn't parse",
                Op::Set(OTHER, json!({
                    "name": OTHER, "value": "x", "sops": { "lastmodified": "whenever" } }))),
            ("alice", "nor a bare string", Op::Set(OTHER, json!("just a string"))),

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

    // The whole reason `document` is `json` rather than `jsonb`, and RawValue
    // rather than `serde_json::Value`, is that key order must survive to keep
    // the sops MAC verifiable. `serde_json::json!` can't exercise that —
    // without `preserve_order` it alphabetizes keys before the request is even
    // sent — so this test posts raw request bodies with deliberately
    // non-alphabetical key order and asserts the stored text retains it.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_secret_document_key_order_is_preserved(pool: sqlx::PgPool) {
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
            const QUERY: &str = "mutation($catalogName: Name!, $document: SecretDocument!) \
                { setSecret(catalogName: $catalogName, document: $document) \
                { changed secret { secretId } } }";

            let body = serde_json::value::RawValue::from_string(format!(
                r#"{{"query":"{QUERY}","variables":{{"catalogName":"aliceCo/ordered","document":{document}}}}}"#
            ))
            .unwrap();
            server.graphql(&body, Some(token)).await
        }

        // `value` before `name`, and `version` and `mac` before `lastmodified`:
        // alphabetization anywhere in the pipeline reorders one of them.
        const DOCUMENT: &str = r#"{"value":"ENC[AES256_GCM,data:aaa,type:str]","name":"aliceCo/ordered","sops":{"version":"3.9.0","mac":"ENC[AES256_GCM,data:mac-aaa]","lastmodified":"2026-08-18T10:00:00Z"}}"#;

        let created = raw_set(&server, &alice, DOCUMENT).await;
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
        assert_eq!(stored, DOCUMENT, "the stored text must retain key order");

        // Formatting is *not* significant: the same document with extra
        // whitespace normalizes to identical text, and is a no-op re-apply of
        // the same entity rather than a new one.
        let spaced = DOCUMENT.replace(",\"", ", \"");
        assert_ne!(spaced, DOCUMENT);
        let reapplied = raw_set(&server, &alice, &spaced).await;
        assert!(
            reapplied["errors"].is_null(),
            "re-apply should succeed: {reapplied}"
        );
        assert_eq!(reapplied["data"]["setSecret"]["changed"], false);
        assert_eq!(
            reapplied["data"]["setSecret"]["secret"]["secretId"], id,
            "a reformatted document is the same entity: {reapplied}"
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
