type Request = models::authorizations::TaskAuthorizationRequest;
type Response = models::authorizations::DecryptAuthorization;

/// Authorizes a task to decrypt a secret, returning its wrapped document.
///
/// The direct caller is config-encryption, which forwards a request body
/// token from a data-plane reactor caller, and holds the KMS grant that can
/// decrypt our successful result.
///
/// The request token is signed by the issuing data-plane:
///
///  * Its `sub` is the Shard ID requesting decryption, or -- where the shard's
///    generation isn't the live one -- a synthetic Shard ID having an all-zero
///    generation. Note we must handle requests for tasks that have not yet been
///    published, and cannot require that task shards are in the Snapshot as
///    `/authorize/task` does.
///
///  * Its `sel` names the requested secret under `estuary.dev/secret-name`.
///
/// Two things are checked:
///
///  * The sibling rule: a task may use only the secrets which sit beside it,
///    `dirname(secret) == dirname(task)`.
///  * Residency: a task the Snapshot knows of must live in the issuing
///    data-plane, so that a compromised plane cannot ask for the secrets of
///    tasks it doesn't run.
///
/// If a task is new (it has no shard), then the requesting data-plane must be
/// admitted by the longest-prefix storage mapping covering the task and secret
/// name -- the same mapping which decides where the task could be created.
///
/// SAFETY:
///
/// A compromised data-plane may craft requests of secrets for tenant storage-
/// mappings that have opted into use of that data-plane. Accepted, because an
/// attacker holding a compromised plane already has a capability to access
/// secrets of *existing* tasks of that same plane, and few real secrets exist
/// without accompanying live tasks.
///
/// As a critical call-out, the storage mapping check guards against a rogue
/// bring-your-own-compute user who compromises their *own* data-plane in an
/// attempt to exfiltrate secrets of *other* data-planes. For this reason if none
/// other, the check is essential and must be preserved.
///
/// More generally, this is an argument in favor of least-privilege when
/// configuring storage mappings.
#[axum::debug_handler(state=std::sync::Arc<crate::App>)]
#[tracing::instrument(skip(env), err(Debug, level = tracing::Level::WARN))]
pub async fn authorize_task_secret(
    mut env: crate::Envelope,
    super::Request(Request { token }): super::Request<Request>,
) -> Result<axum::Json<Response>, crate::ApiError> {
    let unverified = super::parse_untrusted_data_plane_claims(&token)?;

    // Use the `iat` claim to establish the logical start of the request,
    // rounded up to the next second (as it was round down when encoded).
    env.started = tokens::DateTime::from_timestamp_secs(1 + unverified.claims().iat as i64)
        .unwrap_or_default();

    let name = labels::expect_one(unverified.claims().sel.include(), labels::SECRET_NAME)
        .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
    let name = models::Name::new(name);

    // `err` renders as ": {name} doesn't match pattern ...", restating the name.
    if let Err(err) = validator::Validate::validate(&name) {
        return Err(tonic::Status::invalid_argument(format!("invalid secret name{err}")).into());
    }

    let policy_result = evaluate_authorization(
        env.snapshot(),
        &unverified.claims().sub,
        &unverified.claims().iss,
        &token,
        &name,
    );

    let fallback_check_data_plane = match env.authorization_outcome(policy_result).await {
        Ok((_expiry, admit_data_plane)) => admit_data_plane,
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

    let Some(fallback_check_data_plane) = fallback_check_data_plane else {
        // Happy path: the Snapshot was able to establish task residency and we
        // bypass the storage-mapping fallback check.
        return Ok(axum::Json(super::fetch_secret(&env.pg_pool, &name).await?));
    };

    // Storage mapping prefixes are always slash-terminated, so the mappings
    // which could cover `name` are those of its slash-terminated prefixes.
    // Enumerating them lets us ensure we hit the unique index over `catalog_prefix`.
    let prefixes: Vec<&str> = name
        .as_str()
        .rmatch_indices('/')
        .map(|(index, _)| &name.as_str()[..index + 1])
        .collect();

    // The longest covering mapping alone decides admissibility, mirroring
    // publication's `lookup_mapping`: it names the planes a task could be
    // created in, and a parent mapping's planes are never promoted into the
    // decision. No covering mapping at all, or one without the plane, denies.
    let row = sqlx::query!(
        r#"
        SELECT
            s.document AS "document?: models::RawValue",
            s.id AS "secret_id?: models::Id",
            COALESCE(
                (
                    SELECT $3 IN (SELECT json_array_elements_text(m.spec -> 'data_planes'))
                    FROM storage_mappings m
                    WHERE m.catalog_prefix = ANY ($2::text[])
                    ORDER BY length(m.catalog_prefix) DESC
                    LIMIT 1
                ),
                false
            ) AS "admissible!: bool"
        FROM (SELECT $1::text::catalog_name) AS q (catalog_name)
        LEFT JOIN internal.secrets s ON s.catalog_name = q.catalog_name
        "#,
        name.as_str(),
        &prefixes as &[&str],
        fallback_check_data_plane,
    )
    .fetch_one(&env.pg_pool)
    .await?;

    // Admissibility is settled before existence, so that a caller who fails it
    // doesn't learn from a 404 which secrets are there.
    if !row.admissible {
        return Err(tonic::Status::permission_denied(format!(
            "no task of shard {} is known, and the storage mapping of secret '{name}' does not admit data-plane {fallback_check_data_plane}",
            unverified.claims().sub,
        ))
        .into());
    }

    // Absence is terminal: unlike a grant, a secret is read at its current
    // value, so a later read cannot turn this answer around.
    let (Some(document), Some(secret_id)) = (row.document, row.secret_id) else {
        return Err(tonic::Status::not_found(format!("secret '{name}' does not exist")).into());
    };

    Ok(axum::Json(Response {
        document: Some(document),
        secret_id: Some(secret_id),
        retry_millis: 0,
    }))
}

/// Decide the authorization, returning `None` if residency is settled outright,
/// or the data-plane name to verify during the storage-mapping fallback check.
fn evaluate_authorization<'s>(
    snapshot: &'s crate::Snapshot,
    shard_id: &str,
    task_data_plane_fqdn: &str,
    token: &str,
    secret_name: &models::Name,
) -> crate::AuthZResult<Option<&'s str>> {
    // Map `claims.iss`, a data-plane FQDN, into its token-verified data-plane.
    let Some(task_data_plane) = snapshot.verify_data_plane_token(task_data_plane_fqdn, token)?
    else {
        return Err(tonic::Status::unauthenticated(
            "no data-plane keys validated against the token signature",
        ));
    };

    // Map `claims.sub`, a real or synthetic Shard ID, into its live task.
    let (task_name, task) = if let Some(task) = snapshot.task_by_shard_id(shard_id) {
        (task.task_name.as_str(), Some(task))
    } else if let Some(task_name) = synthetic_task_name(shard_id) {
        (task_name, snapshot.task_by_catalog_name(task_name))
    } else {
        return Err(tonic::Status::failed_precondition(format!(
            "task shard {shard_id} within data-plane {task_data_plane_fqdn} is not known"
        )));
    };

    let (Some(task_parent), Some(secret_parent)) =
        (parent_prefix(task_name), parent_prefix(secret_name))
    else {
        return Err(tonic::Status::permission_denied(format!(
            "task '{task_name}' and secret '{secret_name}' are not both catalog names"
        )));
    };

    if task_parent != secret_parent {
        return Err(tonic::Status::permission_denied(format!(
            "task '{task_name}' may only use secrets under '{task_parent}', and '{secret_name}' is not one"
        )));
    }

    let mapping_fallback = if let Some(task) = task {
        // Residency: a task we know of must run in the issuing plane, or bust.
        if task.data_plane_id != task_data_plane.control_id {
            return Err(tonic::Status::permission_denied(format!(
                "task '{task_name}' does not run in data-plane {task_data_plane_fqdn}"
            )));
        }
        None
    } else {
        // Unknown synthetic tasks (Validate / Discover) check the storage-mapping.
        Some(task_data_plane.data_plane_name.as_str())
    };

    Ok((
        snapshot.cordon_at(task_name, task_data_plane),
        mapping_fallback,
    ))
}

/// The task name of a *synthetic* Shard ID: an all-zero generation ID over any
/// key / r-clock range, naming a shard of a generation which isn't live --
/// Discover and Validate, which have no shard, and shards of a specification
/// under test which hasn't published.
///
/// Callers owe us that zero whenever the shard's generation isn't the live one:
/// a new task, and equally a reset, whose built generation is its draft's
/// publication ID. Any other generation resolves through the Snapshot instead,
/// which matches it exactly and so fences off a stale one. The range is only
/// shape-checked, but the fixed shape of both suffixes -- not their values --
/// keeps a truncated Shard ID from yielding a shorter, more privileged name.
fn synthetic_task_name(shard_id: &str) -> Option<&str> {
    let (prefix, range) = shard_id.rsplit_once('/')?;
    let prefix = prefix.strip_suffix("/0000000000000000")?;

    let hex_8 = |s: &str| s.len() == 8 && s.bytes().all(|b| b.is_ascii_hexdigit());
    let (key_begin, r_clock_begin) = range.split_once('-')?;

    if !hex_8(key_begin) || !hex_8(r_clock_begin) {
        return None;
    }
    let (task_type, task_name) = prefix.split_once('/')?;

    matches!(task_type, "capture" | "derivation" | "materialize").then_some(task_name)
}

/// The catalog prefix which directly contains `name`, or None if `name` isn't a
/// catalog name at all. Two names are siblings when their prefixes are equal.
fn parent_prefix(name: &str) -> Option<&str> {
    name.rfind('/').map(|index| &name[..index + 1])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fixture's two data-planes, as (FQDN, HMAC key). Cases name a plane
    /// as a pair so that a signature is never accidentally mismatched to its
    /// issuer -- `bad-signature` does so on purpose, and is the only one.
    const PLANE_ONE: (&str, &str) = ("fqdn1", "key1");
    const PLANE_TWO: (&str, &str) = ("fqdn2", "key3");

    /// Every case is driven through the same fixture Snapshot, whose tasks and
    /// migrations are what each case selects among.
    /// Cases are (label, shard ID, issuing data-plane, secret name).
    #[test]
    fn test_evaluate_authorization() {
        let cases = [
            // Sibling secrets of a task resident in the issuing plane, asked
            // for by the synthetic shard of a Validate and then by a running
            // shard of the task's published generation.
            (
                "resident",
                "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            (
                "resident/running-shard",
                "capture/acmeCo/source-pineapple/0011223344556677/80000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            // A synthetic shard of a *split* task under test: only the
            // all-zero generation bears on the outcome.
            (
                "resident/synthetic-split",
                "capture/acmeCo/source-pineapple/0000000000000000/40000000-c0000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            // The range is still shape-checked, so a malformed one names no
            // shard, synthetic or otherwise.
            (
                "bad-range",
                "capture/acmeCo/source-pineapple/0000000000000000/4000000-c0000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            // A shard of a generation which is no longer published is fenced
            // off, rather than being re-authorized because its name still
            // reads out of the Shard ID.
            (
                "resident/zombie-generation",
                "capture/acmeCo/source-pineapple/7766554433221100/80000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            // A legacy shard, whose Shard ID has no generation ID at all. Its
            // task is named by the Snapshot, which is why nothing here needs
            // to guess whether a trailing name component is a generation.
            (
                "resident/legacy-shard",
                "capture/acmeCo/source-legacy/80000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            (
                "resident/nested",
                "capture/bobCo/widgets/source-squash/0000000000000000/00000000-00000000",
                PLANE_TWO,
                "bobCo/widgets/password",
            ),
            // The three task types a synthetic Shard ID may name.
            (
                "resident/materialize",
                "materialize/acmeCo/materialize-pear/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            (
                "unknown/derivation",
                "derivation/acmeCo/derive-plum/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            (
                "bad-task-type",
                "dekaf/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            // Non-siblings: one level too deep, one level too shallow, and a
            // sibling-looking name under another tenant.
            (
                "child",
                "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/db/password",
            ),
            (
                "parent",
                "capture/bobCo/widgets/source-squash/0000000000000000/00000000-00000000",
                PLANE_TWO,
                "bobCo/password",
            ),
            (
                "other-tenant",
                "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "bobCo/password",
            ),
            // `sub` isn't a shard ID, and a shard ID whose task isn't a
            // catalog name. A shard *template* ID names no shard and isn't
            // synthetic, so it's rejected rather than read as the task
            // `bobCo/widgets`, which would make `bobCo/password` its sibling.
            (
                "not-a-shard-id",
                "acmeCo/source-pineapple",
                PLANE_ONE,
                "acmeCo/password",
            ),
            (
                "template-prefix",
                "capture/bobCo/widgets/source-squash/0000000000000000",
                PLANE_TWO,
                "bobCo/password",
            ),
            (
                "not-a-name",
                "capture/source-pineapple/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            // A secret which isn't a catalog name has no sibling prefix to
            // compare, and `models::Name` allows one, so it's rejected here.
            (
                "secret-not-a-name",
                "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "password",
            ),
            // Residency of a known task, which runs in plane-one.
            // acmeCo/source-banana is migrating plane-one => plane-two,
            // but plane-two is denied until the task's residency actually moves.
            (
                "wrong-plane",
                "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
                PLANE_TWO,
                "acmeCo/password",
            ),
            (
                "migration/src",
                "capture/acmeCo/source-banana/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            (
                "migration/tgt",
                "capture/acmeCo/source-banana/0000000000000000/00000000-00000000",
                PLANE_TWO,
                "acmeCo/password",
            ),
            // A task absent from the Snapshot defers to the storage mappings
            // of the live DB, which only `fetch_secret` can settle.
            (
                "unknown",
                "capture/acmeCo/source-new/0000000000000000/00000000-00000000",
                PLANE_ONE,
                "acmeCo/password",
            ),
            // An otherwise-valid request, signed with the other plane's key.
            (
                "bad-signature",
                "capture/acmeCo/source-pineapple/0000000000000000/00000000-00000000",
                (PLANE_ONE.0, PLANE_TWO.1),
                "acmeCo/password",
            ),
        ];

        let outcomes: Vec<(&str, String)> = cases
            .into_iter()
            .map(|(label, shard, plane, secret)| (label, run(shard, plane, secret)))
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }

    fn run(
        shard_id: &str,
        (task_data_plane_fqdn, hmac_key): (&str, &str),
        secret_name: &str,
    ) -> String {
        let snapshot = crate::Snapshot::build_fixture(None);
        let now = tokens::now().timestamp() as u64;

        let claims = proto_gazette::Claims {
            iat: now,
            exp: now + 100,
            cap: proto_flow::capability::AUTHORIZE,
            iss: task_data_plane_fqdn.to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set([(labels::SECRET_NAME, secret_name)])),
                exclude: None,
            },
            sub: shard_id.to_string(),
        };
        let token = tokens::jwt::sign(
            &claims,
            &tokens::jwt::EncodingKey::from_secret(hmac_key.as_bytes()),
        )
        .unwrap();

        match evaluate_authorization(
            &snapshot,
            shard_id,
            task_data_plane_fqdn,
            &token,
            &models::Name::new(secret_name),
        ) {
            Ok((cordon_at, admit_data_plane)) => {
                let mut out = "Ok".to_string();

                if let Some(cordon_at) = cordon_at {
                    out.push_str(&format!(" cordoned at {cordon_at}"));
                }
                if let Some(admit_data_plane) = admit_data_plane {
                    out.push_str(&format!(" admitting {admit_data_plane}"));
                }
                out
            }
            Err(status) => format!(
                "{} {}",
                tokens::rest::grpc_status_code_to_http(status.code()),
                status.message()
            ),
        }
    }

    // Integration tests below use sqlx::test with an actual database.
    use crate::test_server;

    /// Drive the route as config-encryption would, asking for `secret_names` on
    /// behalf of the synthetic shard of `task` -- a Shard ID with `task`'s type
    /// and name, the shape a Discover or Validate sends -- issued by the
    /// data-plane of FQDN `iss`. Names are plural only so that a selector
    /// bearing no name, or two, is expressible.
    async fn post(
        server: &test_server::TestServer,
        iss: &str,
        task: &str,
        secret_names: &[&str],
    ) -> String {
        let now = tokens::now().timestamp() as u64;

        let claims = proto_gazette::Claims {
            iat: now,
            exp: now + 100,
            cap: proto_flow::capability::AUTHORIZE,
            iss: iss.to_string(),
            sel: proto_gazette::LabelSelector {
                include: Some(labels::build_set(
                    secret_names.iter().map(|name| (labels::SECRET_NAME, *name)),
                )),
                exclude: None,
            },
            sub: format!("{task}/0000000000000000/00000000-00000000"),
        };
        // "c2VjcmV0" of the data_planes fixture, base64-decoded.
        let token =
            tokens::jwt::sign(&claims, &tokens::jwt::EncodingKey::from_secret(b"secret")).unwrap();

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
            // the one secret a passing case could ever return.
            (
                "sibling/in",
                post(
                    &server,
                    "dp.one",
                    "capture/aliceCo/in/capture-foo",
                    &["aliceCo/in/token"],
                )
                .await,
            ),
            (
                "sibling/out",
                post(
                    &server,
                    "dp.one",
                    "materialize/aliceCo/out/materialize-bar",
                    &["aliceCo/out/token"],
                )
                .await,
            ),
            (
                "not-sibling",
                post(
                    &server,
                    "dp.one",
                    "materialize/aliceCo/out/materialize-bar",
                    &["aliceCo/in/token"],
                )
                .await,
            ),
            (
                "absent",
                post(
                    &server,
                    "dp.one",
                    "capture/aliceCo/in/capture-foo",
                    &["aliceCo/in/nonexistent"],
                )
                .await,
            ),
            (
                "malformed",
                post(
                    &server,
                    "dp.one",
                    "capture/aliceCo/in/capture-foo",
                    &["aliceCo/bad name"],
                )
                .await,
            ),
            // The selector must name exactly one secret. Neither none nor two
            // is a request this route can answer.
            (
                "no-selector",
                post(&server, "dp.one", "capture/aliceCo/in/capture-foo", &[]).await,
            ),
            (
                "two-selectors",
                post(
                    &server,
                    "dp.one",
                    "capture/aliceCo/in/capture-foo",
                    &["aliceCo/in/token", "aliceCo/out/token"],
                )
                .await,
            ),
            // Tasks absent from the DB, which defer to storage mappings.
            // `aliceCo/` admits dp.one, `bobCo/` admits only the other plane,
            // and `carolCo/` has no mapping at all. An admitted request still
            // 404s on a secret that isn't there, but a request which isn't
            // admitted is denied without disclosing whether it would have.
            (
                "unknown/admitted",
                post(
                    &server,
                    "dp.one",
                    "capture/aliceCo/in/capture-new",
                    &["aliceCo/in/token"],
                )
                .await,
            ),
            (
                "unknown/admitted-absent",
                post(
                    &server,
                    "dp.one",
                    "capture/aliceCo/in/capture-new",
                    &["aliceCo/in/nonexistent"],
                )
                .await,
            ),
            (
                "unknown/other-plane",
                post(
                    &server,
                    "dp.one",
                    "capture/bobCo/capture-new",
                    &["bobCo/token"],
                )
                .await,
            ),
            (
                "unknown/no-mapping",
                post(
                    &server,
                    "dp.one",
                    "capture/carolCo/capture-new",
                    &["carolCo/token"],
                )
                .await,
            ),
            // The longest covering mapping decides alone: `aliceCo/private/`
            // admits only dp.two, so dp.one is denied there even though the
            // parent `aliceCo/` mapping admits it -- and dp.two is admitted
            // there even though the parent mapping doesn't list it.
            (
                "nested/parent-not-promoted",
                post(
                    &server,
                    "dp.one",
                    "capture/aliceCo/private/capture-new",
                    &["aliceCo/private/token"],
                )
                .await,
            ),
            (
                "nested/admitted",
                post(
                    &server,
                    "dp.two",
                    "capture/aliceCo/private/capture-new",
                    &["aliceCo/private/token"],
                )
                .await,
            ),
        ];

        insta::assert_debug_snapshot!(outcomes, @r#"
        [
            (
                "sibling/in",
                "200 secretId=1111111111111111",
            ),
            (
                "sibling/out",
                "200 secretId=2222222222222222",
            ),
            (
                "not-sibling",
                "403 task 'aliceCo/out/materialize-bar' may only use secrets under 'aliceCo/out/', and 'aliceCo/in/token' is not one",
            ),
            (
                "absent",
                "404 secret 'aliceCo/in/nonexistent' does not exist",
            ),
            (
                "malformed",
                "400 invalid secret name: aliceCo/bad name doesn't match pattern [\\p{Letter}\\p{Number}\\-_\\.]+(/[\\p{Letter}\\p{Number}\\-_\\.]+)* (unmatched portion is:  name)",
            ),
            (
                "no-selector",
                "400 expected one label for estuary.dev/secret-name (got [])",
            ),
            (
                "two-selectors",
                "400 expected one label for estuary.dev/secret-name (got [Label { name: \"estuary.dev/secret-name\", value: \"aliceCo/in/token\", prefix: false }, Label { name: \"estuary.dev/secret-name\", value: \"aliceCo/out/token\", prefix: false }])",
            ),
            (
                "unknown/admitted",
                "200 secretId=1111111111111111",
            ),
            (
                "unknown/admitted-absent",
                "404 secret 'aliceCo/in/nonexistent' does not exist",
            ),
            (
                "unknown/other-plane",
                "403 no task of shard capture/bobCo/capture-new/0000000000000000/00000000-00000000 is known, and the storage mapping of secret 'bobCo/token' does not admit data-plane ops/dp/public/aws-us-west-2-c1",
            ),
            (
                "unknown/no-mapping",
                "403 no task of shard capture/carolCo/capture-new/0000000000000000/00000000-00000000 is known, and the storage mapping of secret 'carolCo/token' does not admit data-plane ops/dp/public/aws-us-west-2-c1",
            ),
            (
                "nested/parent-not-promoted",
                "403 no task of shard capture/aliceCo/private/capture-new/0000000000000000/00000000-00000000 is known, and the storage mapping of secret 'aliceCo/private/token' does not admit data-plane ops/dp/public/aws-us-west-2-c1",
            ),
            (
                "nested/admitted",
                "200 secretId=3333333333333333",
            ),
        ]
        "#);
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
            "capture/aliceCo/in/capture-foo",
            &["aliceCo/in/token"],
        )
        .await;

        assert!(
            outcome.starts_with("200 retryMillis=") && !outcome.ends_with("=0"),
            "expected a non-zero retry, got {outcome}"
        );
    }
}
