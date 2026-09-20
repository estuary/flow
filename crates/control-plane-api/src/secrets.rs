//! Secret storage, and the policy by which tasks may use it.
//!
//! A task may use the secrets which sit beside it in the catalog namespace.
//!
//! Setting is shared by the GraphQL `setSecret` mutation, acting for a user,
//! and by later data-plane routes acting for a task.

/// Why a task is not allowed to access a secret.
#[derive(Debug, thiserror::Error)]
pub enum TaskAccessError {
    #[error("task '{task_name}' and secret '{secret_name}' are not both catalog names")]
    NotCatalogNames {
        task_name: String,
        secret_name: String,
    },
    #[error(
        "task '{task_name}' may only use secrets under '{task_parent}', and '{secret_name}' is not one"
    )]
    NotReadableSibling {
        task_name: String,
        task_parent: String,
        secret_name: String,
    },
}

/// Validate the catalog-name relationship by which a task may access a secret.
pub fn validate_task_access(
    task_name: &models::Name,
    secret_name: &models::Name,
) -> Result<(), TaskAccessError> {
    let (Some(task_parent), Some(secret_parent)) = (
        parent_prefix(task_name.as_str()),
        parent_prefix(secret_name.as_str()),
    ) else {
        return Err(TaskAccessError::NotCatalogNames {
            task_name: task_name.to_string(),
            secret_name: secret_name.to_string(),
        });
    };

    if task_parent != secret_parent {
        return Err(TaskAccessError::NotReadableSibling {
            task_name: task_name.to_string(),
            task_parent: task_parent.to_string(),
            secret_name: secret_name.to_string(),
        });
    }
    Ok(())
}

fn parent_prefix(name: &str) -> Option<&str> {
    name.rfind('/').map(|index| &name[..index + 1])
}

/// A wrapped secret document read from storage.
pub struct StoredSecret {
    pub document: serde_json::Value,
    pub secret_id: models::Id,
}

/// Fetch the current wrapped document of a secret.
pub async fn fetch(
    pg_pool: &sqlx::PgPool,
    name: &models::Name,
) -> sqlx::Result<Option<StoredSecret>> {
    let row = sqlx::query!(
        r#"
        SELECT
            document AS "document!: serde_json::Value",
            id AS "secret_id!: models::Id"
        FROM internal.secrets
        WHERE catalog_name = $1::text::catalog_name
        "#,
        name.as_str(),
    )
    .fetch_optional(pg_pool)
    .await?;

    Ok(row.map(|row| StoredSecret {
        document: row.document,
        secret_id: row.secret_id,
    }))
}

/// Outcome of [`set`].
pub enum SetOutcome {
    /// The document was written, minting `secret_id`.
    Written(models::Id),
    /// The stored document was already identical, so its `secret_id` stands.
    /// Setting is idempotent on the document's value.
    Unchanged(models::Id),
    /// The stored document's `sops.lastmodified` is newer than the provided
    /// one: a stale re-apply, which is refused rather than applied.
    Stale,
    /// Another request created this secret concurrently, and wrote nothing.
    Conflict,
}

/// Structurally validate a wrapped secret document against the name it is being
/// set at, returning its embedded `sops.lastmodified`.
///
/// The error is a plain message, since the two callers render it into different
/// error types.
pub fn validate_document(
    catalog_name: &str,
    document: &serde_json::Value,
) -> Result<chrono::DateTime<chrono::Utc>, String> {
    // Only the fields the control plane must agree with sops about. `value` is
    // checked for presence alone -- its content is ciphertext we cannot read.
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

    let wrapped = <Wrapped as serde::Deserialize>::deserialize(document).map_err(|err| {
        format!("document is not a wrapped secret produced by /secret/encrypt: {err}")
    })?;

    if wrapped.name != catalog_name {
        return Err(format!(
            "document is wrapped for secret '{}', not '{catalog_name}'; a wrapped document is \
             bound to its name and cannot be set under another",
            wrapped.name,
        ));
    }

    // Postgres re-parses the *stored* `lastmodified` text on every later set of
    // this secret, so a value chrono accepts here but Postgres cannot parse
    // would wedge the row: every future set fails until the secret is deleted.
    // The parsers diverge on year zero and on UTC offsets beyond Postgres's
    // ±15:59 -- sops never emits either (it writes UTC 'Z' timestamps), so
    // reject them up front rather than store a poison value.
    let last_modified = wrapped.sops.lastmodified;
    if chrono::Datelike::year(&last_modified) < 1
        || last_modified.offset().local_minus_utc().abs() > 15 * 3600 + 59 * 60
    {
        return Err(format!(
            "document's sops.lastmodified '{last_modified}' is outside the range \
             this API can store"
        ));
    }

    Ok(last_modified.to_utc())
}

/// Write the wrapped `document` of secret `catalog_name`, for a caller already
/// authorized to set it. `last_modified` comes from [`validate_document`].
pub async fn set(
    pg_pool: &sqlx::PgPool,
    catalog_name: &str,
    document: &serde_json::Value,
    last_modified: chrono::DateTime<chrono::Utc>,
) -> sqlx::Result<SetOutcome> {
    // Sorted, because a `Value` serialization is, which is what lets the
    // no-op check below be a string comparison.
    let document = document.to_string();

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
        catalog_name,
        &document,
        last_modified,
    )
    .fetch_one(pg_pool)
    .await?;

    Ok(match (row.written_id, row.prior_id, row.prior_document) {
        (Some(written_id), _, _) => SetOutcome::Written(written_id),
        (None, Some(prior_id), Some(prior_document)) if prior_document == document => {
            SetOutcome::Unchanged(prior_id)
        }
        (None, Some(_), _) => SetOutcome::Stale,
        (None, None, _) => SetOutcome::Conflict,
    })
}

#[cfg(test)]
mod tests {
    use super::{validate_document, validate_task_access};

    const TASK: &str = "acmeCo/in/capture-foo";

    /// Cases are aligned data; rustfmt's call-width budget would otherwise
    /// break each of them across four lines.
    #[rustfmt::skip]
    #[test]
    fn test_task_access() {
        let cases = [
            ("sibling", TASK, "acmeCo/in/token"),

            // Non-siblings: one level too deep, one level too shallow, and a
            // sibling-looking name under another tenant.
            ("child", TASK, "acmeCo/in/db/token"),
            ("parent", TASK, "acmeCo/token"),
            ("other-tenant", TASK, "bobCo/in/token"),

            // Neither name has a prefix to compare when it isn't a catalog
            // name, and `models::Name` permits a single bare segment.
            ("task-not-a-name", "capture-foo", "acmeCo/in/token"),
            ("secret-not-a-name", TASK, "token"),
        ];

        let outcomes: Vec<_> = cases
            .into_iter()
            .map(|(label, task, secret)| {
                let outcome = validate_task_access(
                    &models::Name::new(task),
                    &models::Name::new(secret),
                )
                .map(|()| "Ok".to_string())
                .unwrap_or_else(|err| err.to_string());
                (label, outcome)
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }

    /// The document guard is a pure function shared by the GraphQL mutation and
    /// `/task/set-secret`, so its shape cases live here rather than being paid
    /// for twice through an HTTP and database round trip.
    #[test]
    fn test_validate_document() {
        const NAME: &str = "acmeCo/in/token";
        let wrapped = |name: &str, last_modified: &str| {
            serde_json::json!({
                "name": name,
                "value": "ENC[AES256_GCM,data:aaa,type:str]",
                "sops": {"lastmodified": last_modified},
            })
        };

        let cases = [
            ("ok", wrapped(NAME, "2026-08-18T10:00:00Z")),
            // sops MACs the plaintext `name`, so a document is bound to it and
            // cannot be cloned under another; the mismatch is caught up front
            // rather than surfacing later as a decryption failure.
            (
                "wrong-name",
                wrapped("acmeCo/in/other", "2026-08-18T10:00:00Z"),
            ),
            // A `lastmodified` that chrono accepts but Postgres cannot re-parse
            // would wedge the row: every future set fails until it is deleted.
            ("year-zero", wrapped(NAME, "0000-12-31T23:59:59Z")),
            (
                "offset-beyond-postgres",
                wrapped(NAME, "2026-08-18T10:00:00+16:00"),
            ),
            // An offset sops never emits, but which Postgres does accept.
            (
                "offset-within-postgres",
                wrapped(NAME, "2026-08-18T10:00:00+14:00"),
            ),
            // Documents which aren't wrapped secrets at all, rejected on shape.
            ("no-sops", serde_json::json!({"name": NAME, "value": "x"})),
            (
                "no-value",
                serde_json::json!({"name": NAME, "sops": {"lastmodified": "2026-08-18T10:00:00Z"}}),
            ),
            (
                "unparseable-lastmodified",
                serde_json::json!({"name": NAME, "value": "x", "sops": {"lastmodified": "whenever"}}),
            ),
            ("bare-string", serde_json::json!("just a string")),
        ];

        let outcomes: Vec<_> = cases
            .into_iter()
            .map(|(label, document)| {
                let outcome = validate_document(NAME, &document)
                    .map(|last_modified| last_modified.to_rfc3339())
                    .unwrap_or_else(|err| err);
                (label, outcome)
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }
}
