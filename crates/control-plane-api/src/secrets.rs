//! What it means to *set* a secret, shared by the two callers which do it: the
//! GraphQL `setSecret` mutation, acting for a user, and `/task/set-secret`,
//! acting for a task rotating a credential it manages.
//!
//! Both must agree, because they write the same row and because the rules here
//! are what keep a wrapped document bound to the name it was wrapped for.

/// Outcome of [`set`].
pub enum SetOutcome {
    /// The document was written, minting `secret_id`.
    Written(models::Id),
    /// The stored document was already structurally identical, so its
    /// `secret_id` stands. Setting is idempotent on the document's identity.
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
/// The parse is into a throwaway side copy: `document` itself stays opaque text,
/// because re-serializing it would reorder keys and break the sops MAC.
///
/// The error is a plain message, since the two callers render it into different
/// error types.
pub fn validate_document(
    catalog_name: &str,
    document: &models::RawValue,
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

    let wrapped: Wrapped = serde_json::from_str(document.get()).map_err(|err| {
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
    document: &models::RawValue,
    last_modified: chrono::DateTime<chrono::Utc>,
) -> sqlx::Result<SetOutcome> {
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
        document.get(),
        last_modified,
    )
    .fetch_one(pg_pool)
    .await?;

    Ok(match (row.written_id, row.prior_id, row.prior_document) {
        (Some(written_id), _, _) => SetOutcome::Written(written_id),
        (None, Some(prior_id), Some(prior_document)) if prior_document == document.get() => {
            SetOutcome::Unchanged(prior_id)
        }
        (None, Some(_), _) => SetOutcome::Stale,
        (None, None, _) => SetOutcome::Conflict,
    })
}
