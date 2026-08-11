mod db;

use anyhow::Context;
pub use db::{
    InferredSchemaRow, LiveSpec, fetch_expanded_live_specs, fetch_inferred_schemas,
    fetch_live_spec_names_by_prefix, fetch_live_specs, hard_delete_live_spec,
};
use std::ops::Deref;
use uuid::Uuid;

/// Fetches live specs, returning them as a `tables::LiveCatalog`. Optionally
/// filters the specs based on user capability. If `filter_capability` is
/// `None`, then no filtering will be done.
///
/// `started_at` anchors the staleness check to the given time (request-relative).
/// When `None`, staleness is anchored to each spec's publication time (spec-relative).
/// A denial from a snapshot older than the anchor is provisional — authority
/// committed before the anchor may be missing from it — and surfaces as a
/// retryable error. A snapshot taken after the anchor is authoritative: it is
/// guaranteed to reflect everything committed before the anchor, but not
/// necessarily changes committed after it.
pub async fn get_live_specs(
    user_id: uuid::Uuid,
    names: &[String],
    filter_capability: Option<models::authz::CapabilitySet>,
    db: &sqlx::PgPool,
    snapshot: &crate::Snapshot,
    started_at: Option<tokens::DateTime>,
) -> anyhow::Result<tables::LiveCatalog> {
    let mut live = tables::LiveCatalog::default();

    // Fetch in batches of 512 names. The recursive per-name authorization
    // work which originally motivated batching (see #1895) has moved
    // in-process, but each returned row still carries unbounded `spec` and
    // `built_spec` JSON documents, and a large discover can request thousands
    // of names at once. Batching bounds each statement's execution and
    // transfer time — keeping every statement clear of `statement_timeout`
    // regardless of catalog size — at the cost of a round trip per batch.
    for names_chunk in names.chunks(512) {
        let rows = db::fetch_live_specs(names_chunk, db).await?;
        for row in rows {
            // Spec type might be null because we used to set it to null when deleting specs.
            // For recently deleted specs, it will still be present.
            let Some(catalog_type) = row.spec_type.map(Into::into) else {
                continue;
            };
            let Some(model_json) = row.spec.as_deref() else {
                continue;
            };
            if let Some(min_capability) = filter_capability {
                // For discovers, anchor to the discover request time (started_at).
                // For other callers, anchor to the spec's publication time.
                // An authoritative denial is today's silent drop; a provisional
                // one surfaces as a retryable stale error.
                let anchor = started_at.unwrap_or_else(|| row.last_pub_id.timestamp());
                if !snapshot
                    .user_authorization(user_id, &row.catalog_name, min_capability, Some(anchor))
                    .ok_or_stale(&row.catalog_name)?
                {
                    continue;
                }
            }
            let built_spec_json: &Box<sqlx::types::JsonRawValue> = row.built_spec.as_ref().ok_or_else(|| {
                tracing::warn!(catalog_name = %row.catalog_name, id = %row.id, "got row with spec but not built_spec");
                anyhow::anyhow!("missing built_spec for {:?}, but spec is non-null", row.catalog_name)
            })?.deref();

            live.add_spec(
                catalog_type,
                &row.catalog_name,
                row.id.into(),
                row.data_plane_id.into(),
                row.last_pub_id.into(),
                row.last_build_id.into(),
                model_json,
                built_spec_json,
                row.dependency_hash,
            )
            .with_context(|| format!("deserializing specs for {:?}", row.catalog_name))?;
        }
    }

    Ok(live)
}

/// Fetches the live specs connected to `collection_names` — tasks that read
/// from or write to them — excluding `exclude_names`. When `filter_capability`
/// is set, specs to which the user lacks that capability are silently omitted:
/// expansion is filtering, so a denial is final regardless of Snapshot
/// freshness and is never surfaced as a retryable stale error.
pub async fn get_connected_live_specs(
    user_id: Uuid,
    collection_names: &[&str],
    exclude_names: &[&str],
    filter_capability: Option<models::authz::CapabilitySet>,
    db: &sqlx::PgPool,
    snapshot: &crate::Snapshot,
) -> anyhow::Result<tables::LiveCatalog> {
    let expanded_rows = db::fetch_expanded_live_specs(collection_names, exclude_names, db).await?;
    let mut live = tables::LiveCatalog::default();

    for exp in expanded_rows {
        if let Some(minimum_capability) = filter_capability {
            // Expansion widens validation with specs the caller never named, so
            // a denial is a final omission rather than an error, and never
            // consults Snapshot freshness (`None` anchor): the worst case of a
            // not-yet-observed grant is only a narrower validation, while an
            // anchored check would defer nearly every publication touching a
            // connected spec its user can't admin, since the pinned Snapshot
            // almost always predates the queued row.
            if !snapshot
                .user_authorization(user_id, &exp.catalog_name, minimum_capability, None)
                .ok_or_stale(&exp.catalog_name)?
            {
                continue;
            }
        }
        // TODO: These fields should be non-nullable, so we may be able to remove these checks.
        let Some(spec_type) = exp.spec_type else {
            anyhow::bail!("missing spec_type for expanded row: {:?}", exp.catalog_name);
        };
        let Some(model_json) = &exp.spec else {
            anyhow::bail!("missing spec for expanded row: {:?}", exp.catalog_name);
        };
        let Some(built_json) = &exp.built_spec else {
            anyhow::bail!(
                "missing built_spec for expanded row: {:?}",
                exp.catalog_name
            );
        };

        live.add_spec(
            spec_type,
            &exp.catalog_name,
            exp.id,
            exp.data_plane_id,
            exp.last_pub_id,
            exp.last_build_id,
            &model_json.0,
            &built_json,
            exp.dependency_hash.clone(),
        )?;
    }
    Ok(live)
}

/// Both fetchers apply authorization in-process against a `Snapshot` rather than
/// in SQL, but they trust a denial differently. `get_live_specs` fetches specs
/// the caller explicitly named, where a wrongly-dropped spec corrupts the
/// operation's output; because the Snapshot lags Postgres, a denial is only
/// trusted once the Snapshot is authoritative for the operation asking — its
/// `started_at` request time when the caller has a durable one, or the denied
/// spec's own last publication otherwise — and until then the caller gets a
/// retryable `AuthorizationSnapshotStale` rather than a silently-dropped spec.
/// `get_connected_live_specs` expands to specs the caller never named, purely to
/// widen validation, so a denial is always a final silent omission and Snapshot
/// freshness is never consulted.
/// These tests pin the three-way outcome — included / dropped / retryable — for
/// the former, the exact instant the last two swap over, and the two-way
/// outcome for the latter.
#[cfg(test)]
mod tests {
    use super::*;

    // From `fixtures/authz_specs.sql`. Carol is admin of `carolCo/`; Dan holds no
    // grants at all and so models an unauthorized caller.
    const CAROL: uuid::Uuid = uuid::uuid!("33333333-3333-3333-3333-333333333333");
    const DAN: uuid::Uuid = uuid::uuid!("44444444-4444-4444-4444-444444444444");
    const COLLECTION: &str = "carolCo/data/foo";
    const CAPTURE: &str = "carolCo/in/capture-foo";

    /// Staleness compares the Snapshot's `taken` against the timestamp embedded
    /// in a spec's `last_pub_id`, so read that back rather than recomputing it —
    /// `flowid` is `macaddr8`, which silently widens short literals.
    async fn published_at(pool: &sqlx::PgPool) -> tokens::DateTime {
        sqlx::query_scalar!(
            r#"select last_pub_id as "last_pub_id: models::Id"
            from live_specs where catalog_name = $1"#,
            COLLECTION,
        )
        .fetch_one(pool)
        .await
        .expect("fixture collection should exist")
        .timestamp()
    }

    /// A Snapshot holding the fixture's real grants, stamped `offset` away from
    /// the instant the fixture's specs were published.
    async fn snapshot_offset(pool: &sqlx::PgPool, offset: chrono::TimeDelta) -> crate::Snapshot {
        let mut decrypted_hmac_keys = std::collections::HashMap::new();
        let data = crate::snapshot::try_fetch(pool, &mut decrypted_hmac_keys)
            .await
            .expect("failed to fetch snapshot");
        crate::Snapshot::new(published_at(pool).await + offset, data)
    }

    /// Taken clear of the publication plus `TEMPORAL_SKEW`: denials are definitive.
    async fn authoritative(pool: &sqlx::PgPool) -> crate::Snapshot {
        snapshot_offset(pool, crate::Snapshot::TEMPORAL_SKEW * 4).await
    }

    /// Taken before the publication it would judge: denials are retryable.
    async fn stale(pool: &sqlx::PgPool) -> crate::Snapshot {
        snapshot_offset(pool, -crate::Snapshot::TEMPORAL_SKEW * 4).await
    }

    fn assert_stale_for(err: anyhow::Error, catalog_name: &str) {
        assert!(
            validation::is_authz_snapshot_stale(&err),
            "expected a retryable stale-snapshot error, got: {err:#}"
        );
        assert!(
            err.to_string().contains(catalog_name),
            "stale error should name the offending spec, got: {err:#}"
        );
    }

    /// With no capability filter the Snapshot is never consulted, so even a
    /// wholly unauthorized caller reading against a stale Snapshot gets the spec.
    /// This is the path controllers and other system callers take.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_get_live_specs_unfiltered_never_stale(pool: sqlx::PgPool) {
        let snapshot = stale(&pool).await;
        let live = get_live_specs(DAN, &[COLLECTION.to_string()], None, &pool, &snapshot, None)
            .await
            .expect("an unfiltered fetch should not consult the Snapshot");

        assert_eq!(1, live.collections.len());
        assert_eq!(COLLECTION, live.collections[0].collection.as_str());
    }

    /// An authorized caller gets the spec no matter how old the Snapshot is:
    /// staleness only ever converts a *denial* into a retry.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_get_live_specs_authorized_is_included(pool: sqlx::PgPool) {
        for snapshot in [stale(&pool).await, authoritative(&pool).await] {
            let live = get_live_specs(
                CAROL,
                &[COLLECTION.to_string()],
                Some(models::authz::Capability::CatalogRead.into()),
                &pool,
                &snapshot,
                None,
            )
            .await
            .expect("carol is admin of carolCo/");

            assert_eq!(1, live.collections.len());
        }
    }

    /// An authoritative denial keeps the pre-existing behavior: the spec is
    /// silently omitted rather than raising.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_get_live_specs_authoritative_denial_is_dropped(pool: sqlx::PgPool) {
        let snapshot = authoritative(&pool).await;
        let live = get_live_specs(
            DAN,
            &[COLLECTION.to_string()],
            Some(models::authz::Capability::CatalogRead.into()),
            &pool,
            &snapshot,
            None,
        )
        .await
        .expect("an authoritative denial is not an error");

        assert!(
            live.collections.is_empty(),
            "an unauthorized spec should be omitted"
        );
    }

    /// The new behavior: the same denial, judged by a Snapshot that predates the
    /// spec, is retryable instead — the grant that would allow it may simply not
    /// have propagated into this Snapshot yet.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_get_live_specs_stale_denial_is_retryable(pool: sqlx::PgPool) {
        let snapshot = stale(&pool).await;
        let err = get_live_specs(
            DAN,
            &[COLLECTION.to_string()],
            Some(models::authz::Capability::CatalogRead.into()),
            &pool,
            &snapshot,
            None,
        )
        .await
        .expect_err("a denial against a stale Snapshot should be retryable");

        assert_stale_for(err, COLLECTION);
    }

    /// `get_connected_live_specs` reaches specs by graph traversal rather than by
    /// name, and it filters rather than authorizes: an unauthorized spec is
    /// silently omitted, and the Snapshot's age never converts that omission
    /// into a retryable error. The fixture's capture writes to the collection,
    /// so it is reachable from it.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_get_connected_live_specs_filtering(pool: sqlx::PgPool) {
        // Exclude the collection itself, leaving just the capture that writes it.
        async fn connected(
            pool: &sqlx::PgPool,
            user: uuid::Uuid,
            snapshot: &crate::Snapshot,
            filter: Option<models::authz::CapabilitySet>,
        ) -> anyhow::Result<tables::LiveCatalog> {
            get_connected_live_specs(user, &[COLLECTION], &[COLLECTION], filter, pool, snapshot)
                .await
        }
        let read_filter = Some(models::authz::CapabilitySet::from(
            models::authz::Capability::CatalogRead,
        ));

        let live = connected(&pool, CAROL, &authoritative(&pool).await, read_filter)
            .await
            .expect("carol is authorized");
        assert_eq!(1, live.captures.len());
        assert_eq!(CAPTURE, live.captures[0].capture.as_str());

        let live = connected(&pool, DAN, &authoritative(&pool).await, read_filter)
            .await
            .expect("an authoritative denial is not an error");
        assert!(live.captures.is_empty());

        let live = connected(&pool, DAN, &stale(&pool).await, read_filter)
            .await
            .expect("a denial filters silently even under a stale Snapshot");
        assert!(live.captures.is_empty());

        let live = connected(&pool, DAN, &stale(&pool).await, None)
            .await
            .expect("an unfiltered traversal should not consult the Snapshot");
        assert_eq!(1, live.captures.len());
    }
}
