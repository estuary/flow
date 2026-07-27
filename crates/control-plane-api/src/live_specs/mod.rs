mod db;

use anyhow::Context;
pub use db::{
    InferredSchemaRow, LiveSpec, fetch_expanded_live_specs, fetch_inferred_schemas,
    fetch_live_spec_names_by_prefix, fetch_live_specs, hard_delete_live_spec,
};
use models::Capability;
use std::ops::Deref;
use uuid::Uuid;

/// Fetches live specs, returning them as a `tables::LiveCatalog`. Optionally
/// filters the specs based on user capability. If `filter_capability` is
/// `None`, then no filtering will be done.
pub async fn get_live_specs(
    user_id: uuid::Uuid,
    names: &[String],
    filter_capability: Option<Capability>,
    db: &sqlx::PgPool,
    snapshot: &crate::Snapshot,
) -> anyhow::Result<tables::LiveCatalog> {
    let mut live = tables::LiveCatalog::default();

    // The query that's used by `fetch_live_specs` can be pretty slow because of how
    // it queries authZ capabilities for each name, even if it doesn't exist.
    // Limit each individual query to 512 names to avoid statement timeouts when
    // fetching a large number of specs when `filter_capability` is `Some`.
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
                if !tables::UserGrant::is_authorized(
                    &snapshot.role_grants,
                    &snapshot.user_grants,
                    user_id,
                    &row.catalog_name,
                    min_capability,
                ) {
                    // A denial evaluated against a snapshot that predates the
                    // spec's own update may be spurious: a just-added grant may
                    // not be reflected in this snapshot yet. Signal stale so the
                    // caller can refresh and retry. An authoritative denial
                    // (snapshot taken after the spec's update) falls through to
                    // today's silent drop.
                    if !snapshot.taken_after(row.last_pub_id.timestamp()) {
                        return Err(validation::Error::AuthorizationSnapshotStale {
                            catalog_name: row.catalog_name.clone(),
                        }
                        .into());
                    }
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

pub async fn get_connected_live_specs(
    user_id: Uuid,
    collection_names: &[&str],
    exclude_names: &[&str],
    filter_capability: Option<Capability>,
    db: &sqlx::PgPool,
    snapshot: &crate::Snapshot,
) -> anyhow::Result<tables::LiveCatalog> {
    let expanded_rows =
        db::fetch_expanded_live_specs(user_id, collection_names, exclude_names, db).await?;
    let mut live = tables::LiveCatalog::default();
    for exp in expanded_rows {
        if let Some(minimum_capability) = filter_capability {
            if !tables::UserGrant::is_authorized(
                &snapshot.role_grants,
                &snapshot.user_grants,
                user_id,
                &exp.catalog_name,
                minimum_capability,
            ) {
                // As in `get_live_specs`, a denial evaluated against a snapshot
                // that predates the spec's own update may be spurious. Signal
                // stale so the caller can refresh and retry; otherwise drop.
                if !snapshot.taken_after(exp.last_pub_id.timestamp()) {
                    return Err(validation::Error::AuthorizationSnapshotStale {
                        catalog_name: exp.catalog_name.clone(),
                    }
                    .into());
                }
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
/// in SQL. Because the Snapshot lags Postgres, a denial is only trusted once the
/// Snapshot is authoritative for the spec being denied; otherwise the caller gets
/// a retryable `AuthorizationSnapshotStale` rather than a silently-dropped spec.
/// These tests pin that three-way outcome — included / dropped / retryable — and
/// the exact instant the last two swap over.
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
        let live = get_live_specs(DAN, &[COLLECTION.to_string()], None, &pool, &snapshot)
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
                Some(Capability::Read),
                &pool,
                &snapshot,
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
            Some(Capability::Read),
            &pool,
            &snapshot,
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
            Some(Capability::Read),
            &pool,
            &snapshot,
        )
        .await
        .expect_err("a denial against a stale Snapshot should be retryable");

        assert_stale_for(err, COLLECTION);
    }

    /// The changeover is governed by `Snapshot::taken_after`, whose skew
    /// allowance is exclusive. Pin both sides of that boundary so a change to the
    /// comparison can't quietly turn retryable denials into hard ones.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_get_live_specs_staleness_boundary(pool: sqlx::PgPool) {
        let at_skew = snapshot_offset(&pool, crate::Snapshot::TEMPORAL_SKEW).await;
        let err = get_live_specs(
            DAN,
            &[COLLECTION.to_string()],
            Some(Capability::Read),
            &pool,
            &at_skew,
        )
        .await
        .expect_err("exactly TEMPORAL_SKEW past publication is still stale");
        assert_stale_for(err, COLLECTION);

        let past_skew = snapshot_offset(
            &pool,
            crate::Snapshot::TEMPORAL_SKEW + chrono::TimeDelta::milliseconds(1),
        )
        .await;
        let live = get_live_specs(
            DAN,
            &[COLLECTION.to_string()],
            Some(Capability::Read),
            &pool,
            &past_skew,
        )
        .await
        .expect("one millisecond later the denial is authoritative");
        assert!(live.collections.is_empty());
    }

    /// `get_connected_live_specs` reaches specs by graph traversal rather than by
    /// name, but applies the identical rule. The fixture's capture writes to the
    /// collection, so it is reachable from it.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_get_connected_live_specs_staleness(pool: sqlx::PgPool) {
        // Exclude the collection itself, leaving just the capture that writes it.
        async fn connected(
            pool: &sqlx::PgPool,
            user: uuid::Uuid,
            snapshot: &crate::Snapshot,
            filter: Option<Capability>,
        ) -> anyhow::Result<tables::LiveCatalog> {
            get_connected_live_specs(user, &[COLLECTION], &[COLLECTION], filter, pool, snapshot)
                .await
        }

        let live = connected(
            &pool,
            CAROL,
            &authoritative(&pool).await,
            Some(Capability::Read),
        )
        .await
        .expect("carol is authorized");
        assert_eq!(1, live.captures.len());
        assert_eq!(CAPTURE, live.captures[0].capture.as_str());

        let live = connected(
            &pool,
            DAN,
            &authoritative(&pool).await,
            Some(Capability::Read),
        )
        .await
        .expect("an authoritative denial is not an error");
        assert!(live.captures.is_empty());

        let err = connected(&pool, DAN, &stale(&pool).await, Some(Capability::Read))
            .await
            .expect_err("a denial against a stale Snapshot should be retryable");
        assert_stale_for(err, CAPTURE);

        let live = connected(&pool, DAN, &stale(&pool).await, None)
            .await
            .expect("an unfiltered traversal should not consult the Snapshot");
        assert_eq!(1, live.captures.len());
    }
}
