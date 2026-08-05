//! Shared helpers for authorization tests over the `authz_specs` fixture:
//! Snapshots of the database's real grants, stamped relative to the fixture's
//! publication instant so each test chooses whether a denial is authoritative
//! or provisional.

/// The `authz_specs.sql` collection whose publication anchors staleness.
const ANCHOR_COLLECTION: &str = "carolCo/data/foo";

/// Staleness compares the Snapshot's `taken` against the timestamp embedded
/// in a spec's `last_pub_id`, so read that back rather than recomputing it —
/// `flowid` is `macaddr8`, which silently widens short literals.
pub(crate) async fn published_at(pool: &sqlx::PgPool) -> tokens::DateTime {
    sqlx::query_scalar::<_, models::Id>(
        "select last_pub_id from live_specs where catalog_name = $1",
    )
    .bind(ANCHOR_COLLECTION)
    .fetch_one(pool)
    .await
    .expect("fixture collection should exist")
    .timestamp()
}

/// A Snapshot holding the fixture's real grants, stamped `offset` away from
/// the instant the fixture's specs were published.
pub(crate) async fn snapshot_offset(
    pool: &sqlx::PgPool,
    offset: chrono::TimeDelta,
) -> crate::Snapshot {
    let mut decrypted_hmac_keys = std::collections::HashMap::new();
    let data = crate::snapshot::try_fetch(pool, &mut decrypted_hmac_keys)
        .await
        .expect("failed to fetch snapshot");
    crate::Snapshot::new(published_at(pool).await + offset, data)
}

/// Taken clear of the publication plus `TEMPORAL_SKEW`: denials are definitive.
pub(crate) async fn authoritative(pool: &sqlx::PgPool) -> crate::Snapshot {
    snapshot_offset(pool, crate::Snapshot::TEMPORAL_SKEW * 4).await
}

/// Taken before the publication it would judge: denials are retryable.
pub(crate) async fn stale(pool: &sqlx::PgPool) -> crate::Snapshot {
    snapshot_offset(pool, -crate::Snapshot::TEMPORAL_SKEW * 4).await
}

/// Asserts `err` is the retryable stale-snapshot error and names the spec.
pub(crate) fn assert_stale_for(err: anyhow::Error, catalog_name: &str) {
    assert!(
        validation::is_authz_snapshot_stale(&err),
        "expected a retryable stale-snapshot error, got: {err:#}"
    );
    assert!(
        err.to_string().contains(catalog_name),
        "stale error should name the offending spec, got: {err:#}"
    );
}
