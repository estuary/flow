//! One minted token observed across snapshot refreshes as its user's
//! grants change underneath it.
//!
//! A minted token embeds identity and mask, never grants: every request is
//! decided against the server's current Snapshot. "A grant change takes
//! effect immediately" therefore means *at the next snapshot refresh* — the
//! stages below pin the stale window on each side of a refresh as
//! deliberately as the flip itself.

use super::{ERIN, authorize_collection, mint};
use crate::test_server;

#[sqlx::test(
    migrations = "../../supabase/migrations",
    fixtures(path = "../../fixtures", scripts("data_planes", "masked_suite"))
)]
async fn test_grants_change_under_a_live_token(pool: sqlx::PgPool) {
    let _guard = test_server::init();

    let mut snapshot = test_server::RefreshableSnapshot::start(pool.clone()).await;
    let server = test_server::TestServer::start(pool.clone(), snapshot.watch()).await;
    let erin = server.make_access_token(ERIN, Some("erin@example.test"));

    // Erin holds `read` on sharedCo/; her token is approved for Writer.
    // The mask covers the Write requirement, so the refusal is the walk's:
    // approved-but-unheld bits stay inert until a grant actually holds them.
    let token = mint(&server, &erin, &["Writer"]).await;

    let outcome = authorize_collection(&server, &token, "sharedCo/data/collection", "write").await;
    insta::assert_snapshot!(outcome, @"403 erin@example.test is not authorized to sharedCo/data/collection for Write");
    let outcome = authorize_collection(&server, &token, "sharedCo/data/collection", "read").await;
    assert!(outcome.starts_with("OK"), "{outcome}");

    // === A grant addition activates already-approved bits ===
    sqlx::query("UPDATE public.user_grants SET capability = 'write' WHERE user_id = $1")
        .bind(ERIN)
        .execute(&pool)
        .await
        .unwrap();

    // The running server still holds the prior snapshot: the mutation is
    // not yet visible, which is the deliberate meaning of "immediately".
    let outcome = authorize_collection(&server, &token, "sharedCo/data/collection", "write").await;
    assert!(
        outcome.starts_with("403"),
        "stale snapshot still refuses: {outcome}"
    );

    // At the next refresh the same token — approved for Writer all along —
    // exercises the new grant with no re-mint.
    snapshot.refresh().await;
    let outcome = authorize_collection(&server, &token, "sharedCo/data/collection", "write").await;
    assert!(outcome.starts_with("OK"), "{outcome}");

    // === Revocation takes effect at the next refresh ===
    sqlx::query("DELETE FROM public.user_grants WHERE user_id = $1")
        .bind(ERIN)
        .execute(&pool)
        .await
        .unwrap();

    let outcome = authorize_collection(&server, &token, "sharedCo/data/collection", "read").await;
    assert!(
        outcome.starts_with("OK"),
        "stale snapshot still allows: {outcome}"
    );

    snapshot.refresh().await;
    let outcome = authorize_collection(&server, &token, "sharedCo/data/collection", "read").await;
    insta::assert_snapshot!(outcome, @"403 erin@example.test is not authorized to sharedCo/data/collection for Read");
}
