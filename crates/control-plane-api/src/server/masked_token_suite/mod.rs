//! The no-amplification and compatibility suite for capability-masked
//! tokens (#3376, task 7).
//!
//! Every assertion here drives a token *minted by the real
//! `capability_token` grant* through real routes, over a real snapshot of a
//! real database — the composed pipeline no single enforcement or mint test
//! exercises. What this suite holds is the security claim of the whole
//! stack: a token the mint produces can never exercise more authority than
//! its user's live grants, under any mask.
//!
//! Assertions already pinned elsewhere are deliberately not repeated:
//! per-surface enforcement plumbing with hand-signed masked tokens lives in
//! each surface's own module, and several of the task's named properties
//! are already minted compositions there — a minted token cannot re-mint
//! and the mint refuses service accounts (`token_exchange`), a masked
//! bearer cannot `createRefreshToken` while masked revocation stays open
//! (`graphql/refresh_tokens`), and the `/admin` endpoints fail closed at
//! extraction (`create_data_plane`, `update_l2_reporting`, with the mint's
//! verbatim claim stamping pinned by `token_exchange`).
//!
//! The suite reads against the `masked_suite` fixture, whose grant graph is
//! documented (with the per-edge rationale) at the top of
//! `fixtures/masked_suite.sql`.
//!
//! Organization:
//! - [`walk`]: the subset property as an exhaustive mask-family sweep, plus
//!   the named traversal semantics (`Delegate` confinement, `Assume`
//!   containment, the empty mask) and the legacy-metadata probe.
//! - [`lifecycle`]: one minted token observed across snapshot refreshes as
//!   its user's grants change. "Immediately" means the next snapshot
//!   refresh — the running server flips with no restart and no token
//!   invalidation, which these tests pin in both directions.
//! - [`guards_and_compat`]: the credential loop — refresh tokens remain
//!   full-authority credentials for unmasked callers, and an identity-only
//!   minted token still operates as its user.

mod guards_and_compat;
mod lifecycle;
mod walk;

use crate::test_server::TestServer;

pub(crate) const DANA: uuid::Uuid = uuid::Uuid::from_bytes([0x44; 16]);
pub(crate) const ERIN: uuid::Uuid = uuid::Uuid::from_bytes([0x55; 16]);

/// Mint a capability token through the real endpoint, using `bearer` as the
/// full-authority broker, and return the minted access token.
pub(crate) async fn mint(server: &TestServer, bearer: &str, mask: &[&str]) -> String {
    let (status, body) = post_token(
        server,
        &serde_json::json!({
            "grant_type": "capability_token",
            "capability_mask": mask,
        }),
        Some(bearer),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "mint of {mask:?}: {body}");

    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    body["access_token"].as_str().unwrap().to_string()
}

pub(crate) async fn post_token(
    server: &TestServer,
    body: &serde_json::Value,
    bearer: Option<&str>,
) -> (reqwest::StatusCode, String) {
    let response = server
        .rest_client()
        .post("/api/v1/auth/token", body, bearer)
        .send()
        .await
        .unwrap();
    (response.status(), response.text().await.unwrap())
}

/// Present `bearer` to the canonical walk surface, `POST
/// /authorize/user/collection`, and reduce the response to a compact
/// outcome: the authorized journal prefix on success, the status and body
/// on refusal. Refusal bodies carry the caller's email, so outcome strings
/// double as the identity-copy-through probe for minted bearers.
pub(crate) async fn authorize_collection(
    server: &TestServer,
    bearer: &str,
    collection: &str,
    capability: &str,
) -> String {
    let response = server
        .rest_client()
        .post(
            "/authorize/user/collection",
            &serde_json::json!({
                "collection": collection,
                "capability": capability,
            }),
            Some(bearer),
        )
        .send()
        .await
        .unwrap();
    let (status, body) = (response.status(), response.text().await.unwrap());

    if status == reqwest::StatusCode::OK {
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        format!("OK {}", body["journalNamePrefix"].as_str().unwrap())
    } else {
        format!("{} {body}", status.as_u16())
    }
}
