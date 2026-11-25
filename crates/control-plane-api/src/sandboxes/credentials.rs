//! Credential approvals of `internal.sandbox_credentials`.
//!
//! An approval is consent, not a secret: under a name, for one workspace of
//! one sandbox, its approver allows commands to act with the approver's
//! authority narrowed by capability and prefix masks, until `expires_at`.
//! A command which names the credential receives a token minted afresh from
//! its caller's own credentials and the approval's masks.

use anyhow::Context;

/// Bounds of an approval's duration, in minutes.
pub const DURATION_MINUTES: std::ops::RangeInclusive<i32> = 1..=480;

#[derive(Debug, Clone, PartialEq)]
pub struct Credential {
    pub workspace: String,
    pub name: String,
    pub capability_mask: Vec<String>,
    pub prefix_mask: Vec<String>,
    pub approved_by: uuid::Uuid,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// Checks the arguments of an approval, except for its masks.
pub fn validate(workspace: &str, name: &str, duration_minutes: i32) -> Result<(), String> {
    for (what, value) in [("workspace", workspace), ("credential name", name)] {
        if !sandbox_exec::is_valid_workspace(value) {
            return Err(format!(
                "invalid {what} {value:?}: must match {}",
                sandbox_exec::WORKSPACE_PATTERN
            ));
        }
    }
    if !DURATION_MINUTES.contains(&duration_minutes) {
        return Err(format!(
            "durationMinutes must be between {} and {}, not {duration_minutes}",
            DURATION_MINUTES.start(),
            DURATION_MINUTES.end()
        ));
    }
    Ok(())
}

/// Returns the capability mask of an approval: bundle names in snake case,
/// without duplicates.
///
/// A token's unknown bundle names grant nothing (`ControlClaims::subject`),
/// so an approval which stored one would mint tokens narrower than its
/// approver was shown.
pub fn capability_mask(mask: Vec<String>) -> Result<Vec<String>, String> {
    let known = || {
        <models::authz::CapabilityBundle as strum::VariantArray>::VARIANTS
            .iter()
            .map(|bundle| heck::ToSnakeCase::to_snake_case(format!("{bundle:?}").as_str()))
    };
    let mask = crate::server::public::token_exchange::normalize_capability_mask(mask);

    for name in &mask {
        if !known().any(|bundle| &bundle == name) {
            return Err(format!(
                "Unknown capability {name:?}: use any of {}.",
                itertools::Itertools::join(&mut known(), ", ")
            ));
        }
    }
    Ok(mask)
}

/// Returns the tenants which must have accepted the sandbox agreement before
/// tokens restricted to `prefix_mask` may be minted for sandbox commands.
///
/// Each prefix must be a catalog prefix. Data-plane prefixes under `ops/` are
/// limited to the public data planes (which need no consent) and the private
/// data planes of a tenant (which need the consent of that tenant).
pub fn consent_tenants(
    prefix_mask: &[String],
) -> Result<std::collections::BTreeSet<String>, String> {
    let mut tenants = std::collections::BTreeSet::new();

    for prefix in prefix_mask {
        // CATALOG_PREFIX_RE also matches the empty string.
        if prefix.is_empty() || validator::Validate::validate(&models::Prefix::new(prefix)).is_err()
        {
            return Err(format!(
                "Invalid prefix {prefix:?}: use a nonempty catalog prefix ending in '/'."
            ));
        }
        let parts: Vec<&str> = prefix.split('/').collect();

        if parts.starts_with(&["ops", "dp", "public"]) {
            continue;
        }
        if parts[0] != "ops" {
            tenants.insert(format!("{}/", parts[0]));
            continue;
        }
        // `parts` ends with an empty part, so a tenant's private data planes
        // have at least five.
        if !parts.starts_with(&["ops", "dp", "private"]) || parts.len() < 5 {
            return Err(format!(
                "Unsupported prefix {prefix:?}: use ops/dp/public/ or ops/dp/private/<tenant>/ for data-plane access."
            ));
        }
        tenants.insert(format!("{}/", parts[3]));
    }
    Ok(tenants)
}

/// Inserts the approval, replacing any of the same sandbox, workspace, and
/// name. It expires `duration_minutes` from now.
pub async fn approve(
    pool: &sqlx::PgPool,
    sandbox_id: models::Id,
    workspace: &str,
    name: &str,
    capability_mask: &[String],
    prefix_mask: &[String],
    approved_by: uuid::Uuid,
    duration_minutes: i32,
) -> anyhow::Result<Credential> {
    sqlx::query_as!(
        Credential,
        r#"
        insert into internal.sandbox_credentials (
            sandbox_id, workspace, name, capability_mask, prefix_mask,
            approved_by, expires_at
        )
        values ($1, $2, $3, $4, $5, $6, now() + make_interval(mins => $7))
        on conflict (sandbox_id, workspace, name) do update set
            capability_mask = excluded.capability_mask,
            prefix_mask = excluded.prefix_mask,
            approved_by = excluded.approved_by,
            created_at = now(),
            expires_at = excluded.expires_at
        returning
            workspace,
            name,
            capability_mask,
            prefix_mask,
            approved_by,
            expires_at
        "#,
        sandbox_id as models::Id,
        workspace,
        name,
        capability_mask,
        prefix_mask,
        approved_by,
        duration_minutes,
    )
    .fetch_one(pool)
    .await
    .context("storing sandbox credential approval")
}

/// Fetches the unexpired approvals of the sandbox, by workspace and name.
pub async fn fetch_unexpired(
    pool: &sqlx::PgPool,
    sandbox_id: models::Id,
) -> anyhow::Result<Vec<Credential>> {
    sqlx::query_as!(
        Credential,
        r#"
        select
            workspace,
            name,
            capability_mask,
            prefix_mask,
            approved_by,
            expires_at
        from internal.sandbox_credentials
        where sandbox_id = $1 and expires_at > now()
        order by workspace, name
        "#,
        sandbox_id as models::Id,
    )
    .fetch_all(pool)
    .await
    .context("fetching sandbox credential approvals")
}

/// Fetches the approval of `name` for `workspace` of the sandbox, which may
/// have expired.
pub async fn fetch(
    pool: &sqlx::PgPool,
    sandbox_id: models::Id,
    workspace: &str,
    name: &str,
) -> anyhow::Result<Option<Credential>> {
    sqlx::query_as!(
        Credential,
        r#"
        select
            workspace,
            name,
            capability_mask,
            prefix_mask,
            approved_by,
            expires_at
        from internal.sandbox_credentials
        where sandbox_id = $1 and workspace = $2 and name = $3
        "#,
        sandbox_id as models::Id,
        workspace,
        name,
    )
    .fetch_optional(pool)
    .await
    .context("fetching sandbox credential approval")
}

#[cfg(test)]
mod test {
    #[test]
    fn test_consent_tenants() {
        let cases = [
            vec![],
            vec!["acmeCo/", "acmeCo/nested/", "otherCo/x/"],
            vec!["ops/dp/public/", "ops/dp/public/gcp-us-central1/"],
            vec!["ops/dp/private/acmeCo/", "ops/dp/private/otherCo/aws-eu/"],
            vec!["ops/"],
            vec!["ops/dp/"],
            vec!["ops/dp/private/"],
            vec!["ops/dp/publicX/"],
            vec!["ops/tasks/"],
            vec!["acmeCo"],
            vec![""],
            vec!["/"],
            vec!["acmeCo//"],
            vec!["/acmeCo/"],
            vec!["acme Co/"],
            vec!["acme*Co/"],
            vec!["äcme-Co_1.2/"],
        ];
        let outcomes: Vec<_> = cases
            .into_iter()
            .map(|mask| {
                let mask: Vec<String> = mask.into_iter().map(String::from).collect();
                let outcome = super::consent_tenants(&mask)
                    .map(|tenants| tenants.into_iter().collect::<Vec<_>>());
                (mask, outcome)
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }

    #[test]
    fn test_validate() {
        let long = "a".repeat(64);
        let outcomes = [
            ("ws", "cred", 1),
            ("ws", "cred", 480),
            ("ws-1_a", "0cred", 60),
            ("WS", "cred", 60),
            ("ws", "-cred", 60),
            ("ws", long.as_str(), 60),
            ("ws", "cred", 0),
            ("ws", "cred", 481),
        ]
        .map(|(workspace, name, minutes)| super::validate(workspace, name, minutes));

        insta::assert_debug_snapshot!(outcomes);
    }
}
