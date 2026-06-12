/// Returns catalog prefixes where the authenticated user has at least
/// `min_capability`, optionally narrowed to those overlapping `prefix_filter`
/// and/or falling within `tenant`'s scope.
///
/// Intended for use by GraphQL queries that list resources scoped to the
/// caller's authorized prefixes, with optional prefix and tenant filters.
///
/// `prefix_filter` is **deprecated** in favor of `tenant`; prefer `tenant` for
/// new callers. It remains only for the legacy `catalogPrefix`-style filters on
/// alert configs and invite links, and should be removed once those migrate.
/// When provided, a prefix is included if the filter is a sub-prefix of the
/// grant OR the grant is a sub-prefix of the filter. This bidirectional check
/// lets callers query with a filter that is either broader or narrower than
/// their grants.
///
/// Filtering here is a semantic no-op on query results — callers also apply
/// the filter in SQL, and a grant that doesn't overlap the filter can't match
/// any row that does. It exists to shrink the returned prefix list, which
/// mostly matters for support users (e.g. `estuary_support`) whose many grants
/// would otherwise trip callers' too-many-prefixes guards. Worth reconsidering
/// after refactoring (and perhaps removing the `estuary_support` role).
///
/// When `tenant` is provided, results are clamped to the intersection of the
/// user's authorized set and the tenant's own reach at `min_capability` (see
/// `tenant_prefixes`): where the user's grant is broader than the tenant's
/// reach, the tenant's narrower prefix is returned in its place. Every
/// returned prefix lies within both sets, so the filter can never widen the
/// user's access, and the listing never extends past the tenant's scope.
pub(super) fn authorized_prefixes(
    role_grants: &tables::RoleGrants,
    user_grants: &tables::UserGrants,
    user_id: uuid::Uuid,
    min_capability: impl Into<models::authz::CapabilitySet>,
    prefix_filter: Option<&str>,
    tenant: Option<&str>,
) -> Vec<String> {
    let min_bits: models::authz::CapabilitySet = min_capability.into();

    // BTreeMap iteration from reachable_prefixes is already prefix-sorted,
    // so the parent-prune step can run directly on it.
    let prefixes = tables::UserGrant::reachable_prefixes(role_grants, user_grants, user_id)
        .into_iter()
        .filter(|(prefix, _)| {
            prefix_filter.is_none_or(|pf| prefix.starts_with(pf) || pf.starts_with(*prefix))
        })
        .filter(|(_, (bits, _))| bits.is_superset(min_bits))
        .map(|(prefix, _)| prefix.to_string());

    let mut pruned = prune_covered(prefixes);

    // Clamp to the tenant's own reach at `min_capability`: for each
    // overlapping pair, keep the narrower prefix. The user's prefix wins when
    // it falls within the tenant's reach; the tenant's wins when the user's
    // broader grant covers it (the user is authorized for the narrower prefix
    // by inheritance, so this never widens access — see `tenant_prefixes`).
    if let Some(tenant) = tenant {
        let tenant_scope = tenant_prefixes(role_grants, tenant, min_bits);
        let mut narrowed: Vec<String> = Vec::new();
        for p in &pruned {
            for t in &tenant_scope {
                if p.starts_with(t.as_str()) {
                    narrowed.push(p.clone());
                } else if t.starts_with(p.as_str()) {
                    narrowed.push(t.clone());
                }
            }
        }
        // A prefix can be pushed via several pairings (e.g. the tenant's own
        // prefix plus a reached child of it), re-introducing covered children.
        narrowed.sort();
        pruned = prune_covered(narrowed);
    }

    pruned
}

/// Drops prefixes covered by a preceding broader prefix (and exact
/// duplicates). Input must be sorted, which places a parent immediately
/// before its descendants.
fn prune_covered(sorted: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut pruned: Vec<String> = Vec::new();
    for p in sorted {
        if pruned
            .last()
            .is_none_or(|parent| !p.starts_with(parent.as_str()))
        {
            pruned.push(p);
        }
    }
    pruned
}

/// Returns the catalog prefixes that make up `tenant`'s scope: the tenant
/// prefix itself, plus every prefix reachable from it through the role-grant
/// graph at which the tenant holds at least `min_capability`. Reachability is
/// transitive and uses the same BFS as authorization
/// (`RoleGrant::reachable_nodes`): the tenant's direct grants are always
/// reached, but an onward hop only follows a *delegating* grant — admin
/// delegates, so admin chains extend the scope, while a plain read or write
/// grant is a leaf.
///
/// The capability filter applies to the *reached* prefixes; the tenant's own
/// prefix is always included, as it defines the tenant. Used to narrow a
/// resource listing to a single tenant. This describes the tenant's reach, not
/// the viewer's — callers intersect it with the viewer's authorized prefixes,
/// so it confers no visibility on its own.
pub(super) fn tenant_prefixes(
    role_grants: &tables::RoleGrants,
    tenant: &str,
    capabilities: impl Into<models::authz::CapabilitySet>,
) -> Vec<String> {
    let min_bits: models::authz::CapabilitySet = capabilities.into();

    // Union effective capability bits per reached prefix across every path that
    // arrives there (a prefix can be reached via several grants), mirroring
    // `reachable_prefixes` for users. A prefix qualifies when its union is a
    // superset of `capabilities`.
    let mut by_prefix: std::collections::BTreeMap<&str, models::authz::CapabilitySet> =
        std::collections::BTreeMap::new();
    for node in tables::RoleGrant::reachable_nodes(role_grants, tenant) {
        *by_prefix.entry(node.object_role).or_default() |= node.capabilities;
    }

    let mut out = vec![tenant.to_string()];
    for (prefix, bits) in by_prefix {
        if bits.is_superset(min_bits) {
            out.push(prefix.to_string());
        }
    }
    // A self-grant (acmeCo/ -> acmeCo/) can re-emit the tenant; sort+dedup keeps
    // the result minimal and deterministic.
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::{authorized_prefixes, tenant_prefixes};
    use models::Capability::{Admin, Read, Write};

    fn make_grants(
        user_grants: &[(uuid::Uuid, &str, models::Capability)],
        role_grants: &[(&str, &str, models::Capability)],
    ) -> (tables::UserGrants, tables::RoleGrants) {
        let ug = tables::UserGrants::from_iter(user_grants.iter().map(|(id, obj, cap)| {
            tables::UserGrant {
                user_id: *id,
                object_role: models::Prefix::new(*obj),
                capability: *cap,
                bundles: vec![],
            }
        }));
        let rg = tables::RoleGrants::from_iter(role_grants.iter().map(|(sub, obj, cap)| {
            tables::RoleGrant {
                subject_role: models::Prefix::new(*sub),
                object_role: models::Prefix::new(*obj),
                capability: *cap,
                bundles: vec![],
            }
        }));
        (ug, rg)
    }

    const ALICE: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);

    #[test]
    fn no_filter_returns_all_at_or_above_capability() {
        let (ug, rg) = make_grants(
            &[
                (ALICE, "acmeCo/", Admin),
                (ALICE, "widgets/", Write),
                (ALICE, "readonly/", Read),
            ],
            &[],
        );

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, None);
        assert_eq!(result, vec!["acmeCo/"]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None, None);
        assert_eq!(result, vec!["acmeCo/", "widgets/"]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Read, None, None);
        assert_eq!(result, vec!["acmeCo/", "readonly/", "widgets/"]);
    }

    #[test]
    fn filter_narrower_than_grant() {
        // Grant is on "acmeCo/", filter is "acmeCo/data/" — the grant covers
        // the filter, so "acmeCo/" is included.
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, Some("acmeCo/data/"), None);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn filter_broader_than_grant() {
        // Grant is on "acmeCo/data/", filter is "acmeCo/" — the grant starts
        // with the filter, so "acmeCo/data/" is included.
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/data/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, Some("acmeCo/"), None);
        assert_eq!(result, vec!["acmeCo/data/"]);
    }

    #[test]
    fn filter_excludes_non_overlapping() {
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin), (ALICE, "other/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, Some("acmeCo/"), None);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn no_grants_returns_empty() {
        let (ug, rg) = make_grants(&[], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, None);
        assert!(result.is_empty());
    }

    #[test]
    fn includes_transitive_roles() {
        // Alice has Admin on "acmeCo/", which transitively grants Write on "shared/".
        let (ug, rg) = make_grants(
            &[(ALICE, "acmeCo/", Admin)],
            &[("acmeCo/", "shared/", Write)],
        );

        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None, None);
        assert_eq!(result, vec!["acmeCo/", "shared/"]);

        // Admin threshold excludes the transitive Write grant.
        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, None);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn omits_child_prefixes_covered_by_parent() {
        // Alice has Admin on "acmeCo/" and a direct grant on "acmeCo/data/".
        // The child prefix should be pruned.
        let (ug, rg) = make_grants(
            &[(ALICE, "acmeCo/", Admin), (ALICE, "acmeCo/data/", Admin)],
            &[],
        );

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, None);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn omits_transitive_child_prefixes() {
        // Alice has Admin on "acmeCo/", which grants Write on "acmeCo/team/".
        // "acmeCo/team/" is already covered by "acmeCo/" so it should be pruned.
        let (ug, rg) = make_grants(
            &[(ALICE, "acmeCo/", Admin)],
            &[("acmeCo/", "acmeCo/team/", Write)],
        );

        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None, None);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn different_user_sees_nothing() {
        let bob = uuid::Uuid::from_bytes([0x22; 16]);
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, bob, Read, None, None);
        assert!(result.is_empty());
    }

    #[test]
    fn tenant_filter_narrows_to_tenant_scope() {
        // Alice administers two unrelated tenants; filtering by acmeCo/ drops
        // bobCo/ even though she can manage it.
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin), (ALICE, "bobCo/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, Some("acmeCo/"));
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn tenant_filter_cannot_widen_access() {
        // Alice administers only bobCo/. Filtering by the acmeCo/ tenant — whose
        // scope she has no access to — yields nothing rather than leaking it.
        let (ug, rg) = make_grants(&[(ALICE, "bobCo/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, Some("acmeCo/"));
        assert!(result.is_empty());
    }

    #[test]
    fn tenant_filter_keeps_shared_reach() {
        // Both Alice and the acmeCo/ tenant reach sharedCo/ (she via her own
        // admin chain, the tenant via its role grant), so it survives the
        // tenant filter alongside acmeCo/.
        let (ug, rg) = make_grants(
            &[(ALICE, "acmeCo/", Admin)],
            &[("acmeCo/", "sharedCo/", Admin)],
        );

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, Some("acmeCo/"));
        assert_eq!(result, vec!["acmeCo/", "sharedCo/"]);
    }

    #[test]
    fn tenant_filter_clamps_broad_grant_to_tenant_reach() {
        // Alice has direct grants on acmeCo/ and betaCo/, and the acmeCo/
        // tenant reaches betaCo/nested/ through a role grant. Alice's reach
        // {acmeCo/, betaCo/, betaCo/nested/} prunes to {acmeCo/, betaCo/};
        // the acmeCo/ tenant scope is {acmeCo/, betaCo/nested/}.
        //
        // Her broad betaCo/ grant is clamped to the tenant's narrower
        // betaCo/nested/: the listing stays within the tenant's reach even
        // though Alice could see all of betaCo/ through her own grant.
        let (ug, rg) = make_grants(
            &[(ALICE, "acmeCo/", Admin), (ALICE, "betaCo/", Admin)],
            &[("acmeCo/", "betaCo/nested/", Admin)],
        );

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, Some("acmeCo/"));
        assert_eq!(result, vec!["acmeCo/", "betaCo/nested/"]);
    }

    #[test]
    fn same_prefix_grants_union_capabilities() {
        use models::authz::CapabilityBundle;

        // Two user grants at the same prefix carrying disjoint
        // bundles (Editor and TeamAdmin share no bits). The
        // per-prefix CapabilitySet observed via reachable_prefixes
        // is the union of the two bundles' bits.
        let ug = tables::UserGrants::from_iter(vec![
            tables::UserGrant {
                user_id: ALICE,
                object_role: models::Prefix::new("acmeCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::Editor],
            },
            tables::UserGrant {
                user_id: ALICE,
                object_role: models::Prefix::new("acmeCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::TeamAdmin],
            },
        ]);
        let rg = tables::RoleGrants::new();

        let reachable = tables::UserGrant::reachable_prefixes(&rg, &ug, ALICE);
        assert_eq!(
            reachable["acmeCo/"].0,
            CapabilityBundle::Editor.capabilities() | CapabilityBundle::TeamAdmin.capabilities(),
        );
    }

    #[test]
    fn multi_path_role_grants_union_at_destination() {
        use models::authz::CapabilityBundle;

        // Alice is admin on acmeCo/. Two role grants reach
        // sharedCo/ from acmeCo/ carrying disjoint bundles (Editor
        // and TeamAdmin share no bits). At sharedCo/, the BFS
        // emits a NodeRef per role grant, and reachable_prefixes
        // unions their bits into a single per-prefix CapabilitySet.
        let ug = tables::UserGrants::from_iter(vec![tables::UserGrant {
            user_id: ALICE,
            object_role: models::Prefix::new("acmeCo/"),
            capability: models::Capability::Admin,
            bundles: vec![],
        }]);
        let rg = tables::RoleGrants::from_iter(vec![
            tables::RoleGrant {
                subject_role: models::Prefix::new("acmeCo/"),
                object_role: models::Prefix::new("sharedCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::Editor],
            },
            tables::RoleGrant {
                subject_role: models::Prefix::new("acmeCo/"),
                object_role: models::Prefix::new("sharedCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::TeamAdmin],
            },
        ]);

        let reachable = tables::UserGrant::reachable_prefixes(&rg, &ug, ALICE);
        assert_eq!(
            reachable["sharedCo/"].0,
            CapabilityBundle::Editor.capabilities() | CapabilityBundle::TeamAdmin.capabilities(),
        );
    }

    #[test]
    fn same_prefix_union_does_not_synthesize_ancestor_bits() {
        use models::authz::CapabilityBundle;

        // Regression guard: the union is per-exact-prefix, not across
        // ancestors. Admin on acmeCo/ does NOT make acmeCo/data/ appear in
        // a min=Admin query; acmeCo/data/'s own grant is only Writer.
        let ug = tables::UserGrants::from_iter(vec![
            tables::UserGrant {
                user_id: ALICE,
                object_role: models::Prefix::new("acmeCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::Admin],
            },
            tables::UserGrant {
                user_id: ALICE,
                object_role: models::Prefix::new("acmeCo/data/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::Writer],
            },
        ]);
        let rg = tables::RoleGrants::new();

        // min=Admin: parent acmeCo/ qualifies; acmeCo/data/ is pruned as a
        // child of the qualifying parent. If the union were across
        // ancestors, acmeCo/data/ would qualify on its own (Writer +
        // inherited Admin bits) — it does not.
        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None, None);
        assert_eq!(result, vec!["acmeCo/"]);

        // min=Write: both qualify on their own bits; parent prunes child.
        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None, None);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn tenant_prefixes_read_includes_self_and_all_granted() {
        // At the Read floor every reached grant qualifies. The self-grant
        // (acmeCo/ -> acmeCo/) collapses into the always-present tenant seed.
        let (_ug, rg) = make_grants(
            &[],
            &[
                ("acmeCo/", "acmeCo/", Admin),
                ("acmeCo/", "shared/data/", Read),
                ("acmeCo/", "ops/acmeCo/", Read),
            ],
        );

        let result = tenant_prefixes(&rg, "acmeCo/", Read);
        assert_eq!(result, vec!["acmeCo/", "ops/acmeCo/", "shared/data/"]);
    }

    #[test]
    fn tenant_prefixes_admin_excludes_lower_capability_grants() {
        // At Admin only the admin grant qualifies; the read and write grants
        // drop out. The tenant's own prefix is always in scope regardless.
        let (_ug, rg) = make_grants(
            &[],
            &[
                ("acmeCo/", "teamA/", Admin),
                ("acmeCo/", "shared/data/", Read),
                ("acmeCo/", "widgets/", Write),
            ],
        );

        let result = tenant_prefixes(&rg, "acmeCo/", Admin);
        assert_eq!(result, vec!["acmeCo/", "teamA/"]);
    }

    #[test]
    fn tenant_prefixes_admin_reaches_transitively_through_admin() {
        // An admin chain stays admin-capable at each hop, so an Admin filter
        // still reaches the end of the chain.
        let (_ug, rg) = make_grants(
            &[],
            &[("acmeCo/", "teamA/", Admin), ("teamA/", "sub/", Admin)],
        );

        let result = tenant_prefixes(&rg, "acmeCo/", Admin);
        assert_eq!(result, vec!["acmeCo/", "sub/", "teamA/"]);
    }

    #[test]
    fn tenant_prefixes_filters_on_fine_grained_capability() {
        use models::authz::Capability::ManageServiceAccounts;

        // The production filter is a fine-grained bit, not a legacy capability.
        // ManageServiceAccounts rides in the admin (TeamAdmin) bundle, so an
        // admin grant confers it while a read grant does not.
        let (_ug, rg) = make_grants(
            &[],
            &[("acmeCo/", "teamA/", Admin), ("acmeCo/", "readonly/", Read)],
        );

        let result = tenant_prefixes(&rg, "acmeCo/", ManageServiceAccounts);
        assert_eq!(result, vec!["acmeCo/", "teamA/"]);
    }

    #[test]
    fn tenant_prefixes_excludes_other_subjects() {
        // Only prefixes reachable FROM the tenant count; a grant from another
        // tenant TO acmeCo/ does not widen acmeCo/'s scope.
        let (_ug, rg) = make_grants(
            &[],
            &[
                ("acmeCo/", "shared/", Read),
                ("otherCo/", "acmeCo/", Admin),
                ("otherCo/", "secret/", Admin),
            ],
        );

        let result = tenant_prefixes(&rg, "acmeCo/", Read);
        assert_eq!(result, vec!["acmeCo/", "shared/"]);
    }

    #[test]
    fn tenant_prefixes_bare_tenant_returns_only_self() {
        let (_ug, rg) = make_grants(&[], &[("otherCo/", "x/", Admin)]);

        let result = tenant_prefixes(&rg, "acmeCo/", Read);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn tenant_prefixes_follow_admin_chains_but_stop_at_leaf_grants() {
        // acmeCo/ admins teamA/, which in turn reads shared/. Admin delegates,
        // so the chain extends and shared/ lands in scope at the Read floor.
        // acmeCo/'s read grant on readonly/ is a leaf: a read grant doesn't
        // delegate, so readonly/'s onward grant to deep/ is not followed.
        let (_ug, rg) = make_grants(
            &[],
            &[
                ("acmeCo/", "teamA/", Admin),
                ("teamA/", "shared/", Read),
                ("acmeCo/", "readonly/", Read),
                ("readonly/", "deep/", Read),
            ],
        );

        let result = tenant_prefixes(&rg, "acmeCo/", Read);
        assert_eq!(result, vec!["acmeCo/", "readonly/", "shared/", "teamA/"]);
    }
}
