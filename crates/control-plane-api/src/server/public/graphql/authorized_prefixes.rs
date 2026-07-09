/// Returns catalog prefixes where the authenticated user has at least
/// `min_capability`, optionally narrowed to those overlapping `prefix_filter`.
///
/// Intended for use by GraphQL queries that list resources scoped to the
/// caller's authorized prefixes, with an optional prefix filter.
///
/// When `prefix_filter` is provided, a prefix is included if the filter is a
/// sub-prefix of the grant OR the grant is a sub-prefix of the filter. This
/// bidirectional check lets callers query with a filter that is either broader
/// or narrower than their grants.
pub(super) fn authorized_prefixes(
    role_grants: &tables::RoleGrants,
    user_grants: &tables::UserGrants,
    user_id: uuid::Uuid,
    min_capability: impl Into<models::authz::CapabilitySet>,
    prefix_filter: Option<&str>,
) -> Vec<String> {
    let min_bits: models::authz::CapabilitySet = min_capability.into();

    // BTreeMap iteration from reachable_prefixes is already prefix-sorted,
    // so the parent-prune step below can run directly on it.
    let prefixes = tables::UserGrant::reachable_prefixes(role_grants, user_grants, user_id)
        .into_iter()
        .filter(|(prefix, _)| {
            prefix_filter.is_none_or(|pf| prefix.starts_with(pf) || pf.starts_with(*prefix))
        })
        .filter(|(_, (bits, _))| bits.is_superset(min_bits))
        .map(|(prefix, _)| prefix.to_string());

    let mut pruned: Vec<String> = Vec::new();
    for p in prefixes {
        if pruned
            .last()
            .is_none_or(|parent| !p.starts_with(parent.as_str()))
        {
            pruned.push(p);
        }
    }

    pruned
}

#[cfg(test)]
mod tests {
    use super::authorized_prefixes;
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

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None);
        assert_eq!(result, vec!["acmeCo/"]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None);
        assert_eq!(result, vec!["acmeCo/", "widgets/"]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Read, None);
        assert_eq!(result, vec!["acmeCo/", "readonly/", "widgets/"]);
    }

    #[test]
    fn filter_narrower_than_grant() {
        // Grant is on "acmeCo/", filter is "acmeCo/data/" — the grant covers
        // the filter, so "acmeCo/" is included.
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, Some("acmeCo/data/"));
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn filter_broader_than_grant() {
        // Grant is on "acmeCo/data/", filter is "acmeCo/" — the grant starts
        // with the filter, so "acmeCo/data/" is included.
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/data/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, Some("acmeCo/"));
        assert_eq!(result, vec!["acmeCo/data/"]);
    }

    #[test]
    fn filter_excludes_non_overlapping() {
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin), (ALICE, "other/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, Some("acmeCo/"));
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn no_grants_returns_empty() {
        let (ug, rg) = make_grants(&[], &[]);

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None);
        assert!(result.is_empty());
    }

    #[test]
    fn includes_transitive_roles() {
        // Alice has Admin on "acmeCo/", which transitively grants Write on "shared/".
        let (ug, rg) = make_grants(
            &[(ALICE, "acmeCo/", Admin)],
            &[("acmeCo/", "shared/", Write)],
        );

        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None);
        assert_eq!(result, vec!["acmeCo/", "shared/"]);

        // Admin threshold excludes the transitive Write grant.
        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None);
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

        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None);
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

        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None);
        assert_eq!(result, vec!["acmeCo/"]);
    }

    #[test]
    fn different_user_sees_nothing() {
        let bob = uuid::Uuid::from_bytes([0x22; 16]);
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, bob, Read, None);
        assert!(result.is_empty());
    }

    #[test]
    fn same_prefix_grants_union_capabilities() {
        use models::authz::CapabilityBundle;

        // Two user grants at the same prefix carrying disjoint
        // bundles (Edit and ManageUsers share no bits). The
        // per-prefix CapabilitySet observed via reachable_prefixes
        // is the union of the two bundles' bits.
        let ug = tables::UserGrants::from_iter(vec![
            tables::UserGrant {
                user_id: ALICE,
                object_role: models::Prefix::new("acmeCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::Edit],
            },
            tables::UserGrant {
                user_id: ALICE,
                object_role: models::Prefix::new("acmeCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::ManageUsers],
            },
        ]);
        let rg = tables::RoleGrants::new();

        let reachable = tables::UserGrant::reachable_prefixes(&rg, &ug, ALICE);
        assert_eq!(
            reachable["acmeCo/"].0,
            CapabilityBundle::Edit.capabilities() | CapabilityBundle::ManageUsers.capabilities(),
        );
    }

    #[test]
    fn multi_path_role_grants_union_at_destination() {
        use models::authz::CapabilityBundle;

        // Alice is admin on acmeCo/. Two role grants reach
        // sharedCo/ from acmeCo/ carrying disjoint bundles (Edit
        // and ManageUsers share no bits). At sharedCo/, the BFS
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
                bundles: vec![CapabilityBundle::Edit],
            },
            tables::RoleGrant {
                subject_role: models::Prefix::new("acmeCo/"),
                object_role: models::Prefix::new("sharedCo/"),
                capability: models::Capability::None,
                bundles: vec![CapabilityBundle::ManageUsers],
            },
        ]);

        let reachable = tables::UserGrant::reachable_prefixes(&rg, &ug, ALICE);
        assert_eq!(
            reachable["sharedCo/"].0,
            CapabilityBundle::Edit.capabilities() | CapabilityBundle::ManageUsers.capabilities(),
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
                bundles: vec![CapabilityBundle::Write],
            },
        ]);
        let rg = tables::RoleGrants::new();

        // min=Admin: parent acmeCo/ qualifies; acmeCo/data/ is pruned as a
        // child of the qualifying parent. If the union were across
        // ancestors, acmeCo/data/ would qualify on its own (Writer +
        // inherited Admin bits) — it does not.
        let result = authorized_prefixes(&rg, &ug, ALICE, Admin, None);
        assert_eq!(result, vec!["acmeCo/"]);

        // min=Write: both qualify on their own bits; parent prunes child.
        let result = authorized_prefixes(&rg, &ug, ALICE, Write, None);
        assert_eq!(result, vec!["acmeCo/"]);
    }
}
