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
    min_capability: models::Capability,
    prefix_filter: Option<&str>,
) -> Vec<String> {
    tables::UserGrant::transitive_roles(role_grants, user_grants, user_id)
        .filter(|grant| grant.capability >= min_capability)
        .filter(|grant| {
            prefix_filter.is_none_or(|pf| {
                grant.object_role.starts_with(pf) || pf.starts_with(&*grant.object_role)
            })
        })
        .map(|grant| grant.object_role.to_string())
        .collect()
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
            }
        }));
        let rg = tables::RoleGrants::from_iter(role_grants.iter().map(|(sub, obj, cap)| {
            tables::RoleGrant {
                subject_role: models::Prefix::new(*sub),
                object_role: models::Prefix::new(*obj),
                capability: *cap,
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
    fn different_user_sees_nothing() {
        let bob = uuid::Uuid::from_bytes([0x22; 16]);
        let (ug, rg) = make_grants(&[(ALICE, "acmeCo/", Admin)], &[]);

        let result = authorized_prefixes(&rg, &ug, bob, Read, None);
        assert!(result.is_empty());
    }
}
