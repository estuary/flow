use enumset::EnumSet;
use models::authz;
use superslice::Ext;
use url::Url;

impl super::Resource {
    pub fn fetch<'s>(resources: &'s [Self], url: &Url) -> Option<&'s Self> {
        let range = resources.equal_range_by_key(&url, |resource| &resource.resource);
        resources[range].iter().next()
    }

    pub fn upsert_if_changed(self, resources: &mut super::Resources) {
        let index = resources.binary_search_by(|l| l.resource.cmp(&self.resource));

        match index {
            Ok(index) if self.content_dom.get() != resources[index].content_dom.get() => {
                resources[index] = self;
            }
            Ok(_) => {
                // If DOM isn't changing then don't overwrite the on-disk serialization.
                // This preserves YAML comments or anchors the user may have.
            }
            Err(_) => {
                resources.insert(self);
            }
        }
    }
}

impl super::Import {
    // transitive_imports returns an iterator over the resources that `src`
    // directly or indirectly imports. `src` may have a fragment location,
    // and all imports from scopes which are prefixed by `src` are considered.
    // In other words, if `src` has a fragment location then only imports at
    // or below that location are traversed.
    //
    // `src` itself is not included in the iterator output.
    pub fn transitive_imports<'a>(
        imports: &'a [Self],
        src: &'a Url,
    ) -> impl Iterator<Item = &'a Url> + 'a {
        let edges = move |from: &Url| {
            let range = imports.equal_range_by(|import| {
                if import.scope.as_str().starts_with(from.as_str()) {
                    std::cmp::Ordering::Equal
                } else {
                    import.scope.cmp(from)
                }
            });
            imports[range].iter().map(|import| &import.to_resource)
        };
        pathfinding::directed::bfs::bfs_reach(src, move |f| edges(f)).skip(1)
    }
}

fn effective_bits(
    legacy: models::Capability,
    bundles: &[authz::CapabilityBundle],
) -> authz::CapabilitySet {
    let mut bits = authz::bits_for_legacy(legacy);
    for b in bundles {
        bits |= b.capabilities();
    }
    bits
}

/// True when bits accumulated across `nodes` at prefixes covering
/// `object_role_or_name` satisfy `required`. Bits compose additively
/// across paths: distinct grant paths that each contribute partial bits
/// at covering prefixes can jointly authorize a request that no single
/// path would on its own.
fn any_path_satisfies<'a>(
    nodes: impl IntoIterator<Item = super::NodeRef<'a>>,
    object_role_or_name: &str,
    required: impl Into<authz::CapabilitySet>,
) -> bool {
    let mut remaining = required.into();
    for node in nodes {
        if object_role_or_name.starts_with(node.object_role) {
            remaining -= node.capabilities;
            if remaining.is_empty() {
                return true;
            }
        }
    }
    false
}

/// Narrow one node of a user's walk against the nodes reachable from a scope
/// prefix, yielding the part of `node` which falls inside the scope.
///
/// Prefix sets are compared by containment, not equality, because a node at
/// `acmeCo/` covers every name beneath it. So a user node and a scope node
/// overlap whenever either prefix contains the other, and the overlap is the
/// more specific of the two:
///
/// * user `acmeCo/` with scope `acmeCo/team/` overlaps at `acmeCo/team/`
/// * user `acmeCo/team/` with scope `acmeCo/` overlaps at `acmeCo/team/`
/// * user `acmeCo/` with scope `betaCo/` does not overlap
///
/// Capability is the intersection of both sides. A prefix the scope reaches
/// only by delegation contributes only what was delegated, so scoping to
/// `acmeCo/` grants `charlieCo/` exactly the capability that `acmeCo/`'s role
/// grant conveys — even where the user separately holds more on `charlieCo/`
/// through a grant outside the scope.
///
/// Because every emitted node is bounded by a node of the user's own walk,
/// the scoped set is always a subset of the unscoped set. A scope cannot add
/// authority regardless of the value supplied.
fn narrow_to_scope<'a, 's>(
    node: super::NodeRef<'a>,
    scope_nodes: &'s [super::NodeRef<'a>],
) -> impl Iterator<Item = super::NodeRef<'a>> + use<'a, 's> {
    scope_nodes.iter().filter_map(move |scope_node| {
        let object_role = if node.object_role.starts_with(scope_node.object_role) {
            node.object_role
        } else if scope_node.object_role.starts_with(node.object_role) {
            scope_node.object_role
        } else {
            return None;
        };

        Some(super::NodeRef {
            object_role,
            capabilities: node.capabilities & scope_node.capabilities,
            legacy: std::cmp::min(node.legacy, scope_node.legacy),
        })
    })
}

impl super::RoleGrant {
    pub fn reachable_nodes<'a>(
        role_grants: &'a [super::RoleGrant],
        role_or_name: &'a str,
    ) -> impl Iterator<Item = super::NodeRef<'a>> + 'a {
        let seed = super::NodeRef {
            object_role: role_or_name,
            capabilities: EnumSet::from(authz::Capability::Assume),
            legacy: models::Capability::None,
        };
        pathfinding::directed::bfs::bfs_reach(seed, move |f| {
            next_neighbors(f.clone(), role_grants, &[], uuid::Uuid::nil())
        })
        .skip(1)
    }

    /// Nodes reachable from `scope`, including `scope` itself.
    ///
    /// This differs from `reachable_nodes` in two ways, both because the
    /// output is used to narrow another node set rather than to authorize on
    /// its own:
    ///
    /// * The seed prefix is included. A request scoped to `acmeCo/` must still
    ///   reach `acmeCo/`, not only what `acmeCo/` delegates to.
    /// * The seed carries every capability bit and `Admin`, so intersecting
    ///   with it filters by prefix without also removing capability that the
    ///   user holds at the seed prefix. Bits reached through role grants are
    ///   still whatever those grants convey, so a delegated prefix contributes
    ///   only the capability it was delegated.
    pub fn scope_nodes<'a>(
        role_grants: &'a [super::RoleGrant],
        scope: &'a str,
    ) -> impl Iterator<Item = super::NodeRef<'a>> + 'a {
        let seed = super::NodeRef {
            object_role: scope,
            capabilities: EnumSet::all(),
            legacy: models::Capability::Admin,
        };
        pathfinding::directed::bfs::bfs_reach(seed, move |f| {
            next_neighbors(f.clone(), role_grants, &[], uuid::Uuid::nil())
        })
    }

    pub fn is_authorized<'a>(
        role_grants: &'a [super::RoleGrant],
        subject_role_or_name: &'a str,
        object_role_or_name: &'a str,
        capability: impl Into<authz::CapabilitySet>,
    ) -> bool {
        any_path_satisfies(
            Self::reachable_nodes(role_grants, subject_role_or_name),
            object_role_or_name,
            capability,
        )
    }

    fn to_node_ref<'a>(&'a self, delegatable: authz::CapabilitySet) -> super::NodeRef<'a> {
        super::NodeRef {
            object_role: self.object_role.as_str(),
            capabilities: effective_bits(self.capability, &self.bundles) & delegatable,
            legacy: self.capability,
        }
    }
}

impl super::UserGrant {
    /// Nodes reachable by `principal`.
    ///
    /// Without a scope this is every node reachable from the user's grants.
    /// With one, each reachable node is narrowed against the nodes reachable
    /// from the scope prefix: see `narrow_to_scope`.
    pub fn reachable_nodes<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        principal: super::Principal<'a>,
    ) -> impl Iterator<Item = super::NodeRef<'a>> + 'a {
        let seed = super::NodeRef {
            object_role: "",
            capabilities: EnumSet::from(authz::Capability::Assume),
            legacy: models::Capability::None,
        };
        let unscoped = pathfinding::directed::bfs::bfs_reach(seed, move |f| {
            next_neighbors(f.clone(), role_grants, user_grants, principal.user_id)
        })
        .skip(1);

        let Some(scope) = principal.scope else {
            return itertools::Either::Left(unscoped);
        };

        // Both walks are bounded by the grant graph and hold tens of prefixes
        // in practice, so the scoped set is materialized in one pass. The
        // unscoped branch above stays lazy, which is what `is_authorized`
        // relies on to stop at the first node that satisfies it.
        let scope_nodes: Vec<super::NodeRef<'a>> =
            super::RoleGrant::scope_nodes(role_grants, scope).collect();
        let scoped: Vec<super::NodeRef<'a>> = unscoped
            .flat_map(|node| narrow_to_scope(node, &scope_nodes).collect::<Vec<_>>())
            .collect();

        itertools::Either::Right(scoped.into_iter())
    }

    /// Returns each prefix reachable by `principal` mapped to the union
    /// of capability bits granted at that prefix across every path
    /// through the grant graph, paired with the max legacy `capability`
    /// column value among grants directly emitting that prefix.
    ///
    /// Bits compose additively (multi-path union); the legacy column is
    /// a literal pass-through from storage, max'd across same-prefix
    /// arrivals. Applying a min-capability filter to the bit set agrees
    /// with `is_authorized` on the same inputs.
    pub fn reachable_prefixes<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        principal: super::Principal<'a>,
    ) -> std::collections::BTreeMap<&'a str, (authz::CapabilitySet, models::Capability)> {
        let mut out: std::collections::BTreeMap<
            &'a str,
            (authz::CapabilitySet, models::Capability),
        > = Default::default();
        for node in Self::reachable_nodes(role_grants, user_grants, principal) {
            let entry = out
                .entry(node.object_role)
                .or_insert((authz::CapabilitySet::empty(), models::Capability::None));
            entry.0 |= node.capabilities;
            if node.legacy > entry.1 {
                entry.1 = node.legacy;
            }
        }
        out
    }

    pub fn get_user_capability<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        principal: super::Principal<'a>,
        object_role_or_name: &str,
    ) -> Option<models::Capability> {
        Self::reachable_nodes(role_grants, user_grants, principal)
            .filter(|n| object_role_or_name.starts_with(n.object_role))
            .map(|n| n.legacy)
            .filter(|c| *c != models::Capability::None)
            .max()
    }

    pub fn is_authorized<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        principal: super::Principal<'a>,
        object_role_or_name: &'a str,
        capability: impl Into<authz::CapabilitySet>,
    ) -> bool {
        any_path_satisfies(
            Self::reachable_nodes(role_grants, user_grants, principal),
            object_role_or_name,
            capability,
        )
    }

    fn to_node_ref<'a>(&'a self, delegatable: authz::CapabilitySet) -> super::NodeRef<'a> {
        super::NodeRef {
            object_role: self.object_role.as_str(),
            capabilities: effective_bits(self.capability, &self.bundles) & delegatable,
            legacy: self.capability,
        }
    }
}

// Expand a BFS node into its neighbors. A node is terminal (no expansion)
// unless it carries Delegate or Assume. Delegate passes only the node's own
// capabilities through to neighbors (the child receives `edge_bits & parent_bits`);
// Assume passes all capabilities through unfiltered, modeling identity takeover.
//
// Perf note: bfs_reach keys on the whole NodeRef, so the same object_role
// reached with different capability subsets produces distinct BFS nodes —
// up to 2^N per prefix where N is the number of capability bits. If deep
// grant graphs cause latency, replace bfs_reach with a manual BFS that keys
// visited state on object_role alone and prunes dominated capability subsets.
fn next_neighbors<'a>(
    from: super::NodeRef<'a>,
    role_edges: &'a [super::RoleGrant],
    user_edges: &'a [super::UserGrant],
    user_id: uuid::Uuid,
) -> impl Iterator<Item = super::NodeRef<'a>> + 'a {
    let has_delegate = from.capabilities.contains(authz::Capability::Delegate);
    let has_assume = from.capabilities.contains(authz::Capability::Assume);
    let is_terminal = !has_delegate && !has_assume;
    let delegatable = if has_assume {
        EnumSet::all()
    } else {
        from.capabilities
    };

    let (user_edges, role_edges, prefixes) = match (is_terminal, from.object_role) {
        // Terminal node: no Delegate/Assume bit means no further expansion.
        (true, _) => (&user_edges[..0], &role_edges[..0], None),
        // Seed step: an empty object_role kicks off exploration through
        // `user_grants` for `user_id`. This branch is only reached from
        // the `UserGrant::reachable_nodes` seed.
        (_, "") => {
            let range = user_edges.equal_range_by(|user_grant| user_grant.user_id.cmp(&user_id));
            (&user_edges[range], &role_edges[..0], None)
        }
        // We've delegated authority at `role_or_name`, and are projecting
        // through role_grants to identify other roles and capabilities we
        // take on.
        (_, role_or_name) => {
            // Expand to all roles having a subject_role prefixed by role_or_name.
            // In other words, a delegate of `acmeCo/org/` may use a role with
            // subject `acmeCo/org/team/`. Intuitively, this is because the
            // delegate is authorized to act anywhere under `acmeCo/org/`,
            // which includes any name under `acmeCo/org/team/`.
            let range = role_edges.equal_range_by(|role_grant| {
                if role_grant.subject_role.starts_with(role_or_name) {
                    std::cmp::Ordering::Equal
                } else {
                    role_grant.subject_role.as_str().cmp(role_or_name)
                }
            });
            // Expand to all roles having a subject_role which prefixes role_or_name.
            // In other words, a task `acmeCo/org/task` or delegate of `acmeCo/org/`
            // may use a role with subject `acmeCo/`. Intuitively, this is because
            // the role granted to `acmeCo/` is also granted to any name underneath
            // `acmeCo/`, which includes the present role or name.
            //
            // First split the source role into its prefixes:
            // "acmeCo/one/two/three" => ["acmeCo/one/two/", "acmeCo/one/", "acmeCo/"].
            let prefixes = role_or_name.char_indices().filter_map(|(ind, chr)| {
                if chr == '/' {
                    Some(&role_or_name[..ind + 1])
                } else {
                    None
                }
            });
            // Then for each prefix, find all role_grants where it's the exact subject_role.
            let edges = prefixes
                .map(|prefix| {
                    role_edges
                        .equal_range_by(|role_grant| role_grant.subject_role.as_str().cmp(prefix))
                })
                .map(|range| role_edges[range].into_iter())
                .flatten();

            (&user_edges[..0], &role_edges[range], Some(edges))
        }
    };

    let p1 = user_edges.iter().map(move |g| g.to_node_ref(delegatable));
    let p2 = role_edges.iter().map(move |g| g.to_node_ref(delegatable));
    let p3 = prefixes
        .into_iter()
        .flatten()
        .map(move |g| g.to_node_ref(delegatable));

    p1.chain(p2).chain(p3)
}

impl super::StorageMapping {
    pub fn scope(&self) -> url::Url {
        crate::synthetic_scope("storageMapping", &self.catalog_prefix)
    }
}

#[cfg(test)]
mod test {
    use crate::{Import, Imports, Principal, RoleGrant, RoleGrants, UserGrant, UserGrants};
    use enumset::EnumSet;
    use models::authz::{Capability, CapabilityBundle};

    #[test]
    fn test_transitive_imports() {
        let u = |s: &str| -> url::Url { url::Url::parse(s).unwrap() };

        let mut tbl = Imports::new();
        tbl.insert_row(u("https://example/root#/foo/one/a"), u("https://A"));
        tbl.insert_row(u("https://example/root#/foo/one/b/extra"), u("https://B"));
        tbl.insert_row(u("https://example/root#/foo/two/c"), u("https://C"));
        tbl.insert_row(u("https://A"), u("https://Z"));
        tbl.insert_row(u("https://B"), u("https://Z"));

        for case in [u("https://example/root"), u("https://example/root#/foo")] {
            assert_eq!(
                Import::transitive_imports(&tbl, &case).collect::<Vec<_>>(),
                vec![
                    &u("https://A"),
                    &u("https://B"),
                    &u("https://C"),
                    &u("https://Z"),
                ],
            );
        }
        assert_eq!(
            Import::transitive_imports(&tbl, &u("https://example/root#/foo/one/b"))
                .collect::<Vec<_>>(),
            vec![&u("https://B"), &u("https://Z")],
        );
        assert!(
            Import::transitive_imports(&tbl, &u("https://example/root#/foo/not/found"))
                .collect::<Vec<_>>()
                .is_empty()
        );
    }

    #[test]
    fn test_legacy_admin_grants_propagate() {
        let role_grants = RoleGrants::from_iter(
            [
                (
                    "aliceCo/widgets/",
                    "bobCo/burgers/",
                    models::Capability::Admin,
                ),
                (
                    "aliceCo/anvils/",
                    "carolCo/paper/",
                    models::Capability::Write,
                ),
                (
                    "aliceCo/duplicate/",
                    "carolCo/paper/",
                    models::Capability::Read,
                ),
                (
                    "aliceCo/stuff/",
                    "carolCo/shared/",
                    models::Capability::Read,
                ),
                (
                    "bobCo/alice-vendor/",
                    "aliceCo/bob-shared/",
                    models::Capability::Admin,
                ),
                (
                    "carolCo/shared/",
                    "carolCo/hidden/",
                    models::Capability::Read,
                ),
                (
                    "daveCo/hidden/",
                    "carolCo/hidden/",
                    models::Capability::Admin,
                ),
                (
                    "carolCo/hidden/",
                    "carolCo/even/more/hidden/",
                    models::Capability::Read,
                ),
            ]
            .into_iter()
            .map(|(sub, obj, capability)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            }),
        );
        let user_grants = UserGrants::from_iter(
            [
                (uuid::Uuid::nil(), "bobCo/", models::Capability::Read),
                (uuid::Uuid::nil(), "daveCo/", models::Capability::Admin),
                (
                    uuid::Uuid::max(),
                    "aliceCo/widgets/",
                    models::Capability::Admin,
                ),
                (
                    uuid::Uuid::max(),
                    "carolCo/shared/",
                    models::Capability::Admin,
                ),
            ]
            .into_iter()
            .map(|(user_id, obj, capability)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            }),
        );

        // Admin on daveCo/hidden/ reaches carolCo/hidden/ (admin) and
        // carolCo/even/more/hidden/ (read via viewer bits).
        assert!(RoleGrant::is_authorized(
            &role_grants,
            "daveCo/hidden/thing",
            "carolCo/hidden/thing",
            models::Capability::Write
        ));
        assert!(RoleGrant::is_authorized(
            &role_grants,
            "daveCo/hidden/",
            "carolCo/even/more/hidden/",
            models::Capability::Read
        ));
        assert!(!RoleGrant::is_authorized(
            &role_grants,
            "daveCo/hidden/thing",
            "carolCo/even/more/hidden/",
            models::Capability::Write
        ));

        // User nil: read on bobCo/ (terminal), admin on daveCo/ (propagates).
        assert!(UserGrant::is_authorized(
            &role_grants,
            &user_grants,
            Principal::unscoped(uuid::Uuid::nil()),
            "bobCo/thing",
            models::Capability::Read,
        ));
        assert!(!UserGrant::is_authorized(
            &role_grants,
            &user_grants,
            Principal::unscoped(uuid::Uuid::nil()),
            "bobCo/thing",
            models::Capability::Write,
        ));
        assert!(UserGrant::is_authorized(
            &role_grants,
            &user_grants,
            Principal::unscoped(uuid::Uuid::nil()),
            "carolCo/hidden/thing",
            models::Capability::Read,
        ));

        // User max: admin on aliceCo/widgets/ (propagates to bobCo/burgers/).
        assert!(UserGrant::is_authorized(
            &role_grants,
            &user_grants,
            Principal::unscoped(uuid::Uuid::max()),
            "bobCo/burgers/thing",
            models::Capability::Admin,
        ));
    }

    #[test]
    fn test_legacy_roles_more() {
        let role_grants: Vec<crate::RoleGrant> = serde_json::from_value(serde_json::json!([
          {
            "subject_role": "acmeCo/",
            "object_role": "acmeCo/",
            "capability": "write",
            "bundles": []
          },
          {
            "subject_role": "other_tenant/",
            "object_role": "acmeCo/",
            "capability": "admin",
            "bundles": []
          },
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "acmeCo-૨/",
            "capability": "write",
            "bundles": []
          },
          {
            "subject_role": "other_tenant/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "bundles": []
          },
          {
            "subject_role": "acmeCo-૨/ssss/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "bundles": []
          },
          {
            "subject_role": "acmeCo-૨/aaaa/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "bundles": []
          },
          {
            "subject_role": "acmeCo-૨/dddd/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "bundles": []
          },
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "ops/dp/public/",
            "capability": "read",
            "bundles": []
          },
          {
            "subject_role": "acmeCo/",
            "object_role": "ops/dp/public/",
            "capability": "read",
            "bundles": []
          }
        ]))
        .unwrap();
        let role_grants = crate::RoleGrants::from_iter(role_grants);

        assert!(crate::RoleGrant::is_authorized(
            &role_grants,
            "acmeCo-૨/acme-prod-tables/materialize-snowflake",
            "acmeCo-૨/acme-data/my_data/",
            models::Capability::Read
        ));
    }

    #[test]
    fn test_get_user_capability() {
        use models::Capability::{Admin, Read, Write};
        let role_grants = RoleGrants::from_iter(
            [
                ("acmeCo/", "acmeCo/", Write),
                ("acmeCo/", "ops/private/dp/acmeCo/", Read),
            ]
            .into_iter()
            .map(|(sub, obj, capability)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            }),
        );

        let user1 = uuid::Uuid::from_bytes([1; 16]);
        let user2 = uuid::Uuid::from_bytes([2; 16]);
        let user_grants = UserGrants::from_iter(
            [
                (user1, "acmeCo/", Admin),
                (user2, "acmeCo/", Admin),
                (user2, "ops/private/dp/acmeCo/", Write),
            ]
            .into_iter()
            .map(|(user_id, obj, capability)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            }),
        );

        assert_eq!(
            Some(Read),
            UserGrant::get_user_capability(
                &role_grants,
                &user_grants,
                Principal::unscoped(user1),
                "ops/private/dp/acmeCo/foooo"
            )
        );
        assert_eq!(
            Some(Write),
            UserGrant::get_user_capability(
                &role_grants,
                &user_grants,
                Principal::unscoped(user2),
                "ops/private/dp/acmeCo/foooo"
            )
        );
        assert_eq!(
            None,
            UserGrant::get_user_capability(
                &role_grants,
                &user_grants,
                Principal::unscoped(user1),
                "different/co/altogether"
            )
        );
    }

    #[test]
    fn test_data_plane_user_visibility() {
        use models::Capability::*;

        let role_grants = RoleGrants::from_iter(
            [
                ("acmeCo/", "acmeCo/", Write),
                ("acmeCo/", "ops/private/dp/acmeCo/", Read),
            ]
            .into_iter()
            .map(|(sub, obj, capability)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            }),
        );
        let user_grants = UserGrants::from_iter(
            [
                (uuid::Uuid::from_bytes([1; 16]), "acmeCo/", Admin),
                (uuid::Uuid::from_bytes([2; 16]), "acmeCo/nested/", Admin),
            ]
            .into_iter()
            .map(|(user_id, obj, capability)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            }),
        );

        // User 1 has admin on acmeCo/, which propagates through role grants.
        assert!(UserGrant::is_authorized(
            &role_grants,
            &user_grants,
            Principal::unscoped(uuid::Uuid::from_bytes([1; 16])),
            "ops/private/dp/acmeCo/foo",
            models::Capability::Read,
        ));
        // User 2 has admin on acmeCo/nested/, which also picks up the
        // acmeCo/ role grants (parent prefix matching).
        assert!(UserGrant::is_authorized(
            &role_grants,
            &user_grants,
            Principal::unscoped(uuid::Uuid::from_bytes([2; 16])),
            "ops/private/dp/acmeCo/foo",
            models::Capability::Read,
        ));
    }

    /// Grant graph shared by the scope tests below.
    ///
    /// Alice admins `acmeCo/` and `betaCo/`, and `acmeCo/` holds a read grant
    /// to `charlieCo/`. She also admins `charlieCo/ops/` directly, which is
    /// how the tests distinguish authority reached through the scope from
    /// authority she happens to hold on the same prefix by another path.
    fn scope_fixture() -> (RoleGrants, UserGrants, uuid::Uuid) {
        use models::Capability::{Admin, Read};

        let alice = uuid::Uuid::from_bytes([0xa1; 16]);

        let role_grants = RoleGrants::from_iter([("acmeCo/", "charlieCo/", Read)].into_iter().map(
            |(sub, obj, capability)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            },
        ));
        let user_grants = UserGrants::from_iter(
            [
                (alice, "acmeCo/", Admin),
                (alice, "betaCo/", Admin),
                (alice, "charlieCo/ops/", Admin),
            ]
            .into_iter()
            .map(|(user_id, obj, capability)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability,
                bundles: vec![],
            }),
        );

        (role_grants, user_grants, alice)
    }

    /// Prefixes reachable by `principal`, each with its legacy capability.
    fn scoped_prefixes(
        role_grants: &RoleGrants,
        user_grants: &UserGrants,
        principal: Principal<'_>,
    ) -> Vec<(String, models::Capability)> {
        let mut out: Vec<_> = UserGrant::reachable_prefixes(role_grants, user_grants, principal)
            .into_iter()
            .filter(|(_, (bits, _))| !bits.is_empty())
            .map(|(prefix, (_, legacy))| (prefix.to_string(), legacy))
            .collect();
        out.sort();
        out
    }

    #[test]
    fn test_scope_selects_one_branch() {
        use models::Capability::{Admin, Read};
        let (rg, ug, alice) = scope_fixture();

        // Unscoped, Alice reaches everything she's been granted.
        assert_eq!(
            scoped_prefixes(&rg, &ug, Principal::unscoped(alice)),
            vec![
                ("acmeCo/".to_string(), Admin),
                ("betaCo/".to_string(), Admin),
                ("charlieCo/".to_string(), Read),
                ("charlieCo/ops/".to_string(), Admin),
            ],
        );

        // Scoped to `acmeCo/`, she reaches `acmeCo/` and what it delegates to.
        // `betaCo/` drops out because it is a sibling branch.
        assert_eq!(
            scoped_prefixes(&rg, &ug, Principal::scoped(alice, "acmeCo/")),
            vec![
                ("acmeCo/".to_string(), Admin),
                ("charlieCo/".to_string(), Read),
                ("charlieCo/ops/".to_string(), Read),
            ],
        );

        // Scoped to `betaCo/`, only `betaCo/` remains: it delegates nothing.
        assert_eq!(
            scoped_prefixes(&rg, &ug, Principal::scoped(alice, "betaCo/")),
            vec![("betaCo/".to_string(), Admin)],
        );
    }

    #[test]
    fn test_scope_narrower_than_grant_keeps_ancestor_delegations() {
        use models::Capability::{Admin, Read};
        let (rg, ug, alice) = scope_fixture();

        // The role grant to `charlieCo/` hangs off `acmeCo/`, and a grant on
        // `acmeCo/` reaches all of `acmeCo/`. So scoping to `acmeCo/team/`
        // still reaches `charlieCo/`, even though `acmeCo/team/` is not
        // itself the grant's subject.
        assert_eq!(
            scoped_prefixes(&rg, &ug, Principal::scoped(alice, "acmeCo/team/")),
            vec![
                ("acmeCo/team/".to_string(), Admin),
                ("charlieCo/".to_string(), Read),
                ("charlieCo/ops/".to_string(), Read),
            ],
        );
    }

    #[test]
    fn test_scope_caps_capability_at_what_it_delegates() {
        use models::Capability::{Admin, Read};
        let (rg, ug, alice) = scope_fixture();

        // Alice admins `charlieCo/ops/` directly, so unscoped she holds Admin.
        assert!(UserGrant::is_authorized(
            &rg,
            &ug,
            Principal::unscoped(alice),
            "charlieCo/ops/thing",
            Admin,
        ));

        // Within the `acmeCo/` scope her reach to `charlieCo/` comes from that
        // tenant's read grant, so the direct Admin grant is not in play and
        // Read is the most she holds.
        assert!(UserGrant::is_authorized(
            &rg,
            &ug,
            Principal::scoped(alice, "acmeCo/"),
            "charlieCo/ops/thing",
            Read,
        ));
        assert!(!UserGrant::is_authorized(
            &rg,
            &ug,
            Principal::scoped(alice, "acmeCo/"),
            "charlieCo/ops/thing",
            Admin,
        ));
    }

    #[test]
    fn test_scope_cannot_widen() {
        use models::Capability::Read;
        let (rg, ug, alice) = scope_fixture();

        // A scope naming a prefix Alice cannot reach yields no authority,
        // rather than the authority of that prefix.
        assert!(
            scoped_prefixes(&rg, &ug, Principal::scoped(alice, "deltaCo/")).is_empty(),
            "a scope outside the user's grants must reach nothing",
        );
        assert!(!UserGrant::is_authorized(
            &rg,
            &ug,
            Principal::scoped(alice, "deltaCo/"),
            "deltaCo/thing",
            Read,
        ));

        // Every scope, including one the user cannot reach, produces a subset
        // of the unscoped prefixes. This is the property that makes the scope
        // safe to take from an untrusted request parameter.
        let unscoped: std::collections::BTreeSet<_> =
            scoped_prefixes(&rg, &ug, Principal::unscoped(alice))
                .into_iter()
                .map(|(prefix, _)| prefix)
                .collect();

        for scope in [
            "acmeCo/",
            "acmeCo/team/",
            "betaCo/",
            "charlieCo/",
            "charlieCo/ops/",
            "deltaCo/",
            "estuary_support/",
        ] {
            for (prefix, _) in scoped_prefixes(&rg, &ug, Principal::scoped(alice, scope)) {
                assert!(
                    unscoped.iter().any(|u| prefix.starts_with(u.as_str())),
                    "scope '{scope}' reached '{prefix}', which is outside the unscoped set",
                );
            }
        }
    }

    fn build_scenario(
        user_edges: Vec<(&str, Vec<CapabilityBundle>)>,
        role_edges: Vec<(&str, &str, Vec<CapabilityBundle>)>,
    ) -> (RoleGrants, UserGrants, uuid::Uuid) {
        let user_id = uuid::Uuid::from_bytes([1; 16]);
        let user_grants =
            UserGrants::from_iter(user_edges.into_iter().map(|(obj, bundles)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability: models::Capability::None,
                bundles,
            }));
        let role_grants =
            RoleGrants::from_iter(role_edges.into_iter().map(|(sub, obj, bundles)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability: models::Capability::None,
                bundles,
            }));
        (role_grants, user_grants, user_id)
    }

    fn assert_reachable(
        role_grants: &RoleGrants,
        user_grants: &UserGrants,
        user_id: uuid::Uuid,
        expected: Vec<(&str, EnumSet<Capability>)>,
    ) {
        let mut nodes: Vec<_> =
            UserGrant::reachable_nodes(role_grants, user_grants, Principal::unscoped(user_id))
                .map(|n| (n.object_role.to_string(), n.capabilities))
                .collect();
        nodes.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.as_u32().cmp(&b.1.as_u32())));
        nodes.dedup();

        let expected: Vec<(String, EnumSet<Capability>)> = expected
            .into_iter()
            .map(|(prefix, caps)| (prefix.to_string(), caps))
            .collect();

        assert_eq!(nodes, expected);
    }

    fn assert_authorized(
        role_grants: &RoleGrants,
        user_grants: &UserGrants,
        user_id: uuid::Uuid,
        name: &str,
        required: EnumSet<Capability>,
    ) {
        assert!(
            UserGrant::is_authorized(
                role_grants,
                user_grants,
                Principal::unscoped(user_id),
                name,
                required
            ),
            "expected {user_id} to have {required:?} on {name}",
        );
    }

    fn assert_not_authorized(
        role_grants: &RoleGrants,
        user_grants: &UserGrants,
        user_id: uuid::Uuid,
        name: &str,
        required: EnumSet<Capability>,
    ) {
        assert!(
            !UserGrant::is_authorized(
                role_grants,
                user_grants,
                Principal::unscoped(user_id),
                name,
                required
            ),
            "expected {user_id} NOT to have {required:?} on {name}",
        );
    }

    #[test]
    fn test_reachable_nodes_delegate_propagation() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::Delegate,
                ],
            )],
            vec![
                (
                    "acmeCo/",
                    "bobCo/shared/",
                    vec![
                        CapabilityBundle::Viewer,
                        CapabilityBundle::Billing,
                        CapabilityBundle::Delegate,
                    ],
                ),
                (
                    "bobCo/shared/",
                    "carolCo/data/",
                    vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
                ),
                (
                    "carolCo/data/",
                    "daveCo/sink/",
                    vec![CapabilityBundle::Viewer, CapabilityBundle::Billing],
                ),
            ],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                (
                    "acmeCo/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | Delegate,
                ),
                (
                    "bobCo/shared/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | Delegate,
                ),
                (
                    "carolCo/data/",
                    CapabilityBundle::Viewer.capabilities() | Delegate,
                ),
                ("daveCo/sink/", CapabilityBundle::Viewer.capabilities()),
            ],
        );
    }

    #[test]
    fn test_reachable_nodes_no_delegate_is_terminal() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
            )],
            vec![
                ("acmeCo/", "bobCo/shared/", vec![CapabilityBundle::Viewer]),
                ("bobCo/shared/", "carolCo/", vec![CapabilityBundle::Viewer]),
            ],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                (
                    "acmeCo/",
                    CapabilityBundle::Viewer.capabilities() | Delegate,
                ),
                ("bobCo/shared/", CapabilityBundle::Viewer.capabilities()),
            ],
        );

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![("acmeCo/", vec![CapabilityBundle::Viewer])],
            vec![
                (
                    "acmeCo/",
                    "bobCo/shared/",
                    vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
                ),
                ("bobCo/shared/", "carolCo/", vec![CapabilityBundle::Viewer]),
            ],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![("acmeCo/", CapabilityBundle::Viewer.capabilities())],
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "bobCo/shared/",
            CapabilityBundle::Viewer.capabilities(),
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "carolCo/",
            CapabilityBundle::Viewer.capabilities(),
        );
    }

    #[test]
    fn test_assume_behavior() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![("acmeCo/", vec![CapabilityBundle::Assume])],
            vec![(
                "acmeCo/",
                "bobCo/shared/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::TeamAdmin,
                ],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                ("acmeCo/", EnumSet::from(Assume)),
                (
                    "bobCo/shared/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | CapabilityBundle::TeamAdmin.capabilities(),
                ),
            ],
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "bobCo/shared/nested/",
            CapabilityBundle::Viewer.capabilities() | CapabilityBundle::TeamAdmin.capabilities(),
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "acmeCo/",
            CapabilityBundle::Viewer.capabilities(),
        );

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Writer, CapabilityBundle::Assume],
            )],
            vec![(
                "acmeCo/",
                "bobCo/shared/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::TeamAdmin,
                ],
            )],
        );
        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "acmeCo/",
            CapabilityBundle::Writer.capabilities(),
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "bobCo/shared/",
            CapabilityBundle::Writer.capabilities(),
        );
    }

    #[test]
    fn test_assume_supersedes_delegate() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Delegate,
                    CapabilityBundle::Assume,
                ],
            )],
            vec![(
                "acmeCo/",
                "bobCo/shared/",
                vec![
                    CapabilityBundle::Billing,
                    CapabilityBundle::Viewer,
                    CapabilityBundle::TeamAdmin,
                ],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                (
                    "acmeCo/",
                    CapabilityBundle::Viewer.capabilities() | Assume | Delegate,
                ),
                (
                    "bobCo/shared/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | CapabilityBundle::TeamAdmin.capabilities(),
                ),
            ],
        );

        // Contrast: delegate alone attenuates to the intersection.
        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
            )],
            vec![(
                "acmeCo/",
                "bobCo/shared/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::TeamAdmin,
                ],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                (
                    "acmeCo/",
                    CapabilityBundle::Viewer.capabilities() | Delegate,
                ),
                ("bobCo/shared/", CapabilityBundle::Viewer.capabilities()),
            ],
        );

        // Assume does not add capabilities to the following edge
        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Writer, CapabilityBundle::Assume],
            )],
            vec![(
                "acmeCo/",
                "bobCo/shared/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::TeamAdmin,
                ],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                ("acmeCo/", CapabilityBundle::Writer.capabilities() | Assume),
                (
                    "bobCo/shared/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | CapabilityBundle::TeamAdmin.capabilities(),
                ),
            ],
        );
    }

    #[test]
    fn test_inherited_capabilities() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![
                ("acmeCo/", vec![CapabilityBundle::Viewer]),
                (
                    "acmeCo/interns/",
                    vec![CapabilityBundle::Writer, CapabilityBundle::Delegate],
                ),
            ],
            vec![(
                "acmeCo/",
                "betaCo/shareable/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Writer],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                ("acmeCo/", CapabilityBundle::Viewer.capabilities()),
                (
                    "acmeCo/interns/",
                    CapabilityBundle::Writer.capabilities() | Delegate,
                ),
                ("betaCo/shareable/", CapabilityBundle::Writer.capabilities()),
            ],
        );
    }

    #[test]
    fn test_descendent_capabilities() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![
                ("acmeCo/", vec![CapabilityBundle::Viewer]),
                (
                    "acmeCo/interns/",
                    vec![CapabilityBundle::Writer, CapabilityBundle::Delegate],
                ),
            ],
            vec![(
                "acmeCo/interns/betaCo/",
                "betaCo/shareable/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Writer],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                ("acmeCo/", CapabilityBundle::Viewer.capabilities()),
                (
                    "acmeCo/interns/",
                    CapabilityBundle::Writer.capabilities() | Delegate,
                ),
                ("betaCo/shareable/", CapabilityBundle::Writer.capabilities()),
            ],
        );
    }

    #[test]
    fn test_parent_child_capabilities() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/interns/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Writer,
                    CapabilityBundle::Delegate,
                ],
            )],
            vec![
                (
                    "acmeCo/",
                    "betaCo/shareable/",
                    vec![CapabilityBundle::Viewer],
                ),
                (
                    "acmeCo/interns/betaCo/",
                    "betaCo/shareable/",
                    vec![CapabilityBundle::Writer],
                ),
            ],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                (
                    "acmeCo/interns/",
                    CapabilityBundle::Writer.capabilities() | Delegate,
                ),
                ("betaCo/shareable/", CapabilityBundle::Viewer.capabilities()),
                ("betaCo/shareable/", CapabilityBundle::Writer.capabilities()),
            ],
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "betaCo/shareable/",
            CapabilityBundle::Writer.capabilities(),
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "betaCo/shareable/",
            EnumSet::from(Delegate),
        );
    }

    #[test]
    fn test_multi_path() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![
                (
                    "acmeCo/",
                    vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
                ),
                (
                    "betaCo/",
                    vec![CapabilityBundle::Writer, CapabilityBundle::Delegate],
                ),
            ],
            vec![
                (
                    "acmeCo/",
                    "charlieCo/shareable/",
                    vec![CapabilityBundle::Viewer],
                ),
                ("betaCo/", "charlieCo/", vec![CapabilityBundle::Writer]),
            ],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                (
                    "acmeCo/",
                    CapabilityBundle::Viewer.capabilities() | Delegate,
                ),
                (
                    "betaCo/",
                    CapabilityBundle::Writer.capabilities() | Delegate,
                ),
                ("charlieCo/", CapabilityBundle::Writer.capabilities()),
                (
                    "charlieCo/shareable/",
                    CapabilityBundle::Viewer.capabilities(),
                ),
            ],
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "charlieCo/shareable/",
            CapabilityBundle::Writer.capabilities(),
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "charlieCo/",
            EnumSet::from(Delegate),
        );
    }

    #[test]
    fn test_is_authorized() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
            )],
            vec![
                (
                    "acmeCo/",
                    "bobCo/shared/",
                    vec![
                        CapabilityBundle::Viewer,
                        CapabilityBundle::Billing,
                        CapabilityBundle::Delegate,
                    ],
                ),
                (
                    "bobCo/shared/",
                    "carolCo/data/",
                    vec![CapabilityBundle::Viewer],
                ),
            ],
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "acmeCo/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "acmeCo/thing",
            EnumSet::from(CreateGrant),
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "bobCo/shared/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
        assert_not_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "bobCo/shared/thing",
            EnumSet::from(CreateGrant),
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "carolCo/data/thing",
            CapabilityBundle::Viewer.capabilities(),
        );

        let unknown = uuid::Uuid::from_bytes([9; 16]);
        assert_not_authorized(
            &role_grants,
            &user_grants,
            unknown,
            "acmeCo/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
    }

    #[test]
    fn test_mixed_legacy_and_bundles() {
        use Capability::*;

        let user_id = uuid::Uuid::from_bytes([1; 16]);
        let user_grants = UserGrants::from_iter(vec![UserGrant {
            user_id,
            object_role: models::Prefix::new("acmeCo/"),
            capability: models::Capability::Write,
            bundles: vec![CapabilityBundle::TeamAdmin],
        }]);
        let role_grants = RoleGrants::new();

        let nodes: Vec<_> =
            UserGrant::reachable_nodes(&role_grants, &user_grants, Principal::unscoped(user_id))
                .collect();

        assert_eq!(nodes.len(), 1);
        let node = &nodes[0];
        assert_eq!(node.object_role, "acmeCo/");

        let expected =
            CapabilityBundle::Writer.capabilities() | CapabilityBundle::TeamAdmin.capabilities();
        assert_eq!(node.capabilities, expected);

        assert!(node.capabilities.contains(CatalogRead));
        assert!(node.capabilities.contains(JournalAppend));
        assert!(node.capabilities.contains(CreateGrant));
        assert!(!node.capabilities.contains(SpecEdit));
        assert!(!node.capabilities.contains(Delegate));
    }

    #[test]
    fn test_assume_propagates_full_capability_set() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Assume],
            )],
            vec![(
                "acmeCo/",
                "bobCo/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::Delegate,
                ],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                ("acmeCo/", CapabilityBundle::Viewer.capabilities() | Assume),
                (
                    "bobCo/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | Delegate,
                ),
            ],
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "bobCo/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
    }

    #[test]
    fn test_assume_vs_delegate_capability_filtering() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
            )],
            vec![(
                "acmeCo/",
                "bobCo/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::Delegate,
                ],
            )],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                (
                    "acmeCo/",
                    CapabilityBundle::Viewer.capabilities() | Delegate,
                ),
                ("bobCo/", CapabilityBundle::Viewer.capabilities() | Delegate),
            ],
        );

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Assume],
            )],
            vec![(
                "acmeCo/",
                "bobCo/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::Delegate,
                ],
            )],
        );

        assert_authorized(
            &role_grants,
            &user_grants,
            user_id,
            "bobCo/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
    }

    #[test]
    fn test_assume_chains_through_edges() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Assume],
            )],
            vec![
                (
                    "acmeCo/",
                    "bobCo/",
                    vec![
                        CapabilityBundle::Viewer,
                        CapabilityBundle::Billing,
                        CapabilityBundle::Assume,
                    ],
                ),
                (
                    "bobCo/",
                    "carolCo/",
                    vec![CapabilityBundle::Viewer, CapabilityBundle::Billing],
                ),
            ],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                ("acmeCo/", CapabilityBundle::Viewer.capabilities() | Assume),
                (
                    "bobCo/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | Assume,
                ),
                (
                    "carolCo/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities(),
                ),
            ],
        );
    }

    #[test]
    fn test_assume_does_not_chain_without_edge_delegate() {
        use Capability::*;

        let (role_grants, user_grants, user_id) = build_scenario(
            vec![(
                "acmeCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Assume],
            )],
            vec![
                (
                    "acmeCo/",
                    "bobCo/",
                    vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
                ),
                (
                    "bobCo/",
                    "carolCo/",
                    vec![CapabilityBundle::Viewer, CapabilityBundle::Billing],
                ),
            ],
        );

        assert_reachable(
            &role_grants,
            &user_grants,
            user_id,
            vec![
                ("acmeCo/", CapabilityBundle::Viewer.capabilities() | Assume),
                ("bobCo/", CapabilityBundle::Viewer.capabilities() | Delegate),
                ("carolCo/", CapabilityBundle::Viewer.capabilities()),
            ],
        );
    }

    fn build_role_scenario(role_edges: Vec<(&str, &str, Vec<CapabilityBundle>)>) -> RoleGrants {
        RoleGrants::from_iter(role_edges.into_iter().map(|(sub, obj, bundles)| RoleGrant {
            subject_role: models::Prefix::new(sub),
            object_role: models::Prefix::new(obj),
            capability: models::Capability::None,
            bundles,
        }))
    }

    fn assert_role_reachable(
        role_grants: &RoleGrants,
        role_or_name: &str,
        expected: Vec<(&str, EnumSet<Capability>)>,
    ) {
        let mut nodes: Vec<_> = RoleGrant::reachable_nodes(role_grants, role_or_name)
            .map(|n| (n.object_role.to_string(), n.capabilities))
            .collect();
        nodes.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.as_u32().cmp(&b.1.as_u32())));
        nodes.dedup();

        let expected: Vec<(String, EnumSet<Capability>)> = expected
            .into_iter()
            .map(|(prefix, caps)| (prefix.to_string(), caps))
            .collect();

        assert_eq!(nodes, expected);
    }

    fn assert_role_authorized(
        role_grants: &RoleGrants,
        subject: &str,
        object: &str,
        required: EnumSet<Capability>,
    ) {
        assert!(
            RoleGrant::is_authorized(role_grants, subject, object, required),
            "expected {subject} to have {required:?} on {object}",
        );
    }

    fn assert_role_not_authorized(
        role_grants: &RoleGrants,
        subject: &str,
        object: &str,
        required: EnumSet<Capability>,
    ) {
        assert!(
            !RoleGrant::is_authorized(role_grants, subject, object, required),
            "expected {subject} NOT to have {required:?} on {object}",
        );
    }

    #[test]
    fn test_role_reachable_nodes_delegate_propagation() {
        use Capability::*;

        let role_grants = build_role_scenario(vec![
            (
                "acmeCo/",
                "bobCo/shared/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::Delegate,
                ],
            ),
            (
                "bobCo/shared/",
                "carolCo/data/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Delegate],
            ),
            (
                "carolCo/data/",
                "daveCo/sink/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Billing],
            ),
        ]);

        assert_role_reachable(
            &role_grants,
            "acmeCo/",
            vec![
                (
                    "bobCo/shared/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | Delegate,
                ),
                (
                    "carolCo/data/",
                    CapabilityBundle::Viewer.capabilities() | Delegate,
                ),
                ("daveCo/sink/", CapabilityBundle::Viewer.capabilities()),
            ],
        );
    }

    #[test]
    fn test_role_reachable_nodes_no_delegate_is_terminal() {
        let role_grants = build_role_scenario(vec![
            ("acmeCo/", "bobCo/shared/", vec![CapabilityBundle::Viewer]),
            ("bobCo/shared/", "carolCo/", vec![CapabilityBundle::Viewer]),
        ]);

        assert_role_reachable(
            &role_grants,
            "acmeCo/",
            vec![("bobCo/shared/", CapabilityBundle::Viewer.capabilities())],
        );
    }

    #[test]
    fn test_role_assume_propagates_all_capabilities() {
        use Capability::*;

        let role_grants = build_role_scenario(vec![
            (
                "acmeCo/",
                "bobCo/",
                vec![CapabilityBundle::Viewer, CapabilityBundle::Assume],
            ),
            (
                "bobCo/",
                "carolCo/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::Delegate,
                ],
            ),
        ]);

        assert_role_reachable(
            &role_grants,
            "acmeCo/",
            vec![
                ("bobCo/", CapabilityBundle::Viewer.capabilities() | Assume),
                (
                    "carolCo/",
                    CapabilityBundle::Viewer.capabilities()
                        | CapabilityBundle::Billing.capabilities()
                        | Delegate,
                ),
            ],
        );

        assert_role_authorized(
            &role_grants,
            "acmeCo/",
            "carolCo/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
    }

    #[test]
    fn test_role_is_authorized() {
        let role_grants = build_role_scenario(vec![
            (
                "acmeCo/",
                "bobCo/shared/",
                vec![
                    CapabilityBundle::Viewer,
                    CapabilityBundle::Billing,
                    CapabilityBundle::Delegate,
                ],
            ),
            (
                "bobCo/shared/",
                "carolCo/data/",
                vec![CapabilityBundle::Viewer],
            ),
        ]);

        assert_role_authorized(
            &role_grants,
            "acmeCo/",
            "bobCo/shared/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
        assert_role_authorized(
            &role_grants,
            "acmeCo/",
            "carolCo/data/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
        assert_role_not_authorized(
            &role_grants,
            "acmeCo/",
            "unknown/thing",
            CapabilityBundle::Viewer.capabilities(),
        );
    }
}
