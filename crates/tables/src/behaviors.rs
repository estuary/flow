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

impl super::RoleGrant {
    /// Given a role or name, enumerate all granted roles and capabilities.
    pub fn transitive_roles<'a>(
        role_grants: &'a [super::RoleGrant],
        role_or_name: &'a str,
    ) -> impl Iterator<Item = super::GrantRef<'a>> + 'a {
        let seed = super::GrantRef {
            subject_role: role_or_name,
            object_role: role_or_name,
            capability: models::Capability::Admin,
        };
        pathfinding::directed::bfs::bfs_reach(seed, |f| {
            grant_edges(*f, role_grants, &[], uuid::Uuid::nil())
        })
        .skip(1) // Skip `seed`.
    }

    /// Given a role or name, enumerate all reachable nodes and their orthogonal capabilities.
    pub fn reachable_nodes<'a>(
        role_grants: &'a [super::RoleGrant],
        role_or_name: &'a str,
    ) -> impl Iterator<Item = super::NodeRef<'a>> + 'a {
        let seed = super::NodeRef {
            object_role: role_or_name,
            // Seed with Assume so the first expansion assumes all capabilities
            // through unfiltered — the role itself is the trust root.
            capabilities: vec![models::OrthogonalCapability::Assume],
        };
        pathfinding::directed::bfs::bfs_reach(seed, move |f| {
            next_neighbors(f.clone(), role_grants, &[], uuid::Uuid::nil())
        })
        .skip(1)
    }

    /// Given a role or name, determine if it's authorized to the object name for the given capability.
    pub fn is_authorized<'a>(
        role_grants: &'a [super::RoleGrant],
        subject_role_or_name: &'a str,
        object_role_or_name: &'a str,
        capability: impl Into<models::AnyCapability>,
    ) -> bool {
        match capability.into() {
            models::AnyCapability::Legacy(cap) => {
                Self::transitive_roles(role_grants, subject_role_or_name).any(|role_grant| {
                    object_role_or_name.starts_with(role_grant.object_role)
                        && role_grant.capability >= cap
                })
            }
            models::AnyCapability::Orthogonal(required) => {
                if required.is_empty() {
                    debug_assert!(
                        false,
                        "is_authorized called with empty orthogonal capabilities"
                    );
                    return false;
                }
                let mut remaining = required;
                for node in Self::reachable_nodes(role_grants, subject_role_or_name) {
                    if object_role_or_name.starts_with(node.object_role) {
                        remaining.retain(|cap| !node.capabilities.contains(cap));
                        if remaining.is_empty() {
                            return true;
                        }
                    }
                }
                false
            }
        }
    }

    fn to_ref<'a>(&'a self) -> super::GrantRef<'a> {
        super::GrantRef {
            subject_role: self.subject_role.as_str(),
            object_role: self.object_role.as_str(),
            capability: self.capability,
        }
    }

    fn to_node_ref<'a>(
        &'a self,
        delegatable: &[models::OrthogonalCapability],
    ) -> super::NodeRef<'a> {
        let mut capabilities: Vec<_> = self
            .capabilities
            .iter()
            .filter(|c| delegatable.contains(c))
            .copied()
            .collect();
        capabilities.sort();
        super::NodeRef {
            object_role: self.object_role.as_str(),
            capabilities,
        }
    }
}

impl super::UserGrant {
    /// Given a user, enumerate all granted roles and capabilities.
    pub fn transitive_roles<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        user_id: uuid::Uuid,
    ) -> impl Iterator<Item = super::GrantRef<'a>> + 'a {
        let seed = super::GrantRef {
            subject_role: "",
            object_role: "", // Empty role causes us to map through user_grants.
            capability: models::Capability::Admin,
        };
        pathfinding::directed::bfs::bfs_reach(seed, move |f| {
            grant_edges(*f, role_grants, user_grants, user_id)
        })
        .skip(1) // Skip `seed`.
    }

    /// Given a user, enumerate all reachable nodes and their orthogonal capabilities.
    pub fn reachable_nodes<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        user_id: uuid::Uuid,
    ) -> impl Iterator<Item = super::NodeRef<'a>> + 'a {
        let seed = super::NodeRef {
            object_role: "",
            // Seed with Assume so the first expansion delegates all capabilities
            // through unfiltered — user_grants are the trust root.
            capabilities: vec![models::OrthogonalCapability::Assume],
        };
        pathfinding::directed::bfs::bfs_reach(seed, move |f| {
            next_neighbors(f.clone(), role_grants, user_grants, user_id)
        })
        .skip(1)
    }

    pub fn get_user_capability<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        user_id: uuid::Uuid,
        object_role_or_name: &str,
    ) -> Option<models::Capability> {
        Self::transitive_roles(role_grants, user_grants, user_id)
            .filter(|grant| object_role_or_name.starts_with(grant.object_role))
            .max_by_key(|grant| grant.capability)
            .map(|grant| grant.capability)
    }

    /// Given a user, determine if they're authorized to the object name for the given capability.
    pub fn is_authorized<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        subject_user_id: uuid::Uuid,
        object_role_or_name: &'a str,
        capability: impl Into<models::AnyCapability>,
    ) -> bool {
        match capability.into() {
            models::AnyCapability::Legacy(cap) => {
                Self::transitive_roles(role_grants, user_grants, subject_user_id).any(
                    |role_grant| {
                        object_role_or_name.starts_with(role_grant.object_role)
                            && role_grant.capability >= cap
                    },
                )
            }
            models::AnyCapability::Orthogonal(required) => {
                if required.is_empty() {
                    debug_assert!(
                        false,
                        "is_authorized called with empty orthogonal capabilities"
                    );
                    return false;
                }
                // Capabilities may be split across multiple covering nodes
                // (e.g. read via one path, write via another). Collect the
                // union across all nodes whose object_role is a prefix of
                // the target, bailing early once all required caps are found.
                let mut remaining = required;
                for node in Self::reachable_nodes(role_grants, user_grants, subject_user_id) {
                    if object_role_or_name.starts_with(node.object_role) {
                        remaining.retain(|cap| !node.capabilities.contains(cap));
                        if remaining.is_empty() {
                            return true;
                        }
                    }
                }
                false
            }
        }
    }

    fn to_ref<'a>(&'a self) -> super::GrantRef<'a> {
        super::GrantRef {
            subject_role: "",
            object_role: self.object_role.as_str(),
            capability: self.capability,
        }
    }

    fn to_node_ref<'a>(
        &'a self,
        delegatable: &[models::OrthogonalCapability],
    ) -> super::NodeRef<'a> {
        let mut capabilities: Vec<_> = self
            .capabilities
            .iter()
            .filter(|c| delegatable.contains(c))
            .copied()
            .collect();
        capabilities.sort();
        super::NodeRef {
            object_role: self.object_role.as_str(),
            capabilities,
        }
    }
}

fn grant_edges<'a>(
    from: super::GrantRef<'a>,
    role_grants: &'a [super::RoleGrant],
    user_grants: &'a [super::UserGrant],
    user_id: uuid::Uuid,
) -> impl Iterator<Item = super::GrantRef<'a>> + 'a {
    let (user_grants, role_grants, prefixes) = match (from.capability, from.object_role) {
        // `from` is a place-holder which kicks of exploration through `user_grants` for `user_id`.
        (models::Capability::Admin, "") => {
            let range = user_grants.equal_range_by(|user_grant| user_grant.user_id.cmp(&user_id));
            (&user_grants[range], &role_grants[..0], None)
        }
        // We're an admin of `role_or_name`, and are projecting through
        // role_grants to identify other roles and capabilities we take on.
        (models::Capability::Admin, role_or_name) => {
            // Expand to all roles having a subject_role prefixed by role_or_name.
            // In other words, an admin of `acmeCo/org/` may use a role with
            // subject `acmeCo/org/team/`. Intuitively, this is because the root
            // subject is authorized to create any name under `acmeCo/org/`,
            // which implies an ability to create a name under `acmeCo/org/team/`.
            let range = role_grants.equal_range_by(|role_grant| {
                if role_grant.subject_role.starts_with(role_or_name) {
                    std::cmp::Ordering::Equal
                } else {
                    role_grant.subject_role.as_str().cmp(role_or_name)
                }
            });
            // Expand to all roles having a subject_role which prefixes role_or_name.
            // In other words, a task `acmeCo/org/task` or admin of `acmeCo/org/`
            // may use a role with subject `acmeCo/`. Intuitively, this is because
            // the role granted to `acmeCo/` is also granted to any name underneath
            // `acmeCo/`, which includes the present role or name.
            //
            // First split the source object role into its prefixes:
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
                    role_grants
                        .equal_range_by(|role_grant| role_grant.subject_role.as_str().cmp(prefix))
                })
                .map(|range| role_grants[range].into_iter().map(super::RoleGrant::to_ref))
                .flatten();

            (&user_grants[..0], &role_grants[range], Some(edges))
        }
        (_not_admin, _) => {
            // We perform no expansion through grants which are not Admin.
            (&user_grants[..0], &role_grants[..0], None)
        }
    };

    let p1 = user_grants.iter().map(super::UserGrant::to_ref);
    let p2 = role_grants.iter().map(super::RoleGrant::to_ref);
    let p3 = prefixes.into_iter().flatten();

    p1.chain(p2).chain(p3)
}

// Expand a BFS node into its neighbors. A node is terminal (no expansion)
// unless it carries Delegate or Assume. Delegate passes only the node's own
// capabilities through to neighbors; Assume passes all capabilities.
//
// Perf note: bfs_reach keys on (object_role, capabilities), so the same prefix
// with different capability subsets produces distinct BFS nodes — up to 2^N per
// prefix where N is the number of capabilities. If deep grant graphs cause
// latency, replace bfs_reach with a manual BFS that keys visited state on
// object_role alone and prunes dominated capability subsets.
fn next_neighbors<'a>(
    from: super::NodeRef<'a>,
    role_edges: &'a [super::RoleGrant],
    user_edges: &'a [super::UserGrant],
    user_id: uuid::Uuid,
) -> impl Iterator<Item = super::NodeRef<'a>> + 'a {
    let has_delegate = from
        .capabilities
        .contains(&models::OrthogonalCapability::Delegate);
    let has_assume = from
        .capabilities
        .contains(&models::OrthogonalCapability::Assume);
    let is_terminal = !has_delegate && !has_assume;
    let delegatable = std::sync::Arc::new(if has_assume {
        models::OrthogonalCapability::all()
    } else if has_delegate {
        from.capabilities
    } else {
        vec![]
    });

    let (user_edges, role_edges, prefixes) = match (is_terminal, from.object_role) {
        // the from node is terminal: no further exploration.
        (true, _) => (&user_edges[..0], &role_edges[..0], None),
        // This is the seed: traverse through user_grants to kick off exploration.
        (_, "") => {
            let range = user_edges.equal_range_by(|user_grant| user_grant.user_id.cmp(&user_id));
            (&user_edges[range], &role_edges[..0], None)
        }

        // Expand downward (grants whose subject is under role_or_name)
        // and upward (grants on parent prefixes of role_or_name).
        (_, role_or_name) => {
            let range = role_edges.equal_range_by(|role_grant| {
                if role_grant.subject_role.starts_with(role_or_name) {
                    std::cmp::Ordering::Equal
                } else {
                    role_grant.subject_role.as_str().cmp(role_or_name)
                }
            });
            // Decompose into parent prefixes and binary-search each one
            // (instead of a linear scan for role_or_name.starts_with(subject))
            let prefixes = role_or_name.char_indices().filter_map(|(ind, chr)| {
                if chr == '/' {
                    Some(&role_or_name[..ind + 1])
                } else {
                    None
                }
            });
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

    let a1 = delegatable.clone();
    let a2 = delegatable.clone();

    let p1 = user_edges.iter().map(move |g| g.to_node_ref(&delegatable));
    let p2 = role_edges.iter().map(move |g| g.to_node_ref(&a1));
    let p3 = prefixes
        .into_iter()
        .flatten()
        .map(move |g| g.to_node_ref(&a2));

    p1.chain(p2).chain(p3)
}

impl super::StorageMapping {
    pub fn scope(&self) -> url::Url {
        crate::synthetic_scope("storageMapping", &self.catalog_prefix)
    }
}

#[cfg(test)]
mod test {
    use crate::{Import, Imports, RoleGrant, RoleGrants, UserGrant, UserGrants};

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
    fn test_transitive_roles() {
        use models::Capability::*;

        let role_grants = RoleGrants::from_iter(
            [
                ("aliceCo/widgets/", "bobCo/burgers/", Admin),
                ("aliceCo/anvils/", "carolCo/paper/", Write),
                ("aliceCo/duplicate/", "carolCo/paper/", Read),
                ("aliceCo/stuff/", "carolCo/shared/", Read),
                ("bobCo/alice-vendor/", "aliceCo/bob-shared/", Admin),
                ("carolCo/shared/", "carolCo/hidden/", Read),
                ("daveCo/hidden/", "carolCo/hidden/", Admin),
                ("carolCo/hidden/", "carolCo/even/more/hidden/", Read),
            ]
            .into_iter()
            .map(|(sub, obj, cap)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability: cap,
                capabilities: vec![],
            }),
        );
        let user_grants = UserGrants::from_iter(
            [
                (uuid::Uuid::nil(), "bobCo/", Read),
                (uuid::Uuid::nil(), "daveCo/", Admin),
                (uuid::Uuid::max(), "aliceCo/widgets/", Admin),
                (uuid::Uuid::max(), "carolCo/shared/", Admin),
            ]
            .into_iter()
            .map(|(user_id, obj, cap)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability: cap,
                capabilities: vec![],
            }),
        );

        insta::assert_json_snapshot!(
            RoleGrant::transitive_roles(&role_grants, "aliceCo/anvils/thing").collect::<Vec<_>>(),
            @r#"
        [
          {
            "subject_role": "aliceCo/anvils/",
            "object_role": "carolCo/paper/",
            "capability": "write"
          }
        ]
        "#,
        );

        insta::assert_json_snapshot!(
            RoleGrant::transitive_roles(&role_grants, "daveCo/hidden/task").collect::<Vec<_>>(),
            @r#"
        [
          {
            "subject_role": "daveCo/hidden/",
            "object_role": "carolCo/hidden/",
            "capability": "admin"
          },
          {
            "subject_role": "carolCo/hidden/",
            "object_role": "carolCo/even/more/hidden/",
            "capability": "read"
          }
        ]
        "#,
        );

        assert!(RoleGrant::is_authorized(
            &role_grants,
            "daveCo/hidden/thing",
            "carolCo/hidden/thing",
            Write
        ));
        assert!(RoleGrant::is_authorized(
            &role_grants,
            "daveCo/hidden/",
            "carolCo/even/more/hidden/",
            Read
        ));
        assert!(!RoleGrant::is_authorized(
            &role_grants,
            "daveCo/hidden/thing",
            "carolCo/even/more/hidden/",
            Write
        ));

        insta::assert_json_snapshot!(
            UserGrant::transitive_roles(&role_grants, &user_grants, uuid::Uuid::nil()).collect::<Vec<_>>(),
            @r#"
        [
          {
            "subject_role": "",
            "object_role": "bobCo/",
            "capability": "read"
          },
          {
            "subject_role": "",
            "object_role": "daveCo/",
            "capability": "admin"
          },
          {
            "subject_role": "daveCo/hidden/",
            "object_role": "carolCo/hidden/",
            "capability": "admin"
          },
          {
            "subject_role": "carolCo/hidden/",
            "object_role": "carolCo/even/more/hidden/",
            "capability": "read"
          }
        ]
        "#,
        );

        insta::assert_json_snapshot!(
            UserGrant::transitive_roles(&role_grants, &user_grants, uuid::Uuid::max()).collect::<Vec<_>>(),
            @r#"
        [
          {
            "subject_role": "",
            "object_role": "aliceCo/widgets/",
            "capability": "admin"
          },
          {
            "subject_role": "",
            "object_role": "carolCo/shared/",
            "capability": "admin"
          },
          {
            "subject_role": "aliceCo/widgets/",
            "object_role": "bobCo/burgers/",
            "capability": "admin"
          },
          {
            "subject_role": "carolCo/shared/",
            "object_role": "carolCo/hidden/",
            "capability": "read"
          }
        ]
        "#,
        );
    }

    #[test]
    fn test_transitive_roles_more() {
        let role_grants: Vec<crate::RoleGrant> = serde_json::from_value(serde_json::json!([
          {
            "subject_role": "acmeCo/",
            "object_role": "acmeCo/",
            "capability": "write",
            "capabilities": []
          },
          {
            "subject_role": "other_tenant/",
            "object_role": "acmeCo/",
            "capability": "admin",
            "capabilities": []
          },
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "acmeCo-૨/",
            "capability": "write",
            "capabilities": []
          },
          {
            "subject_role": "other_tenant/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "capabilities": []
          },
          {
            "subject_role": "acmeCo-૨/ssss/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "capabilities": []
          },
          {
            "subject_role": "acmeCo-૨/aaaa/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "capabilities": []
          },
          {
            "subject_role": "acmeCo-૨/dddd/",
            "object_role": "acmeCo-૨/",
            "capability": "admin",
            "capabilities": []
          },
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "ops/dp/public/",
            "capability": "read",
            "capabilities": []
          },
          {
            "subject_role": "acmeCo/",
            "object_role": "ops/dp/public/",
            "capability": "read",
            "capabilities": []
          }
        ]))
        .unwrap();
        let role_grants = crate::RoleGrants::from_iter(role_grants);

        insta::assert_json_snapshot!(
            RoleGrant::transitive_roles(&role_grants, "acmeCo-૨/acme-prod-tables/materialize-snowflake").collect::<Vec<_>>(),
            @r#"
        [
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "acmeCo-૨/",
            "capability": "write"
          },
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "ops/dp/public/",
            "capability": "read"
          }
        ]
        "#,
        );

        assert!(crate::RoleGrant::is_authorized(
            &role_grants,
            "acmeCo-૨/acme-prod-tables/materialize-snowflake",
            "acmeCo-૨/acme-data/my_data/",
            models::Capability::Read
        ));
    }

    #[test]
    fn test_get_user_capability() {
        use models::Capability::*;
        let role_grants = RoleGrants::from_iter(
            [
                ("acmeCo/", "acmeCo/", Write),
                ("acmeCo/", "ops/private/dp/acmeCo/", Read),
            ]
            .into_iter()
            .map(|(sub, obj, cap)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability: cap,
                capabilities: vec![],
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
            .map(|(user_id, obj, cap)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability: cap,
                capabilities: vec![],
            }),
        );

        assert_eq!(
            Some(Read),
            UserGrant::get_user_capability(
                &role_grants,
                &user_grants,
                user1,
                "ops/private/dp/acmeCo/foooo"
            )
        );
        assert_eq!(
            Some(Write),
            UserGrant::get_user_capability(
                &role_grants,
                &user_grants,
                user2,
                "ops/private/dp/acmeCo/foooo"
            )
        );
        assert_eq!(
            None,
            UserGrant::get_user_capability(
                &role_grants,
                &user_grants,
                user1,
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
            .map(|(sub, obj, cap)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability: cap,
                capabilities: vec![],
            }),
        );
        let user_grants = UserGrants::from_iter(
            [
                (uuid::Uuid::from_bytes([1; 16]), "acmeCo/", Admin),
                (uuid::Uuid::from_bytes([2; 16]), "acmeCo/nested/", Admin),
            ]
            .into_iter()
            .map(|(user_id, obj, cap)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability: cap,
                capabilities: vec![],
            }),
        );

        insta::assert_json_snapshot!(
            UserGrant::transitive_roles(&role_grants, &user_grants, uuid::Uuid::from_bytes([1;16])).collect::<Vec<_>>(),
            @r#"
        [
          {
            "subject_role": "",
            "object_role": "acmeCo/",
            "capability": "admin"
          },
          {
            "subject_role": "acmeCo/",
            "object_role": "acmeCo/",
            "capability": "write"
          },
          {
            "subject_role": "acmeCo/",
            "object_role": "ops/private/dp/acmeCo/",
            "capability": "read"
          }
        ]
        "#,
        );

        insta::assert_json_snapshot!(
            UserGrant::transitive_roles(&role_grants, &user_grants, uuid::Uuid::from_bytes([2;16])).collect::<Vec<_>>(),
            @r#"
        [
          {
            "subject_role": "",
            "object_role": "acmeCo/nested/",
            "capability": "admin"
          },
          {
            "subject_role": "acmeCo/",
            "object_role": "acmeCo/",
            "capability": "write"
          },
          {
            "subject_role": "acmeCo/",
            "object_role": "ops/private/dp/acmeCo/",
            "capability": "read"
          }
        ]
        "#,
        );
    }

    fn build_orthogonal_scenario(
        user_edges: Vec<(&str, Vec<models::OrthogonalCapability>)>,
        role_edges: Vec<(&str, &str, Vec<models::OrthogonalCapability>)>,
    ) -> (RoleGrants, UserGrants, uuid::Uuid) {
        let user_id = uuid::Uuid::from_bytes([1; 16]);
        let user_grants =
            UserGrants::from_iter(user_edges.into_iter().map(|(obj, caps)| UserGrant {
                user_id,
                object_role: models::Prefix::new(obj),
                capability: models::Capability::Admin,
                capabilities: caps,
            }));
        let role_grants =
            RoleGrants::from_iter(role_edges.into_iter().map(|(sub, obj, caps)| RoleGrant {
                subject_role: models::Prefix::new(sub),
                object_role: models::Prefix::new(obj),
                capability: models::Capability::Admin,
                capabilities: caps,
            }));
        (role_grants, user_grants, user_id)
    }

    fn assert_reachable(
        role_grants: &RoleGrants,
        user_grants: &UserGrants,
        user_id: uuid::Uuid,
        expected: Vec<(&str, Vec<models::OrthogonalCapability>)>,
    ) {
        let mut nodes: Vec<_> = UserGrant::reachable_nodes(role_grants, user_grants, user_id)
            .map(|n| (n.object_role.to_string(), n.capabilities))
            .collect();
        nodes.sort();
        nodes.dedup();

        let expected: Vec<(String, Vec<models::OrthogonalCapability>)> = expected
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
        required: Vec<models::OrthogonalCapability>,
    ) {
        assert!(
            UserGrant::is_authorized(
                role_grants,
                user_grants,
                user_id,
                name,
                models::AnyCapability::Orthogonal(required.clone()),
            ),
            "expected {user_id} to have {required:?} on {name}",
        );
    }

    fn assert_not_authorized(
        role_grants: &RoleGrants,
        user_grants: &UserGrants,
        user_id: uuid::Uuid,
        name: &str,
        required: Vec<models::OrthogonalCapability>,
    ) {
        assert!(
            !UserGrant::is_authorized(
                role_grants,
                user_grants,
                user_id,
                name,
                models::AnyCapability::Orthogonal(required.clone()),
            ),
            "expected {user_id} NOT to have {required:?} on {name}",
        );
    }

    #[test]
    fn test_reachable_nodes_delegate_propagation() {
        use models::OrthogonalCapability::*;

        // Given: user has [read, billing, delegate] on acmeCo/
        // And cross-tenant role grants:
        //   acmeCo/ -[read, billing, delegate]-> bobCo/shared/
        //   bobCo/shared/ -[read, delegate]-> carolCo/data/
        //   carolCo/data/ -[read, billing]-> daveCo/sink/   (no delegate — terminal)
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Billing, Delegate])],
            vec![
                ("acmeCo/", "bobCo/shared/", vec![Read, Billing, Delegate]),
                ("bobCo/shared/", "carolCo/data/", vec![Read, Delegate]),
                ("carolCo/data/", "daveCo/sink/", vec![Read, Billing]),
            ],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read, Billing, Delegate]),
                ("bobCo/shared/", vec![Read, Billing, Delegate]),
                ("carolCo/data/", vec![Read, Delegate]),
                ("daveCo/sink/", vec![Read]),
            ],
        );
    }

    #[test]
    fn test_reachable_nodes_no_delegate_is_terminal() {
        use models::OrthogonalCapability::*;

        // Given: user has [read, delegate] on acmeCo/
        // And cross-tenant role grants:
        //   acmeCo/ -[read]-> bobCo/shared/          (no delegate — terminal)
        //   bobCo/shared/ -[read]-> carolCo/          (unreachable from user)
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Delegate])],
            vec![
                ("acmeCo/", "bobCo/shared/", vec![Read]),
                ("bobCo/shared/", "carolCo/", vec![Read]),
            ],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read, Delegate]),
                ("bobCo/shared/", vec![Read]),
                // carolCo/ is NOT reachable — bobCo/shared/ has no delegate
            ],
        );

        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read])],
            vec![
                ("acmeCo/", "bobCo/shared/", vec![Read, Delegate]),
                ("bobCo/shared/", "carolCo/", vec![Read]),
            ],
        );

        assert_reachable(&rg, &ug, uid, vec![("acmeCo/", vec![Read])]);
        assert_not_authorized(&rg, &ug, uid, "bobCo/shared/", vec![Read]);
        assert_not_authorized(&rg, &ug, uid, "carolCo/", vec![Read]);
    }

    #[test]
    fn test_assume_behavior() {
        use models::OrthogonalCapability::*;

        // Assume does not grant anything to the object itself
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Assume])],
            vec![("acmeCo/", "bobCo/shared/", vec![Read, Billing, TeamAdmin])],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Assume]),
                ("bobCo/shared/", vec![Read, Billing, TeamAdmin]),
            ],
        );

        assert_authorized(
            &rg,
            &ug,
            uid,
            "bobCo/shared/nested/",
            vec![Read, Billing, TeamAdmin],
        );
        assert_not_authorized(&rg, &ug, uid, "acmeCo/", vec![Read]);

        // Assume does not add capabilities to the following edge
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Write, Assume])],
            vec![("acmeCo/", "bobCo/shared/", vec![Read, Billing, TeamAdmin])],
        );
        assert_authorized(&rg, &ug, uid, "acmeCo/", vec![Write]);
        assert_not_authorized(&rg, &ug, uid, "bobCo/shared/", vec![Write]);
    }

    #[test]
    fn test_assume_supersedes_delegate() {
        use models::OrthogonalCapability::*;

        // Given: user has [read, delegate, assume] on acmeCo/
        // And cross-tenant role grants:
        //   acmeCo/ -[read, billing, team_admin]-> bobCo/shared/
        //
        // With delegate alone, bobCo/shared/ would only receive [read]
        // (the intersection of the parent's caps with the edge's caps).
        // With assume, bobCo/shared/ receives the full edge: [billing, read, team_admin].
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Delegate, Assume])],
            vec![("acmeCo/", "bobCo/shared/", vec![Billing, Read, TeamAdmin])],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Assume, Delegate, Read]),
                ("bobCo/shared/", vec![Read, Billing, TeamAdmin]),
            ],
        );

        // Contrast: delegate alone attenuates to the intersection.
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Delegate])],
            vec![("acmeCo/", "bobCo/shared/", vec![Read, Billing, TeamAdmin])],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Delegate, Read]),
                ("bobCo/shared/", vec![Read]),
            ],
        );

        // Assume does not add capabilities to the following edge
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Write, Assume])],
            vec![("acmeCo/", "bobCo/shared/", vec![Read, Billing, TeamAdmin])],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Write, Assume]),
                ("bobCo/shared/", vec![Billing, Read, TeamAdmin]),
            ],
        );
    }

    #[test]
    fn test_inherited_capabilities() {
        use models::OrthogonalCapability::*;

        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![
                ("acmeCo/", vec![Read]),
                ("acmeCo/interns/", vec![Write, Delegate]),
            ],
            vec![("acmeCo/", "betaCo/shareable/", vec![Read, Write])],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read]),
                ("acmeCo/interns/", vec![Write, Delegate]),
                ("betaCo/shareable/", vec![Write]),
            ],
        );
    }

    #[test]
    fn test_descendent_capabilities() {
        use models::OrthogonalCapability::*;

        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![
                ("acmeCo/", vec![Read]),
                ("acmeCo/interns/", vec![Write, Delegate]),
            ],
            vec![(
                "acmeCo/interns/betaCo/",
                "betaCo/shareable/",
                vec![Read, Write],
            )],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read]),
                ("acmeCo/interns/", vec![Write, Delegate]),
                ("betaCo/shareable/", vec![Write]),
            ],
        );
    }

    #[test]
    fn test_parent_child_capabilities() {
        use models::OrthogonalCapability::*;

        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/interns/", vec![Read, Write, Delegate])],
            vec![
                ("acmeCo/", "betaCo/shareable/", vec![Read]),
                ("acmeCo/interns/betaCo/", "betaCo/shareable/", vec![Write]),
            ],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/interns/", vec![Read, Write, Delegate]),
                ("betaCo/shareable/", vec![Read]),
                ("betaCo/shareable/", vec![Write]),
            ],
        );

        assert_authorized(&rg, &ug, uid, "betaCo/shareable/", vec![Read, Write]);
        assert_not_authorized(&rg, &ug, uid, "betaCo/shareable/", vec![Delegate]);
    }

    #[test]
    fn test_multi_path() {
        use models::OrthogonalCapability::*;

        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![
                ("acmeCo/", vec![Read, Delegate]),
                ("betaCo/", vec![Write, Delegate]),
            ],
            vec![
                ("acmeCo/", "charlieCo/shareable/", vec![Read]),
                ("betaCo/", "charlieCo/", vec![Write]),
            ],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read, Delegate]),
                ("betaCo/", vec![Write, Delegate]),
                ("charlieCo/", vec![Write]),
                ("charlieCo/shareable/", vec![Read]),
            ],
        );

        assert_authorized(&rg, &ug, uid, "charlieCo/shareable/", vec![Read, Write]);
        assert_not_authorized(&rg, &ug, uid, "charlieCo/", vec![Read]);
    }

    #[test]
    fn test_orthogonal_is_authorized() {
        use models::OrthogonalCapability::*;

        // Given: user has [read, delegate] on acmeCo/
        // And cross-tenant role grants:
        //   acmeCo/ -[read, billing, delegate]-> bobCo/shared/
        //   bobCo/shared/ -[read]-> carolCo/data/   (no delegate — terminal)
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Delegate])],
            vec![
                ("acmeCo/", "bobCo/shared/", vec![Read, Billing, Delegate]),
                ("bobCo/shared/", "carolCo/data/", vec![Read]),
            ],
        );

        // Direct grant: user has read on acmeCo/
        assert_authorized(&rg, &ug, uid, "acmeCo/thing", vec![Read]);
        // User grant doesn't include billing
        assert_not_authorized(&rg, &ug, uid, "acmeCo/thing", vec![Billing]);

        // bobCo/shared/: delegatable [read] ∩ grant [read, billing, delegate] = [read] + delegate
        assert_authorized(&rg, &ug, uid, "bobCo/shared/thing", vec![Read]);
        assert_not_authorized(&rg, &ug, uid, "bobCo/shared/thing", vec![Billing]);
        assert_not_authorized(&rg, &ug, uid, "bobCo/shared/thing", vec![Read, Billing]);

        // carolCo/data/: delegatable [read] ∩ grant [read] = [read], terminal
        assert_authorized(&rg, &ug, uid, "carolCo/data/thing", vec![Read]);

        // Unknown user has nothing
        let unknown = uuid::Uuid::from_bytes([9; 16]);
        assert_not_authorized(&rg, &ug, unknown, "acmeCo/thing", vec![Read]);
    }

    #[test]
    fn test_assume_propagates_full_capability_set() {
        use models::OrthogonalCapability::*;

        // User has [Read, Assume] on acmeCo/.
        // Role grant acmeCo/ -> bobCo/ carries [Read, Billing, Delegate].
        // Assume means all capabilities are delegatable, so bobCo/ gets the
        // full edge set [Read, Billing, Delegate] — not just the intersection
        // with the user's own capabilities.
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Assume])],
            vec![("acmeCo/", "bobCo/", vec![Read, Billing, Delegate])],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read, Assume]),
                ("bobCo/", vec![Read, Billing, Delegate]),
            ],
        );

        assert_authorized(&rg, &ug, uid, "bobCo/thing", vec![Read]);
        assert_authorized(&rg, &ug, uid, "bobCo/thing", vec![Billing]);
        assert_authorized(&rg, &ug, uid, "bobCo/thing", vec![Read, Billing]);
    }

    #[test]
    fn test_assume_vs_delegate_capability_filtering() {
        use models::OrthogonalCapability::*;

        // With regular Delegate, only the user's own capabilities pass through.
        // User has [Read, Delegate] — Billing is NOT delegatable.
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Delegate])],
            vec![("acmeCo/", "bobCo/", vec![Read, Billing, Delegate])],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read, Delegate]),
                ("bobCo/", vec![Read, Delegate]),
            ],
        );
        assert_not_authorized(&rg, &ug, uid, "bobCo/thing", vec![Billing]);

        // Same topology but with Assume — Billing passes through.
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Assume])],
            vec![("acmeCo/", "bobCo/", vec![Read, Billing, Delegate])],
        );

        assert_authorized(&rg, &ug, uid, "bobCo/thing", vec![Billing]);
    }

    #[test]
    fn test_assume_chains_through_edges() {
        use models::OrthogonalCapability::*;

        // Assume on the user grant opens the first hop.
        // The edge to bobCo/ carries Assume, so bobCo/ also propagates everything.
        // The edge to carolCo/ carries only [Read, Billing] (no delegate) — terminal.
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Assume])],
            vec![
                ("acmeCo/", "bobCo/", vec![Read, Billing, Assume]),
                ("bobCo/", "carolCo/", vec![Read, Billing]),
            ],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read, Assume]),
                ("bobCo/", vec![Read, Billing, Assume]),
                ("carolCo/", vec![Read, Billing]),
            ],
        );

        assert_authorized(&rg, &ug, uid, "carolCo/thing", vec![Billing]);
    }

    #[test]
    fn test_assume_does_not_chain_without_edge_delegate() {
        use models::OrthogonalCapability::*;

        // Assume on user grant, but the edge to bobCo/ only carries
        // [Read, Delegate] (not Assume). bobCo/ gets [Read, Delegate] and
        // can continue traversal, but only propagates its own caps (Read, Delegate).
        // The edge to carolCo/ carries [Read, Billing] — bobCo/ can only
        // delegate [Read, Delegate], so carolCo/ gets [Read] (Billing filtered out).
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![Read, Assume])],
            vec![
                ("acmeCo/", "bobCo/", vec![Read, Delegate]),
                ("bobCo/", "carolCo/", vec![Read, Billing]),
            ],
        );

        assert_reachable(
            &rg,
            &ug,
            uid,
            vec![
                ("acmeCo/", vec![Read, Assume]),
                ("bobCo/", vec![Read, Delegate]),
                ("carolCo/", vec![Read]),
            ],
        );

        assert_not_authorized(&rg, &ug, uid, "carolCo/thing", vec![Billing]);
    }

    fn build_role_scenario(
        role_edges: Vec<(&str, &str, Vec<models::OrthogonalCapability>)>,
    ) -> RoleGrants {
        RoleGrants::from_iter(role_edges.into_iter().map(|(sub, obj, caps)| RoleGrant {
            subject_role: models::Prefix::new(sub),
            object_role: models::Prefix::new(obj),
            capability: models::Capability::Admin,
            capabilities: caps,
        }))
    }

    fn assert_role_reachable(
        role_grants: &RoleGrants,
        role_or_name: &str,
        expected: Vec<(&str, Vec<models::OrthogonalCapability>)>,
    ) {
        let mut nodes: Vec<_> = RoleGrant::reachable_nodes(role_grants, role_or_name)
            .map(|n| (n.object_role.to_string(), n.capabilities))
            .collect();
        nodes.sort();
        nodes.dedup();

        let expected: Vec<(String, Vec<models::OrthogonalCapability>)> = expected
            .into_iter()
            .map(|(prefix, caps)| (prefix.to_string(), caps))
            .collect();

        assert_eq!(nodes, expected);
    }

    fn assert_role_authorized(
        role_grants: &RoleGrants,
        subject: &str,
        object: &str,
        required: Vec<models::OrthogonalCapability>,
    ) {
        assert!(
            RoleGrant::is_authorized(
                role_grants,
                subject,
                object,
                models::AnyCapability::Orthogonal(required.clone()),
            ),
            "expected {subject} to have {required:?} on {object}",
        );
    }

    fn assert_role_not_authorized(
        role_grants: &RoleGrants,
        subject: &str,
        object: &str,
        required: Vec<models::OrthogonalCapability>,
    ) {
        assert!(
            !RoleGrant::is_authorized(
                role_grants,
                subject,
                object,
                models::AnyCapability::Orthogonal(required.clone()),
            ),
            "expected {subject} NOT to have {required:?} on {object}",
        );
    }

    #[test]
    fn test_role_reachable_nodes_delegate_propagation() {
        use models::OrthogonalCapability::*;

        let rg = build_role_scenario(vec![
            ("acmeCo/", "bobCo/shared/", vec![Read, Billing, Delegate]),
            ("bobCo/shared/", "carolCo/data/", vec![Read, Delegate]),
            ("carolCo/data/", "daveCo/sink/", vec![Read, Billing]),
        ]);

        assert_role_reachable(
            &rg,
            "acmeCo/",
            vec![
                ("bobCo/shared/", vec![Read, Billing, Delegate]),
                ("carolCo/data/", vec![Read, Delegate]),
                ("daveCo/sink/", vec![Read]),
            ],
        );
    }

    #[test]
    fn test_role_reachable_nodes_no_delegate_is_terminal() {
        use models::OrthogonalCapability::*;

        let rg = build_role_scenario(vec![
            ("acmeCo/", "bobCo/shared/", vec![Read]),
            ("bobCo/shared/", "carolCo/", vec![Read]),
        ]);

        assert_role_reachable(&rg, "acmeCo/", vec![("bobCo/shared/", vec![Read])]);
    }

    #[test]
    fn test_role_assume_propagates_all_capabilities() {
        use models::OrthogonalCapability::*;

        // Assume on the first edge opens up the full capability set,
        // so the second edge's Billing passes through even though the
        // first edge doesn't carry Billing.
        let rg = build_role_scenario(vec![
            ("acmeCo/", "bobCo/", vec![Read, Assume]),
            ("bobCo/", "carolCo/", vec![Read, Billing, Delegate]),
        ]);

        assert_role_reachable(
            &rg,
            "acmeCo/",
            vec![
                ("bobCo/", vec![Read, Assume]),
                ("carolCo/", vec![Read, Billing, Delegate]),
            ],
        );

        assert_role_authorized(&rg, "acmeCo/", "carolCo/thing", vec![Read, Billing]);
    }

    #[test]
    fn test_role_is_authorized_orthogonal() {
        use models::OrthogonalCapability::*;

        let rg = build_role_scenario(vec![
            ("acmeCo/", "bobCo/shared/", vec![Read, Billing, Delegate]),
            ("bobCo/shared/", "carolCo/data/", vec![Read]),
        ]);

        assert_role_authorized(&rg, "acmeCo/", "bobCo/shared/thing", vec![Read]);
        assert_role_authorized(&rg, "acmeCo/", "bobCo/shared/thing", vec![Billing]);
        assert_role_authorized(&rg, "acmeCo/", "bobCo/shared/thing", vec![Read, Billing]);

        // carolCo/data/ is reachable but only with Read (Billing filtered by delegatable)
        assert_role_authorized(&rg, "acmeCo/", "carolCo/data/thing", vec![Read]);
        assert_role_not_authorized(&rg, "acmeCo/", "carolCo/data/thing", vec![Billing]);

        // Unreachable prefix
        assert_role_not_authorized(&rg, "acmeCo/", "unknown/thing", vec![Read]);
    }

    #[test]
    fn test_empty_orthogonal_capabilities_returns_false() {
        let (rg, ug, uid) = build_orthogonal_scenario(
            vec![("acmeCo/", vec![models::OrthogonalCapability::Read])],
            vec![],
        );

        assert_not_authorized(&rg, &ug, uid, "acmeCo/", vec![]);
    }
}
