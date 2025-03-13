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

    /// Given a role or name, determine if it's authorized to the object name for the given capability.
    pub fn is_authorized<'a>(
        role_grants: &'a [super::RoleGrant],
        subject_role_or_name: &'a str,
        object_role_or_name: &'a str,
        capability: models::Capability,
    ) -> bool {
        Self::transitive_roles(role_grants, subject_role_or_name).any(|role_grant| {
            object_role_or_name.starts_with(role_grant.object_role)
                && role_grant.capability >= capability
        })
    }

    fn to_ref<'a>(&'a self) -> super::GrantRef<'a> {
        super::GrantRef {
            subject_role: self.subject_role.as_str(),
            object_role: self.object_role.as_str(),
            capability: self.capability,
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

    /// Given a user, determine if they're authorized to the object name for the given capability.
    pub fn is_authorized<'a>(
        role_grants: &'a [super::RoleGrant],
        user_grants: &'a [super::UserGrant],
        subject_user_id: uuid::Uuid,
        object_role_or_name: &'a str,
        capability: models::Capability,
    ) -> bool {
        Self::transitive_roles(role_grants, user_grants, subject_user_id).any(|role_grant| {
            object_role_or_name.starts_with(role_grant.object_role)
                && role_grant.capability >= capability
        })
    }

    fn to_ref<'a>(&'a self) -> super::GrantRef<'a> {
        super::GrantRef {
            subject_role: "",
            object_role: self.object_role.as_str(),
            capability: self.capability,
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
            }),
        );

        insta::assert_json_snapshot!(
            RoleGrant::transitive_roles(&role_grants, "aliceCo/anvils/thing").collect::<Vec<_>>(),
            @r###"
        [
          {
            "subject_role": "aliceCo/anvils/",
            "object_role": "carolCo/paper/",
            "capability": "write"
          }
        ]
        "###,
        );

        insta::assert_json_snapshot!(
            RoleGrant::transitive_roles(&role_grants, "daveCo/hidden/task").collect::<Vec<_>>(),
            @r###"
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
        "###,
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
            @r###"
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
        "###,
        );

        insta::assert_json_snapshot!(
            UserGrant::transitive_roles(&role_grants, &user_grants, uuid::Uuid::max()).collect::<Vec<_>>(),
            @r###"
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
        "###,
        );
    }

    #[test]
    fn test_transitive_roles_more() {
        let role_grants: Vec<crate::RoleGrant> = serde_json::from_value(serde_json::json!([
          {
            "subject_role": "acmeCo/",
            "object_role": "acmeCo/",
            "capability": "write"
          },
          {
            "subject_role": "other_tenant/",
            "object_role": "acmeCo/",
            "capability": "admin"
          },
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "acmeCo-૨/",
            "capability": "write"
          },
          {
            "subject_role": "other_tenant/",
            "object_role": "acmeCo-૨/",
            "capability": "admin"
          },
          {
            "subject_role": "acmeCo-૨/ssss/",
            "object_role": "acmeCo-૨/",
            "capability": "admin"
          },
          {
            "subject_role": "acmeCo-૨/aaaa/",
            "object_role": "acmeCo-૨/",
            "capability": "admin"
          },
          {
            "subject_role": "acmeCo-૨/dddd/",
            "object_role": "acmeCo-૨/",
            "capability": "admin"
          },
          {
            "subject_role": "acmeCo-૨/",
            "object_role": "ops/dp/public/",
            "capability": "read"
          },
          {
            "subject_role": "acmeCo/",
            "object_role": "ops/dp/public/",
            "capability": "read"
          }
        ]))
        .unwrap();
        let role_grants = crate::RoleGrants::from_iter(role_grants);

        insta::assert_json_snapshot!(
            RoleGrant::transitive_roles(&role_grants, "acmeCo-૨/acme-prod-tables/materialize-snowflake").collect::<Vec<_>>(),
            @r###"
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
        "###,
        );

        assert!(crate::RoleGrant::is_authorized(
            &role_grants,
            "acmeCo-૨/acme-prod-tables/materialize-snowflake",
            "acmeCo-૨/acme-data/my_data/",
            models::Capability::Read
        ));
    }
}
