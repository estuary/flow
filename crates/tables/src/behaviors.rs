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
    /// Given a task name, enumerate all roles and capabilities granted to the task.
    pub fn transitive_roles<'a>(
        role_grants: &'a [Self],
        task_name: &'a str,
    ) -> impl Iterator<Item = super::RoleGrantRef<'a>> + 'a {
        let seed = super::RoleGrantRef {
            subject_role: "",
            object_role: task_name,
            capability: models::Capability::Admin,
        };
        pathfinding::directed::bfs::bfs_reach(seed, |f| Self::edges(role_grants, *f)).skip(1)
    }

    /// Given a task name, determine if it's authorized to the object name for the given capability.
    pub fn is_authorized<'a>(
        role_grants: &'a [Self],
        task_name: &'a str,
        object_name: &'a str,
        capability: models::Capability,
    ) -> bool {
        Self::transitive_roles(role_grants, task_name).any(|role_grant| {
            object_name.starts_with(role_grant.object_role) && role_grant.capability >= capability
        })
    }

    /// Cheaply convert a &RoleGrant into an owned type which holds borrows.
    pub fn to_ref<'a>(&'a self) -> super::RoleGrantRef<'a> {
        super::RoleGrantRef {
            subject_role: self.subject_role.as_str(),
            object_role: self.object_role.as_str(),
            capability: self.capability,
        }
    }

    fn edges<'a>(
        role_grants: &'a [Self],
        from: super::RoleGrantRef<'a>,
    ) -> impl Iterator<Item = super::RoleGrantRef<'a>> + 'a {
        // Split the source object role into its prefixes:
        // "acmeCo/one/two/three" => ["acmeCo/one/two/", "acmeCo/one/", "acmeCo/"].
        let prefixes = from.object_role.char_indices().filter_map(|(ind, chr)| {
            if chr == '/' {
                Some(&from.object_role[..ind + 1])
            } else {
                None
            }
        });

        // For each prefix, find all `role_grants` where it's the `subject_role`.
        let edges = prefixes
            .map(|prefix| {
                role_grants
                    .equal_range_by(|role_grant| role_grant.subject_role.as_str().cmp(prefix))
            })
            .map(|range| role_grants[range].into_iter().map(Self::to_ref))
            .flatten();

        // Only 'admin' grants are walked transitively.
        if from.capability >= models::Capability::Admin {
            Some(edges)
        } else {
            None
        }
        .into_iter()
        .flatten()
    }
}

impl super::StorageMapping {
    pub fn scope(&self) -> url::Url {
        crate::synthetic_scope("storageMapping", &self.catalog_prefix)
    }
}

#[cfg(test)]
mod test {
    use crate::{Import, Imports, RoleGrant, RoleGrants};

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
            "daveCo/hidden/thing",
            "carolCo/even/more/hidden/thing",
            Read
        ));
        assert!(!RoleGrant::is_authorized(
            &role_grants,
            "daveCo/hidden/thing",
            "carolCo/even/more/hidden/thing",
            Write
        ));
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
