//! Data-plane placement of newly-created specifications.
//!
//! A spec's data plane is chosen once, when it's created, and is thereafter a
//! property of its live row. This module makes that choice for every drafted
//! spec which has no live counterpart, stamping its `data_plane_id`. The
//! `validation` crate then threads the assignment into built rows and Validate
//! requests.
//!
//! Placement resolves only the data plane names it actually chooses. A storage
//! mapping may list planes that no longer exist, or which the publishing user
//! cannot read (say, a sibling team's private plane), and that must not fail
//! an unrelated publication which places nothing into them.

use std::collections::BTreeMap;

/// Stamp a data plane onto each drafted capture, collection, and
/// materialization which is being created (having a model but no live row).
/// Updates, deletions, and tests are left unplaced.
///
/// `mapping_planes` holds the ordered data-plane names of each storage mapping
/// in `live.storage_mappings`, where the first is the mapping's default.
///
/// `resolve` maps a data-plane name to its ID, or None if the plane doesn't
/// exist or the user may not read it. An explicit plane is always resolved,
/// because the user asked for it, whereas a mapping's default plane is resolved
/// only if a created spec falls back to it.
pub fn assign(
    draft: &mut tables::DraftCatalog,
    live: &tables::LiveCatalog,
    mapping_planes: &BTreeMap<models::Prefix, Vec<String>>,
    explicit_plane_name: Option<&str>,
    resolve: &dyn Fn(&str) -> Option<models::Id>,
) -> tables::Errors {
    let mut errors = tables::Errors::default();

    let explicit = match explicit_plane_name.map(|name| (name, resolve(name))) {
        None => None,
        Some((name, Some(id))) => Some((name, id)),
        Some((name, None)) => {
            errors.push(tables::Error {
                scope: tables::synthetic_scope("dataPlane", name),
                error: anyhow::anyhow!("data plane '{name}' was not found"),
            });
            return errors; // Every created spec would fail.
        }
    };
    // Resolutions of mapping defaults, so that each is resolved and reported once.
    let mut defaults: BTreeMap<&models::Prefix, Option<models::Id>> = BTreeMap::new();

    let mut place = |entity: &str, name: &str, scope: &url::Url| -> models::Id {
        // A spec with no covering mapping is left unplaced. `validation`
        // reports the missing mapping along with a did-you-mean suggestion.
        let Some(mapping) = live.storage_mappings.lookup(name) else {
            return models::Id::zero();
        };
        let prefix = &mapping.catalog_prefix;
        let planes = mapping_planes
            .get(prefix)
            .expect("mapping_planes has every storage mapping");

        if let Some((explicit_name, explicit_id)) = explicit {
            // The `ops/` mapping lists no planes because ops catalogs are
            // created into every plane, including one being created right now.
            if prefix.as_str() == "ops/" || planes.iter().any(|p| p == explicit_name) {
                return explicit_id;
            }
            errors.push(tables::Error {
                scope: scope.clone(),
                error: anyhow::anyhow!(
                    "{entity} {name} storage mapping {prefix} doesn't permit data plane {explicit_name}"
                ),
            });
            return models::Id::zero();
        }

        let Some(default_name) = planes.first() else {
            errors.push(tables::Error {
                scope: scope.clone(),
                error: anyhow::anyhow!(
                    "{entity} {name} storage mapping {prefix} is missing associated data planes"
                ),
            });
            return models::Id::zero();
        };

        let resolved = *defaults.entry(prefix).or_insert_with(|| {
            let resolved = resolve(default_name);
            if resolved.is_none() {
                errors.push(tables::Error {
                    scope: mapping.scope(),
                    error: anyhow::anyhow!("data plane '{default_name}' was not found"),
                });
            }
            resolved
        });
        resolved.unwrap_or(models::Id::zero())
    };

    for row in draft.captures.iter_mut() {
        if row.model.is_some() && live.captures.get_by_key(&row.capture).is_none() {
            row.data_plane_id = place("capture", row.capture.as_str(), &row.scope);
        }
    }
    for row in draft.collections.iter_mut() {
        if row.model.is_some() && live.collections.get_by_key(&row.collection).is_none() {
            row.data_plane_id = place("collection", row.collection.as_str(), &row.scope);
        }
    }
    for row in draft.materializations.iter_mut() {
        let name = &row.materialization;
        if row.model.is_some() && live.materializations.get_by_key(name).is_none() {
            row.data_plane_id = place("materialization", name.as_str(), &row.scope);
        }
    }

    errors
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    const PUBLIC_ONE: models::Id = models::Id::new([1; 8]);
    const PUBLIC_TWO: models::Id = models::Id::new([2; 8]);

    // Resolves the public planes, and nothing else: `ops/dp/private/other`
    // stands in for a plane which exists, but which the user may not read.
    fn resolve(name: &str) -> Option<models::Id> {
        match name {
            "ops/dp/public/one" => Some(PUBLIC_ONE),
            "ops/dp/public/two" => Some(PUBLIC_TWO),
            _ => None,
        }
    }

    /// Place a draft which creates collections `creates` and deletes
    /// `acmeCo/team-a/deleted`, alongside an update of live collection
    /// `acmeCo/team-a/live`. Returns resulting placements and errors.
    fn run(creates: &[&str], explicit: Option<&str>) -> (Vec<String>, Vec<String>) {
        let mut live = tables::LiveCatalog::default();
        let mut mapping_planes = BTreeMap::new();

        for (prefix, planes) in [
            ("acmeCo/", vec!["ops/dp/public/one", "ops/dp/public/two"]),
            ("acmeCo/team-b/", vec!["ops/dp/private/other"]),
            ("acmeCo/planeless/", vec![]),
            ("ops/", vec![]),
        ] {
            let prefix = models::Prefix::new(prefix);
            live.storage_mappings
                .insert_row(&prefix, models::Id::zero(), Vec::new(), Vec::new());
            mapping_planes.insert(prefix, planes.into_iter().map(String::from).collect());
        }

        let live_name = models::Collection::new("acmeCo/team-a/live");
        live.collections.insert_row(
            &live_name,
            models::Id::new([9; 8]),
            models::Id::new([8; 8]),
            models::Id::new([7; 8]),
            models::Id::new([7; 8]),
            models::CollectionDef::example(),
            proto_flow::flow::CollectionSpec::default(),
            None,
        );

        let mut draft = tables::DraftCatalog::default();
        let drafted = creates
            .iter()
            .map(|name| (*name, true))
            .chain([(live_name.as_str(), true), ("acmeCo/team-a/deleted", false)]);

        for (name, has_model) in drafted {
            draft.collections.insert(tables::DraftCollection {
                collection: models::Collection::new(name),
                scope: tables::synthetic_scope(models::CatalogType::Collection, name),
                expect_pub_id: None,
                data_plane_id: models::Id::zero(),
                model: has_model.then(models::CollectionDef::example),
                is_touch: false,
            });
        }

        let errors = super::assign(&mut draft, &live, &mapping_planes, explicit, &resolve);

        let placements = draft
            .collections
            .iter()
            .map(|row| format!("{} => {}", row.collection, row.data_plane_id))
            .collect();
        let errors = errors
            .iter()
            .map(|err| format!("{}: {:#}", err.scope, err.error))
            .collect();

        (placements, errors)
    }

    #[test]
    fn test_mapping_default_is_used() {
        insta::assert_debug_snapshot!(run(&["acmeCo/team-a/thing"], None), @r#"
        (
            [
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
                "acmeCo/team-a/thing => 0101010101010101",
            ],
            [],
        )
        "#);
    }

    #[test]
    fn test_explicit_plane_listed_by_mapping() {
        insta::assert_debug_snapshot!(run(&["acmeCo/team-a/thing"], Some("ops/dp/public/two")), @r#"
        (
            [
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
                "acmeCo/team-a/thing => 0202020202020202",
            ],
            [],
        )
        "#);
    }

    #[test]
    fn test_explicit_plane_not_listed_by_mapping() {
        insta::assert_debug_snapshot!(run(&["acmeCo/team-b/thing"], Some("ops/dp/public/one")), @r#"
        (
            [
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
                "acmeCo/team-b/thing => 0000000000000000",
            ],
            [
                "flow://collection/acmeCo/team-b/thing: collection acmeCo/team-b/thing storage mapping acmeCo/team-b/ doesn't permit data plane ops/dp/public/one",
            ],
        )
        "#);
    }

    #[test]
    fn test_unresolved_explicit_plane_is_an_error_even_if_unused() {
        // Nothing is created, yet the named plane is still reported.
        insta::assert_debug_snapshot!(run(&[], Some("ops/dp/private/other")), @r#"
        (
            [
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
            ],
            [
                "flow://dataPlane/ops/dp/private/other: data plane 'ops/dp/private/other' was not found",
            ],
        )
        "#);
    }

    #[test]
    fn test_ops_mapping_accepts_any_explicit_plane() {
        insta::assert_debug_snapshot!(run(&["ops/dp/public/one/logs"], Some("ops/dp/public/two")), @r#"
        (
            [
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
                "ops/dp/public/one/logs => 0202020202020202",
            ],
            [],
        )
        "#);
    }

    #[test]
    fn test_mapping_without_planes() {
        insta::assert_debug_snapshot!(run(&["acmeCo/planeless/thing"], None), @r#"
        (
            [
                "acmeCo/planeless/thing => 0000000000000000",
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
            ],
            [
                "flow://collection/acmeCo/planeless/thing: collection acmeCo/planeless/thing storage mapping acmeCo/planeless/ is missing associated data planes",
            ],
        )
        "#);
    }

    #[test]
    fn test_spec_without_mapping_is_left_to_validation() {
        insta::assert_debug_snapshot!(run(&["otherCo/thing"], None), @r#"
        (
            [
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
                "otherCo/thing => 0000000000000000",
            ],
            [],
        )
        "#);
    }

    #[test]
    fn test_unresolved_mapping_default_is_reported_once() {
        // team-a's specs are placed despite team-b's unreadable default.
        insta::assert_debug_snapshot!(run(&["acmeCo/team-a/thing", "acmeCo/team-b/one", "acmeCo/team-b/two"], None), @r#"
        (
            [
                "acmeCo/team-a/deleted => 0000000000000000",
                "acmeCo/team-a/live => 0000000000000000",
                "acmeCo/team-a/thing => 0101010101010101",
                "acmeCo/team-b/one => 0000000000000000",
                "acmeCo/team-b/two => 0000000000000000",
            ],
            [
                "flow://storageMapping/acmeCo/team-b/: data plane 'ops/dp/private/other' was not found",
            ],
        )
        "#);
    }
}
