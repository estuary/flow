use crate::{DraftCatalog, DraftRow, LiveCatalog, LiveRow};
use models::ModelDef;
use std::collections::BTreeMap;

/// Used to compute the dependency hash of each built specification. We use this struct instead of
/// passing around `Built_` tables because it allows the built tables to be constructed
/// concurrently, and because this struct can also be used in scenarios outside of validation.
/// Internally, this uses a sparse map from catalog name to `last_pub_id`. Any spec that _isn't_
/// contained in the map is assumed to have a `last_pub_id` of `default_pub_id`. For computing
/// hashes during publication, the `default_pub_id` is set to the current `pub_id`, and drafted
/// specs are removed from the map. Outside of publications, the `default_pub_id` is left zeroed.
pub struct Dependencies<'a> {
    default_pub_id: models::Id,
    by_catalog_name: BTreeMap<&'a str, models::Id>,
}

impl<'a> Dependencies<'a> {
    pub fn from_live(live: &'a LiveCatalog) -> Dependencies<'a> {
        let mut map = BTreeMap::new();
        for r in live.captures.iter() {
            map.insert(r.catalog_name().as_str(), r.last_pub_id());
        }
        for r in live.collections.iter() {
            map.insert(r.catalog_name().as_str(), r.last_pub_id());
        }
        for r in live.materializations.iter() {
            map.insert(r.catalog_name().as_str(), r.last_pub_id());
        }
        for r in live.tests.iter() {
            map.insert(r.catalog_name().as_str(), r.last_pub_id());
        }
        Dependencies {
            default_pub_id: models::Id::zero(),
            by_catalog_name: map,
        }
    }

    /// Returns a `Dependencies` for computing hashes for specs that are
    /// included in the current publication. Note that this function assumes
    /// that any drafted specs with `is_touch: true` will not have their
    /// `last_pub_id` changed by this publication, which is not always true.
    /// Validations may change a drafted touch to a non-touch if any model fixes
    /// are applied, and this will cause the dependency hash to be incorrect for
    /// any specs that depend on the now non-touched spec. This is unfortunate,
    /// but not necessarily a problem we need to worry about, since controllers
    /// will simply re-publish the affected specs, which corrects the dependency
    /// hash.
    pub fn of_publication(
        pub_id: models::Id,
        draft: &'a DraftCatalog,
        live: &'a LiveCatalog,
    ) -> Dependencies<'a> {
        let mut deps = Dependencies::from_live(live);
        // When publishing, use a max valued id as the placeholder for
        // dependencies that don't exist. The specific value is not significant.
        // The important thing is that this placeholder is different than the
        // placeholder that's used when computing hashes in other contexts. This
        // ensures that the dependency hash that's computed by controllers will
        // differ from the hash computed during the publication process in one
        // specific case: when a materialization depends on a source capture
        // that has been deleted. In this case, we want controllers to see a
        // change in the dependency hash, which will cause them to re-publish
        // the materialization and remove the source capture.
        deps.default_pub_id = models::Id::new([255u8; 8]);
        let drafted_specs = draft
            .captures
            .iter()
            .filter(|r| !r.is_touch)
            .map(|r| r.catalog_name().as_str())
            .chain(
                draft
                    .collections
                    .iter()
                    .filter(|r| !r.is_touch)
                    .map(|r| r.catalog_name().as_str()),
            )
            .chain(
                draft
                    .materializations
                    .iter()
                    .filter(|r| !r.is_touch)
                    .map(|r| r.catalog_name().as_str()),
            )
            .chain(
                draft
                    .tests
                    .iter()
                    .filter(|r| !r.is_touch)
                    .map(|r| r.catalog_name().as_str()),
            );
        for name in drafted_specs {
            if let Some(id) = deps.by_catalog_name.get_mut(name) {
                *id = pub_id;
            } else {
                deps.by_catalog_name.insert(name, pub_id);
            }
        }
        deps
    }

    fn get_pub_id(&self, dependency_name: &str) -> models::Id {
        self.by_catalog_name
            .get(dependency_name)
            .copied()
            .unwrap_or(self.default_pub_id)
    }

    pub fn compute_hash<M: ModelDef>(&self, model: &M) -> Option<String> {
        use xxhash_rust::xxh3::Xxh3;

        // TODO: This function can probably be cleaned up if we introduced a function like:
        // `ModelDef::get_dependencies(&self) -> impl Iterator<Item=(&str, FlowType)>`

        let mut deps = Vec::with_capacity(64);

        for source in model.sources() {
            deps.push(source.collection().as_str());
        }

        for target in model.targets() {
            deps.push(target.as_str());
        }

        let maybe_source_cap = model.materialization_source_capture_name();
        if let Some(source_cap) = maybe_source_cap.as_ref() {
            deps.push(source_cap.as_str());
        }

        if deps.is_empty() {
            return None;
        }

        deps.sort();
        deps.dedup();

        let mut hasher = Xxh3::new();
        for name in deps {
            hasher.update(name.as_bytes());
            let last_pub = self.get_pub_id(name);
            hasher.update(&last_pub.as_array());
        }
        Some(format!("{:x}", hasher.digest()))
    }
}

#[cfg(test)]
mod test {
    use proto_flow::flow::CollectionSpec;

    use super::*;
    use crate::{DraftCollection, LiveCapture, LiveCollection, LiveMaterialization};

    fn id(i: u8) -> models::Id {
        models::Id::new([i, 0, 0, 0, 0, 0, 0, 0])
    }

    fn live_catalog() -> LiveCatalog {
        let mut live = crate::LiveCatalog::default();
        let zero = models::Id::zero();

        live.collections.insert(LiveCollection {
            collection: models::Collection::new("test/c1"),
            control_id: zero,
            data_plane_id: zero,
            last_pub_id: id(1),
            last_build_id: zero,
            model: models::CollectionDef::example(),
            spec: CollectionSpec::default(),
            dependency_hash: None,
        });
        live.collections.insert(LiveCollection {
            collection: models::Collection::new("test/c2"),
            control_id: zero,
            data_plane_id: zero,
            last_pub_id: id(2),
            last_build_id: zero,
            model: models::CollectionDef::example(),
            spec: CollectionSpec::default(),
            dependency_hash: None,
        });

        live.captures.insert(LiveCapture {
            capture: models::Capture::new("test/capture"),
            control_id: zero,
            data_plane_id: zero,
            last_pub_id: id(1),
            last_build_id: zero,
            model: models::CaptureDef::example(),
            spec: Default::default(),
            dependency_hash: Some("abc123".to_owned()),
        });
        live.materializations.insert(LiveMaterialization {
            materialization: models::Materialization::new("test/materialize"),
            control_id: zero,
            data_plane_id: zero,
            last_pub_id: zero,
            last_build_id: zero,
            model: models::MaterializationDef::example(),
            spec: Default::default(),
            dependency_hash: Some("cba321".to_owned()),
        });
        live
    }

    #[test]
    fn dependencies_computes_consistent_hash_values() {
        let live = live_catalog();
        let subject = Dependencies::from_live(&live);
        assert_eq!(id(2), subject.get_pub_id("test/c2"));

        assert_hash::<models::CaptureDef>(
            Some("d2c1bbb1be32b48c"),
            &subject,
            serde_json::json!({
                "endpoint": {
                    "connector": {
                        "image": "test/image:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "r": "1" },
                        "target": "test/c1"
                    },
                    {
                        "resource": { "r": "2" },
                        "target": "test/c2"
                    }
                ]
            }),
        );
        assert_hash::<models::MaterializationDef>(
            Some("7b850cd4f60163b0"),
            &subject,
            serde_json::json!({
                "sourceCapture": "test/capture",
                "endpoint": {
                    "connector": {
                        "image": "test/image:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "r": "2" },
                        "source": "test/c2"
                    },
                    {
                        "resource": { "r": "1" },
                        "source": "test/c1"
                    },
                ]
            }),
        );
        // Changing the order of the bindings should not affect the hash
        assert_hash::<models::MaterializationDef>(
            Some("7b850cd4f60163b0"),
            &subject,
            serde_json::json!({
                "sourceCapture": "test/capture",
                "endpoint": {
                    "connector": {
                        "image": "test/image:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "r": "1" },
                        "source": "test/c1"
                    },
                    {
                        "resource": { "r": "2" },
                        "source": "test/c2"
                    },
                ]
            }),
        );

        // Removing the sourceCapture should change the hash
        assert_hash::<models::MaterializationDef>(
            Some("d2c1bbb1be32b48c"),
            &subject,
            serde_json::json!({
                "endpoint": {
                    "connector": {
                        "image": "test/image:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "r": "1" },
                        "source": "test/c1"
                    },
                    {
                        "resource": { "r": "2" },
                        "source": "test/c2"
                    },
                ]
            }),
        );

        // Compute the change as it would be done during the publication, and assert that we get the same value.
        let mut draft = crate::DraftCatalog::default();
        draft.collections.insert(DraftCollection {
            collection: models::Collection::new("test/c2"),
            scope: crate::synthetic_scope(models::CatalogType::Collection, "test/c2"),
            expect_pub_id: None,
            model: Some(models::CollectionDef::example()),
            is_touch: false,
        });

        let subject = Dependencies::of_publication(id(2), &draft, &live);
        assert_eq!(id(2), subject.get_pub_id("test/c2"));
        assert_eq!(id(1), subject.get_pub_id("test/c1"));
        assert_hash::<models::MaterializationDef>(
            Some("7b850cd4f60163b0"),
            &subject,
            serde_json::json!({
                "sourceCapture": "test/capture",
                "endpoint": {
                    "connector": {
                        "image": "test/image:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "r": "1" },
                        "source": "test/c1"
                    },
                    {
                        "resource": { "r": "2" },
                        "source": "test/c2"
                    },
                ]
            }),
        );

        // Simulate publishing at id 3, which should result in a different hash
        let subject = Dependencies::of_publication(id(3), &draft, &live);
        assert_eq!(id(3), subject.get_pub_id("test/c2"));
        assert_eq!(
            models::Id::new([255u8; 8]),
            subject.get_pub_id("test/not-in-publication")
        );
        assert_eq!(id(1), subject.get_pub_id("test/c1"));
        assert_hash::<models::MaterializationDef>(
            Some("e94b0ce2e25aa96a"),
            &subject,
            serde_json::json!({
                "sourceCapture": "test/capture",
                "endpoint": {
                    "connector": {
                        "image": "test/image:test",
                        "config": {}
                    }
                },
                "bindings": [
                    {
                        "resource": { "r": "1" },
                        "source": "test/c1"
                    },
                    {
                        "resource": { "r": "2" },
                        "source": "test/c2"
                    },
                ]
            }),
        );
    }

    fn assert_hash<M: ModelDef>(
        expected: Option<&str>,
        deps: &Dependencies,
        model_json: serde_json::Value,
    ) {
        let model: M = serde_json::from_value(model_json).expect("failed to parse model json");
        let actual = deps.compute_hash(&model);
        assert_eq!(expected, actual.as_deref());
    }
}
