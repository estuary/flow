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

    pub fn of_publication(
        pub_id: models::Id,
        draft: &'a DraftCatalog,
        live: &'a LiveCatalog,
    ) -> Dependencies<'a> {
        let mut deps = Dependencies::from_live(live);
        // Use the current pub_id as the default, and remove any specs that are being modified by the current publication.
        deps.default_pub_id = pub_id;
        for r in draft.captures.iter().filter(|r| !r.is_touch) {
            deps.by_catalog_name.remove(r.catalog_name().as_str());
        }
        for r in draft.collections.iter().filter(|r| !r.is_touch) {
            deps.by_catalog_name.remove(r.catalog_name().as_str());
        }
        for r in draft.materializations.iter().filter(|r| !r.is_touch) {
            deps.by_catalog_name.remove(r.catalog_name().as_str());
        }
        for r in draft.tests.iter().filter(|r| !r.is_touch) {
            deps.by_catalog_name.remove(r.catalog_name().as_str());
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

        let maybe_source_cap = model.materialization_source_capture();
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
        assert_eq!(id(3), subject.get_pub_id("test/d2"));
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
