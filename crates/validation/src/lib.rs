use itertools::Itertools;
use json::Scope;
use tables::EitherOrBoth as EOB;

mod capture;
pub mod collection;
mod data_plane;
mod derivation;
mod errors;
pub mod field_selection;
mod indexed;
mod materialization;
mod noop;
mod reference;
mod schema;
mod storage_mapping;
mod test_step;

pub use errors::Error;
pub use noop::NoOpConnectors;

/// Connectors is a delegated trait -- provided to validate -- through which
/// connector validation RPCs are dispatched. Request and Response must always
/// be Validate / Validated variants, but may include `internal` fields.
pub trait Connectors: Send + Sync {
    fn capture<'a, R>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Capture,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<proto_flow::capture::Response>> + Send + 'a
    where
        R: futures::Stream<Item = proto_flow::capture::Request> + Send + Unpin + 'static;

    fn derive<'a, R>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Collection,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<proto_flow::derive::Response>> + Send + 'a
    where
        R: futures::Stream<Item = proto_flow::derive::Request> + Send + Unpin + 'static;

    fn materialize<'a, R>(
        &'a self,
        data_plane: &'a tables::DataPlane,
        task: &'a models::Materialization,
        request_rx: R,
    ) -> impl futures::Stream<Item = anyhow::Result<proto_flow::materialize::Response>> + Send + 'a
    where
        R: futures::Stream<Item = proto_flow::materialize::Request> + Send + Unpin + 'static;
}

pub async fn validate<C: Connectors>(
    pub_id: models::Id,
    build_id: models::Id,
    project_root: &url::Url,
    connectors: &C,
    explicit_plane_name: Option<&str>,
    draft: &tables::DraftCatalog,
    live: &tables::LiveCatalog,
    fail_fast: bool,
    noop_captures: bool,
    noop_derivations: bool,
    noop_materializations: bool,
    init_vector: &[u8],
) -> tables::Validations {
    let mut errors = tables::Errors::new();

    let explicit_plane =
        match explicit_plane_name.map(|n| (n, data_plane::find_by_name(&live.data_planes, n))) {
            Some((_, Ok(plane))) => Some(plane),
            Some((name, Err(Some(suggest)))) => {
                Error::NoSuchEntitySuggest {
                    this_entity: "build",
                    this_name: "parameter".to_string(),
                    ref_entity: "data plane",
                    ref_name: name.to_string(),
                    suggest_name: suggest.to_string(),
                }
                .push(Scope::new(&project_root), &mut errors);
                None
            }
            Some((name, Err(None))) => {
                Error::NoSuchEntity {
                    this_entity: "build",
                    this_name: "parameter".to_string(),
                    ref_entity: "data plane",
                    ref_name: name.to_string(),
                }
                .push(Scope::new(&project_root), &mut errors);
                None
            }
            None => None,
        };

    storage_mapping::walk_all_storage_mappings(&live.storage_mappings, &mut errors);

    // Build all local collections.
    let mut built_collections = collection::walk_all_collections(
        pub_id,
        build_id,
        &draft.collections,
        &live.inferred_schemas,
        &live.collections,
        &live.data_planes,
        explicit_plane,
        &live.storage_mappings,
        &mut errors,
    );

    // If we failed to build one or more collections then further validation
    // will generate lots of misleading "not found" errors.
    if fail_fast && !errors.is_empty() {
        return tables::Validations {
            built_captures: tables::BuiltCaptures::new(),
            built_collections,
            built_materializations: tables::BuiltMaterializations::new(),
            built_tests: tables::BuiltTests::new(),
            errors,
        };
    }

    let dependencies = tables::Dependencies::of_publication(pub_id, draft, live);

    let built_tests = test_step::walk_all_tests(
        pub_id,
        build_id,
        &draft.tests,
        &live.tests,
        &built_collections,
        &live.data_planes,
        &dependencies,
        &live.storage_mappings,
        &mut errors,
    );

    // Validating tests is fast, and encountered errors are likely to impact
    // task validations (which are slower).
    if fail_fast && !errors.is_empty() {
        return tables::Validations {
            built_captures: tables::BuiltCaptures::new(),
            built_collections,
            built_materializations: tables::BuiltMaterializations::new(),
            built_tests,
            errors,
        };
    }

    // Task validations can run concurrently but require connector call-outs.

    let mut capture_errors = tables::Errors::new();
    let built_captures = capture::walk_all_captures(
        pub_id,
        build_id,
        &draft.captures,
        &live.captures,
        &built_collections,
        connectors,
        &live.data_planes,
        explicit_plane,
        &dependencies,
        noop_captures,
        &live.storage_mappings,
        init_vector,
        &mut capture_errors,
    );

    let mut derive_errors = tables::Errors::new();
    let built_derivations = derivation::walk_all_derivations(
        pub_id,
        build_id,
        &draft.collections,
        &live.collections,
        &built_collections,
        connectors,
        &live.data_planes,
        &dependencies,
        &draft.imports,
        noop_derivations,
        project_root,
        &live.storage_mappings,
        init_vector,
        &mut derive_errors,
    );

    let mut materialize_errors = tables::Errors::new();
    let built_materializations = materialization::walk_all_materializations(
        pub_id,
        build_id,
        &draft.materializations,
        &live.materializations,
        &built_collections,
        connectors,
        &live.data_planes,
        explicit_plane,
        &dependencies,
        noop_materializations,
        &live.storage_mappings,
        &mut materialize_errors,
    );

    // Concurrently validate all tasks.
    let (built_captures, built_derivations, built_materializations) =
        futures::join!(built_captures, built_derivations, built_materializations);

    errors.extend(capture_errors.into_iter());
    errors.extend(derive_errors.into_iter());
    errors.extend(materialize_errors.into_iter());

    // Attach all built derivations to the corresponding collections.
    for (built_index, model, validated, derivation, dependency_hash, model_fixes) in
        built_derivations
    {
        let row = &mut built_collections[built_index];
        row.dependency_hash = dependency_hash;
        row.is_touch = row.is_touch && model_fixes.is_empty();
        row.model.as_mut().unwrap().derive = Some(model);
        row.model_fixes.extend(model_fixes.into_iter());
        row.spec.as_mut().unwrap().derivation = Some(derivation);
        row.validated = Some(validated);
    }

    // Look for name collisions among all top-level catalog entities.
    let collections_it = built_collections
        .iter()
        .map(|c| ("collection", c.collection.as_str(), Scope::new(&c.scope)));
    let captures_it = built_captures
        .iter()
        .map(|c| ("capture", c.capture.as_str(), Scope::new(&c.scope)));
    let materializations_it = built_materializations.iter().map(|m| {
        (
            "materialization",
            m.materialization.as_str(),
            Scope::new(&m.scope),
        )
    });
    let tests_it = built_tests
        .iter()
        .map(|t| ("test", t.test.as_str(), Scope::new(&t.scope)));

    indexed::walk_duplicates(
        captures_it
            .chain(collections_it)
            .chain(materializations_it)
            .chain(tests_it),
        &mut errors,
    );

    tables::Validations {
        built_captures,
        built_collections,
        built_materializations,
        built_tests,
        errors,
    }
}

fn walk_prefix<'a>(
    scope: Scope<'a>,
    entity: &'static str,
    name: &str,
    data_planes: &'a tables::DataPlanes,
    explicit_plane: Option<&'a tables::DataPlane>,
    storage_mappings: &'a [tables::StorageMapping],
    errors: &mut tables::Errors,
) -> Option<(
    &'a [models::Store],   // Partition stores.
    &'a [models::Store],   // Recovery stores.
    &'a tables::DataPlane, // Data-plane for task initialization.
)> {
    let partition = match storage_mapping::lookup_mapping(entity, name, storage_mappings) {
        Ok(m) => m,
        Err(err) => {
            err.push(scope, errors);
            return None; // Cannot continue.
        }
    };
    let recovery = match storage_mapping::lookup_mapping(
        entity,
        &format!("recovery/{name}"),
        storage_mappings,
    ) {
        Ok(m) => m,
        Err(err) => {
            err.push(scope, errors);
            return None; // Cannot continue.
        }
    };

    // Require that `partition` and `recovery` mapping prefixes match one another.
    // This is not absolutely required, but a) is true today and b) holds open
    // a future refactor that composes partition stores, recovery stores,
    // and data-planes into a single "Prefix" concept.
    if Some(partition.catalog_prefix.as_str()) == recovery.catalog_prefix.strip_prefix("recovery/")
    {
        // OK: recovery prefix is "recovery/" + partition prefix.
    } else if partition.catalog_prefix.is_empty() && recovery.catalog_prefix.is_empty() {
        // OK: support for test & flowctl cases using NoOpCatalogResolver.
    } else {
        Error::StorageMappingPrefixMismatch {
            entity,
            name: name.to_string(),
            partition_mapping: partition.catalog_prefix.clone(),
            recovery_mapping: recovery.catalog_prefix.clone(),
        }
        .push(scope, errors);
    }

    // Similarly, require that data-planes of `partition` and `recovery` align.
    if !recovery.data_planes.is_empty() && recovery.data_planes != partition.data_planes {
        Error::StorageMappingDataPlanesMismatch {
            entity,
            name: name.to_string(),
            partition_mapping: partition.catalog_prefix.clone(),
            partition_planes: partition.data_planes.iter().cloned().collect(),
            recovery_planes: recovery.data_planes.iter().cloned().collect(),
        }
        .push(scope, errors);
    }

    // Determine the data plane into which `name` should be initialized.
    let init_data_plane = if let Some(explicit_plane) = explicit_plane {
        if !partition
            .data_planes
            .contains(&explicit_plane.data_plane_name)
        {
            Error::DataPlaneNotInStorageMapping {
                entity,
                name: name.to_string(),
                partition_mapping: partition.catalog_prefix.clone(),
                data_plane: explicit_plane.data_plane_name.clone(),
                listed_data_planes: partition.data_planes.iter().cloned().collect(),
            }
            .push(scope, errors);
        }
        explicit_plane
    } else if let Some(default_plane_name) = partition.data_planes.first() {
        // Default to using the first data-plane attached to the storage mapping.
        // Yes, it's weird that mappings have data planes.
        // This too is holding the door open for a "Prefix" concept.
        match data_plane::find_by_name(data_planes, &default_plane_name) {
            Ok(plane) => plane,
            Err(Some(suggest)) => {
                Error::NoSuchEntitySuggest {
                    this_entity: "storage mapping",
                    this_name: partition.catalog_prefix.to_string(),
                    ref_entity: "data plane",
                    ref_name: default_plane_name.to_string(),
                    suggest_name: suggest.to_string(),
                }
                .push(scope, errors);
                return None; // Cannot continue.
            }
            Err(None) => {
                Error::NoSuchEntity {
                    this_entity: "storage mapping",
                    this_name: partition.catalog_prefix.to_string(),
                    ref_entity: "data plane",
                    ref_name: default_plane_name.to_string(),
                }
                .push(scope, errors);
                return None; // Cannot continue.
            }
        }
    } else {
        // Admissible data-planes must be attached to every storage mapping.
        Error::StorageMappingMissingDataPlanes {
            entity,
            name: name.to_string(),
            partition_mapping: partition.catalog_prefix.clone(),
        }
        .push(scope, errors);
        return None; // Cannot continue.
    };

    Some((&partition.stores, &recovery.stores, init_data_plane))
}

fn walk_transition<'a, D, L, B>(
    pub_id: models::Id,
    build_id: models::Id,
    entity: &'static str,
    explicit_plane: Option<&'a tables::DataPlane>,
    eob: EOB<&'a L, &'a D>,
    data_planes: &'a tables::DataPlanes,
    storage_mappings: &'a tables::StorageMappings,
    errors: &mut tables::Errors,
) -> Result<
    // Result::Ok continues validation of this specification.
    (
        &'a D::Key,               // Catalog name.
        &'a url::Url,             // Scope.
        D::ModelDef,              // Model to validate.
        models::Id,               // Live control-plane ID.
        &'a tables::DataPlane,    // Assigned data-plane.
        &'a [models::Store],      // Partition stores.
        &'a [models::Store],      // Recovery stores.
        models::Id,               // Live publication ID.
        models::Id,               // Live last build ID.
        Option<&'a L::ModelDef>,  // Live model.
        Option<&'a L::BuiltSpec>, // Live built spec.
        bool,                     // Is this a touch operation?
    ),
    // Result::Err is a completed BuiltRow for this specification.
    Option<B>,
>
where
    D: tables::DraftRow,
    L: tables::LiveRow<Key = D::Key, ModelDef = D::ModelDef>,
    B: tables::BuiltRow<Key = D::Key, ModelDef = D::ModelDef, BuiltSpec = L::BuiltSpec>,
    D::Key: AsRef<str>,
{
    match eob {
        EOB::Left(live) => {
            if live.last_build_id() > build_id {
                Error::BuildSuperseded {
                    build_id,
                    larger_id: live.last_build_id(),
                }
                .push(Scope::new(&live.scope()), errors);
            }

            Err(Some(B::new(
                live.catalog_name().clone(),
                live.scope(),
                live.control_id(),
                live.data_plane_id().unwrap_or(models::Id::zero()),
                live.last_pub_id(),
                live.last_build_id(),
                Some(live.model().clone()),
                Vec::new(),
                None,
                Some(live.spec().clone()),
                None,
                false, // !is_touch
                live.dependency_hash().map(|h| h.to_owned()),
            )))
        }
        EOB::Right(draft) => {
            if let Some(expect_id) = draft.expect_pub_id() {
                if expect_id != models::Id::zero() {
                    Error::ExpectPubIdNotMatched {
                        expect_id,
                        actual_id: models::Id::zero(),
                    }
                    .push(Scope::new(draft.scope()), errors);
                }
            }
            if draft.is_touch() {
                Error::TouchModelIsCreate.push(Scope::new(draft.scope()), errors);
            }

            let Some(model) = draft.model() else {
                // This is a deletion but there's no matched live specification.
                Error::DeletedSpecDoesNotExist.push(Scope::new(draft.scope()), errors);
                return Err(None);
            };

            let Some((partition_stores, recovery_stores, assigned_data_plane)) = walk_prefix(
                Scope::new(draft.scope()),
                entity,
                draft.catalog_name().as_ref(),
                data_planes,
                explicit_plane,
                storage_mappings,
                errors,
            ) else {
                return Err(None); // Cannot continue without mapped stores.
            };

            Ok((
                draft.catalog_name(),
                draft.scope(),
                model.clone(),
                models::Id::zero(),  // Has no control-plane ID.
                assigned_data_plane, // Assign default data-plane.
                partition_stores,
                recovery_stores,
                models::Id::zero(), // Never published.
                models::Id::zero(), // Never built.
                None,               // Has no live model.
                None,               // Has no live built spec.
                false,              // !is_touch
            ))
        }
        EOB::Both(live, draft) => {
            match draft.expect_pub_id() {
                Some(expect_id) if expect_id != live.last_pub_id() => {
                    Error::ExpectPubIdNotMatched {
                        expect_id,
                        actual_id: live.last_pub_id(),
                    }
                    .push(Scope::new(draft.scope()), errors);
                }
                _ => (),
            }
            if pub_id < live.last_pub_id() {
                Error::PublicationSuperseded {
                    pub_id,
                    last_pub_id: live.last_pub_id(),
                }
                .push(Scope::new(draft.scope()), errors);
            } else if !draft.is_touch() && pub_id == live.last_pub_id() {
                // Only touch publications are allowed to publish at the same id.
                Error::PubIdNotIncreased {
                    pub_id,
                    last_pub_id: live.last_pub_id(),
                }
                .push(Scope::new(draft.scope()), errors);
            } else if live.last_build_id() > build_id {
                Error::BuildSuperseded {
                    build_id,
                    larger_id: live.last_build_id(),
                }
                .push(Scope::new(draft.scope()), errors);
            }

            let Some(model) = draft.model() else {
                // Catalog specification is being deleted.
                if draft.is_touch() {
                    Error::TouchModelIsDelete.push(Scope::new(draft.scope()), errors);
                }
                return Err(Some(B::new(
                    draft.catalog_name().clone(),
                    draft.scope().clone(),
                    live.control_id(),
                    live.data_plane_id().unwrap_or(models::Id::zero()),
                    live.last_pub_id(),
                    live.last_build_id(),
                    None, // Deletion has no draft model.
                    Vec::new(),
                    None, // Deletion is not validated.
                    None, // Deletion is not built into a spec.
                    Some(live.spec().clone()),
                    false, // !is_touch
                    live.dependency_hash().map(|h| h.to_owned()),
                )));
            };

            if draft.is_touch() && model != live.model() {
                Error::TouchModelIsNotEqual.push(Scope::new(draft.scope()), errors);
            }

            let Some((partition_stores, recovery_stores, init_data_plane)) = walk_prefix(
                Scope::new(draft.scope()),
                entity,
                draft.catalog_name().as_ref(),
                data_planes,
                explicit_plane,
                storage_mappings,
                errors,
            ) else {
                return Err(None); // Cannot continue without mapped stores.
            };

            // For entities with a data plane (captures, collections, materializations),
            // use the assigned data plane. For tests, use the initialization data plane.
            let data_plane = if let Some(data_plane_id) = live.data_plane_id() {
                if let Some(data_plane) = data_planes.get_by_key(&data_plane_id) {
                    data_plane
                } else {
                    Error::MissingDataPlaneId {
                        this_entity: entity,
                        this_name: draft.catalog_name().as_ref().to_string(),
                        data_plane_id,
                    }
                    .push(Scope::new(draft.scope()), errors);

                    init_data_plane
                }
            } else {
                init_data_plane
            };

            Ok((
                draft.catalog_name(),
                draft.scope(),
                model.clone(),
                live.control_id(),
                data_plane,
                partition_stores,
                recovery_stores,
                live.last_pub_id(),
                live.last_build_id(),
                Some(live.model()),
                Some(live.spec()),
                draft.is_touch(),
            ))
        }
    }
}

// Load the resource path encoded in /_meta/path, or return an empty Vec
// if there is no such location, or it's not an array of strings.
pub fn load_resource_meta_path(resource_config_json: &[u8]) -> Vec<String> {
    #[derive(serde::Deserialize)]
    struct Meta {
        path: Vec<String>,
    }
    #[derive(serde::Deserialize)]
    struct Skim {
        #[serde(rename = "_meta")]
        meta: Option<Meta>,
    }

    if let Ok(Skim {
        meta: Some(Meta { path }),
    }) = serde_json::from_slice::<Skim>(resource_config_json)
    {
        path
    } else {
        Vec::new()
    }
}

// Store `resource_path` under /_meta/path of `resource`, returning an updated clone.
fn store_resource_meta(resource: &models::RawValue, path: &[String]) -> models::RawValue {
    type Skim = std::collections::BTreeMap<String, models::RawValue>;

    let Ok(mut resource) = serde_json::from_str::<Skim>(resource.get()) else {
        return resource.clone();
    };

    resource.insert(
        "_meta".to_string(),
        models::RawValue::from_value(&serde_json::json!({
            "path": path
        })),
    );

    serde_json::value::to_raw_value(&resource).unwrap().into()
}

// Strip /_meta from a resource config, before sending it to a connector.
// TODO(johnny): We intend to remove this once connectors are updated.
fn strip_resource_meta(resource: &models::RawValue) -> bytes::Bytes {
    type Skim = std::collections::BTreeMap<String, models::RawValue>;

    let Ok(mut resource) = serde_json::from_str::<Skim>(resource.get()) else {
        return resource.get().to_string().into();
    };
    _ = resource.remove("_meta");

    let resource: Box<str> = serde_json::value::to_raw_value(&resource).unwrap().into();
    let resource: String = resource.into();
    resource.into()
}

/// Generate errors for duplicated, non-empty resource paths.
fn validate_resource_paths<'a>(
    scope: Scope<'a>,
    entity: &'static str,
    bindings_len: usize,
    resource_path: impl Fn(usize) -> &'a [String],
    errors: &mut tables::Errors,
) {
    let mut bindings_index: Vec<usize> = (0..bindings_len)
        .filter(|i| !resource_path(*i).is_empty())
        .collect();
    bindings_index.sort_by_key(|i| resource_path(*i));

    for (l_i, r_i) in bindings_index.into_iter().tuple_windows() {
        if resource_path(l_i) != resource_path(r_i) {
            continue;
        }
        Error::BindingDuplicatesResource {
            entity,
            path: resource_path(l_i).to_vec(),
            lhs_index: l_i,
            rhs_index: r_i,
        }
        .push(scope.push_prop("bindings").push_item(r_i), errors);
    }
}

/// Determine if a collection was reset by inspecting for an equal collection
/// name, but a non-equal journal partition template name. We attach a
/// generation ID to the end of the journal partition template name, so these
/// will differ if and only if the collection was semantically deleted and
/// re-created (either literally, or through a reset).
fn collection_was_reset(
    built_spec: &proto_flow::flow::CollectionSpec,
    live_spec: &Option<proto_flow::flow::CollectionSpec>,
) -> bool {
    if let Some(live_collection) = live_spec {
        if let Some(live_partition_template) = &live_collection.partition_template {
            let built_spec_partition_template_name = built_spec
                .partition_template
                .as_ref()
                .expect("built collections populate partition_template")
                .name
                .as_str();

            if live_collection.name == built_spec.name
                && live_partition_template.name != built_spec_partition_template_name
            {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod test {
    use tables::{BuiltCollection, DraftCollection, LiveCollection};

    use super::*;

    #[test]
    fn walk_transition_validates_is_touch_live_spec_exists() {
        let name = models::Collection::new("test/a");
        let pub_id = models::Id::new([0, 0, 0, 0, 0, 0, 0, 9]);
        let build_id = models::Id::new([0, 0, 0, 0, 0, 0, 0, 10]);
        let (data_planes, storage_mappings) = prefix_fixture();

        let draft = tables::DraftCollection {
            collection: name.clone(),
            scope: tables::synthetic_scope(models::CatalogType::Collection, &name),
            expect_pub_id: None,
            model: Some(models::CollectionDef::example()),
            is_touch: true,
        };
        let mut errors = tables::Errors::default();
        let _ = walk_transition::<DraftCollection, LiveCollection, BuiltCollection>(
            pub_id,
            build_id,
            "collection",
            None,
            EOB::Right(&draft),
            &data_planes,
            &storage_mappings,
            &mut errors,
        );
        assert!(matches!(
            errors.get(0).and_then(|e| e.error.downcast_ref::<Error>()),
            Some(Error::TouchModelIsCreate)
        ));
    }

    #[test]
    fn walk_transition_validates_is_touch_model_is_equal() {
        let name = models::Collection::new("test/a");
        let last_pub_id = models::Id::new([0, 0, 0, 0, 0, 0, 0, 5]);
        let last_build_id = models::Id::new([0, 0, 0, 0, 0, 0, 0, 6]);
        let control_id = models::Id::new([0, 0, 0, 0, 0, 0, 1, 1]);
        let data_plane_id = models::Id::new([0, 0, 0, 0, 0, 0, 2, 2]);
        let live = tables::LiveCollection {
            control_id,
            data_plane_id,
            collection: name.clone(),
            last_pub_id,
            last_build_id,
            model: models::CollectionDef::example(),
            spec: proto_flow::flow::CollectionSpec {
                name: name.to_string(),
                write_schema_json: String::from("{}").into(),
                read_schema_json: String::from("{}").into(),
                key: vec![String::from("/id")],
                uuid_ptr: String::from("/_meta/uuid"),
                partition_fields: vec![],
                projections: vec![],
                ack_template_json: String::from("{}").into(),
                partition_template: None,
                derivation: None,
            },
            dependency_hash: Some("abc123".to_owned()),
        };

        let mut draft = tables::DraftCollection {
            collection: name.clone(),
            scope: tables::synthetic_scope(models::CatalogType::Collection, &name),
            expect_pub_id: None,
            model: Some(models::CollectionDef::example()),
            is_touch: true,
        };

        let mut errors = tables::Errors::default();
        let pub_id = models::Id::new([0, 0, 0, 0, 0, 0, 0, 9]);
        let build_id = models::Id::new([0, 0, 0, 0, 0, 0, 0, 10]);
        let (data_planes, storage_mappings) = prefix_fixture();

        let (
            _name,
            _scope,
            _model,
            _control_id,
            _data_plane,
            _partition_stores,
            _recovery_stores,
            expect_pub_id,
            expect_build_id,
            _live_model,
            _live_spec,
            is_touch,
        ) = walk_transition::<_, _, BuiltCollection>(
            pub_id,
            build_id,
            "collection",
            None,
            EOB::Both(&live, &draft),
            &data_planes,
            &storage_mappings,
            &mut errors,
        )
        .unwrap();
        assert!(errors.is_empty());
        assert!(is_touch);
        assert_eq!(last_pub_id, expect_pub_id);
        assert_eq!(last_build_id, expect_build_id);

        draft.model.as_mut().unwrap().projections.insert(
            models::Field::new("foo"),
            models::Projection::Pointer(models::JsonPointer::new("/fooooooo")),
        );
        let _ = walk_transition::<_, _, tables::BuiltCollection>(
            pub_id,
            build_id,
            "collection",
            None,
            EOB::Both(&live, &draft),
            &data_planes,
            &storage_mappings,
            &mut errors,
        );
        assert!(matches!(
            errors.pop().and_then(|e| e.error.downcast::<Error>().ok()),
            Some(Error::TouchModelIsNotEqual)
        ));

        draft.model = None;
        let _ = walk_transition::<_, _, tables::BuiltCollection>(
            pub_id,
            build_id,
            "collection",
            None,
            EOB::Both(&live, &draft),
            &data_planes,
            &storage_mappings,
            &mut errors,
        );
        assert!(matches!(
            errors.pop().and_then(|e| e.error.downcast::<Error>().ok()),
            Some(Error::TouchModelIsDelete)
        ));
    }

    fn prefix_fixture() -> (tables::DataPlanes, tables::StorageMappings) {
        let mut data_planes = tables::DataPlanes::new();
        data_planes.insert_row(
            models::Id::new([0, 0, 0, 0, 0, 0, 2, 2]),
            "test-plane".to_string(),
            "test-plane.example.com".to_string(),
            vec!["test-key".to_string()],
            models::RawValue::default(),
            models::Collection::new("ops/acmeCo/logs"),
            models::Collection::new("ops/acmeCo/stats"),
            "broker.example.com".to_string(),
            "reactor.example.com".to_string(),
        );
        let mut storage_mappings = tables::StorageMappings::new();
        storage_mappings.insert_row(
            models::Prefix::new("test/"),
            models::Id::zero(),
            vec![],
            vec!["test-plane".to_string()],
        );
        storage_mappings.insert_row(
            models::Prefix::new("recovery/test/"),
            models::Id::zero(),
            vec![],
            vec!["test-plane".to_string()],
        );
        (data_planes, storage_mappings)
    }
}

fn temporary_cross_data_plane_read_check<'a>(
    scope: Scope<'a>,
    source: &tables::BuiltCollection,
    task_data_plane_id: models::Id,
    errors: &mut tables::Errors,
) {
    // ID of the legacy public data-plane ("cronut") in Estuary's production environment.
    // This is here temporarily, to power an error regarding cross-data-plane reads.
    const CRONUT_ID: models::Id = models::Id::new([0x0e, 0x8e, 0x17, 0xd0, 0x4f, 0xac, 0xd4, 0x00]);

    if task_data_plane_id != CRONUT_ID && source.data_plane_id == CRONUT_ID {
        let detail = anyhow::anyhow!(
            concat!(
                "Collection {} is in the legacy public data-plane (GCP:us-central1-c1),\n",
                "but this task is in a different data-plane.\n",
                "\n",
                "At the moment, Estuary does not support cross-data-plane reads from the legacy public data-plane.\n",
                "As a work-around either 1) delete and re-create your task in GCP:us-central1-c1,\n",
                "or 2) delete and re-create your collection in a different data-plane.\n",
            ),
            source.collection,
        ) ;

        Error::Connector { detail }.push(scope, errors);
    }
}

async fn expect_response<'a, R, E, T>(
    scope: Scope<'a>,
    mut response_rx: impl futures::Stream<Item = anyhow::Result<R>> + Unpin,
    extract: E,
    errors: &mut tables::Errors,
) -> Option<T>
where
    E: FnOnce(&mut R) -> anyhow::Result<Option<T>>,
    R: std::fmt::Debug,
{
    use futures::StreamExt;

    let response = match response_rx.next().await {
        Some(response) => response,
        None => Err(anyhow::anyhow!(
            "Expected connector to send {}, but read an EOF",
            std::any::type_name::<R>()
        )),
    };

    let mut response = match response {
        Ok(response) => response,
        Err(err) => {
            Error::Connector { detail: err }.push(scope, errors);
            return None;
        }
    };

    match extract(&mut response) {
        Ok(Some(extracted)) => Some(extracted),
        Ok(None) => {
            Error::Connector {
                detail: anyhow::anyhow!(
                    "Expected connector to send {}, but read {response:?}",
                    std::any::type_name::<T>()
                ),
            }
            .push(scope, errors);
            None
        }
        Err(err) => {
            Error::Connector { detail: err }.push(scope, errors);
            None
        }
    }
}

async fn expect_eof<'a, R>(
    scope: Scope<'a>,
    mut response_rx: impl futures::Stream<Item = anyhow::Result<R>> + Unpin,
    errors: &mut tables::Errors,
) where
    R: std::fmt::Debug,
{
    use futures::StreamExt;

    let response = match response_rx.next().await {
        None => Ok(()),
        Some(Ok(response)) => Err(anyhow::anyhow!(
            "Expected connector to send closing EOF, but read {response:?}",
        )),
        Some(Err(err)) => Err(err),
    };
    if let Err(err) = response {
        Error::Connector { detail: err }.push(scope, errors);
    }
}
