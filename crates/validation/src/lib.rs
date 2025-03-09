use futures::future::BoxFuture;
use sources::Scope;
use tables::EitherOrBoth as EOB;

mod capture;
mod collection;
mod derivation;
mod errors;
mod indexed;
mod materialization;
mod noop;
mod reference;
mod schema;
mod storage_mapping;
mod test_step;

pub use errors::Error;
pub use noop::{NoOpConnectors, NoOpWrapper};

/// Connectors is a delegated trait -- provided to validate -- through which
/// connector validation RPCs are dispatched. Request and Response must always
/// be Validate / Validated variants, but may include `internal` fields.
pub trait Connectors: Send + Sync {
    fn validate_capture<'a>(
        &'a self,
        request: proto_flow::capture::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<proto_flow::capture::Response>>;

    fn validate_derivation<'a>(
        &'a self,
        request: proto_flow::derive::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<proto_flow::derive::Response>>;

    fn validate_materialization<'a>(
        &'a self,
        request: proto_flow::materialize::Request,
        data_plane: &'a tables::DataPlane,
    ) -> BoxFuture<'a, anyhow::Result<proto_flow::materialize::Response>>;
}

pub async fn validate(
    pub_id: models::Id,
    build_id: models::Id,
    project_root: &url::Url,
    connectors: &dyn Connectors,
    draft: &tables::DraftCatalog,
    live: &tables::LiveCatalog,
    fail_fast: bool,
) -> tables::Validations {
    let mut errors = tables::Errors::new();

    // Pluck out the default data-plane. It may not exist, which is an error
    // only if a new specification needs a data-plane assignment.
    let default_plane_id = live
        .data_planes
        .iter()
        .filter_map(|p| {
            if p.is_default {
                Some(p.control_id)
            } else {
                None
            }
        })
        .next();

    storage_mapping::walk_all_storage_mappings(&live.storage_mappings, &mut errors);

    // Build all local collections.
    let mut built_collections = collection::walk_all_collections(
        pub_id,
        build_id,
        default_plane_id,
        &draft.collections,
        &live.collections,
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
        &dependencies,
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
        default_plane_id,
        &live.storage_mappings,
        &dependencies,
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
        default_plane_id,
        &draft.imports,
        project_root,
        &live.storage_mappings,
        &dependencies,
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
        default_plane_id,
        &live.storage_mappings,
        &dependencies,
        &mut materialize_errors,
    );

    // Concurrently validate all tasks.
    let (built_captures, built_derivations, built_materializations) =
        futures::join!(built_captures, built_derivations, built_materializations);

    errors.extend(capture_errors.into_iter());
    errors.extend(derive_errors.into_iter());
    errors.extend(materialize_errors.into_iter());

    // Attach all built derivations to the corresponding collections.
    for (built_index, validated, derivation, dependency_hash) in built_derivations {
        let row = &mut built_collections[built_index];
        row.validated = Some(validated);
        row.spec.as_mut().unwrap().derivation = Some(derivation);
        row.dependency_hash = dependency_hash;
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

fn walk_transition<'a, D, L, B>(
    pub_id: models::Id,
    build_id: models::Id,
    default_plane_id: Option<models::Id>,
    eob: EOB<&'a L, &'a D>,
    errors: &mut tables::Errors,
) -> Result<
    // Result::Ok continues validation of this specification.
    (
        &'a D::Key,               // Catalog name.
        &'a url::Url,             // Scope.
        &'a D::ModelDef,          // Model to validate.
        models::Id,               // Live control-plane ID.
        models::Id,               // Assigned data-plane.
        models::Id,               // Live publication ID.
        models::Id,               // Live last build ID.
        Option<&'a L::ModelDef>,  // Live model.
        Option<&'a L::BuiltSpec>, // Live built spec.
        bool,                     // Is this a touch operation?
    ),
    // Result::Err is a completed BuiltRow for this specification.
    B,
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

            Err(B::new(
                live.catalog_name().clone(),
                live.scope(),
                live.control_id(),
                live.data_plane_id(),
                live.last_pub_id(),
                live.last_build_id(),
                Some(live.model().clone()),
                Vec::new(),
                None,
                Some(live.spec().clone()),
                None,
                false, // !is_touch
                live.dependency_hash().map(|h| h.to_owned()),
            ))
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

            let default_plane_id = default_plane_id.unwrap_or_else(|| {
                Error::MissingDefaultDataPlane {
                    this_entity: draft.catalog_name().as_ref().to_string(),
                }
                .push(Scope::new(draft.scope()), errors);

                models::Id::zero()
            });

            match draft.model() {
                // Catalog specification is being created.
                Some(model) => Ok((
                    draft.catalog_name(),
                    draft.scope(),
                    model,
                    models::Id::zero(), // Has no control-plane ID.
                    default_plane_id,   // Assign default data-plane.
                    models::Id::zero(), // Never published.
                    models::Id::zero(), // Never built.
                    None,               // Has no live model.
                    None,               // Has no live built spec.
                    false,              // !is_touch
                )),
                None => {
                    Error::DeletedSpecDoesNotExist.push(Scope::new(draft.scope()), errors);

                    // Return a placeholder deletion of this specification.
                    Err(B::new(
                        draft.catalog_name().clone(),
                        draft.scope().clone(),
                        models::Id::zero(), // No control-plane ID.
                        models::Id::zero(), // Placeholder data-plane ID.
                        models::Id::zero(),
                        models::Id::zero(),
                        None,
                        Vec::new(),
                        None,
                        None,
                        None,
                        false, // !is_touch
                        None,
                    ))
                }
            }
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

            match draft.model() {
                // Catalog specification is being updated.
                Some(model) => {
                    if draft.is_touch() && model != live.model() {
                        Error::TouchModelIsNotEqual.push(Scope::new(draft.scope()), errors);
                    }

                    Ok((
                        draft.catalog_name(),
                        draft.scope(),
                        model,
                        live.control_id(),
                        live.data_plane_id(),
                        live.last_pub_id(),
                        live.last_build_id(),
                        Some(live.model()),
                        Some(live.spec()),
                        draft.is_touch(),
                    ))
                }
                // Catalog specification is being deleted.
                None => {
                    if draft.is_touch() {
                        Error::TouchModelIsDelete.push(Scope::new(draft.scope()), errors);
                    }
                    Err(B::new(
                        draft.catalog_name().clone(),
                        draft.scope().clone(),
                        live.control_id(),
                        live.data_plane_id(),
                        live.last_pub_id(),
                        live.last_build_id(),
                        None, // Deletion has no draft model.
                        Vec::new(),
                        None, // Deletion is not validated.
                        None, // Deletion is not built into a spec.
                        Some(live.spec().clone()),
                        false, // !is_touch
                        live.dependency_hash().map(|h| h.to_owned()),
                    ))
                }
            }
        }
    }
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
        let dp_id = models::Id::new([0, 0, 0, 0, 0, 0, 0, 11]);

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
            Some(dp_id),
            EOB::Right(&draft),
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
                write_schema_json: String::from("{}"),
                read_schema_json: String::from("{}"),
                key: vec![String::from("/id")],
                uuid_ptr: String::from("/_meta/uuid"),
                partition_fields: vec![],
                projections: vec![],
                ack_template_json: String::from("{}"),
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

        let (
            _name,
            _scope,
            _model,
            _control_id,
            _data_plane_id,
            expect_pub_id,
            expect_build_id,
            _live_model,
            _live_spec,
            is_touch,
        ) = walk_transition::<_, _, BuiltCollection>(
            pub_id,
            build_id,
            None,
            EOB::Both(&live, &draft),
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
            None,
            EOB::Both(&live, &draft),
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
            None,
            EOB::Both(&live, &draft),
            &mut errors,
        );
        assert!(matches!(
            errors.pop().and_then(|e| e.error.downcast::<Error>().ok()),
            Some(Error::TouchModelIsDelete)
        ));
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
