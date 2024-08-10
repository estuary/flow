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
pub use noop::NoOpConnectors;

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
        .filter_map(|p| if p.is_default { Some(p.id) } else { None })
        .next();

    storage_mapping::walk_all_storage_mappings(&live.storage_mappings, &mut errors);

    // Build all local collections.
    let mut built_collections = collection::walk_all_collections(
        pub_id,
        build_id,
        default_plane_id,
        &draft.collections,
        &live.collections,
        &live.inferred_schemas,
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

    let built_tests = test_step::walk_all_tests(
        pub_id,
        build_id,
        &draft.tests,
        &live.tests,
        &built_collections,
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
        &draft.imports,
        project_root,
        &live.storage_mappings,
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
        &mut materialize_errors,
    );

    // Concurrently validate all tasks.
    let (built_captures, built_derivations, built_materializations) =
        futures::join!(built_captures, built_derivations, built_materializations);

    errors.extend(capture_errors.into_iter());
    errors.extend(derive_errors.into_iter());
    errors.extend(materialize_errors.into_iter());

    // Attach all built derivations to the corresponding collections.
    for (built_index, validated, derivation) in built_derivations {
        let row = &mut built_collections[built_index];
        row.validated = Some(validated);
        row.spec.as_mut().unwrap().derivation = Some(derivation);
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
        Option<&'a L::BuiltSpec>, // Live spec.
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
            if live.last_pub_id() > pub_id {
                Error::PublicationSuperseded {
                    pub_id,
                    larger_id: live.last_pub_id(),
                }
                .push(Scope::new(&live.scope()), errors);
            }

            Err(B::new(
                live.catalog_name().clone(),
                live.scope(),
                live.control_id(),
                live.data_plane_id(),
                live.last_pub_id(),
                Some(live.model().clone()),
                None,
                Some(live.spec().clone()),
                None,
            ))
        }
        EOB::Right(draft) => {
            let last_pub_id = models::Id::zero(); // Not published.

            if let Some(expect) = draft.expect_pub_id() {
                if expect != last_pub_id {
                    Error::ExpectPubIdNotMatched {
                        expect_id: expect,
                        actual_id: last_pub_id,
                    }
                    .push(Scope::new(draft.scope()), errors);
                }
            }

            let default_plane_id = default_plane_id.unwrap_or_else(|| {
                Error::MissingDefaultDataPlane {
                    this_entity: draft.catalog_name().as_ref().to_string(),
                }
                .push(Scope::new(draft.scope()), errors);

                models::Id::zero()
            });

            match draft.model() {
                Some(model) => Ok((
                    draft.catalog_name(),
                    draft.scope(),
                    model,
                    models::Id::zero(), // No control-plane ID.
                    default_plane_id,   // Assign default data-plane.
                    last_pub_id,        // Not published (zero).
                    None,               // No live spec.
                )),
                None => {
                    Error::DeletedSpecDoesNotExist.push(Scope::new(draft.scope()), errors);

                    // Return a placeholder deletion of this specification.
                    Err(B::new(
                        draft.catalog_name().clone(),
                        draft.scope().clone(),
                        models::Id::zero(), // No control-plane ID.
                        models::Id::zero(), // Placeholder data-plane ID.
                        last_pub_id,
                        None,
                        None,
                        None,
                        None,
                    ))
                }
            }
        }
        EOB::Both(live, draft) => {
            if live.last_pub_id() > pub_id {
                Error::PublicationSuperseded {
                    pub_id,
                    larger_id: live.last_pub_id(),
                }
                .push(Scope::new(&live.scope()), errors);
            }
            match draft.expect_pub_id() {
                Some(expect) if expect != live.last_pub_id() => {
                    Error::ExpectPubIdNotMatched {
                        expect_id: expect,
                        actual_id: live.last_pub_id(),
                    }
                    .push(Scope::new(draft.scope()), errors);
                }
                _ => (),
            }

            match draft.model() {
                Some(model) => Ok((
                    draft.catalog_name(),
                    draft.scope(),
                    model,
                    live.control_id(),
                    live.data_plane_id(),
                    live.last_pub_id(),
                    Some(live.spec()),
                )),
                // Return a deletion of this specification.
                None => Err(B::new(
                    draft.catalog_name().clone(),
                    draft.scope().clone(),
                    live.control_id(),
                    live.data_plane_id(),
                    live.last_pub_id(),
                    None, // Deletion has no draft model.
                    None, // Deletion is not validated.
                    None, // Deletion is not built into a spec.
                    Some(live.spec().clone()),
                )),
            }
        }
    }
}
