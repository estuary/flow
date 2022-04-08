use super::{indexed, schema, storage_mapping, Error};
use itertools::{EitherOrBoth, Itertools};
use json::schema::types;
use proto_flow::flow;
use std::iter::FromIterator;
use superslice::Ext;
use url::Url;

pub fn walk_all_collections(
    build_config: &flow::build_api::Config,
    collections: &[tables::Collection],
    imports: &[tables::Import],
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> (tables::BuiltCollections, tables::Projections) {
    let mut implicit_projections = tables::Projections::new();
    let mut built_collections = tables::BuiltCollections::new();

    for collection in collections {
        let projections = &projections
            [projections.equal_range_by_key(&&collection.collection, |p| &p.collection)];

        built_collections.insert_row(
            &collection.scope,
            &collection.collection,
            walk_collection(
                build_config,
                collection,
                imports,
                projections,
                schema_shapes,
                storage_mappings,
                errors,
                &mut implicit_projections,
            ),
            None, // Not foreign.
        );
    }

    (built_collections, implicit_projections)
}

fn walk_collection(
    build_config: &flow::build_api::Config,
    collection: &tables::Collection,
    imports: &[tables::Import],
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
    implicit_projections: &mut tables::Projections,
) -> flow::CollectionSpec {
    let tables::Collection {
        collection: name,
        scope,
        schema,
        key,
        journals: _,
    } = collection;

    indexed::walk_name(
        scope,
        "collection",
        name.as_ref(),
        models::Collection::regex(),
        errors,
    );

    if key.is_empty() {
        Error::CollectionKeyEmpty {
            collection: name.to_string(),
        }
        .push(scope, errors);
    }

    let schema = schema_shapes.iter().find(|s| s.schema == *schema).unwrap();
    schema::walk_composite_key(scope, key, schema, errors);

    if schema.shape.type_ != types::OBJECT {
        Error::CollectionSchemaNotObject {
            collection: name.to_string(),
        }
        .push(scope, errors);
    }

    let projections = walk_collection_projections(
        collection,
        projections,
        schema,
        errors,
        implicit_projections,
    );

    let partition_stores = storage_mapping::mapped_stores(
        scope,
        "collection",
        imports,
        name.as_str(),
        storage_mappings,
        errors,
    );

    assemble::collection_spec(
        build_config,
        collection,
        projections,
        &schema.bundle,
        partition_stores,
    )
}

fn walk_collection_projections(
    collection: &tables::Collection,
    projections: &[tables::Projection],
    schema_shape: &schema::Shape,
    errors: &mut tables::Errors,
    implicit_projections: &mut tables::Projections,
) -> Vec<flow::Projection> {
    // Require that projection fields have no duplicates under our collation.
    // This restricts *manually* specified projections, but not canonical ones.
    // Most importantly, this ensures there are no collation-duplicated partitions.
    indexed::walk_duplicates(
        projections
            .iter()
            .map(|p| ("projection", p.field.as_str(), &p.scope)),
        errors,
    );

    let mut specs = Vec::new();
    for eob in projections.iter().merge_join_by(
        schema_shape
            .fields
            .iter()
            .map(|(f, p)| (models::Field::new(f), p)),
        |projection, (field, _)| projection.field.cmp(field),
    ) {
        let (spec, implicit) =
            walk_projection_with_inference(collection, eob, schema_shape, errors);

        if let Some(implicit) = implicit {
            implicit_projections.insert(implicit);
        }
        specs.push(spec);
    }

    specs
}

fn walk_projection_with_inference(
    collection: &tables::Collection,
    eob: EitherOrBoth<&tables::Projection, (models::Field, &models::JsonPointer)>,
    schema_shape: &schema::Shape,
    errors: &mut tables::Errors,
) -> (flow::Projection, Option<tables::Projection>) {
    let (scope, field, location, projection) = match &eob {
        EitherOrBoth::Both(projection, (field, location)) => {
            if &projection.location != *location {
                Error::ProjectionRemapsCanonicalField {
                    field: field.to_string(),
                    canonical_ptr: location.to_string(),
                    wrong_ptr: projection.location.to_string(),
                }
                .push(&projection.scope, errors);
            }
            (
                &projection.scope,
                &projection.field,
                &projection.location,
                Some(projection),
            )
        }
        EitherOrBoth::Left(projection) => (
            &projection.scope,
            &projection.field,
            &projection.location,
            Some(projection),
        ),
        EitherOrBoth::Right((field, location)) => (&collection.scope, field, *location, None),
    };

    let (shape, exists) = schema_shape.shape.locate(&doc::Pointer::from_str(location));

    let mut spec = flow::Projection {
        ptr: location.to_string(),
        field: field.to_string(),
        user_provided: false,
        is_primary_key: collection.key.iter().any(|k| k == location),
        is_partition_key: false,
        inference: Some(assemble::inference(shape, exists)),
    };

    if let Some(projection) = projection {
        if projection.partition {
            indexed::walk_name(
                scope,
                "partition",
                field,
                models::PartitionField::regex(),
                errors,
            );
        }
        schema::walk_explicit_location(
            &projection.scope,
            &schema_shape.schema,
            location,
            projection.partition,
            shape,
            exists,
            errors,
        );

        spec.user_provided = true;
        spec.is_partition_key = projection.partition;

        (spec, None)
    } else {
        // This is a discovered projection not provided by the user.
        let implicit = tables::Projection {
            scope: collection.scope.clone(),
            collection: collection.collection.clone(),
            field: field.clone(),
            location: location.clone(),
            partition: false,
            user_provided: false,
        };
        (spec, Some(implicit))
    }
}

pub fn walk_selector(
    scope: &Url,
    collection: &flow::CollectionSpec,
    selector: &models::PartitionSelector,
    errors: &mut tables::Errors,
) {
    let models::PartitionSelector { include, exclude } = selector;

    for (category, labels) in &[("include", include), ("exclude", exclude)] {
        for (field, values) in labels.iter() {
            let partition = match collection.projections.iter().find(|p| p.field == *field) {
                Some(projection) => {
                    if !projection.is_partition_key {
                        Error::ProjectionNotPartitioned {
                            category: category.to_string(),
                            field: field.clone(),
                            collection: collection.collection.clone(),
                        }
                        .push(scope, errors);
                    }
                    projection
                }
                None => {
                    Error::NoSuchProjection {
                        category: category.to_string(),
                        field: field.clone(),
                        collection: collection.collection.clone(),
                    }
                    .push(scope, errors);
                    continue;
                }
            };

            // Map partition inference to its accepted value type set.
            let type_ = partition
                .inference
                .as_ref()
                .map(|i| types::Set::from_iter(&i.types))
                .unwrap_or(types::ANY);

            for value in values {
                if !type_.overlaps(types::Set::for_value(value)) {
                    Error::SelectorTypeMismatch {
                        category: category.to_string(),
                        field: field.clone(),
                        value: value.to_string(),
                        type_,
                    }
                    .push(scope, errors);
                }

                if value.as_str() == Some("") {
                    Error::SelectorEmptyString {
                        category: category.to_string(),
                        field: field.clone(),
                    }
                    .push(scope, errors);
                }
            }
        }
    }
}
