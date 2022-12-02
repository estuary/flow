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
) -> tables::BuiltCollections {
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
            ),
        );
    }

    built_collections
}

fn walk_collection(
    build_config: &flow::build_api::Config,
    collection: &tables::Collection,
    imports: &[tables::Import],
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> flow::CollectionSpec {
    let tables::Collection {
        scope,
        collection: name,
        spec: models::CollectionDef { key, .. },
        write_schema,
        read_schema,
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

    let write_shape = schema_shapes
        .iter()
        .find(|s| s.schema == *write_schema)
        .unwrap();
    let read_shape = schema_shapes
        .iter()
        .find(|s| s.schema == *read_schema)
        .unwrap();

    if read_shape.shape.type_ != types::OBJECT {
        Error::CollectionSchemaNotObject {
            collection: name.to_string(),
        }
        .push(scope, errors);
    }
    schema::walk_composite_key(scope, key, read_shape, errors);

    if write_schema != read_schema {
        // These checks must also validate against a differentiated write schema.
        if write_shape.shape.type_ != types::OBJECT {
            Error::CollectionSchemaNotObject {
                collection: name.to_string(),
            }
            .push(scope, errors);
        }
        schema::walk_composite_key(scope, key, write_shape, errors);
    }

    let projections =
        walk_collection_projections(collection, projections, write_shape, read_shape, errors);

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
        &write_shape.bundle,
        &read_shape.bundle,
        partition_stores,
    )
}

fn walk_collection_projections(
    collection: &tables::Collection,
    projections: &[tables::Projection],
    write_shape: &schema::Shape,
    read_shape: &schema::Shape,
    errors: &mut tables::Errors,
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

    // Projections which are statically inferred from the JSON schema.
    let implied_projections = read_shape
        .fields
        .iter()
        .map(|(f, p)| (models::Field::new(f), p));

    // Walk merged projections, mapping each to a flow::Projection and producing errors.
    projections
        .iter()
        .merge_join_by(implied_projections, |projection, (infer_field, _)| {
            projection.field.cmp(infer_field)
        })
        .map(|eob| walk_projection_with_inference(collection, eob, write_shape, read_shape, errors))
        .collect()
}

fn walk_projection_with_inference(
    collection: &tables::Collection,
    eob: EitherOrBoth<&tables::Projection, (models::Field, &models::JsonPointer)>,
    write_shape: &schema::Shape,
    read_shape: &schema::Shape,
    errors: &mut tables::Errors,
) -> flow::Projection {
    let (scope, field, location, projection) = match &eob {
        EitherOrBoth::Both(projection, (_, canonical_location)) => {
            let (user_location, _) = projection.spec.as_parts();

            if user_location != *canonical_location {
                Error::ProjectionRemapsCanonicalField {
                    field: projection.field.to_string(),
                    canonical_ptr: canonical_location.to_string(),
                    wrong_ptr: user_location.to_string(),
                }
                .push(&projection.scope, errors);
            }
            (
                &projection.scope,
                &projection.field,
                user_location,
                Some(&projection.spec),
            )
        }
        EitherOrBoth::Left(projection) => {
            let (location, _) = projection.spec.as_parts();
            (
                &projection.scope,
                &projection.field,
                location,
                Some(&projection.spec),
            )
        }
        EitherOrBoth::Right((field, location)) => (&collection.scope, field, *location, None),
    };

    let (r_inference, r_exists) = read_shape.shape.locate(&doc::Pointer::from_str(location));

    let mut spec = flow::Projection {
        ptr: location.to_string(),
        field: field.to_string(),
        explicit: false,
        is_primary_key: collection.spec.key.iter().any(|k| k == location),
        is_partition_key: false,
        inference: Some(assemble::inference(r_inference, r_exists)),
    };

    if let Some(projection) = projection {
        let (_, partition) = projection.as_parts();

        if partition {
            indexed::walk_name(
                scope,
                "partition",
                field,
                models::PartitionField::regex(),
                errors,
            );

            if write_shape.schema != read_shape.schema {
                // Partitioned projections must also validated against a differentiated write schema.
                let (w_inference, w_exists) =
                    write_shape.shape.locate(&doc::Pointer::from_str(location));

                schema::walk_explicit_location(
                    scope,
                    &write_shape.schema,
                    location,
                    true,
                    w_inference,
                    w_exists,
                    errors,
                );
            }
        }
        schema::walk_explicit_location(
            scope,
            &read_shape.schema,
            location,
            partition,
            r_inference,
            r_exists,
            errors,
        );

        spec.explicit = true;
        spec.is_partition_key = partition;
    }

    spec
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
