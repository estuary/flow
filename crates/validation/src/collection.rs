use super::{indexed, schema, Error};
use itertools::{EitherOrBoth, Itertools};
use json::schema::types;
use models::{build, names, tables};
use protocol::flow;
use url::Url;

pub fn walk_all_collections(
    collections: &[tables::Collection],
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    errors: &mut tables::Errors,
) -> (tables::BuiltCollections, tables::Projections) {
    let mut implicit_projections = tables::Projections::new();
    let mut built_collections = tables::BuiltCollections::new();

    for collection in collections {
        let projections = projections
            .iter()
            .filter(|p| p.collection == collection.collection)
            .collect::<Vec<_>>();

        built_collections.push_row(
            &collection.scope,
            &collection.collection,
            walk_collection(
                collection,
                &projections,
                schema_shapes,
                errors,
                &mut implicit_projections,
            ),
        );
    }

    (built_collections, implicit_projections)
}

fn walk_collection(
    collection: &tables::Collection,
    projections: &[&tables::Projection],
    schema_shapes: &[schema::Shape],
    errors: &mut tables::Errors,
    implicit_projections: &mut tables::Projections,
) -> flow::CollectionSpec {
    let tables::Collection {
        collection: name,
        scope,
        schema,
        key,
    } = collection;

    indexed::walk_name(
        scope,
        "collection",
        name.as_ref(),
        &indexed::COLLECTION_RE,
        errors,
    );

    let schema = schema_shapes.iter().find(|s| s.schema == *schema).unwrap();
    let _ = schema::walk_composite_key(scope, key, schema, errors);

    let projections = walk_collection_projections(
        collection,
        projections,
        schema,
        errors,
        implicit_projections,
    );

    build::collection_spec(collection, projections, &schema.bundle)
}

fn walk_collection_projections(
    collection: &tables::Collection,
    projections: &[&tables::Projection],
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
    for eob in projections
        .iter()
        .sorted_by_key(|p| &p.field)
        .merge_join_by(schema_shape.fields.iter(), |projection, (field, _)| {
            projection.field.cmp(field)
        })
    {
        let (spec, implicit) =
            walk_projection_with_inference(collection, eob, schema_shape, errors);

        if let Some(spec) = spec {
            specs.push(spec);
        }
        if let Some(implicit) = implicit {
            implicit_projections.push(implicit);
        }
    }

    specs
}

fn walk_projection_with_inference(
    collection: &tables::Collection,
    eob: EitherOrBoth<&&tables::Projection, &(String, names::JsonPointer)>,
    schema_shape: &schema::Shape,
    errors: &mut tables::Errors,
) -> (Option<flow::Projection>, Option<tables::Projection>) {
    let (scope, field, location, projection) = match eob {
        EitherOrBoth::Both(projection, (field, location)) => {
            if &projection.location != location {
                Error::ProjectionRemapsCanonicalField {
                    field: field.clone(),
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
        EitherOrBoth::Right((field, location)) => (&collection.scope, field, location, None),
    };

    let (shape, exists) = match schema_shape.shape.locate(&doc::Pointer::from_str(location)) {
        Some(t) => t,
        None => {
            Error::NoSuchPointer {
                ptr: location.to_string(),
                schema: schema_shape.schema.clone(),
            }
            .push(scope, errors);
            return (None, None);
        }
    };

    let mut spec = flow::Projection {
        ptr: location.to_string(),
        field: field.clone(),
        user_provided: false,
        is_primary_key: collection.key.iter().any(|k| k == location),
        is_partition_key: false,
        inference: Some(build::inference(shape, exists)),
    };

    if let Some(projection) = projection {
        if projection.partition {
            indexed::walk_name(scope, "partition", field, &indexed::PARTITION_RE, errors);
            schema::walk_keyed_location(
                &projection.scope,
                &schema_shape.schema,
                location,
                shape,
                exists,
                errors,
            );
        }

        spec.user_provided = true;
        spec.is_partition_key = projection.partition;

        (Some(spec), None)
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
        (Some(spec), Some(implicit))
    }
}

pub fn walk_selector(
    scope: &Url,
    collection: &tables::Collection,
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    selector: &names::PartitionSelector,
    errors: &mut tables::Errors,
) {
    // Shape of this |collection|.
    let schema_shape = schema_shapes
        .iter()
        .find(|s| s.schema == collection.schema)
        .unwrap();

    // Filter to projections of this |collection|.
    let projections = projections
        .iter()
        .filter(|p| p.collection == collection.collection)
        .collect::<Vec<_>>();

    let names::PartitionSelector { include, exclude } = selector;

    for (category, labels) in &[("include", include), ("exclude", exclude)] {
        for (field, values) in labels.iter() {
            let partition = match projections.iter().find(|p| p.field == *field) {
                Some(projection) => {
                    if !projection.partition {
                        Error::ProjectionNotPartitioned {
                            category: category.to_string(),
                            field: field.clone(),
                            collection: collection.collection.to_string(),
                        }
                        .push(scope, errors);
                    }
                    projection
                }
                None => {
                    Error::NoSuchProjection {
                        category: category.to_string(),
                        field: field.clone(),
                        collection: collection.collection.to_string(),
                    }
                    .push(scope, errors);
                    continue;
                }
            };

            // Map partition to its accepted value type.
            // We'll error elsewhere if it's not found.
            let type_ = schema_shape
                .shape
                .locate(&doc::Pointer::from_str(&partition.location))
                .map(|(shape, _)| shape.type_)
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
