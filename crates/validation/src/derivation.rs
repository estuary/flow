use super::{collection, indexed, reference, schema, Error};
use itertools::Itertools;
use json::schema::types;
use models::{build, tables};
use protocol::flow;

pub fn walk_all_derivations(
    built_collections: &[tables::BuiltCollection],
    collections: &[tables::Collection],
    derivations: &[tables::Derivation],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_index: &doc::SchemaIndex<'_>,
    schema_shapes: &[schema::Shape],
    transforms: &[tables::Transform],
    errors: &mut tables::Errors,
) -> tables::BuiltDerivations {
    let mut built_derivations = tables::BuiltDerivations::new();

    for derivation in derivations {
        let built_collection = built_collections
            .iter()
            .find(|c| c.collection == derivation.derivation)
            .unwrap();

        let filtered_transforms = transforms
            .iter()
            .filter(|t| t.derivation == derivation.derivation)
            .collect::<Vec<_>>();

        built_derivations.push_row(
            &derivation.scope,
            &derivation.derivation,
            walk_derivation(
                built_collection,
                collections,
                derivation,
                imports,
                projections,
                schema_index,
                schema_shapes,
                &filtered_transforms,
                errors,
            ),
        );
    }

    built_derivations
}

fn walk_derivation(
    built_collection: &tables::BuiltCollection,
    collections: &[tables::Collection],
    derivation: &tables::Derivation,
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_index: &doc::SchemaIndex<'_>,
    schema_shapes: &[schema::Shape],
    transforms: &[&tables::Transform],
    errors: &mut tables::Errors,
) -> flow::DerivationSpec {
    let tables::Derivation {
        scope,
        derivation: _,
        register_schema,
        register_initial,
    } = derivation;

    // Verify that the register's initial value conforms to its schema.
    if schema_index.fetch(&register_schema).is_none() {
        // Referential integrity error, which we've already reported.
    } else if let Err(err) = doc::Validation::validate(
        &mut doc::Validator::new(schema_index),
        register_schema,
        register_initial.clone(),
    )
    .unwrap()
    .ok()
    {
        Error::RegisterInitialInvalid(err).push(scope, errors);
    }

    // We'll collect TransformSpecs and types of each transform's shuffle key (if known).
    let mut built_transforms = Vec::new();
    let mut shuffle_types: Vec<(Vec<types::Set>, &tables::Transform)> = Vec::new();

    // Walk transforms of this derivation.
    for transform in transforms {
        if let Some(type_set) = walk_transform(
            collections,
            imports,
            projections,
            schema_shapes,
            transform,
            &mut built_transforms,
            errors,
        ) {
            shuffle_types.push((type_set, transform));
        }
    }

    indexed::walk_duplicates(
        "transform",
        transforms.iter().map(|t| (&t.transform, &t.scope)),
        errors,
    );

    // Verify that shuffle key types & lengths align.
    for ((l_types, l_transform), (r_types, r_transform)) in shuffle_types.iter().tuple_windows() {
        if l_types != r_types {
            Error::ShuffleKeyMismatch {
                lhs_name: l_transform.transform.to_string(),
                lhs_types: l_types.clone(),
                rhs_name: r_transform.transform.to_string(),
                rhs_types: r_types.clone(),
            }
            .push(&l_transform.scope, errors);
        }
    }

    build::derivation_spec(derivation, built_collection, built_transforms)
}

pub fn walk_transform(
    collections: &[tables::Collection],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    transform: &tables::Transform,
    built_transforms: &mut Vec<flow::TransformSpec>,
    errors: &mut tables::Errors,
) -> Option<Vec<types::Set>> {
    let tables::Transform {
        scope,
        derivation: _,
        priority: _,
        publish_lambda,
        read_delay_seconds: _,
        shuffle_key,
        shuffle_lambda,
        source_collection: source,
        source_partitions,
        source_schema,
        transform: name,
        update_lambda,
    } = transform;

    indexed::walk_name(
        scope,
        "transform",
        name.as_ref(),
        &indexed::TRANSFORM_RE,
        errors,
    );

    if update_lambda.is_none() && publish_lambda.is_none() {
        Error::NoUpdateOrPublish {
            transform: name.to_string(),
        }
        .push(scope, errors);
    }

    // Dereference the transform's source. We can't continue without it.
    let source = match reference::walk_reference(
        scope,
        &format!("transform {}", name.as_str()),
        "collection",
        source,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    ) {
        Some(s) => s,
        None => return None,
    };

    if let Some(selector) = source_partitions {
        // Note that the selector is deliberately checked against the
        // collection's schema shape, and not our own transform source schema.
        let source_shape = schema_shapes
            .iter()
            .find(|s| s.schema == source.schema)
            .unwrap();

        let source_projections = projections
            .iter()
            .filter(|p| p.collection == source.collection)
            .collect::<Vec<_>>();

        collection::walk_selector(
            scope,
            &source.collection,
            &source_projections,
            source_shape,
            &selector,
            errors,
        );
    }

    // Map to an effective source schema & shape.
    let source_schema = match source_schema {
        Some(url) => {
            if url == &source.schema {
                Error::SourceSchemaNotDifferent {
                    schema: url.clone(),
                    collection: source.collection.to_string(),
                }
                .push(scope, errors);
            }
            url
        }
        None => &source.schema,
    };

    // TODO(johnny): Also require that shuffle hashes are the same!

    let shuffle_types = if shuffle_lambda.is_none() {
        // Map to an effective shuffle key.
        let shuffle_key = match shuffle_key {
            Some(key) => {
                if key.iter().eq(source.key.iter()) {
                    Error::ShuffleKeyNotDifferent {
                        transform: name.to_string(),
                        collection: source.collection.to_string(),
                    }
                    .push(scope, errors);
                }
                if key.iter().next().is_none() {
                    Error::ShuffleKeyEmpty {
                        transform: name.to_string(),
                    }
                    .push(scope, errors);
                }
                key
            }
            None => &source.key,
        };
        // Walk and collect key value types, so we can compare
        // with other transforms of this derivation later.
        let source_shape = schema_shapes
            .iter()
            .find(|s| s.schema == *source_schema)
            .unwrap();
        schema::walk_composite_key(scope, shuffle_key, source_shape, errors)
    } else {
        None
    };

    built_transforms.push(build::transform_spec(transform, source));

    shuffle_types
}
