use super::{indexed, schema, storage_mapping, Error, Scope};
use json::schema::types;
use proto_flow::flow;
use std::collections::BTreeMap;

pub fn walk_all_collections(
    build_id: &str,
    collections: &[tables::Collection],
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> tables::BuiltCollections {
    let mut built_collections = tables::BuiltCollections::new();

    for collection in collections {
        if let Some(spec) = walk_collection(build_id, collection, storage_mappings, errors) {
            built_collections.insert_row(&collection.scope, &collection.collection, None, spec);
        }
    }
    built_collections
}

// TODO(johnny): this is temporarily public, as we switch over to built
// specs being explicitly represented by the control plane.
pub fn walk_collection(
    build_id: &str,
    collection: &tables::Collection,
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> Option<flow::CollectionSpec> {
    let tables::Collection {
        scope,
        collection: name,
        spec:
            models::CollectionDef {
                schema,
                write_schema,
                read_schema,
                key,
                projections,
                journals: _,
                derive: _,
                derivation: _,
            },
    } = collection;
    let scope = Scope::new(scope);

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
        .push(scope.push_prop("key"), errors);
    }

    let (write_schema, read_schema) = match (schema, write_schema, read_schema) {
        // One schema used for both writes and reads.
        (Some(schema), None, None) => (
            walk_collection_schema(scope.push_prop("schema"), schema, errors)?,
            None,
        ),
        // Separate schemas used for writes and reads.
        (None, Some(write_schema), Some(read_schema)) => {
            let write =
                walk_collection_schema(scope.push_prop("writeSchema"), write_schema, errors);
            let read = walk_collection_schema(scope.push_prop("readSchema"), read_schema, errors);
            (write?, Some(read?))
        }
        _ => {
            Error::InvalidSchemaCombination {
                collection: name.to_string(),
            }
            .push(scope, errors);
            return None;
        }
    };

    // The collection key must validate as a key-able location
    // across both read and write schemas.
    for (index, ptr) in key.iter().enumerate() {
        let scope = scope.push_prop("key");
        let scope = scope.push_item(index);

        if let Err(err) = write_schema.walk_ptr(ptr, true) {
            Error::from(err).push(scope, errors);
        }
        if let Some(read_schema) = &read_schema {
            if let Err(err) = read_schema.walk_ptr(ptr, true) {
                Error::from(err).push(scope, errors);
            }
        }
    }

    let projections = walk_collection_projections(
        scope.push_prop("projections"),
        &write_schema,
        read_schema.as_ref(),
        key,
        projections,
        errors,
    );

    let partition_stores = storage_mapping::mapped_stores(
        scope,
        "collection",
        name.as_str(),
        storage_mappings,
        errors,
    );

    Some(assemble::collection_spec(
        build_id,
        collection,
        projections,
        partition_stores,
        UUID_PTR,
    ))
}

fn walk_collection_schema(
    scope: Scope,
    bundle: &models::RawValue,
    errors: &mut tables::Errors,
) -> Option<schema::Schema> {
    let schema = match schema::Schema::new(bundle.get()) {
        Ok(schema) => schema,
        Err(err) => {
            err.push(scope, errors);
            return None;
        }
    };

    if schema.shape.type_ != types::OBJECT {
        Error::CollectionSchemaNotObject {
            schema: schema.curi.clone(),
        }
        .push(scope, errors);
        return None; // Squelch further errors.
    }

    for err in schema.shape.inspect() {
        Error::from(err).push(scope, errors);
    }

    Some(schema)
}

fn walk_collection_projections(
    scope: Scope,
    write_schema: &schema::Schema,
    read_schema: Option<&schema::Schema>,
    key: &models::CompositeKey,
    projections: &BTreeMap<models::Field, models::Projection>,
    errors: &mut tables::Errors,
) -> Vec<flow::Projection> {
    let effective_read_schema = if let Some(read_schema) = read_schema {
        read_schema
    } else {
        write_schema
    };

    // Require that projection fields have no duplicates under our collation.
    // This restricts *manually* specified projections, but not canonical ones.
    // Most importantly, this ensures there are no collation-duplicated partitions.
    indexed::walk_duplicates(
        projections
            .iter()
            .map(|(field, _)| ("projection", field.as_str(), scope.push_prop(field))),
        errors,
    );

    let mut saw_root_projection = false;
    let mut saw_uuid_timestamp_projection = false;

    // Map explicit projections into built flow::Projection instances.
    let mut projections = projections
        .iter()
        .map(|(field, projection)| {
            let scope = scope.push_prop(field);

            let (ptr, partition) = match projection {
                models::Projection::Pointer(ptr) => (ptr, false),
                models::Projection::Extended {
                    location,
                    partition,
                } => (location, *partition),
            };

            if partition {
                indexed::walk_name(
                    scope,
                    "partition",
                    field,
                    models::PartitionField::regex(),
                    errors,
                );
            }

            if ptr.as_str() == "" {
                saw_root_projection = true;
            } else if ptr.as_str() == UUID_DATE_TIME_PTR && !partition {
                saw_uuid_timestamp_projection = true;

                // UUID_DATE_TIME_PTR is not a location that actually exists.
                // Return a synthetic projection because walk_ptr() will fail.
                return flow::Projection {
                    ptr: UUID_PTR.to_string(),
                    field: field.to_string(),
                    explicit: true,
                    inference: Some(assemble::inference_uuid_v1_date_time()),
                    ..Default::default()
                };
            }

            if let Err(err) = effective_read_schema.walk_ptr(ptr, partition) {
                Error::from(err).push(scope, errors);
            }
            if matches!(read_schema, Some(_) if partition) {
                // Partitioned projections must also be key-able within the write schema.
                if let Err(err) = write_schema.walk_ptr(ptr, true) {
                    Error::from(err).push(scope, errors);
                }
            }

            let (r_shape, r_exists) = effective_read_schema
                .shape
                .locate(&doc::Pointer::from_str(ptr));

            flow::Projection {
                ptr: ptr.to_string(),
                field: field.to_string(),
                explicit: true,
                is_primary_key: key.iter().any(|k| k == ptr),
                is_partition_key: partition,
                inference: Some(assemble::inference(r_shape, r_exists)),
            }
        })
        .collect::<Vec<_>>();

    // If we didn't see an explicit projection of the root document,
    // add an implicit projection with field "flow_document".
    if !saw_root_projection {
        let (r_shape, r_exists) = effective_read_schema
            .shape
            .locate(&doc::Pointer::from_str(""));

        projections.push(flow::Projection {
            ptr: "".to_string(),
            field: FLOW_DOCUMENT.to_string(),
            inference: Some(assemble::inference(r_shape, r_exists)),
            ..Default::default()
        });
    }
    // If we didn't see an explicit projection of the UUID timestamp,
    // and an implicit projection with field "flow_published_at".
    if !saw_uuid_timestamp_projection {
        projections.push(flow::Projection {
            ptr: UUID_PTR.to_string(),
            field: "flow_published_at".to_string(),
            inference: Some(assemble::inference_uuid_v1_date_time()),
            ..Default::default()
        })
    }

    // Now add implicit projections for the collection key.
    // These may duplicate explicit projections -- that's okay, we'll dedup them later.
    for ptr in key.iter() {
        let (r_shape, r_exists) = effective_read_schema
            .shape
            .locate(&doc::Pointer::from_str(ptr));

        projections.push(flow::Projection {
            ptr: ptr.to_string(),
            field: ptr[1..].to_string(), // Canonical-ize by stripping the leading "/".
            explicit: false,
            is_primary_key: true,
            is_partition_key: false,
            inference: Some(assemble::inference(r_shape, r_exists)),
        });
    }

    // Now add all statically inferred locations from the read-time JSON schema
    // which are not patterns or the document root.
    for (ptr, pattern, r_shape, r_exists) in effective_read_schema.shape.locations() {
        if pattern || ptr.0.is_empty() {
            continue;
        }
        // Canonical-ize by stripping the leading "/".
        let field = ptr.to_string()[1..].to_string();
        // Special case to avoid creating a conflicting projection when the collection
        // schema contains a field with the same name as the default root projection.
        if field == FLOW_DOCUMENT {
            continue;
        }
        projections.push(flow::Projection {
            ptr: ptr.to_string(),
            field,
            explicit: false,
            is_primary_key: false,
            is_partition_key: false,
            inference: Some(assemble::inference(r_shape, r_exists)),
        });
    }

    // Stable-sort on ascending projection field, which preserves the
    // construction order on a per-field basis:
    // * An explicit projection is first, then
    // * A keyed location, then
    // * An inferred location
    projections.sort_by(|l, r| l.field.cmp(&r.field));

    // Look for projections which re-map canonical projections (which is disallowed).
    for (lhs, rhs) in projections.windows(2).map(|pair| (&pair[0], &pair[1])) {
        if lhs.field == rhs.field && lhs.ptr != rhs.ptr {
            Error::ProjectionRemapsCanonicalField {
                field: lhs.field.clone(),
                canonical_ptr: rhs.ptr.to_string(),
                wrong_ptr: lhs.ptr.to_string(),
            }
            .push(scope.push_prop(&lhs.field), errors);
        }
    }

    // Now de-duplicate on field, taking the first entry. Recall that user projections are first.
    projections.dedup_by(|l, r| l.field.cmp(&r.field).is_eq());

    projections
}

pub fn walk_selector(
    scope: Scope,
    collection: &flow::CollectionSpec,
    selector: &models::PartitionSelector,
    errors: &mut tables::Errors,
) {
    let models::PartitionSelector { include, exclude } = selector;

    for (category, labels) in &[("include", include), ("exclude", exclude)] {
        let scope = scope.push_prop(category);

        for (field, values) in labels.iter() {
            let scope = scope.push_prop(field);

            let partition = match collection.projections.iter().find(|p| p.field == *field) {
                Some(projection) => {
                    if !projection.is_partition_key {
                        Error::ProjectionNotPartitioned {
                            category: category.to_string(),
                            field: field.clone(),
                            collection: collection.name.clone(),
                        }
                        .push(scope, errors);
                    }
                    projection
                }
                None => {
                    Error::NoSuchProjection {
                        category: category.to_string(),
                        field: field.clone(),
                        collection: collection.name.clone(),
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

            for (index, value) in values.iter().enumerate() {
                let scope = scope.push_item(index);

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

/// The default field name for the root document projection.
const FLOW_DOCUMENT: &str = "flow_document";
const UUID_PTR: &str = "/_meta/uuid";
const UUID_DATE_TIME_PTR: &str = "/_meta/uuid/date-time";
