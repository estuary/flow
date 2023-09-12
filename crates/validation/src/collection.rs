use super::{indexed, schema, storage_mapping, Error, Scope};
use json::schema::types;
use proto_flow::flow;
use std::collections::BTreeMap;

pub fn walk_all_collections(
    build_id: &str,
    collections: &[tables::Collection],
    inferred_schemas: &BTreeMap<models::Collection, models::Schema>,
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> tables::BuiltCollections {
    let mut built_collections = tables::BuiltCollections::new();

    for collection in collections {
        if let Some(spec) = walk_collection(
            build_id,
            collection,
            inferred_schemas,
            storage_mappings,
            errors,
        ) {
            built_collections.insert_row(&collection.scope, &collection.collection, None, spec);
        }
    }
    built_collections
}

fn walk_collection(
    build_id: &str,
    collection: &tables::Collection,
    inferred_schemas: &BTreeMap<models::Collection, models::Schema>,
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

    let (write_schema, write_bundle, read_schema_bundle) = match (schema, write_schema, read_schema)
    {
        // One schema used for both writes and reads.
        (Some(bundle), None, None) => (
            walk_collection_schema(scope.push_prop("schema"), bundle, errors)?,
            (&bundle as &models::RawValue).clone(),
            None,
        ),
        // Separate schemas used for writes and reads.
        (None, Some(write_bundle), Some(read_bundle)) => {
            let write_schema =
                walk_collection_schema(scope.push_prop("writeSchema"), write_bundle, errors);

            // Potentially extend the user's read schema with definitions
            // for the collection's current write and inferred schemas.
            let read_bundle = extend_read_bundle(
                read_bundle,
                write_bundle,
                inferred_schemas.get(&collection.collection),
            );

            let read_schema =
                walk_collection_schema(scope.push_prop("readSchema"), &read_bundle, errors);
            (
                write_schema?,
                (&write_bundle as &models::RawValue).clone(),
                Some((read_schema?, read_bundle)),
            )
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
        if let Some((read_schema, _read_bundle)) = &read_schema_bundle {
            if let Err(err) = read_schema.walk_ptr(ptr, true) {
                Error::from(err).push(scope, errors);
            }
        }
    }

    let projections = walk_collection_projections(
        scope.push_prop("projections"),
        &write_schema,
        read_schema_bundle.as_ref(),
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
        read_schema_bundle.map(|(_schema, bundle)| bundle),
        partition_stores,
        UUID_PTR,
        write_bundle,
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
    read_schema_bundle: Option<&(schema::Schema, models::RawValue)>,
    key: &models::CompositeKey,
    projections: &BTreeMap<models::Field, models::Projection>,
    errors: &mut tables::Errors,
) -> Vec<flow::Projection> {
    let effective_read_schema = if let Some((read_schema, _read_bundle)) = read_schema_bundle {
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
            if matches!(read_schema_bundle, Some(_) if partition) {
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
            field: FLOW_PUBLISHED_AT.to_string(),
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

fn extend_read_bundle(
    read_bundle: &models::Schema,
    write_bundle: &models::Schema,
    inferred_bundle: Option<&models::Schema>,
) -> models::RawValue {
    use json::schema::keywords;
    use serde_json::{value::to_raw_value, Value};
    type Skim = BTreeMap<String, models::RawValue>;

    let mut read_schema: Skim = serde_json::from_str(read_bundle.get()).unwrap();
    let mut read_defs: Skim = read_schema
        .get(keywords::DEF)
        .map(|d| serde_json::from_str(d.get()).unwrap())
        .unwrap_or_default();

    // Add a definition for the write schema if it's referenced.
    // We cannot add it in all cases because the existing `read_bundle` and
    // `write_bundle` may have a common sub-schema defined, and naively adding
    // it would result in an indexing error due to the duplicate definition.
    // So, we treat $ref: flow://write-schema as a user assertion that there is
    // no such conflicting definition (and we may produce an indexing error
    // later if they're wrong).
    if read_bundle.references_write_schema() {
        let mut write_schema: Skim = serde_json::from_str(write_bundle.get()).unwrap();

        // Set $id to "flow://write-schema".
        _ = write_schema.insert(
            keywords::ID.to_string(),
            models::RawValue::from_value(&Value::String(
                models::Schema::REF_WRITE_SCHEMA_URL.to_string(),
            )),
        );
        // Add as a definition within the read schema.
        read_defs.insert(
            models::Schema::REF_WRITE_SCHEMA_URL.to_string(),
            to_raw_value(&write_schema).unwrap().into(),
        );
    }

    // Add a definition for the inferred schema if it's referenced.
    if read_bundle.references_inferred_schema() {
        let mut inferred_schema: Skim = inferred_bundle
            .map(|s| serde_json::from_str(s.get()).unwrap())
            .unwrap_or(Skim::new()); // Default to the "anything" schema {}.

        // Set $id to "flow://inferred-schema".
        _ = inferred_schema.insert(
            keywords::ID.to_string(),
            models::RawValue::from_value(&Value::String(
                models::Schema::REF_INFERRED_SCHEMA_URL.to_string(),
            )),
        );
        // Add as a definition within the read schema.
        read_defs.insert(
            models::Schema::REF_INFERRED_SCHEMA_URL.to_string(),
            to_raw_value(&inferred_schema).unwrap().into(),
        );
    }

    // Re-serialize the updated definitions of the read schema.
    _ = read_schema.insert(
        keywords::DEF.to_string(),
        serde_json::value::to_raw_value(&read_defs).unwrap().into(),
    );
    to_raw_value(&read_schema).unwrap().into()
}

/// The default field name for the root document projection.
const FLOW_DOCUMENT: &str = "flow_document";
/// The default field name for the document publication time.
const FLOW_PUBLISHED_AT: &str = "flow_published_at";
/// The JSON Pointer of the Flow document UUID.
const UUID_PTR: &str = "/_meta/uuid";
/// The JSON Pointer of the synthetic document publication time.
/// This pointer typically pairs with the FLOW_PUBLISHED_AT field.
const UUID_DATE_TIME_PTR: &str = "/_meta/uuid/date-time";

#[cfg(test)]
mod test {
    use serde_json::json;

    #[test]
    fn test_extend_read_schema() {
        let read_schema = models::Schema::new(models::RawValue::from_value(&json!({
            "$defs": {
                "existing://def": {"type": "array"},
            },
            "maxProperties": 10,
            "allOf": [
                {"$ref": "flow://inferred-schema"},
                {"$ref": "flow://write-schema"},
            ]
        })));
        let write_schema = models::Schema::new(models::RawValue::from_value(&json!({
            "$id": "old://value",
            "required": ["a_key"],
        })));
        let inferred_schema = models::Schema::new(models::RawValue::from_value(&json!({
            "$id": "old://value",
            "minProperties": 5,
        })));

        assert_eq!(
            super::extend_read_bundle(&read_schema, &write_schema, Some(&inferred_schema))
                .to_value(),
            json!({
                "$defs": {
                    "existing://def": {"type": "array"}, // Left alone.
                    "flow://write-schema": { "$id": "flow://write-schema", "required": ["a_key"] },
                    "flow://inferred-schema": { "$id": "flow://inferred-schema", "minProperties": 5 },
                },
                "maxProperties": 10,
                "allOf": [
                    {"$ref": "flow://inferred-schema"},
                    {"$ref": "flow://write-schema"},
                ]
            })
        );

        // Case: no inferred schema is available.
        assert_eq!(
            super::extend_read_bundle(&read_schema, &write_schema, None).to_value(),
            json!({
                "$defs": {
                    "existing://def": {"type": "array"}, // Left alone.
                    "flow://write-schema": { "$id": "flow://write-schema", "required": ["a_key"] },
                    "flow://inferred-schema": { "$id": "flow://inferred-schema" },
                },
                "maxProperties": 10,
                "allOf": [
                    {"$ref": "flow://inferred-schema"},
                    {"$ref": "flow://write-schema"},
                ]
            })
        );

        // Case: pass `write_schema` which has no references.
        assert_eq!(
            super::extend_read_bundle(&write_schema, &write_schema, None).to_value(),
            json!({
                "$defs": {},
                "$id": "old://value",
                "required": ["a_key"],
            })
        );
    }
}
