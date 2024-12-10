use super::{indexed, schema, storage_mapping, walk_transition, Error, Scope};
use json::schema::types;
use proto_flow::flow;
use std::collections::BTreeMap;
use tables::EitherOrBoth as EOB;

pub fn walk_all_collections(
    pub_id: models::Id,
    build_id: models::Id,
    default_plane_id: Option<models::Id>,
    draft_collections: &tables::DraftCollections,
    live_collections: &tables::LiveCollections,
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) -> tables::BuiltCollections {
    // Outer join of live and draft collections.
    let it = live_collections.outer_join(
        draft_collections.iter().map(|r| (&r.collection, r)),
        |eob| match eob {
            EOB::Left(live) => Some((&live.collection, EOB::Left(live))),
            EOB::Right((collection, draft)) => Some((collection, EOB::Right(draft))),
            EOB::Both(live, (collection, draft)) => Some((collection, EOB::Both(live, draft))),
        },
    );

    it.filter_map(|(_collection, eob)| {
        walk_collection(
            pub_id,
            build_id,
            default_plane_id,
            eob,
            storage_mappings,
            errors,
        )
    })
    .collect()
}

fn walk_collection(
    pub_id: models::Id,
    build_id: models::Id,
    default_plane_id: Option<models::Id>,
    eob: EOB<&tables::LiveCollection, &tables::DraftCollection>,
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) -> Option<tables::BuiltCollection> {
    let (
        collection,
        scope,
        model,
        control_id,
        data_plane_id,
        expect_pub_id,
        expect_build_id,
        _live_model,
        live_spec,
        is_touch,
    ) = match walk_transition(pub_id, build_id, default_plane_id, eob, errors) {
        Ok(ok) => ok,
        Err(built) => return Some(built),
    };
    let scope = Scope::new(scope);

    let models::CollectionDef {
        schema,
        write_schema,
        read_schema,
        key,
        projections,
        journals,
        derive: _,
        expect_pub_id: _,
        delete: _,
    } = model;

    indexed::walk_name(
        scope,
        "collection",
        collection,
        models::Collection::regex(),
        errors,
    );

    if key.is_empty() {
        Error::CollectionKeyEmpty {
            collection: collection.to_string(),
        }
        .push(scope.push_prop("key"), errors);
    }

    let (write_schema, write_bundle, read_schema_bundle) = match (schema, write_schema, read_schema)
    {
        // One schema used for both writes and reads.
        (Some(bundle), None, None) => (
            walk_collection_schema(scope.push_prop("schema"), bundle, errors)?,
            bundle.clone(),
            None,
        ),
        // Separate schemas used for writes and reads.
        (None, Some(model_write_schema), Some(model_read_schema)) => {
            let write_schema =
                walk_collection_schema(scope.push_prop("writeSchema"), model_write_schema, errors);

            // Potentially extend the user's read schema with definitions
            // for the collection's current write schema.
            let read_bundle =
                models::Schema::build_read_schema_bundle(model_read_schema, model_write_schema);

            let read_schema =
                walk_collection_schema(scope.push_prop("readSchema"), &read_bundle, errors);
            (
                write_schema?,
                model_write_schema.clone(),
                Some((read_schema?, read_bundle)),
            )
        }
        _ => {
            Error::InvalidSchemaCombination {
                collection: collection.to_string(),
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
    // Projections should be ascending and unique on field.
    assert!(projections.windows(2).all(|p| p[0].field < p[1].field));

    let partition_fields = projections
        .iter()
        .filter_map(|p| {
            if p.is_partition_key {
                Some(p.field.clone())
            } else {
                None
            }
        })
        .collect();

    let partition_stores =
        storage_mapping::mapped_stores(scope, "collection", collection, storage_mappings, errors);

    // Pass-through the existing journal prefix, or create a unique new one.
    let journal_name_prefix = if let Some(flow::CollectionSpec {
        partition_template: Some(template),
        ..
    }) = live_spec
    {
        template.name.clone()
    } else {
        // Semi-colons are disallowed in Gazette journal names.
        let pub_id = pub_id.to_string().replace(":", "");
        format!("{collection}/{pub_id}")
    };

    let partition_template = assemble::partition_template(
        build_id,
        collection,
        &journal_name_prefix,
        journals,
        partition_stores,
    );
    let bundle_to_string = |b: Option<models::Schema>| -> String {
        let b: Option<Box<serde_json::value::RawValue>> = b.map(|b| b.into_inner().into());
        let b: Option<Box<str>> = b.map(Into::into);
        let b: Option<String> = b.map(Into::into);
        b.unwrap_or_default()
    };
    let built_spec = flow::CollectionSpec {
        name: collection.to_string(),
        write_schema_json: bundle_to_string(Some(write_bundle)),
        read_schema_json: bundle_to_string(read_schema_bundle.map(|(_schema, bundle)| bundle)),
        key: key.iter().map(|p| p.to_string()).collect(),
        projections,
        partition_fields,
        uuid_ptr: UUID_PTR.to_string(),
        ack_template_json: serde_json::json!({
                "_meta": {"uuid": "DocUUIDPlaceholder-329Bb50aa48EAa9ef",
                "ack": true,
            } })
        .to_string(),
        partition_template: Some(partition_template),
        derivation: None,
    };

    Some(tables::BuiltCollection {
        collection: collection.clone(),
        scope: scope.flatten(),
        control_id,
        data_plane_id,
        expect_pub_id,
        expect_build_id,
        model: Some(model.clone()),
        spec: Some(built_spec),
        validated: None,
        previous_spec: live_spec.cloned(),
        is_touch,
        // Regular collections don't have dependencies. Derivation validation will set the hash.
        dependency_hash: None,
    })
}

fn walk_collection_schema(
    scope: Scope,
    bundle: &models::Schema,
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
    read_schema_bundle: Option<&(schema::Schema, models::Schema)>,
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
        .filter_map(|(field, projection)| {
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
                return Some(flow::Projection {
                    ptr: UUID_PTR.to_string(),
                    field: field.to_string(),
                    explicit: true,
                    inference: Some(assemble::inference_uuid_v1_date_time()),
                    ..Default::default()
                });
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

            Some(flow::Projection {
                ptr: ptr.to_string(),
                field: field.to_string(),
                explicit: true,
                is_primary_key: key.iter().any(|k| k == ptr),
                is_partition_key: partition,
                inference: Some(assemble::inference(r_shape, r_exists)),
            })
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

    // No conditional because we don't allow re-naming this projection
    projections.push(flow::Projection {
        ptr: doc::TRUNCATION_INDICATOR_PTR.to_string(),
        field: FLOW_TRUNCATED.to_string(),
        inference: Some(assemble::inference_truncation_indicator()),
        ..Default::default()
    });

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

    // Now add statically inferred locations from the read-time JSON schema. We'll do this for
    // all locations except for:
    // - pattern properties
    // - the root location
    // - locations for object properties with empty keys
    // - a `/flow_document` location (if someone captures a table we materialized)
    for (ptr, pattern, r_shape, r_exists) in effective_read_schema.shape.locations() {
        if pattern || ptr.0.is_empty() || ptr.0.ends_with(EMPTY_KEY) {
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
/// The default field name for the document publication time.
const FLOW_PUBLISHED_AT: &str = "flow_published_at";
/// The default field name for the truncation sentinel.
const FLOW_TRUNCATED: &str = "_meta/flow_truncated";
/// The JSON Pointer of the Flow document UUID.
const UUID_PTR: &str = "/_meta/uuid";
/// The JSON Pointer of the synthetic document publication time.
/// This pointer typically pairs with the FLOW_PUBLISHED_AT field.
const UUID_DATE_TIME_PTR: &str = "/_meta/uuid/date-time";

/// Used to check if a pointer ends with an empty key, so we can skip projecting those fields.
const EMPTY_KEY: &'static [doc::ptr::Token] = &[doc::ptr::Token::Property(String::new())];
