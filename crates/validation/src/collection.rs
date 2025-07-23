use super::{indexed, schema, storage_mapping, walk_transition, Error, Scope};
use json::schema::types;
use proto_flow::flow;
use std::collections::BTreeMap;
use tables::EitherOrBoth as EOB;

pub(crate) fn walk_all_collections(
    pub_id: models::Id,
    build_id: models::Id,
    default_plane_id: Option<models::Id>,
    draft_collections: &tables::DraftCollections,
    inferred_schemas: &tables::InferredSchemas,
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

    it.filter_map(|(collection, eob)| {
        walk_collection(
            pub_id,
            build_id,
            default_plane_id,
            eob,
            inferred_schemas.get_by_key(collection),
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
    inferred_schema: Option<&tables::InferredSchema>,
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
        live_model,
        live_spec,
        is_touch,
    ) = match walk_transition(pub_id, build_id, default_plane_id, eob, errors) {
        Ok(ok) => ok,
        Err(built) => return Some(built),
    };
    let scope = Scope::new(scope);
    let mut model_fixes = Vec::new();

    let models::CollectionDef {
        schema: schema_model,
        write_schema: write_model,
        read_schema: mut read_model,
        key,
        projections: projection_models,
        journals,
        derive: _,
        expect_pub_id: _,
        delete: _,
        reset,
    } = model;

    indexed::walk_name(
        scope,
        "collection",
        collection,
        models::Collection::regex(),
        errors,
    );

    if let Some(live_model) = live_model {
        if !reset && key != live_model.key {
            Error::CollectionKeyChanged {
                collection: collection.to_string(),
                live: live_model.key.iter().map(|k| k.to_string()).collect(),
                draft: key.iter().map(|k| k.to_string()).collect(),
            }
            .push(scope.push_prop("key"), errors);
        }
    }

    // Determine the correct `journal_name_prefix` to use and its `generation_id`,
    // which is regenerated if `reset` is true or if this is a new collection.
    // Note we must account for historical journal templates which don't have
    // an embedded generation_id suffix.
    let (journal_name_prefix, generation_id) = match live_spec {
        Some(flow::CollectionSpec {
            partition_template: Some(template),
            ..
        }) if !reset => (
            template.name.clone(),
            assemble::extract_generation_id_suffix(&template.name),
        ),
        Some(_) => {
            model_fixes.push(format!("reset collection to new generation {pub_id}"));
            (assemble::partition_prefix(pub_id, collection), pub_id)
        }
        None => (assemble::partition_prefix(pub_id, collection), pub_id),
    };

    // If the collection has a read schema which references its inferred schema,
    // then update its read schema model to inline its inferred schema.
    if let Some(read_model) = &mut read_model {
        if read_model.references_inferred_schema() {
            let (inferred, model_fix) = if let Some(inferred) = inferred_schema {
                #[derive(serde::Deserialize)]
                struct Skim {
                    #[serde(rename = "x-collection-generation-id")]
                    expect_id: models::Id,
                }

                if let Ok(Skim { expect_id }) = serde_json::from_str::<Skim>(inferred.schema.get())
                {
                    if generation_id == expect_id {
                        (Some(&inferred.schema), "updated inferred schema")
                    } else {
                        (
                            None,
                            "applied inferred schema placeholder (inferred schema is stale)",
                        )
                    }
                } else {
                    (
                        Some(&inferred.schema),
                        "updated inferred schema (invalid x-generation-id fallback)",
                    )
                }
            } else {
                (
                    None,
                    "applied inferred schema placeholder (inferred schema is not available)",
                )
            };

            let inlined = read_model
                .add_defs(&[models::schemas::AddDef {
                    id: models::Schema::REF_INFERRED_SCHEMA_URL,
                    schema: inferred.unwrap_or(models::Schema::inferred_schema_placeholder()),
                    overwrite: true,
                }])
                .unwrap_or_else(|err| {
                    Error::SerdeJson(err).push(scope.push_prop("readSchema"), errors);
                    read_model.clone()
                });

            if inlined != *read_model {
                model_fixes.push(model_fix.to_string());
            }
            *read_model = inlined;
        }
    }

    let (write_spec, read_spec) = walk_collection_read_write_schemas(
        scope,
        &collection,
        &schema_model,
        &write_model,
        &read_model,
        errors,
    )?;

    let effective_read_spec = read_spec
        .as_ref()
        .map(|Schema { spec, .. }| spec)
        .unwrap_or(&write_spec.spec);
    let distinct_write_spec = read_model.is_some().then_some(&write_spec.spec);

    walk_collection_key(
        scope.push_prop("key"),
        &collection,
        &key,
        effective_read_spec,
        distinct_write_spec,
        errors,
    );

    let (projection_models, projection_specs) = walk_collection_projections(
        scope.push_prop("projections"),
        projection_models,
        effective_read_spec,
        &key,
        live_model,
        &mut model_fixes,
        distinct_write_spec,
        errors,
    );
    // Projections should be ascending and unique on field.
    assert!(projection_specs.windows(2).all(|p| p[0].field < p[1].field));

    let partition_fields: Vec<String> = projection_specs
        .iter()
        .filter_map(|p| {
            if p.is_partition_key {
                Some(p.field.clone())
            } else {
                None
            }
        })
        .collect();

    if let Some(live_spec) = live_spec {
        if !reset && partition_fields != live_spec.partition_fields {
            Error::CollectionPartitionsChanged {
                collection: collection.to_string(),
                live: live_spec.partition_fields.clone(),
                draft: partition_fields.clone(),
            }
            .push(scope.push_prop("projections"), errors);
        }
    }

    let partition_stores =
        storage_mapping::mapped_stores(scope, "collection", collection, storage_mappings, errors);

    // Specs always have `write_schema_json`, and may have `read_schema_json` if it's different.
    let write_schema_json = write_spec.model.into_inner().into();
    let read_schema_json = read_spec
        .map(|Schema { model, .. }| model.into_inner().into())
        .unwrap_or_default();

    let partition_template = assemble::partition_template(
        build_id,
        collection,
        journal_name_prefix,
        &journals,
        partition_stores,
    );
    let spec = flow::CollectionSpec {
        name: collection.to_string(),
        write_schema_json,
        read_schema_json,
        key: key.iter().map(|p| p.to_string()).collect(),
        projections: projection_specs,
        partition_fields,
        uuid_ptr: UUID_PTR.to_string(),
        ack_template_json: serde_json::json!({
            "_meta": {"uuid": "DocUUIDPlaceholder-329Bb50aa48EAa9ef", "ack": true}
        })
        .to_string(),
        partition_template: Some(partition_template),
        derivation: None,
    };
    let model = models::CollectionDef {
        schema: schema_model,
        write_schema: write_model,
        read_schema: read_model,
        key,
        projections: projection_models,
        journals,
        derive: None, // Re-attached later by validate().
        expect_pub_id: None,
        delete: false,
        reset: false,
    };

    Some(tables::BuiltCollection {
        collection: collection.clone(),
        scope: scope.flatten(),
        control_id,
        data_plane_id,
        expect_pub_id,
        // Regular collections don't have dependencies. Derivation validation will set the hash.
        dependency_hash: None,
        expect_build_id,
        is_touch: is_touch && model_fixes.is_empty(),
        model: Some(model),
        model_fixes,
        previous_spec: live_spec.cloned(),
        spec: Some(spec),
        validated: None,
    })
}

fn walk_collection_read_write_schemas(
    scope: Scope,
    collection: &models::Collection,
    schema_model: &Option<models::Schema>,
    write_model: &Option<models::Schema>,
    read_model: &Option<models::Schema>,
    errors: &mut tables::Errors,
) -> Option<(Schema, Option<Schema>)> {
    let (write_spec, read_spec) = match (&schema_model, &write_model, &read_model) {
        // A single schema is used for writes and reads.
        (Some(schema_model), None, None) => {
            let scope = scope.push_prop("schema");
            let schema_spec = walk_collection_schema(scope, schema_model.clone(), errors);

            (schema_spec?, None)
        }
        // Separate schemas for writes and reads.
        (None, Some(write_model), Some(read_model)) => {
            // We inline flow://write-schema and flow://relaxed-write-schema
            // into BuiltCollection.read_schema_json, but NOT `read_model`.

            let mut defs = Vec::new();
            let relaxed: Option<models::Schema>;
            let scope_read = scope.push_prop("readSchema");
            let scope_write = scope.push_prop("writeSchema");

            if read_model.references_write_schema() {
                defs.push(models::schemas::AddDef {
                    id: models::Schema::REF_WRITE_SCHEMA_URL,
                    schema: &write_model,
                    overwrite: true,
                });
            }
            if read_model.references_relaxed_write_schema() {
                let has_inferred_schema =
                    // TODO(johnny): This should simply be inferred_schema.is_some().
                    // We cannot do this until `agent` is consistent about threading in inferred schemas.
                    // Instead, use a hack which looks for the inferred schema placeholder,
                    // as a proxy for whether an actual inferred schema is available.
                    !read_model.get().contains("\"inferredSchemaIsNotAvailable\"");

                relaxed = has_inferred_schema
                    .then(|| write_model.to_relaxed_schema())
                    .transpose()
                    .unwrap_or_else(|err| {
                        Error::SerdeJson(err).push(scope_write, errors);
                        None
                    });

                defs.push(models::schemas::AddDef {
                    id: models::Schema::REF_RELAXED_WRITE_SCHEMA_URL,
                    schema: relaxed.as_ref().unwrap_or(&write_model),
                    overwrite: true,
                });
            }

            let read_spec = walk_collection_schema(
                scope_read,
                read_model.add_defs(&defs).unwrap_or_else(|err| {
                    Error::SerdeJson(err).push(scope_read, errors);
                    read_model.clone()
                }),
                errors,
            );
            let write_spec = walk_collection_schema(scope_write, write_model.clone(), errors);

            (write_spec?, Some(read_spec?))
        }
        _ => {
            Error::InvalidSchemaCombination {
                collection: collection.to_string(),
            }
            .push(scope, errors);
            return None;
        }
    };

    Some((write_spec, read_spec))
}

fn walk_collection_schema(
    scope: Scope,
    model: models::Schema,
    errors: &mut tables::Errors,
) -> Option<Schema> {
    let spec = match schema::Schema::new(model.get()) {
        Ok(schema) => schema,
        Err(err) => {
            err.push(scope, errors);
            return None;
        }
    };

    if spec.shape.type_ != types::OBJECT {
        Error::CollectionSchemaNotObject {
            schema: spec.curi.clone(),
        }
        .push(scope, errors);
        return None; // Squelch further errors.
    }

    for err in spec.shape.inspect() {
        Error::from(err).push(scope, errors);
    }

    Some(Schema { model, spec })
}

fn walk_collection_key(
    scope: Scope,
    collection: &models::Collection,
    key: &models::CompositeKey,
    effective_read_spec: &schema::Schema,
    write_spec: Option<&schema::Schema>,
    errors: &mut tables::Errors,
) {
    if key.is_empty() {
        Error::CollectionKeyEmpty {
            collection: collection.to_string(),
        }
        .push(scope, errors);
    }

    // The collection key must validate as a key-able location
    // across both read and write schemas.
    for (index, ptr) in key.iter().enumerate() {
        let scope = scope.push_item(index);

        if let Err(err) = schema::Schema::walk_ptr(effective_read_spec, write_spec, ptr, true) {
            Error::from(err).push(scope, errors);
        }
    }
}

fn walk_collection_projections(
    scope: Scope,
    mut models: BTreeMap<models::Field, models::Projection>,
    effective_read_spec: &schema::Schema,
    key: &models::CompositeKey,
    live_model: Option<&models::CollectionDef>,
    model_fixes: &mut Vec<String>,
    write_spec: Option<&schema::Schema>,
    errors: &mut tables::Errors,
) -> (
    BTreeMap<models::Field, models::Projection>,
    Vec<flow::Projection>,
) {
    // Require that projection fields have no duplicates under our collation.
    // This restricts *manually* specified projections, but not canonical ones.
    // Most importantly, this ensures there are no collation-duplicated partitions.
    indexed::walk_duplicates(
        models
            .iter()
            .map(|(field, _)| ("projection", field.as_str(), scope.push_prop(field))),
        errors,
    );

    let mut saw_root_projection = false;
    let mut saw_uuid_timestamp_projection = false;
    let mut specs = Vec::new();

    // Map explicit projections into built flow::Projection `specs`.
    // We filter model projections which are no longer referenced by the schema.
    models.retain(|field, projection| {
        let scope = scope.push_prop(field);

        let modified = if let Some(live) = live_model {
            live.projections.get(field) != Some(&*projection)
        } else {
            true
        };

        let (raw_ptr, partition) = match projection {
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

        if raw_ptr.as_str() == "" {
            saw_root_projection = true;
        } else if raw_ptr.as_str() == UUID_DATE_TIME_PTR && !partition {
            saw_uuid_timestamp_projection = true;

            // UUID_DATE_TIME_PTR is not a location that actually exists.
            // Return a synthetic projection because walk_ptr() will fail.
            specs.push(flow::Projection {
                ptr: UUID_PTR.to_string(),
                field: field.to_string(),
                explicit: true,
                inference: Some(assemble::inference_uuid_v1_date_time()),
                ..Default::default()
            });
            return true;
        }

        if let Err(err) =
            schema::Schema::walk_ptr(effective_read_spec, write_spec, raw_ptr, partition)
        {
            match err {
                Error::PtrIsImplicit { .. } if !partition && !modified => {
                    // Filter a projection which _used_ to exist, but no longer does.
                    // The goal of this error is to catch user typos and similar mistakes,
                    // but we don't want to block schema updates.
                    model_fixes.push(format!(
                        "removed projection {field}: {raw_ptr}, which is no longer in the schema"
                    ));
                    return false;
                }
                Error::PtrCannotExist { .. } if !partition && !modified => {
                    // Suppress an error if the unchanged explicit projection
                    // cannot exist due to a schema update, and emit its projection.
                    // This matches the behavior of the location's corresponding implicit projection.
                    ()
                }
                err => Error::from(err).push(scope, errors),
            }
        }

        let doc_ptr = doc::Pointer::from_str(raw_ptr);
        let (r_shape, r_exists) = effective_read_spec.shape.locate(&doc_ptr);

        specs.push(flow::Projection {
            ptr: raw_ptr.to_string(),
            field: field.to_string(),
            explicit: true,
            // TODO(johnny): Only canonical projections of key pointers should be `is_primary_key`.
            is_primary_key: key.iter().any(|k| k == raw_ptr),
            is_partition_key: partition,
            inference: Some(assemble::inference(r_shape, r_exists)),
        });
        return true;
    });

    // If we didn't see an explicit projection of the root document,
    // then add an implicit projection with field "flow_document".
    if !saw_root_projection {
        let (r_shape, r_exists) = effective_read_spec
            .shape
            .locate(&doc::Pointer::from_str(""));

        specs.push(flow::Projection {
            ptr: "".to_string(),
            field: FLOW_DOCUMENT.to_string(),
            inference: Some(assemble::inference(r_shape, r_exists)),
            ..Default::default()
        });
    }
    // If we didn't see an explicit projection of the UUID timestamp,
    // and an implicit projection with field "flow_published_at".
    if !saw_uuid_timestamp_projection {
        specs.push(flow::Projection {
            ptr: UUID_PTR.to_string(),
            field: FLOW_PUBLISHED_AT.to_string(),
            inference: Some(assemble::inference_uuid_v1_date_time()),
            ..Default::default()
        })
    }

    // No conditional because we don't allow re-naming this projection
    specs.push(flow::Projection {
        ptr: doc::TRUNCATION_INDICATOR_PTR.to_string(),
        field: FLOW_TRUNCATED.to_string(),
        inference: Some(assemble::inference_truncation_indicator()),
        ..Default::default()
    });

    // Now add implicit projections for the collection key.
    // These may duplicate explicit projections -- that's okay, we'll dedup them later.
    for raw_ptr in key.iter() {
        let doc_ptr = doc::Pointer::from_str(raw_ptr);
        let (r_shape, r_exists) = effective_read_spec.shape.locate(&doc_ptr);

        specs.push(flow::Projection {
            ptr: raw_ptr.to_string(),
            field: raw_ptr[1..].to_string(), // Canonical-ize by stripping the leading "/".
            explicit: false,
            is_primary_key: true,
            is_partition_key: false,
            inference: Some(assemble::inference(r_shape, r_exists)),
        });
    }

    // Now add statically inferred locations from the read-time JSON schema.
    // We'll do this for all locations except for:
    // - pattern properties
    // - the root location
    // - locations for object properties with empty keys
    // - a `/flow_document` location (if someone captures a table we materialized)
    for (doc_ptr, pattern, r_shape, r_exists) in effective_read_spec.shape.locations() {
        if pattern || doc_ptr.0.is_empty() || doc_ptr.0.ends_with(EMPTY_KEY) {
            continue;
        }
        // Canonical-ize by stripping the leading "/".
        let field = &doc_ptr.to_string()[1..];
        // Special case to avoid creating a conflicting projection when the collection
        // schema contains a field with the same name as the default root projection.
        if field == FLOW_DOCUMENT {
            continue;
        }
        specs.push(flow::Projection {
            ptr: doc_ptr.to_string(),
            field: field.to_string(),
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
    specs.sort_by(|l, r| l.field.cmp(&r.field));

    // Look for projections which re-map canonical projections (which is disallowed).
    for (lhs, rhs) in specs.windows(2).map(|pair| (&pair[0], &pair[1])) {
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
    specs.dedup_by(|l, r| l.field.cmp(&r.field).is_eq());

    (models, specs)
}

pub(crate) fn walk_selector(
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

pub fn skim_projections(
    scope: Scope,
    collection: &models::Collection,
    model: &models::CollectionDef,
    errors: &mut tables::Errors,
) -> Vec<flow::Projection> {
    let models::CollectionDef {
        schema: schema_model,
        write_schema: write_model,
        read_schema: read_model,
        key,
        projections: projection_models,
        ..
    } = model;

    let mut ignored_model_fixes = Vec::new();

    let Some((write_spec, read_spec)) = walk_collection_read_write_schemas(
        scope,
        collection,
        schema_model,
        write_model,
        read_model,
        errors,
    ) else {
        return Vec::new();
    };

    let effective_read_spec = read_spec
        .as_ref()
        .map(|Schema { spec, .. }| spec)
        .unwrap_or(&write_spec.spec);
    let distinct_write_spec = read_model.is_some().then_some(&write_spec.spec);

    walk_collection_key(
        scope.push_prop("key"),
        collection,
        &key,
        effective_read_spec,
        distinct_write_spec,
        errors,
    );

    let (_projection_models, projection_specs) = walk_collection_projections(
        scope.push_prop("projections"),
        projection_models.clone(),
        effective_read_spec,
        &key,
        None,
        &mut ignored_model_fixes,
        distinct_write_spec,
        errors,
    );

    projection_specs
}

struct Schema {
    model: models::Schema,
    spec: schema::Schema,
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
