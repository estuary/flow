use super::{collection, indexed, reference, schema, storage_mapping, Drivers, Error};
use futures::FutureExt;
use itertools::{EitherOrBoth, Itertools};
use models::{self, build, tables};
use protocol::{flow, labels, materialize};
use std::collections::{BTreeMap, HashMap};
use superslice::Ext;
use url::Url;

pub async fn walk_all_materializations<D: Drivers>(
    drivers: &D,
    built_collections: &[tables::BuiltCollection],
    collections: &[tables::Collection],
    imports: &[tables::Import],
    materialization_bindings: &[tables::MaterializationBinding],
    materializations: &[tables::Materialization],
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> tables::BuiltMaterializations {
    let mut validations = Vec::new();

    // Group |materialization_bindings| on bindings having the same materialization.
    let materialization_bindings = materialization_bindings
        .into_iter()
        .group_by(|m| &m.materialization);

    // Walk ordered materializations, left-joined by their bindings.
    for (materialization, bindings) in materializations
        .iter()
        .merge_join_by(materialization_bindings.into_iter(), |l, (r, _)| {
            l.materialization.cmp(r)
        })
        .filter_map(|eob| match eob {
            EitherOrBoth::Both(materialization, (_, bindings)) => {
                Some((materialization, Some(bindings)))
            }
            EitherOrBoth::Left(materialization) => Some((materialization, None)),
            EitherOrBoth::Right(_) => None,
        })
    {
        let mut materialization_errors = tables::Errors::new();

        // Require the materialization name is valid.
        indexed::walk_name(
            &materialization.scope,
            "materialization",
            &materialization.materialization,
            &indexed::MATERIALIZATION_RE,
            &mut materialization_errors,
        );

        let validation = walk_materialization_request(
            built_collections,
            collections,
            imports,
            materialization,
            bindings.into_iter().flatten().collect(),
            projections,
            schema_shapes,
            &mut materialization_errors,
        );

        // Skip validation if errors were encountered building the request.
        if materialization_errors.is_empty() {
            validations.push(validation);
        } else {
            errors.extend(materialization_errors.into_iter());
        }
    }

    // Run all validations concurrently.
    let validations =
        validations
            .into_iter()
            .map(|(materialization, binding_models, request)| async move {
                drivers
                    .validate_materialization(request.clone())
                    .map(|response| (materialization, binding_models, request, response))
                    .await
            });
    let validations = futures::future::join_all(validations).await;

    let mut built_materializations = tables::BuiltMaterializations::new();

    for (materialization, binding_models, request, response) in validations {
        // Unwrap |response| and continue if an Err.
        let response = match response {
            Ok(response) => response,
            Err(err) => {
                Error::MaterializationDriver {
                    name: request.materialization,
                    detail: err,
                }
                .push(&materialization.scope, errors);
                continue;
            }
        };

        let materialize::ValidateRequest {
            endpoint_type,
            endpoint_spec_json,
            bindings: binding_requests,
            materialization: name,
        } = request;

        let materialize::ValidateResponse {
            bindings: binding_responses,
        } = response;

        // We constructed |binding_requests| while processing binding models.
        assert!(binding_requests.len() == binding_models.len());

        let tables::Materialization { scope, shards, .. } = materialization;

        if binding_requests.len() != binding_responses.len() {
            Error::MaterializationDriver {
                name: name.to_string(),
                detail: anyhow::anyhow!(
                    "driver returned wrong number of bindings (expected {}, got {})",
                    binding_requests.len(),
                    binding_responses.len()
                ),
            }
            .push(scope, errors);
        }

        // Join requests, responses and models to produce tuples
        // of (scope, built binding).
        let bindings: Vec<_> = binding_requests
            .into_iter()
            .zip(binding_responses.into_iter())
            .zip(binding_models.into_iter())
            .map(|((binding_request, binding_response), binding_model)| {
                let materialize::validate_request::Binding {
                    collection: collection_spec,
                    field_config_json: _,
                    resource_spec_json,
                } = binding_request;

                let materialize::validate_response::Binding {
                    constraints,
                    delta_updates,
                    resource_path,
                } = binding_response;

                let collection_spec = collection_spec.unwrap();
                let fields = walk_materialization_response(
                    &collection_spec,
                    binding_model,
                    constraints,
                    errors,
                );
                let shuffle = models::build::materialization_shuffle(
                    binding_model,
                    &collection_spec,
                    &resource_path,
                );

                (
                    &binding_model.scope,
                    flow::materialization_spec::Binding {
                        collection: Some(collection_spec),
                        field_selection: Some(fields),
                        resource_spec_json,
                        resource_path,
                        delta_updates,
                        shuffle: Some(shuffle),
                    },
                )
            })
            .collect();

        // Look for (and error on) duplicated resource paths within the bindings.
        for ((l_scope, _), (r_scope, binding)) in bindings
            .iter()
            .sorted_by(|(_, l), (_, r)| l.resource_path.cmp(&r.resource_path))
            .tuple_windows()
            .filter(|((_, l), (_, r))| l.resource_path == r.resource_path)
        {
            Error::BindingDuplicatesResource {
                entity: "materialization",
                name: name.to_string(),
                resource: binding.resource_path.iter().join("."),
                rhs_scope: (*r_scope).clone(),
            }
            .push(l_scope, errors);
        }

        // Unzip to strip scopes, leaving built bindings.
        let (_, bindings): (Vec<_>, Vec<_>) = bindings.into_iter().unzip();

        let recovery_stores = storage_mapping::mapped_stores(
            scope,
            "materialization",
            imports,
            &format!("recovery/{}", name.as_str()),
            storage_mappings,
            errors,
        );

        let spec = flow::MaterializationSpec {
            bindings,
            endpoint_spec_json,
            endpoint_type,
            materialization: name.to_string(),
            recovery_log_template: Some(build::recovery_log_template(
                &name,
                labels::TASK_TYPE_MATERIALIZATION,
                recovery_stores,
            )),
            shard_template: Some(build::shard_template(
                &name,
                labels::TASK_TYPE_MATERIALIZATION,
                shards,
                false, // Don't disable wait_for_ack.
            )),
        };

        built_materializations.insert_row(scope, name, spec);
    }

    built_materializations
}

fn walk_materialization_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    collections: &[tables::Collection],
    imports: &[tables::Import],
    materialization: &'a tables::Materialization,
    materialization_bindings: Vec<&'a tables::MaterializationBinding>,
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    errors: &mut tables::Errors,
) -> (
    &'a tables::Materialization,
    Vec<&'a tables::MaterializationBinding>,
    materialize::ValidateRequest,
) {
    let tables::Materialization {
        scope: _,
        materialization: name,
        endpoint_type,
        endpoint_spec,
        shards: _,
    } = materialization;

    let (binding_models, binding_requests): (Vec<_>, Vec<_>) = materialization_bindings
        .iter()
        .filter_map(|materialization_binding| {
            walk_materialization_binding(
                built_collections,
                collections,
                imports,
                materialization_binding,
                projections,
                schema_shapes,
                errors,
            )
            .map(|binding_request| (*materialization_binding, binding_request))
        })
        .unzip();

    let request = materialize::ValidateRequest {
        materialization: name.to_string(),
        bindings: binding_requests,
        endpoint_type: *endpoint_type as i32,
        endpoint_spec_json: endpoint_spec.to_string(),
    };

    (materialization, binding_models, request)
}

fn walk_materialization_binding<'a>(
    built_collections: &'a [tables::BuiltCollection],
    collections: &[tables::Collection],
    imports: &[tables::Import],
    materialization_binding: &'a tables::MaterializationBinding,
    projections: &[tables::Projection],
    schema_shapes: &[schema::Shape],
    errors: &mut tables::Errors,
) -> Option<materialize::validate_request::Binding> {
    let tables::MaterializationBinding {
        scope,
        materialization: name,
        materialization_index: _,
        resource_spec,
        collection,
        fields_include,
        fields_exclude,
        fields_recommended: _,
        source_partitions,
    } = materialization_binding;

    // We must resolve the source collection to continue.
    let source = reference::walk_reference(
        scope,
        "materialization",
        "collection",
        collection,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    )?;

    let built_collection = &built_collections
        [built_collections.lower_bound_by_key(&&source.collection, |c| &c.collection)];

    if let Some(selector) = source_partitions {
        collection::walk_selector(scope, source, projections, schema_shapes, &selector, errors);
    }

    let field_config = walk_materialization_fields(
        scope,
        name,
        built_collection,
        fields_include,
        fields_exclude,
        errors,
    );

    let request = materialize::validate_request::Binding {
        resource_spec_json: resource_spec.to_string(),
        collection: Some(built_collection.spec.clone()),
        field_config_json: field_config.into_iter().collect(),
    };

    Some(request)
}

fn walk_materialization_fields<'a>(
    scope: &Url,
    materialization: &str,
    built_collection: &tables::BuiltCollection,
    include: &BTreeMap<models::Field, models::Object>,
    exclude: &[models::Field],
    errors: &mut tables::Errors,
) -> Vec<(String, String)> {
    let flow::CollectionSpec {
        collection,
        projections,
        ..
    } = &built_collection.spec;

    let mut bag = Vec::new();

    for (field, config) in include {
        if projections.iter().any(|p| p.field == field.as_str()) {
            bag.push((field.to_string(), serde_json::to_string(config).unwrap()));
        } else {
            Error::NoSuchProjection {
                category: "include".to_string(),
                field: field.to_string(),
                collection: collection.clone(),
            }
            .push(scope, errors);
        }
    }

    for field in exclude {
        if !projections.iter().any(|p| p.field == field.as_str()) {
            Error::NoSuchProjection {
                category: "exclude".to_string(),
                field: field.to_string(),
                collection: collection.clone(),
            }
            .push(scope, errors);
        }
        if include.contains_key(field) {
            Error::FieldUnsatisfiable {
                name: materialization.to_string(),
                field: field.to_string(),
                reason: "field is both included and excluded by selector".to_string(),
            }
            .push(scope, errors);
        }
    }

    bag
}

fn walk_materialization_response(
    collection_spec: &flow::CollectionSpec,
    materialization_binding: &tables::MaterializationBinding,
    mut constraints: HashMap<String, materialize::Constraint>,
    errors: &mut tables::Errors,
) -> flow::FieldSelection {
    let tables::MaterializationBinding {
        scope,
        fields_include: include,
        fields_exclude: exclude,
        fields_recommended: recommended,
        ..
    } = materialization_binding;

    let flow::CollectionSpec {
        projections,
        key_ptrs,
        ..
    } = collection_spec;

    // |keys| and |document| are initialized with placeholder None,
    // that we'll revisit as we walk projections & constraints.
    let mut keys = key_ptrs
        .iter()
        .map(|_| Option::<String>::None)
        .collect::<Vec<_>>();
    let mut document = String::new();
    // Projections *not* key parts or the root document spill to |values|.
    let mut values = Vec::new();
    // Required locations (as JSON pointers), and an indication of whether each has been found.
    let mut locations: HashMap<String, bool> = HashMap::new();
    // Encoded field configuration, passed through from |include| to the driver.
    let mut field_config = HashMap::new();

    use materialize::constraint::Type;

    // Sort projections so that we walk, in order:
    // * Fields which *must* be included.
    // * Fields which are user-defined, and should be selected preferentially
    //   for locations where we need only one field.
    // * Everything else.
    let projections = projections
        .iter()
        .sorted_by_key(|p| {
            let must_include = include.get(&models::Field::new(&p.field)).is_some()
                || constraints
                    .get(&p.field)
                    .map(|c| c.r#type == Type::FieldRequired as i32)
                    .unwrap_or_default();

            (!must_include, !p.user_provided) // Negate to order before.
        })
        .collect::<Vec<_>>();

    for projection in projections {
        let flow::Projection { ptr, field, .. } = projection;

        let constraint = constraints
            .remove(field)
            .unwrap_or(materialize::Constraint {
                r#type: Type::FieldForbidden as i32,
                reason: String::new(),
            });

        let type_ = match Type::from_i32(constraint.r#type) {
            Some(t) => t,
            None => {
                Error::MaterializationDriver {
                    name: materialization_binding.materialization.to_string(),
                    detail: anyhow::anyhow!("unknown constraint type {}", constraint.r#type),
                }
                .push(scope, errors);
                Type::FieldForbidden
            }
        };
        let reason = constraint.reason.as_str();

        if matches!(type_, Type::LocationRequired) {
            // Mark that this location must be selected.
            locations.entry(ptr.clone()).or_insert(false);
        }

        // Has this pointer been selected already, via another projection?
        let is_selected_ptr = locations.get(ptr).cloned().unwrap_or_default();
        // What's the index of this pointer in the composite key (if any)?
        let key_index = key_ptrs.iter().enumerate().find(|(_, k)| *k == ptr);

        let resolution = match (
            include.get(&models::Field::new(field)).is_some(),
            exclude.iter().any(|f| f.as_str() == field),
            type_,
        ) {
            // Selector / driver constraints conflict internally:
            (true, true, _) => panic!("included and excluded (should have been filtered)"),
            (_, _, Type::Unsatisfiable) => Err(format!(
                "driver reports as unsatisfiable with reason: {}",
                reason
            )),
            // Selector / driver constraints conflict with each other:
            (true, false, Type::FieldForbidden) => Err(format!(
                "selector includes field, but driver forbids it with reason: {}",
                reason
            )),
            (false, true, Type::FieldRequired) => Err(format!(
                "selector excludes field, but driver requires it with reason: {}",
                reason
            )),

            // Field is required by selector or driver.
            (true, false, _) | (false, false, Type::FieldRequired) => Ok(true),
            // Field is forbidden by selector or driver.
            (false, true, _) | (false, false, Type::FieldForbidden) => Ok(false),
            // Location is required and is not yet selected.
            (false, false, Type::LocationRequired) if !is_selected_ptr => Ok(true),
            // We desire recommended fields, and this location is unseen & recommended.
            // (Note we'll visit a user-provided projection of the location before an inferred one).
            (false, false, Type::LocationRecommended) if !is_selected_ptr && *recommended => {
                Ok(true)
            }

            // Cases where we don't include the field.
            (false, false, Type::FieldOptional) => Ok(false),
            (false, false, Type::LocationRequired) => {
                assert!(is_selected_ptr);
                Ok(false)
            }
            (false, false, Type::LocationRecommended) => {
                assert!(is_selected_ptr || !*recommended);
                Ok(false)
            }
        };

        match resolution {
            Err(reason) => {
                Error::FieldUnsatisfiable {
                    name: materialization_binding.materialization.to_string(),
                    field: field.to_string(),
                    reason,
                }
                .push(scope, errors);
            }
            Ok(false) => { /* No action. */ }
            Ok(true) => {
                let key_slot = key_index.and_then(|(i, _)| keys.get_mut(i));

                // Add to one of |keys|, |document| or |values|.
                if let Some(slot @ None) = key_slot {
                    *slot = Some(field.clone());
                } else if ptr == "" && document == "" {
                    document = field.clone();
                } else {
                    values.push(field.clone());
                }

                // Pass-through JSON-encoded field configuration.
                if let Some(cfg) = include.get(&models::Field::new(field)) {
                    field_config.insert(field.clone(), serde_json::to_string(cfg).unwrap());
                }
                // Mark location as having been selected.
                locations.insert(ptr.clone(), true);
            }
        }
    }

    // Any left-over constraints were unexpectedly not in |projections|.
    for (field, _) in constraints {
        Error::MaterializationDriver {
            name: materialization_binding.materialization.to_string(),
            detail: anyhow::anyhow!("driver sent constraint for unknown field {}", field),
        }
        .push(scope, errors);
    }
    // Any required but unmatched locations are an error.
    for (location, found) in locations {
        if !found {
            Error::LocationUnsatisfiable {
                name: materialization_binding.materialization.to_string(),
                location,
            }
            .push(scope, errors);
        }
    }

    values.sort(); // Must be sorted within FieldSelection.

    flow::FieldSelection {
        keys: keys.into_iter().filter_map(|k| k).collect(),
        values,
        document,
        field_config_json: field_config,
    }
}
