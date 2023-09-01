use super::{
    collection, indexed, reference, storage_mapping, Connectors, Error, NoOpConnectors, Scope,
};
use itertools::Itertools;
use proto_flow::{flow, materialize, ops::log::Level as LogLevel};
use std::collections::{BTreeMap, HashMap};

pub async fn walk_all_materializations(
    build_id: &str,
    built_collections: &[tables::BuiltCollection],
    connectors: &dyn Connectors,
    materializations: &[tables::Materialization],
    storage_mappings: &[tables::StorageMapping],
    errors: &mut tables::Errors,
) -> tables::BuiltMaterializations {
    let mut validations = Vec::new();

    for materialization in materializations {
        let mut materialization_errors = tables::Errors::new();
        let validation = walk_materialization_request(
            built_collections,
            materialization,
            &mut materialization_errors,
        );

        // Skip validation if errors were encountered while building the request.
        if !materialization_errors.is_empty() {
            errors.extend(materialization_errors.into_iter());
        } else if let Some(validation) = validation {
            validations.push(validation);
        }
    }

    // Run all validations concurrently.
    let validations = validations
        .into_iter()
        .map(|(materialization, request)| async move {
            let mut wrapped = materialize::Request {
                validate: Some(request.clone()),
                ..Default::default()
            };

            if let Some(log_level) = materialization
                .spec
                .shards
                .log_level
                .as_ref()
                .and_then(|s| LogLevel::from_str_name(s))
            {
                wrapped.set_internal_log_level(log_level);
            }

            // If shards are disabled, then don't ask the connector to validate.
            // A broken but disabled endpoint should not cause a build to fail.
            let response = if materialization.spec.shards.disable {
                NoOpConnectors.validate_materialization(wrapped)
            } else {
                connectors.validate_materialization(wrapped)
            };
            (materialization, request, response.await)
        });

    let validations: Vec<(
        &tables::Materialization,
        materialize::request::Validate,
        anyhow::Result<materialize::Response>,
    )> = futures::future::join_all(validations).await;

    let mut built_materializations = tables::BuiltMaterializations::new();

    for (materialization, mut request, response) in validations {
        let tables::Materialization {
            scope,
            materialization,
            spec:
                models::MaterializationDef {
                    shards,
                    bindings: binding_models,
                    ..
                },
        } = materialization;
        let scope = Scope::new(scope);

        // Unwrap `response` and bail out if it failed.
        let (validated, network_ports) = match extract_validated(response) {
            Err(err) => {
                err.push(scope, errors);
                continue;
            }
            Ok(ok) => ok,
        };

        let materialize::request::Validate {
            connector_type,
            config_json,
            bindings: binding_requests,
            name,
        } = &mut request;

        let materialize::response::Validated {
            bindings: binding_responses,
        } = &validated;

        if binding_requests.len() != binding_responses.len() {
            Error::WrongConnectorBindings {
                expect: binding_requests.len(),
                got: binding_responses.len(),
            }
            .push(scope, errors);
        }

        // Join requests and responses to produce tuples
        // of (binding index, built binding).
        let built_bindings: Vec<_> = std::mem::take(binding_requests)
            .into_iter()
            .zip(binding_responses.into_iter())
            .enumerate()
            .map(|(binding_index, (binding_request, binding_response))| {
                let materialize::request::validate::Binding {
                    resource_config_json,
                    collection,
                    field_config_json_map: _,
                } = binding_request;

                let materialize::response::validated::Binding {
                    constraints,
                    delta_updates,
                    resource_path,
                } = binding_response;

                // When we lookup the binding in the model, we need to account
                // for the presence of disabled bindings, which would cause
                // binding indexes to differ between the model and the specs
                // from the validation request/response.
                let models::MaterializationBinding {
                    ref source,
                    ref fields,
                    disable: _,
                    priority,
                    resource: _,
                } = binding_models
                    .iter()
                    .filter(|b| !b.disable)
                    .nth(binding_index)
                    .expect("models bindings are consistent with validation requests bindings");

                let field_selection = Some(walk_materialization_response(
                    scope.push_prop("bindings").push_item(binding_index),
                    materialization,
                    fields,
                    collection.as_ref().unwrap(),
                    constraints.clone(),
                    errors,
                ));

                let (source_name, source_partitions, not_before, not_after) = match source {
                    models::Source::Collection(name) => (name, None, None, None),
                    models::Source::Source(models::FullSource {
                        name,
                        partitions,
                        not_before,
                        not_after,
                    }) => (
                        name,
                        partitions.as_ref(),
                        not_before.as_ref(),
                        not_after.as_ref(),
                    ),
                };
                let partition_selector =
                    Some(assemble::journal_selector(source_name, source_partitions));

                let journal_read_suffix = format!(
                    "materialize/{}/{}",
                    materialization,
                    assemble::encode_resource_path(resource_path),
                );

                (
                    binding_index,
                    flow::materialization_spec::Binding {
                        resource_config_json,
                        resource_path: resource_path.clone(),
                        collection,
                        partition_selector,
                        priority: *priority,
                        field_selection,
                        delta_updates: *delta_updates,
                        deprecated_shuffle: None,
                        journal_read_suffix,
                        not_before: not_before.map(assemble::pb_datetime),
                        not_after: not_after.map(assemble::pb_datetime),
                    },
                )
            })
            .collect();

        // Look for (and error on) duplicated resource paths within the bindings.
        for ((l_index, _), (r_index, binding)) in built_bindings
            .iter()
            .sorted_by(|(_, l), (_, r)| l.resource_path.cmp(&r.resource_path))
            .tuple_windows()
            .filter(|((_, l), (_, r))| l.resource_path == r.resource_path)
        {
            let scope = scope.push_prop("bindings");
            let lhs_scope = scope.push_item(*l_index);
            let rhs_scope = scope.push_item(*r_index).flatten();

            Error::BindingDuplicatesResource {
                entity: "materialization",
                name: name.to_string(),
                resource: binding.resource_path.iter().join("."),
                rhs_scope,
            }
            .push(lhs_scope, errors);
        }

        // Unzip to strip binding indices, leaving built bindings.
        let (_, built_bindings): (Vec<_>, Vec<_>) = built_bindings.into_iter().unzip();

        let recovery_stores = storage_mapping::mapped_stores(
            scope,
            "materialization",
            &format!("recovery/{}", name.as_str()),
            storage_mappings,
            errors,
        );

        let spec = flow::MaterializationSpec {
            name: name.clone(),
            connector_type: *connector_type,
            config_json: std::mem::take(config_json),
            bindings: built_bindings,
            recovery_log_template: Some(assemble::recovery_log_template(
                build_id,
                &name,
                labels::TASK_TYPE_MATERIALIZATION,
                recovery_stores,
            )),
            shard_template: Some(assemble::shard_template(
                build_id,
                &name,
                labels::TASK_TYPE_MATERIALIZATION,
                shards,
                false, // Don't disable wait_for_ack.
                &network_ports,
            )),
            network_ports,
        };
        built_materializations.insert_row(scope.flatten(), std::mem::take(name), validated, spec);
    }

    built_materializations
}

fn walk_materialization_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    materialization: &'a tables::Materialization,
    errors: &mut tables::Errors,
) -> Option<(&'a tables::Materialization, materialize::request::Validate)> {
    let tables::Materialization {
        scope,
        materialization: name,
        spec: models::MaterializationDef {
            endpoint, bindings, ..
        },
    } = materialization;
    let scope = Scope::new(scope);

    // Require the materialization name is valid.
    indexed::walk_name(
        scope,
        "materialization",
        &materialization.materialization,
        models::Materialization::regex(),
        errors,
    );

    let (connector_type, config_json) = match endpoint {
        models::MaterializationEndpoint::Connector(config) => (
            flow::materialization_spec::ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::MaterializationEndpoint::Sqlite(sqlite) => (
            flow::materialization_spec::ConnectorType::Sqlite as i32,
            serde_json::to_string(sqlite).unwrap(),
        ),
    };

    let bindings = bindings
        .iter()
        .enumerate()
        // Filter the bindings that we send to the connector to only those that are enabled.
        .filter(|(_, b)| !b.disable)
        .map(|(binding_index, binding)| {
            walk_materialization_binding(
                scope.push_prop("bindings").push_item(binding_index),
                name,
                binding,
                built_collections,
                errors,
            )
        })
        // Force eager evaluation of all results.
        .collect::<Vec<Option<_>>>()
        .into_iter()
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .collect();

    let request = materialize::request::Validate {
        name: name.to_string(),
        connector_type,
        config_json,
        bindings,
    };

    Some((materialization, request))
}

fn walk_materialization_binding<'a>(
    scope: Scope,
    materialization: &str,
    binding: &models::MaterializationBinding,
    built_collections: &'a [tables::BuiltCollection],
    errors: &mut tables::Errors,
) -> Option<materialize::request::validate::Binding> {
    let models::MaterializationBinding {
        resource,
        source,
        fields:
            models::MaterializationFields {
                include: fields_include,
                exclude: fields_exclude,
                recommended: _,
            },
        disable: _,
        priority: _,
    } = binding;

    let (collection, source_partitions) = match source {
        models::Source::Collection(collection) => (collection, None),
        models::Source::Source(models::FullSource {
            name,
            partitions,
            not_before,
            not_after,
        }) => {
            if let (Some(not_before), Some(not_after)) = (not_before, not_after) {
                if not_before > not_after {
                    Error::NotBeforeAfterOrder.push(scope.push_prop("source"), errors);
                }
            }
            (name, partitions.as_ref())
        }
    };

    // We must resolve the source collection to continue.
    let built_collection = reference::walk_reference(
        scope,
        "this materialization binding",
        "collection",
        collection,
        built_collections,
        |c| (&c.collection, Scope::new(&c.scope)),
        errors,
    )?;

    if let Some(selector) = source_partitions {
        collection::walk_selector(scope, &built_collection.spec, &selector, errors);
    }

    let field_config_json_map = walk_materialization_fields(
        scope.push_prop("fields"),
        materialization,
        built_collection,
        fields_include,
        fields_exclude,
        errors,
    );

    let request = materialize::request::validate::Binding {
        resource_config_json: resource.to_string(),
        collection: Some(built_collection.spec.clone()),
        field_config_json_map,
    };

    Some(request)
}

fn walk_materialization_fields<'a>(
    scope: Scope,
    materialization: &str,
    built_collection: &tables::BuiltCollection,
    include: &BTreeMap<models::Field, models::RawValue>,
    exclude: &[models::Field],
    errors: &mut tables::Errors,
) -> BTreeMap<String, String> {
    let flow::CollectionSpec {
        name, projections, ..
    } = &built_collection.spec;

    let mut bag = BTreeMap::new();

    for (field, config) in include {
        let scope = scope.push_prop("include");
        let scope = scope.push_prop(field);

        if projections.iter().any(|p| p.field == field.as_str()) {
            bag.insert(field.to_string(), config.to_string());
        } else {
            Error::NoSuchProjection {
                category: "include".to_string(),
                field: field.to_string(),
                collection: name.clone(),
            }
            .push(scope, errors);
        }
    }

    for (index, field) in exclude.iter().enumerate() {
        let scope = scope.push_prop("exclude");
        let scope = scope.push_item(index);

        if !projections.iter().any(|p| p.field == field.as_str()) {
            Error::NoSuchProjection {
                category: "exclude".to_string(),
                field: field.to_string(),
                collection: name.clone(),
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
    scope: Scope,
    materialization: &models::Materialization,
    fields: &models::MaterializationFields,
    collection: &flow::CollectionSpec,
    mut constraints: BTreeMap<String, materialize::response::validated::Constraint>,
    errors: &mut tables::Errors,
) -> flow::FieldSelection {
    let models::MaterializationFields {
        include,
        exclude,
        recommended,
    } = fields;

    let flow::CollectionSpec {
        projections,
        key: key_ptrs,
        ..
    } = collection;

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
    let mut field_config_json_map = BTreeMap::new();

    use materialize::response::validated::constraint::Type;

    // Sort projections so that we walk, in order:
    // * Fields which *must* be included.
    // * Fields which are explicitly-defined, and should be selected preferentially
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

            (!must_include, !p.explicit) // Negate to order before.
        })
        .collect::<Vec<_>>();

    for projection in projections {
        let flow::Projection { ptr, field, .. } = projection;

        let constraint =
            constraints
                .remove(field)
                .unwrap_or(materialize::response::validated::Constraint {
                    r#type: Type::FieldForbidden as i32,
                    reason: String::new(),
                });

        let type_ = match Type::from_i32(constraint.r#type) {
            None | Some(Type::Invalid) => {
                Error::Connector {
                    detail: anyhow::anyhow!("unknown constraint type {}", constraint.r#type),
                }
                .push(scope, errors);
                Type::FieldForbidden
            }
            Some(t) => t,
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
            // Selector / connector constraints conflict internally:
            (true, true, _) => panic!("included and excluded (should have been filtered)"),
            (_, _, Type::Unsatisfiable) => Err(format!(
                "connector reports as unsatisfiable with reason: {}",
                reason
            )),
            // Selector / connector constraints conflict with each other:
            (true, false, Type::FieldForbidden) => Err(format!(
                "selector includes field, but connector forbids it with reason: {}",
                reason
            )),
            (false, true, Type::FieldRequired) => Err(format!(
                "selector excludes field, but connector requires it with reason: {}",
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
            (_, _, Type::Invalid) => unreachable!("invalid is filtered prior to this point"),
        };

        match resolution {
            Err(reason) => {
                Error::FieldUnsatisfiable {
                    name: materialization.to_string(),
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
                    field_config_json_map.insert(field.clone(), cfg.to_string());
                }
                // Mark location as having been selected.
                locations.insert(ptr.clone(), true);
            }
        }
    }

    // Any left-over constraints were unexpectedly not in |projections|.
    for (field, _) in constraints {
        Error::Connector {
            detail: anyhow::anyhow!("connector sent constraint for unknown field {}", field),
        }
        .push(scope, errors);
    }
    // Any required but unmatched locations are an error.
    for (location, found) in locations {
        if !found {
            Error::LocationUnsatisfiable {
                name: materialization.to_string(),
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
        field_config_json_map,
    }
}

fn extract_validated(
    response: anyhow::Result<materialize::Response>,
) -> Result<(materialize::response::Validated, Vec<flow::NetworkPort>), Error> {
    let response = match response {
        Ok(response) => response,
        Err(err) => return Err(Error::Connector { detail: err }),
    };

    let internal = match response.get_internal() {
        Ok(internal) => internal,
        Err(err) => {
            return Err(Error::Connector {
                detail: anyhow::anyhow!("parsing internal: {err}"),
            });
        }
    };

    let Some(validated) = response.validated else {
        return Err(Error::Connector {
            detail: anyhow::anyhow!("expected Validated but got {}", serde_json::to_string(&response).unwrap()),
        });
    };
    let network_ports = internal.container.unwrap_or_default().network_ports;

    Ok((validated, network_ports))
}
