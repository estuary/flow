use super::{
    collection, indexed, reference, storage_mapping, walk_transition, Connectors, Error,
    NoOpConnectors, Scope,
};
use futures::SinkExt;
use itertools::Itertools;
use proto_flow::{flow, materialize, ops::log::Level as LogLevel};
use std::collections::{BTreeMap, HashMap};
use tables::EitherOrBoth as EOB;

pub async fn walk_all_materializations<C: Connectors>(
    pub_id: models::Id,
    build_id: models::Id,
    draft_materializations: &tables::DraftMaterializations,
    live_materializations: &tables::LiveMaterializations,
    built_collections: &tables::BuiltCollections,
    connectors: &C,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    dependencies: &tables::Dependencies<'_>,
    noop_materializations: bool,
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) -> tables::BuiltMaterializations {
    // Outer join of live and draft materializations.
    let it = live_materializations.outer_join(
        draft_materializations
            .iter()
            .map(|r| (&r.materialization, r)),
        |eob| match eob {
            EOB::Left(live) => Some(EOB::Left(live)),
            EOB::Right((_materialization, draft)) => Some(EOB::Right(draft)),
            EOB::Both(live, (_materialization, draft)) => Some(EOB::Both(live, draft)),
        },
    );

    let futures: Vec<_> = it
        .map(|eob| async {
            let mut local_errors = tables::Errors::new();

            let built_capture = walk_materialization(
                pub_id,
                build_id,
                eob,
                built_collections,
                connectors,
                data_planes,
                default_plane_id,
                dependencies,
                noop_materializations,
                storage_mappings,
                &mut local_errors,
            )
            .await;

            (built_capture, local_errors)
        })
        .collect();

    // Evaluate all validations concurrently.
    let outcomes = futures::future::join_all(futures).await;

    outcomes
        .into_iter()
        .filter_map(|(built, local_errors)| {
            errors.extend(local_errors.into_iter());
            built
        })
        .collect()
}

async fn walk_materialization<C: Connectors>(
    pub_id: models::Id,
    build_id: models::Id,
    eob: EOB<&tables::LiveMaterialization, &tables::DraftMaterialization>,
    built_collections: &tables::BuiltCollections,
    connectors: &C,
    data_planes: &tables::DataPlanes,
    default_plane_id: Option<models::Id>,
    dependencies: &tables::Dependencies<'_>,
    noop_materializations: bool,
    storage_mappings: &tables::StorageMappings,
    errors: &mut tables::Errors,
) -> Option<tables::BuiltMaterialization> {
    let (
        materialization,
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

    let models::MaterializationDef {
        source_capture: _,
        endpoint,
        bindings: all_bindings,
        shards: shard_template,
        expect_pub_id: _,
        delete: _,
        on_incompatible_schema_change: _,
    } = model;

    indexed::walk_name(
        scope,
        "materialization",
        materialization,
        models::Materialization::regex(),
        errors,
    );

    // Unwrap `endpoint` into a connector type and configuration.
    let (connector_type, config_json) = match endpoint {
        models::MaterializationEndpoint::Connector(config) => (
            flow::materialization_spec::ConnectorType::Image as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::MaterializationEndpoint::Local(config) => (
            flow::materialization_spec::ConnectorType::Local as i32,
            serde_json::to_string(config).unwrap(),
        ),
        models::MaterializationEndpoint::Dekaf(config) => (
            flow::materialization_spec::ConnectorType::Dekaf as i32,
            serde_json::to_string(config).unwrap(),
        ),
    };
    // Resolve the data-plane for this task. We cannot continue without it.
    let data_plane =
        reference::walk_data_plane(scope, materialization, data_plane_id, data_planes, errors)?;

    // Start an RPC with the task's connector.
    let (mut request_tx, request_rx) = futures::channel::mpsc::channel(1);
    let response_rx = if noop_materializations || shard_template.disable {
        futures::future::Either::Left(NoOpConnectors.materialize(
            data_plane,
            materialization,
            request_rx,
        ))
    } else {
        futures::future::Either::Right(connectors.materialize(
            data_plane,
            materialization,
            request_rx,
        ))
    };
    futures::pin_mut!(response_rx);

    // Send Request.Spec and receive Response.Spec.
    _ = request_tx
        .send(
            materialize::Request {
                spec: Some(materialize::request::Spec {
                    connector_type,
                    config_json: config_json.clone(),
                }),
                ..Default::default()
            }
            .with_internal(|internal| {
                if let Some(s) = &shard_template.log_level {
                    internal.set_log_level(LogLevel::from_str_name(s).unwrap_or_default());
                }
            }),
        )
        .await;

    let materialize::response::Spec {
        documentation_url: _,
        config_schema_json: _,
        resource_config_schema_json: _,
        resource_path_pointers: _,
        ..
    } = super::expect_response(
        scope,
        &mut response_rx,
        |response| Ok(response.spec.take()),
        errors,
    )
    .await?;

    // We only validate and build enabled bindings, in their declaration order.
    let enabled_bindings: Vec<(usize, &models::MaterializationBinding)> = all_bindings
        .iter()
        .enumerate()
        .filter_map(|(index, binding)| (!binding.disable).then_some((index, binding)))
        .collect();

    // Map enabled bindings into validation requests.
    let binding_requests: Vec<_> = enabled_bindings
        .iter()
        .filter_map(|(binding_index, binding)| {
            walk_materialization_binding(
                scope.push_prop("bindings").push_item(*binding_index),
                materialization,
                binding,
                built_collections,
                data_plane_id,
                source_exclusions(live_model, binding.source.collection()),
                errors,
            )
        })
        .collect();

    // Determine storage mappings for task recovery logs.
    let recovery_stores = storage_mapping::mapped_stores(
        scope,
        "materialization",
        &format!("recovery/{materialization}"),
        storage_mappings,
        errors,
    );

    // We've completed all cheap validation checks.
    // If we've already encountered errors then stop now.
    if !errors.is_empty() {
        return None;
    }

    let validate_request = materialize::request::Validate {
        name: materialization.to_string(),
        connector_type,
        config_json: config_json.clone(),
        bindings: binding_requests.clone(),
        last_materialization: live_spec.cloned(),
        last_version: if expect_pub_id.is_zero() {
            String::new()
        } else {
            expect_pub_id.to_string()
        },
    };

    // Send Request.Validate and receive Response.Validated.
    _ = request_tx
        .send(
            materialize::Request {
                validate: Some(validate_request.clone()),
                ..Default::default()
            }
            .with_internal(|internal| {
                if let Some(s) = &shard_template.log_level {
                    internal.set_log_level(LogLevel::from_str_name(s).unwrap_or_default());
                }
            }),
        )
        .await;

    let (validated_response, network_ports) = super::expect_response(
        scope,
        &mut response_rx,
        |response| {
            let network_ports = match response.get_internal() {
                Ok(internal) => internal.container.unwrap_or_default().network_ports,
                Err(err) => return Err(anyhow::anyhow!("parsing internal: {err}")),
            };
            Ok(response.validated.take().map(|v| (v, network_ports)))
        },
        errors,
    )
    .await?;

    let materialize::response::Validated {
        bindings: binding_responses,
    } = &validated_response;

    if enabled_bindings.len() != binding_responses.len() {
        Error::WrongConnectorBindings {
            expect: binding_requests.len(),
            got: binding_responses.len(),
        }
        .push(scope, errors);
    }

    // Jointly walk binding models, validate requests, and validated responses to produce built bindings.
    let mut built_bindings = Vec::new();

    for ((index, model), (request, response)) in enabled_bindings.iter().zip(
        binding_requests
            .into_iter()
            .zip(binding_responses.into_iter()),
    ) {
        let materialize::request::validate::Binding {
            resource_config_json,
            collection,
            field_config_json_map: _,
            backfill,
        } = request;

        let materialize::response::validated::Binding {
            constraints,
            delta_updates,
            resource_path,
        } = response;

        let models::MaterializationBinding {
            source,
            fields,
            disable: _,
            priority,
            resource: _,
            backfill: _,
            on_incompatible_schema_change: _,
        } = model;

        let field_selection = Some(walk_materialization_response(
            scope.push_prop("bindings").push_item(*index),
            materialization,
            fields,
            collection.as_ref().unwrap(),
            constraints.clone(),
            errors,
        ));

        // Build a partition LabelSelector for this source.
        let (source_partitions, not_before, not_after) = match source {
            models::Source::Collection(_name) => (None, None, None),
            models::Source::Source(models::FullSource {
                name: _,
                partitions,
                not_before,
                not_after,
            }) => (partitions.as_ref(), not_before.as_ref(), not_after.as_ref()),
        };
        let partition_selector = Some(assemble::journal_selector(
            collection.as_ref().unwrap(),
            source_partitions,
        ));

        // Build a state key and read suffix using the transform name as it's resource path.
        let state_key = assemble::encode_state_key(resource_path, backfill);
        let journal_read_suffix = format!("materialize/{materialization}/{state_key}");

        built_bindings.push(flow::materialization_spec::Binding {
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
            backfill,
            state_key,
        });
    }

    // Look for (and error on) duplicated resource paths within the bindings.
    for ((path, (l_index, _)), (_, (r_index, _))) in binding_responses
        .iter()
        .map(|r| &r.resource_path)
        .zip(enabled_bindings.iter())
        .sorted_by(|(l_path, _), (r_path, _)| l_path.cmp(r_path))
        .tuple_windows()
        .filter(|((l_path, _), (r_path, _))| l_path == r_path)
    {
        let scope = scope.push_prop("bindings");
        let lhs_scope = scope.push_item(*l_index);
        let rhs_scope = scope.push_item(*r_index).flatten();

        Error::BindingDuplicatesResource {
            entity: "materialization",
            name: materialization.to_string(),
            resource: path.iter().join("."),
            rhs_scope,
        }
        .push(lhs_scope, errors);
    }

    // Pluck out the current shard ID prefix, or create a unique one if it doesn't exist.
    let shard_id_prefix = if let Some(flow::MaterializationSpec {
        shard_template: Some(shard_template),
        ..
    }) = live_spec
    {
        shard_template.id.clone()
    } else {
        let pub_id = match endpoint {
            // Dekaf materializations don't create any shards, so the problem of
            // deleting and re-creating tasks with the same name, which this
            // shard id template logic was introduced to resolve, isn't applicable.
            // Instead, since the Dekaf service uses the task name to authenticate
            // whereas the authorization API expects the shard template id, it's
            // useful to be able to generate the correct shard template id for a
            // Dekaf materialization given only its task name, so we set the pub id
            // to a well-known value of all zeros.
            models::MaterializationEndpoint::Dekaf(_) => models::Id::zero(),
            models::MaterializationEndpoint::Connector(_)
            | models::MaterializationEndpoint::Local(_) => pub_id,
        };

        assemble::shard_id_prefix(pub_id, materialization, labels::TASK_TYPE_MATERIALIZATION)
    };

    let recovery_log_template = assemble::recovery_log_template(
        build_id,
        materialization,
        labels::TASK_TYPE_MATERIALIZATION,
        &shard_id_prefix,
        recovery_stores,
    );
    let shard_template = assemble::shard_template(
        build_id,
        materialization,
        labels::TASK_TYPE_MATERIALIZATION,
        shard_template,
        &shard_id_prefix,
        false, // Don't disable wait_for_ack.
        &network_ports,
    );
    let built_spec = flow::MaterializationSpec {
        name: materialization.to_string(),
        connector_type,
        config_json,
        bindings: built_bindings,
        recovery_log_template: Some(recovery_log_template),
        shard_template: Some(shard_template),
        network_ports,
        inactive_bindings: Vec::new(),
    };

    let dependency_hash = dependencies.compute_hash(model);
    Some(tables::BuiltMaterialization {
        materialization: materialization.clone(),
        scope: scope.flatten(),
        control_id,
        data_plane_id,
        expect_pub_id,
        expect_build_id,
        model: Some(model.clone()),
        model_fixes: Vec::new(),
        validated: Some(validated_response),
        spec: Some(built_spec),
        previous_spec: live_spec.cloned(),
        is_touch,
        dependency_hash,
    })
}

fn walk_materialization_binding<'a>(
    scope: Scope<'a>,
    catalog_name: &models::Materialization,
    binding: &'a models::MaterializationBinding,
    built_collections: &'a tables::BuiltCollections,
    data_plane_id: models::Id,
    prior_exclusions: impl Iterator<Item = &'a models::Field> + Clone,
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
        backfill,
        on_incompatible_schema_change: _,
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
    let (source_spec, source) = reference::walk_reference(
        scope,
        "this materialization binding",
        collection,
        built_collections,
        errors,
    )?;

    if let Some(selector) = source_partitions {
        collection::walk_selector(scope, &source_spec, &selector, errors);
    }

    let field_config_json_map = walk_materialization_fields(
        scope.push_prop("fields"),
        catalog_name,
        &source_spec,
        fields_include,
        fields_exclude,
        prior_exclusions,
        errors,
    );

    super::temporary_cross_data_plane_read_check(scope, source, data_plane_id, errors);

    let request = materialize::request::validate::Binding {
        resource_config_json: resource.to_string(),
        collection: Some(source_spec),
        field_config_json_map,
        backfill: *backfill,
    };

    Some(request)
}

fn walk_materialization_fields<'a>(
    scope: Scope,
    catalog_name: &models::Materialization,
    collection: &flow::CollectionSpec,
    include: &BTreeMap<models::Field, models::RawValue>,
    exclude: &[models::Field],
    prior_exclusions: impl Iterator<Item = &'a models::Field> + Clone,
    errors: &mut tables::Errors,
) -> BTreeMap<String, String> {
    let flow::CollectionSpec {
        name, projections, ..
    } = collection;

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

        if projections.iter().any(|p| p.field == field.as_str()) {
            // Exclusion matches an existing collection projection.
        } else if prior_exclusions.clone().any(|prior| field == prior) {
            // As a special case, if this exclusion was also present in the prior
            // model of this spec then allow it to carry forward without an error.
            // This is to avoid breaking tasks which exclude inferred schema
            // locations which may go away upon a simplification of the inferred
            // schema (e.g. because they're collapsed into additionalProperties).
        } else {
            Error::NoSuchProjection {
                category: "exclude".to_string(),
                field: field.to_string(),
                collection: name.clone(),
            }
            .push(scope, errors);
        }
        if include.contains_key(field) {
            Error::FieldUnsatisfiable {
                name: catalog_name.to_string(),
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

        let type_ = match Type::try_from(constraint.r#type) {
            Err(_) | Ok(Type::Invalid) => {
                Error::Connector {
                    detail: anyhow::anyhow!("unknown constraint type {}", constraint.r#type),
                }
                .push(scope, errors);
                Type::FieldForbidden
            }
            Ok(t) => t,
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
            // Unsatisfiable is OK only if the field is explicitly excluded
            (_, false, Type::Unsatisfiable) => Err(format!(
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

// Build a Iterator + Clone over all models::Fields excluded by any
// binding of `source` in `model`. The sequence may include duplicates.
fn source_exclusions<'m>(
    model: Option<&'m models::MaterializationDef>,
    source: &'m models::Collection,
) -> impl Iterator<Item = &'m models::Field> + Clone + 'm {
    model
        .into_iter()
        .map(move |model| {
            model
                .bindings
                .iter()
                .filter_map(move |binding| {
                    if !binding.disable && binding.source.collection() == source {
                        Some(binding.fields.exclude.iter())
                    } else {
                        None
                    }
                })
                .flatten()
        })
        .flatten()
}
