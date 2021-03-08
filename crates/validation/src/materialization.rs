use super::{indexed, reference, Drivers, Error};
use futures::FutureExt;
use itertools::Itertools;
use models::{names, tables};
use protocol::{flow, materialize};
use std::collections::{BTreeMap, HashMap};
use url::Url;

pub async fn walk_all_materializations<D: Drivers>(
    drivers: &D,
    built_collections: &[tables::BuiltCollection],
    collections: &[tables::Collection],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    materializations: &[tables::Materialization],
    errors: &mut tables::Errors,
) -> tables::BuiltMaterializations {
    let mut validations = Vec::new();

    for materialization in materializations {
        validations.extend(
            walk_materialization_request(
                built_collections,
                collections,
                endpoints,
                imports,
                materialization,
                errors,
            )
            .into_iter(),
        );
    }

    indexed::walk_duplicates(
        "materialization",
        materializations
            .iter()
            .map(|m| (&m.materialization, &m.scope)),
        errors,
    );

    // Run all validations concurrently.
    let validations = validations.into_iter().map(
        |(ep_type, ep_config, built_collection, materialization, request)| async move {
            drivers
                .validate_materialization(request)
                // Pass-through the materialization & CollectionSpec for future verification.
                .map(|response| {
                    (
                        ep_type,
                        ep_config,
                        built_collection,
                        materialization,
                        response,
                    )
                })
                .await
        },
    );
    let validations = futures::future::join_all(validations).await;

    let mut built_materializations = tables::BuiltMaterializations::new();

    for (ep_type, ep_config, built_collection, materialization, response) in validations {
        match response {
            Ok(response) => {
                let fields = walk_materialization_response(
                    built_collection,
                    materialization,
                    response,
                    errors,
                );
                let spec = models::build::materialization_spec(
                    materialization,
                    built_collection,
                    ep_type,
                    &ep_config,
                    fields,
                );

                built_materializations.push_row(
                    &materialization.scope,
                    &materialization.materialization,
                    &materialization.collection,
                    ep_type,
                    ep_config,
                    spec,
                );
            }
            Err(err) => {
                Error::MaterializationDriver {
                    name: materialization.materialization.to_string(),
                    detail: err,
                }
                .push(&materialization.scope, errors);
            }
        }
    }

    // Require that tuples of (collection, endpoint_type, endpoint_config) are globally unique.
    let cmp = |lhs: &&tables::BuiltMaterialization, rhs: &&tables::BuiltMaterialization| {
        (&lhs.collection, &lhs.endpoint_type)
            .cmp(&(&rhs.collection, &rhs.endpoint_type))
            .then_with(|| json::json_cmp(&lhs.endpoint_config, &rhs.endpoint_config))
    };
    for (lhs, rhs) in built_materializations.iter().sorted_by(cmp).tuple_windows() {
        if cmp(&lhs, &rhs) == std::cmp::Ordering::Equal {
            Error::MaterializationMultiplePushes {
                lhs_name: lhs.materialization.to_string(),
                rhs_name: rhs.materialization.to_string(),
                rhs_scope: rhs.scope.clone(),
                target: lhs.collection.to_string(),
            }
            .push(&lhs.scope, errors);
        }
    }

    built_materializations
}

fn walk_materialization_request<'a>(
    built_collections: &'a [tables::BuiltCollection],
    collections: &[tables::Collection],
    endpoints: &[tables::Endpoint],
    imports: &[&tables::Import],
    materialization: &'a tables::Materialization,
    errors: &mut tables::Errors,
) -> Option<(
    flow::EndpointType,
    serde_json::Value,
    &'a tables::BuiltCollection,
    &'a tables::Materialization,
    materialize::ValidateRequest,
)> {
    let tables::Materialization {
        scope,
        collection: source,
        endpoint,
        fields_exclude,
        fields_include,
        fields_recommended: _,
        materialization: name,
        patch_config,
    } = materialization;

    indexed::walk_name(
        scope,
        "materialization",
        name.as_ref(),
        &indexed::MATERIALIZATION_RE,
        errors,
    );

    let source = reference::walk_reference(
        scope,
        "materialization",
        name,
        "collection",
        source,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    );

    let endpoint = reference::walk_reference(
        scope,
        "materialization",
        name,
        "endpoint",
        endpoint,
        endpoints,
        |e| (&e.endpoint, &e.scope),
        imports,
        errors,
    );

    // We must resolve both |source| and |endpoint| to continue.
    let (source, endpoint) = match (source, endpoint) {
        (Some(s), Some(e)) => (s, e),
        _ => return None,
    };

    let built_collection = built_collections
        .iter()
        .find(|c| c.collection == source.collection)
        .unwrap();

    let mut endpoint_config = endpoint.base_config.clone();
    json_patch::merge(&mut endpoint_config, &patch_config);

    let field_config = walk_materialization_fields(
        scope,
        built_collection,
        fields_include,
        fields_exclude,
        errors,
    );

    let request = materialize::ValidateRequest {
        endpoint_type: endpoint.endpoint_type as i32,
        endpoint_config_json: endpoint_config.to_string(),
        collection: Some(built_collection.spec.clone()),
        field_config_json: field_config.into_iter().collect(),
    };

    Some((
        endpoint.endpoint_type,
        endpoint_config,
        built_collection,
        materialization,
        request,
    ))
}

fn walk_materialization_fields<'a>(
    scope: &Url,
    built_collection: &tables::BuiltCollection,
    include: &BTreeMap<String, names::Object>,
    exclude: &[String],
    errors: &mut tables::Errors,
) -> Vec<(String, String)> {
    let flow::CollectionSpec {
        collection,
        projections,
        ..
    } = &built_collection.spec;

    let mut bag = Vec::new();

    for (field, config) in include {
        if projections.iter().any(|p| p.field == *field) {
            bag.push((field.clone(), serde_json::to_string(config).unwrap()));
        } else {
            Error::NoSuchProjection {
                category: "include".to_string(),
                field: field.clone(),
                collection: collection.clone(),
            }
            .push(scope, errors);
        }
    }

    for field in exclude {
        if !projections.iter().any(|p| p.field == *field) {
            Error::NoSuchProjection {
                category: "exclude".to_string(),
                field: field.clone(),
                collection: collection.clone(),
            }
            .push(scope, errors);
        }
    }

    bag
}

fn walk_materialization_response(
    built_collection: &tables::BuiltCollection,
    materialization: &tables::Materialization,
    response: materialize::ValidateResponse,
    errors: &mut tables::Errors,
) -> flow::FieldSelection {
    let tables::Materialization {
        scope,
        materialization: name,
        fields_include: include,
        fields_exclude: exclude,
        fields_recommended: recommended,
        ..
    } = materialization;

    let flow::CollectionSpec {
        projections,
        key_ptrs,
        ..
    } = &built_collection.spec;

    let materialize::ValidateResponse { mut constraints } = response;

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
            let must_include = include.get(&p.field).is_some()
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
                    name: name.to_string(),
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
            include.get(field).is_some(),
            exclude.iter().any(|f| f == field),
            type_,
        ) {
            // Selector / driver constraints conflict internally:
            (true, true, _) => Err(format!("field is both included and excluded by selector")),
            (_, _, Type::Unsatisfiable) => Err(format!(
                "driver reports as unsatisfiable with reason: {}",
                reason
            )),
            // Selector / driver constraints conflict with each other:
            (true, false, Type::FieldForbidden) => Err(format!(
                "selector includes field, but driver forbids it with reason reason: {}",
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
                    materialization: name.to_string(),
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
                if let Some(cfg) = include.get(field) {
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
            name: name.to_string(),
            detail: anyhow::anyhow!("driver sent constraint for unknown field {}", field),
        }
        .push(scope, errors);
    }
    // Any required but unmatched locations are an error.
    for (location, found) in locations {
        if !found {
            Error::LocationUnsatisfiable {
                materialization: name.to_string(),
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
        field_config,
    }
}
