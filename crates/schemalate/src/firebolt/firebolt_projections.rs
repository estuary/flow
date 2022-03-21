use std::collections::BTreeMap;
use std::iter::FromIterator;

use json::schema::types;
use protocol::flow::{inference::Exists, materialization_spec, CollectionSpec, FieldSelection};
use protocol::materialize::{constraint, validate_request, Constraint};
use serde_json::{json, Map};

// Can we make this a method on FieldSelection itself?
fn all_fields(fs: FieldSelection) -> Vec<String> {
    let mut fields = vec![fs.keys, fs.values].concat();
    if fs.document != "" {
        fields.push(fs.document);
    }
    fields
}

/*pub fn validate_selected_fields(
    constraints: BTreeMap<String, Constraint>,
    proposed: materialization_spec::Binding,
) -> Result<(), InvalidSelectedFields> {
    let fields = all_fields(proposed.field_selection.unwrap());
}*/

pub fn validate_new_projection(
    proposed: validate_request::Binding,
) -> BTreeMap<String, Constraint> {
    proposed
        .collection
        .unwrap()
        .projections
        .iter()
        .map(|projection| {
            let infer = projection.inference.as_ref().unwrap();
            let constraint = {
                if projection.is_primary_key {
                    Constraint {
                        r#type: constraint::Type::LocationRequired.into(),
                        reason: "All locations that are part of the collection key are required."
                            .to_string(),
                    }
                } else if projection.ptr.len() == 0 {
                    // root document
                    Constraint {
                        r#type: constraint::Type::FieldOptional.into(),
                        reason:
                            "The root document is usually not necessary in delta-update connectors."
                                .to_string(),
                    }
                } else {
                    let types = types::Set::from_iter(infer.types.iter());
                    if !types.is_single_type() {
                        Constraint {
                            r#type: constraint::Type::FieldForbidden.into(),
                            reason: "Cannot materialize field with multiple or no types."
                                .to_string(),
                        }
                    } else if types.is_single_scalar_type() {
                        Constraint {
                            r#type: constraint::Type::LocationRecommended.into(),
                            reason: "Scalar values are recommended to be materialized.".to_string(),
                        }
                    } else if matches!(types - types::NULL, types::OBJECT | types::ARRAY) {
                        Constraint {
                            r#type: constraint::Type::FieldOptional.into(),
                            reason: "Object and array fields can be materialized.".to_string(),
                        }
                    } else {
                        unreachable!("Binding is malformed!")
                    }
                }
            };

            (projection.field.clone(), constraint)
        })
        .collect()
}

/// ValidateExistingProjectionRequest used to parse stdin input for validate_existing_projection.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidateExistingProjectionRequest {
    /// Existing Materialization Binding
    #[prost(message, tag = "1")]
    pub existing_binding: Option<materialization_spec::Binding>,
    /// Proposed ValidateRequest Binding
    #[prost(message, tag = "2")]
    pub proposed_binding: Option<validate_request::Binding>,
}

pub fn validate_existing_projection(
    existing: materialization_spec::Binding,
    proposed: validate_request::Binding,
) -> BTreeMap<String, Constraint> {
    let existing_projections = existing.collection.unwrap().projections;
    let fields = all_fields(existing.field_selection.unwrap());
    let collection = proposed.collection.unwrap();

    let mut constraints: BTreeMap<String, Constraint> = {
        fields
            .iter()
            .filter_map({
                |field| {
                    let ep = existing_projections.iter().find(|p| &p.field == field).unwrap();
                    let pp = match collection.projections.iter().find(|p| &p.field == field) {
                        Some(p) => p,
                        None => return Some((field.clone(), Constraint {
                            r#type: constraint::Type::Unsatisfiable.into(),
                            reason: "The proposed materialization is missing the projection, which is required because it's included in the existing materialization".to_string()
                        }))
                    };

                    let ep_infer = ep.inference.as_ref().unwrap();
                    let pp_infer = pp.inference.as_ref().unwrap();

                    let ep_type_set = types::Set::from_iter(ep_infer.types.iter());
                    let pp_type_set = types::Set::from_iter(pp_infer.types.iter());
                    let diff = pp_type_set - ep_type_set;

                    let constraint =
                        if diff != types::INVALID {
                            let new_types: String = diff.to_vec().join(", ");
                            Constraint {
                                r#type: constraint::Type::Unsatisfiable.into(),
                                reason: format!("The proposed projection may contain types {}, which are not part of the original projection.", new_types)
                                    .to_string(),
                            }
                        } else if ep_infer.exists == i32::from(Exists::Must) &&
                                  !ep_type_set.overlaps(types::NULL) &&
                                  pp_infer.exists != i32::from(Exists::Must) {
                            Constraint {
                                r#type: constraint::Type::Unsatisfiable.into(),
                                reason: "The existing projection must exist and be non-null, so the new projection must also exist."
                                    .to_string(),
                            }
                        } else {
                            Constraint {
                                r#type: constraint::Type::FieldRequired.into(),
                                reason: "This field is part of the current materialization."
                                    .to_string(),
                            }
                        };

                    Some((field.clone(), constraint))
                }
            })
            .collect()
    };

    collection.projections.iter().for_each({
        |projection| {
            if !constraints.contains_key(&projection.field) {
                constraints.insert(
                    projection.field.clone(),
                    Constraint {
                        r#type: constraint::Type::FieldForbidden.into(),
                        reason: "This field is not included in the existing materialization."
                            .to_string(),
                    },
                );
            }
        }
    });

    constraints
}

pub fn project_json(
    spec: materialization_spec::Binding,
    doc: serde_json::Value,
) -> serde_json::Value {
    let projections = spec.collection.unwrap().projections;
    let fields = all_fields(spec.field_selection.unwrap());

    fields
        .iter()
        .fold(Map::new(), |mut acc, field| {
            let projection = projections.iter().find(|p| &p.field == field).unwrap();
            let pointer = doc::Pointer::from_str(&projection.ptr);

            acc.insert(
                projection.field.clone(),
                pointer.query(&doc).unwrap().clone(),
            );
            acc
        })
        .into()
}

#[cfg(test)]
mod tests {
    use protocol::flow::{FieldSelection, Inference, Projection};

    use super::*;
    fn check_validate_new_projection(projection: Projection, constraint: Constraint) {
        let result = validate_new_projection(validate_request::Binding {
            collection: Some(CollectionSpec {
                projections: vec![projection.clone()],
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(result[&projection.field], constraint);
    }

    fn check_validate_existing_projection(
        existing_fs: FieldSelection,
        existing_projection: Projection,
        projection: Projection,
        constraint: Constraint,
    ) {
        let result = validate_existing_projection(
            materialization_spec::Binding {
                field_selection: Some(existing_fs),
                collection: Some(CollectionSpec {
                    projections: vec![existing_projection],
                    ..Default::default()
                }),
                ..Default::default()
            },
            validate_request::Binding {
                collection: Some(CollectionSpec {
                    projections: vec![projection.clone()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        assert_eq!(result[&projection.field], constraint);
    }

    fn check_project_json(
        field_selection: FieldSelection,
        projection: Projection,
        input: serde_json::Value,
        expected: serde_json::Value,
    ) {
        assert_eq!(
            project_json(
                materialization_spec::Binding {
                    field_selection: Some(field_selection),
                    collection: Some(CollectionSpec {
                        projections: vec![projection],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                input
            ),
            expected
        );
    }

    #[test]
    fn test_validate_new_projection() {
        check_validate_new_projection(
            Projection {
                field: "pk".to_string(),
                is_primary_key: true,
                inference: Some(Inference {
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::LocationRequired.into(),
                reason: "All locations that are part of the collection key are required."
                    .to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "root".to_string(),
                ptr: "".to_string(),
                inference: Some(Inference {
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldOptional.into(),
                reason: "The root document is usually not necessary in delta-update connectors."
                    .to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "multi_types".to_string(),
                ptr: "multi_types".to_string(),
                inference: Some(Inference {
                    types: vec!["number".to_string(), "string".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "Cannot materialize field with multiple or no types.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "no_type".to_string(),
                ptr: "no_type".to_string(),
                inference: Some(Inference {
                    types: Vec::new(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "Cannot materialize field with multiple or no types.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "boolean".to_string(),
                ptr: "boolean".to_string(),
                inference: Some(Inference {
                    types: vec!["boolean".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::LocationRecommended.into(),
                reason: "Scalar values are recommended to be materialized.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "int".to_string(),
                ptr: "int".to_string(),
                inference: Some(Inference {
                    types: vec!["integer".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::LocationRecommended.into(),
                reason: "Scalar values are recommended to be materialized.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "num".to_string(),
                ptr: "num".to_string(),
                inference: Some(Inference {
                    types: vec!["number".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::LocationRecommended.into(),
                reason: "Scalar values are recommended to be materialized.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "string".to_string(),
                ptr: "string".to_string(),
                inference: Some(Inference {
                    types: vec!["string".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::LocationRecommended.into(),
                reason: "Scalar values are recommended to be materialized.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "obj".to_string(),
                ptr: "obj".to_string(),
                inference: Some(Inference {
                    types: vec!["object".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldOptional.into(),
                reason: "Object and array fields can be materialized.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "arr".to_string(),
                ptr: "arr".to_string(),
                inference: Some(Inference {
                    types: vec!["array".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldOptional.into(),
                reason: "Object and array fields can be materialized.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "null".to_string(),
                ptr: "null".to_string(),
                inference: Some(Inference {
                    types: vec!["null".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "Cannot materialize field with multiple or no types.".to_string(),
            },
        );
    }

    #[test]
    fn test_validate_existing_projection() {
        check_validate_existing_projection(
            FieldSelection {
                keys: vec!["test".to_string()],
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: vec!["boolean".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: vec!["boolean".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldRequired.into(),
                reason: "This field is part of the current materialization.".to_string(),
            },
        );

        check_validate_existing_projection(
            FieldSelection {
                keys: vec!["test".to_string()],
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: vec!["boolean".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Projection {
                field: "new_field".to_string(),
                inference: Some(Inference {
                    types: vec!["boolean".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "This field is not included in the existing materialization.".to_string(),
            },
        );

        check_validate_existing_projection(
            FieldSelection {
                keys: vec!["test".to_string()],
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: vec!["boolean".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: vec!["number".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::Unsatisfiable.into(),
                reason: "The proposed projection may contain types number, which are not part of the original projection.".to_string(),
            },
        );

        check_validate_existing_projection(
            FieldSelection {
                keys: vec!["test".to_string()],
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    exists: Exists::Must.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    exists: Exists::May.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::Unsatisfiable.into(),
                reason: "The existing projection must exist and be non-null, so the new projection must also exist.".to_string(),
            },
        );

        check_validate_existing_projection(
            FieldSelection {
                keys: vec!["test".to_string()],
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: vec!["null".to_string()],
                    exists: Exists::Must.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    exists: Exists::May.into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldRequired.into(),
                reason: "This field is part of the current materialization.".to_string(),
            },
        );
    }

    #[test]
    fn test_project_json() {
        check_project_json(
            FieldSelection {
                keys: vec!["user_id".to_string()],
                ..Default::default()
            },
            Projection {
                field: "user_id".to_string(),
                ptr: "/user/id".to_string(),
                ..Default::default()
            },
            json!({"user": {"id": 2}}),
            json!({"user_id": 2}),
        );

        check_project_json(
            FieldSelection {
                values: vec!["user".to_string()],
                ..Default::default()
            },
            Projection {
                field: "user".to_string(),
                ptr: "/user".to_string(),
                ..Default::default()
            },
            json!({"user": {"id": 2}}),
            json!({"user": {"id": 2}}),
        );

        check_project_json(
            FieldSelection {
                values: vec!["user_id".to_string()],
                ..Default::default()
            },
            Projection {
                field: "user_id".to_string(),
                ptr: "/user/id".to_string(),
                ..Default::default()
            },
            json!({"user": {"id": 2}, "extra_field": {}}),
            json!({"user_id": 2}),
        );
    }
}
