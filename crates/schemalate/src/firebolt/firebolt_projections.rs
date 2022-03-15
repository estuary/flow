use std::collections::{BTreeMap, HashSet};

use protocol::flow::{inference::Exists, materialization_spec::Binding, CollectionSpec};
use protocol::materialize::{constraint, Constraint};

pub fn validate_new_projection(proposed: CollectionSpec) -> BTreeMap<String, Constraint> {
    proposed
        .projections
        .iter()
        .map({
            |projection| {
                let constraint = {
                    if projection.is_primary_key {
                        Constraint {
                            r#type: constraint::Type::LocationRequired.into(),
                            reason:
                                "All locations that are part of the collection key are required."
                                    .to_string(),
                        }
                    } else if projection.ptr.len() == 0 {
                        // root document
                        Constraint {
                            r#type: constraint::Type::LocationRecommended.into(),
                            reason: "The root document should usually be materialized.".to_string(),
                        }
                    } else if let Some(infer) = &projection.inference {
                        if infer.types.len() != 1 {
                            Constraint {
                                r#type: constraint::Type::FieldForbidden.into(),
                                reason: "Cannot materialize field with multiple or no types."
                                    .to_string(),
                            }
                        } else if ["boolean", "integer", "numeric", "string"]
                            .contains(&infer.types[0].as_str())
                        {
                            Constraint {
                                r#type: constraint::Type::LocationRecommended.into(),
                                reason: "Scalar values are recommended to be materialized."
                                    .to_string(),
                            }
                        } else if ["object", "array"].contains(&infer.types[0].as_str()) {
                            Constraint {
                                r#type: constraint::Type::FieldOptional.into(),
                                reason: "Object and array fields can be materialized.".to_string(),
                            }
                        } else {
                            Constraint {
                                r#type: constraint::Type::FieldForbidden.into(),
                                reason: "Cannot materialize this field.".to_string(),
                            }
                        }
                    } else {
                        Constraint {
                            r#type: constraint::Type::FieldForbidden.into(),
                            reason: "Cannot materialize this field.".to_string(),
                        }
                    }
                };

                (projection.field.clone(), constraint)
            }
        })
        .collect()
}

pub fn validate_existing_projection(
    existing: Binding,
    proposed: CollectionSpec,
) -> BTreeMap<String, Constraint> {
    let fs = existing.field_selection.unwrap();
    let existing_projections = existing.collection.unwrap().projections;
    let fields: Vec<String> = Vec::from([fs.keys, fs.values, Vec::from([fs.document])]).concat();
    let mut constraints: BTreeMap<String, Constraint> = {
        fields
            .iter()
            .filter_map({
                |field| {
                    let ep = existing_projections.iter().find(|p| &p.field == field)?;
                    let pp = proposed.projections.iter().find(|p| &p.field == field)?;

                    // TODO: Should we handle an error for None case of these?
                    let ep_infer = ep.inference.as_ref()?;
                    let pp_infer = pp.inference.as_ref()?;

                    let ep_type_set: HashSet<&String> = ep_infer.types.iter().collect();
                    let pp_type_set: HashSet<&String> = pp_infer.types.iter().collect();
                    let diff = pp_type_set.difference(&ep_type_set);

                    let constraint =
                        if diff.clone().count() > 0 {
                            let new_types: String = diff.map(|s| s.to_owned().to_owned()).collect::<Vec<String>>().join(", ");
                            Constraint {
                                r#type: constraint::Type::FieldForbidden.into(),
                                reason: format!("The proposed projection may contain types {}, which are not part of the original projection.", new_types)
                                    .to_string(),
                            }
                        } else if ep_infer.exists == i32::from(Exists::Must) &&
                                  !ep_type_set.contains(&"null".to_string()) &&
                                  pp_infer.exists != i32::from(Exists::Must) {
                            Constraint {
                                r#type: constraint::Type::FieldForbidden.into(),
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

    proposed.projections.iter().for_each({
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

#[cfg(test)]
mod tests {
    use protocol::flow::{FieldSelection, Inference, Projection};

    use super::*;
    fn check_validate_new_projection(projection: Projection, constraint: Constraint) {
        let result = validate_new_projection(CollectionSpec {
            projections: [projection.clone()].to_vec(),
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
            Binding {
                field_selection: Some(existing_fs),
                collection: Some(CollectionSpec {
                    projections: [existing_projection].to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            CollectionSpec {
                projections: [projection.clone()].to_vec(),
                ..Default::default()
            },
        );
        assert_eq!(result[&projection.field], constraint);
    }

    #[test]
    fn test_validate_new_projection() {
        check_validate_new_projection(
            Projection {
                field: "pk".to_string(),
                is_primary_key: true,
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
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::LocationRecommended.into(),
                reason: "The root document should usually be materialized.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "multi_types".to_string(),
                ptr: "multi_types".to_string(),
                inference: Some(Inference {
                    types: ["numeric", r"integer"].map(|s| s.to_string()).to_vec(),
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
                field: "no_inference".to_string(),
                ptr: "no_inference".to_string(),
                inference: None,
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "Cannot materialize this field.".to_string(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "boolean".to_string(),
                ptr: "boolean".to_string(),
                inference: Some(Inference {
                    types: ["boolean".to_string()].to_vec(),
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
                    types: ["integer".to_string()].to_vec(),
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
                    types: ["numeric".to_string()].to_vec(),
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
                    types: ["string".to_string()].to_vec(),
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
                    types: ["object".to_string()].to_vec(),
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
                    types: ["array".to_string()].to_vec(),
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
                    types: ["null".to_string()].to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "Cannot materialize this field.".to_string(),
            },
        );
    }

    #[test]
    fn test_validate_existing_projection() {
        check_validate_existing_projection(
            FieldSelection {
                keys: Vec::from(["test".to_string()]),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: ["boolean".to_string()].to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: ["boolean".to_string()].to_vec(),
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
                keys: Vec::from(["test".to_string()]),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                ..Default::default()
            },
            Projection {
                field: "new_field".to_string(),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "This field is not included in the existing materialization.".to_string(),
            },
        );

        check_validate_existing_projection(
            FieldSelection {
                keys: Vec::from(["test".to_string()]),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: ["boolean".to_string()].to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: ["numeric".to_string()].to_vec(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            Constraint {
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "The proposed projection may contain types numeric, which are not part of the original projection.".to_string(),
            },
        );

        check_validate_existing_projection(
            FieldSelection {
                keys: Vec::from(["test".to_string()]),
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
                r#type: constraint::Type::FieldForbidden.into(),
                reason: "The existing projection must exist and be non-null, so the new projection must also exist.".to_string(),
            },
        );

        check_validate_existing_projection(
            FieldSelection {
                keys: Vec::from(["test".to_string()]),
                ..Default::default()
            },
            Projection {
                field: "test".to_string(),
                inference: Some(Inference {
                    types: ["null".to_string()].to_vec(),
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
}
