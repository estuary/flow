use std::collections::BTreeMap;
use std::iter::FromIterator;

use json::schema::types;
use proto_flow::{
    flow::{inference::Exists, materialization_spec, FieldSelection},
    materialize::request::validate::Binding as ValidateBinding,
    materialize::response::validated::constraint,
    materialize::response::validated::Constraint,
};

use crate::firebolt::errors::BindingConstraintError;

// Can we make this a method on FieldSelection itself?
fn all_fields(fs: FieldSelection) -> Vec<String> {
    let mut fields = vec![fs.keys, fs.values].concat();
    if fs.document != "" {
        fields.push(fs.document);
    }
    fields
}

pub fn validate_binding_against_constraints(
    constraints: BTreeMap<String, Constraint>,
    proposed: materialization_spec::Binding,
) -> Result<(), BindingConstraintError> {
    let fields = all_fields(proposed.field_selection.unwrap());
    let projections = proposed.collection.unwrap().projections;
    let mut projected_pointers: Vec<String> = Vec::new();

    fields.iter().try_for_each(|field| {
        let projection = projections.iter().find(|p| &p.field == field).ok_or(
            BindingConstraintError::NoProjectionForField {
                field: field.to_string(),
            },
        )?;

        projected_pointers.push(projection.ptr.to_string());

        let constraint = &constraints[field];
        let ctype = constraint::Type::try_from(constraint.r#type).unwrap();
        if vec![
            constraint::Type::FieldForbidden,
            constraint::Type::Incompatible,
        ]
        .contains(&ctype)
        {
            return Err(BindingConstraintError::NotMaterializableField {
                field: field.to_string(),
                constraint: format!("{:?}", ctype),
                reason: constraint.reason.to_string(),
            });
        }

        Ok(())
    })?;

    constraints.iter().try_for_each(|(field, constraint)| {
        match constraint::Type::try_from(constraint.r#type).unwrap() {
            constraint::Type::FieldRequired if !fields.contains(field) => {
                return Err(BindingConstraintError::RequiredFieldMissing {
                    field: field.to_string(),
                    reason: constraint.reason.to_string(),
                })
            }
            constraint::Type::LocationRequired => {
                let projection = projections.iter().find(|p| &p.field == field).unwrap();
                if !projected_pointers.contains(&projection.ptr) {
                    return Err(BindingConstraintError::MissingProjection {
                        ptr: projection.ptr.clone(),
                    });
                }
            }
            _ => (),
        };

        Ok(())
    })?;

    Ok(())
}

pub fn validate_new_projection(proposed: ValidateBinding) -> BTreeMap<String, Constraint> {
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
                        folded_field: String::new(),
                    }
                } else if projection.ptr.len() == 0 {
                    // root document
                    Constraint {
                        r#type: constraint::Type::FieldOptional.into(),
                        reason:
                            "The root document is usually not necessary in delta-update connectors."
                                .to_string(),
                        folded_field: String::new(),
                    }
                } else {
                    let types = types::Set::from_iter(infer.types.iter());
                    if !types.is_single_type() {
                        Constraint {
                            r#type: constraint::Type::FieldForbidden.into(),
                            reason: "Cannot materialize field with multiple or no types."
                                .to_string(),
                            folded_field: String::new(),
                        }
                    } else if types.is_single_scalar_type() {
                        Constraint {
                            r#type: constraint::Type::LocationRecommended.into(),
                            reason: "Scalar values are recommended to be materialized.".to_string(),
                            folded_field: String::new(),
                        }
                    } else if matches!(types - types::NULL, types::OBJECT | types::ARRAY) {
                        Constraint {
                            r#type: constraint::Type::FieldOptional.into(),
                            reason: "Object and array fields can be materialized.".to_string(),
                            folded_field: String::new(),
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

pub fn validate_existing_projection(
    existing: materialization_spec::Binding,
    proposed: ValidateBinding,
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
                            r#type: constraint::Type::Incompatible.into(),
                            reason: "The proposed materialization is missing the projection, which is required because it's included in the existing materialization".to_string(),
                            folded_field: String::new(),
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
                                r#type: constraint::Type::Incompatible.into(),
                                reason: format!("The proposed projection may contain types {}, which are not part of the original projection.", new_types)
                                    .to_string(),
                                folded_field: String::new(),
                            }
                        } else if ep_infer.exists == i32::from(Exists::Must) &&
                                  !ep_type_set.overlaps(types::NULL) &&
                                  pp_infer.exists != i32::from(Exists::Must) {
                            Constraint {
                                r#type: constraint::Type::Incompatible.into(),
                                reason: "The existing projection must exist and be non-null, so the new projection must also exist."
                                    .to_string(),
                                folded_field: String::new(),
                            }
                        } else {
                            Constraint {
                                r#type: constraint::Type::FieldRequired.into(),
                                reason: "This field is part of the current materialization."
                                    .to_string(),
                                folded_field: String::new(),
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
                        folded_field: String::new(),
                    },
                );
            }
        }
    });

    constraints
}

#[cfg(test)]
mod tests {
    use proto_flow::flow::{CollectionSpec, FieldSelection, Inference, Projection};

    use super::*;
    fn simple_validate_binding_against_constraints(
        constraints: BTreeMap<String, Constraint>,
        field_selection: FieldSelection,
        projection: Projection,
    ) -> Result<(), BindingConstraintError> {
        validate_binding_against_constraints(
            constraints,
            materialization_spec::Binding {
                field_selection: Some(field_selection),
                collection: Some(CollectionSpec {
                    projections: vec![projection.clone()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
    }

    fn check_validate_new_projection(projection: Projection, constraint: Constraint) {
        let result = validate_new_projection(ValidateBinding {
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
            ValidateBinding {
                collection: Some(CollectionSpec {
                    projections: vec![projection.clone()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        assert_eq!(result[&projection.field], constraint);
    }

    #[test]
    fn test_validate_binding_against_constraints() {
        assert!(matches!(
            simple_validate_binding_against_constraints(
                BTreeMap::from([(
                    "pk".to_string(),
                    Constraint {
                        r#type: constraint::Type::FieldRequired.into(),
                        reason: "".to_string(),
                        folded_field: String::new(),
                    },
                )]),
                FieldSelection {
                    keys: vec![],
                    ..Default::default()
                },
                Projection {
                    field: "blah".to_string(),
                    inference: Some(Inference {
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            ),
            Err(BindingConstraintError::RequiredFieldMissing { .. })
        ));

        assert!(matches!(
            simple_validate_binding_against_constraints(
                BTreeMap::from([(
                    "pk".to_string(),
                    Constraint {
                        r#type: constraint::Type::LocationRequired.into(),
                        reason: "".to_string(),
                        folded_field: String::new(),
                    },
                )]),
                FieldSelection {
                    keys: vec![],
                    ..Default::default()
                },
                Projection {
                    field: "pk".to_string(),
                    inference: Some(Inference {
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            ),
            Err(BindingConstraintError::MissingProjection { .. })
        ));

        assert!(matches!(
            simple_validate_binding_against_constraints(
                BTreeMap::from([(
                    "pk".to_string(),
                    Constraint {
                        r#type: constraint::Type::LocationRequired.into(),
                        reason: "".to_string(),
                        folded_field: String::new(),
                    },
                )]),
                FieldSelection {
                    keys: vec!["test".to_string()],
                    ..Default::default()
                },
                Projection {
                    field: "pk".to_string(),
                    inference: Some(Inference {
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            ),
            // No projection for "test"
            Err(BindingConstraintError::NoProjectionForField { .. })
        ));

        assert!(matches!(
            simple_validate_binding_against_constraints(
                BTreeMap::from([(
                    "pk".to_string(),
                    Constraint {
                        r#type: constraint::Type::Incompatible.into(),
                        reason: "".to_string(),
                        folded_field: String::new(),
                    },
                )]),
                FieldSelection {
                    keys: vec!["pk".to_string()],
                    ..Default::default()
                },
                Projection {
                    field: "pk".to_string(),
                    inference: Some(Inference {
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            ),
            Err(BindingConstraintError::NotMaterializableField { .. })
        ));

        assert!(matches!(
            simple_validate_binding_against_constraints(
                BTreeMap::from([(
                    "pk".to_string(),
                    Constraint {
                        r#type: constraint::Type::FieldForbidden.into(),
                        reason: "".to_string(),
                        folded_field: String::new(),
                    },
                )]),
                FieldSelection {
                    keys: vec!["pk".to_string()],
                    ..Default::default()
                },
                Projection {
                    field: "pk".to_string(),
                    inference: Some(Inference {
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            ),
            Err(BindingConstraintError::NotMaterializableField { .. })
        ));
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
                folded_field: String::new(),
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
                folded_field: String::new(),
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
                folded_field: String::new(),
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
                folded_field: String::new(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "boolfield".to_string(),
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
                folded_field: String::new(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "intfield".to_string(),
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
                folded_field: String::new(),
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
                folded_field: String::new(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "stringfield".to_string(),
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
                folded_field: String::new(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "object".to_string(),
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
                folded_field: String::new(),
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
                folded_field: String::new(),
            },
        );

        check_validate_new_projection(
            Projection {
                field: "nullfield".to_string(),
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
                folded_field: String::new(),
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
                folded_field: String::new(),
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
                folded_field: String::new(),
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
                r#type: constraint::Type::Incompatible.into(),
                reason: "The proposed projection may contain types number, which are not part of the original projection.".to_string(),
                folded_field: String::new(),
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
                r#type: constraint::Type::Incompatible.into(),
                reason: "The existing projection must exist and be non-null, so the new projection must also exist.".to_string(),
                folded_field: String::new(),
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
                folded_field: String::new(),
            },
        );
    }
}
