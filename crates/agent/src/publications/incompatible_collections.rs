use std::collections::BTreeMap;

use models::publications::{AffectedConsumer, IncompatibleCollection, RejectedField};
use proto_flow::materialize::response::validated::constraint;
use tables::BuiltRow;

pub fn get_incompatible_collections(output: &tables::Validations) -> Vec<IncompatibleCollection> {
    // We'll collect a map of collection names to lists of materializations that have rejected the proposed collection changes.
    let mut naughty_collections = BTreeMap::new();

    // Look at materialization validation responses for any collections that have been rejected due to unsatisfiable constraints.
    for mat in output.built_materializations.iter() {
        let Some(validated) = mat.validated() else {
            continue;
        };
        let Some(model) = mat.model() else {
            continue;
        };
        for (i, binding) in validated.bindings.iter().enumerate() {
            let naughty_fields: Vec<RejectedField> = binding
                .constraints
                .iter()
                .filter(|(_, constraint)| {
                    constraint.r#type == constraint::Type::Unsatisfiable as i32
                })
                .map(|(field, constraint)| RejectedField {
                    field: field.clone(),
                    reason: constraint.reason.clone(),
                })
                .collect();
            if !naughty_fields.is_empty() {
                // We must skip over disabled bindings in order to translate the index of the
                // validated binding to the index of the model binding.
                let collection_name = model
                    .bindings
                    .iter()
                    .filter(|b| !b.disable)
                    .skip(i)
                    .next()
                    .unwrap() //
                    .source
                    .collection()
                    .to_string();
                let affected_consumers = naughty_collections
                    .entry(collection_name)
                    .or_insert_with(|| Vec::new());
                affected_consumers.push(AffectedConsumer {
                    name: mat.catalog_name().to_string(),
                    fields: naughty_fields,
                    resource_path: binding.resource_path.clone(),
                });
            }
        }
    }

    naughty_collections
        .into_iter()
        .map(
            |(collection, affected_materializations)| IncompatibleCollection {
                collection,
                affected_materializations,
                requires_recreation: Vec::new(),
            },
        )
        .collect()
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use proto_flow::materialize::response::validated;
    use proto_flow::materialize::response::validated::constraint;

    use super::*;

    #[test]
    fn test_get_incompatible_collections() {
        let live_mat: models::MaterializationDef = serde_json::from_value(serde_json::json!({
            "endpoint": {
                "connector": {
                    "image": "test/materialize:foo",
                    "config": {}
                }
            },
            "bindings": [
                {
                    "resource": {"table": "disabledTable"},
                    "source": "acmeCo/disabledCollection",
                    "disable": true
                },
                {
                    "resource": {"table": "nice"},
                    "source": "acmeCo/niceCollection"
                },
                {
                    "resource": {"table": "naughty"},
                    "source": "acmeCo/naughtyCollection"
                }
            ]
        }))
        .unwrap();

        fn test_constraints(ty: constraint::Type) -> BTreeMap<String, validated::Constraint> {
            let mut m = BTreeMap::new();
            m.insert(
                "test_field".to_string(),
                validated::Constraint {
                    r#type: ty as i32,
                    reason: "cuz this is a test".to_string(),
                },
            );
            m
        }
        let resp = proto_flow::materialize::response::Validated {
            bindings: vec![
                validated::Binding {
                    constraints: test_constraints(constraint::Type::LocationRecommended),
                    resource_path: vec!["nice".to_string()],
                    delta_updates: false,
                },
                validated::Binding {
                    constraints: test_constraints(constraint::Type::Unsatisfiable),
                    resource_path: vec!["naughty".to_string()],
                    delta_updates: false,
                },
            ],
        };

        let mut validations = tables::Validations::default();
        validations.built_materializations.insert_row(
            models::Materialization::new("acmeCo/materialize"),
            tables::synthetic_scope(models::CatalogType::Materialization, "acmeCo/materialize"),
            models::Id::zero(),
            models::Id::zero(),
            models::Id::zero(),
            models::Id::zero(),
            Some(live_mat),
            Vec::new(),
            Some(resp),
            None,
            None,
            false,
            None,
        );

        let result = get_incompatible_collections(&validations);
        assert_eq!(1, result.len());
        let ic = result.into_iter().next().unwrap();

        insta::assert_debug_snapshot!(ic, @r###"
        IncompatibleCollection {
            collection: "acmeCo/naughtyCollection",
            requires_recreation: [],
            affected_materializations: [
                AffectedConsumer {
                    name: "acmeCo/materialize",
                    fields: [
                        RejectedField {
                            field: "test_field",
                            reason: "cuz this is a test",
                        },
                    ],
                    resource_path: [
                        "naughty",
                    ],
                },
            ],
        }
        "###);
    }
}
