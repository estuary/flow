use models::{SourceType, TargetNaming, TargetNamingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct ResourceSpecPointers {
    pub x_collection_name: json::Pointer,
    pub x_schema_name: Option<json::Pointer>,
    pub x_delta_updates: Option<json::Pointer>,
}

/// # Panics
/// If the `full_collection_name` doesn't contain any `/` characters, which should never
/// be the case since we should have already validated the collection name.
pub fn update_materialization_resource_spec(
    target_naming: Option<&TargetNamingStrategy>,
    source_capture: Option<&SourceType>,
    resource_spec: &mut Value,
    resource_spec_pointers: &ResourceSpecPointers,
    full_collection_name: &str,
) -> anyhow::Result<()> {
    // TargetNamingStrategy requires x-schema-name support from the connector.
    if target_naming.is_some() && resource_spec_pointers.x_schema_name.is_none() {
        anyhow::bail!("targetNaming requires a connector that supports x-schema-name");
    }

    let split: Vec<&str> = full_collection_name.rsplit('/').take(2).collect();

    if split.len() < 2 {
        return Err(anyhow::anyhow!(
            "collection name is invalid (does not contain '/')"
        ));
    }

    let (x_collection_name, maybe_x_schema_name) = if let Some(strategy) = target_naming {
        match strategy {
            TargetNamingStrategy::MatchSourceStructure {
                table_template,
                schema_template,
            } => (
                apply_template(table_template.as_deref(), "{{table}}", split[0]),
                Some(apply_template(
                    schema_template.as_deref(),
                    "{{schema}}",
                    split[1],
                )),
            ),
            TargetNamingStrategy::SingleSchema {
                schema,
                table_template,
            } => (
                apply_template(table_template.as_deref(), "{{table}}", split[0]),
                Some(schema.clone()),
            ),
            TargetNamingStrategy::PrefixTableNames {
                schema,
                skip_common_defaults,
                table_template,
            } => {
                let base = if *skip_common_defaults && is_default_schema_name(split[1]) {
                    split[0].to_string()
                } else {
                    format!("{}_{}", split[1], split[0])
                };
                (
                    apply_template(table_template.as_deref(), "{{table}}", &base),
                    Some(schema.clone()),
                )
            }
        }
    } else if let Some(source) = source_capture {
        let source_def = source.to_normalized_def();

        // TODO(js): Remove this legacy path once we finish the target naming migration.
        let schema = match source_def.target_naming {
            TargetNaming::WithSchema => Some(split[1].to_string()),
            TargetNaming::NoSchema
            | TargetNaming::PrefixSchema
            | TargetNaming::PrefixNonDefaultSchema => None,
        };

        let name = match source_def.target_naming {
            TargetNaming::NoSchema | TargetNaming::WithSchema => split[0].to_string(),
            TargetNaming::PrefixNonDefaultSchema if is_default_schema_name(split[1]) => {
                split[0].to_string()
            }
            TargetNaming::PrefixNonDefaultSchema | TargetNaming::PrefixSchema => {
                format!("{}_{}", split[1], split[0])
            }
        };

        (name, schema)
    } else {
        anyhow::bail!(
            "no naming strategy available: both targetNaming and sourceCapture are absent"
        );
    };

    // Write x-collection-name.
    let x_collection_name_ptr = &resource_spec_pointers.x_collection_name;
    let Some(x_collection_name_prev) =
        json::ptr::create_value(x_collection_name_ptr, resource_spec)
    else {
        anyhow::bail!(
            "cannot create location '{x_collection_name_ptr}' in resource spec '{resource_spec}'"
        );
    };
    let _ = std::mem::replace(x_collection_name_prev, x_collection_name.into());

    // Write x-schema-name when resolved.
    if let Some(x_schema_name) = maybe_x_schema_name {
        let Some(x_schema_name_ptr) = &resource_spec_pointers.x_schema_name else {
            anyhow::bail!(
                "sources.targetNaming requires a schema but the materialization connector does not support schemas"
            );
        };
        let Some(x_schema_name_prev) = json::ptr::create_value(x_schema_name_ptr, resource_spec)
        else {
            anyhow::bail!(
                "cannot create location '{x_schema_name_ptr}' in resource spec '{resource_spec}'"
            );
        };
        let _ = std::mem::replace(x_schema_name_prev, x_schema_name.into());
    }

    // Write x-delta-updates from source_capture.
    let delta_updates = source_capture
        .map(|s| s.to_normalized_def().delta_updates)
        .unwrap_or(false);

    if delta_updates {
        let Some(x_delta_updates_ptr) = &resource_spec_pointers.x_delta_updates else {
            anyhow::bail!(
                "sources.deltaUpdates is true, but the materialization connector does not support it"
            );
        };
        let Some(x_delta_updates_prev) =
            json::ptr::create_value(x_delta_updates_ptr, resource_spec)
        else {
            anyhow::bail!(
                "cannot create location '{x_delta_updates_ptr}' in resource spec '{resource_spec}'"
            );
        };
        let _ = std::mem::replace(x_delta_updates_prev, true.into());
    }

    Ok(())
}

/// Runs inference on the given schema and searches for a location within the resource spec
/// that bears the `x-collection-name`, `x-schema-name` or `x-delta-updates` annotations.
/// Returns the pointer to those location, or an error if no `x-collection-name` exists.
/// Errors from parsing the schema are returned directly. The schema must be fully self-contained (a.k.a. bundled),
/// or an error will be returned.
pub fn pointer_for_schema(schema: &str) -> anyhow::Result<ResourceSpecPointers> {
    // While all known connector resource spec schemas are self-contained, we don't
    // actually do anything to guarantee that they are. This function may fail in that case.
    let schema = doc::validation::build_bundle(schema.as_bytes())?;
    let validator = doc::Validator::new(schema)?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    let mut x_collection_name: Option<json::Pointer> = None;
    let mut x_schema_name: Option<json::Pointer> = None;
    let mut x_delta_updates: Option<json::Pointer> = None;

    for (ptr, _, prop_shape, _) in shape.locations() {
        if prop_shape.annotations.contains_key("x-collection-name") {
            x_collection_name = Some(ptr)
        } else if prop_shape.annotations.contains_key("x-schema-name") {
            x_schema_name = Some(ptr)
        } else if prop_shape.annotations.contains_key("x-delta-updates") {
            x_delta_updates = Some(ptr)
        }
    }

    if let Some(x_collection_name_ptr) = x_collection_name {
        Ok(ResourceSpecPointers {
            x_collection_name: x_collection_name_ptr,
            x_schema_name,
            x_delta_updates,
        })
    } else {
        Err(anyhow::anyhow!(
            "resource spec schema does not contain any location annotated with x-collection-name"
        ))
    }
}

fn apply_template(template: Option<&str>, placeholder: &str, value: &str) -> String {
    match template {
        Some(t) => t.replacen(placeholder, value, 1),
        None => value.to_string(),
    }
}

fn is_default_schema_name(schema_name: &str) -> bool {
    schema_name == "public" || schema_name == "dbo"
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    fn pointers_full() -> ResourceSpecPointers {
        ResourceSpecPointers {
            x_collection_name: json::Pointer::from("/collectionName"),
            x_schema_name: Some(json::Pointer::from("/schemaName")),
            x_delta_updates: Some(json::Pointer::from("/deltaUpdates")),
        }
    }

    fn pointers_no_schema() -> ResourceSpecPointers {
        ResourceSpecPointers {
            x_collection_name: json::Pointer::from("/collectionName"),
            x_schema_name: None,
            x_delta_updates: Some(json::Pointer::from("/deltaUpdates")),
        }
    }

    fn pointers_sparse() -> ResourceSpecPointers {
        ResourceSpecPointers {
            x_collection_name: json::Pointer::from("/collectionName"),
            x_schema_name: None,
            x_delta_updates: None,
        }
    }

    /// Helper for the legacy path (source_capture only, no top-level target_naming).
    fn test_update_legacy(
        mut existing: serde_json::Value,
        collection_name: &str,
        target_naming: models::TargetNaming,
        delta_updates: bool,
        pointers: &ResourceSpecPointers,
    ) -> anyhow::Result<serde_json::Value> {
        let source = models::SourceType::Configured(models::SourceDef {
            capture: None,
            target_naming,
            delta_updates,
            fields_recommended: Default::default(),
        });
        update_materialization_resource_spec(
            None,
            Some(&source),
            &mut existing,
            pointers,
            collection_name,
        )?;
        Ok(existing)
    }

    /// Helper for the new path (top-level target_naming, optional source_capture for delta_updates).
    fn test_update_strategy(
        mut existing: serde_json::Value,
        collection_name: &str,
        strategy: &models::TargetNamingStrategy,
        delta_updates: bool,
        pointers: &ResourceSpecPointers,
    ) -> anyhow::Result<serde_json::Value> {
        let source = models::SourceType::Configured(models::SourceDef {
            capture: None,
            target_naming: models::TargetNaming::NoSchema,
            delta_updates,
            fields_recommended: Default::default(),
        });
        update_materialization_resource_spec(
            Some(strategy),
            Some(&source),
            &mut existing,
            pointers,
            collection_name,
        )?;
        Ok(existing)
    }

    #[test]
    fn test_legacy_naming_unchanged() {
        // Legacy tests: behavior is identical to the old function signature.

        // PrefixNonDefaultSchema: non-default schema gets prefixed
        let result = test_update_legacy(
            json!({}),
            "test/skeema/kollection",
            models::TargetNaming::PrefixNonDefaultSchema,
            true,
            &pointers_no_schema(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "skeema_kollection", "deltaUpdates": true}),
            result
        );

        // PrefixNonDefaultSchema: default "public" schema is NOT prefixed
        let result = test_update_legacy(
            result,
            "test/public/differentCollection",
            models::TargetNaming::PrefixNonDefaultSchema,
            true,
            &pointers_no_schema(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "differentCollection", "deltaUpdates": true}),
            result,
        );

        // PrefixNonDefaultSchema: default "dbo" schema is NOT prefixed
        let result = test_update_legacy(
            json!({}),
            "test/dbo/kollection",
            models::TargetNaming::PrefixNonDefaultSchema,
            false,
            &pointers_sparse(),
        )
        .expect("failed to update");
        assert_eq!(json!({"collectionName": "kollection"}), result);

        // WithSchema: schema from collection name
        let result = test_update_legacy(
            json!({}),
            "test/dbo/kollection",
            models::TargetNaming::WithSchema,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "kollection", "schemaName": "dbo"}),
            result,
        );

        // PrefixSchema: always prefixes
        let result = test_update_legacy(
            json!({}),
            "test/public/kollection",
            models::TargetNaming::PrefixSchema,
            true,
            &pointers_no_schema(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "public_kollection", "deltaUpdates": true}),
            result,
        );

        // Error: WithSchema without x-schema-name support
        let err = test_update_legacy(
            json!({}),
            "test/public/kollection",
            models::TargetNaming::WithSchema,
            true,
            &pointers_no_schema(),
        )
        .expect_err("should fail");
        assert_eq!(
            "sources.targetNaming requires a schema but the materialization connector does not support schemas",
            err.to_string()
        );

        // Error: delta_updates without x-delta-updates support
        let err = test_update_legacy(
            json!({}),
            "test/public/kollection",
            models::TargetNaming::NoSchema,
            true,
            &pointers_sparse(),
        )
        .expect_err("should fail");
        assert_eq!(
            "sources.deltaUpdates is true, but the materialization connector does not support it",
            err.to_string()
        );
    }

    #[test]
    fn test_target_naming_strategy_match_source_structure() {
        let strategy = models::TargetNamingStrategy::MatchSourceStructure {
            table_template: None,
            schema_template: None,
        };

        let result = test_update_strategy(
            json!({}),
            "test/mySchema/myTable",
            &strategy,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "myTable", "schemaName": "mySchema"}),
            result,
        );
    }

    #[test]
    fn test_target_naming_strategy_single_schema() {
        let strategy = models::TargetNamingStrategy::SingleSchema {
            schema: "prod".to_string(),
            table_template: None,
        };

        let result = test_update_strategy(
            json!({}),
            "test/mySchema/myTable",
            &strategy,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "myTable", "schemaName": "prod"}),
            result,
        );
    }

    #[test]
    fn test_target_naming_strategy_prefix_table_names() {
        let strategy = models::TargetNamingStrategy::PrefixTableNames {
            schema: "prod".to_string(),
            skip_common_defaults: false,
            table_template: None,
        };

        let result = test_update_strategy(
            json!({}),
            "test/mySchema/myTable",
            &strategy,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "mySchema_myTable", "schemaName": "prod"}),
            result,
        );

        // With skip_common_defaults: "public" is skipped
        let strategy_skip = models::TargetNamingStrategy::PrefixTableNames {
            schema: "prod".to_string(),
            skip_common_defaults: true,
            table_template: None,
        };

        let result = test_update_strategy(
            json!({}),
            "test/public/myTable",
            &strategy_skip,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "myTable", "schemaName": "prod"}),
            result,
        );

        // Without skip_common_defaults: "public" IS prefixed
        let result = test_update_strategy(
            json!({}),
            "test/public/myTable",
            &strategy,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "public_myTable", "schemaName": "prod"}),
            result,
        );
    }

    #[test]
    fn test_target_naming_strategy_without_source_capture() {
        // Materializations without sourceCapture can use targetNaming.
        // delta_updates defaults to false when source_capture is absent.
        let strategy = models::TargetNamingStrategy::SingleSchema {
            schema: "prod".to_string(),
            table_template: None,
        };
        let mut existing = json!({});
        update_materialization_resource_spec(
            Some(&strategy),
            None,
            &mut existing,
            &pointers_full(),
            "test/mySchema/myTable",
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "myTable", "schemaName": "prod"}),
            existing,
        );
    }

    #[test]
    fn test_target_naming_strategy_with_delta_updates() {
        let strategy = models::TargetNamingStrategy::SingleSchema {
            schema: "prod".to_string(),
            table_template: None,
        };

        // delta_updates from source_capture is honored
        let result = test_update_strategy(
            json!({}),
            "test/mySchema/myTable",
            &strategy,
            true,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "myTable", "schemaName": "prod", "deltaUpdates": true}),
            result,
        );
    }

    #[test]
    fn test_target_naming_strategy_requires_x_schema_name() {
        // All strategy variants error when x-schema-name is absent.
        for strategy in &[
            models::TargetNamingStrategy::MatchSourceStructure {
                table_template: None,
                schema_template: None,
            },
            models::TargetNamingStrategy::SingleSchema {
                schema: "prod".to_string(),
                table_template: None,
            },
            models::TargetNamingStrategy::PrefixTableNames {
                schema: "prod".to_string(),
                skip_common_defaults: false,
                table_template: None,
            },
        ] {
            let mut existing = json!({});
            let err = update_materialization_resource_spec(
                Some(strategy),
                None,
                &mut existing,
                &pointers_no_schema(),
                "test/public/kollection",
            )
            .expect_err("should fail");
            assert_eq!(
                "targetNaming requires a connector that supports x-schema-name",
                err.to_string()
            );
        }
    }

    #[test]
    fn test_no_naming_no_source_errors() {
        let mut existing = json!({});
        let err = update_materialization_resource_spec(
            None,
            None,
            &mut existing,
            &pointers_full(),
            "test/public/kollection",
        )
        .expect_err("should fail");
        assert_eq!(
            "no naming strategy available: both targetNaming and sourceCapture are absent",
            err.to_string()
        );
    }

    #[test]
    fn test_match_source_structure_with_templates() {
        let strategy = models::TargetNamingStrategy::MatchSourceStructure {
            table_template: Some("staging_{{table}}".to_string()),
            schema_template: Some("analytics_{{schema}}".to_string()),
        };

        let result = test_update_strategy(
            json!({}),
            "test/mySchema/myTable",
            &strategy,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "staging_myTable", "schemaName": "analytics_mySchema"}),
            result,
        );
    }

    #[test]
    fn test_single_schema_with_table_template() {
        let strategy = models::TargetNamingStrategy::SingleSchema {
            schema: "prod".to_string(),
            table_template: Some("v2_{{table}}".to_string()),
        };

        let result = test_update_strategy(
            json!({}),
            "test/mySchema/myTable",
            &strategy,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "v2_myTable", "schemaName": "prod"}),
            result,
        );
    }

    #[test]
    fn test_prefix_table_names_with_table_template() {
        let strategy = models::TargetNamingStrategy::PrefixTableNames {
            schema: "prod".to_string(),
            skip_common_defaults: false,
            table_template: Some("v_{{table}}".to_string()),
        };

        let result = test_update_strategy(
            json!({}),
            "test/mySchema/myTable",
            &strategy,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "v_mySchema_myTable", "schemaName": "prod"}),
            result,
        );

        // With skip_common_defaults + template
        let strategy_skip = models::TargetNamingStrategy::PrefixTableNames {
            schema: "prod".to_string(),
            skip_common_defaults: true,
            table_template: Some("v_{{table}}".to_string()),
        };

        let result = test_update_strategy(
            json!({}),
            "test/public/myTable",
            &strategy_skip,
            false,
            &pointers_full(),
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "v_myTable", "schemaName": "prod"}),
            result,
        );
    }

    #[test]
    fn test_template_serde_round_trip() {
        // None templates are omitted from serialization.
        let strategy = models::TargetNamingStrategy::MatchSourceStructure {
            table_template: None,
            schema_template: None,
        };
        let json = serde_json::to_value(&strategy).unwrap();
        assert_eq!(json, json!({"strategy": "matchSourceStructure"}));
        let round_tripped: models::TargetNamingStrategy = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, strategy);

        // Present templates are preserved.
        let strategy = models::TargetNamingStrategy::MatchSourceStructure {
            table_template: Some("staging_{{table}}".to_string()),
            schema_template: Some("analytics_{{schema}}".to_string()),
        };
        let json = serde_json::to_value(&strategy).unwrap();
        assert_eq!(
            json,
            json!({"strategy": "matchSourceStructure", "tableTemplate": "staging_{{table}}", "schemaTemplate": "analytics_{{schema}}"})
        );
        let round_tripped: models::TargetNamingStrategy = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, strategy);
    }
}
