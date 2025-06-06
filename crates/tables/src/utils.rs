use models::{SourceType, TargetNaming};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct ResourceSpecPointers {
    pub x_collection_name: doc::Pointer,
    pub x_schema_name: Option<doc::Pointer>,
    pub x_delta_updates: Option<doc::Pointer>,
}

///
/// # Panics
/// If the `full_collection_name` doesn't contain any `/` characters, which should never
/// be the case since we should have already validated the collection name.
pub fn update_materialization_resource_spec(
    source_capture: &SourceType,
    resource_spec: &mut Value,
    resource_spec_pointers: &ResourceSpecPointers,
    full_collection_name: &str,
) -> anyhow::Result<()> {
    let split: Vec<&str> = full_collection_name.rsplit('/').take(2).collect();

    if split.len() < 2 {
        return Err(anyhow::anyhow!(
            "collection name is invalid (does not contain '/')"
        ));
    }

    let source_capture_def = source_capture.to_normalized_def();

    let maybe_x_schema_name = match source_capture_def.target_naming {
        TargetNaming::WithSchema => Some(split[1].to_string()),
        TargetNaming::NoSchema
        | TargetNaming::PrefixSchema
        | TargetNaming::PrefixNonDefaultSchema => None,
    };

    let x_collection_name = match source_capture_def.target_naming {
        TargetNaming::NoSchema | TargetNaming::WithSchema => split[0].to_string(),
        TargetNaming::PrefixNonDefaultSchema if is_default_schema_name(&split[1]) => {
            split[0].to_string()
        }
        TargetNaming::PrefixNonDefaultSchema | TargetNaming::PrefixSchema => {
            format!("{}_{}", split[1], split[0])
        }
    };

    let x_collection_name_ptr = &resource_spec_pointers.x_collection_name;
    let Some(x_collection_name_prev) = x_collection_name_ptr.create_value(resource_spec) else {
        anyhow::bail!(
            "cannot create location '{x_collection_name_ptr}' in resource spec '{resource_spec}'"
        );
    };
    let _ = std::mem::replace(x_collection_name_prev, x_collection_name.into());

    if let Some(x_schema_name) = maybe_x_schema_name {
        let Some(x_schema_name_ptr) = &resource_spec_pointers.x_schema_name else {
            anyhow::bail!(
                "sources.targetNaming requires a schema but the materialization connector does not support schemas"
            );
        };
        let Some(x_schema_name_prev) = x_schema_name_ptr.create_value(resource_spec) else {
            anyhow::bail!(
                "cannot create location '{x_schema_name_ptr}' in resource spec '{resource_spec}'"
            );
        };
        let _ = std::mem::replace(x_schema_name_prev, x_schema_name.into());
    }

    if source_capture_def.delta_updates {
        let Some(x_delta_updates_ptr) = &resource_spec_pointers.x_delta_updates else {
            anyhow::bail!(
                "sources.deltaUpdates is true, but the materialization connector does not support it"
            );
        };
        let Some(x_delta_updates_prev) = x_delta_updates_ptr.create_value(resource_spec) else {
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
pub fn pointer_for_schema(schema_json: &str) -> anyhow::Result<ResourceSpecPointers> {
    // While all known connector resource spec schemas are self-contained, we don't
    // actually do anything to guarantee that they are. This function may fail in that case.
    let schema = doc::validation::build_bundle(schema_json)?;
    let mut builder = doc::SchemaIndexBuilder::new();
    builder.add(&schema)?;
    let index = builder.into_index();
    let shape = doc::Shape::infer(&schema, &index);

    let mut x_collection_name: Option<doc::Pointer> = None;
    let mut x_schema_name: Option<doc::Pointer> = None;
    let mut x_delta_updates: Option<doc::Pointer> = None;
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

fn is_default_schema_name(schema_name: &str) -> bool {
    schema_name == "public" || schema_name == "dbo"
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_updating_materialization_resource_spec() {
        let pointers_full = ResourceSpecPointers {
            x_collection_name: doc::Pointer::from_str("/collectionName"),
            x_schema_name: Some(doc::Pointer::from_str("/schemaName")),
            x_delta_updates: Some(doc::Pointer::from_str("/deltaUpdates")),
        };
        let pointers_no_schema = ResourceSpecPointers {
            x_collection_name: doc::Pointer::from_str("/collectionName"),
            x_schema_name: None,
            x_delta_updates: Some(doc::Pointer::from_str("/deltaUpdates")),
        };
        let pointers_sparse = ResourceSpecPointers {
            x_collection_name: doc::Pointer::from_str("/collectionName"),
            x_schema_name: None,
            x_delta_updates: None,
        };

        // A non-default schema name gets added as a prefix
        let result = test_update(
            json!({}),
            "test/skeema/kollection",
            models::TargetNaming::PrefixNonDefaultSchema,
            true,
            &pointers_no_schema,
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "skeema_kollection", "deltaUpdates": true}),
            result
        );

        // Update the existing resource and expect that it no longer has the schema prefix
        let result = test_update(
            result,
            "test/public/differentCollection",
            models::TargetNaming::PrefixNonDefaultSchema,
            true,
            &pointers_no_schema,
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "differentCollection", "deltaUpdates": true}),
            result,
        );
        // The default dbo schema also doesn't get added as a prefix
        let result = test_update(
            json!({}),
            "test/dbo/kollection",
            models::TargetNaming::PrefixNonDefaultSchema,
            false,
            &pointers_sparse,
        )
        .expect("failed to update");
        assert_eq!(json!({"collectionName": "kollection"}), result,);

        let result = test_update(
            json!({}),
            "test/dbo/kollection",
            models::TargetNaming::WithSchema,
            false,
            &pointers_full,
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "kollection", "schemaName": "dbo"}),
            result,
        );

        let result = test_update(
            json!({}),
            "test/public/kollection",
            models::TargetNaming::PrefixSchema,
            true,
            &pointers_no_schema,
        )
        .expect("failed to update");
        assert_eq!(
            json!({"collectionName": "public_kollection", "deltaUpdates": true}),
            result,
        );

        // Error cases:
        // ResourceSpecPointers are missing x-schema-name
        let err = test_update(
            json!({}),
            "test/public/kollection",
            models::TargetNaming::WithSchema,
            true,
            &pointers_no_schema,
        )
        .expect_err("should fail");
        assert_eq!("sources.targetNaming requires a schema but the materialization connector does not support schemas", err.to_string());

        // ResourceSpecPointers are missing x-delta-updates
        let err = test_update(
            json!({}),
            "test/public/kollection",
            models::TargetNaming::NoSchema,
            true,
            &pointers_sparse,
        )
        .expect_err("should fail");
        assert_eq!(
            "sources.deltaUpdates is true, but the materialization connector does not support it",
            err.to_string()
        );
    }

    fn test_update(
        mut existing: serde_json::Value,
        collection_name: &str,
        target_naming: models::TargetNaming,
        delta_updates: bool,
        pointers: &ResourceSpecPointers,
    ) -> anyhow::Result<serde_json::Value> {
        let sources = models::SourceType::Configured(models::SourceDef {
            capture: None,
            target_naming,
            delta_updates,
        });
        update_materialization_resource_spec(&sources, &mut existing, pointers, collection_name)?;
        Ok(existing)
    }
}
