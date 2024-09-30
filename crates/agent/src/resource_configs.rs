use models::{SourceCaptureSchemaMode, SourceCapture};
use serde_json::Value;

///
/// # Panics
/// If the `full_collection_name` doesn't contain any `/` characters, which should never
/// be the case since we should have already validated the collection name.
pub fn update_materialization_resource_spec(
    source_capture: &SourceCapture,
    resource_spec: &mut Value,
    resource_spec_pointers: &ResourceSpecPointers,
    full_collection_name: &str,
) -> anyhow::Result<()> {
    let split: Vec<&str> = full_collection_name
        .rsplit('/')
        .take(2)
        .collect();

    if split.len() < 2 {
        return Err(anyhow::anyhow!("collection name is invalid (does not contain '/')"))
    }

    let x_collection_name = split[0];
    let x_schema_name = split[1];

    let x_collection_name_ptr = &resource_spec_pointers.x_collection_name;

    let Some(x_collection_name_prev) = x_collection_name_ptr.create_value(resource_spec) else {
        anyhow::bail!(
            "cannot create location '{x_collection_name_ptr}' in resource spec '{resource_spec}'"
        );
    };

    let _ = std::mem::replace(x_collection_name_prev, x_collection_name.into());

    let source_capture_def = source_capture.to_normalized_def();

    if source_capture_def.target_schema == SourceCaptureSchemaMode::FromSourceName {
        let Some(x_schema_name_ptr) = &resource_spec_pointers.x_schema_name else {
            anyhow::bail!(
                "sourceCapture.targetSchema set on a materialization which does not have x-schema-name annotation"
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
                "sourceCapture.deltaUpdates set on a materialization which does not have x-delta-updates annotation"
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

pub struct ResourceSpecPointers {
    pub x_collection_name: doc::Pointer,
    pub x_schema_name: Option<doc::Pointer>,
    pub x_delta_updates: Option<doc::Pointer>,
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
