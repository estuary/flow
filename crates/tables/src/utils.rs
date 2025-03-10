use models::{SourceCapture, SourceCaptureSchemaMode};
use proto_flow::flow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Returns the generation id for a given collection spec, if specified. Legacy
/// collection specs may not have a generation id, in which case `None` is
/// returned.
pub fn get_collection_generation_id(c: &flow::CollectionSpec) -> Option<models::Id> {
    let partition_prefix = &c.partition_template.as_ref()?.name;
    let (_, last) = partition_prefix.rsplit_once('/')?;

    // If this is a legacy collection spec, then the `last` path component will
    // be a string that cannot be parsed as an id.
    models::Id::from_hex(last).ok()
}

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
    source_capture: &SourceCapture,
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
    // If we're setting the schema name as a separate property, then the
    // x-collection-name will be only the last path component of the full
    // collection name. But if there isn't a separate schema name property, or
    // if the user does not wish to use it, then concatenate the last two path
    // components to end up with something like `schema_table`, which helps to
    // avoid conflicts arising from capturing identically named tables from
    // different schemas, and then materializing them into the same schema.
    let set_schema_name = {
        //extra braces prevent rustfmt from doing bad things here
        source_capture_def.target_schema == SourceCaptureSchemaMode::FromSourceName
            && resource_spec_pointers.x_schema_name.is_some()
    };
    let x_collection_name = if set_schema_name {
        split[0].to_string()
    } else {
        format!("{}_{}", split[1], split[0])
    };

    let x_collection_name_ptr = &resource_spec_pointers.x_collection_name;
    let Some(x_collection_name_prev) = x_collection_name_ptr.create_value(resource_spec) else {
        anyhow::bail!(
            "cannot create location '{x_collection_name_ptr}' in resource spec '{resource_spec}'"
        );
    };
    let _ = std::mem::replace(x_collection_name_prev, x_collection_name.into());

    if set_schema_name {
        let x_schema_name = split[1];
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
