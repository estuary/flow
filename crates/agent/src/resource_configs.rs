use serde_json::Value;

///
/// # Panics
/// If the `full_collection_name` doesn't contain any `/` characters, which should never
/// be the case since we should have already validated the collection name.
pub fn update_materialization_resource_spec(
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

    let collection_name = split[0];
    let task_name = split[1];

    let collection_name_ptr = &resource_spec_pointers.collection_name;

    let Some(collection_name_prev) = collection_name_ptr.create_value(resource_spec) else {
        anyhow::bail!(
            "cannot create location '{collection_name_ptr}' in resource spec '{resource_spec}'"
        );
    };

    let _ = std::mem::replace(collection_name_prev, collection_name.into());

    if let Some(task_name_ptr) = &resource_spec_pointers.task_name {
        if let Some(task_name_prev) = task_name_ptr.create_value(resource_spec) {
            let _ = std::mem::replace(task_name_prev, task_name.into());
        }
    }

    Ok(())
}

pub struct ResourceSpecPointers {
    collection_name: doc::Pointer,
    task_name: Option<doc::Pointer>,
}

/// Runs inference on the given schema and searches for a location within the resource spec
/// that bears the `x-collection-name` and `x-schema-name` annotations. Returns the pointer to those location, or an
/// error if no such location exists. Errors from parsing the schema are returned directly.
/// The schema must be fully self-contained (a.k.a. bundled), or an error will be returned.
pub fn pointer_for_schema(schema_json: &str) -> anyhow::Result<ResourceSpecPointers> {
    // While all known connector resource spec schemas are self-contained, we don't
    // actually do anything to guarantee that they are. This function may fail in that case.
    let schema = doc::validation::build_bundle(schema_json)?;
    let mut builder = doc::SchemaIndexBuilder::new();
    builder.add(&schema)?;
    let index = builder.into_index();
    let shape = doc::Shape::infer(&schema, &index);

    let mut collection_name: Option<doc::Pointer> = None;
    let mut task_name: Option<doc::Pointer> = None;
    for (ptr, _, prop_shape, _) in shape.locations() {
        if prop_shape.annotations.contains_key("x-collection-name") {
            collection_name = Some(ptr)
        } else if prop_shape.annotations.contains_key("x-schema-name") {
            task_name = Some(ptr)
        }
    }

    if let Some(collection_name_ptr) = collection_name {
        Ok(ResourceSpecPointers {
            collection_name: collection_name_ptr,
            task_name
        })
    } else {
        Err(anyhow::anyhow!(
            "resource spec schema does not contain any location annotated with x-collection-name"
        ))
    }
}
