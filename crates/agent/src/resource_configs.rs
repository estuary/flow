use serde_json::{value::RawValue, Value};

pub async fn fetch_resource_spec_schema(
    image: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<Box<RawValue>> {
    let Some(colon_idx) = image.find(':') else {
        anyhow::bail!("connector image '{image}' is missing a version tag");
    };
    let image_name = &image[..colon_idx];
    let image_tag = &image[colon_idx..];

    let schema_json = agent_sql::evolutions::fetch_resource_spec_schema(
        image_name.to_owned(),
        image_tag.to_owned(),
        txn,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("no resource spec schema found for image '{image}"))?;
    Ok(schema_json.0)
}

///
/// # Panics
/// If the `full_collection_name` doesn't contain any `/` characters, which should never
/// be the case since we should have already validated the collection name.
pub fn update_materialization_resource_spec(
    resource_spec: &mut Value,
    collection_name_ptr: &doc::Pointer,
    full_collection_name: &str,
) -> anyhow::Result<Value> {
    let resource_name = full_collection_name
        .rsplit_once('/')
        .expect("collection name is invalid (does not contain '/')")
        .1
        .to_owned();

    let Some(prev) = collection_name_ptr.create_value(resource_spec) else {
        anyhow::bail!(
            "cannot create location '{collection_name_ptr}' in resource spec '{resource_spec}'"
        );
    };

    Ok(std::mem::replace(prev, resource_name.into()))
}

/// Runs inference on the given schema and searches for a location within the resource spec
/// that bears the `x-collection-name` annotation. Returns the pointer to that location, or an
/// error if no such location exists. Errors from parsing the schema are returned directly.
/// The schema must be fully self-contained (a.k.a. bundled), or an error will be returned.
pub fn pointer_for_schema(schema_json: &str) -> anyhow::Result<doc::Pointer> {
    // While all known connector resource spec schemas are self-contained, we don't
    // actually do anything to guarantee that they are. This function may fail in that case.
    let schema = doc::validation::build_bundle(schema_json)?;
    let mut builder = doc::SchemaIndexBuilder::new();
    builder.add(&schema)?;
    let index = builder.into_index();
    let shape = doc::Shape::infer(&schema, &index);

    for (ptr, _, prop_shape, _) in shape.locations() {
        if prop_shape.annotations.contains_key("x-collection-name") {
            return Ok(ptr);
        }
    }
    Err(anyhow::anyhow!(
        "resource spec schema does not contain any location annotated with x-collection-name"
    ))
}
