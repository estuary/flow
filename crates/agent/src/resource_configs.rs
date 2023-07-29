use doc::Pointer;
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

#[tracing::instrument(level = "debug")]
pub fn update_materialization_resource_spec(
    materialization_name: &str,
    resource_spec: &mut Value,
    collection_name_ptr: &Pointer,
    new_collection_name: Option<String>,
) -> anyhow::Result<()> {
    let maybe_new_name = new_collection_name
        .as_ref()
        .map(|n| {
            n.rsplit('/')
                .next()
                .expect("collection name must contain a slash")
                .to_owned()
        })
        .or_else(|| {

            // If no explicit name was given, then just add or increment a
            // version suffix to the resource config.
            let nn = collection_name_ptr
                .query(&*resource_spec)
                .and_then(|v| v.as_str())
                .map(crate::next_name);
            tracing::debug!(%resource_spec, next_name = ?nn, ptr = %collection_name_ptr, "determined next name");
            nn
        });
    let Some(new_val) = maybe_new_name else {
        // This may or may not be something we should consider an error. The
        // question comes down to whether or not it's acceptable to have an
        // empty resource config. As far as I know, we've yet to really make a
        // decision one way or the other, as this situation has not come up yet.
        tracing::warn!(%materialization_name, ?new_collection_name, %collection_name_ptr, "not updating resource spec because there is no existing value at that location and no new collection name was provided");
        return Ok(());
    };

    if let Some(prev_val) = collection_name_ptr.create_value(resource_spec) {
        tracing::info!(%prev_val, %new_val, %materialization_name, "updating resource spec");
        *prev_val = Value::String(new_val);
    } else {
        anyhow::bail!("creating x-collection-name JSON location failed");
    }
    Ok(())
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
            return Ok(doc::Pointer::from_str(&ptr));
        }
    }
    Err(anyhow::anyhow!(
        "resource spec schema does not contain any location annotated with x-collection-name"
    ))
}
