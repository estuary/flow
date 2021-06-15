use super::tables;
use serde_json::{json, Value};
use url::Url;

// bundled_schema builds a self-contained JSON schema document for the given
// |schema| URL (which may include a fragment pointer).
// Referenced external schemas are inlined into the document as definitions.
pub fn bundled_schema(
    schema: &Url,
    imports: &[&tables::Import],
    schema_docs: &[&tables::SchemaDoc],
) -> Value {
    // Collect all dependencies of |schema|, inclusive of |schema| itself
    // (as the first item). Project each dependency to a schema document
    // with an associated `$id`.
    let mut schema_no_fragment = schema.clone();
    schema_no_fragment.set_fragment(None);

    let mut dependencies = tables::Import::transitive_imports(imports, &schema_no_fragment)
        .enumerate()
        .filter_map(|(ind, url)| {
            schema_docs
                .binary_search_by_key(&url, |d| &d.schema)
                .ok()
                .and_then(|ind| schema_docs.get(ind))
                .map(|d| {
                    let d = match &d.dom {
                        Value::Object(m) if !m.contains_key("$id") => {
                            let mut m = m.clone();
                            m.insert("$id".to_string(), json!(d.schema.to_string()));
                            Value::Object(m)
                        }
                        // Wrap in a level of nesting to attach the $id.
                        _ => json!({
                            "$id": d.schema.to_string(),
                            "allOf": [&d.dom],
                        }),
                    };

                    // The property name doesn't matter, so long as it's unique
                    // and doesn't clobber an existing $def of the root.
                    // Schema parsers use the internal $id to resolve relative URIs.
                    (format!("__flowInline{}", ind), d)
                })
        });

    let bundle = if schema.fragment().is_some() {
        // If the schema includes a fragment pointer, $ref it from the root
        // document to produce a stand-alone schema.
        json!({
            "$defs": Value::Object(dependencies.collect()),
            "$ref": schema.to_string(),
        })
    } else {
        // This is a reference to the root schema document, which is always the
        // first transitive dependency. Use it as the root of the bundle.
        let mut root = dependencies.next().unwrap().1;

        // If there are additional dependencies, merge them into $def's of the root.
        let to_add: Vec<_> = dependencies.collect();
        if !to_add.is_empty() {
            if let Some(defs) = root
                .as_object_mut()
                .unwrap()
                .entry("$defs")
                .or_insert(json!({}))
                .as_object_mut()
            {
                defs.extend(to_add.into_iter())
            }
        }

        root
    };

    bundle
}
