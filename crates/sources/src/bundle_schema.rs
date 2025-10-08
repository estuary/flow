use models::RawValue;
use serde_json::{Value, json};

// bundled_schema builds a self-contained JSON schema document for the given
// |schema| URL (which may include a fragment pointer).
// Referenced external schemas are inlined into the document as definitions.
pub fn bundle_schema<'a>(
    scope: &url::Url,
    schema: &RawValue,
    imports: &[tables::Import],
    resources: &[tables::Resource],
) -> Value {
    // We'll collect schemas to be bundled.
    let mut schemas = Vec::new();

    // We need to use `scope` as an $id for `schema`, but it contains a fragment
    // location and fragments are disallowed by the JSON Schema spec.
    // To account for this, tweak it by mapping the location into a query parameter.
    let mut tweaked_scope = scope.clone();
    if let Some(ptr) = tweaked_scope.fragment().map(str::to_string) {
        tweaked_scope.set_query(Some(&format!("ptr={ptr}")));
        tweaked_scope.set_fragment(None);
    }

    // Process `schema`. If it's a bare string, then it's an import of another schema and:
    // * If the joined import URL has a fragment location then we must generate
    //   an implicit schema with the fragment $ref.
    // * Otherwise, we can drop this schema altogether and track only its import.
    // If `schema` is not a bare string, it's a regular schema and must be bundled.
    match serde_json::from_str::<Value>(schema.get()).unwrap() {
        Value::String(import) => match tweaked_scope.join(&import) {
            Ok(joined) if joined.fragment().is_some() => {
                schemas.push(embed_id(&tweaked_scope, json!({ "$ref": import })));
            }
            _ => {}
        },
        value @ _ => {
            schemas.push(embed_id(&tweaked_scope, value));
        }
    }

    // Gather and canonical-ize all transitive imported JSON Schemas of `scope`.
    for import in tables::Import::transitive_imports(imports, scope) {
        if let Some(resource) = tables::Resource::fetch(resources, import) {
            schemas.push(embed_id(
                &resource.resource,
                serde_json::from_str::<Value>(resource.content_dom.get()).unwrap(),
            ));
        }
    }

    let mut schemas = schemas.into_iter().enumerate().map(|(index, schema)| {
        // The property name doesn't matter, so long as it's unique
        // and doesn't clobber an existing $def of the root.
        // Schema parsers use the internal $id to resolve relative URIs.
        (format!("__flowInline{index}"), schema)
    });

    // The first schema becomes the root of the bundle.
    let (_, mut root) = schemas.next().unwrap_or_default();

    // If there are additional schemas, merge them into $def's of the root.
    if schemas.len() != 0 {
        root.as_object_mut()
            .unwrap()
            .entry("$defs")
            .or_insert(json!({}))
            .as_object_mut()
            .unwrap()
            .extend(schemas);
    }

    root
}

fn embed_id(curi: &url::Url, dom: Value) -> Value {
    match dom {
        Value::Object(m) => {
            let mut m = m.clone();
            if !m.contains_key("$id") {
                m.insert("$id".to_string(), Value::String(curi.to_string()));
            }
            Value::Object(m)
        }
        // Schema is not an object. Wrap in a level of nesting to attach an $id.
        _ => json!({
            "$id": curi.to_string(),
            "allOf": [dom],
        }),
    }
}

#[cfg(test)]
mod test {
    use super::bundle_schema;

    #[test]
    fn test_bundle_generation() {
        // Load a fixture into imports, collections, and schema docs.
        let fixture = serde_yaml::from_slice(include_bytes!("bundle_schema_test.yaml")).unwrap();
        let tables::DraftCatalog {
            imports,
            resources,
            collections,
            errors,
            ..
        } = crate::scenarios::evaluate_fixtures(Default::default(), &fixture);

        if !errors.is_empty() {
            panic!("unexpected errors {errors:?}");
        }

        // We'll collect bundle documents, and snapshot them at the end.
        let mut bundle_docs = serde_json::Map::new();

        for c in collections.iter() {
            let Some(model) = &c.model else { continue };

            let mut scope = c.scope.clone();
            scope.set_fragment(Some(&format!("{}/schema", scope.fragment().unwrap())));

            // Build the bundled schema DOM.
            let bundle_dom =
                bundle_schema(&scope, model.schema.as_ref().unwrap(), &imports, &resources);

            // Compile the bundle DOM into a schema, and index it.
            // Note that no external schemas are added to the index, unlike |orig_index|.

            // This URL should be overridden by a contained $id, and thus not matter.
            let bundle_curi = url::Url::parse("test://bundle").unwrap();
            let bundle_schema: doc::Schema =
                json::schema::build::build_schema(&bundle_curi, &bundle_dom).unwrap();

            let mut bundle_index = doc::SchemaIndexBuilder::new();
            bundle_index.add(&bundle_schema).unwrap();
            bundle_index.verify_references().unwrap();

            bundle_docs.insert(c.collection.to_string(), bundle_dom);
        }

        insta::assert_yaml_snapshot!(serde_json::json!(bundle_docs));
    }
}
