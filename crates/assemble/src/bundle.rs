use serde_json::{json, Value};

// bundled_schema builds a self-contained JSON schema document for the given
// |schema| URL (which may include a fragment pointer).
// Referenced external schemas are inlined into the document as definitions.
pub fn bundled_schema<'a>(
    schema: &url::Url,
    imports: &[tables::Import],
    resources: &[tables::Resource],
) -> Value {
    // Collect all dependencies of |schema|, inclusive of |schema| itself
    // (as the first item). Project each dependency to a schema document
    // with an associated `$id`.
    let mut schema_no_fragment = schema.clone();
    schema_no_fragment.set_fragment(None);

    // Project each dependency to a schema document with an associated `$id`.
    let mut dependencies = tables::Import::transitive_imports(imports, &schema_no_fragment)
        .enumerate()
        .filter_map(|(ind, url)| {
            resources
                .binary_search_by_key(&url, |r| &r.resource)
                .ok()
                .and_then(|ind| resources.get(ind))
                .map(|resource| {
                    let dom = serde_json::from_str::<serde_json::Value>(resource.content_dom.get())
                        .unwrap();

                    let dom = match dom {
                        Value::Object(m) => {
                            let mut m = m.clone();
                            if !m.contains_key("$id") {
                                m.insert("$id".to_string(), Value::String(url.to_string()));
                            }
                            Value::Object(m)
                        }
                        // Schema is not an object. Wrap in a level of nesting to attach an $id.
                        _ => json!({
                            "$id": url.to_string(),
                            "allOf": [dom],
                        }),
                    };

                    // The property name doesn't matter, so long as it's unique
                    // and doesn't clobber an existing $def of the root.
                    // Schema parsers use the internal $id to resolve relative URIs.
                    (format!("__flowInline{ind}"), dom)
                })
        })
        .collect::<Vec<_>>() // Flatten to an ExactSizeIterator.
        .into_iter();

    let bundle = if schema.fragment().is_some() {
        // If the schema includes a fragment pointer, $ref it from the root
        // document to produce a stand-alone schema.
        json!({
            "$defs": Value::Object(dependencies.collect()),
            "$ref": schema.to_string(),
        })
    } else if dependencies.len() != 0 {
        // This is a reference to the root schema document, which is always the
        // first transitive dependency. Use it as the root of the bundle.
        let mut root = dependencies.next().unwrap().1;

        // If there are additional dependencies, merge them into $def's of the root.
        if dependencies.len() != 0 {
            root.as_object_mut()
                .unwrap()
                .entry("$defs")
                .or_insert(json!({}))
                .as_object_mut()
                .unwrap()
                .extend(dependencies);
        }

        root
    } else {
        Value::Bool(false)
    };

    bundle
}

#[cfg(test)]
mod test {
    use super::bundled_schema;
    use doc::inference;

    #[test]
    fn test_bundle_generation() {
        // Load a fixture into imports, collections, and schema docs.
        let fixture = serde_yaml::from_slice(include_bytes!("bundles.yaml")).unwrap();
        let tables::Sources {
            imports,
            resources,
            collections,
            errors,
            ..
        } = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);

        if !errors.is_empty() {
            panic!("unexpected errors {errors:?}");
        }

        // Compile all schemas and index their compiled forms.
        let orig_compiled = tables::Resource::compile_all_json_schemas(&resources).unwrap();
        let mut orig_index = doc::SchemaIndexBuilder::new();
        for (uri, compiled) in &orig_compiled {
            orig_index.add(compiled).unwrap();
            orig_index.add_alias(compiled, uri).unwrap();
        }
        orig_index.verify_references().unwrap();
        let orig_index = orig_index.into_index();

        // We'll collect bundle documents, and snapshot them at the end.
        let mut bundle_docs = serde_json::Map::new();

        for c in collections.iter() {
            // Build the bundled schema DOM.
            let bundle_dom = bundled_schema(&c.read_schema, &imports, &resources);

            // Compile the bundle DOM into a schema, and index it.
            // Note that no external schemas are added to the index, unlike |orig_index|.

            // This URL should be overridden by a contained $id, and thus not matter.
            let bundle_curi = url::Url::parse("test://bundle").unwrap();
            let bundle_schema: doc::Schema =
                json::schema::build::build_schema(bundle_curi, &bundle_dom).unwrap();

            let mut bundle_index = doc::SchemaIndexBuilder::new();
            bundle_index.add(&bundle_schema).unwrap();
            bundle_index.verify_references().unwrap();
            let bundle_index = bundle_index.into_index();

            // Infer the shape of the original (non-bundled) schema.
            let orig_shape =
                inference::Shape::infer(orig_index.fetch(&c.read_schema).unwrap(), &orig_index);

            // Expect our inferences over the shape of the schema are identical.
            let bundle_shape = inference::Shape::infer(&bundle_schema, &bundle_index);
            assert_eq!(
                orig_shape, bundle_shape,
                "comparing shapes of {:?}",
                c.collection
            );

            bundle_docs.insert(c.collection.to_string(), bundle_dom);
        }

        insta::assert_yaml_snapshot!(serde_json::json!(bundle_docs));
    }
}
