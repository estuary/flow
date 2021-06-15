use doc::inference;
use itertools::Itertools;
use models::tables;

#[test]
fn test_bundle_generation() {
    // Load a fixture into imports, collections, and schema docs.
    let fixture = serde_yaml::from_slice(include_bytes!("bundles.yaml")).unwrap();
    let sources::Tables {
        imports,
        schema_docs,
        collections,
        ..
    } = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);

    // Compile all schemas and index their compiled forms.
    let orig_compiled = tables::SchemaDoc::compile_all(&schema_docs).unwrap();
    let mut orig_index = doc::SchemaIndex::new();
    for compiled in &orig_compiled {
        orig_index.add(compiled).unwrap()
    }
    orig_index.verify_references().unwrap();

    // Index imports and schema_docs (expected ordering of bundled_schema).
    let imports = imports
        .iter()
        .sorted_by_key(|i| (&i.from_resource, &i.to_resource))
        .collect::<Vec<_>>();
    let schema_docs = schema_docs
        .iter()
        .sorted_by_key(|d| &d.schema)
        .collect::<Vec<_>>();

    // We'll collect bundle documents, and snapshot them at the end.
    let mut bundle_docs = serde_json::Map::new();

    for c in collections.iter() {
        // Build the bundled schema DOM.
        let bundle_dom = models::build::bundled_schema(&c.schema, &imports, &schema_docs);

        // Compile the bundle DOM into a schema, and index it.
        // Note that no external schemas are added to the index, unlike |orig_index|.

        // This URL should be overridden by a contained $id, and thus not matter.
        let bundle_curi = url::Url::parse("test://bundle").unwrap();
        let bundle_schema: doc::Schema =
            json::schema::build::build_schema(bundle_curi, &bundle_dom).unwrap();

        let mut bundle_index = doc::SchemaIndex::new();
        bundle_index.add(&bundle_schema).unwrap();
        bundle_index.verify_references().unwrap();

        // Infer the shape of the original (non-bundled) schema.
        let orig_shape = inference::Shape::infer(orig_index.fetch(&c.schema).unwrap(), &orig_index);

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
