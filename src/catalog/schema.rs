use super::{sql_params, ContentType, Error, Resource, Result, DB, BuildContext};
use crate::specs::build as specs;
use crate::doc::Schema as CompiledSchema;
use estuary_json::schema::{
    build::build_schema, 
    Application, Keyword
};
use url::Url;

pub const INLINE_POINTER_KEY: &str = "schema_ptr";

/// The result of successfully registering a schema.
#[derive(Debug, PartialEq)]
pub struct RegisteredSchema {
    /// The `Schema` object, which represents the entire schema document. If registered url had a
    /// fragment identifying a specific sub-schema, then this would be the entire parent schema,
    /// not just the sub-schema.
    pub schema: Schema,
    /// The fully resolved `Url` that should be used to address the schema. If the provided url had
    /// a fragment that identified specific sub-schema, then the fragment will be preserved as part
    /// of this. This is the url that should be persisted as part of the `collections` and
    /// `transforms` tables.
    pub schema_url: Url,
}

/// Schema represents an entire JSON-Schema document.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Schema {
    pub resource: Resource,
}

impl Schema {
    /// Register a JSON-Schema document. If already registered, this is a no-op and its existing
    /// handle is returned. Otherwise the document and all of its recursive references are added to
    /// the catalog. The return value will hold both the full `Schema`, as well as the `Url` that
    /// should be used to reference the specific sub-schema within it.
    pub fn register(context: &BuildContext, spec: &specs::Schema) -> Result<RegisteredSchema> {
        match spec {
            specs::Schema::Url(url) => {
                // Schema URLs frequently have fragment components which locate a specific
                // sub-schema within the schema document. The URL that's used to refer to a
                // specific schema for a Collection or Derivation needs to include that fragment,
                // if one is present. But the URL of the `Resource` must not include the fragment,
                // since the resource corresponds to the entire document, and we always process the
                // entire document in a call to `Schema::register`.
                let schema_url = context.resource_url.join(url)?;
                // Drop the fragment for purposes of resolving and registering the document itself.
                let mut resource_url = schema_url.clone();
                resource_url.set_fragment(None);

                let resource = Resource::register(context.db, ContentType::Schema, &resource_url)?;
                let context = context.for_new_resource(&resource_url);

                Schema::register_schema_resource(&context, resource, resource_url.clone(), schema_url)
            }
            inline @ specs::Schema::Object(_) | inline @ specs::Schema::Bool(_) => {
                // Create the full URL be taking the URL of the parent resource (typcially a
                // catalog yaml document) and adding a query parameter with a json pointer to
                // the location of the inline schema.
                let mut url = context.resource_url.clone();
                let schema_ptr = context.current_location_pointer();
                url.query_pairs_mut().append_pair(INLINE_POINTER_KEY, schema_ptr.as_str());
                url.set_fragment(None); // Clear the fragment just in case there was one in the base.

                // Now we'll create the content for this resource, since `Resource::register` won't
                // be able to resolve the json pointer. Technically, this means that inline schemas
                // get stored twice. Once in the original catalog resource, and again in the schema
                // resource, which has the same URL except with the addition of the query
                // parameter.
                let content = serde_json::to_vec(inline)?;
                let resource = Resource::register_content(context.db, ContentType::Schema, &url, content.as_slice())?;
                // For inline schemas, the `resource_url` and the `schema_url` will be the same.
                Schema::register_schema_resource(context, resource, url.clone(), url)
            }
        }
    }

    fn register_schema_resource(context: &BuildContext, resource: Resource, resource_url: Url, schema_url: Url) -> Result<RegisteredSchema> {

        let schema = Schema { resource };
        if !resource.is_processed(context.db)? {
            resource.mark_as_processed(context.db)?;

            let dom = schema.resource.content(context.db)?;
            let dom = serde_yaml::from_slice::<serde_json::Value>(&dom)?;

            let compiled: CompiledSchema = build_schema(resource_url, &dom)?;

            // Walk the schema to identify sub-schemas having canonical URIs which differ
            // from the registered |url|. Each of these canonical URIs is registered as
            // an alternate URL of this schema resource. By doing this, when we encounter
            // a direct reference to a sub-schema's canonical URI elsewhere, we will
            // correctly resolve it back to this resource.
            schema.register_alternate_urls(context.db, &compiled)?;

            // Walk the schema again, this time registering schemas which it references.
            // Since we've already registered alternate URLs, and usage of those URLs
            // in references will correctly resolve back to this document.
            schema.register_references(context, &compiled)?;
        }

        Ok(RegisteredSchema {schema, schema_url})
    }

    /// Walks compiled schema and registers an alternate URL for each encountered $id.
    fn register_alternate_urls(&self, db: &DB, compiled: &CompiledSchema) -> Result<()> {
        // Register Schemas having fragment-less canonical URIs.
        // Note the JSON-Schema spec requires that $id applications have no fragment.
        if compiled.curi.fragment().is_none() {
            self.resource.register_alternate_url(db, &compiled.curi)?;
        }
        for kw in &compiled.kw {
            if let Keyword::Application(_, child) = kw {
                self.register_alternate_urls(&db, child)?;
            }
        }
        Ok(())
    }

    /// Walks compiled schema and registers schemas which it references.
    fn register_references(&self, context: &BuildContext, compiled: &CompiledSchema) -> Result<()> {

        for kw in &compiled.kw {
            if let Keyword::Application(app, child) = kw {
                // "Ref" applications indirect to a canonical schema URI which may
                // be in this document or another, and often include a fragment
                // component bearing a JSON-pointer into the document. We strip the
                // fragment here, since we're registering with whole-document granularity.
                if let Application::Ref(ref_uri) = app {
                    let mut uri = context.resource_url.join(ref_uri.as_str())?;
                    uri.set_fragment(None);
                    let spec = specs::Schema::Url(uri.to_string());

                    let context = BuildContext::new_from_root(context.db, &uri);
                    let import = Self::register(&context, &spec).map_err(|e| Error::At {
                        loc: format!("$ref: {}", ref_uri),
                        detail: Box::new(e),
                    })?;

                    Resource::register_import(context.db, self.resource, import.schema.resource)?;
                }
                // Recurse to sub-schemas.
                self.register_references(context, child)?;
            }
        }
        Ok(())
    }

    /// Fetch and compile all Schemas in the catalog.
    pub fn compile_all(db: &DB) -> Result<Vec<CompiledSchema>> {
        let mut stmt = db.prepare(
            "SELECT url, content FROM resources NATURAL JOIN resource_urls
                    WHERE content_type = ? AND is_primary;",
        )?;
        let mut rows = stmt.query(sql_params![ContentType::Schema])?;

        let mut schemas = Vec::new();
        while let Some(row) = rows.next()? {
            let (url, blob): (Url, Vec<u8>) = (row.get(0)?, row.get(1)?);
            let dom: serde_json::Value = serde_yaml::from_slice(&blob)?;
            let compiled: CompiledSchema = build_schema(url, &dom)?;
            schemas.push(compiled);
        }
        schemas.shrink_to_fit();
        Ok(schemas)
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{dump_table, dump_tables, init_db_schema, open},
        *,
    };
    use rusqlite::params as sql_params;
    use serde_json::{json, Value};

    #[test]
    fn test_register_with_alt_urls_and_self_references() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

        let doc = json!({
            "$id": "test://example/root",
            "$defs": {
                "a": {
                    "$id": "test://example/other/a-doc",
                    "items": [
                        true,
                        {"$ref": "b-doc#/items/1"},
                    ],
                },
                "b": {
                    "$id": "test://example/other/b-doc",
                    "items": [
                        {"$ref": "a-doc#/items/0"},
                        true,
                    ],
                },
                "c": true,
            },
            "allOf": [
                {"$ref": "other/a-doc#/items/1"},
                {"$ref": "test://example/other/b-doc#/items/0"},
                {"$ref": "#/$defs/c"},
                {"$ref": "root#/$defs/c"},
                {"$ref": "test://example/root#/$defs/c"},
            ],
        });
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE);",
            &[doc],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (10, 'test://actual', TRUE);",
            sql_params![],
        )?;

        let schema_url = Url::parse("test://actual")?;
        let build_context = BuildContext::new_from_root(&db, &schema_url);

        // Kick off registration using the registered resource path. Expect it works.
        let registered_schema = Schema::register(&build_context, &specs::Schema::Url(schema_url.to_string()))?;

        assert_eq!(schema_url, registered_schema.schema_url);
        let s = registered_schema.schema;
        assert_eq!(s.resource.id, 10);
        assert!(s.resource.is_processed(&db)?);

        assert_eq!(
            dump_table(&db, "resource_urls")?,
            json!([
                (10, "test://actual", true),
                (10, "test://example/root", Value::Null),
                (10, "test://example/other/a-doc", Value::Null),
                (10, "test://example/other/b-doc", Value::Null),
            ]),
        );

        Ok(())
    }

    #[test]
    fn test_register_with_external_references() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

        let doc_a = json!({"$ref": "b#/$defs/c"});
        let doc_b = json!({"$defs": {"c": {"$ref": "c"}}});
        let doc_c = json!(true);
        let doc_d = json!(false);

        db.execute(
            "INSERT INTO resources
            (resource_id, content_type, content, is_processed) VALUES
            (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
            (20, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
            (30, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
            (40, 'application/schema+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![&doc_a, &doc_b, &doc_c, &doc_d],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (10, 'file:///dev/null/a', TRUE),
                (20, 'file:///dev/null/b', TRUE),
                (30, 'file:///dev/null/c', TRUE),
                (40, 'file:///dev/null/d', TRUE);
            ",
            sql_params![],
        )?;
        let url = Url::parse("file:///dev/null/a")?;
        let context = BuildContext::new_from_root(&db, &url);

        let registered_schema = Schema::register(&context, &specs::Schema::Url(url.to_string()))?;
        assert_eq!(url, registered_schema.schema_url);
        let s = registered_schema.schema;
        assert_eq!(s.resource.id, 10);

        assert_eq!(
            dump_tables(&db, &["resources", "resource_imports"])?,
            json!({
                "resources": [
                    [10, "application/schema+yaml", doc_a.to_string(), true],
                    [20, "application/schema+yaml", doc_b.to_string(), true],
                    [30, "application/schema+yaml", doc_c.to_string(), true],
                    [40, "application/schema+yaml", doc_d.to_string(), false], // Not reached.
                ],
                "resource_imports": [
                    [20, 30],
                    [10, 20],
                ],
            }),
        );

        Ok(())
    }

    #[test]
    fn inline_schema_is_registered() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

        let catalog_yaml = r##"
            collections:
                - name: testcollection
                  schema:
                    type: object
                    additionalProperties: true
                  key: [/mykey]
                  fixtures: [nonexistant/fixtures.yaml]
        "##;

        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) 
            VALUES
            (10, 'application/vnd.estuary.dev-catalog-spec+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![catalog_yaml],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                (10, 'file:///dev/null/a/catalog.yaml', TRUE);",
            sql_params![],
        )?;

        let url = Url::parse("file:///dev/null/a/catalog.yaml")?;
        let context = BuildContext::new_from_root(&db, &url);
        let spec = serde_yaml::from_str::<specs::Catalog>(catalog_yaml)?;

        let result = context.process_child_field("collections/0/schema", &spec.collections[0].schema, Schema::register)
            .expect("failed to register schema");

        let expected_url = Url::parse("file:///dev/null/a/catalog.yaml?schema_ptr=%2Fcollections%2F0%2Fschema")?;
        assert_eq!(expected_url, result.schema_url);
        assert_eq!(expected_url, result.schema.resource.primary_url(&db)?);
        let schema_content = result.schema.resource.content(&db)?;
        let actual_schema: Value = serde_json::from_slice(schema_content.as_slice())
            .expect("failed to parse schema");

        let expected_schema = json!({
            "type": "object",
            "additionalProperties": true
        });
        assert_eq!(expected_schema, actual_schema);

        Ok(())
    }

}
