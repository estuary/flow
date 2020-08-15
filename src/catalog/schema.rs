use super::{sql_params, ContentType, Resource, Result, Scope, DB};
use crate::doc::{Schema as CompiledSchema, SchemaIndex};
use crate::specs::build as specs;
use estuary_json::schema::{build::build_schema, Application, Keyword};
use url::Url;

pub const INLINE_POINTER_KEY: &str = "ptr";

/// Schema represents a JSON-Schema document, and an optional fragment which
/// further locates a specific sub-schema thereof.
#[derive(Debug)]
pub struct Schema {
    pub resource: Resource,
    pub fragment: Option<String>,
}

impl Schema {
    /// Retrieve the primary URL of this Schema Resource, joined with the fragment sub-location.
    pub fn primary_url_with_fragment(&self, db: &DB) -> Result<Url> {
        let mut url = self.resource.primary_url(db)?;
        url.set_fragment(self.fragment.as_deref());
        Ok(url)
    }

    /// Register a JSON-Schema document relative to a current, external Resource
    /// (which must be the current Resource of the Scope). The JSON-Schema may
    /// be either a relative URL, or inline.
    /// * If a relative URL, the canonical URL is determined by joining with the
    ///   Scope's base URL in the usual way.
    /// * If an inline URL, a canonical URL is created by extending the Scope's
    ///   base URL with a query argument which captures the Scope's current
    ///   location. The inline document is then registered as a resource at that
    ///   (fictional) URL.
    pub fn register(scope: Scope, spec: &specs::Schema) -> Result<Schema> {
        // Map either inline vs relative URL cases to a canonical URL.
        let url = match spec {
            specs::Schema::Object(_) | specs::Schema::Bool(_) => {
                // Create the full URL by taking the URL of the parent resource (typically a
                // catalog yaml document) and adding a query parameter with a json pointer to
                // the location of the inline schema.
                let mut url = scope.resource().primary_url(scope.db)?;
                url.set_query(Some(&format!(
                    "{}={}",
                    INLINE_POINTER_KEY,
                    scope.location.url_escaped()
                )));

                Resource::register_content(
                    scope.db,
                    ContentType::Schema,
                    &url,
                    serde_json::to_vec(spec)?.as_slice(),
                )?;
                url
            }
            specs::Schema::Url(url) => scope.resource().primary_url(scope.db)?.join(url)?,
        };

        Self::register_url(scope, &url)
    }

    /// Register a JSON-Schema document at the URL, which must be the canonical
    /// URI of the schema. If already registered, this is a no-op and its existing
    /// handle is returned. Otherwise the document and all of its recursive references
    /// are added to the catalog.
    pub fn register_url(scope: Scope, canonical_url: &Url) -> Result<Schema> {
        // Schema URLs frequently have fragment components which locate a specific
        // sub-schema within the schema document. Decompose it to track separately,
        // then work with the entire schema document.
        let fragment = canonical_url.fragment().map(str::to_owned);
        let mut base_url = canonical_url.clone();
        base_url.set_fragment(None);

        let resource = Resource::register(scope.db, ContentType::Schema, &base_url)?;
        let scope = scope.push_resource(resource);

        let dom = resource.content(scope.db)?;
        let dom = serde_yaml::from_slice::<serde_json::Value>(&dom)?;
        let compiled: CompiledSchema = build_schema(resource.primary_url(scope.db)?, &dom)?;

        if !resource.is_processed(scope.db)? {
            resource.mark_as_processed(scope.db)?;

            // Walk the schema to identify sub-schemas having canonical URIs which differ
            // from the registered |url|. Each of these canonical URIs is registered as
            // an alternate URL of this schema resource. By doing this, when we encounter
            // a direct reference to a sub-schema's canonical URI elsewhere, we will
            // correctly resolve it back to this resource.
            Self::register_alternate_urls(scope, &compiled)?;

            // Walk the schema again, this time registering schemas which it references.
            // Since we've already registered alternate URLs, usages of those URLs
            // in references will correctly resolve back to this document.
            Self::register_references(scope, &compiled)?;
        }

        // Index the compiled schema, and confirm that the registered URL (with fragment)
        // resolves correctly.
        let mut index = SchemaIndex::new();
        index.add(&compiled)?;
        index.must_fetch(canonical_url)?;

        Ok(Schema { resource, fragment })
    }

    /// Walks compiled schema and registers an alternate URL for each encountered $id.
    fn register_alternate_urls(scope: Scope, compiled: &CompiledSchema) -> Result<()> {
        // Register Schemas having fragment-less canonical URIs.
        // Note the JSON-Schema spec requires that $id applications have no fragment.
        if compiled.curi.fragment().is_none() {
            scope
                .resource()
                .register_alternate_url(scope.db, &compiled.curi)?;
        }
        for kw in &compiled.kw {
            if let Keyword::Application(app, child) = kw {
                // Add Application keywords to the Scope's Location.
                let location = app.push_keyword(&scope.location);
                let scope = Scope {
                    location: app.push_keyword_target(&location),
                    ..scope
                };
                Self::register_alternate_urls(scope, child)?;
            }
        }
        Ok(())
    }

    /// Walks compiled schema and registers schemas which it references.
    fn register_references(scope: Scope, compiled: &CompiledSchema) -> Result<()> {
        for kw in &compiled.kw {
            if let Keyword::Application(app, child) = kw {
                // Add Application keywords to the Scope's Location.
                let location = app.push_keyword(&scope.location);
                let scope = Scope {
                    location: app.push_keyword_target(&location),
                    ..scope
                };
                // "Ref" applications indirect to a canonical schema URI which may
                // be in this document or another, and often include a fragment
                // component bearing a JSON-pointer into the document. We strip the
                // fragment here, since we're registering with whole-document granularity.
                if let Application::Ref(uri) = app {
                    let refed = Self::register_url(scope, uri)?;
                    Resource::register_import(scope, refed.resource)?;
                }
                // Recurse to sub-schemas.
                Self::register_references(scope, child)?;
            }
        }
        Ok(())
    }

    /*
    pub fn compile_required(db: &DB, collection: Collection) -> Result<Vec<CompiledSchema>> {
        use std::collections::HashSet;
        let mut resource_ids = HashSet::new();
        let mut current = collection.resource.id;

        // loop:
        //   - get schemas for this resource
        //   - get resources that this resource imports
        //       - for each recurse if not already fetched
    }

    fn compile_schemas_for_resource(
        db: &DB,
        resource_id: i64,
        visited: &mut HashSet<i64>,
    ) -> Result<Vec<CompiledSchema>> {
        let mut stmt = db.prepare(
            "SELECT url, content FROM resources NATURAL JOIN resource_urls
                WHERE resource_id = ? AND content_type = ? AND is_primary;",
        )?;
        let mut rows = stmt
            .query(sql_params![resource_id, ContentType::Schema])?
            .mapped(|row| (row.get::<String>(0)?, row.get::<Vec<u8>>(1)?));
        let mut schemas = Vec::new();

        for row in rows {
            let (url, content) = row?;

        }
    }
    */

    /// Fetch and compile all Schemas in the catalog.
    pub fn compile_all(db: &DB) -> Result<Vec<CompiledSchema>> {
        Self::compile_for(db, 1) // 1 is the root resource ID.
    }

    /// Fetch and compile all Schemas of the Resource, as well as Schemas it transitively imports.
    pub fn compile_for(db: &DB, resource_id: i64) -> Result<Vec<CompiledSchema>> {
        let mut stmt = db.prepare(
            "SELECT schema_uri, schema_content FROM resource_schemas WHERE resource_id = ?",
        )?;
        let mut rows = stmt.query(sql_params![resource_id])?;

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
            "$defs": {
                "wrapper": {
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
                }
            },
            "$ref": "test://example/root",
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

        let url = Url::parse("test://actual")?;
        // Kick off registration using the registered resource path. Expect it works.
        let s = Scope::empty(&db).then(|scope| Schema::register_url(scope, &url))?;

        assert_eq!(s.resource.id, 10);
        assert!(s.resource.is_processed(&db)?);
        assert!(s.fragment.is_none());
        assert_eq!(s.primary_url_with_fragment(&db)?, url);

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

        let doc_a = json!({"$defs": {"a": {"$ref": "b#/$defs/c"}}});
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
        let url = Url::parse("file:///dev/null/a#/$defs/a")?;
        let s = Schema::register_url(Scope::empty(&db), &url)?;
        assert_eq!(s.resource.id, 10);
        assert_eq!(s.fragment.as_ref().unwrap(), "/$defs/a");
        assert_eq!(s.primary_url_with_fragment(&db)?, url);

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

        let spec = serde_yaml::from_str::<specs::Catalog>(catalog_yaml)?;

        let result = Scope::empty(&db)
            .push_resource(Resource { id: 10 })
            .push_prop("path to")
            .push_item(32)
            .push_prop("schema")
            .then(|scope| Schema::register(scope, &spec.collections[0].schema))?;

        assert_eq!(
            Url::parse("file:///dev/null/a/catalog.yaml?ptr=/path%20to/32/schema")?,
            result.resource.primary_url(&db)?
        );
        assert!(result.fragment.is_none());

        let schema_content = result.resource.content(&db)?;
        let actual_schema: Value =
            serde_json::from_slice(schema_content.as_slice()).expect("failed to parse schema");

        let expected_schema = json!({
            "type": "object",
            "additionalProperties": true
        });
        assert_eq!(expected_schema, actual_schema);

        Ok(())
    }
}
