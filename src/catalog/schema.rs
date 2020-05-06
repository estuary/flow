use super::{sql_params, ContentType, Resource, Result, DB};
use crate::doc::Schema as CompiledSchema;
use estuary_json::schema::{build::build_schema, Application, Keyword};
use url::Url;

/// Schema represents a catalog JSON-Schema document.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Schema {
    pub resource: Resource,
}

impl Schema {
    /// Register a JSON-Schema document at the URI. If already registered, this
    /// is a no-op and its existing handle is returned. Otherwise the document
    /// and all of its recursive references are added to the catalog.
    pub fn register(db: &DB, url: &Url) -> Result<Schema> {
        // Schema URLs frequently have fragment components which locate a
        // specific sub-schema within the schema document. Drop the fragment
        // for purposes of resolving and registering the document itself.
        let mut url = url.clone();
        url.set_fragment(None);

        let schema = Schema {
            resource: Resource::register(db, ContentType::Schema, &url)?,
        };
        if schema.resource.is_processed(db)? {
            return Ok(schema);
        }
        schema.resource.mark_as_processed(db)?;

        let dom = schema.resource.content(db)?;
        let dom = serde_yaml::from_slice::<serde_json::Value>(&dom)?;
        let compiled: CompiledSchema = build_schema(url, &dom)?;

        // Walk the schema to identify sub-schemas having canonical URIs which differ
        // from the registered |url|. Each of these canonical URIs is registered as
        // an alternate URL of this schema resource. By doing this, when we encounter
        // a direct reference to a sub-schema's canonical URI elsewhere, we will
        // correctly resolve it back to this resource.
        schema.register_alternate_urls(db, &compiled)?;

        // Walk the schema again, this time registering schemas which it references.
        // Since we've already registered alternate URLs, and usage of those URLs
        // in references will correctly resolve back to this document.
        schema.register_references(db, &compiled)?;

        Ok(schema)
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
    fn register_references(&self, db: &DB, compiled: &CompiledSchema) -> Result<()> {
        for kw in &compiled.kw {
            if let Keyword::Application(app, child) = kw {
                // "Ref" applications indirect to a canonical schema URI which may
                // be in this document or another, and often include a fragment
                // component bearing a JSON-pointer into the document. We strip the
                // fragment here, since we're registering with whole-document granularity.
                if let Application::Ref(uri) = app {
                    let mut uri = uri.clone();
                    uri.set_fragment(None);

                    let import = Self::register(db, &uri)?;
                    Resource::register_import(&db, self.resource, import.resource)?;
                }
                // Recurse to sub-schemas.
                self.register_references(&db, child)?;
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

        // Kick off registration using the registered resource path. Expect it works.
        let s = Schema::register(&db, &Url::parse("test://actual")?)?;
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

        let s = Schema::register(&db, &Url::parse("file:///dev/null/a")?)?;
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
}
