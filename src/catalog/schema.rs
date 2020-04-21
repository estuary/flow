use super::{Resource, Result};
use crate::doc::Schema as CompiledSchema;
use estuary_json::schema::{build::build_schema, Application, Keyword};
use rusqlite::{params as sql_params, Connection as DB};
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
    pub fn register(db: &DB, uri: Url) -> Result<Schema> {
        let schema = Schema {
            resource: Resource::register(db, uri)?,
        };
        if !schema.resource.added {
            return Ok(schema);
        }

        let dom = schema.resource.fetch_to_string(db)?;
        let dom = serde_yaml::from_str::<serde_json::Value>(&dom)?;
        db.prepare_cached("INSERT INTO schemas (resource_id, document_json) VALUES(?, ?);")?
            .execute(sql_params![schema.resource.id, dom.to_string().as_str()])?;

        // Recursively traverse static references to catalog other schemas on
        // which this schema document depends.
        schema.add_references(&db, &schema.compile(db)?)?;

        Ok(schema)
    }

    // Walks recursive references of the compiled Schema.
    fn add_references(&self, db: &DB, compiled: &CompiledSchema) -> Result<()> {
        for kw in &compiled.kw {
            match kw {
                Keyword::Application(Application::Ref(ref_uri), _) => {
                    let import = Self::register(db, ref_uri.clone())?;
                    Resource::register_import(&db, self.resource, import.resource)?;
                }
                Keyword::Application(_, child) => {
                    self.add_references(&db, child)?;
                }
                // No-ops.
                Keyword::Anchor(_)
                | Keyword::RecursiveAnchor
                | Keyword::Validation(_)
                | Keyword::Annotation(_) => (),
            }
        }
        Ok(())
    }

    /// Fetch the bundle of Schemas which are directly or indirectly
    /// imported and referenced by the named Resource.
    pub fn _fetch_bundle(db: &DB, source: Resource) -> Result<Vec<Schema>> {
        let mut out = Vec::new();
        let mut stmt = db.prepare_cached(
            "SELECT schemas.resource_id FROM
                    resource_transitive_imports AS rti
                    JOIN schemas
                    ON rti.import_id = schemas.resource_id AND rti.source_id = ?
                    GROUP BY schemas.resource_id",
        )?;
        let mut rows = stmt.query(sql_params![source.id])?;

        while let Some(row) = rows.next()? {
            out.push(Schema {
                resource: Resource {
                    id: row.get(0)?,
                    added: false,
                },
            });
        }
        Ok(out)
    }

    /// Compile the Schema.
    pub fn compile(&self, db: &DB) -> Result<CompiledSchema> {
        let dom: String = db
            .prepare_cached("SELECT document_json FROM schemas WHERE resource_id = ?;")?
            .query_row(&[self.resource.id], |row| row.get(0))?;
        let dom = serde_json::from_str::<serde_json::Value>(&dom)?;
        let compiled: CompiledSchema = build_schema(self.resource.uri(db)?, &dom)?;
        Ok(compiled)
    }
}

#[cfg(test)]
mod test {
    use super::{super::db, *};
    use serde_json::json;
    use tempfile;

    #[test]
    fn test_add_and_fetch() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();

        let fixtures = [
            (
                Url::from_file_path(dir.path().join("a.json")).unwrap(),
                json!({
                    "allOf": [{"$ref": "b.json"}],
                    "oneOf": [true],
                    "else": {"$ref": "#/oneOf/0"}, // Self-reference.
                }),
            ),
            (
                Url::from_file_path(dir.path().join("b.json")).unwrap(),
                json!({
                   "if": {"$ref": "c.json#/$defs/foo"},
                }),
            ),
            (
                Url::from_file_path(dir.path().join("c.json")).unwrap(),
                json!({
                    "$defs": {"foo": true},
                }),
            ),
        ];
        for (uri, val) in fixtures.iter() {
            std::fs::write(uri.to_file_path().unwrap(), val.to_string())?;
        }

        let db = DB::open_in_memory()?;
        db::init(&db)?;

        // Adding a.json also adds b & c.json.
        let sd_a = Schema::register(&db, fixtures[0].0.clone())?;

        // When 'a.json' is fetched, 'b/c.json' are as well.
        let bundle = Schema::_fetch_bundle(&db, sd_a.resource)?;
        assert_eq!(bundle.len(), 3);

        // Expect 'b.json' & 'c.json' were already added by 'a.json'.
        let sd_b = Schema::register(&db, fixtures[1].0.clone())?;
        let sd_c = Schema::register(&db, fixtures[2].0.clone())?;
        assert!(!sd_b.resource.added);
        assert!(!sd_c.resource.added);

        // If 'b.json' is fetched, so is 'c.json' but not 'a.json'.
        let bundle = Schema::_fetch_bundle(&db, sd_b.resource)?;
        assert_eq!(bundle.len(), 2);
        let bundle = Schema::_fetch_bundle(&db, sd_c.resource)?;
        assert_eq!(bundle.len(), 1);

        assert_eq!(
            db::dump_tables(&db, &["resource_imports", "schemas"])?,
            json!({
                "resource_imports": [(2, 3), (1, 2)], // B => C, & A => B.
                "schemas": [
                    (fixtures[0].1.clone(), 1),
                    (fixtures[1].1.clone(), 2),
                    (fixtures[2].1.clone(), 3),
                ],
            }),
        );
        Ok(())
    }
}
