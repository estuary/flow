use crate::{doc::Schema, specs::build as specs};
use estuary_json::schema::{build::build_schema, Application, Keyword};
use log::info;
use rusqlite::{params as sql_params, Connection as DB, Error as DBError, Result as DBResult};
use url::Url;

use super::{error::Error, regexp_sql_fn};
type Result<T> = std::result::Result<T, Error>;

/// Create the catalog SQL schema in the connected database.
pub fn create_schema(db: &DB) -> Result<()> {
    regexp_sql_fn::install(db)?; // Install support for REGEXP operator.
    db.execute_batch(include_str!("schema.sql"))?;
    Ok(())
}

/// Resource within the catalog: a file of some kind
/// that's addressable via an associated and canonical URI.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
struct Resource {
    /// Assigned ID of this resource.
    pub id: i64,
    /// If interned, whether the resource was just added.
    pub added: bool,
}

impl Resource {
    /// Register a resource URI to the catalog, if not already known.
    /// Any fragment component of the URI is discarded before indexing.
    pub fn register(db: &DB, mut uri: Url) -> Result<Resource> {
        uri.set_fragment(None);

        // Intern the URI, if it hasn't been already.
        let mut s =
            db.prepare_cached("INSERT INTO resources (uri) VALUES (?) ON CONFLICT DO NOTHING;")?;
        let added = s.execute(&[&uri])? == 1;

        // Fetch the assigned resource ID.
        let mut s = db.prepare_cached("SELECT id FROM resources WHERE uri = ?;")?;
        let id = s.query_row(&[&uri], |row| row.get(0))?;

        Ok(Resource { id, added })
    }

    /// Fetch the URI of this resource.
    fn uri(&self, db: &DB) -> Result<Url> {
        let mut s = db.prepare_cached("SELECT uri FROM resources WHERE id = ?;")?;
        let base: String = s.query_row(&[self.id], |row| row.get(0))?;
        Ok(url::Url::parse(&base)?)
    }

    /// Join a relative URI using this Resource as its base URI.
    fn join(&self, db: &DB, relative_uri: &str) -> Result<Url> {
        let base = self.uri(db)?;
        match base.join(relative_uri) {
            Ok(url) => Ok(url),
            Err(detail) => Err(Error::URLJoinErr {
                relative: relative_uri.to_owned(),
                base,
                detail,
            }),
        }
    }

    /// Attempt to fetch the contents of this resource into a string.
    /// This is only expected to work when building the catalog.
    /// After that, resources may not be reachable and the catalog is
    /// expected to be fully self-contained.
    fn fetch_to_string(&self, db: &DB) -> Result<String> {
        let uri = self.uri(db)?;
        match uri.scheme() {
            "file" => {
                let path = uri
                    .to_file_path()
                    .map_err(|_| Error::FetchErr(uri.clone()))?;
                Ok(std::fs::read_to_string(path)?)
            }
            _ => Err(Error::FetchErr(uri)),
        }
    }

    /// Registers an import of Resource |import| by |source|.
    fn register_import(db: &DB, source: Resource, import: Resource) -> Result<()> {
        if source.id == import.id {
            return Ok(()); // A source implicitly imports itself.
        }
        // Check for a transitive import going the other way. If one is present,
        // this import is invalid as it would introduce an import cycle.
        let mut s = db.prepare_cached(
            "SELECT 1 FROM resource_transitive_imports
            WHERE import_id = ? AND source_id = ?;",
        )?;

        match s.query_row(&[source.id, import.id], |_| Ok(())) {
            // Success case returns no rows.
            Err(DBError::QueryReturnedNoRows) => (),
            // A returned row means an import cycle would be created.
            Ok(()) => {
                return Err(Error::CyclicImport {
                    source_uri: source.uri(db)?.into_string(),
                    import_uri: import.uri(db)?.into_string(),
                })
            }
            // All other SQLite errors.
            Err(e) => return Err(Error::SQLiteErr(e)),
        }

        // Having verified this doesn't create a cycle, now do the insert.
        // Don't fail if this import already exists.
        let mut s = db.prepare_cached(
            "INSERT INTO resource_imports (source_id, import_id)
                    VALUES (?, ?) ON CONFLICT DO NOTHING;",
        )?;
        s.execute(&[source.id, import.id])?;

        Ok(())
    }

    /// Verify that a transitive import path from |source| to |import| exists.
    fn verify_import(db: &DB, source: Resource, import: Resource) -> Result<()> {
        let mut s = db.prepare_cached(
            "SELECT 1 FROM resource_transitive_imports
            WHERE source_id = ? AND import_id = ?;",
        )?;

        match s.query_row(&[source.id, import.id], |_| Ok(())) {
            Ok(()) => Ok(()),
            Err(DBError::QueryReturnedNoRows) => Err(Error::MissingImport {
                source_uri: source.uri(db)?.into_string(),
                import_uri: import.uri(db)?.into_string(),
            }),
            Err(e) => Err(Error::SQLiteErr(e)),
        }
    }
}

/// SchemaDocument represents a catalog JSON-Schema document.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
struct SchemaDoc(Resource);

impl SchemaDoc {
    /// Register a JSON-Schema document at the URI. If already registered, this
    /// is a no-op and its existing handle is returned. Otherwise the document
    /// and all of its recursive references are added to the catalog.
    fn register(db: &DB, uri: url::Url) -> Result<SchemaDoc> {
        let sd = SchemaDoc(Resource::register(db, uri)?);
        if !sd.0.added {
            return Ok(sd);
        }

        let dom = sd.0.fetch_to_string(db)?;
        let dom = serde_yaml::from_str::<serde_json::Value>(&dom)?;
        db.prepare_cached(
            "INSERT INTO schema_documents (resource_id, document_json) VALUES(?, ?);",
        )?
        .execute(sql_params![sd.0.id, dom.to_string().as_str()])?;

        // Recursively traverse static references to catalog other schemas on
        // which this schema document depends.
        Self::add_references(&db, sd, &sd.compile(db)?)?;

        info!("added schema document {}@{:?}", sd.0.id, sd.0.uri(db)?);
        Ok(sd)
    }

    // Walks recursive references of the compiled |schema| document .
    fn add_references(db: &DB, source: Self, schema: &Schema) -> Result<()> {
        for kw in &schema.kw {
            match kw {
                Keyword::Application(Application::Ref(ref_uri), _) => {
                    let import = Self::register(db, ref_uri.clone())?;
                    Resource::register_import(&db, source.0, import.0)?;
                }
                Keyword::Application(_, child) => {
                    Self::add_references(&db, source, child)?;
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

    /// Resource of this SchemaDocument.
    fn resource(&self) -> Resource {
        self.0
    }

    /// Fetch the bundle of SchemaDocuments which are directly or indirectly
    /// imported and referenced by the named Resource.
    fn fetch_bundle(db: &DB, source: Resource) -> Result<Vec<SchemaDoc>> {
        let mut out = Vec::new();
        let mut stmt = db.prepare_cached(
            "SELECT sd.resource_id FROM
                    resource_transitive_imports AS rti
                    JOIN schema_documents AS sd
                    ON rti.import_id = sd.resource_id AND rti.source_id = ?
                    GROUP BY sd.resource_id",
        )?;
        let mut rows = stmt.query(sql_params![source.id])?;

        while let Some(row) = rows.next()? {
            out.push(SchemaDoc(Resource {
                id: row.get(0)?,
                added: false,
            }));
        }
        Ok(out)
    }

    /// Compile a Schema at the SchemaDocument root.
    fn compile(&self, db: &DB) -> Result<Schema> {
        let dom: String = db
            .prepare_cached("SELECT document_json FROM schema_documents WHERE resource_id = ?;")?
            .query_row(&[self.0.id], |row| row.get(0))?;
        let dom = serde_json::from_str::<serde_json::Value>(&dom)?;
        let compiled: Schema = build_schema(self.0.uri(db)?, &dom)?;
        Ok(compiled)
    }
}

/// Lambda represents a catalog Lambda function.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
struct Lambda(i64);

static TYPESCRIPT: &str = "typescript";
static SQLITE: &str = "sqlite";
static REMOTE: &str = "remote";

impl Lambda {
    fn register(db: &DB, source: Resource, spec: &specs::Lambda) -> Result<Lambda> {
        use specs::Lambda::*;
        let (embed, runtime, body) = match spec {
            Remote(endpoint) => {
                Url::parse(endpoint)?; // Must be a base URI.
                (true, REMOTE, endpoint)
            }
            Sqlite(body) => (true, SQLITE, body),
            SqliteFile(uri) => (false, SQLITE, uri),
            Typescript(body) => (true, TYPESCRIPT, body),
            TypescriptFile(uri) => (false, TYPESCRIPT, uri),
        };

        if embed {
            // Embedded lambdas always insert a new row.
            db.prepare_cached("INSERT INTO lambdas (runtime, body, resource_id) VALUES (?, ?, ?)")?
                .execute(sql_params![runtime, body, source.id])?;
            return Ok(Lambda(db.last_insert_rowid()));
        }

        // This lambda specification is an indirect to a file.

        let import = source.join(db, body)?;
        let import = Resource::register(db, import)?;
        Resource::register_import(db, source, import)?;

        // Attempt to find an existing row for this lambda.
        // We don't use import.added because *technically* the lambda could be
        // represented twice with different runtimes. This is almost certainly
        // a bug, but here we just represent what the spec says and trust we'll
        // fail later with a better error (i.e., compilation failed).

        let row = db
            .prepare_cached("SELECT id FROM lambdas WHERE runtime = ? AND resource_id = ?")?
            .query_row(sql_params![runtime, import.id], |row| row.get(0));

        match row {
            Ok(id) => return Ok(Lambda(id)),         // Found.
            Err(DBError::QueryReturnedNoRows) => (), // Not found. Fall through to insert.
            Err(err) => return Err(err.into()),      // Other DBError.
        }

        db.prepare_cached("INSERT INTO lambdas (runtime, body, resource_id) VALUES (?, ?, ?)")?
            .execute(sql_params![runtime, import.fetch_to_string(db)?, import.id])?;
        Ok(Lambda(db.last_insert_rowid()))
    }
}

/// Collection represents a catalog Collection.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
struct Collection(i64);

impl Collection {
    /*
    fn register(db: &DB, source: Resource, spec: &specs::Collection) -> Result<Collection> {
    }
    */
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use std::io::Write;
    use tempfile;

    #[test]
    fn test_create_schema() -> Result<()> {
        let db = DB::open_in_memory()?;
        create_schema(&db)
    }

    #[test]
    fn test_resource_interning() -> Result<()> {
        let db = DB::open_in_memory()?;
        create_schema(&db)?;

        let u1 = Url::parse("file:///1")?;
        let u2 = Url::parse("https:///2?")?;
        let u3 = Url::parse("file:///1#ignored")?;

        let r = Resource::register(&db, u1.clone())?;
        assert_eq!(r, Resource { id: 1, added: true });
        assert_eq!(r.uri(&db)?, u1); // Expect it fetches back to it's URL.

        let r = Resource::register(&db, u2.clone())?;
        assert_eq!(r, Resource { id: 2, added: true });
        assert_eq!(r.uri(&db)?, u2);

        let r = Resource::register(&db, u3.clone())?;
        assert_eq!(
            r,
            Resource {
                id: 1,
                added: false
            }
        );
        assert_eq!(r.uri(&db)?, u1);

        Ok(())
    }

    #[test]
    fn test_resource_joining() -> Result<()> {
        let db = DB::open_in_memory()?;
        create_schema(&db)?;
        let r = Resource::register(&db, Url::parse("file:///a/dir/base.path")?)?;

        // Case: join with a relative file.
        assert_eq!(
            r.join(&db, "other/file.path")?,
            Url::parse("file:///a/dir/other/file.path")?
        );
        // Case: join with a relative file in parent directory.
        assert_eq!(
            r.join(&db, "../sibling/file.path")?,
            Url::parse("file:///a/sibling/file.path")?
        );
        // Case: join with a URI which is it's own base.
        assert_eq!(
            r.join(&db, "http://example/file.path")?,
            Url::parse("http://example/file.path")?
        );
        Ok(())
    }

    #[test]
    fn test_resource_fetch_file() -> Result<()> {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.as_file_mut().write("hello!".as_bytes()).unwrap();

        let uri = Url::from_file_path(file.path()).unwrap();

        let db = DB::open_in_memory()?;
        create_schema(&db)?;

        let r = Resource::register(&db, uri)?;
        assert_eq!(r.fetch_to_string(&db)?, "hello!");
        Ok(())
    }

    #[test]
    fn test_resource_import_tracking() -> Result<()> {
        let db = DB::open_in_memory()?;
        create_schema(&db)?;

        let a = Resource::register(&db, Url::parse("file:///a")?)?;
        let b = Resource::register(&db, Url::parse("https://b")?)?;
        let c = Resource::register(&db, Url::parse("file:///c")?)?;
        let d = Resource::register(&db, Url::parse("http://d")?)?;

        // A resource may implicitly reference itself (only).
        Resource::verify_import(&db, a, a)?;
        assert_eq!(
            "'file:///a' references 'http://d/' without directly or indirectly importing it",
            format!("{}", Resource::verify_import(&db, a, d).unwrap_err())
        );

        // Marking a self-import is a no-op (and doesn't break CTE evaluation).
        Resource::register_import(&db, a, a)?;

        // A => B => D.
        Resource::register_import(&db, a, b)?;
        Resource::register_import(&db, b, d)?;
        Resource::verify_import(&db, a, d)?;

        // It's not okay for D => A (since A => B => D).
        assert_eq!(
            "'http://d/' imports 'file:///a', but 'file:///a' already transitively imports 'http://d/'",
            format!("{}", Resource::register_import(&db, d, a).unwrap_err()));
        // Or for D => B (since B => D).
        Resource::register_import(&db, d, b).unwrap_err();

        // Imports may form a DAG. Create some valid alternate paths.
        // A => C => D.
        Resource::register_import(&db, c, d)?;
        Resource::register_import(&db, a, c)?;
        // A => B => C => D.
        Resource::register_import(&db, b, c)?;
        // A => D.
        Resource::register_import(&db, a, d)?;

        Resource::verify_import(&db, a, b)?;
        Resource::verify_import(&db, a, c)?;
        Resource::verify_import(&db, a, d)?;
        Resource::verify_import(&db, b, d)?;
        Resource::verify_import(&db, b, c)?;
        Resource::verify_import(&db, c, d)?;

        // C still does not import B, however.
        assert_eq!(
            "'file:///c' references 'https://b/' without directly or indirectly importing it",
            format!("{}", Resource::verify_import(&db, c, b).unwrap_err())
        );
        Ok(())
    }

    #[test]
    fn test_add_and_fetch_schemas() -> Result<()> {
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
        create_schema(&db)?;

        // Adding a.json also adds b & c.json.
        let sd_a = SchemaDoc::register(&db, fixtures[0].0.clone())?;

        // When 'a.json' is fetched, 'b/c.json' are as well.
        let bundle = SchemaDoc::fetch_bundle(&db, sd_a.resource())?;
        assert_eq!(bundle.len(), 3);

        // Expect 'b.json' & 'c.json' were already added by 'a.json'.
        let sd_b = SchemaDoc::register(&db, fixtures[1].0.clone())?;
        let sd_c = SchemaDoc::register(&db, fixtures[2].0.clone())?;
        assert!(!sd_b.resource().added);
        assert!(!sd_c.resource().added);

        // If 'b.json' is fetched, so is 'c.json' but not 'a.json'.
        let bundle = SchemaDoc::fetch_bundle(&db, sd_b.resource())?;
        assert_eq!(bundle.len(), 2);
        let bundle = SchemaDoc::fetch_bundle(&db, sd_c.resource())?;
        assert_eq!(bundle.len(), 1);

        Ok(())
    }

    #[test]
    fn test_add_and_fetch_lambdas() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();

        // Two lambda fixture files.
        std::fs::write(dir.path().join("lambda.one"), "file one")?;
        std::fs::write(dir.path().join("lambda.two"), "file two")?;

        let db = DB::open_in_memory()?;
        create_schema(&db)?;

        let root = Url::from_file_path(dir.path().join("root.spec")).unwrap();
        let root = Resource::register(&db, root)?;
        let file_1 = Resource::register(&db, root.join(&db, "lambda.one")?)?;
        let file_2 = Resource::register(&db, root.join(&db, "lambda.two")?)?;

        use specs::Lambda::*;
        let fixtures = [
            Sqlite("block 1".to_owned()),
            Sqlite("block 2".to_owned()),
            Typescript("block 3".to_owned()),
            Remote("http://host".to_owned()),
            SqliteFile("lambda.one".to_owned()),
            SqliteFile("lambda.one".to_owned()), // De-duplicated repeat.
            TypescriptFile("lambda.two".to_owned()),
            TypescriptFile("lambda.one".to_owned()), // Repeat with different runtime.
        ];

        for fixture in fixtures.iter() {
            Lambda::register(&db, root, fixture)?;
        }

        let mut s = db.prepare("SELECT id, runtime, body, resource_id FROM lambdas")?;
        let rows = s.query_map(sql_params![], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
        let rows: DBResult<Vec<(i64, String, String, i64)>> = rows.collect();

        assert_eq!(
            rows?,
            vec![
                (1, "sqlite".to_owned(), "block 1".to_owned(), root.id),
                (2, "sqlite".to_owned(), "block 2".to_owned(), root.id),
                (3, "typescript".to_owned(), "block 3".to_owned(), root.id),
                (4, "remote".to_owned(), "http://host".to_owned(), root.id),
                (5, "sqlite".to_owned(), "file one".to_owned(), file_1.id),
                (6, "typescript".to_owned(), "file two".to_owned(), file_2.id),
                (7, "typescript".to_owned(), "file one".to_owned(), file_1.id),
            ],
        );

        Ok(())
    }
}
