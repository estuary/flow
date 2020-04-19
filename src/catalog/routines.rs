use rusqlite::{Connection as DB, Error as DBError};
use url::Url;

use super::{error::Error, regexp_sql_fn};
type Result<T> = std::result::Result<T, Error>;

/// Create the catalog SQL schema in the connected database.
pub fn create_schema(db: &DB) -> Result<()> {
    regexp_sql_fn::install(db)?; // Install support for REGEXP operator.
    db.execute_batch(include_str!("schema.sql"))?;
    Ok(())
}

/// Resource represents a catalog resource.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
struct Resource {
    /// Assigned ID of this resource.
    pub id: i64,
    /// If interned, whether the resource was just added.
    pub added: bool,
}

impl Resource {
    /// Intern a resource URI -- exclusive of its fragment -- to a Resource.
    pub fn intern(db: &DB, mut uri: Url) -> Result<Resource> {
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

    /// Adds an import of Resource |import| by |source|.
    fn add_import(db: &DB, source: Resource, import: Resource) -> Result<()> {
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

fn add_schema_document(db: &DB, r: Resource) -> Result<()> {
    let doc = r.fetch_to_string(db)?;
    let doc = serde_yaml::from_str::<serde_json::Value>(&doc)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
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

        let r = Resource::intern(&db, u1.clone())?;
        assert_eq!(r, Resource { id: 1, added: true });
        assert_eq!(r.uri(&db)?, u1); // Expect it fetches back to it's URL.

        let r = Resource::intern(&db, u2.clone())?;
        assert_eq!(r, Resource { id: 2, added: true });
        assert_eq!(r.uri(&db)?, u2);

        let r = Resource::intern(&db, u3.clone())?;
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
        let r = Resource::intern(&db, Url::parse("file:///a/dir/base.path")?)?;

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

        let r = Resource::intern(&db, uri)?;
        assert_eq!(r.fetch_to_string(&db)?, "hello!");
        Ok(())
    }

    #[test]
    fn test_resource_import_tracking() -> Result<()> {
        let db = DB::open_in_memory()?;
        create_schema(&db)?;

        let a = Resource::intern(&db, Url::parse("file:///a")?)?;
        let b = Resource::intern(&db, Url::parse("https://b")?)?;
        let c = Resource::intern(&db, Url::parse("file:///c")?)?;
        let d = Resource::intern(&db, Url::parse("http://d")?)?;

        assert_eq!(
            "'file:///a' references 'http://d/' without directly or indirectly importing it",
            format!("{}", Resource::verify_import(&db, a, d).unwrap_err())
        );

        // A => B => D.
        Resource::add_import(&db, a, b)?;
        Resource::add_import(&db, b, d)?;
        Resource::verify_import(&db, a, d)?;

        // It's not okay for D => A (since A => B => D).
        assert_eq!(
            "'http://d/' imports 'file:///a', but 'file:///a' already transitively imports 'http://d/'",
            format!("{}", Resource::add_import(&db, d, a).unwrap_err()));
        // Or for D => B (since B => D).
        Resource::add_import(&db, d, b).unwrap_err();

        // Imports may form a DAG. Create some valid alternate paths.
        // A => C => D.
        Resource::add_import(&db, c, d)?;
        Resource::add_import(&db, a, c)?;
        // A => B => C => D.
        Resource::add_import(&db, b, c)?;
        // A => D.
        Resource::add_import(&db, a, d)?;

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
}
