use std::io;

use std::fs;
use serde_json;
use serde_yaml;
use thiserror;
use rusqlite::{self, params};

use Error::*;

use crate::catalog;
use crate::specs;
use crate::schema;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("joining '{relative}' with base URL '{base}': {detail}")]
    URLJoinErr {
        base: url::Url,
        relative: String,
        detail: url::ParseError,
    },
    #[error("parsing URL: {0}")]
    URLParseErr(#[from] url::ParseError),
    #[error("failed to parse YAML: {0}")]
    YAMLErr(#[from] serde_yaml::Error),
    #[error("failed to parse JSON: {0}")]
    JSONErr(#[from] serde_json::Error),
    #[error("catalog database error: {0}")]
    SQLiteErr(#[from] rusqlite::Error),
    #[error("resource '{0}' imports '{1}', but '{1}' also transitively imports '{0}'")]
    CyclicImport(String, String),

    #[error("invalid file URI: {0}")]
    InvalidFileURI(url::Url),
    #[error("failed to build schema: {0}")]
    SchemaBuildErr(#[from] schema::build::Error)

    /*
    //#[error("failed to compile JSON-Schema: {0}")]
    //BuildErr(#[from] schema::build::Error),
    #[error("schema index error: {0}")]
    IndexErr(#[from] schema::index::Error),
    #[error("converting from SQL: {0}")]
    SQLConversionErr(#[from] rusqlite::types::FromSqlError),
    #[error("{0}")]
    CanonicalError(#[from] specs::canonical::Error),
    */
}

type Schema = schema::Schema<specs::Annotation>;

pub struct Builder { db: rusqlite::Connection }

impl Builder {
    pub fn new(db: rusqlite::Connection) -> Builder { Builder { db } }

    pub fn done(self) -> rusqlite::Connection { self.db }

    fn intern_resource(&self, mut uri: url::Url) -> rusqlite::Result<(i64, bool)> {
        uri.set_fragment(None);

        // Intern the URI, if it hasn't been already.
        let mut s = self.db.prepare_cached("
            INSERT INTO resources (uri) VALUES (?) ON CONFLICT DO NOTHING;")?;
        let added = s.execute(&[&uri])? == 1;

        // Fetch the assigned resource ID.
        let mut s = self.db.prepare_cached("
            SELECT id FROM resources WHERE uri = ?;")?;
        let id = s.query_row(&[&uri], |row| row.get(0))?;

        Ok((id, added))
    }

    fn query_resource(&self, id: i64) -> Result<url::Url, Error> {
        let mut s = self.db.prepare_cached("
            SELECT uri FROM resources WHERE id = ?;")?;
        let base: String = s.query_row(&[id], |row| row.get(0))?;
        Ok(url::Url::parse(&base)?)
    }

    fn query_collection(&self, name: &str) -> Result<i64, Error> {
        let mut s = self.db.prepare_cached("
            SELECT id FROM collections WHERE name = ?;")?;
        Ok(s.query_row(&[name], |row| row.get(0))?)
    }

    fn join_resource(&self, base_id: i64, relative: &str) -> Result<url::Url, Error> {
        let base = self.query_resource(base_id)?;
        match base.join(relative) {
            Ok(url) => Ok(url),
            Err(detail) => Err(URLJoinErr {
                relative: relative.to_owned(),
                base,
                detail,
            }),
        }
    }

    fn fetch_resource<T>(&self, id: i64) -> Result<T, Error>
    where T: serde::de::DeserializeOwned,
    {
        let uri = self.query_resource(id)?;

        match uri.scheme() {
            "file" => {
                let path = uri.to_file_path().map_err(|_| InvalidFileURI(uri.clone()))?;

                Ok(serde_yaml::from_reader(fs::File::open(path)?)?)
            }
            _ => panic!("unknown URL scheme '{}'", uri.scheme()),
        }
    }

    fn process_resource_import(&self, id: i64, import_id: i64) -> Result<(), Error> {
        println!("processing resource import {} => {}", id, import_id);

        // Check for a transitive import going the other way. If one is present,
        // this import is invalid as it would introduce an import cycle.
        let mut s = self.db.prepare_cached("
            SELECT r2.uri, r1.uri
            FROM resources AS r1, resources AS r2, resource_transitive_imports AS rti
            WHERE r1.id = rti.id AND r2.id = rti.import_id AND rti.id = ? AND rti.import_id = ?;")?;

        match s.query_row(&[import_id, id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        }) {
            // Success case returns no rows.
            Err(rusqlite::Error::QueryReturnedNoRows) => (),
            // A returned row means an import cycle would be created.
            Ok((a, b)) => return Err(CyclicImport(a, b)),
            // All other SQLite errors.
            Err(e) => return Err(SQLiteErr(e)),
        }

        // Having verified this doesn't create a cycle, now do the insert.
        // Don't fail if this import already exists.
        let mut s = self.db.prepare_cached("
            INSERT INTO resource_imports (id, import_id) VALUES (?, ?) ON CONFLICT DO NOTHING;")?;
        s.execute(&[id, import_id])?;

        Ok(())
    }

    fn process_schema(&self, uri: url::Url) -> Result<i64, Error> {
        let (id, added) = self.intern_resource(uri)?;
        if !added {
            return Ok(id)
        }
        println!("processing schema file {}:{}", id, self.query_resource(id)?);

        let doc = self.fetch_resource::<serde_json::Value>(id)?;

        let compiled: Schema = schema::build::build_schema(self.query_resource(id)?, &doc)?;
        self.walk_schema_references(id, &compiled)?;

        let mut s = self.db.prepare_cached(
            "INSERT INTO schema_documents (resource_id, document) values (?, ?);\
        ")?.execute(params![id, doc.to_string().as_str()])?;
        Ok(id)
    }

    fn walk_schema_references(&self, id: i64, schema: &Schema) -> Result<(), Error> {
        use schema::Keyword;
        use schema::Application;

        Ok(for kw in &schema.kw {
            match kw {
                Keyword::Application(Application::Ref(ref_uri), _)  => {
                    let import_id = self.process_schema(ref_uri.clone())?;
                    if id != import_id {
                        self.process_resource_import(id, import_id)?;
                    }
                },
                Keyword::Application(_, child) => self.walk_schema_references(id, &child)?,
                // No-ops.
                Keyword::Anchor(_) | Keyword::RecursiveAnchor | Keyword::Validation(_) | Keyword::Annotation(_) => (),
            }
        })
    }

    fn process_fixtures(&self, cid: i64, uri: url::Url) -> Result<i64, Error> {
        let (rid, _added) = self.intern_resource(uri)?;
        println!("processing fixtures file {}:{}:{}", rid, cid, self.query_resource(rid)?);

        let fixtures = self.fetch_resource::<Vec<specs::Fixture>>(rid)?;
        for fixture in fixtures.into_iter() {
            self.db.prepare_cached("
                INSERT INTO fixtures (
                    collection_id,
                    document,
                    key,
                    projections,
                    resource_id
                ) VALUES (?, ?, ?, ?, ?);
            ")?.execute(params![
                cid,
                fixture.document,
                serde_json::Value::Array(fixture.key),
                serde_json::Value::Object(fixture.projections),
                rid,
            ])?;
        }
        Ok(rid)
    }

    pub fn process_specs(&self, uri: url::Url) -> Result<i64, Error> {
        let (rid, added) = self.intern_resource(uri)?;
        if !added {
            return Ok(rid)
        }
        println!("processing spec file {}:{}", rid, self.query_resource(rid)?);

        let spec = self.fetch_resource::<specs::Node>(rid)?;

        for import in &spec.import {
            let import = self.join_resource(rid, import)?;
            let import_id = self.process_specs(import)?;
            self.process_resource_import(rid, import_id)?;
        }

        for c in &spec.collections {
            let schema_uri = self.join_resource(rid, &c.schema)?;
            let schema_id = self.process_schema(schema_uri.clone())?;
            self.process_resource_import(rid, schema_id)?;

            self.db.prepare_cached("
                INSERT INTO collections (resource_id, name, schema_uri, key) VALUES (?, ?, ?, ?);
            ")?.execute(params![rid, &c.name, schema_uri, serde_json::to_string(&c.key)?])?;
            let cid = self.query_collection(&c.name)?;

            for proj in &c.projections {
                self.db.prepare_cached("
                    INSERT INTO projections (
                        collection_id,
                        name,
                        ptr,
                        partition
                    ) VALUES (?, ?, ?, ?);
                ")?.execute(params![cid, &proj.name, &proj.ptr, proj.partition])?;
            }

            for fixture in &c.fixtures {
                let fixture = self.join_resource(rid, fixture)?;
                let fixture_id = self.process_fixtures(cid, fixture)?;
                self.process_resource_import(rid, fixture_id)?;
            }

            /*
            let mut s = self.db.prepare_cached(
                "INSERT INTO collection (url, schema_url, key, partitions)\
                        VALUES (?, ?, ?, ?);")?;
            s.execute(&[
                name.as_str(),
                schema.as_str(),
                serde_json::to_string(&c.key)?.as_str(),
                serde_json::to_string(&c.partitions)?.as_str(),
            ])?;
            */
        }
        Ok(rid)
    }

}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_resource_interning() -> Result<(), Error> {
        let db = rusqlite::Connection::open_in_memory()?;
        catalog::create_schema(&db)?;
        let b = Builder::new(db);

        assert_eq!(b.intern_resource(url::Url::parse("file:///1")?)?, (1, true));
        assert_eq!(b.intern_resource(url::Url::parse("https:///2?")?)?, (2, true));
        assert_eq!(b.intern_resource(url::Url::parse("https:///2?")?)?, (2, false));
        assert_eq!(b.intern_resource(url::Url::parse("file:///1#ignored")?)?, (1, false));

        Ok(())
    }

    #[test]
    fn test_resource_include() -> Result<(), Error> {
        let db = rusqlite::Connection::open_in_memory()?;
        catalog::create_schema(&db)?;
        let b = Builder::new(db);

        assert_eq!(b.intern_resource(url::Url::parse("file:///a")?)?, (1, true));
        assert_eq!(b.intern_resource(url::Url::parse("https://b")?)?, (2, true));
        assert_eq!(b.intern_resource(url::Url::parse("file:///c")?)?, (3, true));

        b.process_resource_import(2, 3)?;
        b.process_resource_import(1, 2)?;

        let s = format!("{}", b.process_resource_import(3, 1).unwrap_err());
        assert_eq!(s, "resource 'file:///c' imports 'file:///a', \
            but 'file:///a' also transitively imports 'file:///c'");

        Ok(())
    }
}

