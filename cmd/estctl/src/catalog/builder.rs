use estuary_json::schema;
use rusqlite::{self, params};
use serde_json;
use serde_yaml;
use std::fs;
use std::io;
use thiserror;

use crate::specs;

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

    #[error("schema index error: {0}")]
    IndexErr(#[from] schema::index::Error),

    #[error("invalid file URI: {0}")]
    InvalidFileURI(url::Url),
    #[error("failed to build schema: {0}")]
    SchemaBuildErr(#[from] schema::build::Error),

    #[error("failed to find collection '{name}': '{detail}'")]
    QueryCollectionErr {
        name: String,
        detail: rusqlite::Error,
    },
}
use Error::*;

type Schema = schema::Schema<estuary_json_ext::Annotation>;

pub struct Builder {
    db: rusqlite::Connection,
}

impl Builder {
    pub fn new(db: rusqlite::Connection) -> Builder {
        Builder { db }
    }

    pub fn done(self) -> rusqlite::Connection {
        self.db
    }

    fn intern_resource(&self, mut uri: url::Url) -> rusqlite::Result<(i64, bool)> {
        uri.set_fragment(None);

        // Intern the URI, if it hasn't been already.
        let mut s = self.db.prepare_cached(
            "
            INSERT INTO resources (uri) VALUES (?) ON CONFLICT DO NOTHING;",
        )?;
        let added = s.execute(&[&uri])? == 1;

        // Fetch the assigned resource ID.
        let mut s = self.db.prepare_cached(
            "
            SELECT id FROM resources WHERE uri = ?;",
        )?;
        let id = s.query_row(&[&uri], |row| row.get(0)).unwrap();

        Ok((id, added))
    }

    fn query_resource(&self, id: i64) -> Result<url::Url, Error> {
        let mut s = self.db.prepare_cached(
            "
            SELECT uri FROM resources WHERE id = ?;",
        )?;
        let base: String = s.query_row(&[id], |row| row.get(0)).unwrap();
        Ok(url::Url::parse(&base)?)
    }

    fn query_collection(&self, name: &str, _from_resource_id: i64) -> Result<i64, Error> {
        let id = self
            .db
            .prepare_cached(
                "
            SELECT id FROM collections WHERE name = ?;",
            )?
            .query_row(&[name], |row| row.get(0))
            .map_err(|e| QueryCollectionErr {
                name: name.to_owned(),
                detail: e,
            })?;

        // TODO: verify there's an import path from |from_resource_id|.

        Ok(id)
    }

    fn load_schemas(&self, resource_id: i64) -> Result<Vec<Schema>, Error> {
        let mut out = Vec::new();
        let mut stmt = self.db.prepare_cached(
            "
            SELECT r.uri, sd.document FROM
                resource_transitive_imports AS rti
                JOIN schema_documents AS sd ON rti.import_id = sd.resource_id
                JOIN resources AS r ON rti.import_id = r.id
                WHERE rti.id = ?
                GROUP BY sd.resource_id;",
        )?;
        let mut rows = stmt.query(params![resource_id])?;

        while let Some(row) = rows.next()? {
            let uri: url::Url = row.get(0)?;
            let doc: serde_json::Value = row.get(1)?;
            let doc = schema::build::build_schema(uri, &doc)?;
            out.push(doc);
        }
        Ok(out)
    }

    pub fn do_inference(&self) -> Result<(), Error> {
        let mut stmt = self.db.prepare_cached(
            "\
            SELECT id, name, resource_id, schema_uri FROM collections;",
        )?;
        let mut rows = stmt.query(rusqlite::NO_PARAMS)?;

        while let Some(row) = rows.next()? {
            let collection_id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            let resource_id: i64 = row.get(2)?;
            let schema_uri: url::Url = row.get(3)?;
            let schema_bundle = self.load_schemas(resource_id)?;

            println!(
                "doing inference for {:?} {:?} {:?} {:?} {:?}",
                collection_id,
                name,
                resource_id,
                schema_uri,
                schema_bundle.len()
            );

            // Index the imported bundle of schemas.
            let mut idx = schema::index::Index::new();
            for scm in &schema_bundle {
                idx.add(scm)?;
            }
            idx.verify_references()?;

            // Fetch the specific schema referenced by the collection, and FOOBAR.
            let scm = idx.must_fetch(&schema_uri)?;

            for inf in schema::inference::extract(&scm, &idx, true)? {
                let type_set_str = inf.type_set.as_str(Vec::new());

                self.db
                    .prepare_cached(
                        "
                    INSERT INTO inferences (
                        collection_id,
                        ptr,
                        is_pattern,
                        types,
                        is_base64,
                        is_scalar,
                        content_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?);",
                    )?
                    .execute(params![
                        collection_id,
                        inf.ptr,
                        if inf.is_pattern { Some(true) } else { None },
                        serde_json::to_string(&type_set_str)?,
                        if inf.is_base64 { Some(true) } else { None },
                        if inf.type_set.is_scalar() {
                            Some(true)
                        } else {
                            None
                        },
                        inf.content_type,
                    ])?;
            }
        }
        Ok(())
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

    fn fetch_resource(&self, id: i64) -> Result<String, Error> {
        let uri = self.query_resource(id)?;

        match uri.scheme() {
            "file" => {
                let path = uri
                    .to_file_path()
                    .map_err(|_| InvalidFileURI(uri.clone()))?;
                Ok(fs::read_to_string(path)?)
            }
            _ => panic!("unknown URL scheme '{}'", uri.scheme()),
        }
    }

    fn process_resource_import(&self, id: i64, import_id: i64) -> Result<(), Error> {
        println!("processing resource import {} => {}", id, import_id);

        // Check for a transitive import going the other way. If one is present,
        // this import is invalid as it would introduce an import cycle.
        let mut s = self.db.prepare_cached(
            "
            SELECT r2.uri, r1.uri
            FROM resources AS r1, resources AS r2, resource_transitive_imports AS rti
            WHERE r1.id = rti.id AND r2.id = rti.import_id AND rti.id = ? AND rti.import_id = ?;",
        )?;

        match s.query_row(&[import_id, id], |row| Ok((row.get(0)?, row.get(1)?))) {
            // Success case returns no rows.
            Err(rusqlite::Error::QueryReturnedNoRows) => (),
            // A returned row means an import cycle would be created.
            Ok((a, b)) => return Err(CyclicImport(a, b)),
            // All other SQLite errors.
            Err(e) => return Err(SQLiteErr(e)),
        }

        // Having verified this doesn't create a cycle, now do the insert.
        // Don't fail if this import already exists.
        let mut s = self.db.prepare_cached(
            "
            INSERT INTO resource_imports (id, import_id) VALUES (?, ?) ON CONFLICT DO NOTHING;",
        )?;
        s.execute(&[id, import_id])?;

        Ok(())
    }

    fn process_schema(&self, uri: url::Url) -> Result<i64, Error> {
        let (id, added) = self.intern_resource(uri)?;
        if !added {
            return Ok(id);
        }
        println!("processing schema file {}:{}", id, self.query_resource(id)?);

        let doc_str = self.fetch_resource(id)?;
        let doc = serde_yaml::from_str::<serde_json::Value>(&doc_str)?;

        let compiled: Schema = schema::build::build_schema(self.query_resource(id)?, &doc)?;
        self.walk_schema_references(id, &compiled)?;

        self.db
            .prepare_cached(
                "
            INSERT INTO schema_documents (resource_id, document) values (?, ?);",
            )?
            .execute(params![id, doc.to_string().as_str()])?;
        Ok(id)
    }

    fn walk_schema_references(&self, id: i64, schema: &Schema) -> Result<(), Error> {
        use schema::Application;
        use schema::Keyword;

        Ok(for kw in &schema.kw {
            match kw {
                Keyword::Application(Application::Ref(ref_uri), _) => {
                    let import_id = self.process_schema(ref_uri.clone())?;
                    if id != import_id {
                        self.process_resource_import(id, import_id)?;
                    }
                }
                Keyword::Application(_, child) => self.walk_schema_references(id, &child)?,
                // No-ops.
                Keyword::Anchor(_)
                | Keyword::RecursiveAnchor
                | Keyword::Validation(_)
                | Keyword::Annotation(_) => (),
            }
        })
    }

    fn process_fixtures(&self, cid: i64, uri: url::Url) -> Result<i64, Error> {
        let (rid, _added) = self.intern_resource(uri)?;
        println!(
            "processing fixtures file {}:{}:{}",
            rid,
            cid,
            self.query_resource(rid)?
        );

        let bytes = self.fetch_resource(rid)?;
        let fixtures = serde_yaml::from_str::<Vec<specs::Fixture>>(&bytes)?;

        for fixture in fixtures.into_iter() {
            self.db
                .prepare_cached(
                    "
                INSERT INTO fixtures (
                    collection_id,
                    document,
                    key,
                    projections,
                    resource_id
                ) VALUES (?, ?, ?, ?, ?);
            ",
                )?
                .execute(params![
                    cid,
                    fixture.document,
                    serde_json::Value::Array(fixture.key),
                    serde_json::Value::Object(fixture.projections),
                    rid,
                ])?;
        }
        Ok(rid)
    }

    fn process_transform(
        &self,
        rid: i64,
        target_id: i64,
        transform: &specs::Transform,
    ) -> Result<i64, Error> {
        let source_id = self.query_collection(&transform.source, rid)?;

        let source_schema_uri = match &transform.source_schema {
            Some(uri) => {
                let uri = self.join_resource(rid, uri)?;
                let schema_id = self.process_schema(uri.clone())?;
                self.process_resource_import(rid, schema_id)?;
                Some(uri)
            }
            None => None,
        };

        let deref = |fn_uri: &str| -> Result<(String, i64), Error> {
            let fn_uri = self.join_resource(rid, fn_uri)?;
            let (fn_rid, _) = self.intern_resource(fn_uri)?;
            self.process_resource_import(rid, fn_rid)?;
            let fn_body = self.fetch_resource(fn_rid)?;
            Ok((fn_body, fn_rid))
        };

        use specs::Lambda::*;
        let (typ, body, body_rid, bootstrap, bootstrap_rid) = match &transform.lambda {
            Jq(body_uri) => {
                let (body, body_rid) = deref(body_uri)?;
                ("jq", Some(body), Some(body_rid), None, None)
            }
            JqBlock(body) => ("jq", Some(body.clone()), Some(rid), None, None),
            Sqlite { bootstrap, body } => {
                if let Some(bootstrap) = bootstrap {
                    let (bootstrap, bootstrap_rid) = deref(bootstrap)?;
                    let (body, body_rid) = deref(body)?;
                    (
                        "sqlite",
                        Some(body),
                        Some(body_rid),
                        Some(bootstrap),
                        Some(bootstrap_rid),
                    )
                } else {
                    let (body, body_rid) = deref(body)?;
                    ("sqlite", Some(body), Some(body_rid), None, None)
                }
            }
            SqliteBlock { bootstrap, body } => {
                if let Some(bootstrap) = bootstrap {
                    (
                        "sqlite",
                        Some(body.clone()),
                        Some(rid),
                        Some(bootstrap.clone()),
                        Some(rid),
                    )
                } else {
                    ("sqlite", Some(body.clone()), Some(rid), None, None)
                }
            }
        };

        self.db
            .prepare_cached(
                "
            INSERT INTO lambdas (
                type,
                body,
                body_resource_id,
                bootstrap,
                bootstrap_resource_id
            ) VALUES (?, ?, ?, ?, ?);
        ",
            )?
            .execute(params![typ, body, body_rid, bootstrap, bootstrap_rid,])?;

        let lambda_id = self
            .db
            .prepare_cached("SELECT last_insert_rowid();")?
            .query_row(rusqlite::NO_PARAMS, |row| row.get(0))?;

        let (shuf_key, shuf_broadcast, shuf_choose) = match &transform.shuffle {
            Some(shuffle) => (
                shuffle
                    .key
                    .as_ref()
                    .map(|k| serde_json::to_string(&k).unwrap()),
                shuffle.broadcast,
                shuffle.choose,
            ),
            None => (None, None, None),
        };

        self.db
            .prepare_cached(
                "
            INSERT INTO transforms (
                source_id,
                source_schema_uri,
                shuffle_key,
                shuffle_broadcast,
                shuffle_choose,
                target_id,
                lambda_id,
                resource_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?
            .execute(params![
                source_id,
                source_schema_uri,
                shuf_key,
                shuf_broadcast,
                shuf_choose,
                target_id,
                lambda_id,
                rid,
            ])?;

        Ok(lambda_id)
    }

    pub fn process_specs(&self, uri: url::Url) -> Result<i64, Error> {
        let (rid, added) = self.intern_resource(uri)?;
        if !added {
            return Ok(rid);
        }
        println!("processing spec file {}:{}", rid, self.query_resource(rid)?);

        let spec_str = self.fetch_resource(rid)?;
        let spec = serde_yaml::from_str::<specs::Node>(&spec_str)?;

        for import in &spec.import {
            let import = self.join_resource(rid, import)?;
            let import_id = self.process_specs(import)?;
            self.process_resource_import(rid, import_id)?;
        }

        for c in &spec.collections {
            let schema_uri = self.join_resource(rid, &c.schema)?;
            let schema_id = self.process_schema(schema_uri.clone())?;
            self.process_resource_import(rid, schema_id)?;

            self.db
                .prepare_cached(
                    "
                INSERT INTO collections (
                    resource_id,
                    name,
                    schema_uri,
                    key
                ) VALUES (?, ?, ?, ?);
            ",
                )?
                .execute(params![
                    rid,
                    &c.name,
                    schema_uri,
                    serde_json::to_string(&c.key)?,
                ])?;
            let cid = self.query_collection(&c.name, rid)?;

            for proj in &c.projections {
                self.db
                    .prepare_cached(
                        "
                    INSERT INTO projections (
                        collection_id,
                        name,
                        ptr,
                        partition
                    ) VALUES (?, ?, ?, ?);
                ",
                    )?
                    .execute(params![cid, &proj.name, &proj.ptr, proj.partition,])?;
            }

            // Process fixtures of the collection.
            for fixture in &c.fixtures {
                let fixture = self.join_resource(rid, fixture)?;
                let fixture_id = self.process_fixtures(cid, fixture)?;
                self.process_resource_import(rid, fixture_id)?;
            }

            // Process the collection's derivation.
            if let Some(derivation) = &c.derivation {
                let fixed_shards = match derivation.inner_state {
                    specs::InnerState::Ephemeral => None,
                    specs::InnerState::Durable { parallelism } => Some(parallelism),
                };
                self.db
                    .prepare_cached(
                        "
                    INSERT INTO derivations (
                        collection_id,
                        fixed_shards,
                        resource_id
                    ) VALUES (?, ?, ?);
                ",
                    )?
                    .execute(params![cid, fixed_shards, rid])?;

                for t in &derivation.transform {
                    self.process_transform(rid, cid, t)?;
                }
            }
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
        crate::catalog::create_schema(&db)?;
        let b = Builder::new(db);

        assert_eq!(b.intern_resource(url::Url::parse("file:///1")?)?, (1, true));
        assert_eq!(
            b.intern_resource(url::Url::parse("https:///2?")?)?,
            (2, true)
        );
        assert_eq!(
            b.intern_resource(url::Url::parse("https:///2?")?)?,
            (2, false)
        );
        assert_eq!(
            b.intern_resource(url::Url::parse("file:///1#ignored")?)?,
            (1, false)
        );

        Ok(())
    }

    #[test]
    fn test_resource_include() -> Result<(), Error> {
        let db = rusqlite::Connection::open_in_memory()?;
        crate::catalog::create_schema(&db)?;
        let b = Builder::new(db);

        assert_eq!(b.intern_resource(url::Url::parse("file:///a")?)?, (1, true));
        assert_eq!(b.intern_resource(url::Url::parse("https://b")?)?, (2, true));
        assert_eq!(b.intern_resource(url::Url::parse("file:///c")?)?, (3, true));

        b.process_resource_import(2, 3)?;
        b.process_resource_import(1, 2)?;

        let s = format!("{}", b.process_resource_import(3, 1).unwrap_err());
        assert_eq!(
            s,
            "resource 'file:///c' imports 'file:///a', \
            but 'file:///a' also transitively imports 'file:///c'"
        );

        Ok(())
    }
}
