use rusqlite;

mod builder;
mod regexp_sql_fn;

pub use builder::Builder;

pub fn create_schema(db: &rusqlite::Connection) -> rusqlite::Result<()> {
    regexp_sql_fn::create(db)?; // Install support for REGEXP operator.
    db.execute_batch(include_str!("schema.sql"))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create_schema() -> rusqlite::Result<()> {
        let db = rusqlite::Connection::open_in_memory()?;
        create_schema(&db)
    }
}

/*
pub fn load_spec_node(db: &rusqlite::Connection, node: url::Url) -> Result<(), Error> {
    let (id, inserted) = intern_resource(db, &node);
    if !inserted {
        return Ok(())
    }

    println!("loading {}", &node);

    let br = io::BufReader::new(self.fs.open(&node)?);
    let spec: specs::Node = serde_yaml::from_reader(br)?;
    //let spec = spec.into_canonical(&node)?;

    for inc in &spec.include {
        let inc = canonical::join(&node, &inc)?;
        self.load_node(inc)?;
    }

    for c in &spec.collections {
        let name = canonical::join(&node, &c.name)?;

        let schema = canonical::join(&node, &c.schema)?;
        self.load_schema(schema.clone())?;

        let mut s = self.db.prepare_cached(
            "INSERT INTO collection (url, schema_url, key, partitions)\
                        VALUES (?, ?, ?, ?);")?;

        s.execute(&[
            name.as_str(),
            schema.as_str(),
            serde_json::to_string(&c.key)?.as_str(),
            serde_json::to_string(&c.partitions)?.as_str(),
        ])?;
    }

    Ok(())
}




















type Schema = schema::Schema<specs::Annotation>;

impl Catalog {

    pub fn define_schema(db: rusqlite::Connection) -> rusqlite::Result<rusqlite::Connection> {
    }

    pub fn new(db: rusqlite::Connection, fs: Box<dyn FileSystem>) -> Result<Loader, Error> {
        let db = Catalog::define_schema(db)?;

        db.execute_batch("
            ATTACH ':memory:' as tmp;
            CREATE TABLE tmp.seen ( url TEXT NOT NULL PRIMARY KEY );

            BEGIN;
            PRAGMA defer_foreign_keys = true;
        ")?;

        Ok(Loader {
            fs,
            db,
        })
    }

    pub fn build_schema_catalog(&self) -> Result<Vec<Schema>, Error> {
        let mut schemas = self.db.prepare_cached("SELECT url, body FROM schema;")?;
        let schemas = schemas.
            query_and_then(rusqlite::NO_PARAMS, |row: &rusqlite::Row| -> Result<Schema, Error> {
                let url = url::Url::parse(row.get_raw(0).as_str()?)?;
                let raw = serde_json::from_str(row.get_raw(1).as_str()?)?;
                Ok(schema::build::build_schema(url, &raw)?)
            })?;

        let mut catalog = Vec::new();
        for schema in schemas {
            catalog.push(schema?);
        }
        Ok(catalog)
    }

    pub fn finish(self) -> Result<rusqlite::Connection, Error> {
        let catalog = self.build_schema_catalog()?;

        let mut ind = schema::index::Index::new();
        for s in &catalog {
            ind.add(s)?;
        }
        ind.verify_references()?;

        /*
        let tmp: Option<String> = self.db.query_row(
            "SELECT url FROM collection WHERE schema_url NOT IN (SELECT url FROM schema);",
            rusqlite::NO_PARAMS,
            |row| row.get(0),
        ).optional()?;

        if let Some(url) = tmp {
            panic!("got url {} ", url)
        };
        */

        self.db.execute_batch("
            COMMIT;
            DETACH DATABASE tmp;
        ")?;

        Ok(self.db)
    }

    fn already_seen(&mut self, url: &url::Url) -> Result<bool, Error> {
        let mut s = self.db.prepare_cached(
            "INSERT INTO tmp.seen (url) VALUES (?) ON CONFLICT DO NOTHING;")?;

        Ok(s.execute(&[url.as_str()])? == 0)
    }


    fn load_schema(&mut self, mut url: url::Url) -> Result<(), Error> {
        url.set_fragment(None);

        if self.already_seen(&url)? {
            return Ok(())
        }
        println!("loading {}", url);

        let br = io::BufReader::new(self.fs.open(&url)?);
        let raw_schema: serde_json::Value = {
            if url.path().ends_with(".yaml") {
                serde_yaml::from_reader(br)?
            } else {
                serde_json::from_reader(br)?
            }
        };

        let compiled: Schema = schema::build::build_schema(url.clone(), &raw_schema)?;
        self.walk_schema(&url, &compiled)?;

        let mut s = self.db.prepare_cached(
            "INSERT INTO schema (url, body) VALUES (?, ?);")?;

        s.execute(&[url.as_str(), raw_schema.to_string().as_str()])?;
        Ok(())
    }

    fn walk_schema(&mut self, base: &url::Url, schema: &Schema) -> Result<(), Error> {
        use schema::Keyword;
        use schema::Application;

        Ok(for kw in &schema.kw {
            match kw {
                Keyword::Application(Application::Ref(ruri), _)  => {
                    self.load_schema(ruri.clone())?
                },
                Keyword::Application(_, child) => self.walk_schema(base, &child)?,
                // No-ops.
                Keyword::Anchor(_) | Keyword::RecursiveAnchor | Keyword::Validation(_) | Keyword::Annotation(_) => (),
            }
        })
    }

    /*
    fn process_root(&mut self, base: url::Url, spec: specs::Project) -> Result<(), Error> {
        for mut c in spec.collections {
            c.name = base.join(&c.name)?.to_string();
            c.schema = base.join(&c.schema)?.to_string();

            if !c.examples.is_empty() {
                c.examples = base.join(&c.examples)?.to_string();
            }
            if let Some(d) = &mut c.derivation {
                self.process_derivation(&base, d)
            }
        }
        Ok(())
    }

    fn process_derivation(&mut self, base: &url::Url, spec: &mut specs::Derivation) -> Result<(), Error> {
        use specs::Derivation;

        match &mut d {
            Derivation::Jq(d) =>
        }
    }

    fn process_path(&mut self, base: &url::Url, path: &mut String) -> Result<(), Error> {
        Ok(*path = base.join(path)?.to_string())
    }

    */
}
*/
