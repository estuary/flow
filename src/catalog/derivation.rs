use super::{Collection, Lambda, Resource, Result, Schema};
use crate::specs::build as specs;
use rusqlite::{params as sql_params, Connection as DB};

/// Derivation is a catalog Collection which is derived from other Collections.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Derivation {
    pub collection: Collection,
}

impl Derivation {
    /// Register a catalog Collection as a Derivation.
    pub fn register(
        db: &DB,
        collection: Collection,
        spec: &specs::Derivation,
    ) -> Result<Derivation> {
        db.prepare_cached("INSERT INTO derivations (collection_id, parallelism) VALUES (?, ?)")?
            .execute(sql_params![collection.id, spec.parallelism])?;

        let derivation = Derivation { collection };

        for spec in &spec.bootstrap {
            derivation.register_bootstrap(db, spec)?;
        }
        for spec in &spec.transform {
            derivation.register_transform(db, spec)?;
        }
        Ok(derivation)
    }

    fn register_bootstrap(&self, db: &DB, spec: &specs::Lambda) -> Result<()> {
        let lambda = Lambda::register(db, self.collection.resource, spec)?;

        db.prepare_cached("INSERT INTO bootstraps (derivation_id, lambda_id) VALUES (?, ?)")?
            .execute(sql_params![self.collection.id, lambda.id])?;
        Ok(())
    }

    fn register_transform(&self, db: &DB, spec: &specs::Transform) -> Result<()> {
        let source = Collection::query(db, &spec.source, self.collection.resource)?;

        // Register optional source schema.
        let mut schema_uri: Option<String> = None;

        if let Some(uri) = &spec.source_schema {
            let uri = self.collection.resource.join(db, uri)?;
            let schema = Schema::register(db, uri.clone())?;
            Resource::register_import(db, self.collection.resource, schema.resource)?;
            schema_uri = Some(uri.into_string())
        }

        let lambda = Lambda::register(db, self.collection.resource, &spec.lambda)?;

        db.prepare_cached(
            "INSERT INTO transforms (
                        derivation_id,
                        source_collection_id,
                        source_schema_uri,
                        shuffle_key_json,
                        shuffle_broadcast,
                        shuffle_choose,
                        lambda_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )?
        .execute(sql_params![
            self.collection.id,
            source.id,
            schema_uri,
            spec.shuffle
                .key
                .as_ref()
                .map(|k| serde_json::to_string(&k).unwrap()),
            spec.shuffle.broadcast,
            spec.shuffle.choose,
            lambda.id,
        ])?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{db, Source},
        *,
    };
    use serde_json::json;
    use tempfile;
    use url::Url;

    #[test]
    fn test_register() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let fixtures = [
            (
                Url::from_file_path(dir.path().join("src-schema.json")).unwrap(),
                json!(true),
            ),
            (
                Url::from_file_path(dir.path().join("alt-schema.json")).unwrap(),
                json!({ "$defs": {"alt-def": true} }),
            ),
            (
                Url::from_file_path(dir.path().join("derived-schema.json")).unwrap(),
                json!(true),
            ),
        ];
        for (uri, val) in fixtures.iter() {
            std::fs::write(uri.to_file_path().unwrap(), val.to_string())?;
        }

        let db = DB::open_in_memory()?;
        db::init(&db)?;

        let source = fixtures[0].0.join("root")?;
        let source = Source {
            resource: Resource::register(&db, source)?,
        };

        // Collection which is derived from.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "src/collection",
            "schema": "src-schema.json",
            "key": ["/key/1", "/key/0"],
        }))?;
        Collection::register(&db, source, &spec)?;

        // Derived collection with:
        //  - Explicit parallelism.
        //  - Explicit alternate source schema.
        //  - Explicit shuffle key w/ choose.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "d1/collection",
            "schema": "derived-schema.json",
            "key": ["/d1-key"],
            "derivation": {
                "parallelism": 8,
                "bootstrap": [
                    {"nodeJS": "nodeJS bootstrap"},
                ],
                "transform": [
                    {
                        "source": "src/collection",
                        "sourceSchema": "alt-schema.json#/$defs/alt-def",
                        "shuffle": {
                            "key": ["/shuffle", "/key"],
                            "choose": 3,
                        },
                        "lambda": {"nodeJS": "lambda one"},
                    },
                ],
            }
        }))?;
        Collection::register(&db, source, &spec)?;

        // Derived collection with implicit defaults.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "d2/collection",
            "schema": "derived-schema.json",
            "key": ["/d2-key"],
            "derivation": {
                "transform": [
                    {
                        "source": "src/collection",
                        "lambda": {"nodeJS": "lambda two"},
                    },
                ],
            }
        }))?;
        Collection::register(&db, source, &spec)?;

        // Expect the tranform records the absolute schema URI, with fragment.
        let full_alt_schema_uri = source
            .resource
            .uri(&db)?
            .join("alt-schema.json#/$defs/alt-def")?
            .to_string();

        let dump = db::dump_tables(
            &db,
            &[
                "derivations",
                "schemas",
                "transforms",
                "bootstraps",
                "lambdas",
            ],
        )?;

        assert_eq!(
            dump,
            json!({
                "bootstraps":[
                    [1, 2, 1],
                ],
                "derivations": [
                    [2, 8],
                    [3, null],
                ],
                "lambdas":[
                    [1,"nodeJS","nodeJS bootstrap",1],
                    [2,"nodeJS","lambda one",1],
                    [3,"nodeJS","lambda two",1],
                ],
                "schemas":[
                    [true, 2],
                    [true, 3],
                    [{"$defs":{"alt-def":true}}, 4],
                ],
                "transforms":[
                    [1,2,1,full_alt_schema_uri, ["/shuffle","/key"],null,3,2],
                    [2,3,1,null,null,null,null,3],
                ],
            }),
        );

        Ok(())
    }
}
