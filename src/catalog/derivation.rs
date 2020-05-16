use super::{sql_params, Collection, Error, Lambda, Resource, Result, Schema, DB};
use crate::specs::build as specs;

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
        // Map spec source collection name to its collection ID.
        let (cid, rid) = db
            .prepare_cached("SELECT collection_id, resource_id FROM collections WHERE name = ?")?
            .query_row(&[&spec.source.collection], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|e| Error::At {
                loc: format!("querying source collection {:?}", spec.source),
                detail: Box::new(e.into()),
            })?;

        let source = Collection {
            id: cid,
            resource: Resource { id: rid },
        };
        // Verify that the catalog spec of the source collection is imported by this collection's.
        Resource::verify_import(db, self.collection.resource, source.resource)?;

        // Register optional source schema. Like the collection's schema, this
        // URL may have a fragment component locating a specific sub-schema to
        // use. Drop the fragment when registering the schema document.
        let schema_url = match &spec.source.schema {
            None => None,
            Some(url) => {
                let url = self.collection.resource.join(db, url)?;

                let schema = Schema::register(db, &url).map_err(|err| Error::At {
                    loc: format!("source schema {:?}", &url),
                    detail: Box::new(err),
                })?;
                Resource::register_import(db, self.collection.resource, schema.resource)?;

                Some(url)
            }
        };

        let lambda = Lambda::register(db, self.collection.resource, &spec.lambda)?;

        db.prepare_cached(
            "INSERT INTO transforms (
                        derivation_id,
                        source_collection_id,
                        lambda_id,
                        source_schema_uri,
                        shuffle_key_json,
                        shuffle_broadcast,
                        shuffle_choose
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )?
        .execute(sql_params![
            self.collection.id,
            source.id,
            lambda.id,
            schema_url,
            spec.shuffle
                .key
                .as_ref()
                .map(|k| serde_json::to_string(&k).unwrap()),
            spec.shuffle.broadcast,
            spec.shuffle.choose,
        ])?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{dump_tables, init_db_schema, open, Source},
        *,
    };
    use serde_json::{json, Value};

    #[test]
    fn test_register() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

        let a_schema = json!(true);
        let alt_schema = json!({"$anchor": "foobar"});
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE),
                    (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
                    (20, 'application/schema+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![a_schema, alt_schema],
        )?;
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/spec', TRUE),
                    (10, 'test://example/a-schema.json', TRUE),
                    (20, 'test://example/alt-schema.json', TRUE);",
            sql_params![],
        )?;
        db.execute(
            "INSERT INTO collections (name, schema_uri, key_json, resource_id) VALUES
                    ('src/collection', 'test://example/a-schema.json', '[\"/key\"]', 1);",
            sql_params![],
        )?;
        let source = Source {
            resource: Resource { id: 1 },
        };

        // Derived collection with:
        //  - Explicit parallelism.
        //  - Explicit alternate source schema.
        //  - Explicit shuffle key w/ choose.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "d1/collection",
            "schema": "a-schema.json",
            "key": ["/d1-key"],
            "derivation": {
                "parallelism": 8,
                "bootstrap": [
                    {"nodeJS": "nodeJS bootstrap"},
                ],
                "transform": [
                    {
                        "source": "src/collection",
                        "sourceSchema": "alt-schema.json#foobar",
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
            "schema": "a-schema.json",
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

        let dump = dump_tables(&db, &["derivations", "transforms", "bootstraps", "lambdas"])?;

        assert_eq!(
            dump,
            json!({
                "derivations": [
                    [2, 8],
                    [3, null],
                ],
                "bootstraps":[
                    [1, 2, 1],
                ],
                "lambdas":[
                    [1, "nodeJS","nodeJS bootstrap", Value::Null],
                    [2, "nodeJS","lambda one", Value::Null],
                    [3, "nodeJS","lambda two", Value::Null],
                ],
                "transforms":[
                    [1, 2, 1, 2, "test://example/alt-schema.json#foobar", ["/shuffle", "/key"], null, 3],
                    [2, 3, 1, 3, null, null, null, null],
                ],
            }),
        );

        Ok(())
    }
}
