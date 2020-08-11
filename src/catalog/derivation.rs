use super::{sql_params, Collection, Lambda, Resource, Result, Schema, Scope};
use crate::specs::build as specs;

/// Derivation is a catalog Collection which is derived from other Collections.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Derivation {
    pub collection: Collection,
}

impl Derivation {
    /// Register a catalog Collection as a Derivation.
    pub fn register(
        scope: Scope,
        collection: Collection,
        spec: &specs::Derivation,
    ) -> Result<Derivation> {
        // Register and import the register schema document.
        let register = scope.push_prop("register").then(|scope| {
            let register = Schema::register(scope, &spec.register)?;
            Resource::register_import(scope, register.resource)?;
            Ok(register)
        })?;

        scope
            .db
            .prepare_cached(
                "INSERT INTO derivations (
                collection_id,
                parallelism,
                register_uri
            ) VALUES (?, ?, ?)",
            )?
            .execute(sql_params![
                collection.id,
                spec.parallelism,
                register.primary_url_with_fragment(scope.db)?,
            ])?;

        let derivation = Derivation { collection };

        for (index, spec) in spec.bootstrap.iter().enumerate() {
            scope
                .push_prop("bootstrap")
                .push_item(index)
                .then(|scope| derivation.register_bootstrap(scope, spec))?;
        }
        for (name, spec) in spec.transform.iter() {
            scope
                .push_prop("transform")
                .push_prop(name)
                .then(|scope| derivation.register_transform(scope, name, spec))?;
        }
        Ok(derivation)
    }

    fn register_bootstrap(&self, scope: Scope, spec: &specs::Lambda) -> Result<()> {
        let lambda = Lambda::register(scope, spec)?;

        scope
            .db
            .prepare_cached("INSERT INTO bootstraps (derivation_id, lambda_id) VALUES (?, ?)")?
            .execute(sql_params![self.collection.id, lambda.id])?;
        Ok(())
    }

    fn register_transform(&self, scope: Scope, name: &str, spec: &specs::Transform) -> Result<()> {
        // Map spec source collection name to its collection ID.
        let source = scope.push_prop("source").then(|scope| {
            let (cid, rid) = scope
                .db
                .prepare_cached(
                    "SELECT collection_id, resource_id FROM collections WHERE collection_name = ?",
                )?
                .query_row(&[&spec.source.name], |r| Ok((r.get(0)?, r.get(1)?)))?;

            let source = Collection {
                id: cid,
                resource: Resource { id: rid },
            };
            // Verify that the catalog spec of the source collection is imported by this collection's catalog.
            Resource::verify_import(scope, source.resource)?;
            Ok(source)
        })?;
        // Register optional source schema.
        let schema_url = scope
            .push_prop("source")
            .push_prop("schema")
            .then(|scope| match &spec.source.schema {
                Some(schema) => {
                    let schema = Schema::register(scope, schema)?;
                    Resource::register_import(scope, schema.resource)?;
                    Ok(Some(schema.primary_url_with_fragment(scope.db)?))
                }
                None => Ok(None),
            })?;

        // Register "update" and "publish" lambdas.
        let update = match &spec.update {
            None => None,
            Some(l) => Some(
                scope
                    .push_prop("update")
                    .then(|scope| Lambda::register(scope, l))?,
            ),
        };
        let publish = match &spec.publish {
            None => None,
            Some(l) => Some(
                scope
                    .push_prop("publish")
                    .then(|scope| Lambda::register(scope, l))?,
            ),
        };

        scope
            .db
            .prepare_cached(
                "INSERT INTO transforms (
                        derivation_id,
                        transform_name,
                        source_collection_id,
                        update_id,
                        publish_id,
                        source_schema_uri,
                        shuffle_key_json,
                        shuffle_broadcast,
                        shuffle_choose,
                        read_delay_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?
            .execute(sql_params![
                self.collection.id,
                name,
                source.id,
                update.map(|l| l.id),
                publish.map(|l| l.id),
                schema_url,
                spec.shuffle
                    .key
                    .as_ref()
                    .map(|k| serde_json::to_string(&k).unwrap()),
                spec.shuffle.broadcast,
                spec.shuffle.choose,
                spec.read_delay.map(|d| d.as_secs() as i64),
            ])?;

        self.register_transform_source_partitions(
            scope,
            scope.db.last_insert_rowid(),
            source.id,
            &spec.source.partitions,
        )?;

        Ok(())
    }

    fn register_transform_source_partitions(
        &self,
        scope: Scope,
        transform_id: i64,
        collection_id: i64,
        parts: &specs::PartitionSelector,
    ) -> Result<()> {
        for (m, is_exclude, scope) in &[
            (&parts.include, false, scope.push_prop("include")),
            (&parts.exclude, true, scope.push_prop("exclude")),
        ] {
            for (field, values) in m.iter() {
                for (index, value) in values.iter().enumerate() {
                    scope.push_prop(field).push_item(index).then(|scope| {
                        Ok(scope
                            .db
                            .prepare_cached(
                                "INSERT INTO transform_source_partitions (
                                    transform_id,
                                    collection_id,
                                    field,
                                    value_json,
                                    is_exclude
                                ) VALUES (?, ?, ?, ?, ?);",
                            )?
                            .execute(sql_params![
                                transform_id,
                                collection_id,
                                field,
                                value,
                                is_exclude,
                            ])?)
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{dump_tables, init_db_schema, open},
        *,
    };
    use serde_json::json;

    #[test]
    fn test_register() -> Result<()> {
        let db = open(":memory:")?;
        init_db_schema(&db)?;

        let a_schema = json!(true);
        let alt_schema = json!({"$anchor": "foobar"});
        let register_schema = json!({"$defs": {"qib": true}});
        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE),
                    (10, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
                    (20, 'application/schema+yaml', CAST(? AS BLOB), FALSE),
                    (30, 'application/schema+yaml', CAST(? AS BLOB), FALSE);",
            sql_params![a_schema, alt_schema, register_schema],
        )?;
        db.execute_batch(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/spec', TRUE),
                    (10, 'test://example/a-schema.json', TRUE),
                    (20, 'test://example/alt-schema.json', TRUE),
                    (30, 'test://example/reg-schema.json', TRUE);
                INSERT INTO collections (collection_name, schema_uri, key_json, resource_id) VALUES
                    ('src/collection', 'test://example/a-schema.json', '[\"/key\"]', 1);
                INSERT INTO projections (collection_id, field, location_ptr) VALUES
                    (1, 'a_field', '/a/field'),
                    (1, 'other_field', '/other/field');
                INSERT INTO partitions (collection_id, field) VALUES
                    (1, 'a_field'),
                    (1, 'other_field');",
        )?;
        let scope = Scope::empty(&db);
        let scope = scope.push_resource(Resource { id: 1 });

        // Derived collection with:
        //  - Explicit parallelism.
        //  - External register schema.
        //  - Explicit alternate source schema.
        //  - Explicit shuffle key w/ choose.
        //  - Explicit read delay.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "d1/collection",
            "schema": "a-schema.json",
            "key": ["/d1-key"],
            "derivation": {
                "parallelism": 8,
                "register": "reg-schema.json#/$defs/qib",
                "bootstrap": [
                    {"nodeJS": "nodeJS bootstrap"},
                ],
                "transform": {
                    "some-name": {
                        "source": {
                            "name": "src/collection",
                            "schema": "alt-schema.json#foobar",
                            "partitions": {
                                "include": {"a_field": ["foo", 42]},
                                "exclude": {"other_field": [false]},
                            },
                        },
                        "readDelay": "1 hour",
                        "shuffle": {
                            "key": ["/shuffle", "/key"],
                            "choose": 3,
                        },
                        "update": {"nodeJS": "update one"},
                        "publish": {"nodeJS": "publish one"},
                    },
                },
            }
        }))?;
        Collection::register(scope, &spec)?;

        // Derived collection with implicit defaults.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "d2/collection",
            "schema": "a-schema.json",
            "key": ["/d2-key"],
            "derivation": {
                "register": true,
                "transform": {
                    "do-the-thing": {
                        "source": {"name": "src/collection"},
                        "publish": {"nodeJS": "publish two"},
                    },
                },
            }
        }))?;
        Collection::register(scope, &spec)?;

        let dump = dump_tables(
            &db,
            &[
                "derivations",
                "transforms",
                "transform_source_partitions",
                "bootstraps",
                "lambdas",
            ],
        )?;

        assert_eq!(
            dump,
            json!({
                "derivations": [
                    [2, 8, "test://example/reg-schema.json#/$defs/qib"],
                    [3, null, "test://example/spec?ptr=/derivation/register"],
                ],
                "bootstraps":[
                    [1, 2, 1],
                ],
                "lambdas":[
                    [1, "nodeJS","nodeJS bootstrap", null],
                    [2, "nodeJS","update one", null],
                    [3, "nodeJS","publish one", null],
                    [4, "nodeJS","publish two", null],
                ],
                "transforms":[
                    [1, 2, "some-name",    1, 2, 3, "test://example/alt-schema.json#foobar", ["/shuffle", "/key"], null, 3, 3600],
                    [2, 3, "do-the-thing", 1, null, 4, null, null, null, null, null],
                ],
                "transform_source_partitions":[
                    [1, 1, "a_field", "foo", false],
                    [1, 1, "a_field", 42, false],
                    [1, 1, "other_field", false, true],
                ],
            }),
        );

        Ok(())
    }
}
