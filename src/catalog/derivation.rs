use super::{sql_params, Collection, Error, Lambda, Resource, Result, Schema, Scope};
use crate::doc::{validate, FullContext, SchemaIndex, Validator};
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
        let register_schema = scope
            .push_prop("register")
            .push_prop("schema")
            .then(|scope| {
                let register = Schema::register(scope, &spec.register.schema)?;
                Resource::register_import(scope, register.resource)?;
                Ok(register)
            })?;

        let register_schema_uri = register_schema.primary_url_with_fragment(scope.db)?;

        // Require that the initial register value validates against the schema.
        scope.push_prop("register").push_prop("initial").then(|_| {
            let mut index = SchemaIndex::new();
            let schemas = Schema::compile_for(scope.db, register_schema.resource.id)?;
            for schema in &schemas {
                index.add(&schema)?;
            }

            let mut validator = Validator::<FullContext>::new(&index);
            validate(&mut validator, &register_schema_uri, &spec.register.initial)
                .map_err(Error::FailedValidation)
        })?;

        scope
            .db
            .prepare_cached(
                "INSERT INTO derivations (
                collection_id,
                register_schema_uri,
                register_initial_json
            ) VALUES (?, ?, ?)",
            )?
            .execute(sql_params![
                collection.id,
                register_schema_uri,
                spec.register.initial,
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
                        read_delay_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?
            .execute(sql_params![
                self.collection.id,
                name,
                source.id,
                update.map(|l| l.id),
                publish.map(|l| l.id),
                schema_url,
                spec.shuffle
                    .as_ref()
                    .map(|k| serde_json::to_string(&k).unwrap()),
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
        let db = open(":memory:").unwrap();
        init_db_schema(&db).unwrap();

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
                INSERT INTO projections (collection_id, field, location_ptr, user_provided) VALUES
                    (1, 'a_field', '/a/field', true),
                    (1, 'other_field', '/other/field', true);

                INSERT INTO partitions (collection_id, field) VALUES
                    (1, 'a_field'),
                    (1, 'other_field');",
        )
        .unwrap();
        let scope = Scope::empty(&db);
        let scope = scope.push_resource(Resource { id: 1 });

        // Derived collection with:
        //  - Explicit register schema & initial value.
        //  - Explicit alternate source schema.
        //  - Explicit shuffle key.
        //  - Explicit read delay.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "d1/collection",
            "schema": "a-schema.json",
            "key": ["/d1-key"],
            "derivation": {
                "register": {
                    "schema": "reg-schema.json#/$defs/qib",
                    "initial": {"initial": ["value", 32]},
                },
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
                        "shuffle": ["/shuffle", "/key"],
                        "update": {"nodeJS": "update one"},
                        "publish": {"nodeJS": "publish one"},
                    },
                },
            }
        }))?;
        Collection::register(scope, &spec).unwrap();

        // Derived collection with implicit defaults.
        let spec: specs::Collection = serde_json::from_value(json!({
            "name": "d2/collection",
            "schema": "a-schema.json",
            "key": ["/d2-key"],
            "derivation": {
                "transform": {
                    "do-the-thing": {
                        "source": {"name": "src/collection"},
                        "publish": {"nodeJS": "publish two"},
                    },
                },
            }
        }))?;
        Collection::register(scope, &spec).unwrap();

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
                    [2, "test://example/reg-schema.json#/$defs/qib", {"initial": ["value", 32]}],
                    [3, "test://example/spec?ptr=/derivation/register/schema", null],
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
                    [1, 2, "some-name",    1, 2, 3, "test://example/alt-schema.json#foobar", ["/shuffle", "/key"], 3600],
                    [2, 3, "do-the-thing", 1, null, 4, null, null, null],
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
