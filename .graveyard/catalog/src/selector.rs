use super::{specs, sql_params, Collection, Result, Scope, DB};

/// Selector is a selection over partitions of a Collection.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub struct Selector {
    pub id: i64,
}

impl Selector {
    /// Register a partition Selector of the given Collection.
    pub fn register(
        scope: Scope,
        collection: Collection,
        spec: &specs::PartitionSelector,
    ) -> Result<Selector> {
        scope
            .db
            .prepare_cached("INSERT INTO partition_selectors (collection_id) VALUES (?)")?
            .execute(&[collection.id])?;

        let selector = Selector {
            id: scope.db.last_insert_rowid(),
        };

        for (m, is_exclude, scope) in &[
            (&spec.include, false, scope.push_prop("include")),
            (&spec.exclude, true, scope.push_prop("exclude")),
        ] {
            for (field, values) in m.iter() {
                for (index, value) in values.iter().enumerate() {
                    scope.push_prop(field).push_item(index).then(|scope| {
                        Ok(scope
                            .db
                            .prepare_cached(
                                "INSERT INTO partition_selector_labels (
                                    selector_id,
                                    collection_id,
                                    field,
                                    value_json,
                                    is_exclude
                                ) VALUES (?, ?, ?, ?, ?);",
                            )?
                            .execute(sql_params![
                                selector.id,
                                collection.id,
                                field,
                                value,
                                is_exclude,
                            ])?)
                    })?;
                }
            }
        }
        Ok(selector)
    }

    /// Load a Selector back into its comprehensive PartitionSelector specification.
    pub fn load(&self, db: &DB) -> Result<specs::PartitionSelector> {
        let mut stmt =
            db.prepare("SELECT selector_json FROM partition_selectors_json WHERE selector_id = ?")?;
        stmt.query_row(sql_params![self.id], |row| {
            Ok(serde_json::from_str::<specs::PartitionSelector>(row.get_raw(0).as_str()?).unwrap())
        })
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::{create, dump_tables, Collection, Resource},
        *,
    };
    use serde_json::json;

    #[test]
    fn test_register_and_load() {
        let db = create(":memory:").unwrap();

        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (1, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE);",
            sql_params![],
        )
        .unwrap();
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (1, 'test://example/spec', TRUE)",
            sql_params![],
        )
        .unwrap();

        let scope = Scope::empty(&db);
        let scope = scope.push_resource(Resource { id: 1 });

        let collection: specs::Collection = serde_json::from_value(json!({
            "name": "test/collection",
            "schema": {
                "properties": {
                    "a": {"type": "string"},
                    "b": {"type": "integer"},
                },
            },
            "key": ["/key"],
            "projections": {
                "field_a": {"location": "/a", "partition": true},
                "field_b": {"location": "/b", "partition": true},
            }
        }))
        .unwrap();
        let collection = Collection::register(scope, &collection).unwrap();

        let selector_spec = json!({
            "include": {"field_a": [true, null, 42], "field_b": ["52"]},
            "exclude": {"field_a": [-1], "field_b": [null]},
        });
        let selector = Selector::register(
            scope,
            collection,
            &serde_json::from_value(selector_spec.clone()).unwrap(),
        )
        .unwrap();

        assert_eq!(selector.id, 1);

        let dump = dump_tables(
            &db,
            &[
                "partition_selector_labels",
                "partition_selectors",
                "partition_selectors_json",
            ],
        )
        .unwrap();

        assert_eq!(
            dump,
            json!({
                "partition_selectors":[
                    [1, 1],
                ],
                "partition_selector_labels":[
                    [1, 1, "field_a", true, false],
                    [1, 1, "field_a", null, false],
                    [1, 1, "field_a", 42, false],
                    [1, 1, "field_b", "52", false],

                    [1, 1, "field_a", -1, true],
                    [1, 1, "field_b", null, true],
                ],
                // Expect view partition_selectors_json projects the selector
                // into a shape compatible with specs::ProtocolSelector.
                "partition_selectors_json":[
                    [1, 1, {
                        "include": {
                            "field_a": [true, null, 42],
                            "field_b": ["52"],
                        },
                        "exclude": {
                            "field_a": [-1],
                            "field_b": [null],
                        },
                    }]
                ],
            }),
        );

        // We can load a selector back into a specification.
        let recovered = selector.load(scope.db).unwrap();
        let recovered = serde_json::to_value(&recovered).unwrap();
        assert_eq!(selector_spec, recovered);
    }
}
