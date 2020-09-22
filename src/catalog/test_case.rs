use super::{specs, sql_params, Collection, Result, Scope, Selector};
use serde_json::Value;

/// TestCase represents a catalog test case and contained sequence of test steps.
#[derive(Debug)]
pub struct TestCase {
    pub id: i64,
}

impl TestCase {
    /// Register a TestCase of the catalog.
    pub fn register(scope: Scope, name: &str, steps: &Vec<specs::TestStep>) -> Result<TestCase> {
        scope
            .db
            .prepare_cached(
                "INSERT INTO test_cases (
                test_case_name,
                resource_id
            ) VALUES (?, ?)",
            )?
            .execute(sql_params![name, scope.resource().id])?;

        let case = TestCase {
            id: scope.db.last_insert_rowid(),
        };

        for (index, step) in steps.iter().enumerate() {
            let scope = scope.push_item(index);
            match step {
                specs::TestStep::Ingest(spec) => Self::register_ingest(scope, &case, index, spec)?,
                specs::TestStep::Verify(spec) => Self::register_verify(scope, &case, index, spec)?,
            };
        }
        Ok(case)
    }

    pub fn register_ingest(
        scope: Scope,
        case: &TestCase,
        index: usize,
        spec: &specs::TestStepIngest,
    ) -> Result<()> {
        let collection = scope
            .push_prop("collection")
            .then(|scope| Collection::get_by_name(scope, &spec.collection))?;

        scope
            .db
            .prepare_cached(
                "INSERT INTO test_step_ingests (
                    test_case_id,
                    step_index,
                    collection_id,
                    documents_json
                ) VALUES (?, ?, ?, ?)",
            )?
            .execute(sql_params![
                case.id,
                index as i64,
                collection.id,
                Value::Array(spec.documents.clone()),
            ])?;

        Ok(())
    }

    pub fn register_verify(
        scope: Scope,
        case: &TestCase,
        index: usize,
        spec: &specs::TestStepVerify,
    ) -> Result<()> {
        let collection = scope
            .push_prop("collection")
            .then(|scope| Collection::get_by_name(scope, &spec.collection))?;

        // Register optional source partition selector.
        let selector = scope.push_prop("partitions").then(|scope| {
            spec.partitions
                .as_ref()
                .map(|spec| Selector::register(scope, collection, spec))
                .map_or(Ok(None), |v| v.map(Some))
        })?;

        scope
            .db
            .prepare_cached(
                "INSERT INTO test_step_verifies (
                    test_case_id,
                    step_index,
                    collection_id,
                    selector_id,
                    documents_json
                ) VALUES (?, ?, ?, ?, ?)",
            )?
            .execute(sql_params![
                case.id,
                index as i64,
                collection.id,
                selector.map(|s| s.id),
                Value::Array(spec.documents.clone()),
            ])?;

        Ok(())
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
    fn test_register() {
        let db = create(":memory:").unwrap();

        db.execute(
            "INSERT INTO resources (resource_id, content_type, content, is_processed) VALUES
                    (111, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE);",
            sql_params![],
        )
        .unwrap();
        db.execute(
            "INSERT INTO resource_urls (resource_id, url, is_primary) VALUES
                    (111, 'test://example/spec', TRUE)",
            sql_params![],
        )
        .unwrap();

        let scope = Scope::empty(&db);
        let scope = scope.push_resource(Resource { id: 111 });

        let collection: specs::Collection = serde_json::from_value(json!({
            "name": "test/collection",
            "schema": {
                "properties": { "a": {"type": "string"} },
            },
            "key": ["/key"],
            "projections": {
                "a_field": {"location": "/a", "partition": true},
            }
        }))
        .unwrap();
        Collection::register(scope, &collection).unwrap();

        let steps: Vec<specs::TestStep> = serde_json::from_value(json!([
            {"ingest": {
                "collection": "test/collection",
                "documents": [{"ingest":1}, true],
            }},
            // No partition selector provided.
            {"verify": {
                "collection": "test/collection",
                "documents": [{"verify":2}, false],
            }},
            // With explicit selector.
            {"verify": {
                "collection": "test/collection",
                "partitions": {
                    "include": {"a_field": ["some-val"]},
                },
                "documents": [{"verify":3}, "fin"],
            }},
        ]))
        .unwrap();

        let case = TestCase::register(scope, "my test", &steps).unwrap();
        assert_eq!(case.id, 1);

        let dump = dump_tables(
            &db,
            &[
                "test_cases",
                "test_step_ingests",
                "test_step_verifies",
                "test_cases_json",
            ],
        )
        .unwrap();

        assert_eq!(
            dump,
            json!({
                "test_cases": [
                    [1, "my test", 111],
                ],
                "test_step_ingests": [
                    [1, 0, 1, [{"ingest":1}, true]],
                ],
                "test_step_verifies": [
                    [1, 1, 1, null, [{"verify":2}, false]],
                    [1, 2, 1, 1, [{"verify":3}, "fin"]],
                ],
                "test_cases_json": [
                    [1, {
                        "name": "my test",
                        "steps": [
                            {
                                "operation": "ingest",
                                "collection": "test/collection",
                                "documents": [{"ingest":1}, true],
                            },
                            {
                                "operation": "verify",
                                "collection": "test/collection",
                                "documents": [{"verify":2}, false],
                                "selector": {},
                            },
                            {
                                "operation": "verify",
                                "collection": "test/collection",
                                "documents": [{"verify":3}, "fin"],
                                "selector": {
                                    "include": [{"name": "a_field", "value": "some-val"}],
                                    "exclude": [],
                                },
                            },
                        ],
                    }]
                ],
            }),
        );
    }
}
