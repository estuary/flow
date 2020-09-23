use super::{specs, sql_params, Collection, Result, Scope, Selector, DB};
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

    /// Load a TestCase back into its comprehensive specification.
    pub fn load(&self, db: &DB) -> Result<(String, Vec<specs::TestStep>)> {
        let mut stmt = db.prepare(
            "SELECT test_case_name, steps_json FROM test_cases_json WHERE test_case_id = ?",
        )?;
        stmt.query_row(sql_params![self.id], |row| {
            Ok((
                row.get(0)?, // test_case_name.
                serde_json::from_str::<Vec<specs::TestStep>>(row.get_raw(1).as_str()?).unwrap(),
            ))
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

        let steps_fixture = json!([
            {"ingest": {
                "collection": "test/collection",
                "documents": [{"ingest":1}, true],
            }},
            // No partition selector provided.
            {"verify": {
                "collection": "test/collection",
                "partitions": null,
                "documents": [{"verify":2}, false],
            }},
            // With explicit selector.
            {"verify": {
                "collection": "test/collection",
                "partitions": {
                    "include": {"a_field": ["some-val"]},
                    "exclude": {},
                },
                "documents": [{"verify":3}, "fin"],
            }},
        ]);

        let case = TestCase::register(
            scope,
            "my test",
            &serde_json::from_value(steps_fixture.clone()).unwrap(),
        )
        .unwrap();
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
                    [1, "my test", steps_fixture]
                ],
            }),
        );

        // Load the TestCase. Expect we recover the original, registered fixture.
        let (loaded_name, loaded_steps) = case.load(&db).unwrap();
        let loaded_steps = serde_json::to_value(&loaded_steps).unwrap();
        assert_eq!(&loaded_name, "my test");
        assert_eq!(&loaded_steps, &steps_fixture);
    }
}
