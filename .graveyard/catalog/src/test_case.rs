use super::{specs, sql_params, Collection, Result, Scope, Selector, DB};
use itertools::Itertools;
use serde_json::Value;
use std::fmt::{self, Display};

/// TestCase represents a catalog test case and contained sequence of test steps.
#[derive(Debug)]
pub struct TestCase {
    pub id: i64,
}

impl TestCase {
    /// Register a TestCase of the catalog.
    pub fn register(scope: Scope, name: &str, steps: &[specs::TestStep]) -> Result<TestCase> {
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
            .then(|scope| Collection::get_imported_by_name(scope, &spec.collection.as_ref()))?;

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
            .then(|scope| Collection::get_imported_by_name(scope, &spec.collection.as_ref()))?;

        let key = collection.key(scope.db)?;
        verify_are_documents_ordered(key, spec.documents.as_slice())?;

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

fn verify_are_documents_ordered(collection_key: Vec<String>, documents: &[Value]) -> Result<()> {
    use doc::Pointer;
    let pointers = collection_key
        .iter()
        .map(|p| Pointer::from(p))
        .collect::<Vec<_>>();

    let is_sorted = is_sorted_by(documents, |a, b| Pointer::compare(&pointers, a, b));
    if !is_sorted {
        let mut sorted = documents.to_vec();
        // This is a stable sort, so the suggested order will change what's given as minimally as
        // possible.
        sorted.sort_by(|a, b| Pointer::compare(&pointers, a, b));
        Err(TestVerifyOutOfOrder {
            collection_key,
            reordered_documents: sorted,
        })?
    }
    Ok(())
}

// This function exists in the standard library, but it is not yet stable.
// See: https://github.com/rust-lang/rust/issues/53485
fn is_sorted_by<T, F>(seq: &[T], cmp_fun: F) -> bool
where
    F: Fn(&T, &T) -> std::cmp::Ordering,
{
    for (i, item) in seq.iter().enumerate() {
        if let Some(n) = seq.get(i + 1) {
            if cmp_fun(item, n) == std::cmp::Ordering::Greater {
                return false;
            }
        } else {
            return true;
        }
    }
    true
}

#[derive(Debug)]
pub struct TestVerifyOutOfOrder {
    collection_key: Vec<String>,
    reordered_documents: Vec<Value>,
}

impl Display for TestVerifyOutOfOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "The documents to verify must be provided in lexicographic order according to the collection key: [{}]",
                 self.collection_key.iter().join(", "))?;
        let ordered = serde_json::to_string_pretty(&self.reordered_documents).unwrap();
        writeln!(f, "A suggested ordering is: \n{}", ordered)?;
        Ok(())
    }
}
impl std::error::Error for TestVerifyOutOfOrder {}

#[cfg(test)]
mod test {
    use super::{
        super::{create, dump_tables, Collection, Resource},
        *,
    };
    use crate::specs::{CollectionName, TestStep, TestStepVerify};
    use crate::Error;
    use serde_json::json;

    #[test]
    fn register_returns_error_when_expected_documents_are_not_ordered() {
        let db = create(":memory:").unwrap();
        db.execute_batch(r##"
            INSERT INTO resources (resource_id, content_type, content, is_processed)
            VALUES (111, 'application/vnd.estuary.dev-catalog-spec+yaml', X'1234', FALSE);

            INSERT INTO collections (collection_id, collection_name, schema_uri, key_json, resource_id)
            VALUES (7, 'foo', 'test://schema.json', '["/a", "/b"]', 111);
            "##).unwrap();

        let spec = TestStep::Verify(TestStepVerify {
            collection: CollectionName::new("foo"),
            documents: vec![
                json!({"b": 9}),
                json!({"a": 1, "b": 5}),
                json!({"a": 2, "b": 5}),
                json!({"a": 2, "b": 6}),
                json!({"a": 2, "b": 6}),
                json!({"a": 2, "b": 6}),
                json!({"a": 3}),
                json!({"c": "cee"}),
                json!({"d": "dee"}),
            ],
            partitions: None,
        });
        let scope = Scope::for_test(&db, 111);

        let err = TestCase::register(scope, "test-the-test", &[spec])
            .expect_err("expected an err")
            .unlocate();

        match err {
            Error::TestInvalid(ooo) => {
                let expected_order = vec![
                    json!({"c": "cee"}),
                    json!({"d": "dee"}),
                    json!({"b": 9}),
                    json!({"a": 1, "b": 5}),
                    json!({"a": 2, "b": 5}),
                    json!({"a": 2, "b": 6}),
                    json!({"a": 2, "b": 6}),
                    json!({"a": 2, "b": 6}),
                    json!({"a": 3}),
                ];
                assert_eq!(expected_order, ooo.reordered_documents);
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

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
            &serde_json::from_value::<Vec<_>>(steps_fixture.clone()).unwrap(),
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
