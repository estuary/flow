use super::{collection, errors::Error, indexed, reference, schema, Scope};
use flow::test_spec::step::Type as StepType;
use proto_flow::flow;

pub fn walk_all_tests(
    built_collections: &[tables::BuiltCollection],
    tests: &[tables::Test],
    errors: &mut tables::Errors,
) -> tables::BuiltTests {
    let mut built_tests = tables::BuiltTests::new();

    for tables::Test {
        scope,
        test,
        spec: steps,
    } in tests
    {
        let scope = Scope::new(scope);

        indexed::walk_name(scope, "test", test, models::Test::regex(), errors);

        let steps = steps
            .iter()
            .enumerate()
            .filter_map(|(step_index, test_step)| {
                walk_test_step(
                    scope.push_item(step_index),
                    built_collections,
                    step_index,
                    test_step,
                    errors,
                )
            })
            .collect();

        built_tests.insert_row(
            scope.flatten(),
            test,
            flow::TestSpec {
                name: test.to_string(),
                steps,
            },
        );
    }

    indexed::walk_duplicates(
        tests
            .iter()
            .map(|s| ("test", s.test.as_str(), Scope::new(&s.scope))),
        errors,
    );

    built_tests
}

pub fn walk_test_step(
    scope: Scope,
    built_collections: &[tables::BuiltCollection],
    step_index: usize,
    test_step: &models::TestStep,
    errors: &mut tables::Errors,
) -> Option<flow::test_spec::Step> {
    // Decompose the test step into its parts.
    let (step_type, collection, description, documents, selector) = match test_step {
        models::TestStep::Ingest(models::TestStepIngest {
            description,
            collection,
            documents,
        }) => (StepType::Ingest, collection, description, documents, None),

        models::TestStep::Verify(models::TestStepVerify {
            description,
            collection: models::Source::Collection(collection),
            documents,
        }) => (StepType::Verify, collection, description, documents, None),

        models::TestStep::Verify(models::TestStepVerify {
            description,
            collection:
                models::Source::Source(models::FullSource {
                    name: collection,
                    partitions,
                }),
            documents,
        }) => (
            StepType::Verify,
            collection,
            description,
            documents,
            partitions.as_ref(),
        ),
    };
    let scope = match step_type {
        StepType::Ingest => scope.push_prop("ingest"),
        StepType::Verify => scope.push_prop("verify"),
    };

    let collection = reference::walk_reference(
        scope.push_prop("collection"),
        "this test step",
        "collection",
        collection.as_str(),
        built_collections,
        |c| (&c.collection, Scope::new(&c.scope)),
        errors,
    )?;
    let documents = serde_json::from_str::<Vec<serde_json::Value>>(documents.get())
        .expect("a documents fixture is verified to be an array of objects during load");

    if let StepType::Ingest = step_type {
        // Require that all documents validate for both writes and reads.
        let mut write_schema = schema::Schema::new(&collection.spec.write_schema_json).ok();
        let mut read_schema = Some(&collection.spec.read_schema_json)
            .filter(|s| !s.is_empty())
            .and_then(|schema| schema::Schema::new(schema).ok());

        for (doc_index, doc) in documents.iter().enumerate() {
            for schema in [&mut read_schema, &mut write_schema] {
                if let Some(err) = schema
                    .as_mut()
                    .and_then(|s| s.validator.validate(None, doc).unwrap().ok().err())
                {
                    Error::IngestDocInvalid(err)
                        .push(scope.push_prop("documents").push_item(doc_index), errors);
                }
            }
        }
    } else {
        // Verify that any verified documents are ordered correctly w.r.t.
        // the collection's key.
        for (doc_index, (lhs, rhs)) in documents.windows(2).map(|p| (&p[0], &p[1])).enumerate() {
            if json::json_cmp_at(&collection.spec.key, lhs, rhs).is_gt() {
                Error::TestVerifyOrder
                    .push(scope.push_prop("documents").push_item(doc_index), errors);
            }
        }
    };

    // Verify a provided partition selector is valid.
    if let Some(selector) = selector {
        collection::walk_selector(
            scope.push_prop("collection").push_prop("partitions"),
            &collection.spec,
            &selector,
            errors,
        );
    }

    Some(flow::test_spec::Step {
        step_type: step_type as i32,
        step_index: step_index as u32,
        step_scope: scope.flatten().into(),
        collection: collection.collection.to_string(),
        docs_json_vec: documents.into_iter().map(|d| d.to_string()).collect(),
        partitions: Some(assemble::journal_selector(&collection.collection, selector)),
        description: description.clone(),
    })
}
