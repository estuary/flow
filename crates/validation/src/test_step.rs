use super::{collection, errors::Error, indexed, reference, schema};
use itertools::Itertools;
use models::tables;
use protocol::flow::{self, test_spec::step::Type as TestStepType};

pub fn walk_all_test_steps(
    collections: &[tables::Collection],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_index: &doc::SchemaIndex<'_>,
    schemas: &[schema::Shape],
    test_steps: &[tables::TestStep],
    errors: &mut tables::Errors,
) -> tables::BuiltTests {
    let mut built_tests = tables::BuiltTests::new();

    for (test, steps) in &test_steps
        .iter()
        .sorted_by_key(|s| (&s.test, s.step_index))
        .group_by(|s| &s.test)
    {
        let steps: Vec<_> = steps
            .map(|test_step| {
                walk_test_step(
                    collections,
                    imports,
                    projections,
                    schema_index,
                    schemas,
                    test_step,
                    errors,
                )
                .into_iter()
            })
            .flatten()
            .collect();

        built_tests.push_row(
            test,
            flow::TestSpec {
                test: test.to_string(),
                steps,
            },
        );
    }

    indexed::walk_duplicates(
        "test",
        test_steps
            .iter()
            .filter(|s| s.step_index == 0)
            .map(|s| (&s.test, &s.scope)),
        errors,
    );

    built_tests
}

pub fn walk_test_step(
    collections: &[tables::Collection],
    imports: &[&tables::Import],
    projections: &[tables::Projection],
    schema_index: &doc::SchemaIndex<'_>,
    schema_shapes: &[schema::Shape],
    test_step: &tables::TestStep,
    errors: &mut tables::Errors,
) -> Option<flow::test_spec::Step> {
    let tables::TestStep {
        scope,
        collection,
        documents,
        partitions,
        step_index: _,
        step_type,
        test,
    } = test_step;

    // Map to slices of documents which are ingested or verified by this step.
    let (ingest, verify) = match step_type {
        TestStepType::Ingest => (documents.as_slice(), &[] as &[_]),
        TestStepType::Verify => (&[] as &[_], documents.as_slice()),
    };

    // Dereference test collection, returning early if not found.
    let collection = match reference::walk_reference(
        scope,
        &format!("test step {}", test.as_str()),
        "collection",
        collection,
        collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    ) {
        Some(s) => s,
        None => return None,
    };

    // Verify that any ingest documents conform to the collection schema.
    if schema_index.fetch(&collection.schema).is_none() {
        // Referential integrity error, which we've already reported.
    } else {
        let mut validator = doc::Validator::new(schema_index);
        for doc in ingest {
            if let Err(err) =
                doc::Validation::validate(&mut validator, &collection.schema, doc.clone())
                    .unwrap()
                    .ok()
            {
                Error::IngestDocInvalid(err).push(scope, errors);
            }
        }
    }

    // Verify that any verified documents are ordered correctly w.r.t.
    // the collection's key.
    if verify
        .iter()
        .tuple_windows()
        .map(|(lhs, rhs)| json::json_cmp_at(&collection.key, lhs, rhs))
        .any(|ord| ord == std::cmp::Ordering::Greater)
    {
        Error::TestVerifyOrder.push(scope, errors);
    }

    if let Some(selector) = partitions {
        let schema_shape = schema_shapes
            .iter()
            .find(|s| s.schema == collection.schema)
            .unwrap();

        let projections = projections
            .iter()
            .filter(|p| p.collection == collection.collection)
            .collect::<Vec<_>>();

        collection::walk_selector(
            scope,
            &collection.collection,
            &projections,
            schema_shape,
            selector,
            errors,
        );
    }

    Some(models::build::test_step_spec(test_step, collection))
}
