use super::{collection, errors::Error, indexed, reference, schema};
use itertools::Itertools;
use proto_flow::flow;
use superslice::Ext;

pub fn walk_all_test_steps(
    built_collections: &[tables::BuiltCollection],
    imports: &[tables::Import],
    resources: &[tables::Resource],
    schema_shapes: &[schema::Shape],
    test_steps: &[tables::TestStep],
    errors: &mut tables::Errors,
) -> tables::BuiltTests {
    let mut built_tests = tables::BuiltTests::new();

    for (test, steps) in &test_steps.iter().group_by(|s| &s.test) {
        let steps = steps.collect::<Vec<_>>();
        let scope = &steps[0].scope;

        indexed::walk_name(scope, "test", test, models::Test::regex(), errors);

        let steps: Vec<_> = steps
            .into_iter()
            .map(|test_step| {
                walk_test_step(
                    built_collections,
                    imports,
                    resources,
                    schema_shapes,
                    test_step,
                    errors,
                )
                .into_iter()
            })
            .flatten()
            .collect();

        built_tests.insert_row(
            scope,
            test,
            flow::TestSpec {
                test: test.to_string(),
                steps,
            },
        );
    }

    indexed::walk_duplicates(
        test_steps
            .iter()
            .filter(|s| s.step_index == 0)
            .map(|s| ("test", s.test.as_str(), &s.scope)),
        errors,
    );

    built_tests
}

pub fn walk_test_step(
    built_collections: &[tables::BuiltCollection],
    imports: &[tables::Import],
    resources: &[tables::Resource],
    schema_shapes: &[schema::Shape],
    test_step: &tables::TestStep,
    errors: &mut tables::Errors,
) -> Option<flow::test_spec::Step> {
    let tables::TestStep {
        scope,
        test,
        step_index,
        spec,
        documents,
    } = test_step;

    let documents = &resources[resources.equal_range_by_key(&documents, |r| &r.resource)];
    let documents: Vec<serde_json::Value> = match documents.first() {
        Some(tables::Resource {
            content_type: flow::ContentType::DocumentsFixture,
            content_dom,
            ..
        }) => serde_json::from_str(content_dom.get()).expect(
            "a DocumentsFixture resource is verified to be an array of objects during load",
        ),
        _ => Vec::new(),
    };

    // Map to slices of documents which are ingested or verified by this step.
    let (collection, ingest, verify) = match spec {
        models::TestStep::Ingest(ingest) => (&ingest.collection, documents.as_slice(), &[] as &[_]),
        models::TestStep::Verify(verify) => (&verify.collection, &[] as &[_], documents.as_slice()),
    };

    // Dereference test collection, returning early if not found.
    let collection = match reference::walk_reference(
        scope,
        &format!("test {} step {}", test.as_str(), step_index),
        "collection",
        collection,
        built_collections,
        |c| (&c.collection, &c.scope),
        imports,
        errors,
    ) {
        Some(s) => s,
        None => return None,
    };
    // Pluck the collection schema Shape, which must exist but could be a placeholder.
    let shape = &schema_shapes[schema_shapes
        .equal_range_by_key(&collection.spec.schema_uri.as_str(), |s| s.schema.as_str())][0];

    // Verify that any ingest documents conform to the collection schema.
    if shape.index.fetch(&shape.schema).is_none() {
        // Referential integrity error, which we've already reported.
    } else {
        let mut validator = doc::Validator::new(&shape.index);
        for doc in ingest {
            if let Err(err) = doc::Validation::validate(&mut validator, &shape.schema, doc)
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
        .map(|(lhs, rhs)| json::json_cmp_at(&collection.spec.key_ptrs, lhs, rhs))
        .any(|ord| ord == std::cmp::Ordering::Greater)
    {
        Error::TestVerifyOrder.push(scope, errors);
    }

    // Verify a provided partition selector is valid.
    if let models::TestStep::Verify(models::TestStepVerify {
        partitions: Some(selector),
        ..
    }) = spec
    {
        collection::walk_selector(scope, &collection.spec, &selector, errors);
    }

    Some(assemble::test_step_spec(test_step, &documents))
}
