use super::{collection, errors::Error, indexed, reference, schema, walk_transition, Scope};
use flow::test_spec::step::Type as StepType;
use proto_flow::flow;
use tables::EitherOrBoth as EOB;

pub fn walk_all_tests(
    pub_id: models::Id,
    build_id: models::Id,
    draft_tests: &tables::DraftTests,
    live_tests: &tables::LiveTests,
    built_collections: &tables::BuiltCollections,
    dependencies: &tables::Dependencies<'_>,
    errors: &mut tables::Errors,
) -> tables::BuiltTests {
    // Outer join of live and draft tests.
    let it = live_tests.outer_join(draft_tests.iter().map(|r| (&r.test, r)), |eob| match eob {
        EOB::Left(live) => Some(EOB::Left(live)),
        EOB::Right((_test, draft)) => Some(EOB::Right(draft)),
        EOB::Both(live, (_test, draft)) => Some(EOB::Both(live, draft)),
    });

    it.filter_map(|eob| {
        walk_test(
            pub_id,
            build_id,
            eob,
            built_collections,
            dependencies,
            errors,
        )
    })
    .collect()
}

fn walk_test(
    pub_id: models::Id,
    build_id: models::Id,
    eob: EOB<&tables::LiveTest, &tables::DraftTest>,
    built_collections: &tables::BuiltCollections,
    dependencies: &tables::Dependencies<'_>,
    errors: &mut tables::Errors,
) -> Option<tables::BuiltTest> {
    let (
        test,
        scope,
        model,
        control_id,
        _data_plane_id,
        expect_pub_id,
        expect_build_id,
        live_spec,
        is_touch,
    ) = match walk_transition(pub_id, build_id, Some(models::Id::zero()), eob, errors) {
        Ok(ok) => ok,
        Err(built) => return Some(built),
    };
    let scope = Scope::new(scope);

    let models::TestDef { steps, .. } = model;

    indexed::walk_name(scope, "test", test, models::Test::regex(), errors);

    // Map steps into built steps.
    let built_steps: Vec<_> = steps
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

    let built_spec = flow::TestSpec {
        name: test.to_string(),
        steps: built_steps,
    };

    let dependency_hash = dependencies.compute_hash(model);
    Some(tables::BuiltTest {
        test: test.clone(),
        scope: scope.flatten(),
        control_id,
        expect_pub_id,
        expect_build_id,
        model: Some(model.clone()),
        spec: Some(built_spec),
        previous_spec: live_spec.cloned(),
        is_touch,
        dependency_hash,
    })
}

pub fn walk_test_step<'a>(
    scope: Scope<'a>,
    built_collections: &'a tables::BuiltCollections,
    step_index: usize,
    test_step: &'a models::TestStep,
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
                    not_before,
                    not_after,
                }),
            documents,
        }) => {
            if not_before.is_some() {
                Error::TestStepNotBeforeAfter.push(scope.push_prop("notBefore"), errors);
            }
            if not_after.is_some() {
                Error::TestStepNotBeforeAfter.push(scope.push_prop("notAfter"), errors);
            }

            (
                StepType::Verify,
                collection,
                description,
                documents,
                partitions.as_ref(),
            )
        }
    };
    let scope = match step_type {
        StepType::Ingest => scope.push_prop("ingest"),
        StepType::Verify => scope.push_prop("verify"),
    };

    let (spec, _) = reference::walk_reference(
        scope.push_prop("collection"),
        "this test step",
        collection,
        built_collections,
        errors,
    )?;
    let documents = serde_json::from_str::<Vec<serde_json::Value>>(documents.get())
        .expect("a documents fixture is verified to be an array of objects during load");

    if let StepType::Ingest = step_type {
        // Require that all documents validate for both writes and reads.
        let mut write_schema = schema::Schema::new(&spec.write_schema_json).ok();
        let mut read_schema = Some(&spec.read_schema_json)
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
    } else if let Ok(key) =
        extractors::for_key(&spec.key, &spec.projections, &doc::SerPolicy::noop())
    {
        // Verify that any verified documents are ordered correctly w.r.t.
        // the collection's key.
        for (doc_index, (lhs, rhs)) in documents.windows(2).map(|p| (&p[0], &p[1])).enumerate() {
            if doc::Extractor::compare_key(&key, lhs, rhs).is_gt() {
                Error::TestVerifyOrder
                    .push(scope.push_prop("documents").push_item(doc_index), errors);
            }
        }
    };

    // Verify a provided partition selector is valid.
    if let Some(selector) = selector {
        collection::walk_selector(
            scope.push_prop("collection").push_prop("partitions"),
            &spec,
            &selector,
            errors,
        );
    }

    Some(flow::test_spec::Step {
        step_type: step_type as i32,
        step_index: step_index as u32,
        step_scope: scope.flatten().to_string(),
        collection: collection.to_string(),
        docs_json_vec: documents.into_iter().map(|d| d.to_string()).collect(),
        partitions: Some(assemble::journal_selector(&spec, selector)),
        description: description.clone(),
    })
}
