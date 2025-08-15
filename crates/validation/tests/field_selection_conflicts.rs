use proto_flow::flow;

mod common;

const MODEL_YAML: &str = include_str!("field_selection_conflicts.yaml");
#[test]
fn test_backfill_binding() {
    let (selections, errors) = run("{}");
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_abort() {
    let (selections, errors) = run(r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: abort
"#);
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_disable_binding() {
    let (selections, errors) = run(r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: disableBinding
"#);
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_disable_task() {
    let (selections, errors) = run(r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: disableTask
"#);
    insta::assert_debug_snapshot!((selections, errors));
}

fn run(
    patch_yaml: &str,
) -> (
    Vec<(
        Vec<flow::FieldSelection>,
        Option<models::MaterializationDef>,
        Vec<String>,
    )>,
    tables::Errors,
) {
    let outcome = common::run(MODEL_YAML, patch_yaml);

    let mut errors = outcome.errors_draft;
    errors.extend(outcome.errors.into_iter());

    let selections: Vec<_> = outcome
        .built_materializations
        .into_iter()
        .map(
            |tables::BuiltMaterialization {
                 spec,
                 model,
                 model_fixes: fixes,
                 ..
             }| {
                (
                    std::mem::take(&mut spec.unwrap().bindings)
                        .into_iter()
                        .filter_map(|b| b.field_selection)
                        .collect::<Vec<_>>(),
                    model,
                    fixes,
                )
            },
        )
        .collect();

    (selections, errors)
}
