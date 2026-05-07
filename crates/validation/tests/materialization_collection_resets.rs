mod common;

const MODEL_YAML: &str = include_str!("materialization_collection_resets.yaml");

#[test]
fn test_bindings_are_backfilled_on_reset() {
    let outcome = common::run(MODEL_YAML, "{}");
    assert!(outcome.errors.is_empty());
    assert!(outcome.errors_draft.is_empty());
    // Expect that both bindings are backfilled (resets cascade as
    // backfills for all onIncompatibleSchemaChange variants except abort).
    insta::assert_debug_snapshot!(outcome.built_materializations);
}

#[test]
fn test_reset_backfills_except_on_abort() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    testing/materialize:
      bindings:
      - source: testing/reset/a
        onIncompatibleSchemaChange: disableTask
        resource:
          _meta: { path: [a] }
          table: a
      - source: testing/reset/b
        onIncompatibleSchemaChange: disableBinding
        resource:
          _meta: { path: [b] }
          table: b
        "#,
    );
    assert!(outcome.errors.is_empty());
    assert!(outcome.errors_draft.is_empty());
    // Expect that both bindings are backfilled (resets cascade as
    // backfills for all onIncompatibleSchemaChange variants except abort).
    insta::assert_debug_snapshot!(
        "reset_backfills_for_disable_task_and_binding",
        outcome.built_materializations
    );

    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    testing/materialize:
      onIncompatibleSchemaChange: abort
      bindings:
      - source: testing/reset/a
        onIncompatibleSchemaChange: disableBinding
        resource:
          _meta: { path: [a] }
          table: a
      - source: testing/reset/b
        # Inherit the top-level `onIncompatibleSchemaChange: abort`
        resource:
          _meta: { path: [b] }
          table: b
        "#,
    );
    // Binding b inherits the top-level `abort` and errors because its source was reset.
    // The materialization is discarded (no `BuiltMaterialization`) once any binding errors,
    // so binding a's in-memory backfill is not visible in the selections snapshot.
    insta::assert_debug_snapshot!("reset_errors_on_abort", (selections, errors));
}
