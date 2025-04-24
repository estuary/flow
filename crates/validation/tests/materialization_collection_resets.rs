mod common;

const MODEL_YAML: &str = include_str!("materialization_collection_resets.yaml");

#[test]
fn test_bindings_are_backfilled_or_disabled() {
    let outcome = common::run(MODEL_YAML, "{}");
    // Expect that the binding for a is backfilled,
    // and the binding for b is disabled.
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_on_incompatible_schema_change_disable_task() {
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
    // Expect that the whole task gets disabled, along with the binding for b.
    // The binding for a should remain enabled.
    insta::assert_debug_snapshot!(outcome);
}

#[test]
fn test_on_incompatible_schema_change_abort() {
    let outcome = common::run(
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
    // Expect that the whole task gets disabled, along with the binding for b.
    // The binding for a should remain enabled.
    insta::assert_debug_snapshot!(outcome);
}
