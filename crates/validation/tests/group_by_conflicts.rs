mod common;

const MODEL_YAML: &str = include_str!("group_by_conflicts.yaml");

#[test]
fn test_backfill_binding() {
    let (selections, errors) = common::run_selection(MODEL_YAML, "{}");
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_backfill_binding_with_reset() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      # Expect we ignore the group-by key change, since we're resetting anyway.
      reset: true
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_abort() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: abort
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_disable_binding() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: disableBinding
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_disable_task() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: disableTask
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_reenable_with_backfill() {
    // Backfilling a re-enabled binding allows the changed group-by.
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: disableBinding
      bindings:
        - source: the/collection
          resource:
            _meta: { path: [a] }
            table: a
          fields:
            recommended: 1
          backfill: 1

driver:
  liveMaterializations:
    the/materialization:
      bindings:
        - source: the/collection
          resource:
            _meta: { path: [a] }
            table: a
          fields:
            recommended: 1
          disable: true
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_noop_with_manual_group_by() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: abort
      bindings:
        - source: the/collection
          resource:
            _meta: { path: [a] }
            table: a
          fields:
            groupBy: [f_one] # Override to use old collection key.
            recommended: 1
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_noop_with_delta_binding() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
driver:
  liveMaterializations:
    the/materialization:
      lastFields:
        - keys: [] # Marks as delta-updates.
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_manual_group_by_change() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    the/collection:
      key: [/f_one] # Not changing.

  materializations:
    the/materialization:
      onIncompatibleSchemaChange: abort
      bindings:
        - source: the/collection
          resource:
            _meta: { path: [a] }
            table: a
          fields:
            groupBy: [f_two] # Changed from f_one.
            recommended: 1
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}
