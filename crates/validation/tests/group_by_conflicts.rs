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

driver:
  storageMappings:
    the/:
      stores: [{ provider: GCS, bucket: estuary-trial }]
      recoveryStores: [{ provider: GCS, bucket: durable-recovery }]
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
fn test_trial_storage_disables_binding() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
driver:
  storageMappings:
    the/:
      stores: [{ provider: GCS, bucket: estuary-trial, prefix: collection-data/ }]
      recoveryStores: [{ provider: GCS, bucket: estuary-trial }]
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_trial_storage_with_task_backfill() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: backfill

driver:
  storageMappings:
    the/:
      stores: [{ provider: GCS, bucket: estuary-trial, prefix: collection-data/ }]
      recoveryStores: [{ provider: GCS, bucket: estuary-trial }]
"#,
    );
    insta::assert_debug_snapshot!((selections, errors));
}

#[test]
fn test_trial_storage_with_binding_backfill() {
    let (selections, errors) = common::run_selection(
        MODEL_YAML,
        r#"
test://example/catalog.yaml:
  materializations:
    the/materialization:
      onIncompatibleSchemaChange: disableTask
      bindings:
        - source: the/collection
          resource:
            _meta: { path: [a] }
            table: a
          fields:
            recommended: 1
          onIncompatibleSchemaChange: backfill

driver:
  storageMappings:
    the/:
      stores: [{ provider: GCS, bucket: estuary-trial, prefix: collection-data/ }]
      recoveryStores: [{ provider: GCS, bucket: estuary-trial }]
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
  storageMappings:
    the/:
      stores: [{ provider: GCS, bucket: estuary-trial, prefix: collection-data/ }]
      recoveryStores: [{ provider: GCS, bucket: estuary-trial }]
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

#[test]
fn test_durable_primary_with_trial_secondary() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
driver:
  storageMappings:
    the/:
      stores:
        - { provider: GCS, bucket: durable-primary }
        - { provider: GCS, bucket: estuary-trial }
      recoveryStores: [{ provider: GCS, bucket: estuary-trial }]
"#,
    );
    assert!(outcome.errors.is_empty());
    assert!(outcome.errors_draft.is_empty());
    let built = &outcome.built_materializations[0];
    let binding = &built.model.as_ref().unwrap().bindings[0];
    insta::assert_debug_snapshot!((
        binding.disable,
        binding.backfill,
        &built.auto_disabled_bindings
    ));
}
