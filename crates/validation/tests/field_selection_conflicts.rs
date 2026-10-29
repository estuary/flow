mod common;

const MODEL_YAML: &str = include_str!("field_selection_conflicts.yaml");

#[test]
fn test_backfill_binding() {
    let (selections, errors) = common::run_selection(MODEL_YAML, "{}");
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

driver:
  storageMappings:
    the/:
      stores: [{ provider: GCS, bucket: estuary-trial }]
      recoveryStores: [{ provider: GCS, bucket: estuary-trial }]
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
