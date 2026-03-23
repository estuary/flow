mod common;

const MODEL_YAML: &str = include_str!("model.yaml");

#[test]
fn test_valid_trigger() {
    let outcome = common::run(
        MODEL_YAML,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      triggers:
        config:
          - url: "https://example.com/webhook"
            method: POST
            headers:
              Authorization: "Bearer my-secret-token"
              X-Tenant-Id: "tenant-abc"
            payloadTemplate: |
              {
                "materialization": "{{materialization_name}}",
                "auth": "{{headers.Authorization}}",
                "tenant": "{{headers.X-Tenant-Id}}",
                "collections": [{{#each collection_names}}"{{this}}"{{#unless @last}}, {{/unless}}{{/each}}]
              }
            timeout: 60s
            maxAttempts: 5
"#,
    );
    assert!(
        outcome.errors.is_empty(),
        "expected no errors, got: {:?}",
        outcome.errors
    );

    let built_mat = outcome
        .built_materializations
        .iter()
        .find(|m| m.materialization.as_ref() == "testing/db-views")
        .expect("should have built materialization");
    let spec = built_mat.spec.as_ref().unwrap();
    let triggers: serde_json::Value = serde_json::from_slice(&spec.triggers_json).unwrap();
    insta::assert_json_snapshot!(triggers);
}

/// A single trigger with multiple problems: invalid URL, zero timeout, and
/// unknown template variable.
#[test]
fn test_trigger_validation_errors() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      triggers:
        config:
          - url: "not a url"
            payloadTemplate: '{"bogus": "{{bogus}}"}'
            timeout: 0s
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_template_renders_invalid_json() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      triggers:
        config:
          - url: "https://example.com/webhook"
            payloadTemplate: 'not valid json {{materialization_name}}'
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

#[test]
fn test_template_syntax_error() {
    let errors = common::run_errors(
        MODEL_YAML,
        r#"
test://example/db-views:
  materializations:
    testing/db-views:
      triggers:
        config:
          - url: "https://example.com/webhook"
            payloadTemplate: '{"unclosed": "{{#each collection_names}}"}'
"#,
    );
    insta::assert_debug_snapshot!(errors);
}
