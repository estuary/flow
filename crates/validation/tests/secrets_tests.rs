mod common;

const SECRETS_YAML: &str = include_str!("secrets.yaml");

#[test]
fn secrets_publish_with_builtin_environments() {
    let actual = [("python", "py"), ("typescript", "ts")].map(|(language, extension)| {
        let dependencies = if language == "python" {
            "dependencies: {}"
        } else {
            ""
        };
        let outcome = common::run(
            SECRETS_YAML,
            &format!(
                r#"
test://example/catalog.yaml:
  collections:
    acmeCo/rollups:
      derive:
        using:
          connector: null
          {language}:
            module: rollups.{extension}
            environment: &environment {{ REGION: us-east-1 }}
        secrets:
          /credentials/token: null
          /environment/API_TOKEN: acmeCo/api-token
        shards:
          flags:
            enable-runtime-v2: "true"

test://example/rollups.{extension}: {language} module placeholder

driver:
  derivations:
    acmeCo/rollups:
      config:
        image: ghcr.io/estuary/derive-{language}:stable
        config:
          address: null
          module: {language} module placeholder
          {dependencies}
          environment: *environment
"#
            ),
        );
        let derivation = outcome
            .built_collections
            .iter()
            .find(|row| row.collection.as_str() == "acmeCo/rollups")
            .unwrap()
            .spec
            .as_ref()
            .unwrap()
            .derivation
            .as_ref()
            .unwrap();

        // The mock checks the Validate configuration; also check that the
        // built spec retains the environment and secrets for runtime startup.
        let config: serde_json::Value = serde_json::from_slice(&derivation.config_json).unwrap();
        (language, outcome.errors, config, derivation.secrets.clone())
    });
    insta::assert_debug_snapshot!(actual);
}

/// Typed built-in configurations are serialized for the same plaintext check
/// as raw connector configurations.
#[test]
fn secrets_reject_plaintext_values_of_builtin_environments() {
    let outcomes = [("python", "py"), ("typescript", "ts")].map(|(language, extension)| {
        let dependencies = if language == "python" {
            "dependencies: {}"
        } else {
            ""
        };
        common::run_errors(
            SECRETS_YAML,
            &format!(
                r#"
test://example/catalog.yaml:
  collections:
    acmeCo/rollups:
      derive:
        using:
          connector: null
          {language}:
            module: rollups.{extension}
            environment: &environment {{ API_TOKEN: plaintext-token }}
        shards:
          flags:
            enable-runtime-v2: "true"

test://example/rollups.{extension}: {language} module placeholder

driver:
  derivations:
    acmeCo/rollups:
      config:
        image: ghcr.io/estuary/derive-{language}:stable
        config:
          address: null
          module: {language} module placeholder
          {dependencies}
          environment: *environment
      configSchema:
        type: object
        properties:
          environment:
            type: object
            properties:
              API_TOKEN: {{ type: string, secret: true }}
"#,
            ),
        )
    });
    insta::assert_debug_snapshot!(outcomes);
}

/// The base fixture publishes cleanly: one task of each type draws a sibling
/// secret into a plaintext configuration (the capture without naming the V2
/// runtime flag at all), a disabled capture skips the connector and its
/// plaintext invariant, and a capture without a stanza is not subject to it.
#[test]
fn secrets_publish_across_task_types() {
    let outcome = common::run(SECRETS_YAML, "{}");
    insta::assert_debug_snapshot!(outcome.errors);
}

/// A stanza requires the V2 runtime, whose default differs by task type: a
/// capture opts out only by pinning the flag false, while a derivation or
/// materialization must opt in. `environment` of a built-in derivation
/// requires it in its own right.
#[test]
fn secrets_require_the_v2_runtime() {
    let errors = common::run_errors(
        SECRETS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/source-widgets:
      shards: { flags: { enable-runtime-v2: "false" } }
  collections:
    acmeCo/rollups:
      derive:
        using:
          connector: null
          python:
            module: rollups.py
            environment: &environment { REGION: us-east-1 }
        shards: { flags: null }
  materializations:
    acmeCo/dest-widgets:
      shards: { flags: { enable-runtime-v2: "false" } }

test://example/rollups.py: python module placeholder

driver:
  derivations:
    acmeCo/rollups:
      # Absent the V2 runtime, built-in derivations pin the `dev` image tag.
      config:
        image: ghcr.io/estuary/derive-python:dev
        config:
          address: null
          module: python module placeholder
          dependencies: {}
          environment: *environment
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

/// A `sops` key marks a wrapped configuration and is reserved in a plaintext
/// one, so its presence excludes `secrets` (`null` included, which the sniff's
/// own unit test covers). The SQLite derivation exercises that built-in
/// endpoints accept `secrets` without a model-level compatibility error.
#[test]
fn secrets_reject_incompatible_endpoints() {
    let errors = common::run_errors(
        SECRETS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/source-widgets:
      endpoint:
        connector:
          config: &config
            sops: { mac: "ENC[...]" }
  collections:
    acmeCo/rollups:
      derive:
        using:
          connector: null
          sqlite: { migrations: [] }
        transforms:
          - name: fromWidgets
            source: acmeCo/widgets
            shuffle: any
            lambda: select 1;
driver:
  captures:
    acmeCo/source-widgets:
      config:
        config: *config
  derivations:
    acmeCo/rollups:
      connectorType: SQLITE
      config:
        image: null
        config: null
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

/// Dekaf uses `secrets` like any other endpoint. Its stanza pointers and its
/// plaintext invariant both address the *inner* configuration -- the half of
/// the `{variant, config}` wrapper which a connector actually sees -- so a
/// resolved `/token` publishes and a plaintext one is rejected.
#[test]
fn secrets_resolve_a_dekaf_endpoint() {
    let outcomes = ["null", "not-a-secret"].map(|token| {
        common::run_errors(
            SECRETS_YAML,
            &format!(
                r#"
test://example/catalog.yaml:
  materializations:
    acmeCo/dest-widgets:
      endpoint:
        connector: null
        dekaf:
          variant: kafka
          config:
            strict_topic_names: true
            token: {token}
      secrets:
        /credentials/password: null
        /token: acmeCo/warehouse-password
driver:
  materializations:
    acmeCo/dest-widgets:
      connectorType: DEKAF
      config:
        image: null
        variant: kafka
        config:
          address: null
          strict_topic_names: true
          token: {token}
      configSchema:
        properties:
          address: null
          tunnel: null
          credentials: null
          strict_topic_names: {{ type: boolean }}
          token: {{ type: string, secret: true }}
"#
            ),
        )
    });
    insta::assert_debug_snapshot!(outcomes);
}

/// The plaintext invariant: no location the connector annotates `secret: true`
/// may hold a value. The capture's `password` is annotated in only one `oneOf`
/// branch -- which conservative schema traversal catches -- and its
/// configuration is deliberately schema-invalid besides, since a raw
/// configuration is validated only after its secrets merge. The
/// materialization's `tunnel` is annotated at the parent, which covers its
/// whole subtree.
#[test]
fn secrets_reject_plaintext_values() {
    let errors = common::run_errors(
        SECRETS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/source-widgets:
      endpoint:
        connector:
          config: &captureConfig
            address: 42
            credentials:
              user: alice
              password: hunter2
              token: invented-token
  collections:
    acmeCo/rollups:
      derive:
        using:
          connector:
            config: &deriveConfig
              credentials:
                token: invented-token
  materializations:
    acmeCo/dest-widgets:
      endpoint:
        connector:
          config: &materializeConfig
            tunnel: { host: bastion.example.com }
driver:
  captures:
    acmeCo/source-widgets:
      config:
        config: *captureConfig
  derivations:
    acmeCo/rollups:
      config:
        config: *deriveConfig
  materializations:
    acmeCo/dest-widgets:
      config:
        config: *materializeConfig
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

/// A task may use only secrets which sit directly beside it.
#[test]
fn secrets_must_be_siblings() {
    let errors = common::run_errors(
        SECRETS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/source-widgets:
      secrets:
        /credentials: acmeCo/nested/db-credentials
  collections:
    acmeCo/rollups:
      derive:
        secrets:
          /credentials/token: bobCo/api-token
  materializations:
    acmeCo/dest-widgets:
      secrets:
        /credentials/password: warehouse-password
"#,
    );
    insta::assert_debug_snapshot!(errors);
}
