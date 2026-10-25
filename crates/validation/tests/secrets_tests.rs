mod common;

const SECRETS_YAML: &str = include_str!("secrets.yaml");

#[test]
fn secrets_publish_across_task_types() {
    let outcome = common::run(SECRETS_YAML, "{}");
    insta::assert_debug_snapshot!(outcome.errors);
}

/// A stanza requires the V2 runtime, whose default differs by task type: a
/// capture opts out only by pinning the flag false, while a derivation or
/// materialization must opt in.
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
        acmeCo/warehouse-password: /token
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
        acmeCo/db-credentials: null
        acmeCo/nested/db-credentials: /credentials
  collections:
    acmeCo/rollups:
      derive:
        secrets:
          acmeCo/api-token: null
          bobCo/api-token: /credentials/token
  materializations:
    acmeCo/dest-widgets:
      secrets:
        acmeCo/warehouse-password: null
        warehouse-password: /credentials/password
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

/// Each stanza entry is format-checked and its pointer dry-run over the
/// configuration, in secret-name order.
#[test]
fn secrets_reject_malformed_stanza_entries() {
    let errors = common::run_errors(
        SECRETS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/source-widgets:
      endpoint:
        connector:
          config: &config
            address: db.example.com
            credentials: { user: alice }
      secrets:
        acmeCo/db-credentials: null
        acmeCo/a-new: /new
        acmeCo/b-deep: /new/deep
        acmeCo/c-replace: /credentials/user
        acmeCo/d-under: /credentials/user/name
        acmeCo/bad name: /credentials
        acmeCo/no-slash: credentials/password
        acmeCo/empty-token: /credentials//password
        acmeCo/through-scalar: /address/port
        acmeCo/sparse: /credentials/tokens/1000000000
driver:
  captures:
    acmeCo/source-widgets:
      config:
        config: *config
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

/// The image rule: a task running an explicit connector image may also use
/// secrets named `<prefix>/connectors/<image repository>/<leaf>`, which is how
/// a vendor's OAuth client secret reaches every tenant using that connector.
/// One task of each type takes one, alongside the sibling it already had.
#[test]
fn secrets_admit_image_rule_for_connector_images() {
    let outcome = common::run(
        SECRETS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/source-widgets:
      secrets:
        vendorCo/oauth/connectors/source/image/oauth-client: /credentials/token
  collections:
    acmeCo/rollups:
      derive:
        secrets:
          vendorCo/oauth/connectors/derive/image/oauth-client: /credentials/user
  materializations:
    acmeCo/dest-widgets:
      secrets:
        vendorCo/oauth/connectors/dest/image/oauth-client: /credentials/user
"#,
    );

    // The stanza publishes into each built spec, ready for the runtime to
    // resolve -- which is where the image is held to what it actually declares,
    // including the location of each secret.
    let capture = outcome
        .built_captures
        .iter()
        .find(|row| row.capture.as_str() == "acmeCo/source-widgets")
        .unwrap();

    insta::assert_debug_snapshot!((
        outcome.errors,
        capture.spec.as_ref().unwrap().secrets.clone(),
    ));
}

/// The image rule reaches only explicit connector images. An endpoint with no
/// image of its own -- Local, Dekaf, or a built-in derivation -- stays
/// sibling-only, even for a secret named exactly as the rule would have it.
#[test]
fn secrets_image_rule_excludes_imageless_endpoints() {
    let errors = common::run_errors(
        SECRETS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/source-widgets:
      endpoint:
        connector: null
        local:
          command: [source-widgets]
          config: { address: db.example.com }
      secrets:
        vendorCo/oauth/connectors/source/image/oauth-client: /credentials/token
  collections:
    acmeCo/rollups:
      derive:
        using:
          connector: null
          python:
            module: rollups.py
        secrets:
          # Named for the image a built-in derivation actually runs.
          vendorCo/oauth/connectors/ghcr.io/estuary/derive-python/oauth-client: /environment/TOKEN
  materializations:
    acmeCo/dest-widgets:
      endpoint:
        connector: null
        dekaf:
          variant: kafka
          config: { token: null }
      secrets:
        vendorCo/oauth/connectors/ghcr.io/estuary/dekaf-kafka/oauth-client: /token

test://example/rollups.py: python module placeholder
"#,
    );
    insta::assert_debug_snapshot!(errors);
}
