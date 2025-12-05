use std::sync::Arc;

/// Keychain represents a specific KMS key which `sops` will use.
#[derive(serde::Deserialize, schemars::JsonSchema, Debug, Clone)]
pub enum Keychain {
    // Encrypt via AGE.
    Age(String),
    // Encrypt via AWS KMS.
    Aws(String),
    // Encrypt via GCP KMS.
    Gcp(String),
}

/// A request to encrypt a subset of values in `config`. The values to be encrypted are derived from
/// the provided `schema` using the `secret` annotation. Any field with `"secret": true` in the
/// schema will be encrypted by extending its property name with `_sops`.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct Request {
    /// A JSON schema that is used both for validation of the config, and to identify the specific
    /// properties that should be encrypted. Any property that has a `"secret": true` annotation
    /// will be encrypted, and all others will be left as plain text. Encrypted properties will
    /// have a suffix added to its name (e.g. "api_key" -> "api_key_sops").
    pub schema: serde_json::Value,
    /// The plain text configuration to encrypt. This must validate against the provided JSON
    /// schema. If provided in YAML format, then all comments will be stripped.
    pub config: serde_json::Value,
    /// Keychain to use for this encryption request.
    #[serde(default)]
    pub keychain: Option<Keychain>,
}

/// Error that describes a failed request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to build json schema")]
    SchemaBuild(#[from] ::json::schema::build::Errors<doc::Annotation>),
    #[error("failed to verify json schema references")]
    SchemaIndex(#[from] ::json::schema::index::Error),
    #[error("failed to validate config against schema:\n {}", serde_json::to_string_pretty(&.0).unwrap())]
    FailedValidation(serde_json::Value),
    #[error("`sops` process failed:\n{0}")]
    SopsFailed(String),
    #[error("failed to parse sops output")]
    SopsOutput(#[source] serde_json::Error),
    #[error("failed to run `sops`")]
    SopsError(#[source] std::io::Error),
}

// Given an input schema and config instance, validate the config and re-write it
// to add `_sops` suffixes to all secret fields.
fn prepare_config_for_sops(
    schema: &serde_json::Value,
    config: serde_json::Value,
) -> Result<serde_json::Value, Error> {
    let schema_url = url::Url::parse("request://schema").unwrap();
    let schema = json::schema::build::<doc::Annotation>(&schema_url, schema)?;

    let mut builder = json::schema::index::Builder::new();
    builder.add(&schema)?;
    builder.verify_references()?;
    let index = builder.into_index();

    let mut validator = json::validator::Validator::new(&index);
    let (is_valid, outcomes) = validator.validate(&schema, &config, |outcome| {
        // Collect secret annotations and errors, but no other annotations.
        match &outcome {
            json::validator::Outcome::Annotation(doc::Annotation::Secret(true)) => Some(outcome),
            json::validator::Outcome::Annotation(_) => None,
            _ => Some(outcome),
        }
    });

    if !is_valid {
        return Err(Error::FailedValidation(
            json::validator::build_basic_output(&config, &outcomes),
        ));
    }

    let mut tape: Vec<i32> = outcomes
        .into_iter()
        .map(|outcome| outcome.tape_index)
        .collect();
    tape.sort();
    tape.dedup();

    let mut tape_index = 0i32;

    let (config, _) = apply_sops_suffix(&mut tape.as_slice(), &mut tape_index, config, false);
    Ok(config)
}

fn apply_sops_suffix(
    tape: &mut &[i32],
    tape_index: &mut i32,
    node: serde_json::Value,
    is_secret: bool,
) -> (serde_json::Value, bool) {
    let is_secret = if matches!(tape.get(0), Some(i) if i == tape_index) {
        *tape = &tape[1..];
        true
    } else {
        is_secret
    };
    *tape_index += 1; // Consume self.

    match node {
        serde_json::Value::Object(fields) => {
            let mut out = serde_json::Map::<String, serde_json::Value>::with_capacity(fields.len());

            for (property, child) in fields {
                let (child, child_is_secret) =
                    apply_sops_suffix(tape, tape_index, child, is_secret);

                if child_is_secret {
                    out.insert(format!("{}_sops", property), child);
                } else {
                    out.insert(property, child);
                }
            }
            // Non-leaf objects are never themselves suffixed.
            (serde_json::Value::Object(out), false)
        }
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());

            for item in items {
                let (item, _child_is_secret) = apply_sops_suffix(tape, tape_index, item, is_secret);
                out.push(item);
            }
            // Non-leaf arrays are never themselves suffixed.
            (serde_json::Value::Array(out), false)
        }
        scalar => (scalar, is_secret),
    }
}

async fn invoke_sops(
    keychain: &Keychain,
    config: serde_json::Value,
) -> Result<Box<serde_json::value::RawValue>, Error> {
    let mut child = async_process::Command::new("sops");

    match keychain {
        Keychain::Age(age_key) => {
            child.args(&["--age", age_key]);
        }
        Keychain::Aws(aws_key) => {
            child.args(&["--kms", aws_key]);
        }
        Keychain::Gcp(gcp_key) => {
            child.args(&["--gcp-kms", gcp_key]);
        }
    }
    child.args(&[
        "--encrypt",
        "--encrypted-suffix=_sops",
        "--input-type=json",
        "--output-type=json",
        "/dev/stdin",
    ]);

    let config = serde_json::to_vec(&config).unwrap();
    let std::process::Output {
        status,
        stdout,
        stderr,
    } = async_process::input_output(&mut child, &config)
        .await
        .map_err(Error::SopsError)?;

    if status.success() {
        let config: Box<serde_json::value::RawValue> =
            serde_json::from_slice(&stdout).map_err(Error::SopsOutput)?;

        Ok(config)
    } else {
        Err(Error::SopsFailed(
            String::from_utf8_lossy(&stderr).into_owned(),
        ))
    }
}

#[axum::debug_handler]
#[tracing::instrument(skip(schema, config), err(level = tracing::Level::WARN))]
pub async fn encrypt_config(
    axum::extract::State(default_keychain): axum::extract::State<Arc<Keychain>>,
    axum::Json(Request {
        schema,
        config,
        keychain,
    }): axum::Json<Request>,
) -> Result<axum::Json<Box<serde_json::value::RawValue>>, Error> {
    let keychain = if let Some(keychain) = &keychain {
        keychain
    } else {
        &default_keychain
    };

    let config = prepare_config_for_sops(&schema, config)?;
    let encrypted = invoke_sops(keychain, config).await?;

    Ok(axum::Json(encrypted))
}

impl axum::response::IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let error = format!("{:?}", anyhow::Error::new(self));
        (axum::http::StatusCode::BAD_REQUEST, error).into_response()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_prepare_config_basic_secrets() {
        // Tests simple secrets, nested secrets, and multiple secrets together
        let schema = json!({
            "type": "object",
            "properties": {
                "api_key": { "type": "string", "secret": true },
                "client_id": { "type": "string" },
                "credentials": {
                    "type": "object",
                    "properties": {
                        "password": { "type": "string", "secret": true },
                        "token": { "type": "number", "secret": true },
                        "email": { "type": "string" }
                    }
                }
            }
        });
        let config = json!({
            "api_key": "key123",
            "client_id": "public789",
            "credentials": {
                "password": "hunter2",
                "token": 42,
                "email": "user@example.com"
            }
        });

        insta::assert_json_snapshot!(prepare_config_for_sops(&schema, config).unwrap(), @r###"
        {
          "api_key_sops": "key123",
          "client_id": "public789",
          "credentials": {
            "email": "user@example.com",
            "password_sops": "hunter2",
            "token_sops": 42
          }
        }
        "###);
    }

    #[test]
    fn test_prepare_config_arrays() {
        // Tests both array of scalars and array of objects with secrets
        let schema = json!({
            "type": "object",
            "properties": {
                "tokens": {
                    "type": "array",
                    "items": { "type": "string", "secret": true }
                },
                "connections": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": { "type": "string" },
                            "password": { "type": "string", "secret": true }
                        }
                    }
                }
            }
        });
        let config = json!({
            "tokens": ["token1", "token2"],
            "connections": [
                { "name": "db1", "password": "pass1" },
                { "name": "db2", "password": "pass2" }
            ]
        });

        insta::assert_json_snapshot!(prepare_config_for_sops(&schema, config).unwrap(), @r###"
        {
          "connections": [
            {
              "name": "db1",
              "password_sops": "pass1"
            },
            {
              "name": "db2",
              "password_sops": "pass2"
            }
          ],
          "tokens": [
            "token1",
            "token2"
          ]
        }
        "###);
    }

    #[test]
    fn test_prepare_config_validation_failure() {
        let schema = json!({
            "type": "object",
            "properties": {
                "count": { "type": "integer" }
            },
            "required": ["count"]
        });
        let config = json!({ "count": "not_an_integer" });

        insta::assert_debug_snapshot!(prepare_config_for_sops(&schema, config).unwrap_err(), @r#"
        FailedValidation(
            Array [
                Object {
                    "absoluteKeywordLocation": String("request://schema#/properties/count"),
                    "detail": String("Type mismatch: expected a integer"),
                    "instanceLocation": String("/count"),
                    "instanceValue": String("not_an_integer"),
                },
            ],
        )
        "#);
    }

    #[test]
    fn test_prepare_config_secret_inheritance() {
        // Tests that secret: true on parent marks all children as secret
        // Covers objects, arrays, nested hierarchies, and mixed structures
        let schema = json!({
            "type": "object",
            "properties": {
                "secret_obj": {
                    "type": "object",
                    "secret": true,
                    "properties": {
                        "username": { "type": "string" },
                        "nested": {
                            "type": "object",
                            "properties": {
                                "value": { "type": "string" }
                            }
                        }
                    }
                },
                "secret_array": {
                    "type": "array",
                    "secret": true,
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "integer" }
                        }
                    }
                },
                "public": { "type": "string" }
            }
        });
        let config = json!({
            "secret_obj": {
                "username": "admin",
                "nested": { "value": "deep" }
            },
            "secret_array": [
                { "id": 1 },
                { "id": 2 }
            ],
            "public": "visible"
        });

        insta::assert_json_snapshot!(prepare_config_for_sops(&schema, config).unwrap(), @r###"
        {
          "public": "visible",
          "secret_array": [
            {
              "id_sops": 1
            },
            {
              "id_sops": 2
            }
          ],
          "secret_obj": {
            "nested": {
              "value_sops": "deep"
            },
            "username_sops": "admin"
          }
        }
        "###);
    }

    // Age keypair generated for testing only. DO NOT use in production.
    const TEST_AGE_KEY: &str = "age1z3nuz2xvzwrsjcrx0ewwa0cy0422m6dcunv87lzkg828j6whs5astjfuxg";

    #[tokio::test]
    async fn test_invoke_sops_various_structures() {
        // Test comprehensive fixture covering: scalars (string/number/bool), nested objects,
        // arrays, deep nesting, and mixed public/secret fields
        let config = json!({
            "string_sops": "secret_string",
            "number_sops": 3.14,
            "bool_sops": true,
            "public_scalar": "visible",
            "nested": {
                "host": "localhost",
                "password_sops": "hunter2",
                "port": 5432,
                "deep": {
                    "api_key_sops": "secret123",
                    "public_data": 42
                }
            },
            "array_sops": ["token1", "token2"],
            "public_array": [1, 2, 3],
            "array_of_objects": [
                { "name": "db1", "password_sops": "pass1" },
                { "name": "db2", "password_sops": "pass2" }
            ]
        });

        let result = invoke_sops(&Keychain::Age(TEST_AGE_KEY.to_string()), config)
            .await
            .unwrap();

        let encrypted: serde_json::Value = serde_json::from_str(result.get()).unwrap();

        // Verify public fields remain unchanged
        assert_eq!(encrypted["public_scalar"], "visible");
        assert_eq!(encrypted["nested"]["host"], "localhost");
        assert_eq!(encrypted["nested"]["port"], 5432);
        assert_eq!(encrypted["nested"]["deep"]["public_data"], 42);
        assert_eq!(encrypted["public_array"], json!([1, 2, 3]));
        assert_eq!(encrypted["array_of_objects"][0]["name"], "db1");
        assert_eq!(encrypted["array_of_objects"][1]["name"], "db2");

        // Verify secret fields are encrypted (all become strings starting with ENC[)
        assert!(
            encrypted["string_sops"]
                .as_str()
                .unwrap()
                .starts_with("ENC[")
        );
        assert!(
            encrypted["number_sops"]
                .as_str()
                .unwrap()
                .starts_with("ENC[")
        );
        assert!(encrypted["bool_sops"].as_str().unwrap().starts_with("ENC["));
        assert!(
            encrypted["nested"]["password_sops"]
                .as_str()
                .unwrap()
                .starts_with("ENC[")
        );
        assert!(
            encrypted["nested"]["deep"]["api_key_sops"]
                .as_str()
                .unwrap()
                .starts_with("ENC[")
        );

        // Verify secret arrays
        let array = encrypted["array_sops"].as_array().unwrap();
        for token in array {
            assert!(token.as_str().unwrap().starts_with("ENC["));
        }

        assert!(
            encrypted["array_of_objects"][0]["password_sops"]
                .as_str()
                .unwrap()
                .starts_with("ENC[")
        );
        assert!(
            encrypted["array_of_objects"][1]["password_sops"]
                .as_str()
                .unwrap()
                .starts_with("ENC[")
        );

        // Verify sops metadata
        assert!(encrypted["sops"].is_object());
        assert!(encrypted["sops"]["age"].is_array());
        assert!(encrypted["sops"]["version"].is_string());
    }
}
