//! Checks of a connector's unsealed configurations against the schemas of its
//! Spec response, which produce warnings and never errors: a connector may
//! advertise a stricter schema than it accepts, as for legacy formats.
//!
//! Configurations carry merged secrets, so a check describes only the
//! locations of errors and never the values of the configuration.

/// Top-level properties of a built-in connector's endpoint configuration which
/// carry the user's code and declared `spec`. They're removed before the
/// configuration reaches user code, and aren't described by the declared schema.
/// These mirror `validation::builtin::{PYTHON_SENTINEL, TYPESCRIPT_SENTINEL}`,
/// which this crate doesn't depend upon.
const BUILTIN_SENTINELS: &[&str] = &["_python", "_typescript"];
/// Top-level property of a resource configuration in which validation records
/// the binding's resource path. Connectors needn't describe it.
const RESOURCE_META: &str = "_meta";
/// At most this many invalid bindings, and errors of each configuration, are
/// listed in a warning, which otherwise counts them.
const MAX_LISTED: usize = 5;

/// An endpoint configuration, or the resource configuration of a binding,
/// which doesn't validate against its schema.
pub(crate) struct Invalid {
    /// Label of the invalid binding, or None for the endpoint configuration.
    pub binding: Option<String>,
    pub errors: Vec<doc::validation::ErrorLocation>,
}

/// Label of a binding in warnings: its collection, or else its index.
pub(crate) fn binding_label(
    collection: Option<&proto_flow::flow::CollectionSpec>,
    index: usize,
) -> String {
    match collection {
        Some(collection) if !collection.name.is_empty() => collection.name.clone(),
        _ => format!("#{index}"),
    }
}

/// Check an unsealed endpoint configuration and the resource configurations
/// of its bindings against the schemas of the connector's Spec response.
///
/// Validators are built once per call (a session's start), and a schema which
/// is trivial (`{}` or `true`) or fails to build is not checked: a schema
/// which fails to build will fail elsewhere, such as in IAM detection.
pub(crate) fn check(
    config_schema: &[u8],
    resource_config_schema: &[u8],
    config: &[u8],
    resource_configs: &[(String, bytes::Bytes)],
) -> Vec<Invalid> {
    let mut invalid = Vec::new();

    if let Some(mut validator) = build_validator(config_schema) {
        let errors = errors_of(&mut validator, config, BUILTIN_SENTINELS);
        if !errors.is_empty() {
            invalid.push(Invalid {
                binding: None,
                errors,
            });
        }
    }

    if resource_configs.is_empty() {
        return invalid;
    }
    let Some(mut validator) = build_validator(resource_config_schema) else {
        return invalid;
    };

    for (binding, resource_config) in resource_configs {
        let errors = errors_of(&mut validator, resource_config, &[RESOURCE_META]);
        if !errors.is_empty() {
            invalid.push(Invalid {
                binding: Some(binding.clone()),
                errors,
            });
        }
    }
    invalid
}

fn build_validator(schema: &[u8]) -> Option<doc::Validator> {
    let trimmed = std::str::from_utf8(schema).unwrap_or_default().trim();
    if trimmed.is_empty() || trimmed == "{}" || trimmed == "true" {
        return None;
    }

    let schema = match doc::validation::build_bundle(schema) {
        Ok(schema) => schema,
        Err(err) => {
            tracing::debug!(%err, "not checking configurations of a schema which fails to build");
            return None;
        }
    };
    match doc::Validator::new(schema) {
        Ok(validator) => Some(validator),
        Err(err) => {
            tracing::debug!(%err, "not checking configurations of a schema which fails to index");
            None
        }
    }
}

fn errors_of(
    validator: &mut doc::Validator,
    config: &[u8],
    ignored: &[&str],
) -> Vec<doc::validation::ErrorLocation> {
    let mut config: serde_json::Value = match serde_json::from_slice(config) {
        Ok(config) => config,
        // A configuration which isn't JSON is the connector's to report.
        Err(_) => return Vec::new(),
    };
    if let serde_json::Value::Object(object) = &mut config {
        for property in ignored {
            object.remove(*property);
        }
    }
    validator.error_locations(&config)
}

/// Map a check's findings into at most two warning logs of the connector:
/// one of the endpoint configuration, and one of all invalid bindings.
pub(crate) fn warnings(task_name: &str, invalid: Vec<Invalid>) -> Vec<ops::Log> {
    let (endpoint, bindings): (Vec<Invalid>, Vec<Invalid>) = invalid
        .into_iter()
        .partition(|invalid| invalid.binding.is_none());

    let listed = |errors: &[doc::validation::ErrorLocation]| {
        crate::json_field(&&errors[..errors.len().min(MAX_LISTED)])
    };
    let mut logs = Vec::new();

    if let Some(Invalid { errors, .. }) = endpoint.first() {
        logs.push(crate::build_log(
            ops::LogLevel::Warn,
            "endpoint configuration doesn't match the connector's schema",
            [
                ("task", crate::json_field(&task_name)),
                ("errors", listed(errors)),
                ("total_errors", crate::json_field(&errors.len())),
            ],
        ));
    }
    if !bindings.is_empty() {
        let listed_bindings: Vec<serde_json::Value> = bindings
            .iter()
            .take(MAX_LISTED)
            .map(|Invalid { binding, errors }| {
                serde_json::json!({
                    "binding": binding,
                    "errors": &errors[..errors.len().min(MAX_LISTED)],
                    "total_errors": errors.len(),
                })
            })
            .collect();

        logs.push(crate::build_log(
            ops::LogLevel::Warn,
            "resource configurations don't match the connector's schema",
            [
                ("task", crate::json_field(&task_name)),
                ("invalid_bindings", crate::json_field(&bindings.len())),
                ("bindings", crate::json_field(&listed_bindings)),
            ],
        ));
    }
    logs
}

#[cfg(test)]
mod test {
    use crate::protocol::Protocol;

    fn render(logs: &[ops::Log]) -> String {
        serde_json::to_string_pretty(
            &logs
                .iter()
                .map(|log| {
                    serde_json::json!({
                        "level": log.level,
                        "message": log.message,
                        "fields": log
                            .fields_json_map
                            .iter()
                            .map(|(key, value)| {
                                (key.clone(), serde_json::from_slice::<serde_json::Value>(value).unwrap())
                            })
                            .collect::<serde_json::Map<_, _>>(),
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn warnings_never_include_values() {
        let config_schema = serde_json::json!({
            "type": "object",
            "properties": {
                "host": {"type": "string", "pattern": "^[a-z.]+$"},
                "credentials": {
                    "type": "object",
                    "properties": {
                        "client_id": {"type": "string"},
                        "client_secret": {"type": "string", "secret": true, "maxLength": 8},
                        "token": {"const": "expected"},
                    },
                    "required": ["client_id", "client_secret"],
                },
            },
            "required": ["host"],
            "additionalProperties": false,
        });
        let resource_config_schema = serde_json::json!({
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
            "additionalProperties": false,
        });
        // Secrets are merged into the configuration when it's unsealed.
        let config = serde_json::json!({
            "host": "Not A Valid Host hunter2",
            "credentials": {
                "client_secret": "a-merged-secret-hunter2",
                "token": "another-secret-hunter2",
            },
            // Built-in sentinels aren't described by the declared schema.
            "_python": {"package": "source_acme"},
        });

        let invalid = super::check(
            config_schema.to_string().as_bytes(),
            resource_config_schema.to_string().as_bytes(),
            config.to_string().as_bytes(),
            &[
                // Valid, as `_meta` isn't described by the connector.
                (
                    "acmeCo/widgets".to_string(),
                    r#"{"name":"widgets","_meta":{"path":["widgets"]}}"#.into(),
                ),
                (
                    "acmeCo/gadgets".to_string(),
                    r#"{"name":42,"secret":"hunter2"}"#.into(),
                ),
            ],
        );
        let rendered = render(&super::warnings("acmeCo/source-acme", invalid));

        assert!(!rendered.contains("hunter2"), "{rendered}");
        insta::assert_snapshot!(rendered);
    }

    #[test]
    fn warnings_are_capped() {
        // Many invalid bindings, each with many errors, make just one warning
        // which lists a few of each and counts them all.
        let schema = serde_json::json!({
            "type": "object",
            "properties": (0..8).map(|i| (format!("p{i}"), serde_json::json!({"type": "string"}))).collect::<serde_json::Map<_, _>>(),
        });
        let resource: bytes::Bytes = serde_json::json!(
            (0..8)
                .map(|i| (format!("p{i}"), serde_json::json!(i)))
                .collect::<serde_json::Map<_, _>>()
        )
        .to_string()
        .into();
        let bindings: Vec<(String, bytes::Bytes)> = (0..12)
            .map(|i| (format!("acmeCo/collection-{i}"), resource.clone()))
            .collect();

        let invalid = super::check(
            schema.to_string().as_bytes(),
            schema.to_string().as_bytes(),
            &resource,
            &bindings,
        );
        let logs = super::warnings("acmeCo/source-acme", invalid);
        assert_eq!(logs.len(), 2);

        let fields: serde_json::Value =
            serde_json::from_slice(&logs[1].fields_json_map["bindings"]).unwrap();
        let bindings = fields.as_array().unwrap();
        assert_eq!(bindings.len(), 5);
        assert_eq!(bindings[0]["errors"].as_array().unwrap().len(), 5);
        assert_eq!(bindings[0]["total_errors"], 8);
        assert_eq!(&logs[1].fields_json_map["invalid_bindings"][..], b"12");
        assert_eq!(&logs[0].fields_json_map["total_errors"][..], b"8");
    }

    #[test]
    fn unusable_schemas_and_configs_are_not_checked() {
        let valid_schema = br#"{"type": "object", "required": ["a"]}"#;

        for (schema, config) in [
            // Trivial schemas always pass.
            (&b""[..], &br#"{"anything": "goes"}"#[..]),
            (b"{}", br#"{"anything": "goes"}"#),
            (b" true ", br#"{"anything": "goes"}"#),
            // A schema which fails to bundle.
            (br#"{"type": 42}"#, br#"{"anything": "goes"}"#),
            // A schema having an unresolved reference.
            (br#"{"$ref": "https://example.com/missing.json"}"#, br#"{}"#),
            // A configuration which isn't JSON.
            (valid_schema, b"not { json"),
        ] {
            let invalid = super::check(
                schema,
                schema,
                config,
                &[("binding".to_string(), config.to_vec().into())],
            );
            assert!(
                invalid.is_empty(),
                "{} {}",
                String::from_utf8_lossy(schema),
                String::from_utf8_lossy(config)
            );
        }
    }

    #[test]
    fn resource_configs_of_each_protocol() {
        use proto_flow::{capture, derive, materialize};

        let parse = |value: serde_json::Value| value.to_string();
        let collection =
            |name: &str| serde_json::json!({"name": name, "key": ["/id"], "writeSchema": {}});

        let capture_validate: capture::Request = serde_json::from_str(&parse(serde_json::json!({
            "validate": {
                "name": "acmeCo/source",
                "bindings": [
                    {"resourceConfig": {"name": "a"}, "collection": collection("acmeCo/a")},
                    {"resourceConfig": {"name": "b"}},
                ],
            }
        })))
        .unwrap();
        let capture_open: capture::Request = serde_json::from_str(&parse(serde_json::json!({
            "open": {"capture": {
                "name": "acmeCo/source",
                "bindings": [{"resourceConfig": {"name": "a"}, "collection": collection("acmeCo/a")}],
            }}
        })))
        .unwrap();
        let capture_apply: capture::Request = serde_json::from_str(&parse(serde_json::json!({
            "apply": {"capture": {
                "name": "acmeCo/source",
                "bindings": [{"resourceConfig": {"name": "a"}, "collection": collection("acmeCo/a")}],
            }}
        })))
        .unwrap();
        let capture_discover: capture::Request = serde_json::from_str(&parse(
            serde_json::json!({"discover": {"name": "acmeCo/source"}}),
        ))
        .unwrap();

        let materialize_validate: materialize::Request = serde_json::from_str(&parse(serde_json::json!({
            "validate": {
                "name": "acmeCo/dest",
                "bindings": [{"resourceConfig": {"table": "a"}, "collection": collection("acmeCo/a")}],
            }
        })))
        .unwrap();
        let materialize_open: materialize::Request = serde_json::from_str(&parse(serde_json::json!({
            "open": {"materialization": {
                "name": "acmeCo/dest",
                "bindings": [{"resourceConfig": {"table": "a"}, "collection": collection("acmeCo/a")}],
            }}
        })))
        .unwrap();

        // Transforms which omit their `lambda` (null or empty) have no
        // resource configuration to check.
        let derive_validate: derive::Request = serde_json::from_str(&parse(serde_json::json!({
            "validate": {
                "collection": collection("acmeCo/derived"),
                "transforms": [
                    {"name": "withLambda", "lambdaConfig": {"readOnly": true}},
                    {"name": "nullLambda", "lambdaConfig": null},
                    {"name": "noLambda"},
                ],
            }
        })))
        .unwrap();
        let mut derived = collection("acmeCo/derived");
        derived["derivation"] = serde_json::json!({
            "transforms": [
                {"name": "withLambda", "lambdaConfig": {"readOnly": true}},
                {"name": "noLambda"},
            ],
        });
        let derive_open: derive::Request =
            serde_json::from_str(&parse(serde_json::json!({"open": {"collection": derived}})))
                .unwrap();

        let labels = |configs: Vec<(String, bytes::Bytes)>| -> Vec<(String, String)> {
            configs
                .into_iter()
                .map(|(label, config)| (label, String::from_utf8(config.to_vec()).unwrap()))
                .collect()
        };
        insta::assert_debug_snapshot!([
            (
                "capture validate",
                labels(crate::capture::Capture::resource_configs(&capture_validate))
            ),
            (
                "capture open",
                labels(crate::capture::Capture::resource_configs(&capture_open))
            ),
            (
                "capture apply",
                labels(crate::capture::Capture::resource_configs(&capture_apply))
            ),
            (
                "capture discover",
                labels(crate::capture::Capture::resource_configs(&capture_discover))
            ),
            (
                "materialize validate",
                labels(crate::materialize::Materialize::resource_configs(
                    &materialize_validate
                ))
            ),
            (
                "materialize open",
                labels(crate::materialize::Materialize::resource_configs(
                    &materialize_open
                ))
            ),
            (
                "derive validate",
                labels(crate::derive::Derive::resource_configs(&derive_validate))
            ),
            (
                "derive open",
                labels(crate::derive::Derive::resource_configs(&derive_open))
            ),
        ]);
    }
}
