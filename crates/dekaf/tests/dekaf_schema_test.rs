use anyhow::Context;
use dekaf::connector;
use itertools::Itertools;
use proto_flow::{flow::materialization_spec::ConnectorType, materialize};
use serde_json::json;
use validation_tests::Outcome;

fn run_validation(fixture: &str) -> anyhow::Result<Outcome> {
    let outcome = validation_tests::run(fixture, "{}");

    let mut errors = outcome
        .errors
        .iter()
        .chain(outcome.errors_draft.iter())
        .peekable();

    if errors.peek().is_some() {
        let formatted = errors.format("\n");
        anyhow::bail!("Validation errors: {formatted:?}")
    }

    Ok(outcome)
}

fn json_schema_to_shape(schema: &str) -> anyhow::Result<doc::Shape> {
    let json_schema = doc::validation::build_bundle(schema)?;
    let validator = doc::Validator::new(json_schema)?;
    Ok(doc::Shape::infer(
        &validator.schemas()[0],
        validator.schema_index(),
    ))
}

fn build_test_fixture(
    schema: serde_json::Value,
    field_selection: serde_json::Value,
    config: connector::DekafConfig,
    bindings: Option<Vec<materialize::response::validated::Binding>>,
) -> String {
    let materialization = if let Some(bindings) = bindings {
        json!({
            "connectorType": "DEKAF",
            "config": {
                "variant": "foo",
                "config": config
            },
            "bindings": bindings
        })
    } else {
        json!({
            "connectorType": "DEKAF",
            "config": {
                "variant": "foo",
                "config": config
            },
            "bindings": [{
                "constraints": {},
                "resourcePath": ["anything"]
            }]
        })
    };

    serde_json::to_string_pretty(&json!({
        "test://example/catalog.yaml": {
            "collections": {
                "test/collection": schema
            },
            "materializations": {
                "test/materialization":{
                    "endpoint": {
                        "dekaf": {
                            "variant": "foo",
                            "config": config
                        },
                    },
                    "bindings": [
                        {
                            "source": "test/collection",
                            "resource": {
                                "topic_name": "foo"
                            },
                            "fields": field_selection
                        }
                    ]
                }
            }
        },
        "driver": {
            "dataPlanes": {
                "1d:1d:1d:1d:1d:1d:1d:1d": {"default": true}
            },
            "materializations": {
                "test/materialization": materialization
            }
        }
    }))
    .unwrap()
}

/// Helper function to run validation and get the components needed for field extraction
async fn get_extraction_components(
    schema: serde_json::Value,
    field_selection: serde_json::Value,
    config: connector::DekafConfig,
) -> anyhow::Result<(
    doc::Shape,
    proto_flow::flow::FieldSelection,
    Vec<proto_flow::flow::Projection>,
)> {
    // First run to get validated bindings
    let fixture = build_test_fixture(
        schema.clone(),
        // Just need something that passes validation, we'll pass in the real
        // field selection in the second pass once we have the bindings
        json!({"recommended": true}),
        config.clone(),
        None,
    );
    let outcome = run_validation(fixture.as_ref())?;
    let built_materializations = outcome.built_materializations;

    let materialization_spec = built_materializations
        .first()
        .context("No materializations built")?
        .spec
        .as_ref()
        .context("No spec")?;

    let validate_req = materialize::request::Validate {
        name: "what".to_string(),
        connector_type: ConnectorType::Dekaf as i32,
        config_json: materialization_spec.config_json.to_owned(),
        bindings: materialization_spec
            .bindings
            .iter()
            .map(
                |binding| proto_flow::materialize::request::validate::Binding {
                    resource_config_json: binding.resource_config_json.clone(),
                    collection: binding.collection.clone(),
                    field_config_json_map: binding
                        .field_selection
                        .as_ref()
                        .expect("No field selection")
                        .field_config_json_map
                        .clone(),
                    backfill: 0,
                },
            )
            .collect_vec(),
        last_materialization: None,
        last_version: "foo".to_owned(),
    };

    let validate_resp = connector::unary_materialize(materialize::Request {
        validate: Some(validate_req),
        ..Default::default()
    })
    .await?;

    let bindings = validate_resp
        .validated
        .as_ref()
        .context("No validated response")?
        .bindings
        .clone();

    // Second run with validated bindings to get final components
    let fixture = build_test_fixture(schema, field_selection, config, Some(bindings));
    let outcome = run_validation(fixture.as_ref())?;
    let built_materializations = outcome.built_materializations;
    let built_collections = outcome.built_collections;

    let collection = built_collections.first().context("No collections built")?;
    let collection_spec = collection.spec.as_ref().context("No collection spec")?;
    let materialization = built_materializations
        .first()
        .context("No materializations built")?;
    let materialization_spec = materialization.spec.as_ref().context("No spec")?;

    let schema = if collection_spec.read_schema_json.len() > 0 {
        &collection_spec.read_schema_json
    } else {
        &collection_spec.write_schema_json
    };

    let shape = json_schema_to_shape(schema)?;
    let field_selection = materialization_spec
        .bindings
        .first()
        .context("No bindings")?
        .field_selection
        .as_ref()
        .context("No field selection")?
        .clone();

    Ok((shape, field_selection, collection_spec.projections.clone()))
}

async fn roundtrip(
    endpoint_config: connector::DekafConfig,
    schema: serde_json::Value,
    field_selection: serde_json::Value,
    docs: Vec<serde_json::Value>,
) -> anyhow::Result<Vec<Result<apache_avro::types::Value, apache_avro::Error>>> {
    let (shape, field_selection, projections) =
        get_extraction_components(schema, field_selection, endpoint_config.clone()).await?;

    let (avro_schema, extractors) = dekaf::utils::build_field_extractors(
        shape,
        field_selection,
        projections,
        endpoint_config.deletions,
    )?;

    extract_and_decode(docs, extractors, avro_schema)
}

fn extract_and_decode(
    docs: Vec<serde_json::Value>,
    extractors: Vec<(apache_avro::Schema, dekaf::utils::CustomizableExtractor)>,
    avro_schema: apache_avro::Schema,
) -> anyhow::Result<Vec<Result<apache_avro::types::Value, apache_avro::Error>>> {
    docs.into_iter()
        .map(|doc| {
            // Extract and encode document
            let mut encoded = Vec::new();
            dekaf::extract_and_encode(extractors.as_slice(), &doc, &mut encoded)?;

            // Now decode it back into a Value representation
            Ok(apache_avro::from_avro_datum(
                &avro_schema,
                &mut encoded.as_slice(),
                None,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
}

#[tokio::test]
async fn test_allof_with_null_default() -> anyhow::Result<()> {
    for output in roundtrip(
        connector::DekafConfig {
            deletions: connector::DeletionMode::Kafka,
            token: "1234".to_string(),
            strict_topic_names: false,
        },
        json!({
          "schema": {
            "allOf": [
              {
                "properties": {
                  "id": {
                    "title": "Id",
                    "type": "integer"
                  },
                  "conflicts": {
                    "type": ["integer", "null"],
                    "default": null,
                    "title": "Updatedbyuserid"
                  }
                },
                "required": ["id"],
                "type": "object"
              },
              {
                "properties": {
                  "id": {
                    "title": "Id",
                    "type": "integer"
                  },
                  "conflicts": {
                    "type": "integer"
                  }
                },
                "required": ["id"],
                "type": "object"
              }
            ]
          },
          "key": ["/id"]
        }),
        json!({
            "recommended": true
        }),
        vec![json!({
          "id": 671963468
        })],
    )
    .await?
    {
        insta::assert_debug_snapshot!(output?);
    }

    Ok(())
}

#[tokio::test]
async fn test_field_selection_specific_fields() -> anyhow::Result<()> {
    for output in roundtrip(
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::Kafka,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "field_a": {
                        "type": "string",
                    },
                    "field_b": {
                        "type": "string",
                    }
                },
                "type": "object",
                "required": [
                    "key",
                    "field_a",
                    "field_b"
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "include": {
                "field_a": {}
            },
            "recommended": false
        }),
        vec![json!({
            "key": "first",
            "field_a": "foo",
            "field_b": "bar"
        })],
    )
    .await?
    {
        insta::assert_debug_snapshot!(output?);
    }

    Ok(())
}

#[tokio::test]
async fn test_field_selection_recommended_fields() -> anyhow::Result<()> {
    for output in roundtrip(
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::Kafka,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "field_a": {
                        "type": "string",
                    },
                    "field_b": {
                        "type": "string",
                    }
                },
                "type": "object",
                "required": [
                    "key",
                    "field_a",
                    "field_b"
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "recommended": true
        }),
        vec![json!({
            "key": "first",
            "field_a": "foo",
            "field_b": "bar"
        })],
    )
    .await?
    {
        insta::assert_debug_snapshot!(output?);
    }

    Ok(())
}

#[tokio::test]
async fn test_deletions() -> anyhow::Result<()> {
    for (idx, doc) in roundtrip(
        dekaf::connector::DekafConfig {
            deletions: dekaf::connector::DeletionMode::CDC,
            strict_topic_names: false,
            token: "1234".to_string(),
        },
        json!({
            "schema": {
                "properties": {
                    "key": {
                        "type": "string"
                    },
                    "_meta": {
                        "properties": {
                            "op": {
                                "type": "string"
                            }
                        },
                        "type": "object"
                    }
                },
                "type": "object",
                "required": [
                    "key",
                    "_meta",
                ],
            },
            "key": [
                "/key"
            ]
        }),
        json!({
            "recommended": true
        }),
        vec![
            json!({
                "key": "first",
                "_meta": {
                    "op": "c"
                },
            }),
            json!({
                "key": "second",
                "_meta": {
                    "op": "d"
                },
            }),
        ],
    )
    .await?
    .into_iter()
    .enumerate()
    {
        insta::assert_debug_snapshot!(format!("deletions-{}", idx), doc?);
    }

    Ok(())
}

#[tokio::test]
async fn test_old_style_deletions() -> anyhow::Result<()> {
    let shape = json_schema_to_shape(
        r#"{
       "properties": {
            "key": {
                "type": "string"
            },
            "_meta": {
                "properties": {
                    "op": {
                        "type": "string"
                    }
                },
                "type": "object"
            }
        },
        "additionalProperties": {
            "type": "string"
        },
        "type": "object",
        "required": [
            "key",
            "_meta"
        ]
    }"#,
    )?;

    let (avro_schema, extractors) =
        dekaf::utils::build_LEGACY_field_extractors(shape, connector::DeletionMode::CDC)?;

    let decoded = extract_and_decode(
        vec![
            json!({
                "key": "first",
                "_meta": {
                    "op": "c"
                },
            }),
            json!({
                "key": "second",
                "_meta": {
                    "op": "d"
                },
            }),
            json!({
                "key": "second",
                "_meta": {
                    "op": "d"
                },
                "additional": "I should end up in _flow_extra"
            }),
        ],
        extractors,
        avro_schema,
    )?;

    for (idx, doc) in decoded.into_iter().enumerate() {
        insta::assert_debug_snapshot!(format!("old-deletions-{}", idx), doc?);
    }

    Ok(())
}
