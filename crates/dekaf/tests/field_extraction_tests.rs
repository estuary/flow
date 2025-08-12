use anyhow::Context;
use dekaf::connector::{DekafConfig, DeletionMode};
use itertools::Itertools;
use serde_json::json;

async fn get_extraction_components(
    fixture_path: String,
) -> anyhow::Result<(
    doc::Shape,
    DekafConfig,
    proto_flow::flow::FieldSelection,
    Vec<proto_flow::flow::Projection>,
)> {
    let source = build::arg_source_to_url(fixture_path.as_ref(), false)
        .context("Failed to create source URL")?;

    let build::Output { built, draft, .. } = managed_build(source).await;

    if !built.errors.is_empty() || !draft.errors.is_empty() {
        // Remove the error scope to avoid including file path components that break the snapshot
        let errors = built
            .errors
            .into_iter()
            .chain(draft.errors.into_iter())
            .map(|err| format!("{:?}", err.error))
            .join(", ");
        anyhow::bail!(errors);
    }

    let materialization = built
        .built_materializations
        .get_key(&models::Materialization::new("test/materialization"))
        .context(format!(
            "Couldn't find test materialization. Found: {}",
            built
                .built_materializations
                .iter()
                .map(|mat| mat.materialization.clone())
                .join(", ")
        ))?;

    let binding = materialization
        .spec
        .as_ref()
        .expect("Missing spec")
        .bindings
        .iter()
        .next()
        .context("Materialization has no bindings")?;

    let collection_spec = built
        .built_collections
        .get_key(&models::Collection::new(
            &binding
                .collection
                .as_ref()
                .expect("missing binding collection")
                .name,
        ))
        .context("Couldn't find bound collection")?
        .spec
        .as_ref()
        .context("Collection spec is missing")?;

    let schema = if collection_spec.read_schema_json.len() > 0 {
        &collection_spec.read_schema_json
    } else {
        &collection_spec.write_schema_json
    };
    let shape = json_schema_to_shape(schema)?;

    let endpoint_config =
        dekaf::extract_dekaf_config(&materialization.spec.clone().expect("missing spec"))
            .await
            .expect("Failed to extract DekafConfig");

    Ok((
        shape,
        endpoint_config,
        binding
            .field_selection
            .clone()
            .expect("missing field selection"),
        collection_spec.projections.clone(),
    ))
}

fn json_schema_to_shape(schema: &[u8]) -> anyhow::Result<doc::Shape> {
    let json_schema = doc::validation::build_bundle(schema)?;
    let validator = doc::Validator::new(json_schema)?;
    Ok(doc::Shape::infer(
        &validator.schemas()[0],
        validator.schema_index(),
    ))
}

async fn managed_build(source: url::Url) -> build::Output {
    use tables::CatalogResolver;

    let file_root = std::path::Path::new("/");
    let draft = build::load(&source, file_root).await;
    if !draft.errors.is_empty() {
        return build::Output::new(draft, Default::default(), Default::default());
    }
    let catalog_names = draft.all_spec_names().collect();
    let live = build::NoOpCatalogResolver.resolve(catalog_names).await;
    if !live.errors.is_empty() {
        return build::Output::new(draft, live, Default::default());
    }

    build::validate(
        models::Id::new([32; 8]),
        models::Id::new([1; 8]),
        true,
        "",
        ops::tracing_log_handler,
        false,
        false,
        false,
        &build::project_root(&source),
        draft,
        live,
    )
    .await
}

async fn roundtrip(
    fixture_path: String,
    docs: &[u8],
) -> anyhow::Result<Vec<Result<apache_avro::types::Value, apache_avro::Error>>> {
    let (shape, endpoint_config, field_selection, projections) =
        get_extraction_components(fixture_path).await?;

    let (avro_schema, extractors) = dekaf::utils::build_field_extractors(
        shape,
        field_selection,
        projections,
        endpoint_config.deletions,
    )?;

    extract_and_decode(docs, extractors, avro_schema)
}

fn extract_and_decode(
    docs: &[u8],
    extractors: Vec<(apache_avro::Schema, dekaf::utils::CustomizableExtractor)>,
    avro_schema: apache_avro::Schema,
) -> anyhow::Result<Vec<Result<apache_avro::types::Value, apache_avro::Error>>> {
    let mut parser = simd_doc::Parser::new();
    let alloc = doc::Allocator::new();

    () = parser.chunk(docs, 0).unwrap();

    let mut parsed = Vec::new();

    loop {
        let (_, parsed_docs) = parser.parse_many(&alloc).unwrap();
        if parsed_docs.len() == 0 {
            break;
        } else {
            parsed.extend(parsed_docs);
        }
    }

    parsed
        .into_iter()
        .map(|(doc, _offset)| {
            let mut encoded = Vec::new();
            dekaf::extract_and_encode(extractors.as_slice(), &doc, &mut encoded)?;

            Ok(apache_avro::from_avro_datum(
                &avro_schema,
                &mut encoded.as_slice(),
                None,
            ))
        })
        .collect::<Result<Vec<_>, _>>()
}

fn serde_to_jsonl(docs: Vec<serde_json::Value>) -> anyhow::Result<Vec<u8>> {
    let mut jsonl = Vec::new();
    for doc in docs {
        let line = serde_json::to_vec(&doc)?;
        jsonl.extend(line);
        jsonl.push(b'\n');
    }
    Ok(jsonl)
}

#[tokio::test]
async fn test_allof_with_null_default() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/allof_with_null_default.yaml".to_string();
    let docs = vec![json!({
        "id": 1234
    })];

    for output in roundtrip(fixture_path, serde_to_jsonl(docs)?.as_slice()).await? {
        insta::assert_debug_snapshot!(output?);
    }

    Ok(())
}

#[tokio::test]
async fn test_field_selection_specific_fields() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/field_selection_specific_fields.yaml".to_string();
    let docs = vec![json!({
        "key": "first",
        "field_a": "foo",
        "field_b": "bar"
    })];

    for output in roundtrip(fixture_path, serde_to_jsonl(docs)?.as_slice()).await? {
        insta::assert_debug_snapshot!(output?);
    }

    Ok(())
}

#[tokio::test]
async fn test_field_selection_recommended_fields() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/field_selection_recommended_fields.yaml".to_string();
    let docs = vec![json!({
        "key": "first",
        "field_a": "foo",
        "field_b": "bar"
    })];

    for output in roundtrip(fixture_path, serde_to_jsonl(docs)?.as_slice()).await? {
        insta::assert_debug_snapshot!(output?);
    }

    Ok(())
}

#[tokio::test]
async fn test_deletions() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/deletions.yaml".to_string();
    let docs = vec![
        json!({
            "key": "first",
            "_meta": { "op": "c" }
        }),
        json!({
            "key": "second",
            "_meta": { "op": "d" }
        }),
    ];

    for (idx, output) in roundtrip(fixture_path, serde_to_jsonl(docs)?.as_slice())
        .await?
        .into_iter()
        .enumerate()
    {
        insta::assert_debug_snapshot!(format!("deletions-{}", idx), output?);
    }

    Ok(())
}

#[tokio::test]
async fn test_old_style_deletions() -> anyhow::Result<()> {
    let shape = json_schema_to_shape(
        br#"{
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
        dekaf::utils::build_LEGACY_field_extractors(shape, DeletionMode::CDC)?;

    let decoded = extract_and_decode(
        serde_to_jsonl(vec![
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
        ])?
        .as_slice(),
        extractors,
        avro_schema,
    )?;

    for (idx, doc) in decoded.into_iter().enumerate() {
        insta::assert_debug_snapshot!(format!("old-deletions-{}", idx), doc?);
    }

    Ok(())
}

#[tokio::test]
async fn test_fields_with_hyphens() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/fields_with_hyphens.yaml".to_string();
    let docs = vec![json!({
        "id": 1234,
        "hyphenated-field": "test value",
    })];

    insta::assert_debug_snapshot!(roundtrip(fixture_path, serde_to_jsonl(docs)?.as_slice()).await);

    Ok(())
}

#[tokio::test]
async fn test_null_only_fields() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/null_only_location.yaml".to_string();
    let docs = vec![json!({
        "id": 1234,
        "null-only-field": null,
    })];

    insta::assert_debug_snapshot!(roundtrip(fixture_path, serde_to_jsonl(docs)?.as_slice()).await);

    Ok(())
}

#[tokio::test]
async fn test_longs() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/longs_with_zero.yaml".to_string();
    let raw_input = r#"{"key": 1234, "my_long": 0.0}
{"key": 1234, "my_long": 10.0}
"#;

    insta::assert_debug_snapshot!(roundtrip(fixture_path.clone(), raw_input.as_bytes()).await);

    let raw_input = r#"{"key": 1234, "my_long": 0.1}
"#;

    insta::assert_debug_snapshot!(roundtrip(fixture_path, raw_input.as_bytes()).await);

    Ok(())
}

#[tokio::test]
async fn test_number_or_string_format_number() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/number_or_string_format_number.yaml".to_string();
    let raw_input = r#"{"key": 1234, "string_int_fmt_numberield": 1.0}
    {"key": 1234, "string_int_fmt_numberield": 1}
    {"key": 1234, "string_int_fmt_numberield": "1.0"}
    {"key": 1234, "string_int_fmt_numberield": "1.1"}
    {"key": 1234, "string_int_fmt_numberield": "1"}
    "#;

    insta::assert_debug_snapshot!(roundtrip(fixture_path.clone(), raw_input.as_bytes()).await?);

    Ok(())
}
