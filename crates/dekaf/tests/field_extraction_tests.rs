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

    let build::Output { built, .. } = managed_build(source).await;

    if !built.errors.is_empty() {
        anyhow::bail!("Build errors: {:?}", built.errors);
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

fn json_schema_to_shape(schema: &str) -> anyhow::Result<doc::Shape> {
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
    docs: Vec<serde_json::Value>,
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
    docs: Vec<serde_json::Value>,
    extractors: Vec<(apache_avro::Schema, dekaf::utils::CustomizableExtractor)>,
    avro_schema: apache_avro::Schema,
) -> anyhow::Result<Vec<Result<apache_avro::types::Value, apache_avro::Error>>> {
    docs.into_iter()
        .map(|doc| {
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

#[tokio::test]
async fn test_allof_with_null_default() -> anyhow::Result<()> {
    let fixture_path = "tests/fixtures/allof_with_null_default.yaml".to_string();
    let docs = vec![json!({
        "id": 1234
    })];

    for output in roundtrip(fixture_path, docs).await? {
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

    for output in roundtrip(fixture_path, docs).await? {
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

    for output in roundtrip(fixture_path, docs).await? {
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

    for (idx, output) in roundtrip(fixture_path, docs).await?.into_iter().enumerate() {
        insta::assert_debug_snapshot!(format!("deletions-{}", idx), output?);
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
        dekaf::utils::build_LEGACY_field_extractors(shape, DeletionMode::CDC)?;

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
