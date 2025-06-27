use doc::{
    reduce::Strategy,
    shape::{location::Exists, Reduction},
    Annotation, Shape,
};
use json::schema;
use models::SourceType;
use serde::{Deserialize, Serialize};
use tables::utils::ResourceSpecPointers;
use wasm_bindgen::prelude::*;

pub mod collection;
pub mod field_selection;
mod utils;


#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);

    // Uncomment if you would like to log in JS with a call _kind of_ line `console.log`
    // #[wasm_bindgen(js_namespace = console)]
    // fn log(s: &str, b: JsValue);
}

#[derive(Serialize, Deserialize)]
pub struct Property {
    pub name: Option<String>,
    pub is_pattern_property: bool,
    pub exists: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub reduction: String,
    pub pointer: String,
    pub types: Vec<String>,
    pub enum_vals: Vec<serde_json::Value>,
    pub string_format: Option<String>,
}

fn reduce_description(reduce: Reduction) -> &'static str {
    match reduce {
        Reduction::Multiple => "multiple strategies may apply",
        Reduction::Strategy(Strategy::Append) => "append",
        Reduction::Strategy(Strategy::FirstWriteWins(_)) => "first-write-wins",
        Reduction::Strategy(Strategy::JsonSchemaMerge) => "merge json schemas",
        Reduction::Strategy(Strategy::LastWriteWins(_)) => "last-write-wins",
        Reduction::Strategy(Strategy::Maximize(_)) => "maximize",
        Reduction::Strategy(Strategy::Merge(_)) => "merge",
        Reduction::Strategy(Strategy::Minimize(_)) => "minimize",
        Reduction::Strategy(Strategy::Set(_)) => "set",
        Reduction::Strategy(Strategy::Sum) => "sum",
        Reduction::Unset => "unset",
    }
}

#[derive(Serialize, Deserialize)]
pub struct AnalyzedSchema {
    pub properties: Vec<Property>,
}

#[wasm_bindgen]
pub fn infer(schema: JsValue) -> Result<JsValue, JsValue> {
    let schema_uri =
        url::Url::parse("https://estuary.dev").expect("parse should not fail on hard-coded url");

    let parsed_schema: serde_json::Value =
        ::serde_wasm_bindgen::from_value(schema).map_err(|err| {
            let err_string = format!("invalid JSON schema: {:?}", err);
            JsValue::from_str(&err_string)
        })?;
    let schema =
        schema::build::build_schema::<Annotation>(schema_uri, &parsed_schema).map_err(|err| {
            let err_string = format!("invalid JSON schema: {}", err);
            JsValue::from_str(&err_string)
        })?;

    let mut index = schema::index::IndexBuilder::new();
    index.add(&schema).map_err(|err| {
        let err_string = format!("invalid JSON schema reference: {}", err);
        JsValue::from_str(&err_string)
    })?;
    index.verify_references().map_err(|err| {
        let err_string = format!("invalid JSON schema reference: {}", err);
        JsValue::from_str(&err_string)
    })?;
    let index = index.into_index();

    let shape = Shape::infer(&schema, &index);

    let properties: Vec<Property> = shape
        .locations()
        .into_iter()
        .map(|(ptr, is_pattern, prop_shape, exists)| {
            let name = if ptr.0.is_empty() || is_pattern {
                None
            } else {
                Some((&ptr.to_string()[1..]).to_string())
            };
            let types = prop_shape.type_.iter().map(|ty| ty.to_string()).collect();

            let enum_vals = prop_shape.enum_.clone().unwrap_or_default();
            let string_format = prop_shape.string.format.as_ref().map(|f| f.to_string());
            let ex = match exists {
                Exists::May => "may",
                Exists::Cannot => "cannot",
                Exists::Implicit => "implicit",
                Exists::Must => "must",
            };
            Property {
                name,
                exists: ex.to_string(),
                is_pattern_property: is_pattern,
                title: prop_shape.title.clone().map(Into::into),
                description: prop_shape.description.clone().map(Into::into),
                reduction: reduce_description(prop_shape.reduction.clone()).to_string(),
                pointer: ptr.to_string(),
                types,
                enum_vals,
                string_format,
            }
        })
        .collect();

    serde_wasm_bindgen::to_value(&AnalyzedSchema { properties }).map_err(|err| {
        let msg = format!("failed to serialize result: {}", err);
        JsValue::from_str(&msg)
    })
}

#[wasm_bindgen]
pub fn extend_read_bundle(input: JsValue) -> Result<JsValue, JsValue> {
    let input: serde_json::Value = ::serde_wasm_bindgen::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("invalid JSON: {:?}", err)))?;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Input {
        read: models::Schema,
        write: models::Schema,
        inferred: Option<models::Schema>,
    }
    let relaxed: Option<models::Schema>;

    let Input {
        read,
        write,
        inferred,
    } = serde_json::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("invalid input: {:?}", err)))?;

    let mut defs = Vec::new();

    if read.references_write_schema() {
        defs.push(models::schemas::AddDef {
            id: models::Schema::REF_WRITE_SCHEMA_URL,
            schema: &write,
            overwrite: true,
        });
    }
    if read.references_relaxed_write_schema() {
        relaxed = inferred
            .is_some()
            .then(|| write.to_relaxed_schema())
            .transpose()
            .map_err(|err| JsValue::from_str(&format!("{err:?}")))?;

        defs.push(models::schemas::AddDef {
            id: models::Schema::REF_RELAXED_WRITE_SCHEMA_URL,
            schema: relaxed.as_ref().unwrap_or(&write),
            overwrite: true,
        });
    }
    if read.references_inferred_schema() {
        defs.push(models::schemas::AddDef {
            id: models::Schema::REF_INFERRED_SCHEMA_URL,
            schema: inferred
                .as_ref()
                .unwrap_or(models::Schema::inferred_schema_placeholder()),
            overwrite: false,
        })
    }

    let outcome = read
        .add_defs(&defs)
        .map_err(|err| JsValue::from_str(&format!("{err:?}")))?;

    serde_wasm_bindgen::to_value(&outcome).map_err(|err| JsValue::from_str(&format!("{err:?}")))
}

#[wasm_bindgen]
pub fn get_resource_config_pointers(input: JsValue) -> Result<JsValue, JsValue> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Input {
        spec: serde_json::Value,
    }
    let Input { spec } = serde_wasm_bindgen::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("Invalid JSON: {:?}", err)))?;

    let pointers = tables::utils::pointer_for_schema(&serde_json::to_string(&spec).unwrap())
        .map_err(|err| JsValue::from_str(&format!("Failed getting pointers: {:?}", err)))?;

    #[derive(Serialize, Deserialize)]
    struct Output {
        pointers: ResourceSpecPointers,
    }
    serde_wasm_bindgen::to_value(&Output { pointers })
        .map_err(|err| JsValue::from_str(&format!("{err:?}")))
}

pub use collection::skim_collection_projections;
pub use field_selection::evaluate_field_selection;

#[wasm_bindgen]
pub fn update_materialization_resource_spec(input: JsValue) -> Result<JsValue, JsValue> {
    let input: serde_json::Value = ::serde_wasm_bindgen::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("invalid JSON: {:?}", err)))?;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Input {
        source_capture: SourceType,
        resource_spec: serde_json::Value,
        resource_spec_pointers: ResourceSpecPointers,
        collection_name: String,
    }

    let Input {
        source_capture,
        resource_spec,
        resource_spec_pointers,
        collection_name,
    } = serde_json::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("invalid input: {:?}", err)))?;

    let mut resource_spec = resource_spec.clone();
    tables::utils::update_materialization_resource_spec(
        &source_capture,
        &mut resource_spec,
        &resource_spec_pointers,
        collection_name.as_ref(),
    )
    .map_err(|err| JsValue::from_str(&format!("Failed updating resource spec: {:?}", err)))?;

    // Outputting as a string as I just could NOT get it to return JSON correctly
    let output = serde_json::to_string(&resource_spec).unwrap();
    serde_wasm_bindgen::to_value(&{ output }).map_err(|err| JsValue::from_str(&format!("{err:?}")))
}
