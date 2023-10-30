use doc::{
    reduce::Strategy,
    shape::{location::Exists, Reduction},
    Annotation, Shape,
};
use json::schema;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

mod utils;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
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

    let Input {
        read,
        write,
        inferred,
    } = serde_json::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("invalid input: {:?}", err)))?;

    let output = models::Schema::extend_read_bundle(&read, &write, inferred.as_ref());

    serde_wasm_bindgen::to_value(&output).map_err(|err| JsValue::from_str(&format!("{err:?}")))
}
