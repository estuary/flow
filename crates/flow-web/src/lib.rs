mod utils;

use doc::inference::{Exists, Reduction, Shape};
use doc::Annotation;
use json::schema;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

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
fn reduce_description(reduce: doc::inference::Reduction) -> &'static str {
    match reduce {
        Reduction::Unset => "unset",
        Reduction::Append => "append",
        Reduction::FirstWriteWins => "first-write-wins",
        Reduction::LastWriteWins => "last-write-wins",
        Reduction::Maximize => "maximize",
        Reduction::Merge => "merge",
        Reduction::Minimize => "minimize",
        Reduction::Set => "set",
        Reduction::Sum => "sum",
        Reduction::Multiple => "multiple strategies may apply",
        Reduction::JsonSchemaMerge => "merge json schemas",
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
            let name = if ptr.is_empty() || is_pattern {
                None
            } else {
                Some((&ptr[1..]).to_string())
            };
            let types = prop_shape.type_.iter().map(|ty| ty.to_string()).collect();

            let enum_vals = prop_shape
                .enum_
                .as_ref()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|val| val.clone())
                .collect();
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
                title: prop_shape.title.clone(),
                description: prop_shape.description.clone(),
                reduction: reduce_description(prop_shape.reduction.clone()).to_string(),
                pointer: ptr,
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
