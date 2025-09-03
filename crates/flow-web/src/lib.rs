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

#[wasm_bindgen]
pub fn get_resource_config_pointers(input: JsValue) -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

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
    crate::utils::set_panic_hook();

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
