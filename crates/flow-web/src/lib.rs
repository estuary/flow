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
pub fn get_trigger_config_schema() -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

    let schema = models::triggers::triggers_schema();

    schema
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|err| JsValue::from_str(&format!("{err:?}")))
}

/// Strip HMAC-excluded fields from trigger configs before passing to the
/// config-encryption service. Returns `{stripped, originals}` where `stripped`
/// is the JSON string to send for encryption and `originals` is an opaque token
/// to pass back to `restore_trigger_hmac_excluded_fields` afterward.
#[wasm_bindgen]
pub fn strip_trigger_hmac_excluded_fields(triggers_json: &str) -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

    let mut triggers: models::Triggers = serde_json::from_str(triggers_json)
        .map_err(|err| JsValue::from_str(&format!("invalid triggers JSON: {err}")))?;

    let originals = models::triggers::strip_hmac_excluded_fields(&mut triggers);

    #[derive(Serialize)]
    struct Output {
        stripped: String,
        originals: Vec<models::triggers::HmacExcludedOriginals>,
    }
    serde_wasm_bindgen::to_value(&Output {
        stripped: serde_json::to_string(&triggers).unwrap(),
        originals,
    })
    .map_err(|err| JsValue::from_str(&format!("{err:?}")))
}

/// Restore HMAC-excluded fields to an encrypted triggers JSON string. Pass the
/// `originals` token returned by `strip_trigger_hmac_excluded_fields` and the
/// encrypted JSON string returned by the config-encryption service. Returns the
/// final triggers JSON string with non-secret fields restored.
#[wasm_bindgen]
pub fn restore_trigger_hmac_excluded_fields(
    encrypted_json: &str,
    originals: JsValue,
) -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

    let mut triggers: models::Triggers = serde_json::from_str(encrypted_json)
        .map_err(|err| JsValue::from_str(&format!("invalid encrypted JSON: {err}")))?;

    let originals: Vec<models::triggers::HmacExcludedOriginals> =
        serde_wasm_bindgen::from_value(originals)
            .map_err(|err| JsValue::from_str(&format!("invalid originals token: {err:?}")))?;

    models::triggers::restore_hmac_excluded_fields(&mut triggers, originals);

    serde_wasm_bindgen::to_value(&serde_json::to_string(&triggers).unwrap())
        .map_err(|err| JsValue::from_str(&format!("{err:?}")))
}

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
