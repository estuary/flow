//! Built collection resolution for WASM bindings.
//!
//! This module exposes the `proto_flow::linked` resolution of a built task's
//! bindings to their collections, so that web interfaces read built specs
//! without handling the inline and indirect forms themselves.

use proto_flow::flow;
use serde::Serialize;
use serde_json::value::RawValue;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = JSON, js_name = stringify, catch)]
    fn json_stringify(value: &JsValue) -> Result<Option<String>, JsValue>;

    #[wasm_bindgen(js_namespace = JSON, js_name = parse, catch)]
    fn json_parse(text: &str) -> Result<JsValue, JsValue>;
}

/// Resolves the collections of a built capture or materialization.
///
/// A built spec names the collection of each binding either inline, or by
/// `collectionIndex` into its `linkedCollections` table. This function
/// converts the spec to the indirect form, and returns its table alongside
/// the explicit table index of each active binding.
///
/// # Input Structure
///
/// The input should be a JSON object with these fields:
/// - `specType`: Either "capture" or "materialization"
/// - `builtSpec`: The built spec, as stored in `live_specs.built_spec` or `draft_specs.built_spec`
///
/// # Returns
///
/// Returns an object containing:
/// - `collections`: Array of distinct collection specs, ordered by name
/// - `bindings`: For each active binding in order, the index of its collection
///   in `collections`, or null if the binding doesn't resolve to a collection
///
/// # Errors
///
/// Returns JavaScript errors for invalid input or processing failures.
#[wasm_bindgen]
pub fn resolve_built_collections(input: JsValue) -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

    // Cross the JS boundary as JSON text in both directions. A built spec
    // with thousands of bindings converts several times faster this way than
    // value by value through serde_wasm_bindgen, and its embedded JSON
    // documents arrive as plain objects.
    let input = json_stringify(&input)?.ok_or_else(|| JsValue::from_str("invalid JSON"))?;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    enum SpecType {
        Capture,
        Materialization,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Input<'a> {
        spec_type: SpecType,
        #[serde(borrow)]
        built_spec: &'a RawValue,
    }

    #[derive(Serialize)]
    struct Output {
        collections: Vec<proto_flow::Arc<flow::CollectionSpec>>,
        bindings: Vec<Option<u32>>,
    }

    let Input {
        spec_type,
        built_spec,
    } = serde_json::from_str(&input)
        .map_err(|err| JsValue::from_str(&format!("Invalid input: {:?}", err)))?;

    let invalid_spec = |err| JsValue::from_str(&format!("Invalid built spec: {:?}", err));

    // After `indirect()`, a binding resolves with `Some(index)` into
    // `linked_collections`, or not at all.
    let output = match spec_type {
        SpecType::Capture => {
            let mut spec: flow::CaptureSpec =
                serde_json::from_str(built_spec.get()).map_err(invalid_spec)?;
            spec.indirect();

            Output {
                bindings: spec
                    .resolved_bindings()
                    .map(|(_binding, resolved)| resolved.and_then(|(_collection, index)| index))
                    .collect(),
                collections: spec.linked_collections,
            }
        }
        SpecType::Materialization => {
            let mut spec: flow::MaterializationSpec =
                serde_json::from_str(built_spec.get()).map_err(invalid_spec)?;
            spec.indirect();

            Output {
                bindings: spec
                    .resolved_bindings()
                    .map(|(_binding, resolved)| resolved.and_then(|(_collection, index)| index))
                    .collect(),
                collections: spec.linked_collections,
            }
        }
    };

    // Free the input text before writing the output text, so that wasm
    // memory doesn't hold both at once.
    let capacity = input.len();
    drop(input);
    let mut text = Vec::with_capacity(capacity);
    serde_json::to_writer(&mut text, &output)
        .map_err(|err| JsValue::from_str(&format!("Failed to serialize result: {}", err)))?;
    let text = String::from_utf8(text)
        .map_err(|err| JsValue::from_str(&format!("Failed to serialize result: {}", err)))?;

    json_parse(&text)
}
