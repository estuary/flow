//! Built collection resolution for WASM bindings.
//!
//! This module exposes the `proto_flow::linked` resolution of a built task's
//! bindings to their collections, so that web interfaces read built specs
//! without handling the inline and indirect forms themselves.

use proto_flow::flow;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Resolves the collections of a built capture or materialization.
///
/// A built spec names the collection of each binding either inline, or by
/// `collectionIndex` into its `linkedCollections` table. This function
/// returns the collections of the spec alongside the explicit index of each
/// active binding's collection, whichever form the spec is in.
///
/// Collections are returned by reference: they are the objects of the given
/// `builtSpec` and not copies of them. Only the properties which decide how
/// a binding resolves are decoded, so a call costs in proportion to the
/// number of bindings and not to the size of their collections.
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
/// - `collections`: A new array of the spec's own collection objects. For a
///   spec in indirect form these are the entries of its `linkedCollections`.
///   For a spec in inline form they are the collection of each active binding
///   which has one, in binding order, so a collection which is used by
///   several bindings appears once for each of them.
/// - `bindings`: For each active binding in order, the index of its collection
///   in `collections`, or null if the binding doesn't resolve to a collection
///
/// # Errors
///
/// Returns JavaScript errors for invalid input or processing failures.
#[wasm_bindgen]
pub fn resolve_built_collections(input: JsValue) -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    enum SpecType {
        Capture,
        Materialization,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Input {
        spec_type: SpecType,
        #[serde(with = "serde_wasm_bindgen::preserve")]
        built_spec: JsValue,
    }

    #[derive(Serialize)]
    struct Output {
        #[serde(with = "serde_wasm_bindgen::preserve")]
        collections: js_sys::Array,
        bindings: Vec<Option<u32>>,
    }

    let Input {
        spec_type,
        built_spec,
    } = serde_wasm_bindgen::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("Invalid input: {:?}", err)))?;

    macro_rules! resolve {
        ($spec:ty, $binding:ty) => {{
            let bindings_key =
                json_property(|spec: &mut $spec| spec.bindings = vec![Default::default()]);
            let linked_key = json_property(|spec: &mut $spec| {
                spec.linked_collections = vec![Default::default()]
            });
            let collection_key = json_property(|binding: &mut $binding| {
                binding.collection = Some(Default::default())
            });
            let index_key = json_property(|binding: &mut $binding| binding.collection_index = 1);

            // Stringify only the properties which decide how bindings resolve.
            // This leaves a skeleton of the spec: its bindings and its table,
            // with every collection reduced to an empty object.
            let skeleton = js_sys::JSON::stringify_with_replacer(
                &built_spec,
                &js_sys::Array::of4(&bindings_key, &linked_key, &collection_key, &index_key),
            )?
            .as_string()
            .ok_or_else(|| JsValue::from_str("Invalid built spec: not a JSON value"))?;

            let spec: $spec = serde_json::from_str(&skeleton)
                .map_err(|err| JsValue::from_str(&format!("Invalid built spec: {:?}", err)))?;

            // Start from the table of the spec, if it has one.
            let collections = if spec.linked_collections.is_empty() {
                js_sys::Array::new()
            } else {
                js_sys::Array::from(&js_sys::Reflect::get(&built_spec, &linked_key)?)
            };
            let js_bindings = js_sys::Reflect::get(&built_spec, &bindings_key)?;

            let mut bindings = Vec::with_capacity(spec.bindings.len());
            for (index, (_binding, resolved)) in spec.resolved_bindings().enumerate() {
                bindings.push(match resolved {
                    // Indirect form: the binding names an entry of the table.
                    Some((_collection, Some(collection_index))) => Some(collection_index),
                    // Inline form: the binding carries its own collection.
                    Some((_collection, None)) => {
                        let binding = js_sys::Reflect::get_u32(&js_bindings, index as u32)?;
                        let collection = js_sys::Reflect::get(&binding, &collection_key)?;
                        Some(collections.push(&collection) - 1)
                    }
                    None => None,
                });
            }

            Output {
                collections,
                bindings,
            }
        }};
    }

    let output = match spec_type {
        SpecType::Capture => resolve!(flow::CaptureSpec, flow::capture_spec::Binding),
        SpecType::Materialization => resolve!(
            flow::MaterializationSpec,
            flow::materialization_spec::Binding
        ),
    };

    output
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|err| JsValue::from_str(&format!("Failed to serialize result: {}", err)))
}

// The JSON property which the generated serializer writes for the one field
// that `set` assigns. Property names are taken from the serializers so that
// they cannot drift from the encoding of built specs.
fn json_property<T: Default + Serialize>(set: impl FnOnce(&mut T)) -> JsValue {
    let mut value = T::default();
    set(&mut value);

    match serde_json::to_value(&value) {
        Ok(serde_json::Value::Object(object)) if object.len() == 1 => {
            JsValue::from_str(object.keys().next().unwrap())
        }
        other => panic!("expected an object having one property, not {other:?}"),
    }
}
