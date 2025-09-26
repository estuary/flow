//! Collection projection evaluation for WASM bindings.
//!
//! This module exposes collection validation and projection skimming logic
//! from the validation crate for use in web interfaces.

use json::Scope;
use proto_flow::flow;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Result of collection projection skimming.
///
/// Contains the derived projections and any validation errors encountered.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionProjectionsResult {
    /// Derived projections from the collection schema
    pub projections: Vec<flow::Projection>,
    /// Validation errors encountered during processing
    pub errors: Vec<String>,
}

/// Skims projections from a collection definition.
///
/// This function processes a collection's schema, key, and projection definitions
/// to derive the actual projections that would be available for materialization.
/// It validates the collection configuration and returns both the projections
/// and any validation errors.
///
/// # Input Structure
///
/// The input should be a JSON object with these fields:
/// - `collection`: The collection name (e.g., "acmeCo/users")
/// - `model`: The complete collection definition including schema, key, and projections
///
/// # Returns
///
/// Returns a `CollectionProjectionsResult` containing:
/// - `projections`: Array of derived projection objects
/// - `errors`: Array of validation error messages
///
/// # Errors
///
/// Returns JavaScript errors for invalid input or processing failures.
#[wasm_bindgen]
pub fn skim_collection_projections(input: JsValue) -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

    // Must transcode through serde_json due to RawValue.
    let input: serde_json::Value = ::serde_wasm_bindgen::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("invalid JSON: {:?}", err)))?;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Input {
        collection: String,
        model: models::CollectionDef,
    }

    let Input { collection, model } = serde_json::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("Invalid input: {:?}", err)))?;

    // Create a scope for validation
    let scope_url = url::Url::parse(&format!("flow://collection/{}", collection))
        .map_err(|err| JsValue::from_str(&format!("Invalid collection name: {}", err)))?;
    let scope = Scope::new(&scope_url);

    // Create collection model
    let collection_model = models::Collection::new(&collection);

    // Create errors container
    let mut errors = tables::Errors::new();

    // Call the skim_projections function
    let projections =
        validation::collection::skim_projections(scope, &collection_model, &model, &mut errors);

    // Convert errors to strings
    let error_strings: Vec<String> = errors
        .into_iter()
        .map(|error| format!("{:#}", error.error))
        .collect();

    let result = CollectionProjectionsResult {
        projections,
        errors: error_strings,
    };

    serde_wasm_bindgen::to_value(&result)
        .map_err(|err| JsValue::from_str(&format!("Failed to serialize result: {}", err)))
}
