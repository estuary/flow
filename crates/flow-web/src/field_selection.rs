//! Field selection evaluation for WASM bindings.
//!
//! This module exposes the improved field selection logic from the validation crate
//! for use in web interfaces. It provides user-friendly error messages and structured
//! data about field selection outcomes.

use proto_flow::{flow, materialize};
use serde::{Deserialize, Serialize};
use validation::field_selection::{self, Reject, Select};
use wasm_bindgen::prelude::*;

/// Wrapper for Select with both structured and rendered data.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectOutput {
    /// The structured select reason
    pub reason: Select,
    /// Human-readable description of the select reason
    pub detail: String,
}

/// Wrapper for Reject with both structured and rendered data.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectOutput {
    /// The structured reject reason
    pub reason: Reject,
    /// Human-readable description of the reject reason
    pub detail: String,
}

/// Represents the outcome for a single field in field selection evaluation.
///
/// Each field will have either a select reason, reject reason, or both (indicating a conflict).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldOutcome {
    /// The field name (e.g., "userId", "metadata")
    pub field: String,
    /// Structured select reason with human-readable detail.
    /// None if the field was not selected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub select: Option<SelectOutput>,
    /// Structured reject reason with human-readable detail.
    /// None if the field was not rejected.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject: Option<RejectOutput>,
    /// Whether this field has an incompatible constraint conflict.
    /// True when there's a conflict and the reject reason is ConnectorIncompatible.
    #[serde(skip_serializing_if = "is_false")]
    pub is_incompatible: bool,
}

/// Complete result of field selection evaluation.
///
/// Contains both detailed per-field outcomes and the final field selection
/// that would be used for materialization.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldSelectionResult {
    /// Detailed outcome for each field that was considered
    pub outcomes: Vec<FieldOutcome>,
    /// The final field selection configuration
    pub selection: proto_flow::flow::FieldSelection,
    /// Whether there are any conflicts that need user attention
    pub has_conflicts: bool,
}

/// Evaluates field selection for a materialization binding.
///
/// This function runs the improved field selection logic and returns detailed
/// information about why each field was selected or rejected, along with the
/// final field selection configuration.
///
/// # Input Structure
///
/// The input should be a JSON object with these fields:
/// - `collection`: Object containing:
///   - `name`: The collection name (e.g., "acmeCo/users")
///   - `model`: The collection definition
/// - `binding`: Object containing:
///   - `live`: Optional existing materialization binding spec
///   - `model`: The materialization binding configuration from the user
///   - `validated`: Validated constraints from the connector
///
/// # Returns
///
/// Returns a `FieldSelectionResult` containing:
/// - `outcomes`: Per-field selection/rejection reasons
/// - `selection`: Final field selection with keys/values/document
/// - `hasConflicts`: Whether there are unresolved conflicts
///
/// # Errors
///
/// Returns JavaScript errors for invalid input or evaluation failures.
#[wasm_bindgen]
pub fn evaluate_field_selection(input: JsValue) -> Result<JsValue, JsValue> {
    crate::utils::set_panic_hook();

    // Must transcode through serde_json due to RawValue.
    let input: serde_json::Value = ::serde_wasm_bindgen::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("invalid JSON: {:?}", err)))?;

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Binding {
        live: Option<flow::materialization_spec::Binding>,
        model: models::MaterializationBinding,
        validated: materialize::response::validated::Binding,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Collection {
        name: String,
        model: models::CollectionDef,
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Input {
        collection: Collection,
        binding: Binding,
    }

    let Input {
        collection:
            Collection {
                name: collection_name,
                model: collection_model,
            },
        binding:
            Binding {
                live: binding_live,
                model: binding_model,
                validated: binding_validated,
            },
    } = serde_json::from_value(input)
        .map_err(|err| JsValue::from_str(&format!("Invalid input: {:?}", err)))?;

    // Build projections from the collection definition using skim_projections
    let scope_url = url::Url::parse(&format!("flow://collection/{}", collection_name))
        .map_err(|err| JsValue::from_str(&format!("Invalid collection name: {}", err)))?;
    let mut errors = tables::Errors::new();

    let collection_projections = validation::collection::skim_projections(
        json::Scope::new(&scope_url),
        &models::Collection::new(&collection_name),
        &collection_model,
        &mut errors,
    );

    // Return early if there were errors building projections
    if !errors.is_empty() {
        let errors: String = errors
            .into_iter()
            .map(|error| format!("{:#}", error.error))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(JsValue::from_str(&format!(
            "Failed to build collection projections:\n{errors}",
        )));
    }

    let models::MaterializationBinding {
        fields: model_fields,
        backfill: model_backfill,
        ..
    } = binding_model;

    let materialize::response::validated::Binding {
        case_insensitive_fields,
        constraints: validated_constraints,
        resource_path: validated_resource_path,
        ..
    } = binding_validated;

    let live_field_selection = if let Some(live_spec) = &binding_live {
        assert_eq!(
            validated_resource_path, live_spec.resource_path,
            "sanity check: validated and live resource path must match"
        );
        // If we intend to back-fill, then live fields have no effect on selection.
        if live_spec.backfill == model_backfill {
            live_spec.field_selection.as_ref()
        } else {
            None
        }
    } else {
        None
    };

    let group_by: Vec<String> = if !model_fields.group_by.is_empty() {
        model_fields
            .group_by
            .iter()
            .map(models::Field::to_string)
            .collect()
    } else {
        // Fall back to canonical projections of the collection key.
        collection_model
            .key
            .iter()
            .map(|k| k[1..].to_string())
            .collect()
    };

    let (selects, rejects, field_config) = field_selection::extract_constraints(
        &collection_projections,
        &group_by,
        live_field_selection,
        &model_fields,
        &validated_constraints,
    );
    let (document_field, field_outcomes) = field_selection::group_outcomes(
        case_insensitive_fields,
        &collection_projections,
        rejects,
        selects,
        &validated_constraints,
    );
    let (selection, conflicts) = field_selection::build_selection(
        group_by,
        document_field,
        field_config,
        field_outcomes.clone(),
    );

    // Convert outcomes to structured format with rendered details
    let mut outcomes = Vec::new();
    for (field, outcome) in field_outcomes {
        let is_incompatible = matches!(
            &outcome,
            tables::EitherOrBoth::Both(
                _select,
                validation::field_selection::Reject::ConnectorIncompatible { .. },
            )
        );
        let select = outcome.as_ref().left().map(|s| SelectOutput {
            detail: format!("{s}"),
            reason: s.clone(),
        });
        let reject = outcome.as_ref().right().map(|r| RejectOutput {
            detail: format!("{r}"),
            reason: r.clone(),
        });

        outcomes.push(FieldOutcome {
            field,
            select,
            reject,
            is_incompatible,
        });
    }

    let result = FieldSelectionResult {
        outcomes,
        selection,
        has_conflicts: !conflicts.is_empty(),
    };

    serde_wasm_bindgen::to_value(&result)
        .map_err(|err| JsValue::from_str(&format!("Failed to serialize result: {}", err)))
}

fn is_false(b: &bool) -> bool {
    !b
}
