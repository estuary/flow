use super::Capture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize,  Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum SourceCaptureSchemaMode {
    /// Leave the materialization binding's schema field empty, therefore falling back to the
    /// default schema of the materialization
    LeaveEmpty,
    /// Use the 2nd-to-last component of the collection name as the schema of the materialization
    /// binding
    CollectionSchema,
}

impl Default for SourceCaptureSchemaMode {
    fn default() -> Self {
        SourceCaptureSchemaMode::LeaveEmpty
    }
}

/// SourceCaptureDef specifies configuration for source captures
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceCaptureDef {
    /// Capture name
    pub capture: Capture,

    /// When adding new bindings from a source capture to a materialization, how should the schema
    /// of the materialization binding be set
    #[serde(default)]
    pub schema_mode: SourceCaptureSchemaMode,

    /// When adding new bindings from a source capture to a materialization, should the new
    /// bindings be marked as delta updates
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub delta_updates: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase", untagged)]
pub enum SourceCapture {
    Simple(Capture),
    Configured(SourceCaptureDef),
}

impl SourceCapture {
    pub fn capture_name(&self) -> Capture {
        match self {
            SourceCapture::Simple(capture) => capture.clone(),
            SourceCapture::Configured(sc) => sc.capture.clone(),
        }
    }

    pub fn def(&self) -> SourceCaptureDef {
        match self {
            SourceCapture::Simple(capture) => SourceCaptureDef {
                capture: capture.clone(),
                ..Default::default()
            },
            SourceCapture::Configured(sc) => sc.clone(),
        }
    }
}
