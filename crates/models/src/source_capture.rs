use super::Capture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum TargetNaming {
    /// Leave the materialization binding's schema field empty, therefore
    /// falling back to the default schema of the materialization. For example,
    /// materialize the collection `acmeCo/mySchema/myTable` to a table called
    /// `myTable`, without specifying the schema.
    ///
    /// This used to be called `leaveEmpty`, and that value is still accepted,
    /// but specs will always be written with `noSchema` instead.
    #[serde(alias = "leaveEmpty")]
    NoSchema,
    /// Use the 2nd-to-last component of the collection name as the schema of
    /// the materialization binding. For example, materialize the collection
    /// `acmeCo/mySchema/myTable` to a table called `myTable` in the schema
    /// `mySchema`.
    ///
    /// This used to be called `fromSourceName`, and that value is still
    /// accepted, but specs will always be written with `withSchema` instead.
    #[serde(alias = "fromSourceName")]
    WithSchema,
    /// Use the 2nd-to-last component of the collection name to prefix the
    /// destination resource name, leaving the schema unspecified. For example,
    /// materialize the collection `acmeCo/mySchema/myTable` to a table called
    /// `mySchema_myTable`.
    PrefixSchema,

    /// Like `prefixSchema`, except that it will omit the prefix for the
    /// following common default schema names:
    /// - public
    /// - dbo
    PrefixNonDefaultSchema,
}

impl Default for TargetNaming {
    fn default() -> Self {
        TargetNaming::PrefixNonDefaultSchema
    }
}

/// SourceCaptureDef specifies configuration for source captures
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourcesDef {
    /// Capture name
    pub capture: Option<Capture>,

    /// When adding new bindings from a source capture to a materialization, how should the schema
    /// of the materialization binding be set
    #[serde(
        default,
        alias = "targetSchema",
        skip_serializing_if = "super::is_default"
    )]
    pub target_naming: TargetNaming,

    /// When adding new bindings from a source capture to a materialization, should the new
    /// bindings be marked as delta updates
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub delta_updates: bool,
}

impl SourcesDef {
    pub fn without_source_capture(mut self) -> Self {
        self.capture.take();
        self
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase", untagged)]
pub enum Sources {
    Simple(Capture),
    Configured(SourcesDef),
}

impl Sources {
    pub fn capture_name(&self) -> Option<&Capture> {
        match self {
            Sources::Simple(capture) => Some(&capture),
            Sources::Configured(sc) => sc.capture.as_ref(),
        }
    }

    /// Convert the enum to a normalized SourceCaptureDef by normalizing the Simple case
    pub fn to_normalized_def(&self) -> SourcesDef {
        match self {
            Sources::Simple(capture) => SourcesDef {
                capture: Some(capture.clone()),
                ..Default::default()
            },
            Sources::Configured(sc) => sc.clone(),
        }
    }
}
