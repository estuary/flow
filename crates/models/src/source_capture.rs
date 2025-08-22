use super::Capture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to name target resources (database tables, for example) for materializing
/// a given Collection.
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

/// Specifies configuration for source captures, and defaults for new bindings
/// that are added to the materialization. Changing these defaults has no effect
/// on existing bindings.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceDef {
    /// # Capture to source from
    /// The materialization will follow the bindings of the named capture
    /// as they change over time.
    #[schemars(with = "Capture")]
    pub capture: Option<Capture>,
    /// # Naming convention for new bindings
    /// New bindings will apply the naming convention to determine the
    /// target's name and schema within the endpoint.
    #[serde(default, alias = "targetSchema")]
    pub target_naming: TargetNaming,
    /// # Delta-updates for new bindings
    /// New bindings will apply this as their delta-updates setting.
    #[serde(default)]
    pub delta_updates: bool,
    /// # Mode for automatic field selection of new bindings
    // TODO(johnny): remove `skip_serializing_if` when released to `flowctl`
    // and most users have updated.
    #[serde(default, skip_serializing_if = "super::is_default")]
    pub fields_recommended: crate::RecommendedDepth,
}

impl SourceDef {
    pub fn without_source_capture(mut self) -> Self {
        self.capture.take();
        self
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase", untagged)]
pub enum SourceType {
    Simple(Capture),
    Configured(SourceDef),
}

impl SourceType {
    pub fn capture_name(&self) -> Option<&Capture> {
        match self {
            SourceType::Simple(capture) => Some(&capture),
            SourceType::Configured(sc) => sc.capture.as_ref(),
        }
    }

    /// Convert the enum to a normalized SourceCaptureDef by normalizing the Simple case
    pub fn to_normalized_def(&self) -> SourceDef {
        match self {
            SourceType::Simple(capture) => SourceDef {
                capture: Some(capture.clone()),
                ..Default::default()
            },
            SourceType::Configured(sc) => sc.clone(),
        }
    }
}
