use super::{
    Collection, ConnectorConfig, Id, JsonPointer, LocalConfig, RawValue, Secret, ShardTemplate,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;

/// A Capture binds an external system and target (e.x., a SQL table or cloud storage bucket)
/// from which data should be continuously captured, with a Flow collection into that captured
/// data is ingested. Multiple Captures may be bound to a single collection, but only one
/// capture may exist for a given endpoint and target.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CaptureDef {
    /// # Continuously keep the collection spec and schema up-to-date
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "AutoDiscover")]
    pub auto_discover: Option<AutoDiscover>,
    /// # Endpoint to capture from.
    pub endpoint: CaptureEndpoint,
    /// # Secrets which are merged into the endpoint configuration.
    /// Maps a JSON pointer within the endpoint configuration to the name of a
    /// secret which is resolved and merged into that location as the task
    /// starts. Secrets are named catalog entities, managed apart from the
    /// specification, and must be siblings of this task: secret
    /// `acmeCo/widgets/password` may be used by capture `acmeCo/widgets/source-pg`.
    ///
    /// Each entry synthesizes a document from its pointer -- pointer `/a/b`
    /// with resolved value `v` becomes `{"a":{"b":v}}` -- which is applied to
    /// the configuration as an RFC 7396 merge patch. Entries are applied in
    /// lexicographic pointer order, so a deeper pointer wins where two entries
    /// overlap, and a `null` value deletes its location.
    ///
    /// Pointer tokens are always object property names: token `2` addresses the
    /// property `"2"` and never an array index, and `-` is the literal property
    /// `"-"`. Arrays are therefore atomic values: to change one, point at the
    /// property holding it and supply the whole array. The empty pointer merges
    /// at the configuration root, and its secret must resolve to an object.
    ///
    /// A configuration using `secrets` must be plaintext: it cannot also be
    /// encrypted, and its top-level `sops` property is reserved.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub secrets: BTreeMap<JsonPointer, Secret>,
    /// # Bound collections to capture from the endpoint.
    pub bindings: Vec<CaptureBinding>,
    /// # Interval of time between invocations of the capture.
    /// Configured intervals are applicable only to connectors which are
    /// unable to continuously tail their source, and which instead produce
    /// a current quantity of output and then exit. Flow will start the
    /// connector again after the given interval of time has passed.
    ///
    /// Intervals are relative to the start of an invocation and not its completion.
    /// For example, if the interval is five minutes, and an invocation of the
    /// capture finishes after two minutes, then the next invocation will be started
    /// after three additional minutes.
    #[serde(
        default = "CaptureDef::default_interval",
        with = "humantime_serde",
        skip_serializing_if = "CaptureDef::is_default_interval"
    )]
    #[schemars(schema_with = "super::duration_schema")]
    pub interval: Duration,
    /// # Salt used for redacting sensitive fields in captured documents.
    /// When provided, this base64-encoded salt is used instead of a generated one.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_opt_bytes"
    )]
    #[schemars(with = "String")]
    pub redact_salt: Option<bytes::Bytes>,
    /// # Template for shards of this capture task.
    #[serde(default, skip_serializing_if = "ShardTemplate::is_empty")]
    pub shards: ShardTemplate,
    /// # Expected publication ID of this capture within the control plane.
    /// When present, a publication of the capture will fail if the
    /// last publication ID in the control plane doesn't match this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Id")]
    pub expect_pub_id: Option<Id>,
    /// # Delete this capture.
    /// When true, a publication will delete this capture.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub delete: bool,
    /// # Reset this capture.
    /// Publishing a value of `true` will reset this capture, which is
    /// equivalent to deleting and then re-creating the capture but
    /// applied as a single publication.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub reset: bool,
}

/// Settings to determine how Flow should stay abreast of ongoing changes to collections and schemas.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AutoDiscover {
    /// Automatically add new bindings discovered from the source.
    #[serde(default)]
    pub add_new_bindings: bool,
    /// Whether to automatically evolve collections and/or materialization
    /// bindings to handle changes to collections that would otherwise be
    /// incompatible with the existing catalog.
    #[serde(default)]
    pub evolve_incompatible_collections: bool,
}

/// An endpoint from which Flow will capture.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum CaptureEndpoint {
    /// # A Connector.
    Connector(ConnectorConfig),
    /// # A local command (development only).
    Local(LocalConfig),
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
#[schemars(example = CaptureBinding::example())]
pub struct CaptureBinding {
    /// # Endpoint resource to capture from.
    pub resource: RawValue,
    /// # Whether to disable the binding
    /// Disabled bindings are inactive, and not validated.
    /// They can be used to represent discovered resources that are
    /// intentionally not being captured.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub disable: bool,
    /// # Name of the collection to capture into.
    // Note(johnny): If we need to add details about how data is written to a
    // target, we should turn this into a Target enum as has already been done
    // with Source (used by Materialization & Derive).
    pub target: Collection,
    /// # Backfill counter for this binding.
    /// Every increment of this counter will result in a new backfill of this
    /// binding from the captured endpoint. For example when capturing from a
    /// SQL table, incrementing this counter will cause the table to be
    /// re-captured in its entirety from the source database.
    ///
    /// Note that a backfill does *not* truncate the target collection,
    /// and documents published by a backfilled binding will coexist with
    /// (and be ordered after) any documents which were published as part
    /// of a preceding backfill.
    #[serde(default, skip_serializing_if = "super::is_u32_zero")]
    pub backfill: u32,
}

impl CaptureDef {
    pub fn default_interval() -> Duration {
        Duration::from_secs(300) // 5 minutes.
    }
    fn is_default_interval(interval: &Duration) -> bool {
        *interval == Self::default_interval()
    }

    pub fn example() -> Self {
        Self {
            auto_discover: Some(AutoDiscover {
                add_new_bindings: true,
                evolve_incompatible_collections: true,
            }),
            endpoint: CaptureEndpoint::Connector(ConnectorConfig::example()),
            secrets: BTreeMap::new(),
            bindings: vec![CaptureBinding::example()],
            interval: Self::default_interval(),
            shards: ShardTemplate::default(),
            expect_pub_id: None,
            delete: false,
            reset: false,
            redact_salt: None,
        }
    }
}

impl CaptureBinding {
    pub fn example() -> Self {
        Self {
            resource: serde_json::from_value(json!({"stream": "a_stream"})).unwrap(),
            disable: false,
            target: Collection::new("target/collection"),
            backfill: 0,
        }
    }
}

impl super::ModelDef for CaptureDef {
    fn sources(&self) -> impl Iterator<Item = &crate::Source> {
        std::iter::empty()
    }
    fn targets(&self) -> impl Iterator<Item = &crate::Collection> {
        self.bindings
            .iter()
            .filter(|b| !b.disable)
            .map(|b| &b.target)
    }

    fn catalog_type(&self) -> crate::CatalogType {
        crate::CatalogType::Capture
    }

    fn is_enabled(&self) -> bool {
        !self.shards.disable
    }

    fn connector_image(&self) -> Option<String> {
        match &self.endpoint {
            CaptureEndpoint::Connector(cfg) => Some(cfg.image.to_owned()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn secrets_stanza_round_trips() {
        let fixture = json!({
            "endpoint": {"connector": {"image": "an/image", "config": {}}},
            "secrets": {
                "": "acmeCo/widgets/whole-config",
                "/credentials/password": "acmeCo/widgets/password",
            },
            "bindings": [],
        });
        let model: CaptureDef = serde_json::from_value(fixture.clone()).unwrap();

        // Pointers order lexicographically, which is the order in which
        // resolved secrets are merge-patched into the configuration.
        assert_eq!(
            model
                .secrets
                .keys()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>(),
            ["", "/credentials/password"],
        );
        assert_eq!(serde_json::to_value(&model).unwrap(), fixture);

        // An empty stanza is omitted entirely.
        let model = CaptureDef {
            secrets: BTreeMap::new(),
            ..model
        };
        assert!(
            serde_json::to_value(&model)
                .unwrap()
                .get("secrets")
                .is_none()
        );
    }
}
