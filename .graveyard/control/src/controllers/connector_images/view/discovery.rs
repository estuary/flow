use crate::models::connector_images::{ConnectorImage, DiscoveryOptions};
use crate::models::connectors::Connector;
use crate::models::{JsonObject, JsonValue};
use crate::services::connectors::DiscoveredBinding;

/// View model for rendering a Catalog from a DiscoveryResponse.
pub struct DiscoveredCatalog {
    connector: Connector,
    image: ConnectorImage,
    config: JsonObject,
    bindings: Vec<DiscoveredBinding>,
    options: DiscoveryOptions,
}

impl DiscoveredCatalog {
    pub fn new(
        connector: Connector,
        image: ConnectorImage,
        config: JsonObject,
        bindings: Vec<DiscoveredBinding>,
        options: DiscoveryOptions,
    ) -> Self {
        Self {
            connector,
            image,
            config,
            bindings,
            options,
        }
    }

    pub fn image(&self) -> &ConnectorImage {
        &self.image
    }

    pub fn render_catalog(&self) -> JsonValue {
        serde_json::json!( {
            "captures": self.capture_definitions(),
            "collections": self.discovered_collections(),
        })
    }

    pub fn render_config(&self) -> JsonObject {
        self.config.clone()
    }

    pub fn render_schemas(&self) -> JsonObject {
        let mut schemas = JsonObject::new();
        for binding in self.bindings.iter() {
            schemas.insert(
                binding.schema_name(),
                binding.document_schema_json.clone().into(),
            );
        }
        schemas
    }

    pub fn name(&self) -> String {
        format!("{}.flow.json", self.connector.codename())
    }

    pub fn config_name(&self) -> String {
        format!("{}.config.json", self.connector.codename())
    }

    fn capture_definitions(&self) -> JsonObject {
        let mut captures = JsonObject::new();
        captures.insert(self.capture_name(), self.capture_def());
        captures
    }

    fn capture_def(&self) -> JsonValue {
        serde_json::json!( {
            "endpoint": {
                "connector": {
                    "image": self.image.pinned_version(),
                    "config": self.config_url(),
                }
            },
            "bindings": self.capture_bindings(),
        })
    }

    fn capture_bindings(&self) -> Vec<JsonValue> {
        let mut capture_bindings = Vec::with_capacity(self.bindings.len());

        for binding in self.bindings.iter() {
            capture_bindings.push(serde_json::json!( {
                "resource": binding.resource_spec_json,
                "target": self.collection_name(binding),
            }));
        }

        capture_bindings
    }

    fn discovered_collections(&self) -> JsonObject {
        let mut collections = JsonObject::new();

        for binding in self.bindings.iter() {
            collections.insert(
                self.collection_name(binding),
                serde_json::json!( {
                    "schema": binding.schema_url(),
                    "key": binding.key(),
                }),
            );
        }

        collections
    }

    fn config_url(&self) -> String {
        format!("{}.config.json", self.connector.codename())
    }

    fn capture_name(&self) -> String {
        let prefix = &self.options.catalog_prefix;
        let name = &self.options.capture_name;
        format!("{prefix}/{name}")
    }

    fn collection_name(&self, binding: &DiscoveredBinding) -> String {
        let prefix = &self.options.catalog_prefix;
        let name = &binding.recommended_name;
        format!("{prefix}/{name}")
    }
}
