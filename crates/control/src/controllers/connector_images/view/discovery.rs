use std::collections::BTreeMap;

use bytes::Bytes;
use models::Capture;
use models::CaptureBinding;
use models::CaptureDef;
use models::CaptureEndpoint;
use models::Catalog;
use models::Collection;
use models::CollectionDef;
use models::Config;
use models::ConnectorConfig;
use models::ContentFormat;
use models::ContentType;
use models::Import;
use models::Object;
use models::RelativeUrl;
use models::ResourceDef;
use models::Schema;
use models::ShardTemplate;

use crate::models::connector_images::{ConnectorImage, DiscoveryOptions};
use crate::services::connectors::DiscoveredBinding;

type Resources = BTreeMap<String, ResourceDef>;

/// View model for rendering a Catalog from a DiscoveryResponse.
pub struct DiscoveredCatalog {
    image: ConnectorImage,
    config: Object,
    bindings: Vec<DiscoveredBinding>,
    options: DiscoveryOptions,
}

impl DiscoveredCatalog {
    pub fn new(
        image: ConnectorImage,
        config: Object,
        bindings: Vec<DiscoveredBinding>,
        options: DiscoveryOptions,
    ) -> Self {
        Self {
            image,
            config,
            bindings,
            options,
        }
    }

    pub fn image(&self) -> &ConnectorImage {
        &self.image
    }

    /// Generates a catalog from the data gathered during discovery. This will
    /// produce a top level catalog which mostly inlines content as additional
    /// resources and imports them.
    pub fn root_catalog(&self) -> Result<Catalog, serde_json::Error> {
        let catalog = Catalog {
            resources: self.inlined_resources()?,
            import: self.imports(),
            ..Default::default()
        };
        Ok(catalog)
    }

    /// Import the inlined catalog definition we created for the discovered capture.
    fn imports(&self) -> Vec<Import> {
        let capture_catalog = Import::Extended {
            content_type: ContentType::Catalog(ContentFormat::Json),
            url: RelativeUrl::new(self.capture_catalog_name()),
        };

        vec![capture_catalog]
    }

    /// Generates inlined resource definitions for all of the items we found
    /// during discovery. This includes the catalog with the capture definition,
    /// the collections, the config, and the schemas.
    fn inlined_resources(&self) -> Result<Resources, serde_json::Error> {
        let mut resources = BTreeMap::new();

        resources.insert(
            self.capture_catalog_name(),
            as_resource_def(self.capture_catalog())?,
        );
        resources.insert(self.config_name(), as_resource_def(self.config_content())?);

        for binding in self.bindings.iter() {
            resources.insert(
                binding.schema_name(),
                as_resource_def(Schema::Object(binding.document_schema_json.clone()))?,
            );
        }

        Ok(resources)
    }

    fn capture_catalog(&self) -> Catalog {
        Catalog {
            captures: self.capture_definitions(),
            collections: self.discovered_collections(),
            ..Default::default()
        }
    }

    fn config_content(&self) -> Config {
        Config::Inline(self.config.clone())
    }

    fn capture_definitions(&self) -> BTreeMap<Capture, CaptureDef> {
        let mut captures = BTreeMap::new();
        captures.insert(self.capture_name(), self.capture_def());
        captures
    }

    fn capture_def(&self) -> CaptureDef {
        CaptureDef {
            endpoint: CaptureEndpoint::Connector(ConnectorConfig {
                image: self.image.pinned_version(),
                config: self.config_url(),
            }),
            bindings: self.capture_bindings(),
            interval: CaptureDef::default_interval(),
            shards: ShardTemplate::default(),
        }
    }

    fn capture_bindings(&self) -> Vec<CaptureBinding> {
        let mut capture_bindings = Vec::with_capacity(self.bindings.len());

        for binding in self.bindings.iter() {
            capture_bindings.push(CaptureBinding {
                resource: binding.resource_spec_json.clone(),
                target: self.collection_name(binding),
            });
        }

        capture_bindings
    }

    fn discovered_collections(&self) -> BTreeMap<Collection, CollectionDef> {
        let mut collections = BTreeMap::new();

        for binding in self.bindings.iter() {
            collections.insert(
                self.collection_name(binding),
                CollectionDef {
                    schema: binding.schema_url(),
                    key: binding.key(),
                    projections: Default::default(),
                    derivation: Default::default(),
                    journals: Default::default(),
                },
            );
        }

        collections
    }

    fn capture_catalog_name(&self) -> String {
        format!("{}.flow.json", self.options.capture_name)
    }

    fn config_name(&self) -> String {
        format!("{}.config.json", self.options.capture_name)
    }

    fn config_url(&self) -> Config {
        Config::Url(RelativeUrl::new(self.config_name()))
    }

    fn capture_name(&self) -> Capture {
        let prefix = &self.options.catalog_prefix;
        let name = &self.options.capture_name;
        Capture::new(format!("{prefix}/{name}"))
    }

    fn collection_name(&self, binding: &DiscoveredBinding) -> Collection {
        let prefix = &self.options.catalog_prefix;
        let name = &binding.recommended_name;
        Collection::new(format!("{prefix}/{name}"))
    }
}

trait TypedContent {
    fn content_type(&self) -> ContentType;
}

impl TypedContent for Catalog {
    fn content_type(&self) -> ContentType {
        ContentType::Catalog(ContentFormat::Json)
    }
}
impl TypedContent for Config {
    fn content_type(&self) -> ContentType {
        ContentType::Config(ContentFormat::Json)
    }
}
impl TypedContent for Schema {
    fn content_type(&self) -> ContentType {
        ContentType::JsonSchema(ContentFormat::Json)
    }
}

fn as_resource_def<C>(content: C) -> Result<ResourceDef, serde_json::Error>
where
    C: TypedContent + serde::Serialize,
{
    let content_type = content.content_type();
    let json_content = serde_json::to_vec(&content)?;

    Ok(ResourceDef {
        content_type,
        content: Bytes::from(json_content),
    })
}
