use protocol::flow::ContentType as ProtoContentType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::RelativeUrl;

/// An Import includes a referenced resource in the catalog.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(untagged, deny_unknown_fields, rename_all = "camelCase")]
#[schemars(example = "Import::example_url")]
#[schemars(example = "Import::example_extended")]
pub enum Import {
    Url(RelativeUrl),
    Extended {
        /// # The resource to import.
        url: RelativeUrl,
        /// # The content-type of the imported resource.
        content_type: ContentType,
    },
}

impl Import {
    // Get the RelativeUrl of this Import.
    pub fn relative_url(&self) -> &RelativeUrl {
        match self {
            Self::Url(url) => url,
            Self::Extended { url, .. } => url,
        }
    }
    // Get the ContentType of this Import.
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Url(_) => ContentType::Catalog,
            Self::Extended { content_type, .. } => *content_type,
        }
    }

    fn example_url() -> Self {
        Self::Url(RelativeUrl::new("./a/flow.yaml"))
    }
    fn example_extended() -> Self {
        Self::Extended {
            url: RelativeUrl::new("https://example/schema.json"),
            content_type: ContentType::JsonSchema,
        }
    }
}

/// ContentType is the type of an imported resource's content.
#[derive(Deserialize, Debug, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "SCREAMING_SNAKE_CASE")]
#[schemars(example = "ContentType::example")]
pub enum ContentType {
    /// Resource is a Flow catalog (as YAML or JSON).
    Catalog,
    /// Resource is a JSON schema (as YAML or JSON).
    JsonSchema,
    /// Resource is a TypeScript module file.
    TypescriptModule,
    /// Resource is a compiled NPM package.
    #[schemars(skip)]
    NpmPackage,
}

impl ContentType {
    pub fn example() -> Self {
        Self::Catalog
    }
}

impl From<ProtoContentType> for ContentType {
    fn from(t: ProtoContentType) -> Self {
        match t {
            ProtoContentType::CatalogSpec => Self::Catalog,
            ProtoContentType::JsonSchema => Self::JsonSchema,
            ProtoContentType::TypescriptModule => Self::TypescriptModule,
            ProtoContentType::NpmPackage => Self::NpmPackage,
        }
    }
}
impl Into<ProtoContentType> for ContentType {
    fn into(self) -> ProtoContentType {
        match self {
            Self::Catalog => ProtoContentType::CatalogSpec,
            Self::JsonSchema => ProtoContentType::JsonSchema,
            Self::TypescriptModule => ProtoContentType::TypescriptModule,
            Self::NpmPackage => ProtoContentType::NpmPackage,
        }
    }
}
