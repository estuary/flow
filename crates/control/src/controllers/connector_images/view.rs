use axum::Json;

use crate::controllers::connector_images::routes;
use crate::controllers::connectors::routes as connector_routes;
use crate::controllers::json_api::{DocumentData, Links, Many, One, RawJson, Resource};
use crate::models::connector_images::ConnectorImage;
use crate::models::connectors::{Connector, ConnectorOperation};
use crate::models::id::Id;
use crate::models::{JsonObject, JsonValue};

pub mod discovery;

pub fn index(images: Vec<ConnectorImage>) -> Json<Many<ConnectorImage>> {
    let resources = images.into_iter().map(Resource::from).collect();
    let links = Links::default().put("self", routes::index());
    Json(DocumentData::new(resources, links))
}

pub fn create(image: ConnectorImage) -> Json<One<ConnectorImage>> {
    let resource = DocumentData::new(Resource::<ConnectorImage>::from(image), Links::default());
    Json(resource)
}

pub fn show(image: ConnectorImage) -> Json<One<ConnectorImage>> {
    let resource = DocumentData::new(Resource::<ConnectorImage>::from(image), Links::default());
    Json(resource)
}

pub fn spec(connector: Connector, image: ConnectorImage, spec: RawJson) -> Json<One<RawJson>> {
    let mut links = Links::default()
        .put("self", routes::spec(image.id))
        .put("image", routes::show(image.id))
        .put("connector", connector_routes::show(image.connector_id));

    if connector.supports(ConnectorOperation::Discover) {
        links = links.put("discovered_catalog", routes::discovered_catalog(image.id))
    }

    let resource = Resource {
        id: Id::nonce(),
        r#type: "connector_spec",
        attributes: spec,
        links,
    };
    Json(DocumentData::new(resource, Links::default()))
}

pub fn discovered_catalog(catalog: discovery::DiscoveredCatalog) -> Json<One<JsonValue>> {
    let links = Links::default()
        .put("self", routes::discovered_catalog(catalog.image().id))
        .put("image", routes::show(catalog.image().id));

    let resource = Resource {
        id: Id::nonce(),
        r#type: "discovered_catalog",
        attributes: render_root_catalog(&catalog),
        links: Links::default(),
    };

    Json(DocumentData::new(resource, links))
}

/// We're rendering a discovered catalog as a nested catalog. At the root level,
/// it only includes an import statement and a list of resources. The inner
/// catalog specifies the details for the Capture, Config, and any Schemas. This
/// allows us to keep these individual units separate from each other while
/// still adhering closely to the Catalog format.
fn render_root_catalog(catalog: &discovery::DiscoveredCatalog) -> serde_json::Value {
    let mut bundled = JsonObject::new();
    bundled.insert(
        import_url(catalog.name()),
        serde_json::json!({
            "contentType": "CATALOG",
            "content": catalog.render_catalog(),
        }),
    );
    bundled.insert(
        import_url(catalog.config_name()),
        serde_json::json!({
            "contentType": "CONFIG",
            "content": catalog.render_config(),
        }),
    );

    for (name, schema) in catalog.render_schemas().into_iter() {
        bundled.insert(
            import_url(name),
            serde_json::json!({
                "contentType": "JSON_SCHEMA",
                "content": schema,
            }),
        );
    }

    serde_json::json!({
        "import": [import_url(catalog.name())],
        "resources": bundled
    })
}

fn import_url(path: impl AsRef<str>) -> String {
    format!("flow://discovered/{}", path.as_ref())
}

impl From<ConnectorImage> for Resource<ConnectorImage> {
    fn from(image: ConnectorImage) -> Self {
        let links = Links::default()
            .put("self", routes::show(image.id))
            .put("spec", routes::spec(image.id))
            .put("connector", connector_routes::show(image.connector_id));

        Resource {
            id: image.id,
            r#type: "connector_image",
            attributes: image,
            links,
        }
    }
}
