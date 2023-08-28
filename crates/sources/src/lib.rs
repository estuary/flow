mod bundle_schema;
mod indirect;
mod inline;
mod loader;
pub mod merge;
pub mod scenarios;
mod scope;

use std::collections::HashMap;

pub use bundle_schema::bundle_schema;
pub use indirect::{indirect_large_files, rebuild_catalog_resources};
pub use inline::inline_sources;
pub use loader::{FetchFuture, Fetcher, LoadError, Loader};
pub use scope::Scope;
use serde::Serialize;

#[derive(Copy, Clone, Debug)]
pub enum Format {
    Json,
    Yaml,
}

impl Format {
    pub fn from_scope(scope: &url::Url) -> Self {
        if scope.as_str().ends_with("json") {
            Format::Json
        } else {
            Format::Yaml
        }
    }
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
        }
    }
    fn serialize(&self, value: &models::RawValue) -> bytes::Bytes {
        let mut de = serde_json::Deserializer::from_str(value.get());
        let mut buf = Vec::new();

        match self {
            Self::Json => serde_transcode::transcode(
                &mut de,
                &mut serde_json::Serializer::with_formatter(
                    &mut buf,
                    serde_json::ser::PrettyFormatter::new(),
                ),
            )
            .unwrap(),
            Self::Yaml => {
                serde_transcode::transcode(&mut de, &mut serde_yaml::Serializer::new(&mut buf))
                    .unwrap()
            }
        }
        buf.into()
    }
}

/// Computes an md5 hash of each spec in the catalog, and checks it against the
/// given set of `spec_checksums`. If `spec_checksums` contains an equivalent
/// hash for an item, then it is removed from the catalog. The input checksums
/// must be formatted as lowercase hex strings.
pub fn remove_unchanged_specs(
    spec_checksums: &HashMap<String, String>,
    catalog: &mut models::Catalog,
) {
    catalog
        .collections
        .retain(|name, spec| is_spec_changed(&spec_checksums, name, spec));
    catalog
        .captures
        .retain(|name, spec| is_spec_changed(&spec_checksums, name, spec));
    catalog
        .materializations
        .retain(|name, spec| is_spec_changed(&spec_checksums, name, spec));
    catalog
        .tests
        .retain(|name, spec| is_spec_changed(&spec_checksums, name, spec));
}

fn is_spec_changed(
    existing_specs: &HashMap<String, String>,
    new_catalog_name: &impl AsRef<str>,
    new_catalog_spec: &impl Serialize,
) -> bool {
    if let Some(existing_spec_md5) = existing_specs.get(&new_catalog_name.as_ref().to_string()) {
        let buf = serde_json::to_vec(new_catalog_spec).expect("new spec must be serializable");
        let new_spec_md5 = format!("{:x}", md5::compute(&buf));
        return *existing_spec_md5 != new_spec_md5;
    }
    // If there's no existing md5, then the spec is new, which is considered a change.
    true
}
