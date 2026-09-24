use super::RawValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Splits a full connector image name into separate image and tag components.
/// The resulting tag begins with `:` or with a digest's `@` if either is
/// present, and a tag followed by a digest stays whole (`:v1@sha256:...`),
/// so the components always concatenate back into `image_full`. Otherwise,
/// the tag will be an empty string.
pub fn split_image_tag(image_full: &str) -> (String, String) {
    let mut image = image_full.to_string();

    // A digest follows the only `@` a reference may contain, and may itself
    // be preceded by a tag, as in `repo:v1@sha256:...`.
    let digest_pivot = image.find('@').unwrap_or(image.len());

    // A registry may include a port, so only a colon in the final path component
    // can delimit an image tag.
    let image_name_start = image[..digest_pivot]
        .rfind('/')
        .map_or(0, |pivot| pivot + 1);
    let tag_pivot = image[image_name_start..digest_pivot]
        .find(':')
        .map_or(digest_pivot, |pivot| image_name_start + pivot);

    let tag = image.split_off(tag_pivot);
    (image, tag)
}

/// Reserved catalog-name component which separates an arbitrary owner prefix
/// from the exact repository of an image-owned secret.
pub const IMAGE_SECRET_SEPARATOR: &str = "connectors";

/// Whether `secret` is owned by `repository` under the image-secret naming
/// rule: `<prefix>/connectors/<repository>/<leaf>`.
///
/// The rightmost `connectors` component is the separator, so the owner prefix
/// remains arbitrary. Repositories containing that reserved component cannot
/// participate because they would make the boundary ambiguous. Nor can a
/// repository naming a registry port (`localhost:5000/image`), as catalog names
/// do not allow `:`.
pub fn image_owns_secret(secret: &str, repository: &str) -> bool {
    if repository
        .split('/')
        .any(|component| component == IMAGE_SECRET_SEPARATOR)
    {
        return false;
    }

    let Some((parent, _leaf)) = secret.rsplit_once('/') else {
        return false;
    };
    let separator = format!("/{IMAGE_SECRET_SEPARATOR}/");
    let Some((prefix, embedded_repository)) = parent.rsplit_once(&separator) else {
        return false;
    };

    !prefix.is_empty() && embedded_repository == repository
}

/// Connectors with an image name starting with this value are Dekaf-type materializations. No image with this
/// name exists, instead we use it to identify which connectors get marked as `connector_type: ConnectorType::Dekaf`,
/// causing the runtime to invoke Dekaf's in-tree connector logic in `[dekaf::connector]`
pub const DEKAF_IMAGE_NAME_PREFIX: &str = "ghcr.io/estuary/dekaf-";

/// Dekaf doesn't use images, but important information such as endpoint/resource config schema are associated
/// with a particular `connector_tags` row. Rather than refactoring this deeply interconnected piece of the system,
/// we've decided to just give Dekaf a `connector_tags` row. This is its tag.
pub const DEKAF_IMAGE_TAG: &str = ":v1";

/// Dekaf service configuration
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct DekafConfig {
    /// # Dekaf variant type.
    /// Since we support integrating with a bunch of different providers via Dekaf,
    /// this allows us to store which of those connector variants this particular Dekaf connector was
    /// created as, in order to e.g link to the correct docs URL, show the correct name and logo, etc.
    pub variant: String,
    /// # Dekaf endpoint config.
    pub config: RawValue,
}

impl DekafConfig {
    pub fn image_name(&self) -> String {
        format!("{DEKAF_IMAGE_NAME_PREFIX}{}{DEKAF_IMAGE_TAG}", self.variant)
    }
}

/// Connector image and configuration specification.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct ConnectorConfig {
    /// # Image of the connector.
    pub image: String,
    /// # Configuration of the connector.
    pub config: RawValue,
}

impl ConnectorConfig {
    pub fn example() -> Self {
        Self {
            image: "connector/image:tag".to_string(),
            config: serde_json::from_str("\"connector-config.yaml\"").unwrap(),
        }
    }
}

/// Local command and its configuration.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct LocalConfig {
    /// # Command to execute
    pub command: Vec<String>,
    /// # Configuration of the command.
    pub config: RawValue,
    /// # Environment variables
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// # Use protobuf codec instead of JSON.
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub protobuf: bool,
}

impl LocalConfig {
    pub fn example() -> Self {
        Self {
            command: vec![
                "my-connector".to_string(),
                "--arg=one".to_string(),
                "--arg=two".to_string(),
            ],
            config: serde_json::from_value(serde_json::json!({"field": "value", "otherField": 42}))
                .unwrap(),
            env: BTreeMap::new(),
            protobuf: false,
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn split_image_tag_handles_registry_ports_and_digests() {
        for (image, expected) in [
            (
                "ghcr.io/estuary/source-example:v1",
                ("ghcr.io/estuary/source-example", ":v1"),
            ),
            (
                "registry.example:5000/source-example:v1",
                ("registry.example:5000/source-example", ":v1"),
            ),
            (
                "registry.example:5000/source-example@sha256:abcdef",
                ("registry.example:5000/source-example", "@sha256:abcdef"),
            ),
            (
                "registry.example:5000/source-example",
                ("registry.example:5000/source-example", ""),
            ),
            ("source-example:v1", ("source-example", ":v1")),
            ("source-example", ("source-example", "")),
            // A tag and a digest together: the repository excludes both, and
            // the tag half carries both so that the pin is never dropped.
            (
                "ghcr.io/estuary/derive-python:v1@sha256:abcdef",
                ("ghcr.io/estuary/derive-python", ":v1@sha256:abcdef"),
            ),
            (
                "registry.example:5000/source-example:v1@sha256:abcdef",
                ("registry.example:5000/source-example", ":v1@sha256:abcdef"),
            ),
            // Digests of algorithms other than sha256.
            (
                "ghcr.io/estuary/source-example@sha512:abcdef",
                ("ghcr.io/estuary/source-example", "@sha512:abcdef"),
            ),
        ] {
            let actual = super::split_image_tag(image);
            assert_eq!((actual.0.as_str(), actual.1.as_str()), expected, "{image}");
        }
    }

    #[test]
    fn image_secret_names_have_an_unambiguous_repository_boundary() {
        let repository = "registry.vendor.test/acme/source-example";

        for (secret, expected) in [
            (
                "vendor/connectors/registry.vendor.test/acme/source-example/oauth-client",
                true,
            ),
            (
                "arbitrary/connectors/prefix/connectors/registry.vendor.test/acme/source-example/oauth-client",
                true,
            ),
            (
                "vendor/registry.vendor.test/acme/source-example/oauth-client",
                false,
            ),
            (
                "connectors/registry.vendor.test/acme/source-example/oauth-client",
                false,
            ),
            (
                "vendor/connectors/registry.vendor.test/acme/source-example/nested/oauth-client",
                false,
            ),
        ] {
            assert_eq!(
                super::image_owns_secret(secret, repository),
                expected,
                "{secret}"
            );
        }

        // The longer repository is refused because it contains the separator;
        // the rightmost separator deterministically assigns this name to the
        // shorter repository instead.
        assert!(!super::image_owns_secret(
            "vendor/connectors/registry.vendor.test/connectors/registry.attacker.test/source/client",
            "registry.vendor.test/connectors/registry.attacker.test/source",
        ));
        assert!(super::image_owns_secret(
            "vendor/connectors/registry.vendor.test/connectors/registry.attacker.test/source/client",
            "registry.attacker.test/source",
        ));
    }
}
