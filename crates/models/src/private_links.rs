use serde::{Deserialize, Serialize};

/// Private link configuration for a customer-owned data plane: AWS
/// PrivateLink, Azure Private Link, or GCP Private Service Connect.
// `#[serde(untagged)]` matches each variant by its required fields, preserving
// the `private_links json[]` column shape consumed by the data-plane
// controller. The types previously lived in `data-plane-controller::shared::stack`
// and are re-exported there for existing DPC callers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::Union, async_graphql::OneofObject),
    graphql(name = "PrivateLinkConfig", input_name = "PrivateLinkConfigInput")
)]
#[serde(untagged)]
pub enum PrivateLink {
    AWS(AWSPrivateLink),
    Azure(AzurePrivateLink),
    GCP(GCPPrivateServiceConnect),
}

/// Cloud provider of a private link. Distinct from a data plane's provider
/// (a private link is never local) and used to disambiguate the service
/// identity, since AWS and Azure links both key on `service_name`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::Enum))]
#[serde(rename_all = "lowercase")]
pub enum PrivateLinkProvider {
    Aws,
    Azure,
    Gcp,
}

impl PrivateLinkProvider {
    /// Lowercase wire/DB representation (`aws`/`azure`/`gcp`).
    pub fn as_str(&self) -> &'static str {
        match self {
            PrivateLinkProvider::Aws => "aws",
            PrivateLinkProvider::Azure => "azure",
            PrivateLinkProvider::Gcp => "gcp",
        }
    }
}

impl PrivateLink {
    /// The provider's service identifier for this link: `service_name` for AWS
    /// and Azure, `service_attachment` for GCP. These are required fields, so
    /// every link has one. It is the stable per-link identity and the join key
    /// against the data plane's provisioned endpoint results.
    pub fn service_identity(&self) -> &str {
        match self {
            PrivateLink::AWS(link) => &link.service_name,
            PrivateLink::Azure(link) => &link.service_name,
            PrivateLink::GCP(link) => &link.service_attachment,
        }
    }

    /// The cloud provider of this link, derived from its variant.
    pub fn provider(&self) -> PrivateLinkProvider {
        match self {
            PrivateLink::AWS(_) => PrivateLinkProvider::Aws,
            PrivateLink::Azure(_) => PrivateLinkProvider::Azure,
            PrivateLink::GCP(_) => PrivateLinkProvider::Gcp,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::SimpleObject, async_graphql::InputObject),
    graphql(name = "AWSPrivateLink", input_name = "AWSPrivateLinkInput")
)]
pub struct AWSPrivateLink {
    pub region: String,
    pub az_ids: Vec<String>,
    pub service_name: String,
    // AWS region of the PrivateLink service when it differs from the endpoint's
    // region (cross-region PrivateLink). When unset, est-dry-dock defaults to
    // `region`. Mirrors `service_region` in the est-dry-dock Pydantic model.
    #[serde(
        default,
        deserialize_with = "deserialize_empty_string_as_none",
        skip_serializing_if = "is_none_or_empty"
    )]
    pub service_region: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::SimpleObject, async_graphql::InputObject),
    graphql(name = "AzurePrivateLink", input_name = "AzurePrivateLinkInput")
)]
pub struct AzurePrivateLink {
    pub service_name: String,
    pub location: String,
    // `dns_name` and `resource_type` are optional. On the wire they round-trip
    // as "field absent" for None and as the string value for Some; an incoming
    // empty string is normalized to None on deserialize and on serialize, which
    // preserves byte-for-byte compatibility with historical rows that wrote
    // either a missing field or `""`.
    #[serde(
        default,
        deserialize_with = "deserialize_empty_string_as_none",
        skip_serializing_if = "is_none_or_empty"
    )]
    pub dns_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_empty_string_as_none",
        skip_serializing_if = "is_none_or_empty"
    )]
    pub resource_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "async-graphql",
    derive(async_graphql::SimpleObject, async_graphql::InputObject),
    graphql(
        name = "GCPPrivateServiceConnect",
        input_name = "GCPPrivateServiceConnectInput"
    )
)]
pub struct GCPPrivateServiceConnect {
    pub service_attachment: String,
    pub region: String,
    pub dns_zone_name: String,
    pub dns_record_names: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    #[cfg_attr(feature = "async-graphql", graphql(default))]
    pub all_ports: bool,
}

fn is_false(b: &bool) -> bool {
    !b
}

fn is_none_or_empty(opt: &Option<String>) -> bool {
    opt.as_deref().map_or(true, str::is_empty)
}

fn deserialize_empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.filter(|s| !s.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn azure_optional_fields_round_trip() {
        // Three historical shapes for the optional fields all parse to the
        // same Azure variant, and serializing omits them entirely.
        let absent = r#"{"service_name":"svc","location":"eastus"}"#;
        let empty =
            r#"{"service_name":"svc","location":"eastus","dns_name":"","resource_type":""}"#;
        let some =
            r#"{"service_name":"svc","location":"eastus","dns_name":"d","resource_type":"t"}"#;

        for raw in [absent, empty] {
            let link: PrivateLink = serde_json::from_str(raw).unwrap();
            let PrivateLink::Azure(azure) = &link else {
                panic!("expected Azure variant for {raw}");
            };
            assert_eq!(azure.dns_name, None);
            assert_eq!(azure.resource_type, None);
            // Round-trip serializes to the canonical absent shape.
            assert_eq!(serde_json::to_string(&link).unwrap(), absent);
        }

        let link: PrivateLink = serde_json::from_str(some).unwrap();
        let PrivateLink::Azure(azure) = &link else {
            panic!("expected Azure variant");
        };
        assert_eq!(azure.dns_name.as_deref(), Some("d"));
        assert_eq!(azure.resource_type.as_deref(), Some("t"));
        assert_eq!(serde_json::to_string(&link).unwrap(), some);
    }

    #[test]
    fn aws_service_region_round_trip() {
        // `service_region` is optional; an absent field and an empty string
        // both parse to None and serialize back to the canonical absent shape,
        // matching the Azure optionals.
        let absent = r#"{"region":"us-east-1","az_ids":["use1-az1"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc"}"#;
        let empty = r#"{"region":"us-east-1","az_ids":["use1-az1"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc","service_region":""}"#;
        let some = r#"{"region":"us-east-1","az_ids":["use1-az1"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc","service_region":"us-west-2"}"#;

        for raw in [absent, empty] {
            let link: PrivateLink = serde_json::from_str(raw).unwrap();
            let PrivateLink::AWS(aws) = &link else {
                panic!("expected AWS variant for {raw}");
            };
            assert_eq!(aws.service_region, None);
            assert_eq!(serde_json::to_string(&link).unwrap(), absent);
        }

        let link: PrivateLink = serde_json::from_str(some).unwrap();
        let PrivateLink::AWS(aws) = &link else {
            panic!("expected AWS variant");
        };
        assert_eq!(aws.service_region.as_deref(), Some("us-west-2"));
        assert_eq!(serde_json::to_string(&link).unwrap(), some);
    }

    #[test]
    fn untagged_dispatch_order() {
        // Variant dispatch is determined by required-field presence in the
        // declared AWS, Azure, GCP order. Each provider matches only on its
        // unique required field set.
        let aws: PrivateLink = serde_json::from_str(
            r#"{"region":"us-east-1","az_ids":["use1-az1"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc"}"#,
        ).unwrap();
        assert!(matches!(aws, PrivateLink::AWS(_)));

        let azure: PrivateLink =
            serde_json::from_str(r#"{"service_name":"svc","location":"eastus"}"#).unwrap();
        assert!(matches!(azure, PrivateLink::Azure(_)));

        let gcp: PrivateLink = serde_json::from_str(
            r#"{"service_attachment":"projects/p/regions/r/serviceAttachments/sa","region":"r","dns_zone_name":"z","dns_record_names":["n"]}"#,
        ).unwrap();
        assert!(matches!(gcp, PrivateLink::GCP(_)));
    }

    #[test]
    fn gcp_all_ports_default_omitted() {
        let gcp: PrivateLink = serde_json::from_str(
            r#"{"service_attachment":"projects/p/regions/r/serviceAttachments/sa","region":"r","dns_zone_name":"z","dns_record_names":["n"]}"#,
        ).unwrap();
        let PrivateLink::GCP(g) = &gcp else {
            unreachable!()
        };
        assert!(!g.all_ports);
        // False default is skipped on serialize.
        assert!(!serde_json::to_string(&gcp).unwrap().contains("all_ports"));
    }

    #[test]
    fn service_identity_and_provider_per_variant() {
        let aws: PrivateLink = serde_json::from_str(
            r#"{"region":"us-east-1","az_ids":["use1-az1"],"service_name":"com.amazonaws.vpce.us-east-1.vpce-svc-abc"}"#,
        ).unwrap();
        assert_eq!(
            aws.service_identity(),
            "com.amazonaws.vpce.us-east-1.vpce-svc-abc"
        );
        assert_eq!(aws.provider(), PrivateLinkProvider::Aws);

        let azure: PrivateLink =
            serde_json::from_str(r#"{"service_name":"/subscriptions/x/svc","location":"eastus"}"#)
                .unwrap();
        assert_eq!(azure.service_identity(), "/subscriptions/x/svc");
        assert_eq!(azure.provider(), PrivateLinkProvider::Azure);

        let gcp: PrivateLink = serde_json::from_str(
            r#"{"service_attachment":"projects/p/regions/r/serviceAttachments/sa","region":"r","dns_zone_name":"z","dns_record_names":["n"]}"#,
        ).unwrap();
        assert_eq!(
            gcp.service_identity(),
            "projects/p/regions/r/serviceAttachments/sa"
        );
        assert_eq!(gcp.provider(), PrivateLinkProvider::Gcp);
        assert_eq!(gcp.provider().as_str(), "gcp");
    }
}
