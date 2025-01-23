#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PulumiStack {
    #[serde(rename = "secretsprovider")]
    pub secrets_provider: String,
    #[serde(
        default,
        rename = "encryptedkey",
        skip_serializing_if = "String::is_empty"
    )]
    pub encrypted_key: String,
    pub config: PulumiStackConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PulumiStackConfig {
    #[serde(rename = "est-dry-dock:model")]
    pub model: DataPlane,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DataPlane {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fqdn: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_assume_role: Option<AWSAssumeRole>,
    pub builds_root: url::Url,
    pub builds_kms_keys: Vec<String>,
    pub control_plane_api: url::Url,
    pub data_buckets: Vec<url::Url>,
    pub gcp_project: String,
    pub ssh_subnets: Vec<ipnetwork::IpNetwork>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub private_links: Vec<AWSPrivateLink>,
    pub deployments: Vec<Deployment>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AWSAssumeRole {
    pub role_arn: String,
    pub external_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AWSPrivateLink {
    pub region: String,
    pub az_ids: Vec<String>,
    pub service_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Etcd,
    Gazette,
    Reactor,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Deployment {
    pub role: Role,
    pub template: serde_json::Value,
    pub oci_image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oci_image_override: Option<String>,
    pub desired: usize,
    pub current: usize,
}

#[derive(Debug, serde::Deserialize)]
pub struct PulumiExports {
    pub ansible: AnsibleInventory,
    pub control: ControlExports,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AnsibleInventory {
    pub all: AnsibleInventoryAll,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AnsibleInventoryAll {
    pub children: std::collections::BTreeMap<String, AnsibleRole>,
    pub vars: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AnsibleRole {
    pub hosts: std::collections::BTreeMap<String, AnsibleHost>,
    pub vars: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AnsibleHost {
    pub ansible_host: std::net::IpAddr,
    pub ansible_user: String,
    pub host_fqdn: String,
    pub local_cert_pem: String,
    pub local_private_key_pem: String,
    pub oci_image: String,
    pub private_ip4: Option<std::net::Ipv4Addr>,
    pub private_ip6: Option<std::net::Ipv6Addr>,
    pub provider: String,
    pub public_ip4: std::net::Ipv4Addr,
    pub public_ip6: std::net::Ipv6Addr,
    pub role: String,
    pub role_fqdn: String,
    pub starting: bool,
    pub stopping: bool,
    pub zone: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ControlExports {
    pub aws_iam_user_arn: String,
    pub aws_link_endpoints: Vec<serde_json::Value>,
    pub cidr_blocks: Vec<ipnetwork::IpNetwork>,
    pub gcp_service_account_email: String,
    pub hmac_keys: Vec<String>,
    pub ssh_key: String,
}
