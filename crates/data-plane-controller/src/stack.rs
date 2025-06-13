use itertools::{EitherOrBoth, Itertools};

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub enum Status {
    Idle,
    /// Controller is setting the encryption key for Pulumi stack secrets.
    SetEncryption,
    /// Controller is previewing changes proposed by Pulumi without applying them.
    PulumiPreview,
    /// Controller is refreshing any remotely-changed resources,
    /// such as replaced EC2 instances.
    PulumiRefresh,
    /// Controller is creating any scaled-up cloud resources,
    /// updating DNS records for resources which are scaling down,
    /// and updating the Ansible inventory.
    PulumiUp1,
    /// Controller is awaiting DNS propagation for any replaced resources
    /// as well as resources which are scaling down.
    AwaitDNS1,
    /// Controller is running Ansible to initialize and refresh servers.
    Ansible,
    /// Controller is updating DNS records for resources which have now
    /// started and is destroying any scaled-down cloud resources which
    /// have now stopped.
    PulumiUp2,
    /// Controller is awaiting DNS propagation for any scaled-up
    /// resources which have now started.
    AwaitDNS2,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct State {
    // DataPlane which this controller manages.
    pub data_plane_id: models::Id,
    // Git branch of the dry-dock repo for this data-plane.
    pub deploy_branch: String,
    // DateTime of the last `pulumi up` for this data-plane.
    pub last_pulumi_up: chrono::DateTime<chrono::Utc>,
    // DateTime of the last `pulumi refresh` for this data-plane.
    pub last_refresh: chrono::DateTime<chrono::Utc>,
    // Token to which controller logs are directed.
    pub logs_token: sqlx::types::Uuid,
    // Pulumi configuration for this data-plane.
    pub stack: PulumiStack,
    // Name of the data-plane "stack" within the Pulumi tooling.
    pub stack_name: String,
    // Status of this controller.
    pub status: Status,

    // Is this controller disabled?
    // When disabled, refresh and converge operations are queued but not run.
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,

    // Is there a pending preview for this data-plane?
    #[serde(default, skip_serializing_if = "is_false")]
    pub pending_preview: bool,
    // If pending a preview, on which branch should the preview run?
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub preview_branch: String,
    // Is there a pending refresh for this data-plane?
    #[serde(default, skip_serializing_if = "is_false")]
    pub pending_refresh: bool,
    // Is there a pending converge for this data-plane?
    #[serde(default, skip_serializing_if = "is_false")]
    pub pending_converge: bool,

    // When Some, updated Pulumi stack exports to be written back into the `data_planes` row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_exports: Option<ControlExports>,
    // When true, an updated Pulumi stack model to be written back into the `data_planes` row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_stack: Option<PulumiStack>,
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_byoc: Option<AzureBYOC>,
    pub builds_root: url::Url,
    pub builds_kms_keys: Vec<String>,
    pub control_plane_api: url::Url,
    pub data_buckets: Vec<url::Url>,
    pub gcp_project: String,
    pub ssh_subnets: Vec<ipnetwork::IpNetwork>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub private_links: Vec<PrivateLink>,
    pub deployments: Vec<Deployment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_limits: Option<ConnectorLimits>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConnectorLimits {
    pub cpu: String,
    pub memory: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AWSAssumeRole {
    pub role_arn: String,
    pub external_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AzureBYOC {
    pub tenant_id: String,
    pub subscription_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum PrivateLink {
    AWS(AWSPrivateLink),
    Azure(AzurePrivateLink),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AWSPrivateLink {
    pub region: String,
    pub az_ids: Vec<String>,
    pub service_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AzurePrivateLink {
    pub service_name: String,
    pub location: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Etcd,
    Gazette,
    Reactor,
    Bastion,
    Dekaf,
}

/// A Deployment under a rollout will be updated with each data-plane
/// convergence, by adjusting the Deployment's `desired` toward the
/// rollout `target` by at-most `step`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Rollout {
    /// Target `desired` of the Deployment once complete.
    pub target: usize,
    /// Maximum amount to increment or decrement `desired` towards `target`.
    pub step: usize,
}

/// A Release is matched against a current Deployment of a data-plane.
/// When matched, it begins a new rollout which swaps the current
/// Deployment for a new one.
#[derive(Clone, Debug)]
pub struct Release {
    /// Previous OCI image which is matched to a Deployment.
    pub prev_image: String,
    /// Next OCI image which is applied in the released Deployment.
    pub next_image: String,
    /// Rollout step.
    /// - When positive, a "surge" rollout is performed which first surges
    ///   `next_image` capacity and then removes `prev_image` capacity.
    /// - When negative, a "replace" rollout is performed which first removes
    ///   `prev_image` capacity and then replaces with `next_image` capacity.
    pub step: i32,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollout: Option<Rollout>,
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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ControlExports {
    pub aws_iam_user_arn: String,
    pub aws_link_endpoints: Vec<serde_json::Value>,
    pub azure_application_client_id: String,
    pub azure_application_name: String,
    pub azure_link_endpoints: Vec<serde_json::Value>,
    pub bastion_tunnel_private_key: Option<String>,
    pub cidr_blocks: Vec<ipnetwork::IpNetwork>,
    pub gcp_service_account_email: String,
    pub hmac_keys: Vec<String>,
    pub encrypted_hmac_keys: serde_json::Value,
    pub ssh_key: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct PulumiStackResourceChanges {
    #[serde(default)]
    pub same: usize,
    #[serde(default)]
    pub update: usize,
    #[serde(default)]
    pub delete: usize,
    #[serde(default)]
    pub create: usize,
}

impl PulumiStackResourceChanges {
    pub fn changed(&self) -> bool {
        return self.update > 0 || self.delete > 0 || self.create > 0;
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PulumiStackHistory {
    pub resource_changes: PulumiStackResourceChanges,
}

impl State {
    pub fn verify_transition(last: &Self, next: &Self) -> anyhow::Result<()> {
        if last.stack_name != next.stack_name {
            anyhow::bail!(
                "pulumi stack name cannot change from {} to {}",
                last.stack_name,
                next.stack_name,
            );
        }
        if last.logs_token != next.logs_token {
            anyhow::bail!(
                "data-plane logs token cannot change from {} to {}",
                last.logs_token,
                next.logs_token,
            );
        }
        PulumiStack::verify_transition(&last.stack, &next.stack)
    }
}

impl PulumiStack {
    pub fn verify_transition(last: &Self, next: &Self) -> anyhow::Result<()> {
        if last.encrypted_key != next.encrypted_key {
            anyhow::bail!(
                "pulumi stack encrypted key cannot change from {} to {}",
                last.encrypted_key,
                next.encrypted_key,
            );
        }
        if last.secrets_provider != next.secrets_provider {
            anyhow::bail!(
                "pulumi stack secrets provider cannot change from {} to {}",
                last.secrets_provider,
                next.secrets_provider,
            );
        }
        DataPlane::verify_transition(&last.config.model, &next.config.model)
    }
}

impl DataPlane {
    pub fn verify_transition(last: &Self, next: &Self) -> anyhow::Result<()> {
        if last.gcp_project != next.gcp_project {
            anyhow::bail!(
                "pulumi stack gcp_project cannot change from {} to {}",
                last.gcp_project,
                next.gcp_project,
            );
        }
        for (index, zipped) in (last.deployments)
            .iter()
            .zip_longest(next.deployments.iter())
            .enumerate()
        {
            match zipped {
                EitherOrBoth::Left(cur_deployment) => {
                    anyhow::bail!(
                        "cannot remove deployment {} at index {index}; scale it down with `desired` = 0 instead",
                        serde_json::to_string(cur_deployment).unwrap()
                    );
                }
                EitherOrBoth::Right(next_deployment) => {
                    if next_deployment.current != 0 {
                        anyhow::bail!(
                            "new deployment {} at index {index} must have `current` = 0; scale up using `desired` instead",
                            serde_json::to_string(next_deployment).unwrap()
                        );
                    } else if next_deployment.desired == 0 && next_deployment.rollout.is_none() {
                        anyhow::bail!(
                            "new deployment {} at index {index} must have `desired` > 0",
                            serde_json::to_string(next_deployment).unwrap()
                        );
                    }
                }
                EitherOrBoth::Both(
                    current @ Deployment {
                        current: cur_current,
                        oci_image: cur_oci_image,
                        role: cur_role,
                        template: cur_template,
                        desired: _,            // Allowed to change.
                        oci_image_override: _, // Allowed to change.
                        rollout: _,            // Allowed to change.
                    },
                    next @ Deployment {
                        current: next_current,
                        oci_image: next_oci_image,
                        role: next_role,
                        template: next_template,
                        desired: _,            // Allowed to change.
                        oci_image_override: _, // Allowed to change.
                        rollout: _,            // Allowed to change.
                    },
                ) => {
                    if cur_current != next_current
                        || cur_oci_image != next_oci_image
                        || cur_role != next_role
                        || cur_template != next_template
                    {
                        anyhow::bail!(
                            "invalid transition of deployment at index {index} (you may only append new deployments or update `desired` or `oci_image_override` of this one): {} =!=> {}",
                            serde_json::to_string(current).unwrap(),
                            serde_json::to_string(next).unwrap(),
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Evaluate the `rollout` polices attached to Deployments,
    /// and potentially perform a coordinated rollout step which updates the
    /// desired replicas of deployments subject to a rollout.
    /// Returns true iff a change was made to any Deployment.
    pub fn evaluate_release_steps(&mut self, releases: &[Release]) -> bool {
        assert!(self
            .deployments
            .iter()
            .all(|deployment| deployment.current == deployment.desired));

        let mut changed = false;

        // Apply steps to all deployments with a rollout.
        for deployment in self.deployments.iter_mut() {
            changed = deployment.step_rollout() || changed;
        }

        // Find deployments which match a release, and start new a rollout.
        let mut added = Vec::new();

        for last in self.deployments.iter_mut() {
            if last.rollout.is_some() {
                continue; // Must complete current rollout before starting a next.
            }
            let Some(release) = releases.iter().find(|r| r.prev_image == last.oci_image) else {
                continue;
            };

            let mut next = Deployment {
                current: 0,
                desired: 0,
                oci_image: release.next_image.clone(),
                oci_image_override: None,
                role: last.role.clone(),
                rollout: Some(Rollout {
                    step: release.step.abs() as usize,
                    target: last.desired,
                }),
                template: last.template.clone(),
            };

            last.rollout = Some(Rollout {
                step: release.step.abs() as usize,
                target: 0,
            });

            if release.step < 0 {
                // Replace by stepping down the `last` deployment now,
                // and then stepping up `next` after convergence.
                last.step_rollout();
            } else {
                // Surge by stepping up the `next` deployment now,
                // and then stepping down `last` after convergence.
                next.step_rollout();
            };

            added.push(next);
            changed = true;
        }

        self.deployments.extend(added);
        changed
    }
}

impl Deployment {
    pub fn mark_current(&mut self) -> bool {
        self.current = self.desired;

        if matches!(&self.rollout, Some(rollout) if rollout.target == self.current) {
            self.rollout = None;
        }
        self.current != 0 || self.rollout.is_some()
    }

    pub fn step_rollout(&mut self) -> bool {
        let Some(rollout) = &self.rollout else {
            return false;
        };
        if self.desired < rollout.target {
            self.desired += rollout.step.min(rollout.target - self.desired);
        } else {
            self.desired -= rollout.step.min(self.desired - rollout.target);
        }
        true
    }
}

fn is_false(b: &bool) -> bool {
    !b
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::value::Value;
    use std::collections::HashMap;

    #[test]
    fn data_plane_parse() {
        let fixtures =
            serde_json::from_str::<HashMap<String, Value>>(include_str!("data_plane_fixture.json"))
                .unwrap();

        assert_eq!(
            serde_json::from_value::<DataPlane>(fixtures.get("aws_private_link").unwrap().clone())
                .unwrap()
                .private_links[0],
            PrivateLink::AWS(AWSPrivateLink {
                az_ids: vec!["a".to_string(), "b".to_string()],
                region: "us-west-2".to_string(),
                service_name: "service".to_string(),
            }),
        );

        let azure_parsed = serde_json::from_value::<DataPlane>(
            fixtures.get("azure_private_link").unwrap().clone(),
        )
        .unwrap();
        assert_eq!(
            azure_parsed.private_links[0],
            PrivateLink::Azure(AzurePrivateLink {
                location: "eastus".to_string(),
                service_name: "service".to_string(),
            }),
        );
        assert_eq!(
            azure_parsed.azure_byoc,
            Some(AzureBYOC {
                subscription_id: "12345678".to_string(),
                tenant_id: "910111213".to_string(),
            }),
        );
    }

    #[test]
    fn transition_checks() {
        let last: State = serde_json::from_str(include_str!("state_fixture.json")).unwrap();

        insta::assert_debug_snapshot!(State::verify_transition(
            &last,
            &State {
                stack_name: "invalid".to_string(),
                ..last.clone()
            }
        ).unwrap_err(), @r###""pulumi stack name cannot change from private-AcmeCo-aws-us-west-2-c1 to invalid""###);

        insta::assert_debug_snapshot!(State::verify_transition(
            &last,
            &State {
                logs_token: sqlx::types::Uuid::nil(),
                ..last.clone()
            }
        ).unwrap_err(), @r###""data-plane logs token cannot change from 4cb6ceef-36bc-4f57-89b9-8e4b11f82f0b to 00000000-0000-0000-0000-000000000000""###);

        let last = last.stack;

        insta::assert_debug_snapshot!(PulumiStack::verify_transition(
            &last,
            &PulumiStack{
                encrypted_key: "invalid".to_string(),
                ..last.clone()
            }
        ).unwrap_err(), @r###""pulumi stack encrypted key cannot change from encryptedkey to invalid""###);

        insta::assert_debug_snapshot!(PulumiStack::verify_transition(
            &last,
            &PulumiStack{
                secrets_provider: "invalid".to_string(),
                ..last.clone()
            }
        ).unwrap_err(), @r###""pulumi stack secrets provider cannot change from gcpkms://projects/the-project/locations/us-central1/keyRings/the-key-ring/cryptoKeys/the-key to invalid""###);

        let last = last.config.model;

        insta::assert_debug_snapshot!(DataPlane::verify_transition(
            &last,
            &DataPlane{
                gcp_project: "invalid".to_string(),
                ..last.clone()
            }
        ).unwrap_err(), @r###""pulumi stack gcp_project cannot change from the-gcp-project to invalid""###);

        let mut deployments = last.deployments.clone();
        deployments[0].current = 32;

        insta::assert_debug_snapshot!(DataPlane::verify_transition(
            &last,
            &DataPlane{
                deployments,
                ..last.clone()
            }
        ).unwrap_err(), @r###""invalid transition of deployment at index 0 (you may only append new deployments or update `desired` or `oci_image_override` of this one): {\"role\":\"etcd\",\"template\":{\"ami_image_id\":\"ami-01a8b7cc84780badb\",\"instance_type\":\"m5d.large\",\"provider\":\"aws\",\"region\":\"us-west-2\",\"zone\":\"a\"},\"oci_image\":\"quay.io/coreos/etcd:v3.5.17\",\"desired\":3,\"current\":3} =!=> {\"role\":\"etcd\",\"template\":{\"ami_image_id\":\"ami-01a8b7cc84780badb\",\"instance_type\":\"m5d.large\",\"provider\":\"aws\",\"region\":\"us-west-2\",\"zone\":\"a\"},\"oci_image\":\"quay.io/coreos/etcd:v3.5.17\",\"desired\":3,\"current\":32}""###);

        let mut deployments = last.deployments.clone();
        deployments.pop();

        insta::assert_debug_snapshot!(DataPlane::verify_transition(
            &last,
            &DataPlane{
                deployments,
                ..last.clone()
            }
        ).unwrap_err(), @r###""cannot remove deployment {\"role\":\"reactor\",\"template\":{\"ami_image_id\":\"ami-01a8b7cc84780badb\",\"instance_type\":\"r5d.xlarge\",\"provider\":\"aws\",\"region\":\"us-west-2\",\"zone\":\"a\"},\"oci_image\":\"ghcr.io/estuary/flow:v0.5.11\",\"desired\":7,\"current\":7} at index 2; scale it down with `desired` = 0 instead""###);

        let mut deployments = last.deployments.clone();
        deployments.push(Deployment {
            current: 32,
            ..deployments[0].clone()
        });

        insta::assert_debug_snapshot!(DataPlane::verify_transition(
            &last,
            &DataPlane{
                deployments,
                ..last.clone()
            }
        ).unwrap_err(), @r###""new deployment {\"role\":\"etcd\",\"template\":{\"ami_image_id\":\"ami-01a8b7cc84780badb\",\"instance_type\":\"m5d.large\",\"provider\":\"aws\",\"region\":\"us-west-2\",\"zone\":\"a\"},\"oci_image\":\"quay.io/coreos/etcd:v3.5.17\",\"desired\":3,\"current\":32} at index 3 must have `current` = 0; scale up using `desired` instead""###);

        let mut deployments = last.deployments.clone();
        deployments.push(Deployment {
            current: 0,
            desired: 0,
            ..deployments[0].clone()
        });

        insta::assert_debug_snapshot!(DataPlane::verify_transition(
            &last,
            &DataPlane{
                deployments,
                ..last.clone()
            }
        ).unwrap_err(), @r###""new deployment {\"role\":\"etcd\",\"template\":{\"ami_image_id\":\"ami-01a8b7cc84780badb\",\"instance_type\":\"m5d.large\",\"provider\":\"aws\",\"region\":\"us-west-2\",\"zone\":\"a\"},\"oci_image\":\"quay.io/coreos/etcd:v3.5.17\",\"desired\":0,\"current\":0} at index 3 must have `desired` > 0""###);

        let mut deployments = last.deployments.clone();
        deployments.push(Deployment {
            current: 0,
            desired: 0,
            rollout: Some(Rollout { step: 1, target: 1 }),
            ..deployments[0].clone()
        });

        assert!(matches!(
            DataPlane::verify_transition(
                &last,
                &DataPlane {
                    deployments,
                    ..last.clone()
                }
            ),
            Ok(_)
        ));
    }

    #[test]
    fn simulate_noop_rollout() {
        let State { mut stack, .. } =
            serde_json::from_str(include_str!("state_fixture.json")).unwrap();

        let releases = &[Release {
            prev_image: "not/matched:v1".to_string(),
            next_image: "not/matched:v2".to_string(),
            step: 3,
        }];

        insta::assert_json_snapshot!(&simulate_rollout(&mut stack.config.model, releases));
    }

    #[test]
    fn simulate_etcd_rollout() {
        let State { mut stack, .. } =
            serde_json::from_str(include_str!("state_fixture.json")).unwrap();

        let releases = &[Release {
            prev_image: "quay.io/coreos/etcd:v3.5.17".to_string(),
            next_image: "quay.io/coreos/etcd:next".to_string(),
            step: -1,
        }];

        insta::assert_json_snapshot!(&simulate_rollout(&mut stack.config.model, releases));
    }

    #[test]
    fn simulate_gazette_and_reactor_rollout() {
        let State { mut stack, .. } =
            serde_json::from_str(include_str!("state_fixture.json")).unwrap();

        let releases = &[
            Release {
                prev_image: "ghcr.io/gazette/broker:v0.100".to_string(),
                next_image: "ghcr.io/gazette/broker:next".to_string(),
                step: 3,
            },
            Release {
                prev_image: "ghcr.io/estuary/flow:v0.5.11".to_string(),
                next_image: "ghcr.io/estuary/flow:next".to_string(),
                step: 3,
            },
            Release {
                prev_image: "not/matched:v1".to_string(),
                next_image: "not/matched:v2".to_string(),
                step: 3,
            },
        ];

        insta::assert_json_snapshot!(&simulate_rollout(&mut stack.config.model, releases));
    }

    #[test]
    fn simulate_chained_surge_releases() {
        let State { mut stack, .. } =
            serde_json::from_str(include_str!("state_fixture.json")).unwrap();

        stack
            .config
            .model
            .deployments
            .retain(|d| d.role == Role::Gazette);

        let releases = &[
            Release {
                prev_image: "ghcr.io/gazette/broker:v0.100".to_string(),
                next_image: "ghcr.io/gazette/broker:v1".to_string(),
                step: 100,
            },
            Release {
                prev_image: "ghcr.io/gazette/broker:v1".to_string(),
                next_image: "ghcr.io/gazette/broker:v2".to_string(),
                step: 100,
            },
            Release {
                prev_image: "ghcr.io/gazette/broker:v2".to_string(),
                next_image: "ghcr.io/gazette/broker:v3".to_string(),
                step: 7,
            },
        ];

        insta::assert_json_snapshot!(&simulate_rollout(&mut stack.config.model, releases));
    }

    #[test]
    fn simulate_chained_replace_release() {
        let State { mut stack, .. } =
            serde_json::from_str(include_str!("state_fixture.json")).unwrap();

        stack
            .config
            .model
            .deployments
            .retain(|d| d.role == Role::Etcd);

        let releases = &[
            Release {
                prev_image: "quay.io/coreos/etcd:v3.5.17".to_string(),
                next_image: "quay.io/coreos/etcd:v3.6".to_string(),
                step: -1,
            },
            Release {
                prev_image: "quay.io/coreos/etcd:v3.6".to_string(),
                next_image: "quay.io/coreos/etcd:v3.7".to_string(),
                step: -1,
            },
        ];

        insta::assert_json_snapshot!(&simulate_rollout(&mut stack.config.model, releases));
    }

    fn simulate_rollout(model: &mut DataPlane, releases: &[Release]) -> Vec<Vec<Deployment>> {
        let mut outcomes = Vec::new();
        outcomes.push(model.deployments.clone());

        loop {
            let changed = model.evaluate_release_steps(releases);
            outcomes.push(model.deployments.clone());

            if !changed {
                return outcomes;
            }

            model.deployments.retain_mut(Deployment::mark_current);
        }
    }
}
