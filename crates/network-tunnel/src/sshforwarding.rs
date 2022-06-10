use std::os::unix::prelude::PermissionsExt;
use std::process::Stdio;

use super::errors::Error;
use super::networktunnel::NetworkTunnel;

use async_trait::async_trait;
use schemars::JsonSchema;
use tokio::io::AsyncReadExt;
use tokio::process::Child;
use tokio::process::Command;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schemars(
    title = "SSH Tunnel",
    description = "Connect to your system through an SSH server that acts as a bastion host for your network."
)]
pub struct SshForwardingConfig {
    /// Endpoint of the remote SSH server that supports tunneling, in the form of ssh://user@hostname[:port]
    pub ssh_endpoint: String,
    /// Deprecated field specifying the user used to connect to the SSH endpoint.
    /// User must now be specified as part of the ssh_endpoint, however to be backward-compatible
    /// we still allow the option (but do not expose it in JSONSchema).
    /// See [`SshForwarding::backward_compatible_ssh_endpoint`] for more information
    #[serde(default)]
    #[schemars(skip)]
    pub user: Option<String>,
    /// Private key to connect to the remote SSH server.
    #[schemars(schema_with = "private_key_schema")]
    pub private_key: String,
    /// Host name to connect from the remote SSH server to the remote destination (e.g. DB) via internal network.
    pub forward_host: String,
    /// Port of the remote destination.
    #[schemars(schema_with = "forward_port_schema")]
    pub forward_port: u16,
    /// Local port to start the SSH tunnel.
    #[schemars(schema_with = "local_port_schema")]
    pub local_port: u16,
}

fn private_key_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "title": "SSH Private Key",
        "description": "The private key for connecting to the remote SSH server",
        "type": "string",
        // This annotation is interpreted by the UI to render this as a multiline input.
        "multiline": true,
    }))
    .unwrap()
}

fn forward_port_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    port_schema(
        "Forward Port",
        "The port number that the data source is listening on",
    )
}

fn local_port_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    port_schema("Local Port", "The local port number to use for the tunnel, which should match the port that's used in your base connector configuration")
}

fn port_schema(title: &str, description: &str) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "title": title,
        "description": description,
        "type": "integer",
        "minimum": 1_i32,
        "maximum": 65536_i32
    }))
    .unwrap()
}

pub struct SshForwarding {
    config: SshForwardingConfig,
    process: Option<Child>,
}

impl SshForwarding {
    pub fn new(config: SshForwardingConfig) -> Self {
        Self {
            config,
            process: None,
        }
    }

    // We used to have `user` as a field on SSHForwarding config
    // In order to be backward-compatible, we still allow that field, and if it exists we add
    // the user ourselves manually
    fn backward_compatible_ssh_endpoint(user: Option<&String>, ssh_endpoint: &String) -> String {
        match user {
            Some(user) => ssh_endpoint.replace("ssh://", &format!("ssh://{user}@")),
            None => ssh_endpoint.clone(),
        }
    }
}

#[async_trait]
impl NetworkTunnel for SshForwarding {
    async fn prepare(&mut self) -> Result<(), Error> {
        // Write the key to a temporary file
        let mut temp_key_path = std::env::temp_dir();
        temp_key_path.push("id_rsa");

        tokio::fs::write(&temp_key_path, self.config.private_key.as_bytes()).await?;
        tokio::fs::set_permissions(&temp_key_path, std::fs::Permissions::from_mode(0o600)).await?;

        let local_port = self.config.local_port;
        let ssh_endpoint = SshForwarding::backward_compatible_ssh_endpoint(
            self.config.user.as_ref(),
            &self.config.ssh_endpoint,
        );
        let forward_host = &self.config.forward_host;
        let forward_port = self.config.forward_port;

        let mut child = Command::new("ssh")
            .args(vec![
                // Disable psuedo-terminal allocation
                "-T".to_string(),
                // Be verbose so we can pick up signals about status of the tunnel
                "-v".to_string(),
                // This is necessary unless we also ask for the public key from users
                "-o".to_string(),
                "StrictHostKeyChecking no".to_string(),
                // Pass the private key
                "-i".to_string(),
                temp_key_path.into_os_string().into_string().unwrap(),
                // Do not execute a remote command. Just forward the ports.
                "-N".to_string(),
                // Port forwarding stanza
                "-L".to_string(),
                format!("{local_port}:{forward_host}:{forward_port}"),
                ssh_endpoint,
            ])
            .stderr(Stdio::piped())
            .spawn()?;

        // Read stderr of SSH until we find "Local forwarding listening", which means
        // the ports are open and we are ready to serve requests
        let mut stderr = child.stderr.take().unwrap();
        let mut last_line = String::new();
        loop {
            let mut buffer = [0; 64];

            let n = stderr.read(&mut buffer).await?;

            if n == 0 {
                break;
            }

            let read_str = std::str::from_utf8(&buffer).unwrap();
            tracing::debug!(read_str);
            last_line.push_str(read_str);

            if last_line.contains("Local forwarding listening") {
                break;
            }
            let split_by_newline: Vec<_> = last_line.split('\n').collect();
            last_line = split_by_newline
                .last()
                .map(|s| s.to_string())
                .unwrap_or(String::new());
        }

        self.process = Some(child);

        Ok(())
    }

    async fn start_serve(&mut self) -> Result<(), Error> {
        self.process.as_mut().unwrap().wait().await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::sshforwarding::SshForwarding;

    #[test]
    fn test_backward_compatible_ssh_endpoint() {
        assert_eq!(
            SshForwarding::backward_compatible_ssh_endpoint(
                Some(&"user".to_string()),
                &"ssh://estuary.dev:22".to_string(),
            ),
            "ssh://user@estuary.dev:22"
        );

        assert_eq!(
            SshForwarding::backward_compatible_ssh_endpoint(
                None,
                &"ssh://estuary.dev:22".to_string(),
            ),
            "ssh://estuary.dev:22"
        );

        assert_eq!(
            SshForwarding::backward_compatible_ssh_endpoint(
                None,
                &"ssh://user@estuary.dev:22".to_string(),
            ),
            "ssh://user@estuary.dev:22"
        );
    }
}
