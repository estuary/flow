use std::any::Any;
use std::io::ErrorKind;
use std::os::unix::prelude::PermissionsExt;
use std::process::Stdio;

use super::errors::Error;
use super::networktunnel::NetworkTunnel;

use async_trait::async_trait;
use rand::Rng;
use schemars::JsonSchema;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio::process::Command;

use serde::{Deserialize, Serialize};

pub const ENDPOINT_ADDRESS_KEY: &str = "address";

#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
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
    /// The hostname of the remote destination (e.g. the database server).
    #[serde(default)]
    #[schemars(skip)]
    pub forward_host: String,
    /// The port of the remote destination (e.g. the database server).
    #[serde(default)]
    #[schemars(skip)]
    pub forward_port: u16,
    /// The local port which will be connected to the remote host/port over an SSH tunnel.
    /// This should match the port that's used in your basic connector configuration.
    #[serde(default)]
    #[schemars(skip)]
    pub local_port: u16,
}

fn private_key_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "title": "SSH Private Key",
        "description": "The private key for connecting to the remote SSH server",
        "type": "string",
        // This annotation is interpreted by the UI to render this as a multiline input.
        "multiline": true,
        "secret": true
    }))
    .unwrap()
}

pub struct SshForwarding {
    config: SshForwardingConfig,
    process: Option<Child>,
}

fn split_host_port(hostport: String) -> Option<(String, u16)> {
    let mut splits = hostport.as_str().splitn(2, ':');
    let host = splits.next()?.to_string();
    let port: u16 = splits.next()?.parse().ok()?;
    Some((host, port))
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
    fn adjust_endpoint_spec(
        &mut self,
        mut endpoint_spec: serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        // If any of the `forward_host`, `forward_port`, or `local_port` properties are
        // set then the user is assumed to want explicit/manual configuration and we
        // don't need to perform any further adjustment. If they're all unset we will
        // proceed to configure things automagically.
        if self.config.forward_host != ""
            || self.config.forward_port != 0
            || self.config.local_port != 0
        {
            tracing::warn!(
                "ssh tunneling with explicit host/port config: forwarding local port {} to remote host {}:{}",
                self.config.local_port,
                self.config.forward_host,
                self.config.forward_port
            );
            return Ok(endpoint_spec);
        }

        let address = endpoint_spec[ENDPOINT_ADDRESS_KEY]
            .as_str()
            .map(|x| x.to_string())
            .ok_or(Error::MissingDestinationAddress)?;
        let (forward_host, forward_port) =
            split_host_port(address.clone()).ok_or(Error::BadDestinationAddress(address))?;
        let local_port = rand::thread_rng().gen_range(10000..20000);

        self.config.forward_host = forward_host;
        self.config.forward_port = forward_port;
        self.config.local_port = local_port;

        let address = format!("127.0.0.1:{}", local_port);
        endpoint_spec[ENDPOINT_ADDRESS_KEY] = serde_json::json!(address);
        Ok(endpoint_spec)
    }

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

        tracing::info!(
            "ssh forwarding local port {} to remote host {}:{}",
            local_port,
            forward_host,
            forward_port
        );

        let args = vec![
            // Disable psuedo-terminal allocation
            "-T".to_string(),
            // Be verbose so we can pick up signals about status of the tunnel
            "-v".to_string(),
            // This is necessary unless we also ask for the public key from users
            "-o".to_string(),
            "StrictHostKeyChecking no".to_string(),
            // Indicate that ssh is to use ipv4 only. Otherwise sometimes it tries to bind to [::1] instead of 127.0.0.1,
            // which breaks things when the runtime environment isn't set up to support ipv6
            "-o".to_string(),
            "AddressFamily inet".to_string(),
            // Ask the client to time out after 5 seconds
            "-o".to_string(),
            "ConnectTimeout=5".to_string(),
            // Pass the private key
            "-i".to_string(),
            temp_key_path.into_os_string().into_string().unwrap(),
            // Do not execute a remote command. Just forward the ports.
            "-N".to_string(),
            // Port forwarding stanza
            "-L".to_string(),
            format!("{local_port}:{forward_host}:{forward_port}"),
            ssh_endpoint,
        ];

        tracing::debug!("spawning ssh tunnel: {}", args.join(" "));
        let mut child = Command::new("ssh")
            .args(args)
            .stderr(Stdio::piped())
            .spawn()?;

        // Read stderr of SSH until we find a signal message that
        // the ports are open and we are ready to serve requests
        let stderr = child.stderr.take().unwrap();
        let mut lines = BufReader::new(stderr).lines();
        self.process = Some(child);

        tracing::debug!("listening on ssh tunnel stderr");
        while let Some(line) = lines.next_line().await? {
            // OpenSSH will enter interactive session after tunnelling has been
            // successful
            if line.contains("Entering interactive session.") {
                tracing::debug!("ssh tunnel is listening & ready for serving requests");
                return Ok(());
            }

            // Otherwise apply a little bit of intelligence to translate OpenSSH
            // log messages to appropriate connector_proxy log levels.
            if line.starts_with("debug1:") {
                tracing::debug!("ssh: {}", &line);
            } else if line.starts_with("Warning: Permanently added") {
                tracing::debug!("ssh: {}", &line);
            } else if line.contains("Permission denied") {
                tracing::error!("ssh: {}", &line);
            } else if line.contains("Network is unreachable") {
                tracing::error!("ssh: {}", &line);
            } else if line.contains("Connection timed out") {
                tracing::error!("ssh: {}", &line);
            } else {
                tracing::info!("ssh: {}", &line);
            }
        }

        // This function's job was just to launch the SSH tunnel and wait until
        // it's ready to serve traffic. If stderr closes unexpectedly we treat
        // this as a probably-erroneous form of 'success', and rely on the later
        // `start_serve` exit code checking to report a failure.
        tracing::warn!("unexpected end of output from ssh tunnel");
        Ok(())
    }

    async fn start_serve(&mut self) -> Result<(), Error> {
        tracing::debug!("awaiting ssh tunnel process");
        let exit_status = self.process.as_mut().unwrap().wait().await?;
        if !exit_status.success() {
            tracing::error!(
                exit_code = ?exit_status.code(),
                message = "network tunnel ssh exit with non-zero code."
            );

            return Err(Error::TunnelExitNonZero(format!("{:#?}", exit_status)));
        }

        Ok(())
    }

    async fn cleanup(&mut self) -> Result<(), Error> {
        if let Some(process) = self.process.as_mut() {
            match process.kill().await {
                // InvalidInput means the process has already exited, in which case
                // we do not need to cleanup the process
                Err(e) if e.kind() == ErrorKind::InvalidInput => Ok(()),
                a => a,
            }?;
        }

        Ok(())
    }

    // This is only used for testing
    fn as_any(&self) -> &dyn Any {
        self
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
