/// ControllerConfig contains all configuration needed to execute data-plane controller work.
#[derive(Clone)]
pub struct ControllerConfig {
    pub dns_ttl: std::time::Duration,
    pub dry_dock_remote: String,
    pub ops_remote: String,
    pub secrets_provider: String,
    pub state_backend: url::Url,
    pub dry_run: bool,
}

impl From<&ControllerConfig> for crate::protocol::ControllerConfig {
    fn from(config: &ControllerConfig) -> Self {
        Self {
            dns_ttl: config.dns_ttl,
            dry_dock_remote: config.dry_dock_remote.clone(),
            ops_remote: config.ops_remote.clone(),
            secrets_provider: config.secrets_provider.clone(),
            state_backend: config.state_backend.clone(),
            dry_run: config.dry_run,
        }
    }
}

impl From<crate::protocol::ControllerConfig> for ControllerConfig {
    fn from(config: crate::protocol::ControllerConfig) -> Self {
        Self {
            dns_ttl: config.dns_ttl,
            dry_dock_remote: config.dry_dock_remote,
            ops_remote: config.ops_remote,
            secrets_provider: config.secrets_provider,
            state_backend: config.state_backend,
            dry_run: config.dry_run,
        }
    }
}
