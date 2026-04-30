/// Runtime implements the various services that constitute the Flow Runtime.
#[allow(dead_code)]
#[derive(Clone)]
pub struct Service<L: crate::LogHandler> {
    pub plane: crate::Plane,
    pub container_network: String,
    pub log_handler: L,
    pub set_log_level: Option<std::sync::Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
    pub task_name: String,
    pub publisher_factory: gazette::journal::ClientFactory,
}

#[allow(dead_code)]
impl<L: crate::LogHandler> Service<L> {
    /// Build a new Runtime.
    /// - `plane`: the type of data plane in which this Runtime is operating.
    /// - `container_network`: the Docker container network used for connector containers.
    /// - `log_handler`: handler to which connector logs are dispatched.
    /// - `set_log_level`: callback for adjusting the log level implied by runtime requests.
    /// - `task_name`: name which is used to label any started connector containers.
    /// - `publisher_factory`: client factory for creating and appending to collection partitions.
    pub fn new(
        plane: crate::Plane,
        container_network: String,
        log_handler: L,
        set_log_level: Option<std::sync::Arc<dyn Fn(ops::LogLevel) + Send + Sync>>,
        task_name: String,
        publisher_factory: gazette::journal::ClientFactory,
    ) -> Self {
        Self {
            plane,
            container_network,
            log_handler,
            set_log_level,
            task_name,
            publisher_factory,
        }
    }

    /// Apply the dynamic log level if a setter was provided.
    pub fn set_log_level(&self, level: ops::LogLevel) {
        if level == ops::LogLevel::UndefinedLevel {
            // No-op
        } else if let Some(set_log_level) = &self.set_log_level {
            (set_log_level)(level);
        }
    }
}
