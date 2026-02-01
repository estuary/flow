pub mod job;
pub mod protocol;
pub mod service;
pub mod shared;

pub use shared::controller::ControllerConfig;

pub const DNS_TTL_ACTUAL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
pub const DNS_TTL_DRY_RUN: std::time::Duration = std::time::Duration::from_secs(10);
