mod middleware;
pub use middleware::Middleware;

pub mod combine;
pub mod connectors;
pub mod log_level;
pub mod rocksdb;

fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    tonic::Status::internal(format!("{err:?}"))
}
