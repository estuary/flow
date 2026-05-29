pub mod capture;
pub mod derive;
pub mod materialize;
pub(crate) mod recovery;
mod rocksdb;
mod service;

use rocksdb::RocksDB;
pub use service::Service;

/// Build gRPC client metadata bearing a self-signed `LEAD` token when `signer`
/// is `Some`, scoped to `shard_id`'s task prefix so a leader stream opened with
/// it can only operate on shards of this task. Empty metadata when `None`
/// (unauthenticated local contexts, e.g. `flowctl preview`).
pub(crate) fn leader_bearer(
    signer: Option<&proto_grpc::Signer>,
    shard_id: &str,
) -> tonic::Result<proto_grpc::Metadata> {
    match signer {
        Some(signer) => signer.shard_bearer(proto_flow::capability::LEAD, shard_id),
        None => Ok(proto_grpc::Metadata::new()),
    }
}
