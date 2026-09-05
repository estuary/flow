//! `runtime-local` runs `runtime-next` tasks locally with synthetic shards, and
//! with no Gazette broker, etcd, or Go consumer.
//!
//! It hosts an in-process tonic server ([`services::Run`]) carrying
//! `runtime_next::Service` and, optionally, a caller-supplied
//! `shuffle::Service`; synthesizes the SessionLoop / Join / Task envelopes the
//! Go controller would normally send; and drives N synthetic shards as tokio
//! tasks over long-lived SessionLoop streams.
//!
//! The crate is generic over the three `runtime-next` host seams —
//! [`runtime_next::PublisherFactory`], [`runtime_next::LoggerFactory`], and
//! [`runtime_next::ShuffleSessionFactory`] — so callers decide where source
//! documents come from and where output documents, stats, and logs go. Two
//! consumers exist: `flowctl raw preview-next` (stdout publisher, live-journal
//! or fixture shuffle) and `catalog-tests` (in-memory collection store,
//! channel-fed shuffle).
//!
//! Nothing here knows about catalog tests. That layering is deliberate and is
//! enforced by the crate boundary: `catalog-tests` depends on `runtime-local`,
//! never the reverse.
//!
//! # Key entry points
//!
//! - [`services::Run`] — per-invocation resources: the tonic server and the
//!   shuffle-log directory, and a `proto_grpc::connector::Router`.
//! - [`materialize_driver::run_sessions`] / [`derive_driver::run_sessions`] /
//!   [`capture_driver::run_sessions`] — drive N shards of one task through a
//!   sequence of sessions.
//! - [`segments::write_transaction`] — write documents directly as
//!   `shuffle::log` segments and return the checkpoint frontier that makes them
//!   visible, bypassing journals entirely. A caller which doesn't yet hold a
//!   whole transaction pushes into a [`segments::TxnState`] instead.
//! - [`segments::fixture_opener`] — the channel-fed `ShuffleSessionFactory`
//!   which relays those frontiers to the leader.

pub mod capture_driver;
pub mod derive_driver;
pub mod materialize_driver;
pub mod segments;
pub mod services;
pub mod shards;

use runtime_next::{LoggerFactory, PublisherFactory};
/// The connector router of every local context: one
/// `Plane::Local` connector `Service`, served in-process, reached with bearers
/// minted from a throwaway key the pair shares.
pub fn local_router(
    network: String,
    registry: service_kit::Registry,
) -> std::sync::Arc<dyn proto_grpc::connector::Router> {
    let (_service, router) = connector::Service::new_local(network, registry);
    std::sync::Arc::new(router)
}

/// Controls threaded into each driver: the connector-state seed and final-state
/// request carried on shard zero's SessionLoop, plus the publisher and logger
/// factories installed on each shard `Service`. Cheap to clone — both factories
/// are `Clone` by their trait bound.
#[derive(Clone)]
pub struct Controls<P: PublisherFactory, L: LoggerFactory> {
    /// Connector state for shard zero to establish as its base document, sent
    /// as `SessionLoop.initial_connector_state_json`. Empty to leave the
    /// runtime's own `{}` seed in place.
    pub initial_state_json: bytes::Bytes,
    /// Ask shard zero to report its reduced connector state on each `Stopped`,
    /// which each driver's `run_sessions` returns as the run's final state. A
    /// run which stops before any session reaches `Stopped` reports
    /// `initial_state_json`.
    pub report_final_state: bool,
    pub publisher_factory: P,
    pub logger_factory: L,
}

impl<P: PublisherFactory, L: LoggerFactory> Controls<P, L> {
    /// Build the `SessionLoop` which opens one shard's stream. Shard zero
    /// carries the connector-state seed and the final-state request; other
    /// shards carry neither, as their connector state must recover empty.
    pub(crate) fn session_loop(&self, shard_index: u32) -> runtime_next::proto::SessionLoop {
        let is_shard_zero = shard_index == 0;

        runtime_next::proto::SessionLoop {
            // No path: `RocksDB` then makes and owns a tempdir per shard.
            rocksdb_descriptor: None,
            initial_connector_state_json: if is_shard_zero {
                self.initial_state_json.clone()
            } else {
                bytes::Bytes::new()
            },
            report_final_state: is_shard_zero && self.report_final_state,
        }
    }
}

/// Await every per-shard driver handle of a `run_sessions` fan-out: the first
/// error wins (later ones are traced as secondary), and shard zero's reported
/// final state — always the first handle — is the run's.
pub(crate) async fn join_shard_drivers(
    handles: Vec<tokio::task::JoinHandle<anyhow::Result<Option<bytes::Bytes>>>>,
    task_kind: &'static str,
) -> anyhow::Result<Option<bytes::Bytes>> {
    let mut first_err: Option<anyhow::Error> = None;
    let mut final_state = None;

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            // Only shard zero was asked to report a final state.
            Ok(Ok(state)) if i == 0 => final_state = state,
            Ok(Ok(_state)) => (),
            Ok(Err(err)) if first_err.is_none() => first_err = Some(err),
            Ok(Err(err)) => {
                tracing::warn!(error = ?err, task_kind, "secondary shard driver error")
            }
            Err(panic) if first_err.is_none() => {
                first_err = Some(anyhow::anyhow!("{task_kind} shard driver panic: {panic}"))
            }
            Err(panic) => tracing::warn!(?panic, task_kind, "secondary shard driver panic"),
        }
    }

    match first_err {
        Some(err) => Err(err),
        None => Ok(final_state),
    }
}

/// A shard driver's teardown: EOF the request stream, read the response stream
/// through to termination, and reduce to the driver's outcome — the session
/// loop's own `result` wins over a teardown error, and `final_state` rides on
/// full success.
pub(crate) async fn teardown_shard_stream<T>(
    request_tx: tokio::sync::mpsc::UnboundedSender<tonic::Result<T>>,
    mut response_rx: tokio::sync::mpsc::UnboundedReceiver<tonic::Result<T>>,
    result: anyhow::Result<()>,
    final_state: Option<bytes::Bytes>,
) -> anyhow::Result<Option<bytes::Bytes>> {
    std::mem::drop(request_tx); // EOF requests graceful teardown.

    let drained = async {
        while let Some(msg) = response_rx.recv().await {
            _ = msg.map_err(runtime_next::status_to_anyhow)?;
        }
        anyhow::Ok(())
    }
    .await;

    result.and(drained).map(|()| final_state)
}

/// Raise a task's minimum transaction duration, so the leader holds each
/// transaction open for at least `delay` and batches source output into fewer,
/// larger transactions.
pub fn set_min_txn_duration(
    shard_template: Option<&mut proto_gazette::consumer::ShardSpec>,
    delay: std::time::Duration,
) {
    let Some(shard_template) = shard_template else {
        return;
    };
    let min = pbjson_types::Duration {
        seconds: delay.as_secs() as i64,
        nanos: delay.subsec_nanos() as i32,
    };
    // Keep the close-policy band well-formed if the template's configured
    // maximum is below the requested minimum.
    if shard_template
        .max_txn_duration
        .as_ref()
        .map_or(true, |max| {
            (max.seconds, max.nanos) < (min.seconds, min.nanos)
        })
    {
        shard_template.max_txn_duration = Some(min.clone());
    }
    shard_template.min_txn_duration = Some(min);
}

/// Force one-transaction-per-checkpoint in the leader by collapsing the task's
/// transaction-duration window, so each fed transaction commits as exactly one
/// runtime transaction.
///
/// A literal `max_txn_duration` of zero would deadlock the leader: `HeadIdle`
/// gates the first checkpoint load on `open_age < max_txn_duration`, and a fresh
/// transaction's `open_age` is zero. The smallest positive duration loads one
/// checkpoint, after which the Load round's IO advances the clock past the bound
/// and the transaction closes.
pub fn force_single_transaction(shard_template: Option<&mut proto_gazette::consumer::ShardSpec>) {
    if let Some(shard_template) = shard_template {
        shard_template.min_txn_duration = Some(pbjson_types::Duration {
            seconds: 0,
            nanos: 0,
        });
        shard_template.max_txn_duration = Some(pbjson_types::Duration {
            seconds: 0,
            nanos: 1,
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_set_min_txn_duration() {
        let dur = |seconds, nanos| pbjson_types::Duration { seconds, nanos };

        // An unset maximum is raised alongside the minimum.
        let mut template = proto_gazette::consumer::ShardSpec::default();
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(10, 0)));

        // A maximum above the delay is left alone.
        template.max_txn_duration = Some(dur(30, 0));
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(30, 0)));

        // A maximum below the delay is raised to keep the band well-formed.
        template.max_txn_duration = Some(dur(5, 0));
        set_min_txn_duration(Some(&mut template), std::time::Duration::from_secs(10));
        assert_eq!(template.min_txn_duration, Some(dur(10, 0)));
        assert_eq!(template.max_txn_duration, Some(dur(10, 0)));
    }
}
