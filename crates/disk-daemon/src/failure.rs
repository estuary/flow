//! Why a tenure failed, and the gRPC code its stream ends with.
//!
//! Every error is terminal, and the code a tenure ends with is the only part of a
//! failure a client can act on. It turns on why the tenure failed rather than on what
//! the message says, so a failure the tenure brought on itself travels as a
//! [`Failure`] cause, which survives `anyhow::Context`. [`status`] finds that cause,
//! or classifies the broker error beneath it, however deeply context is stacked over
//! either. `client::Error::of` is the other end of this contract.

/// A failure whose cause names what the tenure stream must report.
///
/// Everything the tenure stream does not find one of is the daemon, its host, or
/// its brokers.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// What the tenure asked for, rather than anything of this daemon's.
    ///
    /// A retry of an invalid request can never succeed, while a retry after a
    /// broker outage or a lost fence can. This is `INVALID_ARGUMENT`.
    #[error("{0}")]
    Invalid(String),

    /// A request which is well-formed, but out of turn for what the tenure owes.
    ///
    /// A second `Prepare` before its `Acknowledge`, an `Acknowledge` with nothing
    /// prepared, or an `Acknowledge` of bytes no `Prepare` returned, is a client
    /// which lost track of the delta it owes. No retry of it can succeed, but
    /// nothing about the request itself is wrong, so this is `FAILED_PRECONDITION`
    /// rather than `INVALID_ARGUMENT`.
    #[error("{0}")]
    OutOfOrder(String),

    /// The tenure's cancellation token fired while a request's work was in flight.
    ///
    /// Under a request that can only be the daemon draining. The token is also
    /// cancelled once a tenure is over, but nothing of the client's is in flight
    /// then. The disk may be served by another host, so this is `UNAVAILABLE` — the
    /// same code a drain observed between requests already gets.
    #[error("{0}")]
    Ended(String),
}

/// `anyhow::ensure!` for a rule a tenure broke. A validator states each rule in
/// one place, and the tenure stream reports the right code for it.
macro_rules! ensure_valid {
    ($condition:expr, $($message:tt)*) => {
        if !$condition {
            return Err(::anyhow::Error::new($crate::failure::Failure::Invalid(format!(
                $($message)*
            ))));
        }
    };
}

pub(crate) use ensure_valid;

/// The status a failure ends its tenure with.
///
/// A client cannot act on a message, so the code is the contract:
///
/// - `INVALID_ARGUMENT` is what the tenure asked for. A retry cannot succeed.
/// - `FAILED_PRECONDITION` is a request out of turn, per [`Failure::OutOfOrder`].
/// - `ABORTED` is a lost fence. Another tenure owns this disk, and this one must
///   not take it back.
/// - `UNAUTHENTICATED` is a credential the broker refused. A client should
///   refresh it and open again.
/// - `UNAVAILABLE` is a broker this daemon could not reach, or a tenure the
///   daemon's drain cut short. Another host may reach that broker, or serve
///   that disk.
/// - `INTERNAL` is everything else, which is the daemon or its host failing.
///
/// `tenure` reports a request its state cannot serve as `FAILED_PRECONDITION`
/// directly, without reaching here. The crate README says what a client should do
/// with each code.
pub fn status(err: anyhow::Error) -> tonic::Status {
    let code = match err
        .chain()
        .find_map(|cause| cause.downcast_ref::<Failure>())
    {
        Some(Failure::Invalid(_)) => tonic::Code::InvalidArgument,
        Some(Failure::OutOfOrder(_)) => tonic::Code::FailedPrecondition,
        Some(Failure::Ended(_)) => tonic::Code::Unavailable,
        // Anything the tenure did not bring on itself is the daemon, its host, or
        // its brokers, and only a broker failure carries a code beyond `INTERNAL`.
        None => broker_code(&err),
    };

    tonic::Status::new(code, format!("{err:#}"))
}

/// gRPC code of a failure which is not the tenure's own, per [`status`].
fn broker_code(err: &anyhow::Error) -> tonic::Code {
    match err
        .chain()
        .find_map(|cause| cause.downcast_ref::<gazette::Error>())
    {
        Some(gazette::Error::BrokerStatus(proto_gazette::broker::Status::RegisterMismatch)) => {
            tonic::Code::Aborted
        }
        // `UNAUTHENTICATED` is not a promise that every credential problem
        // arrives this way. A broker may refuse whatever it was doing rather
        // than the credential. Gazette answers an expired token on an append
        // with `DeadlineExceeded`, because the pipeline the append waited for
        // is what timed out.
        Some(gazette::Error::Grpc(status))
            if matches!(
                status.code(),
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied,
            ) =>
        {
            tonic::Code::Unauthenticated
        }
        Some(broker) if broker.is_transient() => tonic::Code::Unavailable,
        _ => tonic::Code::Internal,
    }
}

#[cfg(test)]
mod test {
    use super::{Failure, status};

    /// A cause is classified however deeply context is stacked over it. Every
    /// failure reaches the tenure stream that way.
    #[test]
    fn test_a_failure_is_classified_by_its_cause() {
        let cases: Vec<(anyhow::Error, tonic::Code)> = vec![
            (
                anyhow::Error::new(Failure::Invalid("device size 0".to_string()))
                    .context("creating a disk"),
                tonic::Code::InvalidArgument,
            ),
            (
                anyhow::Error::new(Failure::OutOfOrder(
                    "a prepared delta is still awaiting its commit".to_string(),
                ))
                .context("preparing acmeCo/disk/one"),
                tonic::Code::FailedPrecondition,
            ),
            (
                anyhow::Error::new(gazette::Error::BrokerStatus(
                    proto_gazette::broker::Status::RegisterMismatch,
                ))
                .context("appending to acmeCo/disk/one"),
                tonic::Code::Aborted,
            ),
            (
                anyhow::Error::new(gazette::Error::Grpc(tonic::Status::unauthenticated(
                    "token has expired",
                )))
                .context("appending to acmeCo/disk/one"),
                tonic::Code::Unauthenticated,
            ),
            (
                anyhow::Error::new(gazette::Error::Grpc(tonic::Status::permission_denied(
                    "not authorized to append",
                ))),
                tonic::Code::Unauthenticated,
            ),
            // Whatever the broker was doing refuses a credential which runs out
            // under a live append, so it does not report this code.
            (
                anyhow::Error::new(gazette::Error::Grpc(tonic::Status::deadline_exceeded(
                    "waiting for pipeline",
                ))),
                tonic::Code::Internal,
            ),
            (
                anyhow::Error::new(gazette::Error::UnexpectedEof).context("probing"),
                tonic::Code::Unavailable,
            ),
            (
                anyhow::Error::new(Failure::Ended(
                    "the tenure ended while its playback backfilled".to_string(),
                ))
                .context("promoting acmeCo/disk/one"),
                tonic::Code::Unavailable,
            ),
            (
                anyhow::Error::new(gazette::Error::BrokerStatus(
                    proto_gazette::broker::Status::JournalNotFound,
                )),
                tonic::Code::Internal,
            ),
            (
                anyhow::anyhow!("the image could not be written"),
                tonic::Code::Internal,
            ),
        ];

        for (err, expect) in cases {
            let status = status(err);
            assert_eq!(status.code(), expect, "{status}");
        }
    }
}
