use crate::proto;
use proto_gazette::{fixed_framing, uuid};

mod lifecycle;
mod replay;
mod spec;
mod writer;

pub use lifecycle::{Claimed, Recovered, Standby};
#[cfg(test)]
pub(crate) use lifecycle::{Promoted, fence};
pub use replay::Playback;
pub use replay::uncommitted::UncommittedDelta;
pub use writer::Writer;

/// Append state carried from [`Standby::new`] through promotion into the writer.
struct Journal {
    name: String,
    /// Lists the journal and updates its recovery-floor label.
    client: gazette::journal::Client,
    /// Cancelled once the tenure is over, which stops this writer appending and
    /// gives up whatever broker call is in flight.
    ended: tokio_util::sync::CancellationToken,
    /// Installed in the journal's `author` register and checked by subsequent
    /// appends. Also the producer of this tenure's deltas; fences use separate
    /// producers.
    author: uuid::Producer,
    /// Write head the broker last confirmed, which every flush learns again.
    /// Together with `floor` it gives the range a recovery of this disk would
    /// have to read.
    head: i64,
    floor: i64,
    /// Whether the journal carries a recovery-floor label, which says the disk has
    /// committed state. The writer stores one once a delta first commits.
    marked: bool,
}

impl Journal {
    /// Buffer a complete record, yielding to appender backpressure at its boundary.
    /// While this waits, the recording channel fills and eventually parks writes.
    /// Retries send identical bytes; replay deduplicates records by UUID.
    async fn append_record(
        &mut self,
        appender: &mut publisher::Appender,
        record: &proto::DiskRecord,
    ) -> anyhow::Result<()> {
        fixed_framing::encode(record, &mut appender.buffer);

        until_ended(&self.ended, "appending", async {
            appender.checkpoint().await.map_err(append_failure)
        })
        .await
    }

    /// Append the exact prepared acknowledgement and wait for confirmation.
    /// Flush before accepting the next delta, per `writer::ledger::Ledger::taking`.
    async fn append_ack(
        &mut self,
        appender: &mut publisher::Appender,
        ack: &bytes::Bytes,
    ) -> anyhow::Result<()> {
        appender.buffer.extend_from_slice(ack);

        self.flush(appender).await
    }

    /// Flush and learn the broker-confirmed head. Retried appends can duplicate
    /// content, so locally counted bytes cannot determine journal offsets.
    /// Horizon-opening decisions always follow a flush.
    async fn flush(&mut self, appender: &mut publisher::Appender) -> anyhow::Result<()> {
        // Register the barrier before flushing so it covers every append started
        // by the flush and returns the last response.
        let barrier = appender.barrier();

        () = until_ended(&self.ended, "appending", async {
            appender.flush().await.map_err(append_failure)
        })
        .await?;

        // The barrier is a broker wait like the flush, and the tenure may end
        // under either of them.
        let response = until_ended(&self.ended, "appending", async {
            barrier.await.map_err(append_failure)
        })
        .await?;

        if let Some(commit) = &response.commit {
            self.head = commit.end;
        }
        Ok(())
    }

    /// Store the recovery floor without failing the tenure if the update fails.
    /// Replay can derive the floor again, and later horizons retry the update.
    async fn store_floor(&mut self, floor: i64) {
        match spec::advance_floor(&self.client, &self.name, floor).await {
            Ok(()) => self.marked = true,
            Err(err) => tracing::warn!(
                journal = self.name,
                floor,
                ?err,
                "could not store the disk's recovery floor",
            ),
        }
    }
}

/// Cancel broker work when the tenure ends. Otherwise transient-error retries
/// could hold the mount and device through an outage, blocking daemon drain.
async fn until_ended<T>(
    ended: &tokio_util::sync::CancellationToken,
    what: &str,
    work: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    ended.run_until_cancelled(work).await.unwrap_or_else(|| {
        Err(proto_grpc::status_to_anyhow(tonic::Status::unavailable(
            format!("the tenure ended while {what}"),
        )))
    })
}

/// A failed broker call, as a failure which carries the code its tenure ends with.
///
/// The caller of a broker call decides what its failure means, as Gazette's own
/// callers do: these are the calls, so this is where a failure becomes one a client
/// can act on. A failure it does not classify is the daemon's own, and its tenure
/// ends as `UNKNOWN`.
fn broker_failure(err: gazette::Error) -> anyhow::Error {
    match broker_code(&err) {
        Some(code) => proto_grpc::status_to_anyhow(tonic::Status::new(code, err.to_string())),
        None => anyhow::Error::new(err),
    }
}

/// A failed append, as [`broker_failure`] classifies it. The appender reports a
/// status which carries the broker's error as its source, for its caller to
/// classify, and keeps its own code where that error is not one a client acts on.
fn append_failure(status: tonic::Status) -> anyhow::Error {
    let code = std::error::Error::source(&status)
        .and_then(|source| source.downcast_ref::<gazette::Error>())
        .and_then(broker_code);

    match code {
        Some(code) => {
            proto_grpc::status_to_anyhow(tonic::Status::new(code, status.message().to_string()))
        }
        None => proto_grpc::status_to_anyhow(status),
    }
}

/// The code a client acts on for a broker's `err`, if it is one:
///
/// - `ABORTED` is a lost fence. Another tenure owns this disk, and this one must
///   not take it back.
/// - `UNAUTHENTICATED` is a credential the broker refused. A client should refresh
///   it and open again.
/// - `UNAVAILABLE` is a broker this daemon could not reach. Another host may.
fn broker_code(err: &gazette::Error) -> Option<tonic::Code> {
    match err {
        gazette::Error::BrokerStatus(proto_gazette::broker::Status::RegisterMismatch) => {
            Some(tonic::Code::Aborted)
        }
        // `UNAUTHENTICATED` is not a promise that every credential problem
        // arrives this way. A broker may refuse whatever it was doing rather
        // than the credential. Gazette answers an expired token on an append
        // with `DeadlineExceeded`, because the pipeline the append waited for
        // is what timed out.
        gazette::Error::Grpc(status)
            if matches!(
                status.code(),
                tonic::Code::Unauthenticated | tonic::Code::PermissionDenied,
            ) =>
        {
            Some(tonic::Code::Unauthenticated)
        }
        err if err.is_transient() => Some(tonic::Code::Unavailable),
        _ => None,
    }
}

fn uuid_bytes(producer: uuid::Producer, clock: uuid::Clock, flags: uuid::Flags) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(uuid::build(producer, clock, flags).as_bytes())
}

#[cfg(test)]
mod test {
    use super::{append_failure, broker_failure};
    use proto_gazette::broker;

    fn code(err: &anyhow::Error) -> Option<tonic::Code> {
        err.downcast_ref::<proto_grpc::StatusError>()
            .map(|status| status.code())
    }

    /// A broker's failure becomes the code its client acts on, or none, which ends
    /// the tenure as `UNKNOWN`.
    #[test]
    fn test_a_broker_failure_is_classified_where_it_is_made() {
        let mut out = Vec::new();

        for err in [
            gazette::Error::BrokerStatus(broker::Status::RegisterMismatch),
            gazette::Error::Grpc(tonic::Status::unauthenticated("token has expired")),
            gazette::Error::Grpc(tonic::Status::permission_denied("not authorized")),
            // Whatever the broker was doing refuses a credential which runs out
            // under a live append, so it does not report this code.
            gazette::Error::Grpc(tonic::Status::deadline_exceeded("waiting for pipeline")),
            gazette::Error::UnexpectedEof,
            gazette::Error::BrokerStatus(broker::Status::JournalNotFound),
        ] {
            let rendered = err.to_string();
            out.push(format!("{rendered:<60}{:?}", code(&broker_failure(err))));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        unexpected broker status: RegisterMismatch                  Some(Aborted)
        gRPC code: Unauthenticated, message: token has expired      Some(Unauthenticated)
        gRPC code: PermissionDenied, message: not authorized        Some(Unauthenticated)
        gRPC code: DeadlineExceeded, message: waiting for pipeline  None
        unexpected server EOF                                       Some(Unavailable)
        unexpected broker status: JournalNotFound                   None
        ");
    }

    /// The appender reports a status which carries the broker's error as its
    /// source. That error is classified, and a status whose error is not one a
    /// client acts on keeps the appender's own code.
    #[test]
    fn test_an_append_failure_is_classified_by_its_broker_error() {
        let appended = |code: tonic::Code, err: gazette::Error| {
            let mut status = tonic::Status::new(code, err.to_string());
            status.set_source(std::sync::Arc::new(err));
            status
        };
        let mut out = Vec::new();

        for status in [
            appended(
                tonic::Code::Internal,
                gazette::Error::BrokerStatus(broker::Status::RegisterMismatch),
            ),
            appended(
                tonic::Code::NotFound,
                gazette::Error::BrokerStatus(broker::Status::JournalNotFound),
            ),
            tonic::Status::internal("simulated failure"),
        ] {
            let source = std::error::Error::source(&status)
                .map_or("no broker error".to_string(), |source| source.to_string());
            let status_from = format!("{:?} carrying {source}", status.code());

            out.push(format!(
                "{status_from:<62}{:?}",
                code(&append_failure(status))
            ));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        Internal carrying unexpected broker status: RegisterMismatch  Some(Aborted)
        NotFound carrying unexpected broker status: JournalNotFound   Some(NotFound)
        Internal carrying no broker error                             Some(Internal)
        ");
    }
}
