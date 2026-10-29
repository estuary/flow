use crate::proto;
use proto::{request, response};
use tokio::sync::mpsc;

mod promoted;
mod standby;

/// Run one attachment of a disk over its stream of requests.
pub async fn run(
    config: std::sync::Arc<crate::daemon::Config>,
    peer: (u32, u32),
    cancel: tokio_util::sync::CancellationToken,
    mut requests: tonic::Streaming<proto::Request>,
    responses: mpsc::Sender<tonic::Result<proto::Response>>,
) {
    // The promoted attachment lives out here rather than in `serve`, so that a
    // panic or cancel which ends `serve` part-way still leaves it for `stop`.
    let mut promoted = None;
    let serving = serve(&mut promoted, &config, peer, &mut requests, &responses);

    let mut outcome = tokio::select! {
        outcome = proto_grpc::catch_panic(serving) => outcome,
        () = responses.closed() => Err(proto_grpc::anyhow_to_status(crate::cancelled())),
        () = cancel.cancelled() => Err(proto_grpc::anyhow_to_status(crate::cancelled())),
    };

    // A client's close waits on teardown, so a failed teardown fails the close.
    if let Some(attachment) = promoted
        && let Err(err) = attachment.stop().await
    {
        match outcome {
            Ok(()) => outcome = Err(proto_grpc::anyhow_to_status(err)),
            Err(_) => tracing::error!(?err, "failed to tear down a disk"),
        }
    }

    if let Err(status) = outcome {
        tracing::warn!(%status, "attachment failed");
        _ = responses.send(Err(status)).await;
    }
}

/// Serve `requests` in protocol order.
async fn serve(
    promoted: &mut Option<promoted::Promoted>,
    config: &crate::daemon::Config,
    peer: (u32, u32),
    requests: &mut tonic::Streaming<proto::Request>,
    responses: &mpsc::Sender<tonic::Result<proto::Response>>,
) -> anyhow::Result<()> {
    let (journal, device_size) = match recv(requests).await? {
        None => return Ok(()),
        Some(request::Request::Open(proto::Open {
            journal,
            device_size,
        })) => (journal, device_size),
        Some(_) => {
            return Err(crate::failed_precondition(
                "the first request of an attachment must be Open",
            ));
        }
    };
    // A selector's empty label value matches any value, so listing an empty
    // name would match every journal.
    if journal.is_empty() {
        return Err(crate::invalid_argument("Open names no journal".to_string()));
    }
    let blocks = match u32::try_from(device_size / crate::BLOCK_SIZE as u64) {
        Ok(blocks)
            if device_size >= crate::MIN_DEVICE_SIZE
                && device_size % crate::BLOCK_SIZE as u64 == 0 =>
        {
            blocks
        }
        _ => {
            return Err(crate::invalid_argument(format!(
                "device_size {device_size} must be a multiple of {} of at least {}, \
                 and at most 2^32 - 1 blocks",
                crate::BLOCK_SIZE,
                crate::MIN_DEVICE_SIZE,
            )));
        }
    };
    let standby = standby::Standby::open(
        config.brokers.clone(),
        &config.image_dir,
        config.horizon_policy,
        journal,
        blocks,
    )
    .await?;
    () = send(responses, response::Response::Opened(proto::Opened {})).await?;

    let (standby, request) = standby.tail(recv(requests)).await?;
    let ack_intent = match request? {
        None => return Ok(()),
        Some(request::Request::Promote(proto::Promote { ack_intent })) if ack_intent.is_empty() => {
            None
        }
        Some(request::Request::Promote(proto::Promote { ack_intent })) => {
            if !is_ack_intent(&ack_intent) {
                return Err(crate::invalid_argument(
                    "Promote carries a malformed ACK intent".to_string(),
                ));
            }
            Some(ack_intent)
        }
        Some(_) => {
            return Err(crate::failed_precondition(
                "a standby attachment serves only Promote",
            ));
        }
    };
    let attachment = promoted.insert(
        standby
            .promote(config.mount_dir.clone(), peer, ack_intent)
            .await?,
    );

    attachment.serve(requests, responses).await
}

async fn recv(
    requests: &mut tonic::Streaming<proto::Request>,
) -> anyhow::Result<Option<request::Request>> {
    match futures::StreamExt::next(requests).await {
        None => Ok(None),
        Some(Err(status)) => Err(proto_grpc::status_to_anyhow(status)),
        Some(Ok(proto::Request { request: None })) => Err(crate::invalid_argument(
            "request carries no message".to_string(),
        )),
        Some(Ok(proto::Request {
            request: Some(request),
        })) => Ok(Some(request)),
    }
}

async fn send(
    responses: &mpsc::Sender<tonic::Result<proto::Response>>,
    response: response::Response,
) -> anyhow::Result<()> {
    responses
        .send(Ok(proto::Response {
            response: Some(response),
        }))
        .await
        .map_err(|_| {
            proto_grpc::status_to_anyhow(tonic::Status::cancelled(
                "the client dropped its attachment",
            ))
        })
}

/// `ack_intent` is exactly one framed record of an ACK, as
/// `journal::Writer::prepare` returns.
fn is_ack_intent(ack_intent: &[u8]) -> bool {
    use proto_gazette::{fixed_framing, uuid};

    let mut buf = bytes::BytesMut::from(ack_intent);
    let Ok(fixed_framing::Frame::Record { message, .. }) =
        fixed_framing::unpack::<proto::DiskRecord>(&mut buf)
    else {
        return false;
    };
    let is_ack = uuid::Uuid::from_slice(&message.uuid)
        .ok()
        .and_then(|uuid| uuid::parse(uuid).ok())
        .is_some_and(|(_, _, flags)| flags.is_ack());

    buf.is_empty() && message.chunk.is_none() && is_ack
}

#[cfg(test)]
mod test {
    use crate::proto;
    use proto_gazette::{fixed_framing, uuid};

    #[test]
    fn test_is_ack_intent() {
        let record = |flags, chunk| proto::DiskRecord {
            uuid: bytes::Bytes::copy_from_slice(
                uuid::build(uuid::Producer([1; 6]), uuid::Clock::from_u64(1), flags).as_bytes(),
            ),
            chunk,
            ..Default::default()
        };
        let framed = |records: &[proto::DiskRecord]| {
            let mut buf = bytes::BytesMut::new();
            for record in records {
                fixed_framing::encode(record, &mut buf);
            }
            buf.to_vec()
        };
        let ack = record(uuid::Flags::ACK_TXN, None);
        let mut trailing = framed(&[ack.clone()]);
        trailing.push(0);

        let cases: Vec<(&str, Vec<u8>, bool)> = vec![
            ("ack", framed(&[ack.clone()]), true),
            ("unframed", b"not a record".to_vec(), false),
            ("truncated", framed(&[ack.clone()])[..10].to_vec(), false),
            ("trailing bytes", trailing, false),
            ("two records", framed(&[ack.clone(), ack.clone()]), false),
            (
                "continue",
                framed(&[record(uuid::Flags::CONTINUE_TXN, None)]),
                false,
            ),
            (
                "with chunk",
                framed(&[record(uuid::Flags::ACK_TXN, Some(Default::default()))]),
                false,
            ),
            ("no uuid", framed(&[proto::DiskRecord::default()]), false),
        ];
        for (name, intent, expect) in cases {
            assert_eq!(super::is_ack_intent(&intent), expect, "{name}");
        }
    }
}
