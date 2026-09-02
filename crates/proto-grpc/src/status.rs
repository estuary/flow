//! Bounding of `tonic::Status` messages, so that a status survives its trip
//! over the wire.

/// Maximum byte length of a `tonic::Status` message that we build from a
/// formatted error. gRPC status text rides in an HTTP/2 trailer; an oversized
/// trailer forces the header block across many CONTINUATION frames, tripping
/// `h2`'s `too_many_continuations` guard, which aborts the connection and
/// *replaces* the real status text with an opaque transport error
///
/// The ceiling exists so that *any* error, anticipated or not, stays within a
/// single HTTP/2 frame (default `max_frame_size` is 16 KiB) and never needs a
/// CONTINUATION frame at all. The `grpc-message` trailer is percent-encoded, so
/// a byte can expand up to 3x (`%XX`); 4 KiB raw is ≤ 12 KiB encoded, fitting
/// one frame even in the worst case.
pub const MAX_STATUS_MESSAGE_LEN: usize = 4096;

/// Build a `tonic::Status::unknown` whose message is bounded to
/// [`MAX_STATUS_MESSAGE_LEN`] bytes. The anyhow debug format leads with the
/// error's top-level context and appends lower-level detail, so truncating the
/// tail preserves the human-meaningful prefix.
pub fn bounded_unknown_status(message: String) -> tonic::Status {
    tonic::Status::unknown(bound_message(message))
}

/// Bound the message of a `Status` we did not format ourselves — one round
/// tripped from a peer or produced by a connector — preserving its code,
/// details, and metadata. Guards the same h2 trailer limit as
/// [`bounded_unknown_status`] so that e.g. a misbehaving connector emitting a
/// huge message can't produce a status that's dropped in transit. Returns the
/// status untouched when its message already fits.
pub fn bound_status(status: tonic::Status) -> tonic::Status {
    if status.message().len() <= MAX_STATUS_MESSAGE_LEN {
        return status;
    }
    tonic::Status::with_details_and_metadata(
        status.code(),
        bound_message(status.message().to_string()),
        bytes::Bytes::copy_from_slice(status.details()),
        status.metadata().clone(),
    )
}

/// Truncate `message` to at most [`MAX_STATUS_MESSAGE_LEN`] bytes, cutting the
/// tail on a UTF-8 boundary and marking the elision. Returns it unchanged when
/// it already fits.
fn bound_message(mut message: String) -> String {
    if message.len() > MAX_STATUS_MESSAGE_LEN {
        const SUFFIX: &str = "… [truncated]";
        // Reserve room for SUFFIX and back off to a UTF-8 char boundary.
        let mut end = MAX_STATUS_MESSAGE_LEN - SUFFIX.len();
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push_str(SUFFIX);
    }
    message
}

/// Map an `anyhow::Error` into a `tonic::Status`.
/// If the error is already a Status, it's downcast (and bounded).
/// Otherwise an Unknown status wraps the formatted error chain, bounded to
/// [`MAX_STATUS_MESSAGE_LEN`].
pub fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    match err.downcast::<tonic::Status>() {
        Ok(status) => bound_status(status),
        Err(err) => bounded_unknown_status(format!("{err:?}")),
    }
}

/// Convert a handler's returned error or panic into a gRPC status.
///
/// Catching panics prevents a dropped response sender from looking like a
/// successful end-of-stream to the peer. The status includes only the panic
/// payload; the panic hook logs its location and any enabled backtrace locally.
pub async fn catch_panic<F, T>(future: F) -> tonic::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(future)).await {
        Ok(result) => result.map_err(anyhow_to_status),
        Err(payload) => {
            let message = payload
                .downcast_ref::<&'static str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");

            Err(bounded_unknown_status(format!(
                "handler panicked: {message}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn catch_panic_renders_every_outcome() {
        assert_eq!(super::catch_panic(async { Ok(1) }).await.unwrap(), 1);

        let returned =
            super::catch_panic(async { Err::<(), _>(anyhow::format_err!("a returned error")) })
                .await
                .unwrap_err();
        assert_eq!(returned.message(), "a returned error");

        let panicked = super::catch_panic::<_, ()>(async { panic!("{} payload", "a formatted") })
            .await
            .unwrap_err();
        assert_eq!(panicked.message(), "handler panicked: a formatted payload");

        let non_string = super::catch_panic::<_, ()>(async { std::panic::panic_any(42u64) })
            .await
            .unwrap_err();
        assert_eq!(
            non_string.message(),
            "handler panicked: <non-string panic payload>"
        );

        // An oversized payload is bounded like any other status message.
        let huge = super::catch_panic::<_, ()>(async { panic!("{}", "a".repeat(1 << 16)) })
            .await
            .unwrap_err();
        assert!(huge.message().len() <= super::MAX_STATUS_MESSAGE_LEN);
        assert!(huge.message().ends_with("… [truncated]"), "{huge}");
    }
}
