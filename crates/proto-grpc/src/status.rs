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
/// [`MAX_STATUS_MESSAGE_LEN`] bytes. A rendered error chain leads with its
/// top-level context and appends lower-level detail, so truncating the tail
/// preserves the human-meaningful prefix.
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

/// A `tonic::Status` carried as an error.  Instances are constructed through
/// [`status_to_anyhow`], which callers are responsible for using at conversion
/// boundaries. A bare `?` on a `tonic::Result` instead lifts the direct `Status`
/// into the chain, which converts to Unknown and renders as tonic's field dump.
/// This is ugly on purpose, and marks a conversion that needs to be mapped
/// while preserving essential information.
///
/// Its status is never Unknown: [`status_to_anyhow`] unwraps that code into a
/// bare error message, as it's how a non-Status error is encoded onto the wire.
///
/// It derefs to its status, so a holder calls `code()`, `message()`, and the
/// rest directly.
#[derive(Debug)]
pub struct StatusError(pub tonic::Status);

impl std::fmt::Display for StatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({:?})", self.0.message(), self.0.code())
    }
}

impl std::ops::Deref for StatusError {
    type Target = tonic::Status;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::error::Error for StatusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
    }
}

/// Map an `anyhow::Error` into a `tonic::Status`.
/// A [`StatusError`] anywhere in the chain keeps its code, details, and
/// metadata, with the layers around it folded into its message.
/// Anything else becomes an Unknown status wrapping the rendered error chain.
/// Either way the message is bounded to [`MAX_STATUS_MESSAGE_LEN`].
pub fn anyhow_to_status(err: anyhow::Error) -> tonic::Status {
    let message = render_chain(&err);

    match err.downcast::<StatusError>() {
        Ok(StatusError(mut status)) => bound_status(tonic::Status::with_details_and_metadata(
            status.code(),
            message,
            bytes::Bytes::copy_from_slice(status.details()),
            std::mem::take(status.metadata_mut()),
        )),
        Err(_err) => bounded_unknown_status(message),
    }
}

/// Map a `tonic::Status` into an `anyhow::Error`, unwrapping Unknown while
/// preserving every other status as a [`StatusError`] for lossless downcasting.
pub fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    match status.code() {
        tonic::Code::Unknown => anyhow::anyhow!(status.message().to_owned()),
        _ => anyhow::Error::new(StatusError(status)),
    }
}

/// Establish a protocol expectation annotated with its originating peer.
pub fn verify<'p>(source: &'static str, expect: &'static str, peer: &'p str) -> Verify<'p> {
    Verify {
        source,
        expect,
        peer,
    }
}

pub struct Verify<'p> {
    source: &'static str,
    expect: &'static str,
    peer: &'p str,
}

impl Verify<'_> {
    #[inline]
    pub fn ok<T>(&self, value: tonic::Result<T>) -> anyhow::Result<T> {
        value.map_err(|status| self.fail_status(status))
    }

    #[inline]
    pub fn eof<T: serde::Serialize>(&self, value: Option<tonic::Result<T>>) -> anyhow::Result<()> {
        match value {
            None => Ok(()),
            Some(Err(status)) => Err(self.fail_status(status)),
            Some(Ok(value)) => Err(self.fail_msg(value)),
        }
    }

    #[inline]
    pub fn not_eof<T>(&self, value: Option<tonic::Result<T>>) -> anyhow::Result<T> {
        match value {
            Some(value) => self.ok(value),
            None => Err(self.fail_err(anyhow::anyhow!("unexpected EOF"))),
        }
    }

    #[inline]
    pub fn is_eof<T: serde::Serialize>(&self, value: Option<T>) -> anyhow::Result<()> {
        match value {
            None => Ok(()),
            Some(value) => Err(self.fail_msg(value)),
        }
    }

    #[must_use]
    #[cold]
    pub fn fail_msg<T: serde::Serialize>(&self, msg: T) -> anyhow::Error {
        let rendered = bound_message(serde_json::to_string(&msg).unwrap());
        anyhow::format_err!(
            "{} protocol error (expected {}) from {}: {rendered}",
            self.source,
            self.expect,
            self.peer
        )
    }

    #[must_use]
    #[cold]
    pub fn fail_err(&self, err: anyhow::Error) -> anyhow::Error {
        err.context(format!(
            "{} error (expected {}) from {}",
            self.source, self.expect, self.peer
        ))
    }

    #[must_use]
    #[cold]
    pub fn fail_status(&self, status: tonic::Status) -> anyhow::Error {
        self.fail_err(status_to_anyhow(status))
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

/// Render an error chain into a status message: its layers joined as anyhow's
/// alternate `Display` (`{err:#}`) joins them, but with a [`StatusError`]
/// contributing only its message. Its code is carried by the status being
/// built, and naming it again in the message would double up when that status
/// is displayed as an error in turn.
fn render_chain(err: &anyhow::Error) -> String {
    err.chain()
        .map(|err| match err.downcast_ref::<StatusError>() {
            Some(err) => err.0.message().to_string(),
            None => err.to_string(),
        })
        .collect::<Vec<_>>()
        .join(": ")
}

#[cfg(test)]
mod tests {
    /// Every error shape we convert, so that one snapshot fixes the whole
    /// mapping: which errors keep their code, how contexts around a status
    /// fold into its message, and what the peer which receives it then reads.
    fn fixtures() -> Vec<(&'static str, anyhow::Error)> {
        let not_found = || {
            super::status_to_anyhow(tonic::Status::not_found(
                "secret 'acmeCo/token' does not exist",
            ))
        };

        vec![
            ("a plain error", anyhow::anyhow!("connector exited")),
            (
                "a plain error under context",
                anyhow::anyhow!("connector exited").context("starting connector"),
            ),
            ("a status which is the error", not_found()),
            (
                "a status under context",
                not_found().context("failed to resolve secret 'acmeCo/token'"),
            ),
            (
                "a status under two contexts",
                not_found()
                    .context("failed to resolve secret 'acmeCo/token'")
                    .context("starting connector"),
            ),
            (
                "an Unknown status under context",
                super::status_to_anyhow(tonic::Status::unknown("connector exited"))
                    .context("starting connector"),
            ),
            (
                // Lossless, and ugly enough to find: a status which reached a
                // chain through `?` rather than `status_to_anyhow`.
                "an unmapped bare status",
                anyhow::Error::from(tonic::Status::permission_denied("token is not authorized"))
                    .context("opening derive stream"),
            ),
            (
                // A transport source never crosses the wire, so folding it
                // into the message is what carries it to the peer at all.
                "a status carrying a transport source",
                super::status_to_anyhow({
                    let mut status = tonic::Status::unavailable("broker is down");
                    status.set_source(std::sync::Arc::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "connection reset",
                    )));
                    status
                }),
            ),
            (
                "a Verify annotation of a status",
                super::verify("capture", "spec response", "connector")
                    .fail_status(tonic::Status::invalid_argument("bad config")),
            ),
        ]
    }

    #[test]
    fn anyhow_to_status_maps_every_error_shape() {
        let rendered: Vec<String> = fixtures()
            .into_iter()
            .map(|(name, err)| {
                let status = super::anyhow_to_status(err);
                let peer = super::status_to_anyhow(status.clone());

                format!(
                    "{name}\n  {:?}: {}\n  read by its peer: {peer:#}",
                    status.code(),
                    status.message(),
                )
            })
            .collect();

        insta::assert_snapshot!(rendered.join("\n"), @r#"
        a plain error
          Unknown: connector exited
          read by its peer: connector exited
        a plain error under context
          Unknown: starting connector: connector exited
          read by its peer: starting connector: connector exited
        a status which is the error
          NotFound: secret 'acmeCo/token' does not exist
          read by its peer: secret 'acmeCo/token' does not exist (NotFound)
        a status under context
          NotFound: failed to resolve secret 'acmeCo/token': secret 'acmeCo/token' does not exist
          read by its peer: failed to resolve secret 'acmeCo/token': secret 'acmeCo/token' does not exist (NotFound)
        a status under two contexts
          NotFound: starting connector: failed to resolve secret 'acmeCo/token': secret 'acmeCo/token' does not exist
          read by its peer: starting connector: failed to resolve secret 'acmeCo/token': secret 'acmeCo/token' does not exist (NotFound)
        an Unknown status under context
          Unknown: starting connector: connector exited
          read by its peer: starting connector: connector exited
        an unmapped bare status
          Unknown: opening derive stream: status: 'The caller does not have permission to execute the specified operation', self: "token is not authorized"
          read by its peer: opening derive stream: status: 'The caller does not have permission to execute the specified operation', self: "token is not authorized"
        a status carrying a transport source
          Unavailable: broker is down: connection reset
          read by its peer: broker is down: connection reset (Unavailable)
        a Verify annotation of a status
          InvalidArgument: capture error (expected spec response) from connector: bad config
          read by its peer: capture error (expected spec response) from connector: bad config (InvalidArgument)
        "#);
    }

    /// Each conversion is stable under a further hop: a status folded into its
    /// message is the error of the peer which receives it, and folds no more.
    #[test]
    fn every_conversion_is_stable_across_another_hop() {
        for (name, err) in fixtures() {
            let once = super::anyhow_to_status(err);
            let twice = super::anyhow_to_status(super::status_to_anyhow(once.clone()));

            assert_eq!(once.code(), twice.code(), "{name}");
            assert_eq!(once.message(), twice.message(), "{name}");
        }
    }

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
