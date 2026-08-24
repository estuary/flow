//! `flowctl raw sync-now`: force a materialization to immediately commit its
//! open transaction, and exit once that transaction is fully acknowledged --
//! so that `flowctl raw sync-now --task X && run-analytics.sh` does what it
//! says. See the TaskControl service in `go/protocols/runtime/runtime.proto`.
//!
//! We call the reactor front door's HTTP/NDJSON endpoint, the same one the
//! dashboard uses. `go/runtime/task_control_http.go` documents its contract.

use crate::CliContext;
use anyhow::Context;
use std::time::Duration;

/// Path of TaskControl.SyncNow on a reactor front door. Mirrors
/// `TaskControlSyncNowPath` in `go/runtime/task_control_http.go`.
pub const SYNC_NOW_PATH: &str = "/v1/task-control/sync-now";

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct SyncNow {
    #[clap(flatten)]
    task: crate::ops::TaskSelector,
}

/// Attempt SyncNow until it completes or a failure proves terminal.
///
/// SyncNow is idempotent, and a retry's "everything as of this call" contract
/// still covers all data which preceded the first attempt.
pub async fn do_sync_now(ctx: &CliContext, args: &SyncNow) -> anyhow::Result<()> {
    let auth = crate::dataplane::user_task_auth_watch(&ctx.rest, &ctx.user_tokens, &args.task.task);
    let task = &args.task.task;
    let client = new_http_client()?;

    let mut acked_ever = false;
    let mut backoff = Duration::from_secs(1);

    loop {
        // Re-read the front door on each attempt, as an hour-long wait may
        // have outlived its reactor token.
        let (address, token) = {
            let ready = auth.ready().await.token();
            let model = ready.result()?;
            (model.reactor_address.clone(), model.reactor_token.clone())
        };
        let url = url::Url::parse(&address)
            .and_then(|url| url.join(SYNC_NOW_PATH))
            .with_context(|| format!("building a sync-now URL from reactor address {address}"))?;

        let failed = match attempt(&client, &url, &token, task).await {
            Ok(()) => return Ok(()),
            Err(failed) => failed,
        };
        acked_ever |= failed.acked;

        if !failed.is_retryable(acked_ever) {
            return Err(failed.into_error(task));
        }
        tracing::warn!(
            error = %failed.detail(),
            retry_in = ?backoff,
            "sync-now attempt failed; retrying",
        );

        tokio::time::sleep(backoff).await;
        backoff = if failed.acked {
            Duration::from_secs(1) // Progress was made; start over.
        } else {
            (backoff * 2).min(Duration::from_secs(30))
        };
    }
}

/// Build the HTTP client which calls the reactor front door.
///
/// `reqwest` verifies against webpki's bundled roots, and -- unlike tonic's
/// `tls-native-roots` -- ignores `SSL_CERT_FILE`. Honor it explicitly, so that
/// a data plane behind a private CA (such as a local stack) is reachable.
fn new_http_client() -> anyhow::Result<reqwest::Client> {
    // The read timeout maps a stalled response stream onto the retryable
    // Transport failure. It's per-read: it bounds the gap between the
    // server's 15s heartbeats, not the (possibly hour-long) wait overall.
    let mut builder = reqwest::Client::builder().read_timeout(Duration::from_secs(60));

    if let Some(path) = std::env::var_os("SSL_CERT_FILE") {
        let path = std::path::PathBuf::from(path);
        let pem = std::fs::read(&path)
            .with_context(|| format!("reading SSL_CERT_FILE {}", path.display()))?;

        for cert in reqwest::Certificate::from_pem_bundle(&pem)
            .with_context(|| format!("parsing certificates of {}", path.display()))?
        {
            builder = builder.add_root_certificate(cert);
        }
    }
    Ok(builder.build()?)
}

/// One NDJSON line of a SyncNow response stream. Both fields are absent in a
/// line we don't understand.
#[derive(serde::Deserialize)]
struct Line {
    result: Option<proto_flow::runtime::SyncNowResponse>,
    error: Option<ErrorLine>,
}

/// A terminal `{"error": ...}` line, in grpc-gateway's mid-stream convention.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ErrorLine {
    grpc_code: i32,
    message: String,
}

/// Failure of a single SyncNow attempt.
#[derive(Debug)]
pub struct Failed {
    /// The leader acknowledged this attempt before it failed, as happens on a
    /// leader restart or shard reassignment mid-wait.
    pub acked: bool,
    pub kind: FailedKind,
}

#[derive(Debug)]
pub enum FailedKind {
    /// A terminal error line of the data plane, carrying its gRPC code.
    Status { code: tonic::Code, message: String },
    /// A response which isn't a line of the documented protocol, as an
    /// intervening proxy or load balancer returns in the data plane's stead.
    Unparsed {
        status: reqwest::StatusCode,
        body: String,
    },
    /// The request, or its response stream, failed in transport.
    Transport(String),
    /// The stream ended without its final Done response.
    Eof,
}

impl Failed {
    /// Is this failure a transient condition of the task's leader, rather than
    /// a verdict on the request? `acked_ever` distinguishes the two for
    /// NotFound and EOF: before any acknowledgement they're the diagnostic
    /// that the task isn't running here (or isn't on the V2 runtime), but
    /// afterwards they're a leader restart racing our reconnect -- the
    /// replacement session isn't addressable until it reaches Join consensus.
    pub fn is_retryable(&self, acked_ever: bool) -> bool {
        match &self.kind {
            FailedKind::Status { code, .. } => match code {
                // The leader itself reports Unavailable to ask for a retry,
                // and DeadlineExceeded is our reactor token's claims deadline.
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => true,
                tonic::Code::NotFound => acked_ever,
                _ => false,
            },
            // A 5xx from an intermediary is transient; a 4xx is a verdict.
            FailedKind::Unparsed { status, .. } => status.is_server_error(),
            FailedKind::Transport(_) => true,
            FailedKind::Eof => acked_ever,
        }
    }

    fn detail(&self) -> String {
        match &self.kind {
            FailedKind::Status { code, message } => format!("{code:?}: {message}"),
            FailedKind::Unparsed { status, body } => format!("HTTP {status}: {body}"),
            FailedKind::Transport(detail) => detail.clone(),
            FailedKind::Eof => "the leader hung up without a Done response".to_string(),
        }
    }

    pub fn into_error(self, task: &str) -> anyhow::Error {
        anyhow::anyhow!("sync-now of {task} did not complete: {}", self.detail())
    }
}

/// Invoke SyncNow once, returning when the leader reports the awaited
/// transaction committed. Nothing is printed: the exit status is the whole
/// contract.
pub async fn attempt(
    client: &reqwest::Client,
    url: &url::Url,
    token: &str,
    task: &str,
) -> Result<(), Failed> {
    let mut acked = false;

    let response = client
        .post(url.clone())
        .bearer_auth(token)
        .json(&proto_flow::runtime::SyncNowRequest {
            task_name: task.to_string(),
        })
        .send()
        .await
        .map_err(|err| Failed {
            acked,
            kind: FailedKind::Transport(err.to_string()),
        })?;

    let status = response.status();
    let mut lines = tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(
        tokio_util::io::StreamReader::new(futures::TryStreamExt::map_err(
            response.bytes_stream(),
            std::io::Error::other,
        )),
    ));

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) if line.trim().is_empty() => continue,
            Ok(Some(line)) => line,
            // A well-formed stream always returns from its Done, below.
            Ok(None) if status.is_success() => {
                return Err(Failed {
                    acked,
                    kind: FailedKind::Eof,
                });
            }
            Ok(None) => {
                return Err(Failed {
                    acked,
                    kind: FailedKind::Unparsed {
                        status,
                        body: String::new(),
                    },
                });
            }
            Err(err) => {
                return Err(Failed {
                    acked,
                    kind: FailedKind::Transport(err.to_string()),
                });
            }
        };

        let Ok(Line { result, error }) = serde_json::from_str::<Line>(&line) else {
            return Err(Failed {
                acked,
                kind: FailedKind::Unparsed { status, body: line },
            });
        };

        if let Some(ErrorLine { grpc_code, message }) = error {
            return Err(Failed {
                acked,
                kind: FailedKind::Status {
                    code: tonic::Code::from_i32(grpc_code),
                    message,
                },
            });
        }

        use proto_flow::runtime::sync_now_response::Response;
        match result.and_then(|result| result.response) {
            // Our request reached the leader and the commit is forced.
            Some(Response::Ack(_)) => acked = true,
            // Consumed for liveness only.
            Some(Response::Heartbeat(_)) => {}
            Some(Response::Done(_)) => return Ok(()),
            None => {} // A line, or response variant, we don't know about.
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Failed, FailedKind};

    /// Retry classification: NotFound and EOF are the "not running here"
    /// diagnostic until an Ack proves otherwise, while Unavailable and
    /// DeadlineExceeded are always transients of the leader or of our token.
    #[test]
    fn retry_classification() {
        let cases = [
            ("not-found, never acked", not_found(), false, false),
            ("not-found, acked earlier", not_found(), true, true),
            ("unavailable, never acked", unavailable(), false, true),
            ("unavailable, acked earlier", unavailable(), true, true),
            ("deadline, never acked", deadline(), false, true),
            ("deadline, acked earlier", deadline(), true, true),
            ("permission denied", permission_denied(), true, false),
            ("eof, never acked", eof(), false, false),
            ("eof, acked earlier", eof(), true, true),
            ("bad gateway", bad_gateway(), false, true),
            ("not found by a proxy", proxied_not_found(), false, false),
            ("transport", transport(), false, true),
        ];

        for (name, failed, acked_ever, want) in cases {
            assert_eq!(failed.is_retryable(acked_ever), want, "case `{name}`");
        }
    }

    fn status(code: tonic::Code) -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Status {
                code,
                message: "as the leader or front door reports".to_string(),
            },
        }
    }

    fn not_found() -> Failed {
        status(tonic::Code::NotFound)
    }

    fn unavailable() -> Failed {
        status(tonic::Code::Unavailable)
    }

    // As the reactor front door reports when the claims deadline of a
    // long-running relay elapses.
    fn deadline() -> Failed {
        status(tonic::Code::DeadlineExceeded)
    }

    fn permission_denied() -> Failed {
        status(tonic::Code::PermissionDenied)
    }

    fn bad_gateway() -> Failed {
        unparsed(reqwest::StatusCode::BAD_GATEWAY)
    }

    // A 404 from an intermediary is a verdict on our URL, not on the task.
    fn proxied_not_found() -> Failed {
        unparsed(reqwest::StatusCode::NOT_FOUND)
    }

    fn unparsed(status: reqwest::StatusCode) -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Unparsed {
                status,
                body: "<html>".to_string(),
            },
        }
    }

    fn eof() -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Eof,
        }
    }

    fn transport() -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Transport("connection reset".to_string()),
        }
    }
}
