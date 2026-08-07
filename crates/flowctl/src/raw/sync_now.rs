//! `flowctl raw sync-now`: force a materialization to immediately commit its
//! open transaction, and exit once that transaction is fully acknowledged --
//! so that `flowctl raw sync-now --task X && run-analytics.sh` does what it
//! says. See the TaskControl service in `go/protocols/runtime/runtime.proto`.

use crate::CliContext;
use std::io::Write;
use std::time::Duration;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct SyncNow {
    #[clap(flatten)]
    task: crate::ops::TaskSelector,
    /// Also print the periodic progress heartbeats of the awaited transaction.
    #[clap(long)]
    progress: bool,
    /// Print the acknowledgement and exit without awaiting the commit.
    /// The commit has still been forced.
    #[clap(long)]
    no_wait: bool,
    /// Give up after this long (e.g. "10m"), bounding the entire invocation.
    /// By default sync-now waits indefinitely, retrying leader restarts.
    #[clap(long)]
    timeout: Option<humantime::Duration>,
}

pub async fn do_sync_now(ctx: &CliContext, args: &SyncNow) -> anyhow::Result<()> {
    let Some(timeout) = args.timeout else {
        return run(ctx, args).await;
    };
    tokio::time::timeout(timeout.into(), run(ctx, args))
        .await
        .map_err(|_elapsed| {
            anyhow::anyhow!("sync-now of {} timed out after {timeout}", args.task.task)
        })?
}

/// Attempt SyncNow until it completes, a failure proves terminal, or the
/// caller's timeout elapses.
///
/// SyncNow is idempotent, and a retry's "everything as of this call" contract
/// still covers all data which preceded the first attempt.
async fn run(ctx: &CliContext, args: &SyncNow) -> anyhow::Result<()> {
    let auth = crate::dataplane::user_task_auth_watch(&ctx.rest, &ctx.user_tokens, &args.task.task);
    let task = &args.task.task;

    let mut acked_ever = false;
    let mut backoff = Duration::from_secs(1);

    loop {
        // Re-mint the client on each attempt, as an hour-long wait may have
        // outlived its token.
        let mut client = crate::dataplane::new_task_control_client(&auth).await?;

        let failed = match attempt(&mut client, args, &mut std::io::stdout()).await {
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

/// Failure of a single SyncNow attempt.
#[derive(Debug)]
struct Failed {
    /// The leader acknowledged this attempt before it failed, as happens on a
    /// leader restart or shard reassignment mid-wait.
    acked: bool,
    kind: FailedKind,
}

#[derive(Debug)]
enum FailedKind {
    Status(tonic::Status),
    /// The stream ended without its final Done response.
    Eof,
    /// Failure writing to `out`, rather than any failure of the task.
    Write(anyhow::Error),
}

impl Failed {
    /// Is this failure a transient condition of the task's leader, rather than
    /// a verdict on the request? `acked_ever` distinguishes the two for
    /// NotFound and EOF: before any acknowledgement they're the diagnostic
    /// that the task isn't running here (or isn't on the V2 runtime), but
    /// afterwards they're a leader restart racing our reconnect -- the
    /// replacement session isn't addressable until it reaches Join consensus.
    fn is_retryable(&self, acked_ever: bool) -> bool {
        match &self.kind {
            // The leader itself reports Unavailable to ask for a retry.
            FailedKind::Status(status) if status.code() == tonic::Code::Unavailable => true,
            FailedKind::Status(status) if status.code() == tonic::Code::NotFound => acked_ever,
            FailedKind::Status(_) => false,
            FailedKind::Eof => acked_ever,
            FailedKind::Write(_) => false,
        }
    }

    fn detail(&self) -> String {
        match &self.kind {
            FailedKind::Status(status) => format!("{status}"),
            FailedKind::Eof => "stream ended without a Done response".to_string(),
            FailedKind::Write(err) => format!("{err:#}"),
        }
    }

    fn into_error(self, task: &str) -> anyhow::Error {
        let context = format!("sync-now of {task} did not complete");

        match self.kind {
            FailedKind::Status(status) => runtime_next::status_to_anyhow(status).context(context),
            FailedKind::Eof => anyhow::anyhow!("{context}: the leader hung up on us"),
            FailedKind::Write(err) => err.context(context),
        }
    }
}

/// Invoke SyncNow once, writing its Ack and Done (and, if `--progress`, its
/// heartbeats) to `out` as JSON lines.
async fn attempt(
    client: &mut crate::dataplane::TaskControlClient,
    args: &SyncNow,
    out: &mut impl Write,
) -> Result<(), Failed> {
    let mut acked = false;

    let mut stream = match client
        .sync_now(proto_flow::runtime::SyncNowRequest {
            task_name: args.task.task.clone(),
        })
        .await
    {
        Ok(response) => response.into_inner(),
        Err(status) => {
            return Err(Failed {
                acked,
                kind: FailedKind::Status(status),
            });
        }
    };

    loop {
        let message = match stream.message().await {
            Ok(Some(message)) => message,
            // A well-formed stream always returns from its Done, below,
            // so reaching EOF here means the leader hung up on us.
            Ok(None) => {
                return Err(Failed {
                    acked,
                    kind: FailedKind::Eof,
                });
            }
            Err(status) => {
                return Err(Failed {
                    acked,
                    kind: FailedKind::Status(status),
                });
            }
        };

        use proto_flow::runtime::sync_now_response::Response;
        match &message.response {
            Some(Response::Ack(_)) => {
                acked = true;

                if let Err(err) = write_line(out, &message) {
                    return Err(Failed {
                        acked,
                        kind: FailedKind::Write(err),
                    });
                }
                // Hanging up now is harmless: the commit has been forced.
                if args.no_wait {
                    return Ok(());
                }
            }
            Some(Response::Progress(_)) => {
                // Heartbeats are consumed for liveness, but an hour-long wait
                // is a lot of lines to print unasked.
                if args.progress
                    && let Err(err) = write_line(out, &message)
                {
                    return Err(Failed {
                        acked,
                        kind: FailedKind::Write(err),
                    });
                }
            }
            Some(Response::Done(_)) => {
                if let Err(err) = write_line(out, &message) {
                    return Err(Failed {
                        acked,
                        kind: FailedKind::Write(err),
                    });
                }
                return Ok(());
            }
            None => {} // A response variant we don't know about.
        }
    }
}

fn write_line(
    out: &mut impl Write,
    message: &proto_flow::runtime::SyncNowResponse,
) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *out, message)?;
    out.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::{Failed, FailedKind, SyncNow, attempt};
    use futures::StreamExt;
    use proto_flow::runtime::{SyncNowRequest, SyncNowResponse, sync_now_response};

    #[tokio::test]
    async fn awaits_done_and_suppresses_heartbeats() {
        let (outcome, output) = run(script(), false, false).await;

        outcome.expect("attempt completed");
        insta::assert_snapshot!(output, @r###"
        {"ack":{"outcome":"HELD_COLLAPSED","status":{"sourcedDocsTotal":"32","headPhase":"Extend"}}}
        {"done":{"committedDocsTotal":"32","openAgeMillis":"1500"}}
        "###);
    }

    #[tokio::test]
    async fn progress_flag_prints_heartbeats() {
        let (outcome, output) = run(script(), true, false).await;

        outcome.expect("attempt completed");
        insta::assert_snapshot!(output, @r###"
        {"ack":{"outcome":"HELD_COLLAPSED","status":{"sourcedDocsTotal":"32","headPhase":"Extend"}}}
        {"progress":{"sourcedDocsTotal":"64","headPhase":"StartCommit"}}
        {"done":{"committedDocsTotal":"32","openAgeMillis":"1500"}}
        "###);
    }

    #[tokio::test]
    async fn no_wait_hangs_up_after_ack() {
        let (outcome, output) = run(script(), false, true).await;

        outcome.expect("attempt completed");
        insta::assert_snapshot!(output, @r###"
        {"ack":{"outcome":"HELD_COLLAPSED","status":{"sourcedDocsTotal":"32","headPhase":"Extend"}}}
        "###);
    }

    /// Retry classification: NotFound and EOF are the "not running here"
    /// diagnostic until an Ack proves otherwise, while Unavailable is always
    /// a leader transient.
    #[test]
    fn retry_classification() {
        let cases = [
            ("not-found, never acked", not_found(), false, false),
            ("not-found, acked earlier", not_found(), true, true),
            ("unavailable, never acked", unavailable(), false, true),
            ("unavailable, acked earlier", unavailable(), true, true),
            ("eof, never acked", eof(), false, false),
            ("eof, acked earlier", eof(), true, true),
            ("permission denied", permission_denied(), true, false),
        ];

        for (name, failed, acked_ever, want) in cases {
            assert_eq!(failed.is_retryable(acked_ever), want, "case `{name}`");
        }
    }

    #[tokio::test]
    async fn broken_stream_after_ack_is_retryable() {
        let (outcome, _output) = run(
            vec![
                Ok(ack()),
                Err(tonic::Status::unavailable("leader went away")),
            ],
            false,
            false,
        )
        .await;

        let Err(failed) = outcome else {
            panic!("expected a failure");
        };
        assert!(failed.acked);
        assert!(failed.is_retryable(true));
    }

    #[tokio::test]
    async fn eof_before_ack_is_fatal_on_a_first_attempt() {
        let (outcome, _output) = run(Vec::new(), false, false).await;

        let Err(failed) = outcome else {
            panic!("expected a failure");
        };
        assert!(!failed.acked);
        assert!(!failed.is_retryable(false));
        insta::assert_snapshot!(
            format!("{:#}", failed.into_error("acmeCo/foo/materialize-bar")),
            @"sync-now of acmeCo/foo/materialize-bar did not complete: the leader hung up on us"
        );
    }

    #[tokio::test]
    async fn eof_after_ack_is_retryable() {
        let (outcome, _output) = run(vec![Ok(ack())], false, false).await;

        let Err(failed) = outcome else {
            panic!("expected a failure");
        };
        assert!(failed.acked);
        assert!(failed.is_retryable(true));
    }

    #[tokio::test]
    async fn terminal_status_is_contextualized() {
        let (outcome, _output) = run(
            vec![Err(tonic::Status::not_found("task not in this data plane"))],
            false,
            false,
        )
        .await;

        let Err(failed) = outcome else {
            panic!("expected a failure");
        };
        assert!(!failed.is_retryable(false));
        insta::assert_snapshot!(
            format!("{:#}", failed.into_error("acmeCo/foo/materialize-bar")),
            @"sync-now of acmeCo/foo/materialize-bar did not complete: status: 'Some requested entity was not found', self: \"task not in this data plane\""
        );
    }

    fn not_found() -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Status(tonic::Status::not_found("no live leader session here")),
        }
    }

    fn unavailable() -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Status(tonic::Status::unavailable("leader session ended")),
        }
    }

    fn permission_denied() -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Status(tonic::Status::permission_denied("not authorized")),
        }
    }

    fn eof() -> Failed {
        Failed {
            acked: false,
            kind: FailedKind::Eof,
        }
    }

    // A leader's full response sequence: ack, one heartbeat, and done.
    fn script() -> Vec<tonic::Result<SyncNowResponse>> {
        vec![
            Ok(ack()),
            Ok(SyncNowResponse {
                response: Some(sync_now_response::Response::Progress(
                    sync_now_response::Status {
                        sourced_docs_total: 64,
                        head_phase: "StartCommit".to_string(),
                        ..Default::default()
                    },
                )),
            }),
            Ok(SyncNowResponse {
                response: Some(sync_now_response::Response::Done(sync_now_response::Done {
                    committed_docs_total: 32,
                    committed_bytes_total: 0,
                    open_age_millis: 1500,
                })),
            }),
        ]
    }

    fn ack() -> SyncNowResponse {
        SyncNowResponse {
            response: Some(sync_now_response::Response::Ack(sync_now_response::Ack {
                outcome: sync_now_response::Outcome::HeldCollapsed as i32,
                status: Some(sync_now_response::Status {
                    sourced_docs_total: 32,
                    head_phase: "Extend".to_string(),
                    ..Default::default()
                }),
            })),
        }
    }

    /// Drive `attempt` against a stub leader replaying `script`.
    async fn run(
        script: Vec<tonic::Result<SyncNowResponse>>,
        progress: bool,
        no_wait: bool,
    ) -> (Result<(), Failed>, String) {
        // `dial_channel` builds a TLS-capable Endpoint, which rustls refuses
        // to do without a process-level provider (installed by main.rs).
        _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    proto_grpc::runtime::task_control_server::TaskControlServer::new(Stub(
                        std::sync::Mutex::new(script),
                    )),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );

        let channel = gazette::dial_channel(&format!("http://{addr}")).unwrap();
        let mut client =
            proto_grpc::runtime::task_control_client::TaskControlClient::with_interceptor(
                channel,
                proto_grpc::Metadata::new(),
            );

        let args = SyncNow {
            task: crate::ops::TaskSelector {
                task: "acmeCo/foo/materialize-bar".to_string(),
            },
            progress,
            no_wait,
            timeout: None,
        };
        let mut output = Vec::new();
        let outcome = attempt(&mut client, &args, &mut output).await;

        server.abort();
        (outcome, String::from_utf8(output).unwrap())
    }

    struct Stub(std::sync::Mutex<Vec<tonic::Result<SyncNowResponse>>>);

    #[tonic::async_trait]
    impl proto_grpc::runtime::task_control_server::TaskControl for Stub {
        type SyncNowStream = futures::stream::BoxStream<'static, tonic::Result<SyncNowResponse>>;

        async fn sync_now(
            &self,
            _request: tonic::Request<SyncNowRequest>,
        ) -> tonic::Result<tonic::Response<Self::SyncNowStream>> {
            let script = std::mem::take(&mut *self.0.lock().unwrap());
            Ok(tonic::Response::new(futures::stream::iter(script).boxed()))
        }
    }
}
