//! Integration tests of `flowctl raw sync-now`'s single-attempt core, driven
//! over real HTTP against a stub reactor front door which replays scripted
//! NDJSON responses.

use flowctl::raw::sync_now::{Failed, FailedKind, SYNC_NOW_PATH, attempt};
use proto_flow::runtime::{SyncNowResponse, sync_now_response};

/// A full stream — ack, heartbeat, done — completes, and heartbeats
/// arriving mid-wait neither terminate it nor are mistaken for Done.
#[tokio::test]
async fn awaits_done_through_heartbeats() {
    let outcome = run(Script::ok(vec![ack(), heartbeat(), done()])).await;

    outcome.expect("attempt completed");
}

#[tokio::test]
async fn eof_before_ack_is_fatal_on_a_first_attempt() {
    let outcome = run(Script::ok(Vec::new())).await;

    let Err(failed) = outcome else {
        panic!("expected a failure");
    };
    assert!(!failed.acked);
    assert!(!failed.is_retryable(false));
    insta::assert_snapshot!(
        format!("{:#}", failed.into_error("acmeCo/foo/materialize-bar")),
        @"sync-now of acmeCo/foo/materialize-bar did not complete: the leader hung up without a Done response"
    );
}

#[tokio::test]
async fn eof_after_ack_is_retryable() {
    let outcome = run(Script::ok(vec![ack()])).await;

    let Err(failed) = outcome else {
        panic!("expected a failure");
    };
    assert!(failed.acked);
    assert!(failed.is_retryable(true));
}

/// A body which aborts mid-stream, as a leader restart or a dropped
/// connection produces, is always retryable — whether or not we managed to
/// read the Ack before the connection went away.
#[tokio::test]
async fn broken_stream_is_retryable() {
    let outcome = run(Script {
        status: reqwest::StatusCode::OK,
        lines: vec![line(ack())],
        abort: true,
    })
    .await;

    let Err(failed) = outcome else {
        panic!("expected a failure");
    };
    assert!(
        matches!(failed.kind, FailedKind::Transport(_)),
        "{failed:?}"
    );
    assert!(failed.is_retryable(false));
}

/// A terminal error line renders as the data plane's own message.
#[tokio::test]
async fn terminal_error_line_is_contextualized() {
    let outcome = run(Script {
        status: reqwest::StatusCode::NOT_FOUND,
        lines: vec![
            r#"{"error":{"grpcCode":5,"httpCode":404,"message":"task acmeCo/foo/materialize-bar has no live leader session here","httpStatus":"Not Found"}}"#.to_string(),
        ],
        abort: false,
    })
    .await;

    let Err(failed) = outcome else {
        panic!("expected a failure");
    };
    assert!(!failed.is_retryable(false));
    insta::assert_snapshot!(
        format!("{:#}", failed.into_error("acmeCo/foo/materialize-bar")),
        @"sync-now of acmeCo/foo/materialize-bar did not complete: NotFound: task acmeCo/foo/materialize-bar has no live leader session here"
    );
}

/// A load balancer answering in the data plane's stead speaks neither
/// NDJSON nor gRPC codes, and its 5xx is retryable.
#[tokio::test]
async fn unparsed_gateway_response_is_retryable() {
    let outcome = run(Script {
        status: reqwest::StatusCode::BAD_GATEWAY,
        lines: vec!["<html>".to_string()],
        abort: false,
    })
    .await;

    let Err(failed) = outcome else {
        panic!("expected a failure");
    };
    assert!(failed.is_retryable(false));
    insta::assert_snapshot!(
        format!("{:#}", failed.into_error("acmeCo/foo/materialize-bar")),
        @"sync-now of acmeCo/foo/materialize-bar did not complete: HTTP 502 Bad Gateway: <html>"
    );
}

fn ack() -> SyncNowResponse {
    SyncNowResponse {
        response: Some(sync_now_response::Response::Ack(sync_now_response::Ack {})),
    }
}

fn heartbeat() -> SyncNowResponse {
    SyncNowResponse {
        response: Some(sync_now_response::Response::Heartbeat(
            sync_now_response::Heartbeat {},
        )),
    }
}

fn done() -> SyncNowResponse {
    SyncNowResponse {
        response: Some(sync_now_response::Response::Done(
            sync_now_response::Done {},
        )),
    }
}

/// Render a response as the front door's `{"result": ...}` NDJSON line.
fn line(response: SyncNowResponse) -> String {
    serde_json::json!({"result": response}).to_string()
}

/// A scripted front-door response.
#[derive(Clone)]
struct Script {
    status: reqwest::StatusCode,
    /// NDJSON lines of the response body.
    lines: Vec<String>,
    /// Abort the body after its lines, rather than ending it cleanly.
    abort: bool,
}

impl Script {
    fn ok(responses: Vec<SyncNowResponse>) -> Self {
        Self {
            status: reqwest::StatusCode::OK,
            lines: responses.into_iter().map(line).collect(),
            abort: false,
        }
    }
}

/// Drive `attempt` against a stub front door replaying `script`.
async fn run(script: Script) -> Result<(), Failed> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let app = axum::Router::new().route(
        SYNC_NOW_PATH,
        axum::routing::post(move |body: String| {
            let script = script.clone();
            async move {
                assert_eq!(body, r#"{"taskName":"acmeCo/foo/materialize-bar"}"#);
                serve(script)
            }
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    let url = url::Url::parse(&format!("http://{addr}{SYNC_NOW_PATH}")).unwrap();
    let outcome = attempt(
        &reqwest::Client::new(),
        &url,
        "a-reactor-token",
        "acmeCo/foo/materialize-bar",
    )
    .await;

    server.abort();
    outcome
}

/// Serve `script` as a chunk-per-line streamed body, so that a mid-stream
/// abort is distinguishable from a clean end.
fn serve(script: Script) -> axum::response::Response {
    let Script {
        status,
        lines,
        abort,
    } = script;

    let chunks = lines
        .into_iter()
        .map(|line| Ok(format!("{line}\n")))
        .chain(abort.then(|| Err(std::io::Error::other("mid-stream abort"))));

    axum::response::IntoResponse::into_response((
        axum::http::StatusCode::from_u16(status.as_u16()).unwrap(),
        axum::body::Body::from_stream(futures::stream::iter(chunks)),
    ))
}
