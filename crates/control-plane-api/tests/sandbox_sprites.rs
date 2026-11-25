//! Tests of the Sprites provider.
//!
//! Most run against in-process fakes of the Sprites API. Those named `live_`
//! run against the real API, so they're ignored by default. They create
//! sprites and delete them when done. They need `SPRITES_TOKEN`, a Sprites
//! API token, and exactly one of these sources of an x86_64 Linux flowctl
//! which has `flowctl raw sandbox-exec`:
//!
//! - `SANDBOX_FLOWCTL_URL`: a download URL, from which `provision` installs it.
//! - `SANDBOX_FLOWCTL_PATH`: a local build, such as `target/debug/flowctl`.
//!   Each test uploads it into its sprite and then runs `Sprites::setup`.
//!
//! Run them with:
//!
//!   mise exec -- cargo nextest run -p control-plane-api --test sandbox_sprites --run-ignored only

use axum::response::IntoResponse;
use control_plane_api::sandboxes::provider::{Error, Exec, Output, Provider, WRAPPER_ARGV};
use control_plane_api::sandboxes::sprites::Sprites;
use control_plane_api::sprites::Client;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite;

type ServerSocket = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

/// Bounds each step of a fake server, so a missing message fails a test
/// rather than hanging it.
const STEP: std::time::Duration = std::time::Duration::from_secs(5);
/// Within this long of being dropped, an Exec must have closed its connection.
const PROMPTLY: std::time::Duration = std::time::Duration::from_secs(1);

fn provider(base_url: url::Url, prefix: &str) -> Sprites {
    Sprites {
        client: std::sync::Arc::new(Client::with_base_url("test-token".to_string(), base_url)),
        flowctl_url: "https://flowctl.invalid/flowctl".parse().unwrap(),
        prefix: prefix.to_string(),
    }
}

/// The URI and Authorization header of the exec request of a fake Sprites API.
type Handshake = (String, String);

/// Starts a fake Sprites API which accepts one exec WebSocket and runs
/// `script` over it. If `reject` is set, the upgrade is refused with it.
/// The returned task yields the exec request.
async fn fake_exec<F, Fut>(
    reject: Option<(u16, &'static str)>,
    script: F,
) -> (Sprites, tokio::task::JoinHandle<Handshake>)
where
    F: FnOnce(ServerSocket) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());

    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut handshake = None;

        let callback = |request: &tungstenite::handshake::server::Request,
                        response: tungstenite::handshake::server::Response| {
            handshake = Some((
                request.uri().to_string(),
                request.headers()["authorization"]
                    .to_str()
                    .unwrap()
                    .to_string(),
            ));
            match reject {
                None => Ok(response),
                Some((status, body)) => Err(tungstenite::http::Response::builder()
                    .status(status)
                    .body(Some(body.to_string()))
                    .unwrap()),
            }
        };
        let accepted = tokio_tungstenite::accept_hdr_async(stream, callback).await;

        match (accepted, reject) {
            (Ok(socket), None) => script(socket).await,
            (Err(_), Some(_)) => {}
            (accepted, _) => panic!("unexpected handshake outcome: {:?}", accepted.err()),
        }
        handshake.unwrap()
    });

    (provider(base_url.parse().unwrap(), "sbx-"), task)
}

/// The next message which isn't a reply to the server's ping.
async fn recv(socket: &mut ServerSocket) -> tungstenite::Message {
    loop {
        let message = tokio::time::timeout(STEP, socket.next())
            .await
            .expect("timed out awaiting a message")
            .expect("connection ended")
            .expect("connection failed");
        if !matches!(message, tungstenite::Message::Pong(_)) {
            return message;
        }
    }
}

async fn send(socket: &mut ServerSocket, message: tungstenite::Message) {
    socket.send(message).await.unwrap();
}

fn binary(data: &[u8]) -> tungstenite::Message {
    tungstenite::Message::Binary(bytes::Bytes::copy_from_slice(data))
}

fn text(data: &str) -> tungstenite::Message {
    tungstenite::Message::Text(data.into())
}

/// Expects a close message, and then the end of the TCP connection within
/// `within`. Like Sprites, it doesn't reply to the close message: the
/// connection is read below the WebSocket, which therefore sends nothing.
async fn expect_close(mut socket: ServerSocket, within: std::time::Duration) {
    assert_eq!(recv(&mut socket).await, tungstenite::Message::Close(None));

    let tcp = socket.get_mut();
    let mut buf = [0u8; 1024];
    tokio::time::timeout(within, async {
        loop {
            match tokio::io::AsyncReadExt::read(tcp, &mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => panic!("unexpected {n} bytes after the close message"),
            }
        }
    })
    .await
    .expect("connection wasn't closed promptly");
}

/// Output items as strings, through the last.
async fn collect(
    output: &mut futures::stream::BoxStream<'static, Result<Output, Error>>,
) -> Vec<String> {
    let mut items = Vec::new();
    while let Some(item) = tokio::time::timeout(STEP, output.next())
        .await
        .expect("timed out awaiting output")
    {
        items.push(match item {
            Ok(output) => format!("{output:?}"),
            Err(err) => format!("error: {err:#}"),
        });
    }
    items
}

#[tokio::test]
async fn test_exec_relays_frames() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        send(
            &mut socket,
            text(r#"{"type":"session_info","session_id":"42","command":"cat","tty":false}"#),
        )
        .await;
        send(&mut socket, binary(b"\x01out")).await;
        send(&mut socket, binary(b"\x02err")).await;
        send(&mut socket, tungstenite::Message::Ping(bytes::Bytes::new())).await;
        send(&mut socket, text(r#"{"type":"port_opened","port":80}"#)).await;

        // Stdin data and its EOF, which the command echoes and then exits.
        assert_eq!(recv(&mut socket).await, binary(b"\x00hello"));
        send(&mut socket, binary(b"\x01hello")).await;
        assert_eq!(recv(&mut socket).await, binary(&[4]));
        send(&mut socket, binary(&[3, 7])).await;

        expect_close(socket, PROMPTLY).await;
    })
    .await;

    let Exec {
        mut stdin,
        mut output,
    } = sprites
        .exec(
            "sbx-one",
            &serde_json::json!({}),
            &["cat", "two words&more"],
        )
        .await
        .unwrap();
    stdin
        .send(bytes::Bytes::from_static(b"hello"))
        .await
        .unwrap();
    drop(stdin);

    let items = collect(&mut output).await;
    let handshake = server.await.unwrap();

    insta::assert_debug_snapshot!((items, handshake), @r#"
    (
        [
            "Stdout(b\"out\")",
            "Stderr(b\"err\")",
            "Stdout(b\"hello\")",
            "Exit(7)",
        ],
        (
            "/v1/sprites/sbx-one/exec?cmd=cat&cmd=two+words%26more&stdin=true&tty=false&max_run_after_disconnect=5s",
            "Bearer test-token",
        ),
    )
    "#);
}

#[tokio::test]
async fn test_exit_event_and_chunked_stdin() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        // Stdin is sent in messages of at most 32 KiB.
        let mut sizes = Vec::new();
        loop {
            let tungstenite::Message::Binary(data) = recv(&mut socket).await else {
                panic!("expected a binary message");
            };
            if data[..] == [4] {
                break;
            }
            assert_eq!(data[0], 0);
            sizes.push(data.len() - 1);
        }
        assert_eq!(sizes, vec![32 * 1024, 32 * 1024, 100]);

        send(&mut socket, text(r#"{"type":"exit","exit_code":3}"#)).await;
        expect_close(socket, PROMPTLY).await;
    })
    .await;

    let Exec {
        mut stdin,
        mut output,
    } = sprites
        .exec("sbx-one", &serde_json::json!({}), &["true"])
        .await
        .unwrap();
    stdin
        .send(vec![b'x'; 64 * 1024 + 100].into())
        .await
        .unwrap();
    drop(stdin);

    assert_eq!(collect(&mut output).await, vec!["Exit(3)"]);
    server.await.unwrap();
}

#[tokio::test]
async fn test_output_continues_after_stdin_is_dropped() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        assert_eq!(recv(&mut socket).await, binary(&[4]));
        send(&mut socket, binary(b"\x01after eof")).await;
        send(&mut socket, binary(&[3, 0])).await;
        expect_close(socket, PROMPTLY).await;
    })
    .await;

    let Exec { stdin, mut output } = sprites
        .exec("sbx-one", &serde_json::json!({}), &["cat"])
        .await
        .unwrap();
    drop(stdin);

    assert_eq!(
        collect(&mut output).await,
        vec!["Stdout(b\"after eof\")", "Exit(0)"]
    );
    server.await.unwrap();
}

#[tokio::test]
async fn test_drop_exec_sends_eof_and_closes() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        assert_eq!(recv(&mut socket).await, binary(b"\x00partial"));
        assert_eq!(recv(&mut socket).await, binary(&[4]));
        expect_close(socket, PROMPTLY).await;
    })
    .await;

    let mut exec = sprites
        .exec("sbx-one", &serde_json::json!({}), &["sleep", "600"])
        .await
        .unwrap();
    exec.stdin
        .send(bytes::Bytes::from_static(b"partial"))
        .await
        .unwrap();
    drop(exec);

    server.await.unwrap();
}

#[tokio::test]
async fn test_cancelled_task_sends_eof_and_closes() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        assert_eq!(recv(&mut socket).await, binary(b"\x00partial"));
        assert_eq!(recv(&mut socket).await, binary(&[4]));
        expect_close(socket, PROMPTLY).await;
    })
    .await;

    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut exec = sprites
            .exec("sbx-one", &serde_json::json!({}), &["sleep", "600"])
            .await
            .unwrap();
        exec.stdin
            .send(bytes::Bytes::from_static(b"partial"))
            .await
            .unwrap();
        started_tx.send(()).unwrap();
        // Holds `exec` until aborted.
        let _ = exec.output.next().await;
        unreachable!("the command never produces output");
    });
    started_rx.await.unwrap();
    task.abort();

    server.await.unwrap();
}

#[tokio::test]
async fn test_drop_output_then_stdin() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        // More output than the connection buffers, which the Exec must
        // read and discard to avoid blocking this write.
        let chunk = [&[1u8][..], &[b'o'; 64 * 1024]].concat();
        for _ in 0..128 {
            tokio::time::timeout(STEP, socket.send(binary(&chunk)))
                .await
                .expect("output wasn't drained")
                .unwrap();
        }
        // Stdin is still relayed.
        assert_eq!(recv(&mut socket).await, binary(b"\x00still here"));
        assert_eq!(recv(&mut socket).await, binary(&[4]));
        expect_close(socket, PROMPTLY).await;
    })
    .await;

    let Exec { mut stdin, output } = sprites
        .exec("sbx-one", &serde_json::json!({}), &["cat"])
        .await
        .unwrap();
    drop(output);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    stdin
        .send(bytes::Bytes::from_static(b"still here"))
        .await
        .unwrap();
    drop(stdin);

    server.await.unwrap();
}

#[tokio::test]
async fn test_drop_exec_with_stalled_stdin() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        // Not reading stalls the Exec's stdin writes, and then it's dropped.
        // It gives up on writing within a deadline and closes the connection.
        tokio::time::sleep(std::time::Duration::from_secs(6)).await;

        let started = std::time::Instant::now();
        let mut data_messages = 0;
        loop {
            match socket.next().await {
                Some(Ok(tungstenite::Message::Binary(data))) if data[0] == 0 => data_messages += 1,
                Some(Ok(message)) => panic!("unexpected {message:?}"),
                Some(Err(_)) | None => break,
            }
        }
        assert!(data_messages > 0);
        assert!(started.elapsed() < PROMPTLY, "{:?}", started.elapsed());
    })
    .await;

    let task = tokio::spawn(async move {
        let mut exec = sprites
            .exec("sbx-one", &serde_json::json!({}), &["sleep", "600"])
            .await
            .unwrap();
        loop {
            exec.stdin.send(vec![0; 1 << 20].into()).await.unwrap();
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    task.abort();

    server.await.unwrap();
}

#[tokio::test]
async fn test_exit_closes_while_stdin_is_held() {
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        send(&mut socket, binary(&[3, 0])).await;
        expect_close(socket, PROMPTLY).await;
    })
    .await;

    let Exec {
        mut stdin,
        mut output,
    } = sprites
        .exec("sbx-one", &serde_json::json!({}), &["true"])
        .await
        .unwrap();

    assert_eq!(collect(&mut output).await, vec!["Exit(0)"]);
    server.await.unwrap();

    let err = stdin
        .send(bytes::Bytes::from_static(b"too late"))
        .await
        .unwrap_err();
    insta::assert_snapshot!(format!("{err:#}"), @"sprite exec connection is closed");
}

#[tokio::test]
async fn test_exec_errors() {
    // Sprites reports an error event.
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        send(&mut socket, binary(b"\x01partial")).await;
        send(&mut socket, text(r#"{"type":"error","error":"boom"}"#)).await;
        expect_close(socket, PROMPTLY).await;
    })
    .await;
    let mut exec = sprites
        .exec("sbx-one", &serde_json::json!({}), &["true"])
        .await
        .unwrap();
    let errored = collect(&mut exec.output).await;
    server.await.unwrap();

    // Sprites closes the connection without an exit status.
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        send(&mut socket, binary(b"\x02partial")).await;
        socket.close(None).await.unwrap();
    })
    .await;
    let mut exec = sprites
        .exec("sbx-one", &serde_json::json!({}), &["true"])
        .await
        .unwrap();
    let closed = collect(&mut exec.output).await;
    server.await.unwrap();

    // An unknown frame tag.
    let (sprites, server) = fake_exec(None, |mut socket| async move {
        send(&mut socket, binary(b"\x09what")).await;
        expect_close(socket, PROMPTLY).await;
    })
    .await;
    let mut exec = sprites
        .exec("sbx-one", &serde_json::json!({}), &["true"])
        .await
        .unwrap();
    let unknown = collect(&mut exec.output).await;
    server.await.unwrap();

    insta::assert_debug_snapshot!((errored, closed, unknown), @r#"
    (
        [
            "Stdout(b\"partial\")",
            "error: Sprites command failed: boom",
        ],
        [
            "Stderr(b\"partial\")",
            "error: sprite exec connection closed without an exit status",
        ],
        [
            "error: sprite sent an command frame with unknown stream tag 9",
        ],
    )
    "#);
}

#[tokio::test]
async fn test_exec_rejected() {
    let (sprites, server) = fake_exec(Some((404, "sprite not found")), |_| async {}).await;
    let result = sprites
        .exec("sbx-missing", &serde_json::json!({}), &["true"])
        .await;
    assert!(matches!(result, Err(Error::Gone)), "{:?}", result.err());
    server.await.unwrap();

    let (sprites, server) = fake_exec(Some((500, "boom")), |_| async {}).await;
    let Err(Error::Other(err)) = sprites
        .exec("sbx-one", &serde_json::json!({}), &["true"])
        .await
    else {
        panic!("expected an error other than Gone");
    };
    insta::assert_snapshot!(format!("{err:#}"), @"failed to start command: HTTP 500 Internal Server Error: boom");
    server.await.unwrap();
}

/// A fake of the Sprites API's sprite listing and deletion. Listings are
/// served in pages of the names in `pages`, and their queries are recorded.
async fn fake_api(
    pages: Vec<Vec<&'static str>>,
) -> (
    Sprites,
    std::sync::Arc<std::sync::Mutex<Vec<std::collections::BTreeMap<String, String>>>>,
) {
    type Query = std::collections::BTreeMap<String, String>;
    let queries = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Query>::new()));

    let list = {
        let queries = queries.clone();
        move |axum::extract::Query(query): axum::extract::Query<Query>| {
            let (pages, queries) = (pages.clone(), queries.clone());
            async move {
                let index: usize = match query.get("continuation_token") {
                    None => 0,
                    Some(token) => token.strip_prefix("page-").unwrap().parse().unwrap(),
                };
                queries.lock().unwrap().push(query);

                let has_more = index + 1 < pages.len();
                let mut body = serde_json::json!({
                    "sprites": pages[index]
                        .iter()
                        .map(|name| serde_json::json!({"id": format!("id-{name}"), "name": name, "status": "cold"}))
                        .collect::<Vec<_>>(),
                    "has_more": has_more,
                    "running": 0,
                    "warm": 0,
                    "cold": pages[index].len(),
                    "name": "acmeCo",
                });
                if has_more {
                    body["next_continuation_token"] = format!("page-{}", index + 1).into();
                }
                axum::Json(body)
            }
        }
    };
    let delete = |axum::extract::Path(name): axum::extract::Path<String>| async move {
        match name.as_str() {
            "sbx-present" => axum::http::StatusCode::NO_CONTENT.into_response(),
            "sbx-broken" => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": "broken"})),
            )
                .into_response(),
            _ => (
                axum::http::StatusCode::NOT_FOUND,
                axum::Json(serde_json::json!({"error": "sprite not found"})),
            )
                .into_response(),
        }
    };
    let router = axum::Router::new()
        .route("/v1/sprites", axum::routing::get(list))
        .route("/v1/sprites/{name}", axum::routing::delete(delete));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    (provider(base_url.parse().unwrap(), "sbx-"), queries)
}

#[tokio::test]
async fn test_list_pages() {
    let (sprites, queries) = fake_api(vec![vec!["sbx-a", "sbx-b"], vec![], vec!["sbx-c"]]).await;

    let names = sprites.list().await.unwrap();
    let queries = queries.lock().unwrap().clone();

    insta::assert_debug_snapshot!((names, queries), @r#"
    (
        [
            "sbx-a",
            "sbx-b",
            "sbx-c",
        ],
        [
            {
                "max_results": "500",
                "prefix": "sbx-",
            },
            {
                "continuation_token": "page-1",
                "max_results": "500",
                "prefix": "sbx-",
            },
            {
                "continuation_token": "page-2",
                "max_results": "500",
                "prefix": "sbx-",
            },
        ],
    )
    "#);
}

#[tokio::test]
async fn test_list_checks_prefix() {
    let (sprites, _queries) = fake_api(vec![vec!["sbx-a", "other"]]).await;
    let Err(Error::Other(err)) = sprites.list().await else {
        panic!("expected an error");
    };
    insta::assert_snapshot!(format!("{err:#}"), @r#"sprite list for prefix "sbx-" included "other""#);
}

#[tokio::test]
async fn test_destroy() {
    let (sprites, _queries) = fake_api(Vec::new()).await;
    let state = serde_json::json!({});

    assert!(sprites.destroy("sbx-present", &state).await.unwrap());
    assert!(!sprites.destroy("sbx-absent", &state).await.unwrap());
    let Err(Error::Other(err)) = sprites.destroy("sbx-broken", &state).await else {
        panic!("expected an error");
    };
    insta::assert_snapshot!(format!("{err:#}"), @"failed to delete sprite: broken (HTTP 500 Internal Server Error)");
}

// Live tests against the Sprites API.

/// Where live tests get the flowctl which sprites install.
enum Flowctl {
    /// `SANDBOX_FLOWCTL_URL`, from which `provision` downloads it.
    Url,
    /// `SANDBOX_FLOWCTL_PATH`, a local build which is uploaded into each sprite.
    Path(std::path::PathBuf),
}

/// Where an uploaded flowctl is put, for `Sprites::setup` to install.
const UPLOAD_PATH: &str = "/tmp/flowctl-upload";
const UPLOAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// Deletes the sprites of `prefix` when dropped, including on a panic.
struct Cleanup {
    token: String,
    prefix: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        // Drop can't await, and may run on the test's runtime. The client is
        // new because pooled connections belong to the runtime that opened them.
        let (client, prefix) = (Client::new(self.token.clone()), self.prefix.clone());
        let result = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    for name in client.list_sprites(&prefix).await? {
                        client.delete_sprite(&name).await?;
                    }
                    anyhow::Ok(())
                })
        })
        .join()
        .unwrap();
        // Not a panic: this may run while unwinding from one.
        if let Err(err) = result {
            eprintln!("failed to delete test sprites of {}: {err:#}", self.prefix);
        }
    }
}

/// A provider whose prefix is unique to this test, its cleanup guard,
/// and the source of its flowctl.
fn live_setup() -> (Sprites, Cleanup, Flowctl) {
    let token = std::env::var("SPRITES_TOKEN").expect("SPRITES_TOKEN is required");

    let (flowctl_url, flowctl) = match (
        std::env::var("SANDBOX_FLOWCTL_URL").ok(),
        std::env::var_os("SANDBOX_FLOWCTL_PATH"),
    ) {
        (Some(url), None) => (url, Flowctl::Url),
        (None, Some(path)) => (format!("file://{UPLOAD_PATH}"), Flowctl::Path(path.into())),
        _ => panic!("set exactly one of SANDBOX_FLOWCTL_URL and SANDBOX_FLOWCTL_PATH"),
    };

    let id = uuid::Uuid::new_v4().simple().to_string();
    let prefix = format!("sbx-test-{}-", &id[..12]);
    let client = std::sync::Arc::new(Client::new(token.clone()));
    // The exec WebSocket's TLS needs a process-wide provider, which the agent
    // binary installs at startup. An error means another test installed it.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let sprites = Sprites {
        client: client.clone(),
        flowctl_url: flowctl_url.parse().expect("flowctl URL is a URL"),
        prefix: prefix.clone(),
    };
    (sprites, Cleanup { token, prefix }, flowctl)
}

/// Provisions sprite `handle`. With a local flowctl, it's created through
/// the client, the flowctl is uploaded, and then `Sprites::setup` installs it.
async fn live_provision(sprites: &Sprites, flowctl: &Flowctl, handle: &str) -> serde_json::Value {
    let Flowctl::Path(path) = flowctl else {
        let started = std::time::Instant::now();
        let state = sprites.provision(handle).await.unwrap();
        eprintln!("provisioned {handle} in {:?}", started.elapsed());
        return state;
    };

    let started = std::time::Instant::now();
    sprites.client.create_sprite(handle).await.unwrap();
    eprintln!("created {handle} in {:?}", started.elapsed());

    let started = std::time::Instant::now();
    tokio::time::timeout(UPLOAD_TIMEOUT, upload(sprites, handle, path))
        .await
        .expect("timed out uploading flowctl");
    eprintln!("uploaded {} in {:?}", path.display(), started.elapsed());

    let started = std::time::Instant::now();
    sprites.setup(handle).await.unwrap();
    eprintln!("set up {handle} in {:?}", started.elapsed());

    serde_json::json!({})
}

/// Uploads the file at `path` to [`UPLOAD_PATH`] through exec stdin,
/// and verifies its SHA-256 with a second exec.
async fn upload(sprites: &Sprites, handle: &str, path: &std::path::Path) {
    let contents = bytes::Bytes::from(tokio::fs::read(path).await.unwrap());
    let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&contents));
    let state = serde_json::json!({});

    let script = format!("cat > {UPLOAD_PATH}");
    let Exec { stdin, output } = sprites
        .exec(handle, &state, &["bash", "-c", &script])
        .await
        .unwrap();

    // Chunks are slices of `contents`, which aren't copied until the
    // provider frames them. Output is read concurrently, though `cat`
    // writes only errors.
    let send = async move {
        let mut stdin = stdin;
        let chunks = (0..contents.len())
            .step_by(1 << 20)
            .map(|at| Ok(contents.slice(at..contents.len().min(at + (1 << 20)))));
        stdin
            .send_all(&mut futures::stream::iter(chunks))
            .await
            .unwrap();
        // Dropping stdin sends EOF, which ends `cat`.
    };
    let ((), (stderr, code)) = tokio::join!(send, run_output(output));
    assert_eq!(code, 0, "upload failed: {stderr}");

    let (stdout, stderr, code) = live_run(sprites, handle, &["sha256sum", UPLOAD_PATH], "").await;
    assert_eq!(code, 0, "sha256sum failed: {stderr}");
    assert_eq!(stdout, format!("{digest}  {UPLOAD_PATH}\n"));
}

/// Reads `output` to its end, returning its stderr and exit code.
async fn run_output(
    mut output: futures::stream::BoxStream<'static, Result<Output, Error>>,
) -> (String, i32) {
    let (mut stderr, mut code) = (String::new(), None);
    while let Some(item) = output.next().await {
        match item.unwrap() {
            Output::Stdout(_) => {}
            Output::Stderr(data) => stderr.push_str(&String::from_utf8_lossy(&data)),
            Output::Exit(c) => code = Some(c),
        }
    }
    (stderr, code.expect("Exit is the last item"))
}

/// Runs `argv` directly (not through the wrapper) with `input` as its stdin,
/// returning its stdout, stderr, and exit code.
async fn live_run(
    sprites: &Sprites,
    handle: &str,
    argv: &[&str],
    input: &str,
) -> (String, String, i32) {
    let Exec {
        mut stdin,
        mut output,
    } = sprites
        .exec(handle, &serde_json::json!({}), argv)
        .await
        .unwrap();
    if !input.is_empty() {
        stdin.send(input.to_string().into()).await.unwrap();
    }
    drop(stdin);

    let (mut stdout, mut stderr, mut code) = (String::new(), String::new(), None);
    while let Some(item) = output.next().await {
        match item.unwrap() {
            Output::Stdout(data) => stdout.push_str(&String::from_utf8_lossy(&data)),
            Output::Stderr(data) => stderr.push_str(&String::from_utf8_lossy(&data)),
            Output::Exit(c) => code = Some(c),
        }
    }
    (stdout, stderr, code.expect("Exit is the last item"))
}

#[tokio::test]
#[ignore = "requires SPRITES_TOKEN and a flowctl, and creates sprites"]
async fn live_lifecycle() {
    let (sprites, _cleanup, flowctl) = live_setup();
    let handle = format!("{}a", sprites.prefix);
    let state = serde_json::json!({});

    assert_eq!(sprites.kind(), "sprites");
    assert_eq!(live_provision(&sprites, &flowctl, &handle).await, state);
    assert_eq!(sprites.list().await.unwrap(), vec![handle.clone()]);

    // The baseline, as a login shell of the default user sees it.
    let (baseline, stderr, code) = live_run(
        &sprites,
        &handle,
        &[
            "bash",
            "-lc",
            r#"echo "flowctl: $(command -v flowctl)"
               echo "work: $(stat -c %U /work) $(id -un)"
               echo "res: $RES_OPTIONS"
               flowctl raw sandbox-exec --help >/dev/null && echo "sandbox-exec: ok""#,
        ],
        "",
    )
    .await;
    eprintln!("baseline stderr: {stderr}");
    assert_eq!(code, 0);
    insta::assert_snapshot!(baseline, @r"
    flowctl: /opt/estuary/bin/flowctl
    work: sprite sprite
    res: edns0
    sandbox-exec: ok
    ");

    let (stdout, _, code) = live_run(&sprites, &handle, &["cat"], "relayed\nstdin\n").await;
    assert_eq!((stdout.as_str(), code), ("relayed\nstdin\n", 0));
    assert_eq!(live_run(&sprites, &handle, &["false"], "").await.2, 1);

    assert!(sprites.destroy(&handle, &state).await.unwrap());
    assert!(!sprites.destroy(&handle, &state).await.unwrap());
    assert!(matches!(
        sprites.exec(&handle, &state, &["true"]).await,
        Err(Error::Gone)
    ));
    assert!(sprites.list().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires SPRITES_TOKEN and a flowctl, and creates sprites"]
async fn live_drop_means_eof() {
    let (sprites, _cleanup, flowctl) = live_setup();
    let handle = format!("{}a", sprites.prefix);
    let state = live_provision(&sprites, &flowctl, &handle).await;

    // Logs when its stdin closes, and then lingers, so that only stdin EOF
    // (not the disconnect timeout's SIGTERM) can produce the log in time.
    let script = r#"echo started; cat >/dev/null; echo "$1" >> /tmp/eof.log; sleep 600"#;

    for (case, drop_whole_exec) in [("stdin", false), ("exec", true)] {
        let Exec { stdin, mut output } = sprites
            .exec(&handle, &state, &["bash", "-c", script, "script", case])
            .await
            .unwrap();
        assert_eq!(
            output.next().await.unwrap().unwrap(),
            Output::Stdout(bytes::Bytes::from_static(b"started\n"))
        );

        let started = std::time::Instant::now();
        drop(stdin);
        if drop_whole_exec {
            drop(output);
        }

        loop {
            let (log, _, code) = live_run(
                &sprites,
                &handle,
                &["bash", "-c", "cat /tmp/eof.log 2>/dev/null; true"],
                "",
            )
            .await;
            assert_eq!(code, 0);
            if log.lines().any(|line| line == case) {
                break;
            }
            assert!(
                started.elapsed() < std::time::Duration::from_secs(4),
                "{case}: no EOF logged after {:?}",
                started.elapsed()
            );
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        eprintln!("{case}: EOF logged {:?} after dropping", started.elapsed());
    }

    assert!(sprites.destroy(&handle, &state).await.unwrap());
}

// Live tests of commands run through the wrapper, `flowctl raw sandbox-exec`.

fn encode(frames: &[sandbox_exec::Input]) -> bytes::Bytes {
    let mut buf = bytes::BytesMut::new();
    for frame in frames {
        frame.encode(&mut buf).unwrap();
    }
    buf.freeze()
}

fn header(workspace: &str, command: &str) -> sandbox_exec::Input {
    sandbox_exec::Input::Header(sandbox_exec::Header {
        version: sandbox_exec::PROTOCOL_VERSION,
        workspace: workspace.to_string(),
        command: command.to_string(),
        env: [("HEADER_VAR".to_string(), "from-header".to_string())].into(),
    })
}

/// What a command produced, from the frames of the wrapper and the
/// output of the exec which ran it.
#[derive(Debug, Default)]
struct Outcome {
    stdout: String,
    stderr: String,
    /// The wrapper's last frame: `Exit` or `Error`.
    last: Option<sandbox_exec::Output>,
    /// Stderr of the wrapper process itself.
    wrapper_stderr: String,
    wrapper_exit: Option<i32>,
}

/// Reads `output` to its end, decoding the wrapper's frames from its stdout.
async fn collect_wrapper(
    output: &mut futures::stream::BoxStream<'static, Result<Output, Error>>,
) -> Outcome {
    let mut outcome = Outcome::default();
    let mut buf = bytes::BytesMut::new();

    while let Some(item) = output.next().await {
        match item.expect("output stream item") {
            Output::Stdout(data) => buf.extend_from_slice(&data),
            Output::Stderr(data) => outcome
                .wrapper_stderr
                .push_str(&String::from_utf8_lossy(&data)),
            Output::Exit(code) => outcome.wrapper_exit = Some(code),
        }
        while let Some(frame) = sandbox_exec::Output::decode(&mut buf).unwrap() {
            match frame {
                sandbox_exec::Output::Stdout(data) => {
                    outcome.stdout.push_str(&String::from_utf8_lossy(&data))
                }
                sandbox_exec::Output::Stderr(data) => {
                    outcome.stderr.push_str(&String::from_utf8_lossy(&data))
                }
                last => outcome.last = Some(last),
            }
        }
    }
    assert!(buf.is_empty(), "trailing partial frame");
    outcome
}

/// Runs `command` through the wrapper in `workspace`, with `input` and then
/// StdinEof. Stdin is held until the wrapper exits, as a hang-up cancels it.
async fn run_wrapped(
    sprites: &Sprites,
    handle: &str,
    workspace: &str,
    command: &str,
    input: &[&[u8]],
) -> Outcome {
    let Exec {
        mut stdin,
        mut output,
    } = sprites
        .exec(handle, &serde_json::json!({}), &WRAPPER_ARGV)
        .await
        .unwrap();

    let mut frames = vec![header(workspace, command)];
    frames.extend(
        input
            .iter()
            .map(|data| sandbox_exec::Input::Stdin(bytes::Bytes::copy_from_slice(data))),
    );
    frames.push(sandbox_exec::Input::StdinEof);
    stdin.send(encode(&frames)).await.unwrap();

    let started = std::time::Instant::now();
    let outcome = collect_wrapper(&mut output).await;
    drop(stdin);
    eprintln!(
        "wrapped command in {workspace} ran {:?}; wrapper stderr: {:?}",
        started.elapsed(),
        outcome.wrapper_stderr
    );
    outcome
}

#[tokio::test]
#[ignore = "requires SPRITES_TOKEN and a flowctl, and creates sprites"]
async fn live_wrapper_end_to_end() {
    let (sprites, _cleanup, flowctl) = live_setup();
    let handle = format!("{}a", sprites.prefix);
    let state = live_provision(&sprites, &flowctl, &handle).await;

    let command = r#"
        read line
        echo "stdin: $line"
        echo "rest: $(cat)"
        echo "env: $HEADER_VAR"
        echo "home: $HOME"
        echo "cwd: $(pwd)"
        test -d /work/ws-one && echo "workspace: exists"
        echo "user: $(id -un)"
        echo "flowctl: $(command -v flowctl)"
        echo "res: $RES_OPTIONS"
        sudo -n true && echo "sudo: ok"
        echo "to stderr" >&2
        exit 3
    "#;
    let outcome = run_wrapped(
        &sprites,
        &handle,
        "ws-one",
        command,
        &[b"first line\nsecond ", b"and third"],
    )
    .await;

    // The command's stderr may also hold the login shell's own warnings.
    eprintln!("command stderr: {:?}", outcome.stderr);
    assert!(outcome.stderr.ends_with("to stderr\n"), "{outcome:?}");

    insta::assert_debug_snapshot!((&outcome.stdout, &outcome.last, outcome.wrapper_exit), @r#"
    (
        "stdin: first line\nrest: second and third\nenv: from-header\nhome: /work/ws-one\ncwd: /work/ws-one\nworkspace: exists\nuser: sprite\nflowctl: /opt/estuary/bin/flowctl\nres: edns0\nsudo: ok\n",
        Some(
            Exit {
                code: Some(
                    3,
                ),
                signal: None,
            },
        ),
        Some(
            0,
        ),
    )
    "#);

    // Report which preinstalled tools resolve under the workspace's HOME,
    // and under the default user's own HOME, without asserting a set.
    let tools =
        r#"for tool in node python3; do echo "$tool: $(command -v "$tool" || echo missing)"; done"#;
    let wrapped = run_wrapped(&sprites, &handle, "ws-tools", tools, &[]).await;
    let (direct, direct_stderr, _) = live_run(&sprites, &handle, &["bash", "-lc", tools], "").await;
    eprintln!("tools under the workspace HOME:\n{}", wrapped.stdout);
    eprintln!("tools under /home/sprite:\n{direct}(stderr: {direct_stderr:?})");
    assert!(
        matches!(
            wrapped.last,
            Some(sandbox_exec::Output::Exit { code: Some(0), .. })
        ),
        "{wrapped:?}"
    );

    assert!(sprites.destroy(&handle, &state).await.unwrap());
}

/// Starts a long-running command through the wrapper, and returns its Exec
/// and the PID of the command.
async fn start_sleeper(sprites: &Sprites, handle: &str) -> (Exec, String) {
    let Exec {
        mut stdin,
        mut output,
    } = sprites
        .exec(handle, &serde_json::json!({}), &WRAPPER_ARGV)
        .await
        .unwrap();
    stdin
        .send(encode(&[header(
            "hang",
            "echo $$ > pid; echo started; exec sleep 600",
        )]))
        .await
        .unwrap();

    let mut buf = bytes::BytesMut::new();
    loop {
        match output.next().await.unwrap().unwrap() {
            Output::Stdout(data) => buf.extend_from_slice(&data),
            other => panic!("unexpected {other:?}"),
        }
        if let Some(frame) = sandbox_exec::Output::decode(&mut buf).unwrap() {
            assert_eq!(
                frame,
                sandbox_exec::Output::Stdout(bytes::Bytes::from_static(b"started\n"))
            );
            break;
        }
    }

    let (pid, stderr, code) = live_run(sprites, handle, &["cat", "/work/hang/pid"], "").await;
    assert_eq!(code, 0, "{stderr}");
    (Exec { stdin, output }, pid.trim().to_string())
}

/// Count running wrapper processes and processes having `pid`.
const PROCESSES: &str = r#"
    wrappers=0
    for cmdline in /proc/[0-9]*/cmdline; do
        case "$(tr '\0' ' ' < "$cmdline" 2>/dev/null)" in
            "/opt/estuary/bin/flowctl raw sandbox-exec "*) wrappers=$((wrappers + 1)) ;;
        esac
    done
    if [ -e "/proc/$1" ]; then command=1; else command=0; fi
    echo "$wrappers $command"
"#;

/// Waits until neither a wrapper nor the command at `pid` is running,
/// returning how long that took.
async fn wait_for_exit(
    sprites: &Sprites,
    handle: &str,
    pid: &str,
    within: std::time::Duration,
) -> std::time::Duration {
    let started = std::time::Instant::now();
    loop {
        let (counts, stderr, code) = live_run(
            sprites,
            handle,
            &["bash", "-c", PROCESSES, "processes", pid],
            "",
        )
        .await;
        assert_eq!(code, 0, "{stderr}");

        if counts.trim() == "0 0" {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < within,
            "processes (wrappers, command) still running after {:?}: {counts}",
            started.elapsed()
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

#[tokio::test]
#[ignore = "requires SPRITES_TOKEN and a flowctl, and creates sprites"]
async fn live_wrapper_hang_up() {
    let (sprites, _cleanup, flowctl) = live_setup();
    let handle = format!("{}a", sprites.prefix);
    let state = live_provision(&sprites, &flowctl, &handle).await;

    // Only the hang-up (stdin EOF) can stop the command in time. Sprites' own
    // SIGTERM would come only after MAX_RUN_AFTER_DISCONNECT (5s).
    let (exec, pid) = start_sleeper(&sprites, &handle).await;
    drop(exec);
    let elapsed = wait_for_exit(&sprites, &handle, &pid, std::time::Duration::from_secs(4)).await;
    eprintln!("command and wrapper exited {elapsed:?} after dropping Exec");

    assert!(sprites.destroy(&handle, &state).await.unwrap());
}
