/// Diagnostics and readiness may arrive in one buffered read.
#[tokio::test]
async fn stderr_around_readiness() {
    let cases: &[(&str, &str)] = &[
        (
            "libkrun diagnostics before and after readiness",
            "[2026-10-01T00:00:00Z WARN  devices::virtio::fs::server] unhandled request\n \
                 [2026-10-01T00:00:01Z ERROR vmm::worker] guest wrote too much\n\
                 {\"level\":\"info\",\"message\":\"a connector log\"}\n",
        ),
        (
            "raw lines on both sides of the marker, which they must not absorb",
            "WARN libkrun: first\nWARN libkrun: second\n WARN after readiness\nmore raw\n",
        ),
        (
            "an indented line after readiness is a log, not a second marker",
            " Traceback (most recent call last):\n  File \"main.py\", line 1\n",
        ),
        (
            "a panic, and no readiness",
            "thread 'main' panicked at crates/connector-vmm/src/resolver/mod.rs:10:5:\n\
                 resolver socket lost\n\
                 note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\n",
        ),
        (
            "the engine's own failure, and no readiness",
            "Error: crun: error stat'ing file `/dev/kvm`: No such file or directory: OCI runtime attempted to invoke a command that was not found\n",
        ),
    ];

    let mut table = String::new();
    for (name, stderr) in cases {
        let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(64);
        let (log_sink, _read_through) = crate::LogSink::response(response_tx);
        let (ready_tx, ready_rx) = futures::channel::oneshot::channel();

        super::pump_stderr(
            tokio::io::BufReader::new(stderr.as_bytes()),
            ready_tx,
            log_sink,
            "ghcr.io/estuary/derive-python:stable".to_string(),
            |log| {
                crate::policy::sanitize_connector_log(
                    &bytes::Bytes::from_static(b"\"acmeCo/anvils/derivation\""),
                    log,
                )
            },
        )
        .await;

        table.push_str(&format!("# {name}\nready: {}\n", ready_rx.await.is_ok()));
        while let Ok(response) = response_rx.try_recv() {
            let Some(crate::proto::response::Kind::Log(log)) = response.unwrap().kind else {
                panic!("the pump sends only logs");
            };
            table.push_str(&format!(
                "{}: {:?}\n",
                log.level().as_str_name(),
                log.message
            ));
        }
        table.push('\n');
    }
    insta::assert_snapshot!(table, @r###"
# libkrun diagnostics before and after readiness
ready: true
warn: "[2026-10-01T00:00:00Z WARN  devices::virtio::fs::server] unhandled request"
error: "[2026-10-01T00:00:01Z ERROR vmm::worker] guest wrote too much"
info: "a connector log"
debug: "stopped connector container"

# raw lines on both sides of the marker, which they must not absorb
ready: true
warn: "WARN libkrun: first\nWARN libkrun: second"
warn: "WARN after readiness\nmore raw"
debug: "stopped connector container"

# an indented line after readiness is a log, not a second marker
ready: true
warn: "Traceback (most recent call last):\n  File \"main.py\", line 1"
debug: "stopped connector container"

# a panic, and no readiness
ready: false
error: "thread 'main' panicked at crates/connector-vmm/src/resolver/mod.rs:10:5:\nresolver socket lost\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace"

# the engine's own failure, and no readiness
ready: false
error: "Error: crun: error stat'ing file `/dev/kvm`: No such file or directory: OCI runtime attempted to invoke a command that was not found"
"###);
}

/// A marker must wake startup while stderr is still open. Exercise both a
/// coalesced raw line + marker and a marker delivered after that line.
#[tokio::test]
async fn readiness_does_not_wait_for_eof() {
    use tokio::io::AsyncWriteExt;
    for split in [false, true] {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let (tx, mut logs) = tokio::sync::mpsc::channel(64);
        let (sink, read_through) = crate::LogSink::response(tx);
        let (ready_tx, mut ready_rx) = futures::channel::oneshot::channel();
        if !split {
            writer
                .write_all(b"engine diagnostic\n \nraw after readiness\n")
                .await
                .unwrap();
        }
        let pump = tokio::spawn(super::pump_stderr(
            tokio::io::BufReader::new(reader),
            ready_tx,
            sink,
            "synthetic:local".into(),
            |log| log,
        ));
        if split {
            writer.write_all(b"engine diagnostic\n").await.unwrap();
            let response = tokio::time::timeout(std::time::Duration::from_secs(5), logs.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let Some(crate::proto::response::Kind::Log(log)) = response.kind else {
                panic!("log")
            };
            assert_eq!(log.message, "engine diagnostic");
            assert_eq!(ready_rx.try_recv().unwrap(), None);
            writer.write_all(b" ").await.unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut ready_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(!pump.is_finished(), "readiness must precede EOF");
        writer
            .write_all(b"{\"message\":\"after marker\",\"level\":\"info\"}\n")
            .await
            .unwrap();
        drop(writer);
        pump.await.unwrap();
        _ = read_through.await;
        let mut messages = Vec::new();
        while let Some(response) = logs.recv().await {
            let Some(crate::proto::response::Kind::Log(log)) = response.unwrap().kind else {
                panic!("log")
            };
            messages.push(log.message);
        }
        assert!(messages.iter().any(|message| message == "after marker"));
        assert_eq!(messages.last().unwrap(), "stopped connector container");
    }
}

#[tokio::test]
async fn eof_before_readiness_preserves_partial_diagnostic() {
    let (tx, mut logs) = tokio::sync::mpsc::channel(64);
    let (sink, read_through) = crate::LogSink::response(tx);
    let (ready_tx, ready_rx) = futures::channel::oneshot::channel();
    super::pump_stderr(
        tokio::io::BufReader::new(&b"engine failed without newline"[..]),
        ready_tx,
        sink,
        "synthetic:local".into(),
        |log| log,
    )
    .await;
    assert!(ready_rx.await.is_err());
    _ = read_through.await;
    let response = logs.recv().await.unwrap().unwrap();
    let Some(crate::proto::response::Kind::Log(log)) = response.kind else {
        panic!("log")
    };
    assert_eq!(log.message, "engine failed without newline");
    assert!(
        logs.recv().await.is_none(),
        "no stopped log before readiness"
    );
}
