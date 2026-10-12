mod support;

use disk_daemon::proto;

/// A well-formed request which the attachment's state does not allow.
const OUT_OF_TURN: tonic::Code = tonic::Code::FailedPrecondition;
/// A request which is wrong in itself.
const MALFORMED: tonic::Code = tonic::Code::InvalidArgument;

#[tokio::test]
async fn disk_protocol() {
    let fixture = support::Fixture::start().await;
    let daemon = support::Daemon::start(&fixture, "protocol").await;

    use State::*;
    use Violation::*;

    // Rows are (journal, state, request, code). The journal name carries the row, so a
    // panic in the driver names it.
    for (journal, state, violation, code) in [
        ("before-open", Fresh, Prepare, OUT_OF_TURN),
        ("no-request", Fresh, NoRequest, MALFORMED),
        ("no-journal", Fresh, OpenWithoutJournal, MALFORMED),
        ("misaligned", Fresh, OpenMisaligned, MALFORMED),
        ("twice-opened", Promoted, Open, OUT_OF_TURN),
        ("twice-promoted", Promoted, Promote, OUT_OF_TURN),
        ("standby-prepares", Standby, Prepare, OUT_OF_TURN),
        ("twice-prepared", Prepared, Prepare, OUT_OF_TURN),
        ("early-commit", Promoted, Acknowledge, OUT_OF_TURN),
        ("wrong-commit", Prepared, Acknowledge, OUT_OF_TURN),
    ] {
        let journal = format!("acmeCo/disk/{journal}");

        () = a_refusal_ends_the_attachment(&fixture, &daemon, &journal, state, violation, code)
            .await;
    }

    a_commit_and_the_cut_behind_it_are_pipelined(&fixture, &daemon).await;
    a_cut_pipelined_behind_a_promote_is_served_after_it(&fixture, &daemon).await;

    () = fixture.assert_no_leaks();

    daemon.drain().await;
    fixture.stop().await;
}

/// What the attachment has done before the request which ends it.
enum State {
    Fresh,
    Standby,
    Promoted,
    Prepared,
}

/// The request which ends the attachment.
enum Violation {
    NoRequest,
    OpenWithoutJournal,
    OpenMisaligned,
    Open,
    Promote,
    Prepare,
    /// Bytes which are not the prepared acknowledgement, whether or not one exists.
    Acknowledge,
}

/// Every refused request ends its attachment, and the disk that attachment held goes
/// with it.
async fn a_refusal_ends_the_attachment(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
    journal: &str,
    state: State,
    violation: Violation,
    code: tonic::Code,
) {
    let mut attachment = daemon.attach().await;

    let prepared = match state {
        State::Fresh => None,
        State::Standby => {
            () = attachment
                .stand_by(fixture.open(journal).await)
                .await
                .unwrap();
            None
        }
        State::Promoted => {
            _ = attachment.serve(fixture.open(journal).await).await.unwrap();
            None
        }
        State::Prepared => {
            let mount = attachment.serve(fixture.open(journal).await).await.unwrap();

            () = support::Tree::generation(1).write(&mount.join("data"));
            let ack = attachment.prepare().await.unwrap();
            assert!(!ack.is_empty());

            Some(ack)
        }
    };

    let request = match violation {
        Violation::NoRequest => None,
        Violation::OpenWithoutJournal => Some(proto::request::Request::Open(proto::Open {
            journal: String::new(),
            device_size: support::DEVICE_SIZE,
        })),
        Violation::OpenMisaligned => Some(proto::request::Request::Open(proto::Open {
            device_size: support::DEVICE_SIZE + 1,
            ..fixture.open_absent(journal)
        })),
        Violation::Open => Some(proto::request::Request::Open(fixture.open(journal).await)),
        Violation::Promote => Some(proto::request::Request::Promote(proto::Promote {
            ack: Default::default(),
        })),
        Violation::Prepare => Some(proto::request::Request::Prepare(proto::Prepare {})),
        Violation::Acknowledge => {
            let ack = match &prepared {
                Some(ack) => {
                    let mut ack = ack.to_vec();
                    *ack.last_mut().unwrap() ^= 0xff;
                    ack.into()
                }
                None => bytes::Bytes::from_static(b"not an acknowledgement"),
            };
            Some(proto::request::Request::Acknowledge(proto::Acknowledge {
                ack,
            }))
        }
    };

    () = attachment.send_message(proto::Request { request }).await;
    let status = attachment.reply().await.unwrap_err();

    assert_eq!(status.code(), code, "{journal}: {status}");
    () = attachment.ended().await;
}

/// A client may pipeline its `Acknowledge` and the next `Prepare` without reading a
/// reply between them. The daemon serves them in order and answers in order, and the
/// write between the two cuts is a delta of its own.
async fn a_commit_and_the_cut_behind_it_are_pipelined(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/back-to-back";
    let mut attachment = daemon.attach().await;

    let mount = attachment.serve(fixture.open(journal).await).await.unwrap();
    let first = support::Tree::generation(1);

    () = first.write(&mount.join("data"));

    let ack = attachment.prepare().await.unwrap();
    assert!(!ack.is_empty());

    let tree = first.with("between", b"written across a commit".to_vec());
    () = std::fs::write(mount.join("data/between"), tree.content("between")).unwrap();

    () = attachment.send_acknowledge(ack).await;
    () = attachment.send_prepare().await;

    // Replies come back in request order.
    () = attachment.acknowledged().await.unwrap();

    let ack = attachment.prepared().await.unwrap();
    assert!(!ack.is_empty(), "the write between the two cuts is a delta");

    () = attachment.acknowledge(ack).await.unwrap();
    () = attachment.close().await;

    let client = daemon.client().await;
    let (disk, mount) = client
        .open(fixture.open(journal).await, Default::default())
        .await
        .unwrap();

    () = tree.assert_matches(&mount.join("data"));
    () = disk.close().await.unwrap();
}

/// A prepare pipelined behind promotion must run after promotion completes.
async fn a_cut_pipelined_behind_a_promote_is_served_after_it(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/promote-then-cut";
    let mut attachment = daemon.attach().await;

    () = attachment
        .send(proto::request::Request::Open(fixture.open(journal).await))
        .await;
    () = attachment
        .send(proto::request::Request::Promote(proto::Promote {
            ack: Default::default(),
        }))
        .await;
    () = attachment.send_prepare().await;

    // Replies come back in request order.
    assert!(matches!(
        attachment.reply().await.unwrap(),
        proto::response::Response::Opened(proto::Opened {}),
    ));
    let mount = match attachment.reply().await.unwrap() {
        proto::response::Response::Promoted(promoted) => promoted.mount_path,
        response => panic!("expected Promoted, got {response:?}"),
    };
    assert!(!mount.is_empty());

    // Nothing wrote to the disk, and the daemon committed its own format and mount
    // before it answered `Promoted`.
    let ack = attachment.prepared().await.unwrap();
    assert!(
        ack.is_empty(),
        "the cut behind a promotion has nothing to commit"
    );

    () = attachment.close().await;
}
