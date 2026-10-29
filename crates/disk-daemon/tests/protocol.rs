mod support;

use disk_daemon::proto;
use proto::{request, response};

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

    // (journal, state, violation, code). Each journal names its row, so a panic
    // identifies the case.
    for (journal, state, violation, code) in [
        ("before-open", Fresh, Prepare, OUT_OF_TURN),
        ("no-request", Fresh, NoRequest, MALFORMED),
        ("twice-promoted", Promoted, Promote, OUT_OF_TURN),
        ("standby-prepares", Standby, Prepare, OUT_OF_TURN),
        (
            "unacknowledged-nothing",
            PreparedNothing,
            Prepare,
            OUT_OF_TURN,
        ),
        ("early-commit", Promoted, Acknowledge, OUT_OF_TURN),
    ] {
        let journal = format!("acmeCo/disk/{journal}");

        () = a_refusal_fails_the_attachment(&fixture, &daemon, &journal, state, violation, code)
            .await;
    }

    requests_are_served_and_answered_in_order(&fixture, &daemon).await;

    () = fixture.assert_no_leaks();

    daemon.drain().await;
    fixture.stop().await;
}

/// What the attachment does before the violation.
enum State {
    Fresh,
    Standby,
    Promoted,
    /// Prepared an empty delta.
    PreparedNothing,
}

/// The request which fails the attachment.
enum Violation {
    NoRequest,
    Promote,
    Prepare,
    /// With no prepared delta to commit.
    Acknowledge,
}

/// A refused request fails its attachment and tears down its disk.
async fn a_refusal_fails_the_attachment(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
    journal: &str,
    state: State,
    violation: Violation,
    code: tonic::Code,
) {
    let mut attachment = daemon.attach().await;

    let open = async || request::Request::Open(fixture.open(journal).await);
    let promote = || request::Request::Promote(proto::Promote::default());
    let prepare = || request::Request::Prepare(proto::Prepare {});

    let setup = match state {
        State::Fresh => vec![],
        State::Standby => vec![open().await],
        State::Promoted => vec![open().await, promote()],
        State::PreparedNothing => vec![open().await, promote(), prepare()],
    };
    let replies = attachment.exchange(setup).await.unwrap();

    // Only `PreparedNothing` ends in a `Prepared`, whose delta must be empty.
    if let [.., response::Response::Prepared(prepared)] = &replies[..] {
        assert!(prepared.ack_intent.is_empty(), "{journal}: {replies:?}");
    }

    let request = match violation {
        Violation::NoRequest => None,
        Violation::Promote => Some(promote()),
        Violation::Prepare => Some(prepare()),
        Violation::Acknowledge => Some(request::Request::Acknowledge(proto::Acknowledge {})),
    };

    () = attachment.send_message(proto::Request { request }).await;
    let status = attachment.reply().await.unwrap_err();

    assert_eq!(status.code(), code, "{journal}: {status}");
    () = attachment.closed().await;
}

/// A client may pipeline requests. The daemon serves and answers them in order: a
/// `Prepare` behind a `Promote` runs once promotion completes, and a `Prepare` behind
/// an `Acknowledge` captures the write between the two prepares.
async fn requests_are_served_and_answered_in_order(
    fixture: &support::Fixture,
    daemon: &support::Daemon,
) {
    let journal = "acmeCo/disk/pipelined";
    let mut attachment = daemon.attach().await;

    let replies = attachment
        .exchange(vec![
            request::Request::Open(fixture.open(journal).await),
            request::Request::Promote(proto::Promote::default()),
            request::Request::Prepare(proto::Prepare {}),
            request::Request::Acknowledge(proto::Acknowledge {}),
        ])
        .await
        .unwrap();

    let [
        response::Response::Opened(_),
        response::Response::Promoted(promoted),
        response::Response::Prepared(prepared),
        response::Response::Acknowledged(_),
    ] = &replies[..]
    else {
        panic!("expected Opened, Promoted, Prepared, and Acknowledged: {replies:?}");
    };
    // Empty: the setup delta committed the format and mount before `Promoted`.
    assert!(prepared.ack_intent.is_empty(), "{prepared:?}");

    let mount = std::path::PathBuf::from(&promoted.mount_path);
    let first = support::Tree::generation(1);
    () = first.write(&mount.join("data"));

    let replies = attachment
        .exchange(vec![request::Request::Prepare(proto::Prepare {})])
        .await
        .unwrap();
    assert!(
        matches!(&replies[..], [response::Response::Prepared(prepared)] if !prepared.ack_intent.is_empty()),
        "{replies:?}"
    );

    let tree = first.with("between", b"written across a commit".to_vec());
    () = std::fs::write(mount.join("data/between"), tree.content("between")).unwrap();

    let replies = attachment
        .exchange(vec![
            request::Request::Acknowledge(proto::Acknowledge {}),
            request::Request::Prepare(proto::Prepare {}),
            request::Request::Acknowledge(proto::Acknowledge {}),
        ])
        .await
        .unwrap();
    assert!(
        matches!(
            &replies[..],
            [
                response::Response::Acknowledged(_),
                response::Response::Prepared(prepared),
                response::Response::Acknowledged(_),
            ] if !prepared.ack_intent.is_empty()
        ),
        "the write between the two prepares is a delta: {replies:?}"
    );
    () = attachment.close().await;

    let client = daemon.client().await;
    let (disk, mount) = fixture
        .open_disk(&client, journal, Default::default())
        .await;

    () = tree.assert_matches(&mount.join("data"));
    () = disk.close().await.unwrap();
}
