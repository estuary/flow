use chrono::{DateTime, Utc};
use futures::StreamExt;
use proto_gazette::{broker, uuid};
use std::io::Write;
use task_logs::{Direction, Filter, Level, Limits, Page, Stop};

/// A data plane is somewhat expensive to start, so sub-tests share one and
/// each uses its own journal.
#[tokio::test]
async fn test_scans() {
    let data_plane = e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
        .await
        .expect("DataPlane start");
    let client = &data_plane.journal_client;
    let root = &data_plane.gazette.fragment_root;

    // Three days of logs in persisted fragments, each persisted a minute
    // after midnight, and a fourth day which isn't yet persisted.
    let journal = "ops/tasks/public/test/logs/kind=capture/name=acmeCo%2Fdays/pivot=00";
    let lines = fixture_lines(day(1), 48 * 3 + 24, chrono::TimeDelta::minutes(30));
    let fragments = [&lines[0..48], &lines[48..96], &lines[96..144]];
    let spool = &lines[144..];
    create_journal(client, root, journal, &fragments).await;
    append(client, journal, spool).await;

    paging(client, journal, &lines).await;
    filters(client, journal, &lines, &fragments).await;
    budgets(client, journal, &lines, &fragments).await;

    // A larger spool, which Backward scans read in windows.
    let journal = "ops/tasks/public/test/logs/kind=capture/name=acmeCo%2Fspool/pivot=00";
    let lines = fixture_lines(day(1), 12_000, chrono::TimeDelta::seconds(1));
    create_journal(client, root, journal, &[]).await;
    append(client, journal, &lines).await;

    windows(client, journal, &lines).await;

    data_plane
        .graceful_stop()
        .await
        .expect("DataPlane graceful_stop");
}

async fn paging(client: &gazette::journal::Client, journal: &str, lines: &[FixtureLine]) {
    for concurrency in [1, 4] {
        let limits = Limits {
            lines: 7,
            concurrency,
            ..limits()
        };
        let pages = scan_all(
            client,
            journal,
            Direction::Forward,
            &Filter::default(),
            limits,
        )
        .await;
        assert_eq!(flatten(&pages, Direction::Forward), expect(lines, |_| true));
        assert_eq!(pages.last().unwrap().stop, Stop::End);

        let pages = scan_all(
            client,
            journal,
            Direction::Backward,
            &Filter::default(),
            limits,
        )
        .await;
        assert_eq!(
            flatten(&pages, Direction::Backward),
            expect(lines, |_| true)
        );
        assert_eq!(pages.last().unwrap().stop, Stop::End);
    }
}

async fn filters(
    client: &gazette::journal::Client,
    journal: &str,
    lines: &[FixtureLine],
    fragments: &[&[FixtureLine]],
) {
    let errors = Filter {
        levels: vec![Level::Error],
        ..Default::default()
    };
    let pages = scan_all(client, journal, Direction::Backward, &errors, limits()).await;
    assert_eq!(
        flatten(&pages, Direction::Backward),
        expect(lines, |l| l.level == "error")
    );
    insta::assert_json_snapshot!(summarize(&pages[0]), @r#"
    {
      "lines": [
        "2026-01-01T00:00:00Z",
        "2026-01-01T08:30:00Z",
        "2026-01-01T17:00:00Z",
        "2026-01-02T01:30:00Z",
        "2026-01-02T10:00:00Z",
        "2026-01-02T18:30:00Z",
        "2026-01-03T03:00:00Z",
        "2026-01-03T11:30:00Z",
        "2026-01-03T20:00:00Z",
        "2026-01-04T04:30:00Z"
      ],
      "stop": "End",
      "bytes_read": 42191,
      "reads": 4
    }
    "#);

    // Text is matched case-insensitively.
    let text = Filter {
        text: Some("DISK FULL".to_string()),
        ..Default::default()
    };
    let pages = scan_all(client, journal, Direction::Forward, &text, limits()).await;
    assert_eq!(
        flatten(&pages, Direction::Forward),
        expect(lines, |l| l.level == "error")
    );

    // A Forward scan within two hours of the second day reads only that
    // day's fragment, and stops at its `until`.
    let window = Filter {
        since: Some(at("2026-01-02T06:00:00Z")),
        until: Some(at("2026-01-02T08:00:00Z")),
        ..Default::default()
    };
    let pages = scan_all(client, journal, Direction::Forward, &window, limits()).await;
    assert_eq!(
        flatten(&pages, Direction::Forward),
        expect(lines, |l| l.ts >= at("2026-01-02T06:00:00Z")
            && l.ts < at("2026-01-02T08:00:00Z"))
    );
    assert_eq!((pages[0].stop, pages[0].stats.reads), (Stop::Bound, 1));
    assert!(pages[0].stats.bytes_read <= fragment_bytes(fragments[1]));

    // A Backward scan before a time skips the fragments appended after it.
    let before = Filter {
        until: Some(at("2026-01-02T08:00:00Z")),
        ..Default::default()
    };
    let limits3 = Limits {
        lines: 3,
        ..limits()
    };
    let page = task_logs::scan(client, journal, Direction::Backward, None, &before, limits3)
        .await
        .unwrap();
    assert_eq!(
        summarize(&page).lines,
        vec![
            "2026-01-02T06:30:00Z",
            "2026-01-02T07:00:00Z",
            "2026-01-02T07:30:00Z"
        ]
    );
    assert_eq!(
        (page.stats.reads, page.stats.bytes_read),
        (1, fragment_bytes(fragments[1]))
    );

    // A Backward scan since a time stops at a fragment persisted before it.
    let warnings = Filter {
        levels: vec![Level::Warn],
        since: Some(at("2026-01-03T00:00:00Z")),
        ..Default::default()
    };
    let pages = scan_all(client, journal, Direction::Backward, &warnings, limits()).await;
    assert_eq!(
        flatten(&pages, Direction::Backward),
        expect(lines, |l| l.level == "warn"
            && l.ts >= at("2026-01-03T00:00:00Z"))
    );
    assert_eq!(pages.last().unwrap().stop, Stop::Bound);
}

async fn budgets(
    client: &gazette::journal::Client,
    journal: &str,
    lines: &[FixtureLine],
    fragments: &[&[FixtureLine]],
) {
    // Every scan reads at least one fragment, even with a smaller budget.
    let one_byte = Limits {
        bytes: 1,
        ..limits()
    };
    for direction in [Direction::Forward, Direction::Backward] {
        let pages = scan_all(client, journal, direction, &Filter::default(), one_byte).await;
        assert_eq!(flatten(&pages, direction), expect(lines, |_| true));
        assert!(
            pages[..pages.len() - 1]
                .iter()
                .all(|p| p.stop == Stop::Bytes)
        );
    }

    // A page stops once its lines reach `line_bytes`.
    let one_line = Limits {
        line_bytes: 1,
        ..limits()
    };
    let page = task_logs::scan(
        client,
        journal,
        Direction::Forward,
        None,
        &Filter::default(),
        one_line,
    )
    .await
    .unwrap();
    assert_eq!((page.lines.len(), page.stop), (1, Stop::Lines));

    // Resuming within a persisted fragment decompresses it from its beginning.
    let from = lines[48 + 10].offset;
    let page = task_logs::scan(
        client,
        journal,
        Direction::Forward,
        Some(from),
        &Filter::default(),
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(page.lines[0].offset, from);
    assert_eq!(
        page.stats.bytes_discarded,
        (from - fragments[1][0].offset) as u64
    );
}

async fn windows(client: &gazette::journal::Client, journal: &str, lines: &[FixtureLine]) {
    // The newest lines come from one window of the spool.
    let tail = Limits {
        lines: 5,
        ..limits()
    };
    let page = task_logs::scan(
        client,
        journal,
        Direction::Backward,
        None,
        &Filter::default(),
        tail,
    )
    .await
    .unwrap();
    assert_eq!(
        flatten(std::slice::from_ref(&page), Direction::Backward),
        expect(&lines[lines.len() - 5..], |_| true)
    );
    assert_eq!(page.stats.reads, 1);

    // Paging through the spool crosses windows, which begin within lines.
    let pages = Limits {
        lines: 3000,
        ..limits()
    };
    let pages = scan_all(
        client,
        journal,
        Direction::Backward,
        &Filter::default(),
        pages,
    )
    .await;
    assert_eq!(
        flatten(&pages, Direction::Backward),
        expect(lines, |_| true)
    );
}

/// Scan pages in `direction` until the scan ends, resuming each from the last.
async fn scan_all(
    client: &gazette::journal::Client,
    journal: &str,
    direction: Direction,
    filter: &Filter,
    limits: Limits,
) -> Vec<Page> {
    let mut pages = Vec::new();
    let mut from = None;

    loop {
        let page = task_logs::scan(client, journal, direction, from, filter, limits)
            .await
            .expect("scan");
        let done = matches!(page.stop, Stop::End | Stop::Bound);

        from = Some(page.resume);
        pages.push(page);

        if done {
            return pages;
        }
        assert!(pages.len() < 1000, "scan didn't finish");
    }
}

fn limits() -> Limits {
    Limits {
        lines: 1000,
        line_bytes: 1 << 30,
        bytes: 1 << 30,
        duration: std::time::Duration::from_secs(30),
        concurrency: 4,
    }
}

/// A fixture log line, and the offsets it was written at.
struct FixtureLine {
    offset: i64,
    ts: DateTime<Utc>,
    level: &'static str,
    content: String,
}

/// Build `count` lines spaced by `step`, with offsets as if written contiguously.
fn fixture_lines(begin: DateTime<Utc>, count: usize, step: chrono::TimeDelta) -> Vec<FixtureLine> {
    let mut offset = 0;

    (0..count)
        .map(|i| {
            let ts = begin + step * i as i32;
            let (level, message) = match i {
                i if i % 17 == 0 => ("error", format!("disk full on volume {i}")),
                i if i % 7 == 0 => ("warn", "retrying request after 503".to_string()),
                _ => ("info", "transaction committed".to_string()),
            };
            let clock = uuid::Clock::from_unix(ts.timestamp() as u64, ts.timestamp_subsec_nanos());
            let uuid = uuid::build(uuid::Producer([1, 2, 3, 4, 5, 6]), clock, uuid::Flags::OUTSIDE_TXN);
            let ts_str = ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

            let content = format!(
                r#"{{"_meta":{{"uuid":"{uuid}"}},"shard":{{"kind":"capture","name":"acmeCo/orders","keyBegin":"00000000","rClockBegin":"00000000"}},"ts":"{ts_str}","level":"{level}","message":"{message}","fields":{{"i":{i}}}}}"#
            ) + "\n";

            let line = FixtureLine {
                offset,
                ts,
                level,
                content,
            };
            offset += line.content.len() as i64;
            line
        })
        .collect()
}

/// Write `fragments` as persisted fragment files, each persisted a minute
/// after the day of its last line, and then create `journal` over them.
async fn create_journal(
    client: &gazette::journal::Client,
    root: &std::path::Path,
    journal: &str,
    fragments: &[&[FixtureLine]],
) {
    for fragment in fragments {
        let begin = fragment[0].offset;
        let end = begin + fragment_bytes(fragment) as i64;
        let dir = root.join(journal);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join(format!("{begin:016x}-{end:016x}-{:040x}.gz", end));
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        for line in fragment.iter() {
            gz.write_all(line.content.as_bytes()).unwrap();
        }
        std::fs::write(&path, gz.finish().unwrap()).unwrap();

        let persisted = fragment.last().unwrap().ts.date_naive().succ_opt().unwrap();
        let persisted = persisted.and_hms_opt(0, 1, 0).unwrap().and_utc();
        let file = std::fs::File::options().write(true).open(&path).unwrap();
        file.set_modified(persisted.into()).unwrap();
    }

    let spec = broker::JournalSpec {
        name: journal.to_string(),
        replication: 1,
        labels: Some(labels::build_set([(
            labels::CONTENT_TYPE,
            labels::CONTENT_TYPE_JSON_LINES,
        )])),
        fragment: Some(broker::journal_spec::Fragment {
            length: 1 << 30,
            compression_codec: broker::CompressionCodec::Gzip as i32,
            stores: vec!["file:///".to_string()],
            refresh_interval: Some(std::time::Duration::from_secs(1).into()),
            flush_interval: Some(std::time::Duration::from_secs(48 * 3600).into()),
            retention: None,
            path_postfix_template: String::new(),
        }),
        flags: broker::journal_spec::Flag::ORdwr as u32,
        max_append_rate: 0,
        suspend: None,
    };
    client
        .apply(broker::ApplyRequest {
            changes: vec![broker::apply_request::Change {
                expect_mod_revision: 0,
                upsert: Some(spec),
                delete: String::new(),
            }],
        })
        .await
        .expect("create journal");

    // Wait for the broker to list the fragment files.
    for _ in 0..300 {
        let listed = client
            .list_fragments(broker::FragmentsRequest {
                journal: journal.to_string(),
                ..Default::default()
            })
            .await;
        if matches!(&listed, Ok(resp) if resp.fragments.len() >= fragments.len()) {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("broker didn't list the fragments of {journal}");
}

/// Append `lines` to `journal` at its write head.
async fn append(client: &gazette::journal::Client, journal: &str, lines: &[FixtureLine]) {
    // Name the write head of persisted fragments, which rolls the broker's spool forward to it.
    let listed = client
        .list_fragments(broker::FragmentsRequest {
            journal: journal.to_string(),
            ..Default::default()
        })
        .await
        .expect("list fragments");
    let offset = listed
        .fragments
        .iter()
        .filter_map(|f| f.spec.as_ref())
        .map(|f| f.end)
        .max()
        .unwrap_or_default();

    let content: bytes::Bytes = lines
        .iter()
        .map(|l| l.content.as_str())
        .collect::<String>()
        .into();
    let req = broker::AppendRequest {
        journal: journal.to_string(),
        offset,
        ..Default::default()
    };
    let mut appends = std::pin::pin!(client.append(req, move || {
        futures::stream::once(std::future::ready(Ok(content.clone())))
    }));
    while let Some(resp) = appends.next().await {
        resp.expect("append");
    }
}

/// Lines of `pages` scanned in `direction`, in ascending order.
fn flatten(pages: &[Page], direction: Direction) -> Vec<(DateTime<Utc>, String)> {
    let mut pages = pages.iter().collect::<Vec<_>>();
    if direction == Direction::Backward {
        pages.reverse();
    }
    pages
        .into_iter()
        .flat_map(|page| page.lines.iter().map(|l| (l.ts, l.message.clone())))
        .collect()
}

/// Fixture lines which match `predicate`, in the form of `flatten`.
fn expect(
    lines: &[FixtureLine],
    predicate: impl Fn(&FixtureLine) -> bool,
) -> Vec<(DateTime<Utc>, String)> {
    lines
        .iter()
        .filter(|l| predicate(l))
        .map(|l| {
            let doc: serde_json::Value = serde_json::from_str(&l.content).unwrap();
            (l.ts, doc["message"].as_str().unwrap().to_string())
        })
        .collect()
}

#[derive(serde::Serialize)]
struct Summary {
    lines: Vec<String>,
    stop: Stop,
    bytes_read: u64,
    reads: u32,
}

fn summarize(page: &Page) -> Summary {
    Summary {
        lines: page
            .lines
            .iter()
            .map(|l| l.ts.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .collect(),
        stop: page.stop,
        bytes_read: page.stats.bytes_read,
        reads: page.stats.reads,
    }
}

fn fragment_bytes(lines: &[FixtureLine]) -> u64 {
    lines.iter().map(|l| l.content.len() as u64).sum()
}

fn day(n: u32) -> DateTime<Utc> {
    chrono::NaiveDate::from_ymd_opt(2026, 1, n)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
}

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().to_utc()
}
