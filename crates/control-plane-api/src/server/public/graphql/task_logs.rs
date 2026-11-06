//! The `taskLogs` query: a filtered scan of a task's ops logs journal.
//!
//! The control plane reads the journal from the task's data plane, using a
//! broker token it signs for the task's ops logs journal, and filters lines
//! as it reads them (see the `task-logs` crate).

use async_graphql::types::connection;
use base64::Engine as _;

/// Bounds of the work done by one `taskLogs` request.
const DEFAULT_LINES: usize = 100;
const MAX_LINES: usize = 1_000;
const DEFAULT_BYTES: u64 = 1 << 30;
const MAX_BYTES: u64 = 4 << 30;
const DEFAULT_DURATION: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_DURATION: std::time::Duration = std::time::Duration::from_secs(30);
/// Log lines can be large, so a page is also bounded by its total size.
const MAX_LINE_BYTES: usize = 16 << 20;
/// Persisted fragments that each scan reads at once.
const CONCURRENCY: usize = 4;

/// Shared so that scans reuse connections to fragment stores.
static FRAGMENT_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(gazette::journal::Client::new_fragment_client);

#[derive(Debug, Default)]
pub struct TaskLogsQuery;

#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum TaskLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

/// Conditions which a log line must match. All present conditions must match.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct TaskLogsFilter {
    /// Levels to match. Absent or empty matches every level.
    levels: Option<Vec<TaskLogLevel>>,
    /// Inclusive lower bound of the line's timestamp.
    since: Option<chrono::DateTime<chrono::Utc>>,
    /// Exclusive upper bound of the line's timestamp.
    until: Option<chrono::DateTime<chrono::Utc>>,
    /// Case-insensitive text to find within the line's message or fields.
    text: Option<String>,
}

/// Bounds of the work done by one request, which stops early with a
/// `resumeCursor` once either is reached.
#[derive(Debug, Clone, Default, async_graphql::InputObject)]
pub struct TaskLogsBudget {
    /// Maximum log bytes to read. Default 1GiB, maximum 4GiB.
    max_bytes: Option<super::UInt64>,
    /// Maximum milliseconds to scan. Default 10s, maximum 30s.
    max_millis: Option<u32>,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct TaskLogLine {
    ts: chrono::DateTime<chrono::Utc>,
    level: TaskLogLevel,
    message: String,
    fields: Option<super::JsonObject>,
    /// UUID of the log document, which is unique to the line.
    uuid: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum TaskLogsStop {
    /// The requested number of lines matched.
    Lines,
    /// The byte budget was read.
    Bytes,
    /// The time budget elapsed.
    Time,
    /// The scan passed the `since` (backward) or `until` (forward) of its filter.
    Bound,
    /// A forward scan reached the newest logs,
    /// or a backward scan reached the oldest retained logs.
    End,
}

/// Progress of the scan which produced a page.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct TaskLogsScan {
    /// Cursor from which a scan continues in the same direction: pass it as
    /// `before` to continue a backward scan, or as `after` to continue a
    /// forward scan. A forward scan which stopped at the newest logs can be
    /// continued later to read logs written since.
    resume_cursor: String,
    /// Approximate timestamp of the logs at `resumeCursor`,
    /// for showing how far a scan has progressed.
    resume_ts: Option<chrono::DateTime<chrono::Utc>>,
    stopped_by: TaskLogsStop,
    bytes_read: super::UInt64,
    bytes_discarded: super::UInt64,
    elapsed_millis: u32,
}

pub type TaskLogLineConnection = connection::Connection<
    String,
    TaskLogLine,
    TaskLogsScan,
    connection::EmptyFields,
    connection::DefaultConnectionName,
    connection::DefaultEdgeName,
    connection::DisableNodesField,
>;

#[async_graphql::Object]
impl TaskLogsQuery {
    /// Scan the logs of a task, returning lines which match `filter`.
    ///
    /// Pass `last` (and optionally `before`) to scan backward from the
    /// newest logs, or `first` (and optionally `after`) to scan forward.
    /// Without either, returns the last 100 lines. Lines are always
    /// returned in ascending log order.
    ///
    /// A scan stops upon matching the requested number of lines, upon
    /// reading its `budget`, or upon passing a time bound of `filter`. It
    /// may stop with fewer lines than requested, even zero, in which case
    /// `hasPreviousPage` (backward) or `hasNextPage` (forward) tells whether
    /// continuing the scan from `resumeCursor` may find more.
    #[allow(clippy::too_many_arguments)]
    async fn task_logs(
        &self,
        ctx: &async_graphql::Context<'_>,
        task: String,
        filter: Option<TaskLogsFilter>,
        budget: Option<TaskLogsBudget>,
        after: Option<String>,
        before: Option<String>,
        first: Option<i32>,
        last: Option<i32>,
    ) -> async_graphql::Result<TaskLogLineConnection> {
        let env = ctx.data::<crate::Envelope>()?;
        let task = models::Name::new(task);

        // Authorize before validating anything else, so that error messages
        // cannot be used to probe for names the caller cannot see.
        let policy_result = crate::server::authorize_user_task::evaluate_authorization(
            env.snapshot(),
            env.claims()?,
            &task,
            models::Capability::Read,
        );
        let (_expiry, (data_plane, mut broker_claims, ops_logs_journal, ..)) =
            env.authorization_outcome(policy_result).await?;

        let (direction, from, lines) = match (after, before, first, last) {
            (None, before, None, last) => (
                task_logs::Direction::Backward,
                before
                    .as_deref()
                    .map(decode_cursor)
                    .transpose()?
                    .map(|(begin, _end)| begin),
                last,
            ),
            (after, None, first, None) => (
                task_logs::Direction::Forward,
                after
                    .as_deref()
                    .map(decode_cursor)
                    .transpose()?
                    .map(|(_begin, end)| end),
                first,
            ),
            _ => {
                return Err(async_graphql::Error::new(
                    "pass either `first` and `after` to scan forward, or `last` and `before` to scan backward",
                ));
            }
        };
        let lines = match lines {
            None => DEFAULT_LINES,
            Some(n) if n > 0 && n as usize <= MAX_LINES => n as usize,
            Some(_) => {
                return Err(async_graphql::Error::new(
                    "`first` or `last` must be within 1..=1000",
                ));
            }
        };
        let TaskLogsBudget {
            max_bytes,
            max_millis,
        } = budget.unwrap_or_default();

        let limits = task_logs::Limits {
            lines,
            line_bytes: MAX_LINE_BYTES,
            bytes: max_bytes.map_or(DEFAULT_BYTES, |b| b.0.min(MAX_BYTES)),
            duration: max_millis.map_or(DEFAULT_DURATION, |ms| {
                std::time::Duration::from_millis(ms as u64).min(MAX_DURATION)
            }),
            concurrency: CONCURRENCY,
        };
        let TaskLogsFilter {
            levels,
            since,
            until,
            text,
        } = filter.unwrap_or_default();

        let filter = task_logs::Filter {
            levels: levels
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            since,
            until,
            text,
        };

        let iat = tokens::now();
        broker_claims.iat = iat.timestamp() as u64;
        broker_claims.exp =
            (iat + chrono::TimeDelta::from_std(MAX_DURATION).unwrap() * 2).timestamp() as u64;
        let token = data_plane.sign_claims(broker_claims)?;

        let client = gazette::journal::Client::new(
            data_plane.broker_address.clone(),
            FRAGMENT_CLIENT.clone(),
            proto_grpc::Metadata::new().with_bearer_token(&token)?,
            gazette::Router::new("local"),
        );

        let page = task_logs::scan(&client, &ops_logs_journal, direction, from, &filter, limits)
            .await
            .map_err(|err| async_graphql::Error::new(format!("reading task logs: {err}")))?;

        let more = !matches!(page.stop, task_logs::Stop::End | task_logs::Stop::Bound);
        let (has_previous_page, has_next_page) = match direction {
            task_logs::Direction::Backward => (more, from.is_some()),
            task_logs::Direction::Forward => (from.is_some(), more),
        };

        let task_logs::Stats {
            bytes_read,
            bytes_discarded,
            elapsed,
            ..
        } = page.stats;

        let mut conn = TaskLogLineConnection::with_additional_fields(
            has_previous_page,
            has_next_page,
            TaskLogsScan {
                resume_cursor: encode_cursor(page.resume, page.resume),
                resume_ts: page.resume_ts,
                stopped_by: page.stop.into(),
                bytes_read: bytes_read.into(),
                bytes_discarded: bytes_discarded.into(),
                elapsed_millis: elapsed.as_millis() as u32,
            },
        );
        conn.edges = page
            .lines
            .into_iter()
            .map(|line| {
                connection::Edge::new(
                    encode_cursor(line.offset, line.end),
                    TaskLogLine {
                        ts: line.ts,
                        level: line.level.into(),
                        message: line.message,
                        fields: line.fields.map(async_graphql::Json),
                        uuid: line.uuid,
                    },
                )
            })
            .collect();

        Ok(conn)
    }
}

/// A cursor encodes the journal offsets at which a line begins and ends.
/// `before` a cursor is below its beginning, and `after` it is at its end.
/// A `resumeCursor` encodes a single offset as an empty line.
fn encode_cursor(begin: i64, end: i64) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!("{begin}:{end}"))
}

fn decode_cursor(cursor: &str) -> async_graphql::Result<(i64, i64)> {
    let invalid = || async_graphql::Error::new("invalid cursor");

    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| invalid())?;
    let decoded = String::from_utf8(decoded).map_err(|_| invalid())?;
    let (begin, end) = decoded.split_once(':').ok_or_else(invalid)?;

    Ok((
        begin.parse().map_err(|_| invalid())?,
        end.parse().map_err(|_| invalid())?,
    ))
}

impl From<TaskLogLevel> for task_logs::Level {
    fn from(level: TaskLogLevel) -> Self {
        match level {
            TaskLogLevel::Error => task_logs::Level::Error,
            TaskLogLevel::Warn => task_logs::Level::Warn,
            TaskLogLevel::Info => task_logs::Level::Info,
            TaskLogLevel::Debug => task_logs::Level::Debug,
            TaskLogLevel::Trace => task_logs::Level::Trace,
        }
    }
}

impl From<task_logs::Level> for TaskLogLevel {
    fn from(level: task_logs::Level) -> Self {
        match level {
            task_logs::Level::Error => TaskLogLevel::Error,
            task_logs::Level::Warn => TaskLogLevel::Warn,
            task_logs::Level::Info => TaskLogLevel::Info,
            task_logs::Level::Debug => TaskLogLevel::Debug,
            task_logs::Level::Trace => TaskLogLevel::Trace,
        }
    }
}

impl From<task_logs::Stop> for TaskLogsStop {
    fn from(stop: task_logs::Stop) -> Self {
        match stop {
            task_logs::Stop::Lines => TaskLogsStop::Lines,
            task_logs::Stop::Bytes => TaskLogsStop::Bytes,
            task_logs::Stop::Duration => TaskLogsStop::Time,
            task_logs::Stop::Bound => TaskLogsStop::Bound,
            task_logs::Stop::End => TaskLogsStop::End,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::test_server;
    use futures::StreamExt;
    use proto_gazette::broker;

    #[test]
    fn test_cursor_round_trip() {
        let cursor = super::encode_cursor(1234, 5678);
        assert_eq!(super::decode_cursor(&cursor).unwrap(), (1234, 5678));
        assert!(super::decode_cursor("not a cursor").is_err());
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../../../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_task_logs(pool: sqlx::PgPool) {
        let _guard = test_server::init();

        // Serve data plane `one`, which homes aliceCo/in/capture-foo, from a broker cluster.
        let data_plane =
            e2e_support::DataPlane::start(e2e_support::DataPlaneArgs { broker_count: 1 })
                .await
                .expect("DataPlane start");
        sqlx::query(
            "update public.data_planes set broker_address = $1, hmac_keys = $2, encrypted_hmac_keys = '{}' where id = '111111111111'",
        )
        .bind(&data_plane.gazette.brokers[0].endpoint)
        .bind(vec![data_plane.gazette.auth_keys.clone()])
        .execute(&pool)
        .await
        .unwrap();

        let journal = "ops/tasks/public/one/logs/gen1234/kind=capture/name=aliceCo%2Fin%2Fcapture-foo/pivot=00";
        create_journal(&data_plane.journal_client, journal).await;
        let levels = ["info", "warn", "error", "info", "error"];
        append(&data_plane.journal_client, journal, &levels).await;

        let server = test_server::TestServer::start(
            pool.clone(),
            test_server::snapshot(pool.clone(), true).await,
        )
        .await;
        let token = server.make_access_token(
            uuid::Uuid::from_bytes([0x11; 16]),
            Some("alice@example.com"),
        );

        let query = |variables: serde_json::Value| {
            serde_json::json!({
                "query": r#"
                    query($task: String!, $filter: TaskLogsFilter, $first: Int, $after: String, $last: Int, $before: String) {
                        taskLogs(task: $task, filter: $filter, first: $first, after: $after, last: $last, before: $before) {
                            edges { node { ts level message fields } }
                            pageInfo { hasPreviousPage hasNextPage }
                            resumeCursor
                            resumeTs
                            stoppedBy
                            bytesRead
                        }
                    }
                "#,
                "variables": variables,
            })
        };

        // The newest warning or error, and then the next older ones.
        let page: serde_json::Value = server
            .graphql(
                &query(serde_json::json!({
                    "task": "aliceCo/in/capture-foo",
                    "last": 1,
                    "filter": {"levels": ["ERROR", "WARN"]},
                })),
                Some(&token),
            )
            .await;
        let resume = page["data"]["taskLogs"]["resumeCursor"].clone();

        let older: serde_json::Value = server
            .graphql(
                &query(serde_json::json!({
                    "task": "aliceCo/in/capture-foo",
                    "last": 10,
                    "before": resume,
                    "filter": {"levels": ["ERROR", "WARN"]},
                })),
                Some(&token),
            )
            .await;

        // Text search, scanning forward.
        let search: serde_json::Value = server
            .graphql(
                &query(serde_json::json!({
                    "task": "aliceCo/in/capture-foo",
                    "first": 10,
                    "filter": {"text": "VOLUME 4"},
                })),
                Some(&token),
            )
            .await;

        // A task which Alice can't read, and conflicting arguments.
        let denied: serde_json::Value = server
            .graphql(
                &query(serde_json::json!({"task": "bobCo/in/capture-baz", "last": 1})),
                Some(&token),
            )
            .await;
        let conflicting: serde_json::Value = server
            .graphql(
                &query(
                    serde_json::json!({"task": "aliceCo/in/capture-foo", "first": 1, "last": 1}),
                ),
                Some(&token),
            )
            .await;

        insta::assert_json_snapshot!(
            serde_json::json!({
                "page": page,
                "older": older,
                "search": search,
                "denied": denied["errors"][0]["message"],
                "conflicting": conflicting["errors"][0]["message"],
            }),
            {".**.resumeCursor" => "[cursor]"},
            @r#"
        {
          "conflicting": "pass either `first` and `after` to scan forward, or `last` and `before` to scan backward",
          "denied": "PermissionDenied: alice@example.com is not authorized to bobCo/in/capture-baz for Read",
          "older": {
            "data": {
              "taskLogs": {
                "bytesRead": "826",
                "edges": [
                  {
                    "node": {
                      "fields": {
                        "i": 1
                      },
                      "level": "WARN",
                      "message": "warn on volume 1",
                      "ts": "2026-01-01T00:01:00+00:00"
                    }
                  },
                  {
                    "node": {
                      "fields": {
                        "i": 2
                      },
                      "level": "ERROR",
                      "message": "error on volume 2",
                      "ts": "2026-01-01T00:02:00+00:00"
                    }
                  }
                ],
                "pageInfo": {
                  "hasNextPage": true,
                  "hasPreviousPage": false
                },
                "resumeCursor": "[cursor]",
                "resumeTs": "2026-01-01T00:00:00+00:00",
                "stoppedBy": "END"
              }
            }
          },
          "page": {
            "data": {
              "taskLogs": {
                "bytesRead": "1034",
                "edges": [
                  {
                    "node": {
                      "fields": {
                        "i": 4
                      },
                      "level": "ERROR",
                      "message": "error on volume 4",
                      "ts": "2026-01-01T00:04:00+00:00"
                    }
                  }
                ],
                "pageInfo": {
                  "hasNextPage": false,
                  "hasPreviousPage": true
                },
                "resumeCursor": "[cursor]",
                "resumeTs": "2026-01-01T00:04:00+00:00",
                "stoppedBy": "LINES"
              }
            }
          },
          "search": {
            "data": {
              "taskLogs": {
                "bytesRead": "1034",
                "edges": [
                  {
                    "node": {
                      "fields": {
                        "i": 4
                      },
                      "level": "ERROR",
                      "message": "error on volume 4",
                      "ts": "2026-01-01T00:04:00+00:00"
                    }
                  }
                ],
                "pageInfo": {
                  "hasNextPage": false,
                  "hasPreviousPage": false
                },
                "resumeCursor": "[cursor]",
                "resumeTs": "2026-01-01T00:04:00+00:00",
                "stoppedBy": "END"
              }
            }
          }
        }
        "#
        );

        data_plane
            .graceful_stop()
            .await
            .expect("DataPlane graceful_stop");
    }

    async fn create_journal(client: &gazette::journal::Client, journal: &str) {
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
    }

    /// Append a log line at each of `levels`, a minute apart.
    async fn append(client: &gazette::journal::Client, journal: &str, levels: &[&str]) {
        let content: String = levels
            .iter()
            .enumerate()
            .map(|(i, level)| {
                let uuid = proto_gazette::uuid::build(
                    proto_gazette::uuid::Producer([1, 2, 3, 4, 5, 6]),
                    proto_gazette::uuid::Clock::from_unix(1_767_225_600 + 60 * i as u64, 0),
                    proto_gazette::uuid::Flags::OUTSIDE_TXN,
                );
                format!(
                    r#"{{"_meta":{{"uuid":"{uuid}"}},"shard":{{"kind":"capture","name":"aliceCo/in/capture-foo"}},"ts":"2026-01-01T00:0{i}:00Z","level":"{level}","message":"{level} on volume {i}","fields":{{"i":{i}}}}}"#
                ) + "\n"
            })
            .collect();
        let content = bytes::Bytes::from(content);

        let mut appends = std::pin::pin!(client.append(
            broker::AppendRequest {
                journal: journal.to_string(),
                ..Default::default()
            },
            move || futures::stream::once(std::future::ready(Ok(content.clone()))),
        ));
        // A new journal may not yet have a primary broker. Polling again retries.
        while let Some(resp) = appends.next().await {
            if let Err(err) = resp
                && err.attempt > 20
            {
                panic!("append: {err:?}");
            }
        }
    }
}
