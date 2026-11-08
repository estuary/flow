use serde::Deserialize;

pub async fn stream_logs(
    ctx: &crate::CliContext,
    logs_token: &str,
    mut last_logged_at: Option<crate::Timestamp>,
) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        #[derive(Deserialize, Debug)]
        struct Log {
            log_line: String,
            logged_at: crate::Timestamp,
            stream: String,
        }
        let logs: Vec<Log> = flow_client_next::postgrest::exec::<Vec<Log>>(
            ctx.pg.rpc(
                "view_logs",
                serde_json::json!({
                    "bearer_token": logs_token,
                    "last_logged_at": last_logged_at,
                })
                .to_string(),
            ),
            ctx.access_token().as_deref(),
        )
        .await?;

        if let Some(last) = logs.last() {
            last_logged_at = Some(last.logged_at.clone());
        }

        for Log {
            log_line,
            logged_at,
            stream,
        } in logs
        {
            println!("{logged_at} {stream}> {log_line}");
        }
    }
}

/// One poll of an asynchronous job: its outcome once terminal,
/// and a page of its logs following the requested cursor.
pub struct JobPoll<T> {
    pub outcome: Option<T>,
    pub logs: Vec<LogLine>,
    pub end_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Debug)]
pub struct LogLine {
    pub logged_at: chrono::DateTime<chrono::Utc>,
    pub stream: String,
    pub line: String,
}

impl std::fmt::Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let logged_at = self
            .logged_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        write!(f, "{logged_at} {}> {}", self.stream, self.line)
    }
}

/// Poll a job by `fetch` until it has an outcome, passing its logs to `on_log`.
///
/// `fetch` is called with the cursor of the last log line seen.
/// A page having more logs is followed by an immediate fetch of the next.
/// Otherwise, a job without an outcome is fetched again after `interval`.
/// A job with an outcome has its remaining pages drained, and then returns.
/// Logs written after that final page are not observed.
pub async fn poll_job<T, F, Fut>(
    mut fetch: F,
    interval: std::time::Duration,
    mut on_log: impl FnMut(LogLine),
) -> anyhow::Result<T>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<JobPoll<T>>>,
{
    let mut after = None;
    let mut outcome = None;

    loop {
        let JobPoll {
            outcome: polled,
            logs,
            end_cursor,
            has_next_page,
        } = fetch(after.clone()).await?;

        logs.into_iter().for_each(&mut on_log);

        // An empty page has no cursor, and we resume from the one we have.
        if end_cursor.is_some() {
            after = end_cursor;
        }
        // An outcome is terminal: polls which drain logs report it again.
        outcome = outcome.or(polled);

        if has_next_page {
            continue;
        } else if let Some(outcome) = outcome {
            return Ok(outcome);
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn poll_job_follows_pages_and_drains_after_outcome() {
        // Each step is (outcome, log lines, end cursor, has next page).
        let script: Vec<(Option<&str>, &[&str], Option<&str>, bool)> = vec![
            (None, &["one", "two"], Some("c2"), true),
            (None, &["three"], Some("c3"), false),
            (None, &[], None, false),
            (Some("SUCCESS"), &["four"], Some("c4"), true),
            (Some("SUCCESS"), &["five"], Some("c5"), false),
        ];
        let mut script = script.into_iter();

        let start = tokio::time::Instant::now();
        let mut trace = Vec::new();
        let mut lines = Vec::new();

        let outcome = poll_job(
            |after| {
                let elapsed = start.elapsed().as_secs();
                trace.push(format!("fetch after {after:?} at {elapsed}s"));

                let (outcome, logs, end_cursor, has_next_page) = script.next().unwrap();
                async move {
                    Ok(JobPoll {
                        outcome,
                        logs: logs
                            .iter()
                            .map(|line| LogLine {
                                logged_at: chrono::DateTime::UNIX_EPOCH,
                                stream: "stdout".to_string(),
                                line: line.to_string(),
                            })
                            .collect(),
                        end_cursor: end_cursor.map(str::to_string),
                        has_next_page,
                    })
                }
            },
            std::time::Duration::from_secs(1),
            |line| lines.push(line.to_string()),
        )
        .await
        .unwrap();

        assert_eq!(outcome, "SUCCESS");
        insta::assert_debug_snapshot!((trace, lines), @r#"
        (
            [
                "fetch after None at 0s",
                "fetch after Some(\"c2\") at 0s",
                "fetch after Some(\"c3\") at 1s",
                "fetch after Some(\"c3\") at 2s",
                "fetch after Some(\"c4\") at 2s",
            ],
            [
                "1970-01-01T00:00:00Z stdout> one",
                "1970-01-01T00:00:00Z stdout> two",
                "1970-01-01T00:00:00Z stdout> three",
                "1970-01-01T00:00:00Z stdout> four",
                "1970-01-01T00:00:00Z stdout> five",
            ],
        )
        "#);
    }

    #[tokio::test]
    async fn poll_job_returns_fetch_errors() {
        let error = poll_job::<(), _, _>(
            |_after| async { anyhow::bail!("whoops") },
            std::time::Duration::ZERO,
            |_line| unreachable!(),
        )
        .await
        .unwrap_err();

        insta::assert_snapshot!(error, @"whoops");
    }
}
