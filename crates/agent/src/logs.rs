use proto_flow::ops;
use sqlx::types::Uuid;
use tokio::io::AsyncBufReadExt;
use tracing::{debug, trace};

// Line is a recorded log line.
#[derive(Debug)]
pub struct Line {
    // Token which identifies the line's log set.
    token: Uuid,
    // Stream of this logged line.
    stream: String,
    // Contents of the line.
    line: String,
}

// Tx is the channel sender of log Lines.
pub type Tx = tokio::sync::mpsc::Sender<Line>;

// capture_job_logs consumes newline-delimited lines from the AsyncRead and
// streams each as a Line to the channel Sender.
#[tracing::instrument(level = "debug", err, skip(tx, reader))]
pub async fn capture_lines<R>(
    tx: Tx,
    stream: String,
    token: Uuid,
    reader: R,
) -> Result<(), std::io::Error>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut splits = tokio::io::BufReader::new(reader).split(b'\n');
    while let Some(line) = splits.next_segment().await? {
        // Attempt a direct conversion to String without a copy.
        // Fall back to a lossy UTF8 replacement.
        let line = String::from_utf8(line)
            .unwrap_or_else(|err| String::from_utf8_lossy(err.as_bytes()).into_owned());

        tx.send(Line {
            token,
            stream: stream.clone(),
            line,
        })
        .await
        .unwrap();
    }
    Ok(())
}

/// NULL is a perfectly valid thing to include in UTF8 bytes, but not according
/// to postgres. It rejects `TEXT` values containing nulls. This replaces all
/// null bytes with a space character. The space was chosen somewhat arbitrarily
/// as a goodenuf replacement in this rare edge case.
fn sanitize_null_bytes(line: String) -> String {
    // Check to avoid copying the string if it doesn't contain the character.
    if line.contains('\u{0000}') {
        line.replace('\u{0000}', " ")
    } else {
        line
    }
}

// serve_sink consumes log Lines from the receiver, streaming each
// to the `logs` table of the database.
#[tracing::instrument(ret, skip_all)]
pub async fn serve_sink(
    pg_pool: sqlx::PgPool,
    mut rx: tokio::sync::mpsc::Receiver<Line>,
) -> sqlx::Result<()> {
    // Lines, re-shaped into a columnar form for vectorized dispatch.
    let mut tokens = Vec::new();
    let mut streams = Vec::new();
    let mut lines = Vec::new();

    let mut held_conn = None;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
    let mut used_this_interval = false;

    // The default is to burst multiple ticks if they get delayed.
    // Don't do that.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        // Blocking read of either the next line or the next interval tick.
        tokio::select! {
            recv = rx.recv() => match recv {
                Some(Line{token, stream, line}) =>  {
                    trace!(%token, %stream, %line, "rx (initial)");
                    tokens.push(token);
                    streams.push(stream);
                    lines.push(sanitize_null_bytes(line));
                }
                None => {
                    debug!("rx (eof)");
                    return Ok(())
                }
            },
            _ = interval.tick() => {
                if held_conn.is_some() && !used_this_interval {
                    held_conn = None;
                    debug!("released pg_conn");
                }
                used_this_interval = false;
                continue;
            },
        };

        // Read additional ready lines without blocking.
        while let Ok(Line {
            token,
            stream,
            line,
        }) = rx.try_recv()
        {
            trace!(%token, %stream, %line, "rx (cont)");
            tokens.push(token);
            streams.push(stream);
            lines.push(sanitize_null_bytes(line));
        }

        if let None = held_conn {
            held_conn = Some(pg_pool.acquire().await?);
            debug!("acquired new pg_conn");
        }
        used_this_interval = true;

        // Dispatch the vector of lines to the table.
        let r = sqlx::query(
            r#"
            insert into internal.log_lines (token, stream, log_line)
            select * from unnest($1, $2, $3)
            "#,
        )
        .bind(&tokens)
        .bind(&streams)
        .bind(&lines)
        .execute(held_conn.as_deref_mut().unwrap())
        .await?;

        trace!(rows = ?r.rows_affected(), "inserted logs");

        tokens.clear();
        streams.clear();
        lines.clear();
    }
}

#[derive(Debug, Clone)]
pub struct OpsHandler {
    tx: Tx,
    stream: String,
    token: Uuid,
}

impl runtime::LogHandler for OpsHandler {
    fn log(&self, log: &ops::Log) {
        let Err(tokio::sync::mpsc::error::TrySendError::Full(line)) = self.tx.try_send(Line {
            token: self.token.clone(),
            stream: self.stream.clone(),
            line: render_ops_log_for_ui(log),
        }) else {
            return;
        };

        // Perform an expensive "move" of all other tasks scheduled on the
        // current async executor thread, so that we can block until there's capacity.
        let tx_clone = self.tx.clone();
        _ = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(tx_clone.send(line))
        });
    }
}

/// ops_handler returns an ops::Log handler that dispatches to `tx`
/// using the given `stream` and `token`.
pub fn ops_handler(tx: Tx, stream: String, token: Uuid) -> OpsHandler {
    OpsHandler { tx, stream, token }
}

// TODO(johnny): This is a placeholder until all `internal.log_lines` can be JSON.
// Then we'll pass everything through as JSON and let the UI handle structured presentation.
fn render_ops_log_for_ui(log: &ops::Log) -> String {
    use colored_json::{Color, ColorMode, ColoredFormatter, CompactFormatter, Style, Styler};
    use ops::log::Level;
    use std::fmt::Write;

    // These colors are drawn from Go's logrus package, and are chosen to be consistent with it.
    let red = Color::Red; // 31
    let yellow = Color::Yellow; // 33;
    let blue = Color::Fixed(36);
    let gray = Color::Fixed(37);

    let mut line = String::new();

    let (level_txt, level_color) = match log.level() {
        Level::Trace => ("TRACE", gray),
        Level::Debug => ("DEBUG", gray),
        Level::Info => (" INFO", blue),
        Level::Warn => (" WARN", yellow),
        Level::Error => ("ERROR", red),
        Level::UndefinedLevel => ("UNDEFINED", red),
    };

    // Using colored_json's yansi re-export
    use colored_json::Paint;
    write!(
        &mut line,
        "{}: {: <30}", // Right-pad the message to 30 characters.
        level_txt.paint(level_color).dim(),
        log.message
    )
    .unwrap();

    let f = ColoredFormatter::with_styler(
        CompactFormatter {},
        Styler {
            key: Style::new().fg(blue).italic(),
            string_value: Style::default(),
            string_include_quotation: false,
            ..Default::default()
        },
    );

    for (field, content_json) in &log.fields_json_map {
        let content: serde_json::Value = serde_json::from_slice(content_json).unwrap();

        write!(
            &mut line,
            " {}={}",
            field.as_str().paint(blue).italic(),
            f.clone().to_colored_json(&content, ColorMode::On).unwrap(),
        )
        .unwrap();
    }

    line
}

#[cfg(test)]
mod test {
    use super::render_ops_log_for_ui;
    use proto_flow::ops;

    #[test]
    fn test_log_rendering() {
        let fixture = ops::Log {
            level: ops::log::Level::Info as i32,
            message: "The log message!".to_string(),
            fields_json_map: [
                ("number", "42"),
                ("boolean", "true"),
                ("object", "{\"key\":\"value\"}"),
                ("array", "[1,2,true,\"false\"]"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.into()))
            .collect(),
            ..Default::default()
        };

        println!("{}", render_ops_log_for_ui(&fixture));
        insta::assert_debug_snapshot!(render_ops_log_for_ui(&fixture), @r###""\u{1b}[2;38;5;36m INFO\u{1b}[0m: The log message!               \u{1b}[3;38;5;36marray\u{1b}[0m=\u{1b}[1m[\u{1b}[0m1,2,true,\"false\"\u{1b}[1m]\u{1b}[0m \u{1b}[3;38;5;36mboolean\u{1b}[0m=true \u{1b}[3;38;5;36mnumber\u{1b}[0m=42 \u{1b}[3;38;5;36mobject\u{1b}[0m=\u{1b}[1m{\u{1b}[0m\"\u{1b}[3;38;5;36mkey\u{1b}[0m\":\"value\"\u{1b}[1m}\u{1b}[0m""###);
    }
}
