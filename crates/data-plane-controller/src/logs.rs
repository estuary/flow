use sqlx::types::{chrono, Uuid};
use tokio::io::AsyncBufReadExt;

// Line is a recorded log line.
#[derive(Debug)]
pub struct Line {
    // Token which identifies the line's log set.
    pub token: Uuid,
    // Stream of this logged line.
    pub stream: String,
    // Contents of the line.
    pub line: String,
}

// Tx is the channel sender of log Lines.
pub type Tx = tokio::sync::mpsc::Sender<Line>;

// capture_job_logs consumes newline-delimited lines from the AsyncRead and
// streams each as a Line to the channel Sender.
#[tracing::instrument(level = "debug", err, skip(tx, reader))]
pub async fn capture_lines<R>(
    tx: &Tx,
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
    let mut logged_at = Vec::new();

    loop {
        // Blocking read of the next line.
        match rx.recv().await {
            Some(Line {
                token,
                stream,
                line,
            }) => {
                tokens.push(token);
                streams.push(stream);
                lines.push(sanitize_null_bytes(line));
                logged_at.push(chrono::Utc::now());
            }
            None => {
                return Ok(());
            }
        }

        // Read additional ready lines without blocking.
        while let Ok(Line {
            token,
            stream,
            line,
        }) = rx.try_recv()
        {
            tokens.push(token);
            streams.push(stream);
            lines.push(sanitize_null_bytes(line));

            // Apply a total order to logs by incrementing logged_at with each line.
            // Note that Postgres has microsecond resolution for timestamps.
            logged_at.push(*logged_at.last().unwrap() + std::time::Duration::from_micros(1));
        }

        // Dispatch the vector of lines to the table.
        let r = sqlx::query(
            r#"
            INSERT INTO internal.log_lines (token, stream, log_line, logged_at)
            SELECT * FROM UNNEST($1, $2, $3, $4)
            "#,
        )
        .bind(&tokens)
        .bind(&streams)
        .bind(&lines)
        .bind(&logged_at)
        .execute(&pg_pool)
        .await?;

        tracing::trace!(rows = ?r.rows_affected(), "inserted logs");

        tokens.clear();
        streams.clear();
        lines.clear();
        logged_at.clear();
    }
}
