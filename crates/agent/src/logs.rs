use tokio::io::AsyncBufReadExt;
use tracing::trace;

// Line is a recorded log line.
#[derive(Debug)]
pub struct Line {
    // Token which identifies the line's log set.
    token: uuid::Uuid,
    // Stream of this logged line.
    stream: String,
    // Contents of the line.
    line: String,
}

// Tx is the channel sender of log Lines.
pub type Tx = tokio::sync::mpsc::Sender<Line>;

// capture_job_logs consumes newline-delimited lines from the AsyncRead and
// streams each as a Line to the channel Sender.
#[tracing::instrument(err, skip(tx, reader))]
pub async fn capture_lines<R>(
    tx: Tx,
    stream: String,
    token: uuid::Uuid,
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

    // Block to read lines.
    while let Some(Line {
        token,
        stream,
        line,
    }) = rx.recv().await
    {
        trace!(%token, %stream, %line, "rx (initial)");
        tokens.push(token);
        streams.push(stream);
        lines.push(line);

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
            lines.push(line);
        }

        // Dispatch the vector of lines to the table.
        let r: sqlx::postgres::PgQueryResult = sqlx::query(
            r#"
            INSERT INTO internal.log_lines (token, stream, log_line)
            SELECT * FROM UNNEST($1, $2, $3)
            "#,
        )
        .bind(&tokens)
        .bind(&streams)
        .bind(&lines)
        .execute(&pg_pool)
        .await?;

        trace!(rows = ?r.rows_affected(), "inserted logs");

        tokens.clear();
        streams.clear();
        lines.clear();
    }

    Ok(())
}
