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
#[tracing::instrument(err, skip(tx, reader))]
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
                    lines.push(line);
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
            lines.push(line);
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
