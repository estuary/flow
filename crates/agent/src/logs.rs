use futures::StreamExt;
use tokio::io::AsyncBufReadExt;
use tracing::{debug, trace};

// Line is a recorded log line.
#[derive(Debug)]
pub struct Line {
    // Token which identifies the line's log set.
    token: uuid::Uuid,
    // Stream of this logged line.
    stream: String,
    // Contents of the line.
    line: Vec<u8>,
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
    pg_conn: tokio_postgres::Client,
    mut rx: tokio::sync::mpsc::Receiver<Line>,
) -> Result<(), tokio_postgres::Error> {
    pg_conn
        .execute("SET synchronous_commit = 'off';", &[]) // Don't wait for fsync.
        .await?;
    let stmt = pg_conn
        .prepare("INSERT INTO logs (token, stream, line) VALUES ($1, $2, $3);")
        .await?;

    let stmt = &stmt;
    let pg_conn = &pg_conn;

    let mut awaiting = futures::stream::FuturesOrdered::new();
    let mut closed = false;
    loop {
        tokio::select! {
            // Prefer to drain completed INSERTs before starting new ones.
            biased;

            result = awaiting.next(), if !awaiting.is_empty() => {
                let result = result.unwrap();
                trace!(?result, "log insert completion");
                result?;
            }
            recv = rx.recv(), if !closed => {
                match recv {
                    None => {
                        closed = true;
                        debug!("read rx close");
                    }
                    Some(Line{token, stream, line}) => {
                        trace!(%token, %stream, line = %String::from_utf8_lossy(&line), "rx");

                        // Use a closure to ensure parameters aren't dropped until done.
                        let fut = move || async move {
                            let line = String::from_utf8_lossy(&line);
                            pg_conn.execute(stmt, &[&token, &stream, &line]).await
                        };
                        awaiting.push(fut());
                    }
                }
            }
            else => {
                debug!("awaiting logs are drained and rx is closed; exiting");
                return Ok(())
            }
        }
    }
}
