use serde::Deserialize;

// Poll an async task in `table` having `id` until it's no longer queued.
// While we're waiting print out logs under `logs_token`.
pub async fn poll_while_queued(
    client: &postgrest::Postgrest,
    table: &str,
    id: &str,
    logs_token: &str,
) -> anyhow::Result<String> {
    tracing::info!(%id, %logs_token, "Waiting for {table} job");

    let mut offset = 0;

    let outcome = loop {
        let resp = client
            .from(table)
            .select("job_status->>type")
            .eq("id", id)
            .single()
            .execute()
            .await?
            .error_for_status()?;

        #[derive(Deserialize, Debug)]
        struct PollResponse {
            r#type: String,
        }
        let PollResponse { r#type: job_status } = resp.json().await?;
        tracing::trace!(%job_status, %id, %table, "polled job");

        if job_status != "queued" {
            break job_status;
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let resp = client
            .rpc(
                "view_logs",
                serde_json::json!({
                    "bearer_token": logs_token,
                })
                .to_string(),
            )
            .range(offset, 1 << 24) // Fixed upper bound of 16M log lines.
            .execute()
            .await?
            .error_for_status()?;

        #[derive(Deserialize, Debug)]
        struct Log {
            log_line: String,
            logged_at: crate::Timestamp,
            stream: String,
        }
        let logs: Vec<Log> = resp.json().await?;
        offset += logs.len();

        for Log {
            log_line,
            logged_at,
            stream,
        } in logs
        {
            println!("{logged_at} {stream}> {log_line}");
        }
    };

    Ok(outcome)
}
