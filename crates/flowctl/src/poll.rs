use serde::Deserialize;

// Poll an async task in `table` having `id` until it's no longer queued.
// While we're waiting print out logs under `logs_token`.
pub async fn poll_while_queued(
    client: &crate::Client,
    table: &str,
    id: models::Id,
    logs_token: &str,
) -> anyhow::Result<String> {
    tracing::info!(%id, %logs_token, "Waiting for {table} job");

    tokio::select! {
        outcome = poll_table(client, table, id) => return outcome,
        result = stream_logs(client, logs_token) => return Err(result.unwrap_err()),
    };
}

pub async fn poll_table(
    client: &crate::Client,
    table: &str,
    id: models::Id,
) -> anyhow::Result<String> {
    let outcome = loop {
        let resp = client
            .from(table)
            .select("job_status->>type")
            .eq("id", id.to_string())
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
    };
    Ok(outcome)
}

pub async fn stream_logs(client: &crate::Client, logs_token: &str) -> anyhow::Result<()> {
    let mut last_logged_at = None;

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let resp = client
            .rpc(
                "view_logs",
                serde_json::json!({
                    "bearer_token": logs_token,
                    "last_logged_at": last_logged_at,
                })
                .to_string(),
            )
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
