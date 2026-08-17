//! One session, driven from stdin. This is how a disk is exercised by hand.
//!
//! It speaks the same gRPC a runtime does. It also holds the acknowledgement a
//! publication returns, so a commit is a word rather than a paste. Every event is
//! one line on stdout, and each line begins with what happened. The same tool is
//! therefore both the daemon's demo surface and its smoke test:
//!
//! ```text
//! mounted /var/lib/disks/disk-3
//! published 220
//! committed
//! closed
//! ```

use crate::args;
use crate::proto;
use anyhow::Context;

/// Open a disk, print its mount path, and serve stdin until it ends.
pub async fn run(args: args::Client) -> anyhow::Result<()> {
    let channel =
        tonic::transport::Endpoint::from_shared(format!("unix://{}", args.uds_path.display()))
            .context("a socket path is a URI")?
            .connect()
            .await
            .with_context(|| format!("connecting to the daemon on {:?}", args.uds_path))?;

    let (requests, receiver) = tokio::sync::mpsc::channel(1);

    let mut responses = proto_grpc::disk::disk_client::DiskClient::new(channel)
        .session(tokio_stream::wrappers::ReceiverStream::new(receiver))
        .await
        .context("opening a session")?
        .into_inner();

    () = send(&requests, proto::request::Request::Open(open(&args))).await?;

    match reply(&mut responses).await? {
        proto::response::Response::Opened(proto::Opened { mount_path }) => {
            println!("mounted {mount_path}")
        }
        response => anyhow::bail!("expected Opened, got {response:?}"),
    }

    let mut lines = stdin_lines();
    let mut ack = bytes::Bytes::new();

    loop {
        let line = tokio::select! {
            line = lines.recv() => line,

            // Ending the stream tears the disk down, so an interrupt leaves the
            // same state a clean exit does.
            outcome = tokio::signal::ctrl_c() => {
                () = outcome.context("awaiting SIGINT")?;
                None
            }
        };
        let Some(line) = line else { break };
        let mut words = line.split_whitespace();

        match words.next() {
            None => continue,
            Some("quit") => break,

            Some("publish") => {
                () = send(
                    &requests,
                    proto::request::Request::Publish(proto::Publish {}),
                )
                .await?;

                match reply(&mut responses).await? {
                    proto::response::Response::Published(published) => {
                        ack = published.ack;

                        match ack.is_empty() {
                            true => println!("unchanged"),
                            false => println!("published {}", ack.len()),
                        }
                    }
                    response => anyhow::bail!("expected Published, got {response:?}"),
                }
            }
            Some("commit") => {
                let ack = std::mem::take(&mut ack);
                anyhow::ensure!(!ack.is_empty(), "nothing is published to commit");

                () = send(
                    &requests,
                    proto::request::Request::Commit(proto::Commit { ack }),
                )
                .await?;

                match reply(&mut responses).await? {
                    proto::response::Response::Committed(proto::Committed {}) => {
                        println!("committed")
                    }
                    response => anyhow::bail!("expected Committed, got {response:?}"),
                }
            }
            // A replacement has no reply. A broker which cannot be reached
            // surfaces at the next publication instead.
            Some("broker") => {
                let broker = proto::Broker {
                    endpoint: words.next().unwrap_or_default().to_string(),
                    credential: words.next().unwrap_or_default().to_string(),
                };
                println!("broker {}", broker.endpoint);

                () = send(&requests, proto::request::Request::Broker(broker)).await?;
            }
            Some(other) => {
                eprintln!(
                    "{other:?} is not one of: publish, commit, broker <endpoint> [credential], quit"
                );
            }
        }
    }
    drop(requests);

    // The daemon closes its half only once the disk is unmounted and its device
    // is deleted. This read therefore waits for the teardown.
    if let Some(response) = responses.message().await.context("ending the session")? {
        anyhow::bail!("session closed with an unexpected {response:?}");
    }
    println!("closed");

    Ok(())
}

fn open(args: &args::Client) -> proto::Open {
    proto::Open {
        journal_config: Some(proto::JournalConfig {
            journal: args.journal.clone(),
            fragment_stores: args.fragment_store.clone(),
            replication: args.replication,
            labels: args
                .label
                .iter()
                .map(|(name, value)| proto::Label {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            fragment_length: args.fragment_length,
            flush_interval_seconds: Some(args.flush_interval_seconds),
            refresh_interval_seconds: args.refresh_interval_seconds,
            max_append_rate: Some(args.max_append_rate),
            compression_codec: args.compression_codec as i32,
        }),
        device_size: args.device_size,
        block_size: args.block_size,
        broker: Some(proto::Broker {
            endpoint: args.broker_endpoint.clone(),
            credential: args.broker_credential.clone().unwrap_or_default(),
        }),
        // A disk driven by hand has no durable state of its own to recover an
        // acknowledgement from.
        recovered_acks: Vec::new(),
    }
}

async fn send(
    requests: &tokio::sync::mpsc::Sender<proto::Request>,
    request: proto::request::Request,
) -> anyhow::Result<()> {
    requests
        .send(proto::Request {
            request: Some(request),
        })
        .await
        .map_err(|_| anyhow::anyhow!("the session has ended"))
}

async fn reply(
    responses: &mut tonic::Streaming<proto::Response>,
) -> anyhow::Result<proto::response::Response> {
    match responses.message().await {
        Ok(Some(proto::Response { response })) => response.context("a reply carries no message"),
        Ok(None) => anyhow::bail!("the session ended without a reply"),
        Err(status) => Err(anyhow::anyhow!("{status}")),
    }
}

/// Stdin as a stream of lines. A thread of its own reads it, because a blocking
/// read of a terminal must not hold a runtime worker.
fn stdin_lines() -> tokio::sync::mpsc::Receiver<String> {
    let (lines, receiver) = tokio::sync::mpsc::channel(1);

    _ = std::thread::spawn(move || {
        for line in std::io::BufRead::lines(std::io::stdin().lock()) {
            let Ok(line) = line else { return };

            if lines.blocking_send(line).is_err() {
                return;
            }
        }
    });
    receiver
}
