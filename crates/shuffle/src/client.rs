use anyhow::Context;
use futures::StreamExt;
use proto_flow::shuffle;
use tokio::sync::mpsc;

/// Client for the Session RPC, providing a structured interface for
/// opening a session, requesting checkpoints, and cleanly closing.
///
/// Connects to the shuffle service over gRPC transport.
pub struct SessionClient {
    request_tx: mpsc::Sender<shuffle::SessionRequest>,
    response_rx: tonic::codec::Streaming<shuffle::SessionResponse>,
    endpoint: String,
}

impl SessionClient {
    /// Open a Session over gRPC, sending the Open request and resume
    /// checkpoint, then waiting for the Opened response.
    pub async fn open(
        endpoint: &str,
        session_id: u64,
        task: shuffle::Task,
        members: Vec<shuffle::Member>,
        resume_checkpoint: crate::Frontier,
    ) -> anyhow::Result<Self> {
        let mut grpc_client =
            proto_grpc::shuffle::shuffle_client::ShuffleClient::connect(endpoint.to_string())
                .await
                .context("connecting to shuffle service")?
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);

        let verify = crate::verify("SessionResponse", "Opened", endpoint, 0);
        let (request_tx, request_rx) = crate::new_channel::<shuffle::SessionRequest>();
        let request_rx = tokio_stream::wrappers::ReceiverStream::new(request_rx);

        let mut response_rx = verify
            .ok(grpc_client.session(request_rx).await)?
            .into_inner();

        // Send Open request and read Opened response.
        crate::verify_send(
            &request_tx,
            shuffle::SessionRequest {
                open: Some(shuffle::session_request::Open {
                    session_id,
                    task: Some(task),
                    members,
                }),
                ..Default::default()
            },
        )?;

        match verify.not_eof(response_rx.next().await)? {
            shuffle::SessionResponse {
                opened: Some(shuffle::session_response::Opened {}),
                ..
            } => (),
            response => return Err(verify.fail(response)),
        };

        // Send the resume checkpoint (including the empty-chunk terminator).
        let mut drain = crate::frontier::Drain::new();
        drain.start(resume_checkpoint);

        while let Some(chunk) = drain.next_chunk() {
            // Sends are best-effort: an error here will just be a RST.
            // We'll surface a proper causal error later, on next read.
            let _: Result<(), _> = request_tx
                .send(shuffle::SessionRequest {
                    resume_checkpoint_chunk: Some(chunk),
                    ..Default::default()
                })
                .await;
        }

        Ok(Self {
            request_tx,
            response_rx,
            endpoint: endpoint.to_string(),
        })
    }

    /// Send NextCheckpoint and collect the complete frontier response.
    pub async fn next_checkpoint(&mut self) -> anyhow::Result<crate::Frontier> {
        tracing::debug!("requesting NextCheckpoint");

        let verify = crate::verify(
            "SessionResponse",
            "next_checkpoint_chunk",
            &self.endpoint,
            0,
        );

        let _: Result<(), _> = self
            .request_tx
            .send(shuffle::SessionRequest {
                next_checkpoint: Some(shuffle::session_request::NextCheckpoint {}),
                ..Default::default()
            })
            .await;

        let mut journals = Vec::new();
        loop {
            let chunk = match verify.not_eof(self.response_rx.next().await)? {
                shuffle::SessionResponse {
                    next_checkpoint_chunk: Some(chunk),
                    ..
                } => chunk,
                response => return Err(verify.fail(response)),
            };

            if chunk.journals.is_empty() {
                break;
            }
            journals.extend(crate::JournalFrontier::decode(chunk));
        }

        tracing::debug!(journals = journals.len(), "received NextCheckpoint");
        crate::Frontier::new(journals).context("validating checkpoint frontier")
    }

    /// Cleanly close the session by dropping the request sender and reading server EOF.
    pub async fn close(self) -> anyhow::Result<()> {
        let Self {
            request_tx,
            mut response_rx,
            endpoint,
        } = self;

        drop(request_tx); // Drop to close RPC send.

        let verify = crate::verify("SessionResponse", "EOF", &endpoint, 0);
        verify.eof(response_rx.next().await)
    }
}
