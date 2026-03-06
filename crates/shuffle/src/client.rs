use anyhow::Context;
use futures::StreamExt;
use proto_flow::shuffle;
use tokio::sync::mpsc;

/// Client for the Session RPC, providing a structured interface for
/// opening a session, requesting checkpoints, and cleanly closing.
///
/// Spawns an in-process Session actor via the shuffle Service.
pub struct SessionClient {
    request_tx: mpsc::Sender<shuffle::SessionRequest>,
    response_rx: mpsc::Receiver<tonic::Result<shuffle::SessionResponse>>,
}

impl SessionClient {
    /// Open a Session, sending the Open request and resume
    /// checkpoint, then waiting for the Opened response.
    pub async fn open(
        service: &crate::Service,
        session_id: u64,
        task: shuffle::Task,
        members: Vec<shuffle::Member>,
        resume_checkpoint: crate::Frontier,
    ) -> anyhow::Result<Self> {
        let verify = crate::verify("SessionResponse", "Opened", "(in-process)", 0);
        let (request_tx, request_rx) = crate::new_channel::<shuffle::SessionRequest>();
        let request_rx =
            tokio_stream::wrappers::ReceiverStream::new(request_rx).map(Ok::<_, tonic::Status>);

        let mut response_rx = service.spawn_session(request_rx);

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

        match verify.not_eof(response_rx.recv().await)? {
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
        })
    }

    /// Send NextCheckpoint and collect the complete frontier response.
    pub async fn next_checkpoint(&mut self) -> anyhow::Result<crate::Frontier> {
        tracing::debug!("requesting NextCheckpoint");

        let verify = crate::verify(
            "SessionResponse",
            "next_checkpoint_chunk",
            "(in-process)",
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
            let chunk = match verify.not_eof(self.response_rx.recv().await)? {
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
        } = self;

        drop(request_tx); // Drop to close RPC send.

        let verify = crate::verify("SessionResponse", "EOF", "(in-process)", 0);
        verify.eof(response_rx.recv().await)
    }
}
