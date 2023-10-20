use futures::{channel::mpsc, future::BoxFuture, SinkExt, Stream, StreamExt, TryStreamExt};
use tokio::task::JoinHandle;

/// Container is a description of a running Container instance.
pub use proto_flow::runtime::Container;
use std::collections::BTreeMap;

/// Unsealed is a container context that's ready to spawn.
pub struct Unsealed<Request> {
    /// Image to run.
    pub command: Vec<String>,
    /// Environment variables.
    pub env: BTreeMap<String, String>,
    /// Log-level of the container, if known.
    pub log_level: Option<ops::LogLevel>,
    /// Whether to use protobuf.
    pub protobuf: bool,
    /// First request of the connector stream.
    pub request: Request,
}

/// UnsealFuture is the response type of a function that unseals Requests.
pub type UnsealFuture<Request> = BoxFuture<'static, anyhow::Result<Unsealed<Request>>>;

/// Connector manages the lifecycle of delegate containers in the broader
/// context of a longer-lived connectors RPC stream.
///
/// * Request: The RPC Request type.
/// * Response: The RPC Response type.
/// * Requests: A Stream of Request.
/// * Unseal: Attempt to Unseal a Request, returning Ok with a future that
///   resolves the Unsealed Result or, if the Request does not unseal,
///   then an Error with the unmodified Request.
pub struct Connector<Request, Response, Requests, Unseal, L>
where
    Request: serde::Serialize + prost::Message + Send + Sync + 'static,
    Response: Default + prost::Message + for<'de> serde::Deserialize<'de> + 'static,
    Requests: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    Unseal: Fn(Request) -> Result<UnsealFuture<Request>, Request>,
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
{
    log_handler: L,                                     // Log handler.
    request_rx: Requests,                               // Caller's input request stream.
    response_tx: mpsc::Sender<tonic::Result<Response>>, // Caller's output response stream.
    state: State<Request>,                              // Current container state.
    unseal: Unseal, // Unseals a Request, or returns the Request if it doesn't unseal.
}

enum State<Request> {
    Idle,
    // We're ready to start a container.
    Starting {
        unseal: UnsealFuture<Request>,
    },
    // Container has an active bidirectional stream.
    Running {
        container_status: JoinHandle<tonic::Result<()>>,
        container_tx: mpsc::Sender<Request>,
    },
    // We must restart a new container. We've sent the current one EOF,
    // and are waiting to see its EOF before we begin a new instance.
    Restarting {
        container_status: JoinHandle<tonic::Result<()>>,
        unseal: UnsealFuture<Request>,
    },
    // Requests reached EOF. We've sent EOF into the container and are
    // draining its final responses.
    Draining {
        container_status: JoinHandle<tonic::Result<()>>,
    },
}

impl<Request, Response, Requests, Unseal, L> Connector<Request, Response, Requests, Unseal, L>
where
    Request: Send + Sync + serde::Serialize + prost::Message + 'static,
    Response: Default + prost::Message + for<'de> serde::Deserialize<'de> + 'static,
    Requests: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    Unseal: Fn(Request) -> Result<UnsealFuture<Request>, Request>,
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
{
    pub fn new(
        log_handler: L,
        request_rx: Requests,
        unseal: Unseal,
    ) -> (Self, mpsc::Receiver<tonic::Result<Response>>) {
        let (response_tx, response_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

        (
            Self {
                log_handler,
                request_rx,
                response_tx,
                state: State::<Request>::Idle,
                unseal,
            },
            response_rx,
        )
    }

    /// Run the Connector until it's complete.
    pub async fn run(mut self) {
        loop {
            if let Err(status) = self.step().await {
                if status.code() == tonic::Code::Ok {
                    // Clean EOF.
                } else if let Err(send_error) = self.response_tx.send(Err(status.clone())).await {
                    tracing::warn!(%status, %send_error, "encountered terminal error but receiver is gone");
                }
                break;
            }
        }
    }

    async fn step(&mut self) -> tonic::Result<()> {
        let state = std::mem::replace(&mut self.state, State::Idle);

        Ok(match state {
            State::Idle => {
                let Some(request) = self.request_rx.try_next().await? else {
                    return Err(tonic::Status::ok("EOF")); // All done.
                };
                match (self.unseal)(request) {
                    Ok(unseal) => {
                        self.state = State::Starting { unseal };
                    }
                    Err(request) => {
                        return Err(tonic::Status::invalid_argument(format!(
                            "invalid initial Request: {}",
                            serde_json::to_string(&request).unwrap()
                        )));
                    }
                }
            }
            State::Starting { unseal } => {
                let Unsealed {
                    command,
                    env,
                    log_level,
                    protobuf,
                    request,
                } = unseal.await.map_err(crate::anyhow_to_status)?;

                let (mut container_tx, container_rx) = mpsc::channel(crate::CHANNEL_BUFFER);
                () = container_tx
                    .try_send(request)
                    .expect("can always send first request into buffered channel");

                let codec = if protobuf {
                    connector_init::Codec::Proto
                } else {
                    connector_init::Codec::Json
                };

                // Invoke the underlying local connector.
                let mut connector = connector_init::rpc::new_command(&command);
                connector.envs(&env);

                if let Some(log_level) = log_level {
                    connector.env("LOG_LEVEL", log_level.as_str_name());
                }

                let container_rx = connector_init::rpc::bidi::<Request, Response, _, _>(
                    connector,
                    codec,
                    container_rx.map(Result::Ok),
                    self.log_handler.clone(),
                )?;

                // Spawn task which reads and forwards connector responses.
                let mut response_tx = self.response_tx.clone();

                let container_status = tokio::spawn(async move {
                    let mut container_rx = std::pin::pin!(container_rx);

                    while let Some(response) = container_rx.try_next().await? {
                        if let Err(_) = response_tx.send(Ok(response)).await {
                            return Err(tonic::Status::cancelled(
                                "failed to forward response because receiver is gone",
                            ));
                        }
                    }
                    Ok(())
                });

                self.state = State::Running {
                    container_status,
                    container_tx,
                };
            }
            State::Running {
                mut container_status,
                mut container_tx,
            } => {
                // Wait for a next request or for the container to exit.
                let request = tokio::select! {
                    status = &mut container_status => {
                        () = status.unwrap()?;

                        return Err(tonic::Status::aborted(
                            "connector unexpectedly closed its output stream while its input stream is still open"
                        ))
                    },
                    request = self.request_rx.try_next() => request?,
                };

                match request.map(|request| (self.unseal)(request)) {
                    // A non-sealed request that we simply forward.
                    Some(Err(request)) => {
                        container_tx.feed(request).await.map_err(|_send_err| {
                            tonic::Status::internal(
                                "connector unexpectedly closed its input stream",
                            )
                        })?;

                        self.state = State::Running {
                            container_status,
                            container_tx,
                        };
                    }
                    // Sealed request which requires that we restart the container.
                    Some(Ok(unseal)) => {
                        let _drop_to_send_eof = container_tx;
                        self.state = State::Restarting {
                            container_status,
                            unseal,
                        };
                    }
                    // End of input.
                    None => {
                        let _drop_to_send_eof = container_tx;
                        self.state = State::Draining { container_status };
                    }
                }
            }
            State::Restarting {
                container_status,
                unseal,
            } => {
                () = container_status.await.unwrap()?;
                self.state = State::Starting { unseal };
            }
            State::Draining { container_status } => {
                () = container_status.await.unwrap()?;
                return Err(tonic::Status::ok("EOF")); // All done.
            }
        })
    }
}
