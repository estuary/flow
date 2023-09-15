use super::container;
use futures::{channel::mpsc, future::BoxFuture, SinkExt, Stream, TryStreamExt};
use tokio::task::JoinHandle;

/// Container is a description of a running Container instance.
pub use proto_flow::runtime::Container;

/// Unsealed is a container context that's ready to spawn.
pub struct Unsealed<Request> {
    /// Image to run.
    pub image: String,
    /// Log-level of the container, if known.
    pub log_level: Option<ops::LogLevel>,
    /// First request of the connector stream.
    pub request: Request,
}

/// UnsealFuture is the response type of a function that unseals Requests.
pub type UnsealFuture<Request> = BoxFuture<'static, anyhow::Result<Unsealed<Request>>>;

/// StartRpcFuture is the response type of a function that starts a connector RPC.
pub type StartRpcFuture<Response> =
    BoxFuture<'static, tonic::Result<tonic::Response<tonic::Streaming<Response>>>>;

/// Connector manages the lifecycle of delegate containers in the broader
/// context of a longer-lived connectors RPC stream.
///
/// * Request: The RPC Request type.
/// * Response: The RPC Response type.
/// * Requests: A Stream of Request.
/// * Unseal: Attempt to Unseal a Request, returning Ok with a future that
///   resolves the Unsealed Result or, if the Request does not unseal,
///   then an Error with the unmodified Request.
/// * StartRpc: Start an RPC stream with the container channel.
/// * Attach: Attach a Container description to the first Response
///   of each delegate container lifecycle.
pub struct Connector<Request, Response, Requests, Unseal, StartRpc, Attach, L>
where
    Request: serde::Serialize,
    Response: Send + Sync + 'static,
    Requests: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    Unseal: Fn(Request) -> Result<UnsealFuture<Request>, Request>,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>,
    Attach: Fn(&mut Response, Container) + Clone + Send + Sync + 'static,
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
{
    attach_container: Attach, // Attaches a Container description to a response.
    log_handler: L,           // Log handler.
    network: String,          // Container network to use.
    request_rx: Requests,     // Caller's input request stream.
    response_tx: mpsc::Sender<tonic::Result<Response>>, // Caller's output response stream.
    start_rpc: StartRpc,      // Begins RPC over a started container channel.
    state: State<Request>,    // Current container state.
    task_name: String,        // Name of this task, used to label container.
    task_type: ops::TaskType, // Type of this task, for labeling container.
    unseal: Unseal,           // Unseals a Request, or returns the Request if it doesn't unseal.
}

// TODO(johnny): This State can be extended with a Resumed variant when we
// finally tackle incremental container snapshots & recovery. The Resumed
// variant would introspect and hold the effective Request.Open of the resumed
// container, which would then be matched with a current Open to either resume
// or drain a recovered connector instance.
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
    // Requests reach EOF. We've sent EOF into the container and are
    // draining its final responses.
    Draining {
        container_status: JoinHandle<tonic::Result<()>>,
    },
}

impl<Request, Response, Requests, Unseal, StartRpc, Attach, L>
    Connector<Request, Response, Requests, Unseal, StartRpc, Attach, L>
where
    Request: serde::Serialize,
    Response: Send + Sync + 'static,
    Requests: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    Unseal: Fn(Request) -> Result<UnsealFuture<Request>, Request>,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>,
    Attach: Fn(&mut Response, Container) + Clone + Send + Sync + 'static,
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
{
    pub fn new(
        attach_container: Attach,
        log_handler: L,
        network: &str,
        request_rx: Requests,
        start_rpc: StartRpc,
        task_name: &str,
        task_type: ops::TaskType,
        unseal: Unseal,
    ) -> (Self, mpsc::Receiver<tonic::Result<Response>>) {
        let (response_tx, response_rx) = mpsc::channel(crate::CHANNEL_BUFFER);

        (
            Self {
                attach_container,
                unseal,
                network: network.to_string(),
                request_rx,
                start_rpc,
                response_tx,
                state: State::<Request>::Idle,
                task_name: task_name.to_string(),
                log_handler,
                task_type,
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
                    image,
                    log_level,
                    request,
                } = unseal.await.map_err(crate::anyhow_to_status)?;

                let (mut container_tx, container_rx) = mpsc::channel(crate::CHANNEL_BUFFER);
                () = container_tx
                    .try_send(request)
                    .expect("can always send first request into buffered channel");

                let (container, channel, guard) = container::start(
                    &image,
                    self.log_handler.clone(),
                    log_level,
                    &self.network,
                    &self.task_name,
                    self.task_type,
                )
                .await
                .map_err(crate::anyhow_to_status)?;

                // Start RPC over the container's gRPC `channel`.
                let mut container_rx = (self.start_rpc)(channel, container_rx).await?.into_inner();

                // Spawn task which reads and forwards connector responses.
                let mut attach = Some((container, self.attach_container.clone()));
                let mut response_tx = self.response_tx.clone();

                let container_status = tokio::spawn(async move {
                    let _guard = guard; // Hold guard while still reading responses.

                    while let Some(mut response) = container_rx.try_next().await? {
                        if let Some((container, attach)) = attach.take() {
                            (attach)(&mut response, container);
                        }
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
