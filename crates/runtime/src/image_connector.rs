use super::container;
use futures::future::BoxFuture;
use futures::SinkExt;
use futures::{channel::mpsc, Stream, StreamExt};

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
    Requests: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    Unseal: Fn(Request) -> Result<UnsealFuture<Request>, Request>,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>,
    Attach: Fn(&mut Response, Container),
    L: Fn(&ops::Log) + Clone + Send + Sync + 'static,
{
    attach_container: Attach,
    container: Option<Container>,
    log_handler: L,
    network: String,
    request_rx: Requests,
    response_tx: mpsc::Sender<tonic::Result<Response>>,
    start_rpc: StartRpc,
    state: State<Request, Response>,
    task_name: String,
    task_type: ops::TaskType,
    unseal: Unseal,
}

// TODO(johnny): This State can be extended with a Resumed variant when we
// finally tackle incremental container snapshots & recovery. The Resumed
// variant would introspect and hold the effective Request.Open of the resumed
// container, which would then be matched with a current Open to either resume
// or drain a recovered connector instance.
enum State<Request, Response> {
    // We're ready to start a container.
    Idle,
    // Container has an active bidirectional stream.
    Running {
        container_rx: tonic::Streaming<Response>,
        container_tx: mpsc::Sender<Request>,
        guard: container::Guard,
    },
    // We must restart a new container. We've sent the current one EOF,
    // and are waiting to see its EOF before we begin a new instance.
    Restarting {
        container_rx: tonic::Streaming<Response>,
        _guard: container::Guard,
        unseal: UnsealFuture<Request>,
    },
    // Requests reach EOF. We've sent EOF into the container and are
    // draining its final responses.
    Draining {
        container_rx: tonic::Streaming<Response>,
        _guard: container::Guard,
    },
}

impl<Request, Response, Requests, Unseal, StartRpc, Attach, L>
    Connector<Request, Response, Requests, Unseal, StartRpc, Attach, L>
where
    Request: serde::Serialize,
    Requests: Stream<Item = tonic::Result<Request>> + Send + Unpin + 'static,
    Unseal: Fn(Request) -> Result<UnsealFuture<Request>, Request>,
    StartRpc: Fn(tonic::transport::Channel, mpsc::Receiver<Request>) -> StartRpcFuture<Response>,
    Attach: Fn(&mut Response, Container),
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
                container: None,
                network: network.to_string(),
                request_rx,
                start_rpc,
                response_tx,
                state: State::<Request, Response>::Idle,
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
            // Select over the next request or the next container response.
            // We use Result as a semantic "either" type.
            let rx = match &mut self.state {
                // We're only reading requests.
                State::Idle => Err(self.request_rx.next().await),
                // We're reading requests and container responses.
                State::Running { container_rx, .. } => tokio::select! {
                    rx = container_rx.next() => Ok(rx),
                    rx = self.request_rx.next() => Err(rx),
                },
                // We're only reading container responses.
                State::Restarting { container_rx, .. } | State::Draining { container_rx, .. } => {
                    Ok(container_rx.next().await)
                }
            };

            if !match rx {
                Ok(Some(Ok(rx))) => self.container_rx(rx).await,
                Ok(Some(Err(status))) => self.on_error(status).await,
                Ok(None) => self.container_eof().await,
                Err(Some(Ok(rx))) => self.request_rx(rx).await,
                Err(Some(Err(status))) => self.on_error(status).await,
                Err(None) => self.request_eof().await,
            } {
                break;
            }
        }
    }

    async fn spawn_container(
        &mut self,
        Unsealed {
            image,
            log_level,
            request,
        }: Unsealed<Request>,
    ) -> bool {
        assert!(matches!(&self.state, State::Idle));

        let (mut container_tx, container_rx) = mpsc::channel(crate::CHANNEL_BUFFER);
        () = container_tx
            .try_send(request)
            .expect("can always send first request into buffered channel");

        let started = container::start(
            &image,
            self.log_handler.clone(),
            log_level,
            &self.network,
            &self.task_name,
            self.task_type,
        )
        .await;

        let (container, channel, guard) = match started {
            Ok(ok) => ok,
            Err(err) => return self.on_error(crate::anyhow_to_status(err)).await,
        };

        let container_rx = match (self.start_rpc)(channel, container_rx).await {
            Ok(ok) => ok,
            Err(status) => return self.on_error(status).await,
        }
        .into_inner();

        self.state = State::Running {
            container_rx,
            container_tx,
            guard,
        };
        self.container = Some(container);

        true
    }

    async fn container_rx(&mut self, mut rx: Response) -> bool {
        // Attach Container to the first Response of a delegate container session.
        if let Some(container) = self.container.take() {
            (self.attach_container)(&mut rx, container);
        }

        if let Err(send_error) = self.response_tx.send(Ok(rx)).await {
            tracing::warn!(%send_error, "failed to forward container response");
            false // All done. Container is cancelled via Drop.
        } else {
            true
        }
    }

    async fn request_rx(&mut self, rx: Request) -> bool {
        match (self.unseal)(rx) {
            Ok(unseal) => match std::mem::replace(&mut self.state, State::Idle) {
                State::Idle => match unseal.await {
                    Ok(unsealed) => self.spawn_container(unsealed).await,
                    Err(error) => self.on_error(crate::anyhow_to_status(error)).await,
                },
                State::Running {
                    container_rx,
                    container_tx: _, // Send EOF.
                    guard,
                } => {
                    self.state = State::Restarting {
                        container_rx,
                        _guard: guard,
                        unseal,
                    };
                    true
                }
                State::Restarting { .. } => unreachable!("not reading requests while restarting"),
                State::Draining { .. } => unreachable!("not reading requests while draining"),
            },
            Err(rx) => match &mut self.state {
                State::Idle => {
                    self.on_error(tonic::Status::invalid_argument(format!(
                        "invalid Request when no image container is running: {}",
                        serde_json::to_string(&rx).unwrap()
                    )))
                    .await
                }
                State::Running { container_tx, .. } => match container_tx.send(rx).await {
                    Ok(()) => true,
                    Err(_send_error) => {
                        self.on_error(tonic::Status::internal(
                            "connector unexpectedly closed its running request stream",
                        ))
                        .await
                    }
                },
                State::Restarting { .. } => unreachable!("not reading requests while restarting"),
                State::Draining { .. } => unreachable!("not reading requests while draining"),
            },
        }
    }

    async fn container_eof(&mut self) -> bool {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Idle => unreachable!("not reading responses while idle"),
            State::Running {
                container_rx: _,
                container_tx: _,
                guard: _,
            } => {
                self.on_error(tonic::Status::aborted(
                    "connector unexpectedly closed its response stream while the request stream was still open")).await
            }
            State::Restarting {
                container_rx: _,
                _guard: _,
                unseal,
            } => {
                // Previous delegate has completed; start the next one.
                match unseal.await {
                    Ok(unsealed) => self.spawn_container(unsealed).await,
                    Err(error) => self.on_error(crate::anyhow_to_status(error)).await,
                }
            }
            State::Draining {
                container_rx: _,
                _guard: _,
            } => false, // `request_rx` has already EOF'd. All done.
        }
    }

    async fn request_eof(&mut self) -> bool {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Idle => false, // No running container. All done.
            State::Running {
                container_rx,
                container_tx: _, // Send EOF.
                guard,
            } => {
                self.state = State::Draining {
                    container_rx,
                    _guard: guard,
                };
                true // Wait for EOF from container.
            }
            State::Restarting { .. } => unreachable!("not reading requests"),
            State::Draining { .. } => unreachable!("not reading requests"),
        }
    }

    async fn on_error(&mut self, status: tonic::Status) -> bool {
        if let Err(send_error) = self.response_tx.send(Err(status.clone())).await {
            tracing::warn!(%status, %send_error, "encountered terminal error but response stream is cancelled");
        }
        false // All done. If a container is running, it's cancelled via Drop.
    }
}
